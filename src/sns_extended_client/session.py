from boto3 import resource
import botoinator
from json import dumps as jsondumps
from uuid import uuid4


DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
MESSAGE_POINTER_CLASS = 'com.amazon.sqs.javamessaging.MessageS3Pointer'
RESERVED_ATTRIBUTE_NAME = 'ExtendedPayloadSize'
S3_KEY_ATTRIBUTE_NAME = 'S3Key'
MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE = 'json'
MAX_ALLOWED_ATTRIBUTES = 10 - 1 # 10 for SQS and 1 reserved attribute


def _delete_large_payload_support(self):
  if hasattr(self, '__s3_bucket_name'):
    del self.__s3_bucket_name


def _get_large_payload_support(self):
  return getattr(self, '__s3_bucket_name', None)


def _set_large_payload_support(self, s3_bucket_name):
  assert isinstance(s3_bucket_name, str) or not s3_bucket_name
  setattr(self, '__s3_bucket_name', s3_bucket_name)


def _delete_messsage_size_threshold(self):
  setattr(self, '__message_size_threshold', DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _get_message_size_threshold(self):
  return getattr(self, '__message_size_threshold', DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _set_message_size_threshold(self, message_size_threshold):
  assert isinstance(message_size_threshold, int) and 0 <= message_size_threshold <= DEFAULT_MESSAGE_SIZE_THRESHOLD
  setattr(self, '__message_size_threshold', message_size_threshold)


def _delete_always_through_s3(self):
  setattr(self, '__always_through_s3', False)


def _get_always_through_s3(self):
  return getattr(self, '__always_through_s3', False)


def _set_always_through_s3(self, always_through_s3):
  assert isinstance(always_through_s3, bool)
  assert not always_through_s3 or (always_through_s3 and self.large_payload_support)
  setattr(self, '__always_through_s3', always_through_s3)


def _delete_s3(self):
  if hasattr(self, '__s3'):
    del self.__s3


def _get_s3(self):
  s3 = getattr(self, '__s3', None)
  if not s3:
    s3 = resource('s3')
    self.s3 = s3
  return s3


def _set_s3(self, s3):
  setattr(self, '__s3', s3)


def _is_large_message(self, attributes, encoded_body):
  total = 0
  for key, value in attributes.items():
    total = total + len(key.encode())
    if 'DataType' in value:
      total = total + len(value['DataType'].encode())
    if 'StringValue' in value:
      total = total + len(value['StringValue'].encode())
    if 'BinaryValue' in value:
      total = total + len(value['BinaryValue'])
  total = total + len(encoded_body)
  return self.message_size_threshold < total

def _check_message_attribute_size(self, attributes):
  total = 0
  for key, value in attributes.items():
    total = total + len(key.encode())
    if 'DataType' in value:
      total = total + len(value['DataType'].encode())
    if 'StringValue' in value:
      total = total + len(value['StringValue'].encode())
    if 'BinaryValue' in value:
      total = total + len(value['BinaryValue'])
  
  if total > self.message_size_threshold:
    raise Exception('Message Attribute Size is greater than the message size threshold consider including payload in the message body')


def _get_s3_key(self, message_attributes):
  if S3_KEY_ATTRIBUTE_NAME in message_attributes:
    return message_attributes[S3_KEY_ATTRIBUTE_NAME]['StringValue']
  return str(uuid4())


def _create_s3_put_object_params(self, encoded_body):
  return {
    'ACL': 'private',
    'Body': encoded_body,
    'ContentLength': len(encoded_body)
  }

def _determine_payload(self, message_attributes, message_body, message_structure):
  encoded_body = message_body.encode()
  if self.large_payload_support and (self.always_through_s3 or self._is_large_message(message_attributes, encoded_body)):

    if message_structure == 'json':
      raise Exception('SNS extended client does not support sending JSON messages for large messages.')

    if RESERVED_ATTRIBUTE_NAME in message_attributes:
      raise Exception(f'Message attribute name {RESERVED_ATTRIBUTE_NAME} is reserved for use by SNS extended client.')

    message_attributes[RESERVED_ATTRIBUTE_NAME] = {}
    message_attributes[RESERVED_ATTRIBUTE_NAME]['DataType'] = 'Number'
    message_attributes[RESERVED_ATTRIBUTE_NAME]['StringValue'] = str(len(encoded_body))

    s3_key = self._get_s3_key(message_attributes)

    self.s3.Object(self.large_payload_support, s3_key).put(**self._create_s3_put_object_params(encoded_body))
    message_body = jsondumps([MESSAGE_POINTER_CLASS, {'s3BucketName': self.large_payload_support, 's3Key': s3_key}])

  return message_attributes, message_body


def _add_custom_attributes(class_attributes):
  class_attributes['large_payload_support'] = property(
    _get_large_payload_support, 
    _set_large_payload_support, 
    _delete_large_payload_support
  )
  class_attributes['message_size_threshold'] = property(
    _get_message_size_threshold, 
    _set_message_size_threshold, 
    _delete_messsage_size_threshold
  )
  class_attributes['always_through_s3'] = property(
    _get_always_through_s3,
    _set_always_through_s3,
    _delete_always_through_s3
  )
  class_attributes['s3'] = property(
    _get_s3,
    _set_s3,
    _delete_s3
  )
  class_attributes['_create_s3_put_object_params'] = _create_s3_put_object_params
  class_attributes['_is_large_message'] = _is_large_message
  class_attributes['_determine_payload'] = _determine_payload
  class_attributes['_get_s3_key'] = _get_s3_key


def _add_client_custom_attributes(base_classes, **kwargs):
  _add_custom_attributes(kwargs['class_attributes'])


def _add_topic_resource_custom_attributes(class_attributes, **kwargs):
  _add_custom_attributes(class_attributes)  

def _add_platform_endpoint_resource_custom_attributes(class_attributes, **kwargs):
  _add_custom_attributes(class_attributes)


def _publish_decorator(func):

  def _publish(*args, **kwargs):
    kwargs['MessageAttributes'], kwargs['Message'] = args[0]._determine_payload(kwargs.get('MessageAttributes', {}), kwargs['Message'], kwargs.get('MessageStructure', None))
    return func(*args, **kwargs)

  return _publish

 
class SNSExtendedClientSession(botoinator.session.DecoratedSession):


  def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, region_name=None,
                 botocore_session=None, profile_name=None):
    super().__init__(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      aws_session_token=aws_session_token,
      region_name=region_name,
      botocore_session=botocore_session,
      profile_name=profile_name
    )

    self.events.register('creating-client-class.sns', _add_client_custom_attributes)
    self.events.register('creating-resource-class.sns.Topic', _add_topic_resource_custom_attributes)
    self.events.register('creating-resource-class.sns.PlatformEndpoint', _add_platform_endpoint_resource_custom_attributes)
    self.register_client_decorator('sns', 'publish', _publish_decorator)
    self.register_resource_decorator('sns', 'Topic', 'publish', _publish_decorator)
    self.register_resource_decorator('sns', 'PlatformEndpoint', 'publish', _publish_decorator)
