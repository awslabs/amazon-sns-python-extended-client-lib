from boto3 import resource 
from json import dumps as jsondumps
from uuid import uuid4


DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
MESSAGE_POINTER_CLASS = 'software.amazon.payloadoffloading.PayloadS3Pointer'
LEGACY_RESERVED_ATTRIBUTE_NAME = 'SQSLargePayloadSize'
RESERVED_ATTRIBUTE_NAME = 'ExtendedPayloadSize'
S3_KEY_ATTRIBUTE_NAME = 'S3Key'
MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE = 'json'
MAX_ALLOWED_ATTRIBUTES = 10 - 1 # 10 for SQS and 1 reserved attribute

class SNSExtendedClient(object):

  def __init__(self, sns_client=None, large_payload_support=None, s3_resource=None, topic_resource=None, platformendpoint_resource=None):

    if sns_client:
      self.sns_client_to_be_extended = sns_client
    elif topic_resource:
      self.sns_client_to_be_extended = topic_resource
    elif platformendpoint_resource:
      self.sns_client_to_be_extended = platformendpoint_resource
    else:
      raise ValueError('Provide one of the the client/resources(sns client, topic resource or platform endpoint resource) to be extended')

    self.s3_resource = resource('s3') if s3_resource is None else s3_resource

    if large_payload_support is None:
      raise Exception('Provide an existing S3 bucket name to be used to store messages')
    self._large_payload_support = large_payload_support

    self._message_size_threshold = DEFAULT_MESSAGE_SIZE_THRESHOLD
    self._always_through_s3 = False
    self._use_legacy_attribute = False


  def publish(self, *args, **kwargs):

    if 'Message' not in kwargs:
      raise ValueError('Message can not be empty!')

    kwargs['MessageAttributes'], kwargs['Message'] = self._determine_payload(kwargs['Message'], kwargs.get('MessageAttributes', {}), kwargs.get('MessageStructure', None))

    return self.sns_client_to_be_extended.publish(*args, **kwargs)


  def _determine_payload(self, message_body, message_attributes, message_structure):
    encoded_body = message_body.encode()
    if self._large_payload_support and (self._always_through_s3 or self._is_large_message(message_attributes, encoded_body)):

      if message_structure == 'json':
        raise Exception('SNS extended client does not support sending JSON messages for large messages.')

      for attribute in (RESERVED_ATTRIBUTE_NAME, LEGACY_RESERVED_ATTRIBUTE_NAME):
        if attribute in message_attributes:
          raise Exception(f'Message attribute name {attribute} is reserved for use by SNS extended client.')

      attribute_name_used = LEGACY_RESERVED_ATTRIBUTE_NAME if self._use_legacy_attribute else RESERVED_ATTRIBUTE_NAME
      message_attributes[attribute_name_used] = {}
      message_attributes[attribute_name_used]['DataType'] = 'Number'
      message_attributes[attribute_name_used]['StringValue'] = str(len(encoded_body))

      self._check_message_attributes(message_attributes)
      self._check_size_of_message_attributes(message_attributes)

      s3_key = self._get_s3_key(message_attributes)

      self.s3_resource.Object(self._large_payload_support, s3_key).put(**self._create_s3_put_object_params(encoded_body))

      self._check_size_of_message_attributes(message_attributes)

      message_body = jsondumps([MESSAGE_POINTER_CLASS, {'s3BucketName': self._large_payload_support, 's3Key': s3_key}])
    
    return message_attributes, message_body


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


  def _check_message_attributes(self, message_attributes):
    num_message_attributes = len(message_attributes)
  
    if (num_message_attributes > MAX_ALLOWED_ATTRIBUTES):
      error_message = f'Number of message attributes [{num_message_attributes}] exceeds the maximum allowed for large-payload messages [{MAX_ALLOWED_ATTRIBUTES}].'
      raise Exception(error_message)


  def _check_size_of_message_attributes(self, message_attributes):
    total = 0
    for key, value in message_attributes.items():
      total = total + len(key.encode())
      if 'DataType' in value:
        total = total + len(value['DataType'].encode())
      if 'StringValue' in value:
        total = total + len(value['StringValue'].encode())
      if 'BinaryValue' in value:
        total = total + len(value['BinaryValue'])
      
      if total > self._message_size_threshold:
        raise Exception(f'Message attributes size is greater than the message size threshold {self._message_size_threshold} consider including payload in the message body')


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


  def set_always_through_s3(self):
    self._always_through_s3 = True


  def disable_always_through_s3(self):
    self._always_through_s3 = False


  def use_legacy_attribute(self):
    self._use_legacy_attribute = True


  def disable_legacy_attribute(self):
    self._use_legacy_attribute = False


  def set_message_size_threshold(self, message_size):
    assert isinstance(message_size, int) and message_size > -1
    self._message_size_threshold = min(message_size, DEFAULT_MESSAGE_SIZE_THRESHOLD) # Limit to setting to max message size threshold


  def set_large_payload_support(self, large_payload_support):
    assert isinstance(large_payload_support, str)
    self._large_payload_support = large_payload_support

  
  def add_permission(self, **kwargs):
    return self.sns_client_to_be_extended.add_permission(**kwargs)
  
  def can_paginate(self, operation_name: str):
    return self.sns_client_to_be_extended.can_paginate(operation_name)

  def check_if_phone_number_is_opted_out(self, **kwargs):
    return self.sns_client_to_be_extended.check_if_phone_number_is_opted_out(**kwargs)
  
  def close(self):
    return self.sns_client_to_be_extended.close()
  
  def confirm_subscription(self, **kwargs):
    return self.sns_client_to_be_extended.confirm_subscription(**kwargs)
  

  def create_platform_application(self, **kwargs):
    return self.sns_client_to_be_extended.create_platform_application(**kwargs)

  def create_platform_endpoint(self, **kwargs):
    return self.sns_client_to_be_extended.create_platform_endpoint(**kwargs)

  def create_sms_sandbox_phone_number(self, **kwargs):
    return self.sns_client_to_be_extended.create_sms_sandbox_phone_number(**kwargs)

  def create_topic(self, **kwargs):
    return self.sns_client_to_be_extended.create_topic(**kwargs)

  def delete_endpoint(self, **kwargs):
    return self.sns_client_to_be_extended.delete_endpoint(**kwargs)

  def delete_platform_application(self, **kwargs):
    return self.sns_client_to_be_extended.delete_platform_application(**kwargs)

  def delete_sms_sandbox_phone_number(self, **kwargs):
    return self.sns_client_to_be_extended.delete_sms_sandbox_phone_number(**kwargs)

  def delete_topic(self, **kwargs):
    return self.sns_client_to_be_extended.delete_topic(**kwargs)

  def get_data_protection_policy(self, **kwargs):
    return self.sns_client_to_be_extended.get_data_protection_policy(**kwargs)

  def get_endpoint_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.get_endpoint_attributes(**kwargs)

  def get_paginator(self, operation_name: str):
    return self.sns_client_to_be_extended.get_paginator(operation_name)

  def get_platform_application_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.get_platform_application_attributes(**kwargs)

  def get_sms_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.get_sms_attributes(**kwargs)

  def get_sms_sandbox_account_status(self):
    return self.sns_client_to_be_extended.get_sms_sandbox_account_status()

  def get_subscription_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.get_subscription_attributes(**kwargs)

  def get_topic_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.get_topic_attributes(**kwargs)

  def get_waiter(self, waiter_name: str):
    return self.sns_client_to_be_extended.get_waiter(waiter_name)

  def list_endpoints_by_platform_application(self, **kwargs):
    return self.sns_client_to_be_extended.list_endpoints_by_platform_application(**kwargs)
  

  def list_origination_numbers(self, **kwargs):
    return self.sns_client_to_be_extended.list_origination_numbers(**kwargs)
  

  def list_phone_numbers_opted_out(self, **kwargs):
    return self.sns_client_to_be_extended.list_phone_numbers_opted_out(**kwargs)


  def list_platform_applications(self, **kwargs):
    return self.sns_client_to_be_extended.list_platform_applications(**kwargs)

  def list_sms_sandbox_phone_numbers(self, **kwargs):
    return self.sns_client_to_be_extended.list_sms_sandbox_phone_numbers(**kwargs)


  def list_subscriptions(self, **kwargs):
    return self.sns_client_to_be_extended.list_subscriptions(**kwargs)

  def list_subscriptions_by_topic(self, **kwargs):
    return self.sns_client_to_be_extended.list_subscriptions_by_topic(**kwargs)


  def list_tags_for_resource(self, **kwargs):
    return self.sns_client_to_be_extended.list_tags_for_resource(**kwargs)

  def list_topics(self, **kwargs):
    return self.sns_client_to_be_extended.list_topics(**kwargs)


  def opt_in_phone_number(self, **kwargs):
    return self.sns_client_to_be_extended.opt_in_phone_number(**kwargs)

  def publish_batch(self, **kwargs):
    return self.sns_client_to_be_extended.publish_batch(**kwargs)

  def put_data_protection_policy(self, **kwargs):
    return self.sns_client_to_be_extended.put_data_protection_policy(**kwargs)


  def remove_permission(self, **kwargs):
    return self.sns_client_to_be_extended.remove_permission(**kwargs)

  def set_endpoint_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.set_endpoint_attributes(**kwargs)

  def set_platform_application_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.set_platform_application_attributes(**kwargs)


  def set_sms_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.set_sms_attributes(**kwargs)

  def set_subscription_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.set_subscription_attributes(**kwargs)

  def set_topic_attributes(self, **kwargs):
    return self.sns_client_to_be_extended.set_topic_attributes(**kwargs)


  def subscribe(self, **kwargs):
    return self.sns_client_to_be_extended.subscribe(**kwargs)

  def tag_resource(self, **kwargs):
    return self.sns_client_to_be_extended.tag_resource(**kwargs)

  def unsubscribe(self, **kwargs):
    return self.sns_client_to_be_extended.unsubscribe(**kwargs)

  def untag_resource(self, **kwargs):
    return self.sns_client_to_be_extended.untag_resource(**kwargs)

  def verify_sms_sandbox_phone_number(self, **kwargs):
    return self.sns_client_to_be_extended.verify_sms_sandbox_phone_number(**kwargs)
