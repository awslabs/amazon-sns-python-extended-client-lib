from json import dumps, loads
from uuid import uuid4

import boto3

import botocore.session

import logging
logger = logging.getLogger("sns_extended_client.client")
logger.setLevel(logging.WARNING)

from .exceptions import MissingPayloadOffloadingResource, SNSExtendedClientException

DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
MESSAGE_POINTER_CLASS = "software.amazon.payloadoffloading.PayloadS3Pointer"
LEGACY_MESSAGE_POINTER_CLASS = "com.amazon.sqs.javamessaging.MessageS3Pointer"
LEGACY_RESERVED_ATTRIBUTE_NAME = "SQSLargePayloadSize"
RESERVED_ATTRIBUTE_NAME = "ExtendedPayloadSize"
S3_KEY_ATTRIBUTE_NAME = "S3Key"
MULTIPLE_PROTOCOL_MESSAGE_STRUCTURE = "json"
MAX_ALLOWED_ATTRIBUTES = 10 - 1  # 10 for SQS and 1 reserved attribute


def _delete_large_payload_support(self):
    if hasattr(self, "__s3_bucket_name"):
        del self.__s3_bucket_name


def _get_large_payload_support(self):
    return getattr(self, "__s3_bucket_name", None)


def _set_large_payload_support(self, s3_bucket_name: str):
    if not isinstance(s3_bucket_name, str):
        raise TypeError(f"Given s3 bucket name is not of type str: {s3_bucket_name}")
    if not s3_bucket_name:
        raise ValueError("Empty string is not a valid bucket name.")
    else:
        setattr(self, "__s3_bucket_name", s3_bucket_name)


def _delete_messsage_size_threshold(self):
    setattr(self, "__message_size_threshold", DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _get_message_size_threshold(self):
    return getattr(self, "__message_size_threshold", DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _set_message_size_threshold(self, message_size_threshold: int):
    if not isinstance(message_size_threshold, int):
        raise TypeError(f"message size specified is not of type int: {message_size_threshold}")
    if not 0 <= message_size_threshold <= DEFAULT_MESSAGE_SIZE_THRESHOLD:
        raise ValueError(
            f"Valid range for message size is {0} - {DEFAULT_MESSAGE_SIZE_THRESHOLD}: message size {message_size_threshold} is out of bounds"
        )

    setattr(self, "__message_size_threshold", message_size_threshold)


def _delete_always_through_s3(self):
    setattr(self, "__always_through_s3", False)


def _get_always_through_s3(self):
    return getattr(self, "__always_through_s3", False)


def _set_always_through_s3(self, always_through_s3: bool):
    if not isinstance(always_through_s3, bool):
        raise TypeError(f"Not a Valid boolean value: {always_through_s3}")
    if always_through_s3 and not getattr(self, "large_payload_support", ""):
        raise MissingPayloadOffloadingResource()
    setattr(self, "__always_through_s3", always_through_s3)


def _delete_use_legacy_attribute(self):
    setattr(self, "__always_through_s3", False)


def _get_use_legacy_attribute(self):
    return getattr(self, "__use_legacy_attribute", False)


def _set_use_legacy_attribute(self, use_legacy_attribute: bool):
    if not isinstance(use_legacy_attribute, bool):
        raise TypeError(f"Not a Valid boolean value: {use_legacy_attribute}")

    setattr(self, "__use_legacy_attribute", use_legacy_attribute)


def _is_large_message(self, attributes: dict, encoded_body: bytes):
    total = 0
    for key, value in attributes.items():
        total = total + len(key.encode())
        if "DataType" in value:
            total = total + len(value["DataType"].encode())
        if "StringValue" in value:
            total = total + len(value["StringValue"].encode())
        if "BinaryValue" in value:
            total = total + len(value["BinaryValue"])
    total = total + len(encoded_body)
    return self.message_size_threshold < total


def _check_size_of_message_attributes(self, message_attributes: dict):
    total = 0
    for key, value in message_attributes.items():
        total = total + len(key.encode())
        if "DataType" in value:
            total = total + len(value["DataType"].encode())
        if "StringValue" in value:
            total = total + len(value["StringValue"].encode())
        if "BinaryValue" in value:
            total = total + len(value["BinaryValue"])

    if total > self.message_size_threshold:
        raise SNSExtendedClientException(
            f"Message attributes size is greater than the message size threshold: {self.message_size_threshold} consider including payload in the message body"
        )


def _check_message_attributes(self, message_attributes: dict):
    num_message_attributes = len(message_attributes)

    if num_message_attributes > MAX_ALLOWED_ATTRIBUTES:
        error_message = f"Number of message attributes [{num_message_attributes}] exceeds the maximum allowed for large-payload messages [{MAX_ALLOWED_ATTRIBUTES}]."
        raise SNSExtendedClientException(error_message)


def _get_s3_key(self, message_attributes: dict):
    if S3_KEY_ATTRIBUTE_NAME in message_attributes:
        return message_attributes[S3_KEY_ATTRIBUTE_NAME]["StringValue"]
    return str(uuid4())


def _create_reserved_message_attribute_value(self, encoded_body_size_string):
    return {"DataType": "Number", "StringValue": encoded_body_size_string}


def _make_payload(self, message_attributes: dict, message_body, message_structure: str):
    message_attributes = loads(dumps(message_attributes))
    encoded_body = message_body.encode()
    if self.large_payload_support and (
        self.always_through_s3 or self._is_large_message(message_attributes, encoded_body)
    ):
        if message_structure == "json":
            raise SNSExtendedClientException(
                "SNS extended client does not support sending JSON messages."
            )

        self._check_message_attributes(message_attributes)

        for attribute in (
            RESERVED_ATTRIBUTE_NAME,
            LEGACY_RESERVED_ATTRIBUTE_NAME,
        ):
            if attribute in message_attributes:
                raise SNSExtendedClientException(
                    f"Message attribute name {attribute} is reserved for use by SNS extended client."
                )

        message_pointer_used = (
            LEGACY_MESSAGE_POINTER_CLASS if self.use_legacy_attribute else MESSAGE_POINTER_CLASS
        )

        attribute_name_used = (
            LEGACY_RESERVED_ATTRIBUTE_NAME if self.use_legacy_attribute else RESERVED_ATTRIBUTE_NAME
        )

        message_attributes[attribute_name_used] = self._create_reserved_message_attribute_value(
            str(len(encoded_body))
        )

        self._check_size_of_message_attributes(message_attributes)

        s3_key = self._get_s3_key(message_attributes)

        self.s3_client.put_object(Bucket=self.large_payload_support, Key=s3_key, Body=encoded_body)

        message_body = dumps(
            [
                message_pointer_used,
                {"s3BucketName": self.large_payload_support, "s3Key": s3_key},
            ]
        )

    return message_attributes, message_body


def _publish_decorator(func):
    def _publish(self, **kwargs):
        if (
            "TopicArn" not in kwargs
            and "TargetArn" not in kwargs
            and not getattr(self, "arn", False)
        ):
            raise SNSExtendedClientException("Missing TopicArn: TopicArn is a required feild.")

        kwargs["MessageAttributes"], kwargs["Message"] = self._make_payload(
            kwargs.get("MessageAttributes", {}),
            kwargs["Message"],
            kwargs.get("MessageStructure",None),
        )
        return func(self, **kwargs)

    return _publish



class SNSExtendedClientSession(boto3.session.Session):

    """ 
    A session stores configuration state and allows you to create service
    clients and resources. SNSExtendedClientSession extends the functionality 
    of the boto3 Session object by using the .register event functionality. 
        
    :type aws_access_key_id: string
    :param aws_access_key_id: AWS access key ID
    :type aws_secret_access_key: string
    :param aws_secret_access_key: AWS secret access key
    :type aws_session_token: string
    :param aws_session_token: AWS temporary session token
    :type region_name: string
    :param region_name: Default region when creating new connections
    :type botocore_session: botocore.session.Session
    :param botocore_session: Use this Botocore session instead of creating
                             a new default one.
    :type profile_name: string
    :param profile_name: The name of a profile to use. If not given, then
                         the default profile is used.
            
    """


    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        region_name=None,
        botocore_session=None,
        profile_name=None,
    ):
        if botocore_session is None:
            self._session = botocore.session.get_session()
        else:
            self._session = botocore_session

        self.add_custom_user_agent()

        

        super().__init__(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            botocore_session=self._session,
            profile_name=profile_name,
        )

        # Adding Additional attributes for sns Client, Topic and PlatformEndpoint Objects
        self.events.register("creating-client-class.sns", self.add_custom_attributes)
        self.events.register(
            "creating-resource-class.sns.Topic",
            self.add_custom_attributes,
        )
        self.events.register(
            "creating-resource-class.sns.PlatformEndpoint",
            self.add_custom_attributes,
        )
    
    def add_custom_user_agent(self):
        # Attaching SNSExtendedClient Session to the HTTP headers

        user_agent_header = self.__class__.__name__

        if self._session.user_agent_extra:
            self._session.user_agent_extra += " " + user_agent_header
        else:
            self._session.user_agent_extra = user_agent_header

    def add_custom_attributes(self,class_attributes,**kwargs):

        
        class_attributes["large_payload_support"] = property(
        _get_large_payload_support,
        _set_large_payload_support,
        _delete_large_payload_support,
    )
        class_attributes["message_size_threshold"] = property(
            _get_message_size_threshold,
            _set_message_size_threshold,
            _delete_messsage_size_threshold,
        )
        class_attributes["always_through_s3"] = property(
            _get_always_through_s3,
            _set_always_through_s3,
            _delete_always_through_s3,
        )
        class_attributes["use_legacy_attribute"] = property(
            _get_use_legacy_attribute,
            _set_use_legacy_attribute,
            _delete_use_legacy_attribute,
        )
        class_attributes["s3_client"] = super().client("s3")

        class_attributes[
            "_create_reserved_message_attribute_value"
        ] = _create_reserved_message_attribute_value
        class_attributes["_is_large_message"] = _is_large_message
        class_attributes["_make_payload"] = _make_payload
        class_attributes["_get_s3_key"] = _get_s3_key

        # Adding the S3 client to the object
        
        class_attributes["_check_size_of_message_attributes"] = _check_size_of_message_attributes
        class_attributes["_check_message_attributes"] = _check_message_attributes
        class_attributes["publish"] = _publish_decorator(class_attributes["publish"])