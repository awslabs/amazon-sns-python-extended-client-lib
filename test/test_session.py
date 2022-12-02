import os
import unittest
import pytest
import uuid
import boto3

from json import JSONDecodeError, loads, dumps
from src.sns_extended_client.session import (
    DEFAULT_MESSAGE_SIZE_THRESHOLD,
    MESSAGE_POINTER_CLASS,
    LEGACY_MESSAGE_POINTER_CLASS,
    RESERVED_ATTRIBUTE_NAME,
    LEGACY_RESERVED_ATTRIBUTE_NAME,
    MAX_ALLOWED_ATTRIBUTES,
    SNSExtendedClientSession,
)
from src.sns_extended_client.exceptions import SNSExtendedClientException
from moto import mock_s3, mock_sns, mock_sqs
from unittest.mock import create_autospec


SMALL_MSG_BODY = "small message body"
SMALL_MSG_ATTRIBUTES = {"test attr": {"DataType": "String", "StringValue": "str value"}}
ATTRIBUTES_ADDED = [
    "large_payload_support",
    "message_size_threshold",
    "always_through_s3",
    "s3",
    "_create_s3_put_object_params",
    "_is_large_message",
    "_make_payload",
    "_get_s3_key",
]
LARGE_MSG_BODY = "x" * (DEFAULT_MESSAGE_SIZE_THRESHOLD + 1)


class TestSNSExtendedClient(unittest.TestCase):
    """Tests to check and verify function of the python SNS extended client"""

    test_bucket_name = "test-bucket"
    test_queue_name = "test-queue"
    test_topic_name = "test-topic"

    @classmethod
    def setUpClass(cls):
        """setup method for the test class"""
        # test environment AWS credential setup
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        cls.mock_s3 = mock_s3()
        cls.mock_sqs = mock_sqs()
        cls.mock_sns = mock_sns()
        cls.mock_s3.start()
        cls.mock_sqs.start()
        cls.mock_sns.start()

        # create sns and resource shared by all the test methods
        sns = boto3.client("sns")
        create_topic_response = sns.create_topic(Name=cls.test_topic_name)
        cls.test_topic_arn = create_topic_response.get("TopicArn")

        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=cls.test_bucket_name)

        # create sqs queue and subscribe to sns topic to test messages
        sqs = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
        cls.test_queue_url = sqs.create_queue(QueueName=cls.test_queue_name).get(
            "QueueUrl"
        )

        test_queue_arn = sqs.get_queue_attributes(
            QueueUrl=cls.test_queue_url, AttributeNames=["QueueArn"]
        )["Attributes"].get("QueueArn")

        sns.subscribe(
            TopicArn=cls.test_topic_arn, Protocol="sqs", Endpoint=test_queue_arn
        )

        # shared queue resource
        cls.test_sqs_client = sqs

        return super().setUpClass()

    def setUp(self) -> None:
        """setup function invoked before running every test method"""
        self.sns_extended_client = SNSExtendedClientSession().client("sns")
        self.sns_extended_client.large_payload_support = self.test_bucket_name
        return super().setUp()

    def tearDown(self) -> None:
        """teardown function invoked after running every test method"""
        del self.sns_extended_client
        return super().tearDown()

    @classmethod
    def tearDownClass(cls):
        """TearDown of test class"""
        cls.mock_s3.stop()
        cls.mock_sns.stop()
        cls.mock_sqs.stop()

        return super().tearDownClass()

    def test_default_session_is_extended_client_session(self):
        """Test to verify if default boto3 session is changed on import of SNSExtendedClient"""
        assert boto3.session.Session == SNSExtendedClientSession

    def test_sns_client_attributes_added(self):
        """Check the attributes of SNSExtendedSession Client are available in the client"""
        sns_extended_client = self.sns_extended_client

        assert all((hasattr(sns_extended_client, attr) for attr in ATTRIBUTES_ADDED))

    def test_platform_endpoint_resource_attributes_added(self):
        """Check the attributes of SNSExtendedSession PlatformEndpoint resource are available in the resource object"""
        sns_resource = boto3.resource("sns")
        topic = sns_resource.Topic("arn")

        assert all((hasattr(topic, attr) for attr in ATTRIBUTES_ADDED))

    def test_topic_resource_attributes_added(self):
        """Check the attributes of SNSExtendedSession Topic resource are available in the resource object"""
        sns_resource = boto3.resource("sns")
        topic = sns_resource.PlatformEndpoint(self.test_topic_arn)

        assert all((hasattr(topic, attr) for attr in ATTRIBUTES_ADDED))

    def test_publish_calls_make_payload(self):
        """Test SNSExtendedSession client publish API call invokes _make_payload method to change the message and message attributes"""
        sns_client = self.sns_extended_client

        modified_msg_attr = {"dummy": {"StringValue": "dummy", "DataType": "String"}}
        modified_msg = "dummy msg"
        modified_msg_attr_to_check = {
            "dummy": {"Type": "String", "Value": "dummy"}
        }  # to be compatible with SQS queue message format

        make_payload_mock = create_autospec(
            sns_client._make_payload, return_value=(modified_msg_attr, modified_msg)
        )

        sns_client._make_payload = make_payload_mock
        sns_client.publish(
            TopicArn=self.test_topic_arn, MessageAttributes={}, Message="test"
        )
        # verify the call to _make_payload
        make_payload_mock.assert_called_once_with({}, "test", None)

        # fetch message from sqs queue and verify the modified msg is published
        messages = self.test_sqs_client.receive_message(
            QueueUrl=self.test_queue_url, MessageAttributeNames=["All"]
        ).get("Messages")
        self.assertTrue(self.has_msg_attributes(messages, modified_msg_attr_to_check))
        self.assertTrue(self.has_msg_body(messages, modified_msg))

    def test_topic_publish_calls_make_payload(self):
        """Test SNSExtendedSession Topic Resource publish API call invokes _make_payload method to change the message and message attributes"""
        topic_resource = boto3.resource("sns").Topic(self.test_topic_arn)

        modified_msg_attr = {"dummy": {"StringValue": "dummy", "DataType": "String"}}
        modified_msg = "dummy msg"
        modified_msg_attr_to_check = {
            "dummy": {"Type": "String", "Value": "dummy"}
        }  # to be compatible with SQS queue message format

        make_payload_mock = create_autospec(
            topic_resource._make_payload, return_value=(modified_msg_attr, modified_msg)
        )

        topic_resource._make_payload = make_payload_mock
        topic_resource.publish(MessageAttributes={}, Message="test")
        # verify the call to _make_payload
        make_payload_mock.assert_called_once_with({}, "test", None)

        # fetch message from sqs queue and verify the modified msg is published
        messages = self.test_sqs_client.receive_message(
            QueueUrl=self.test_queue_url, MessageAttributeNames=["All"]
        ).get("Messages")
        self.assertTrue(self.has_msg_attributes(messages, modified_msg_attr_to_check))
        self.assertTrue(self.has_msg_body(messages, modified_msg))

    def test_platform_endpoint_publish_calls_make_payload(self):
        """Test SNSExtendedSession PlatformEndpoint resource publish API call invokes _make_payload method to change the message and message attributes"""
        platform_endpoint_resource = boto3.resource("sns").PlatformEndpoint(
            self.test_topic_arn
        )

        modified_msg_attr = {"dummy": {"StringValue": "dummy", "DataType": "String"}}
        modified_msg = "dummy msg"
        modified_msg_attr_to_check = {
            "dummy": {"Type": "String", "Value": "dummy"}
        }  # to be compatible with SQS queue message format

        make_payload_mock = create_autospec(
            platform_endpoint_resource._make_payload,
            return_value=(modified_msg_attr, modified_msg),
        )

        platform_endpoint_resource._make_payload = make_payload_mock
        platform_endpoint_resource.publish(MessageAttributes={}, Message="test")
        # verify the call to _make_payload
        make_payload_mock.assert_called_once_with({}, "test", None)

        # fetch message from sqs queue and verify the modified msg is published
        messages = self.test_sqs_client.receive_message(
            QueueUrl=self.test_queue_url, MessageAttributeNames=["All"]
        ).get("Messages")
        self.assertTrue(self.has_msg_attributes(messages, modified_msg_attr_to_check))
        self.assertTrue(self.has_msg_body(messages, modified_msg))

    def test_make_payload_small_msg(self):
        """Test publish method uses the output from the determine_payload method call small msg"""
        sns_extended_client = self.sns_extended_client

        expected_msg_attributes = SMALL_MSG_ATTRIBUTES
        expected_msg_body = SMALL_MSG_BODY

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            SMALL_MSG_ATTRIBUTES, SMALL_MSG_BODY, None
        )

        self.assertEqual(actual_msg_body, expected_msg_body)
        self.assertEqual(actual_msg_attr, expected_msg_attributes)

    def test_make_payload_large_msg(self):
        """Test publish method uses the output from the make_payload method call large msg"""
        sns_extended_client = self.sns_extended_client

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            SMALL_MSG_ATTRIBUTES, LARGE_MSG_BODY, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            SMALL_MSG_ATTRIBUTES, LARGE_MSG_BODY, RESERVED_ATTRIBUTE_NAME
        )

        self.assertEqual(expected_msg_attr, actual_msg_attr)

        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False
        self.assertEqual(len(json_body), 2)
        self.assertEqual(json_body[0], MESSAGE_POINTER_CLASS)
        self.assertEqual(json_body[1].get("s3BucketName"), self.test_bucket_name)
        self.assertTrue(self.is_valid_uuid4(json_body[1].get("s3Key")))

        self.assertEqual(LARGE_MSG_BODY, self.get_msg_from_s3(json_body))

    def test_make_payload_always_through_s3(self):
        """Test small message object is stored in S3 when always_through_s3 is set to true"""
        sns_extended_client = self.sns_extended_client

        sns_extended_client.always_through_s3 = True

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            SMALL_MSG_ATTRIBUTES, SMALL_MSG_BODY, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            SMALL_MSG_ATTRIBUTES, SMALL_MSG_BODY, RESERVED_ATTRIBUTE_NAME
        )

        self.assertEqual(expected_msg_attr, actual_msg_attr)

        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False
        self.assertEqual(len(json_body), 2)
        self.assertEqual(json_body[0], MESSAGE_POINTER_CLASS)
        self.assertEqual(json_body[1].get("s3BucketName"), self.test_bucket_name)
        self.assertTrue(self.is_valid_uuid4(json_body[1].get("s3Key")))

        self.assertEqual(SMALL_MSG_BODY, self.get_msg_from_s3(json_body))

    def test_make_payload_reduced_message_size_threshold(self):
        """Test reduced message size threshold message published to S3"""
        sns_extended_client = self.sns_extended_client

        sns_extended_client.message_size_threshold = 128
        small_msg = "x" * (sns_extended_client.message_size_threshold + 1)

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            {}, small_msg, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            {}, small_msg, RESERVED_ATTRIBUTE_NAME
        )

        self.assertEqual(expected_msg_attr, actual_msg_attr)

        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False
        self.assertEqual(len(json_body), 2)
        self.assertEqual(json_body[0], MESSAGE_POINTER_CLASS)
        self.assertEqual(json_body[1].get("s3BucketName"), self.test_bucket_name)
        self.assertTrue(self.is_valid_uuid4(json_body[1].get("s3Key")))

        self.assertEqual(small_msg, self.get_msg_from_s3(json_body))

    def test_make_payload_use_legacy_reserved_attribute(self):
        """Test extended payload messages use the Legacy Reserved Attribute"""
        sns_extended_client = self.sns_extended_client
        sns_extended_client.use_legacy_attribute = True

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            SMALL_MSG_ATTRIBUTES, LARGE_MSG_BODY, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            SMALL_MSG_ATTRIBUTES, LARGE_MSG_BODY, LEGACY_RESERVED_ATTRIBUTE_NAME
        )

        self.assertEqual(loads(actual_msg_body)[0], LEGACY_MESSAGE_POINTER_CLASS)
        self.assertEqual(expected_msg_attr, actual_msg_attr)

    def test_make_payload_use_custom_S3_key(self):
        """Test to verify custom S3 key is used to store message in S3 bucket"""
        sns_extended_client = self.sns_extended_client

        s3_key_message_attribute = {
            "S3Key": {
                "DataType": "String",
                "StringValue": "test_key",
            }
        }

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            s3_key_message_attribute, LARGE_MSG_BODY, None
        )
        expected_msg_body = dumps(
            [
                MESSAGE_POINTER_CLASS,
                {
                    "s3BucketName": sns_extended_client.large_payload_support,
                    "s3Key": "test_key",
                },
            ]
        )
        expected_msg_attr = loads(dumps(s3_key_message_attribute))
        expected_msg_attr[RESERVED_ATTRIBUTE_NAME] = {
            "DataType": "Number",
            "StringValue": str(len(LARGE_MSG_BODY.encode())),
        }

        self.assertEqual(expected_msg_attr, actual_msg_attr)
        self.assertEqual(expected_msg_body, actual_msg_body)
        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False

        self.assertEqual(LARGE_MSG_BODY, self.get_msg_from_s3(json_body))

    def test_check_message_attributes_too_many_attributes(self):
        """Test _check_message_attributes method raises Exception when invoked with many message attributes"""
        sns_extended_client = self.sns_extended_client

        many_message_attributes = {}
        message_attribute_value_string = '{"DataType": "Number", "StringValue": "100"}'
        for i in range(MAX_ALLOWED_ATTRIBUTES + 1):
            many_message_attributes[i] = loads(message_attribute_value_string)

        self.assertRaises(
            SNSExtendedClientException,
            sns_extended_client._check_message_attributes,
            many_message_attributes,
        )

    def test_check_message_attributes_size(self):
        """Test _check_size_of_message_attributes method raises Exception when invoked with big total size of message_attributes"""
        sns_extended_client = self.sns_extended_client

        large_message_attribute = {
            "large_attribute": {"DataType": "String", "StringValue": LARGE_MSG_BODY}
        }
        self.assertRaises(
            SNSExtendedClientException,
            sns_extended_client._check_size_of_message_attributes,
            large_message_attribute,
        )

    def test_is_large_message(self):
        """Test _is_large_message method which is used to determine if extended payload is to be used"""
        sns_extended_client = self.sns_extended_client

        self.assertTrue(
            sns_extended_client._is_large_message({}, LARGE_MSG_BODY)
        )  # large body --> true
        self.assertFalse(
            sns_extended_client._is_large_message({}, SMALL_MSG_BODY)
        )  # small body --> true
        self.assertTrue(
            sns_extended_client._is_large_message(
                {
                    "large_attribute": {
                        "DataType": "String",
                        "StringValue": LARGE_MSG_BODY,
                    }
                },
                SMALL_MSG_BODY,
            )
        )  # large attribute --> True

    def test_publish_json_msg_structure(self):
        """Test publish raises exception before publishing json structured message"""
        sns_extended_client = self.sns_extended_client
        sns_extended_client.always_through_s3 = True

        self.assertRaises(
            SNSExtendedClientException,
            sns_extended_client.publish,
            TopicArn="",
            Message='{"key": "value"}',
            MessageStructure="json",
        )

    def test_missing_topic_arn(self):
        """Test publish raises Exception when publishing without a topic ARN to publish"""
        sns_extended_client = self.sns_extended_client

        self.assertRaises(
            SNSExtendedClientException,
            sns_extended_client.publish,
            Message=SMALL_MSG_BODY,
            MessageAttributes=SMALL_MSG_ATTRIBUTES,
        )

    def has_msg_body(self, messages, expected_msg_body, extended_payload=False):
        """Checks for target message_body in the list of messages from SQS queue"""
        for message in messages:
            try:
                published_msg = (
                    self.get_msg_from_s3(
                        loads(loads(message.get("Body")).get("Message"))
                    )
                    if extended_payload
                    else loads(message.get("Body")).get("Message")
                )
                if published_msg == expected_msg_body:
                    return True
            except JSONDecodeError:
                # skip over non JSON body if looking extended payload objects
                continue

        return False

    def make_expected_message_attribute(
        self, base_attributes, message, reserved_attribute
    ):

        expected_msg_attr = loads(dumps(base_attributes))
        expected_msg_attr[reserved_attribute] = {
            "DataType": "Number",
            "StringValue": str(len(message.encode())),
        }
        return expected_msg_attr

    def has_msg_attributes(self, messages, message_attributes_to_compare):
        """Checks for target message_attributes in the list of messages from SQS queue"""
        for message in messages:
            body = loads(message.get("Body"))
            if message_attributes_to_compare == body.get("MessageAttributes"):
                return True

        return False

    def get_msg_from_s3(self, json_msg):
        """Fetches message from S3 object described in the json_msg of extended payload"""
        s3_client = boto3.client("s3")
        s3_object = s3_client.get_object(
            Bucket=json_msg[1].get("s3BucketName"), Key=json_msg[1].get("s3Key")
        )
        msg = s3_object.get("Body").read().decode()
        return msg

    def is_valid_uuid4(self, val):
        """Checks for valid uuid's"""
        try:
            uuid.UUID(str(val), version=4)
            return True
        except ValueError:
            return False


if __name__ == "__main__":
    unittest.main()
