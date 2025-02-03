import os
import unittest
import uuid
from json import JSONDecodeError, dumps, loads
from unittest.mock import create_autospec

import boto3
from moto import mock_s3, mock_sns, mock_sqs

from sns_extended_client.exceptions import SNSExtendedClientException
from sns_extended_client.session import DEFAULT_MESSAGE_SIZE_THRESHOLD, LEGACY_MESSAGE_POINTER_CLASS, LEGACY_RESERVED_ATTRIBUTE_NAME, MAX_ALLOWED_ATTRIBUTES, MESSAGE_POINTER_CLASS, RESERVED_ATTRIBUTE_NAME, SNSExtendedClientSession

class TestSNSExtendedClient(unittest.TestCase):
    """Tests to check and verify function of the python SNS extended client"""

    @classmethod
    def setUpClass(cls):
        """setup method for the test class"""
        # test environment AWS credential setup
        # amazonq-ignore-next-line
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        cls.test_bucket_name = "test-bucket"
        cls.test_queue_name = "test-queue"
        cls.test_topic_name = "test-topic"
        # cls.mock_s3 = mock_s3()
        cls.mock_sqs = mock_sqs()
        cls.mock_sns = mock_sns()
        # cls.mock_s3.start()
        cls.mock_sqs.start()
        cls.mock_sns.start()

        # create sns and resource shared by all the test methods
        cls.sns = boto3.client("sns",region_name=os.environ["AWS_DEFAULT_REGION"])
        create_topic_response = cls.sns.create_topic(Name=TestSNSExtendedClient.test_topic_name)
        cls.test_topic_arn = create_topic_response.get("TopicArn")

        # create sqs queue and subscribe to sns topic to test messages
        cls.sqs = boto3.client("sqs", region_name=os.environ["AWS_DEFAULT_REGION"])
        cls.test_queue_url = cls.sqs.create_queue(QueueName=TestSNSExtendedClient.test_queue_name).get("QueueUrl")

        test_queue_arn = cls.sqs.get_queue_attributes(
            QueueUrl=cls.test_queue_url, AttributeNames=["QueueArn"]
        )["Attributes"].get("QueueArn")

        cls.sns.subscribe(
            TopicArn=cls.test_topic_arn,
            Protocol="sqs",
            Endpoint=test_queue_arn,
        )

        # shared queue resource
        cls.test_sqs_client = cls.sqs

        return super().setUpClass()
    
    def initialize_extended_client_setup(self):
        """Helper function to initialize extended client session"""
        self.sns_extended_client = SNSExtendedClientSession().client("sns")
        self.sns_extended_client.large_payload_support = TestSNSExtendedClient.test_bucket_name

    def initialize_test_properties(self):
        """Setting up test properties for an object of the SQSExtendedClientSession"""
        self.small_message_body = "small message body"
        self.small_message_attribute = {"SMALL_MESSAGE_ATTRIBUTE": {"DataType": "String", "StringValue": self.small_message_body}}
        self.ATTRIBUTES_ADDED = [
            "large_payload_support",
            "message_size_threshold",
            "always_through_s3",
            "s3_client",
            "_is_large_message",
            "_make_payload",
            "_get_s3_key",
        ]
        self.s3_resource = boto3.resource("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
        self.s3_resource.create_bucket(Bucket=TestSNSExtendedClient.test_bucket_name)
        self.large_msg_body = "x" * (DEFAULT_MESSAGE_SIZE_THRESHOLD + 1)
        self.s3_key  = 'cc562f4d-c6f3-4b0c-bf29-007a902c391f'

        # Developing a message attribute with a S3 Key
        self.message_attributes_with_s3_key = self.small_message_attribute.copy()
        self.message_attributes_with_s3_key['S3Key'] = {
                'StringValue': self.s3_key,
                'DataType': 'String'
            }
        
        # Creating a copy of the message attributes since a new key, LEGACY_RESERVED_ATTRIBUTE_NAME or
        # RESERVED_ATTRIBUTE_NAME, will be added during the `send_message` call. This is useful 
        # for all the tests testing receive_message. 
        self.unmodified_message_attribute = self.message_attributes_with_s3_key.copy()

    @mock_s3
    def setUp(self) -> None:
        """setup function invoked before running every test method"""
        self.initialize_extended_client_setup()
        self.initialize_test_properties()
        # return super().setUp()

    def tearDown(self) -> None:
        """teardown function invoked after running every test method"""
        del self.sns_extended_client
        return super().tearDown()

    @classmethod
    def tearDownClass(cls):
        """TearDown of test class"""
        cls.mock_sns.stop()
        cls.mock_sqs.stop()

        return super().tearDownClass()

    def test_default_session_is_extended_client_session(self):
        """Test to verify if default boto3 session is changed on import of SNSExtendedClient"""
        assert boto3.session.Session == SNSExtendedClientSession

    def test_sns_client_attributes_added(self):
        """Check the attributes of SNSExtendedSession Client are available in the client"""
        sns_extended_client = self.sns_extended_client

        for attr in self.ATTRIBUTES_ADDED:
            print(attr)
            assert hasattr(sns_extended_client, attr)

        assert all((hasattr(sns_extended_client, attr) for attr in self.ATTRIBUTES_ADDED))

    def test_platform_endpoint_resource_attributes_added(self):
        """Check the attributes of SNSExtendedSession PlatformEndpoint resource are available in the resource object"""
        sns_resource = SNSExtendedClientSession().resource("sns")
        topic = sns_resource.Topic("arn")

        assert all((hasattr(topic, attr) for attr in self.ATTRIBUTES_ADDED))

    def test_topic_resource_attributes_added(self):
        """Check the attributes of SNSExtendedSession Topic resource are available in the resource object"""
        sns_resource = SNSExtendedClientSession().resource("sns")
        topic = sns_resource.PlatformEndpoint(self.test_topic_arn)

        assert all((hasattr(topic, attr) for attr in self.ATTRIBUTES_ADDED))

    def test_publish_calls_make_payload(self):
        """Test SNSExtendedSession client publish API call invokes _make_payload method to change the message and message attributes"""
        sns_client = self.sns_extended_client

        modified_msg_attr = {"dummy": {"StringValue": "dummy", "DataType": "String"}}
        modified_msg = "dummy msg"
        modified_msg_attr_to_check = {
            "dummy": {"Type": "String", "Value": "dummy"}
        }  # to be compatible with SQS queue message format

        make_payload_mock = create_autospec(
            sns_client._make_payload,
            return_value=(modified_msg_attr, modified_msg),
        )

        sns_client._make_payload = make_payload_mock
        sns_client.publish(TopicArn=self.test_topic_arn, MessageAttributes={}, Message="test")
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
        topic_resource = SNSExtendedClientSession().resource("sns").Topic(self.test_topic_arn)

        modified_msg_attr = {"dummy": {"StringValue": "dummy", "DataType": "String"}}
        modified_msg = "dummy msg"
        modified_msg_attr_to_check = {
            "dummy": {"Type": "String", "Value": "dummy"}
        }  # to be compatible with SQS queue message format

        make_payload_mock = create_autospec(
            topic_resource._make_payload,
            return_value=(modified_msg_attr, modified_msg),
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
        platform_endpoint_resource = SNSExtendedClientSession().resource("sns").PlatformEndpoint(self.test_topic_arn)

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

        expected_msg_attributes = self.small_message_attribute
        expected_msg_body = self.small_message_body

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            self.small_message_attribute, self.small_message_body, None
        )

        self.assertEqual(actual_msg_body, expected_msg_body)
        self.assertEqual(actual_msg_attr, expected_msg_attributes)

    def test_make_payload_large_msg(self):
        """Test publish method uses the output from the make_payload method call large msg"""
        sns_extended_client = self.sns_extended_client

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            self.small_message_attribute, self.large_msg_body, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            self.small_message_attribute, self.large_msg_body, RESERVED_ATTRIBUTE_NAME
        )

        self.assertEqual(expected_msg_attr, actual_msg_attr)

        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False
        self.assertEqual(len(json_body), 2)
        self.assertEqual(json_body[0], MESSAGE_POINTER_CLASS)
        self.assertEqual(json_body[1].get("s3BucketName"), TestSNSExtendedClient.test_bucket_name)
        self.assertTrue(self.is_valid_uuid4(json_body[1].get("s3Key")))

        self.assertEqual(self.large_msg_body, self.get_msg_from_s3(json_body))

    def test_make_payload_always_through_s3(self):
        """Test small message object is stored in S3 when always_through_s3 is set to true"""
        sns_extended_client = self.sns_extended_client

        sns_extended_client.always_through_s3 = True

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            self.small_message_attribute, self.small_message_body, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            self.small_message_attribute, self.small_message_body, RESERVED_ATTRIBUTE_NAME
        )

        self.assertEqual(expected_msg_attr, actual_msg_attr)

        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False
        self.assertEqual(len(json_body), 2)
        self.assertEqual(json_body[0], MESSAGE_POINTER_CLASS)
        self.assertEqual(json_body[1].get("s3BucketName"), TestSNSExtendedClient.test_bucket_name)
        self.assertTrue(self.is_valid_uuid4(json_body[1].get("s3Key")))

        self.assertEqual(self.small_message_body, self.get_msg_from_s3(json_body))

    def test_make_payload_reduced_message_size_threshold(self):
        """Test reduced message size threshold message published to S3"""
        sns_extended_client = self.sns_extended_client

        sns_extended_client.message_size_threshold = 128
        small_msg = "x" * (sns_extended_client.message_size_threshold + 1)

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload({}, small_msg, None)

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
        self.assertEqual(json_body[1].get("s3BucketName"), TestSNSExtendedClient.test_bucket_name)
        self.assertTrue(self.is_valid_uuid4(json_body[1].get("s3Key")))

        self.assertEqual(small_msg, self.get_msg_from_s3(json_body))

    def test_make_payload_use_legacy_reserved_attribute(self):
        """Test extended payload messages use the Legacy Reserved Attribute"""
        sns_extended_client = self.sns_extended_client
        sns_extended_client.use_legacy_attribute = True

        actual_msg_attr, actual_msg_body = sns_extended_client._make_payload(
            self.small_message_attribute, self.large_msg_body, None
        )

        expected_msg_attr = self.make_expected_message_attribute(
            self.small_message_attribute,
            self.large_msg_body,
            LEGACY_RESERVED_ATTRIBUTE_NAME,
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
            s3_key_message_attribute, self.large_msg_body, None
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
            "StringValue": str(len(self.large_msg_body.encode())),
        }

        self.assertEqual(expected_msg_attr, actual_msg_attr)
        self.assertEqual(expected_msg_body, actual_msg_body)
        try:
            json_body = loads(actual_msg_body)
        except JSONDecodeError:
            assert False

        self.assertEqual(self.large_msg_body, self.get_msg_from_s3(json_body))

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
            "large_attribute": {
                "DataType": "String",
                "StringValue": self.large_msg_body,
            }
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
            sns_extended_client._is_large_message({}, self.large_msg_body)
        )  # large body --> true
        self.assertFalse(
            sns_extended_client._is_large_message({}, self.small_message_body)
        )  # small body --> true
        self.assertTrue(
            sns_extended_client._is_large_message(
                {
                    "large_attribute": {
                        "DataType": "String",
                        "StringValue": self.large_msg_body,
                    }
                },
                self.small_message_body,
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
            Message=self.small_message_body,
            MessageAttributes=self.small_message_attribute,
        )

    def has_msg_body(self, messages, expected_msg_body, extended_payload=False):
        """Checks for target message_body in the list of messages from SQS queue"""
        for message in messages:
            try:
                published_msg = (
                    self.get_msg_from_s3(loads(loads(message.get("Body")).get("Message")))
                    if extended_payload
                    else loads(message.get("Body")).get("Message")
                )
                if published_msg == expected_msg_body:
                    return True
            except JSONDecodeError:
                # skip over non JSON body if looking extended payload objects
                continue

        return False

    def make_expected_message_attribute(self, base_attributes, message, reserved_attribute):
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
        msg = self.s3_resource.Object(json_msg[1].get("s3BucketName"), json_msg[1].get("s3Key")).get()["Body"].read().decode("utf-8")
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
