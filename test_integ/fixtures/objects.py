import boto3
import pytest
import uuid

@pytest.fixture
def default_message_size_threshold():
    return 262144

@pytest.fixture
def small_message_body():
    return "small message body"


@pytest.fixture
def small_message_attribute(small_message_body):
    return {
        'Small_Message_Attribute': {
            'StringValue': small_message_body,
            'DataType': 'String'
        }
    }

@pytest.fixture
def custom_s3_key_attribute():
    return {
        'S3Key': {
            'StringValue': str(uuid.uuid4()),
            'DataType': 'String'
        }
    }


@pytest.fixture
def large_message_body(small_message_body, default_message_size_threshold):
    return "x" * ( default_message_size_threshold + 1 )

@pytest.fixture
def large_message_attribute(large_message_body):
    return  {
        'Large_Message_Attribute': {
            'StringValue': 'Test',
            'DataType': 'String'
        }
    }