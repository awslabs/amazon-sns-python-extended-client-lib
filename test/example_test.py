import boto3
import sns_extended_client
import src.sns_extended_client
from src.sns_extended_client.session import SNSExtendedClientSession


SMALL_MSG_BODY = 'small message body'
SMALL_MSG_ATTRIBUTES = {'test attr':{'DataType': 'String', 'StringValue': 'str value'}}
ATTRIBUTES_ADDED = ['large_payload_support', 'message_size_threshold', 'always_through_s3', 's3', '_create_s3_put_object_params', '_is_large_message', '_determine_payload', '_get_s3_key']

def test_default_session_is_extended_client_session():
    assert boto3.session.Session == SNSExtendedClientSession

def test_determine_payload_s3_not_used():
    sns_extended_client = SNSExtendedClientSession().client('sns')
    msg_attrs, msg_body = sns_extended_client._determine_payload('test_topic_name', SMALL_MSG_ATTRIBUTES, SMALL_MSG_BODY, None )

    assert msg_attrs == SMALL_MSG_ATTRIBUTES
    assert msg_body == SMALL_MSG_BODY


def test_always_through_s3():
    sns_extended_client = SNSExtendedClientSession().client('sns')
    sns_extended_client.always_through_s3 = True
    msg_attrs, msg_body = sns_extended_client._determine_payload('test_topic_name', SMALL_MSG_ATTRIBUTES, SMALL_MSG_BODY, None )

def test_sns_client_attributes_added():
    sns_extended_client = SNSExtendedClientSession().client('sns')
    
    assert all((hasattr(sns_extended_client, attr) for attr in ATTRIBUTES_ADDED))


def test_platform_endpoint_resource_attributes_added():
    sns_resource = SNSExtendedClientSession().resource('sns')
    topic = sns_resource.Topic('arn')

    assert all((hasattr(topic, attr) for attr in ATTRIBUTES_ADDED))


def test_topic_resource_attributes_added():
    sns_resource = SNSExtendedClientSession().resource('sns')
    topic = sns_resource.PlatformEndpoint('arn')

    assert all((hasattr(topic, attr) for attr in ATTRIBUTES_ADDED))


def demo():
    sns_extended_client = SNSExtendedClientSession().client('sns', region_name='us-east-1')

    sns_extended_client.large_payload_support = 'extended-client-bucket-store'
    sns_extended_client.always_through_s3 = True

    sns_extended_client.publish(TopicArn='arn:aws:sns:us-east-1:569434664987:extended-client-topic', Message='This message should be published to S3')

    sns_extended_client.always_through_s3 = False

    sns_extended_client.publish(TopicArn='arn:aws:sns:us-east-1:569434664987:extended-client-topic', Message='This message should be published.')

    sns_extended_client.message_size_threshold = 32
    sns_extended_client.publish(TopicArn='arn:aws:sns:us-east-1:569434664987:extended-client-topic', Message='This message should be published to S3 as it exceeds the limit of the 32 bytes')

    sns_extended_client_resource = SNSExtendedClientSession().resource('sns', region_name='us-east-1')

    topic = sns_extended_client_resource.Topic('arn:aws:sns:us-east-1:569434664987:extended-client-topic')


    topic.large_payload_support = 'extended-client-bucket-store'
    topic.always_through_s3 = True

    topic.publish(Message='This message should be published to S3', MessageAttributes={'ExtendedPayloadSize':{'DataType': 'String', 'StringValue': '347c11c4-a22c-42e4-a6a2-9b5af5b76587'}})

    topic.always_through_s3 = False

    topic.publish(Message='This message should be published.')

    topic.message_size_threshold = 32
    topic.publish(Message='This message should be published to S3 as it exceeds the limit of the 32 bytes')



if __name__ == '__main__':
    demo()

# sqs_session = SQSExtendedClientSession()
# sns_session = SNSExtendedClientSession()

# sqs_extended_client = sqs_session.client('sqs')
# sqs_extended_client.create_queue(QueueName='')
