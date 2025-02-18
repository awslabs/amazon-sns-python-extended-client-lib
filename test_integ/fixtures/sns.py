import boto3
from sns_extended_client import SNSExtendedClientSession
import pytest
import random

@pytest.fixture()
def sns_extended_client(session):
    sns_client = session.client("sns",region_name='us-east-1')
    sns_client.large_payload_support = f'integration-sns-extended-lib-test-bucket-{random.randint(0, 10000)}'
    return sns_client

@pytest.fixture()
def sqs_client(session):
    return session.client("sqs")

@pytest.fixture()
def queue_name():
    return f"IntegrationTestQueue{random.randint(0,10000)}"

@pytest.fixture()
def topic_name():
    return f"IntegrationTestTopic{random.randint(0,10000)}"

@pytest.fixture()
def queue(sqs_client, queue_name):
    queue_object =  sqs_client.create_queue(QueueName=queue_name)

    yield queue_object

    sqs_client.purge_queue(
        QueueUrl=queue_object['QueueUrl']
    )

    sqs_client.delete_queue(
        QueueUrl=queue_object['QueueUrl']
    )    

@pytest.fixture()
def topic(sns_extended_client, topic_name):
    topic_arn =  sns_extended_client.create_topic(Name=topic_name).get("TopicArn")

    yield topic_arn

    sns_extended_client.delete_topic(
        TopicArn=topic_arn
    )  

@pytest.fixture()
def sns_extended_client_with_s3(sns_extended_client):

    client_sns = sns_extended_client

    client_sns.s3_client.create_bucket(
        Bucket=client_sns.large_payload_support
    )

    yield client_sns

    client_sns.s3_client.delete_bucket(
        Bucket=client_sns.large_payload_support,
    )
