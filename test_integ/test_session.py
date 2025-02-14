from sns_extended_client import SNSExtendedClientSession
from botocore.exceptions import ClientError
from .fixtures.session import *
from .fixtures.sns import *
from .fixtures.objects import *
from . import logger
from json import loads
import copy
import logging


def initialize_extended_client_attributes_through_s3(sns_extended_client):
    """

    Acts as a helper for adding attributes to the extended client
    which are required for sending in payloads to S3 buckets.

    sns_extended_client: The SNS Extended Client

    """

    sns_extended_client.always_through_s3 = True
    
    return

def create_allow_sns_to_write_to_sqs_policy_json(topicarn, queuearn):
    """
    Creates a policy document which allows SNS to write to SQS

    topicarn: The ARN of the SNS topic
    queuearn: The ARN of the SQS queue

    """

    logger.info("Creating policy document to allow SNS to write to SQS")
    policy_document = """{{
        "Version":"2012-10-17",
        "Statement":[
            {{
            "Sid":"MyPolicy",
            "Effect":"Allow",
            "Principal" : {{"AWS" : "*"}},
            "Action":"SQS:SendMessage",
            "Resource": "{}",
            "Condition":{{
                "ArnEquals":{{
                "aws:SourceArn": "{}"
                }}
            }}
            }}
        ]
        }}""".format(queuearn, topicarn)

    return policy_document

def publish_message_helper(sns_extended_client, topic_arn, message_body, message_attributes = None,message_group_id = None, message_deduplication_id = None, **kwargs):
    """

    Acts as a helper for publishing a message via the SNS Extended Client.

    sns_extended_client: The SNS Extended Client
    topic_arn: The ARN associated with the SNS Topic
    message_body: The message body
    message_attributes: The message attributes

    """

    publish_message_kwargs = {
        'TopicArn': topic_arn,
        'Message': message_body
    }

    if message_attributes:
        publish_message_kwargs['MessageAttributes'] = message_attributes
    
    if message_group_id:
        publish_message_kwargs['MessageGroupId'] = message_group_id
        publish_message_kwargs['MessageDeduplicationId'] = message_deduplication_id

    logger.info("Publishing the message via the SNS Extended Client")

    response = sns_extended_client.publish(**publish_message_kwargs)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    return

def is_s3_bucket_empty(sns_extended_client):
    """

    Responsible for checking if the S3 bucket created consists 
    of objects at the time of calling the function.

    sns_extended_client: The SNS Extended Client
    
    """
    response = sns_extended_client.s3_client.list_objects_v2(
        Bucket=sns_extended_client.large_payload_support
    )

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return "Contents" not in response

def retrive_message_from_s3(sns_extended_client,s3Key):
    """

    Responsible for retrieving a message from the S3 bucket.

    sns_extended_client: The SNS Extended Client
    s3Key: The S3 Key

    """
    logger.info("Retrieving the message from the S3 bucket")

    target_s3_msg_obj = sns_extended_client.s3_client.get_object(Bucket=sns_extended_client.large_payload_support, Key=s3Key)

    return target_s3_msg_obj['Body'].read().decode()

def s3_bucket_exist(sns_extended_client):
    """

    Responsible for checking if the S3 bucket created exists
    at the time of calling the function.

    sns_extended_client: The SNS Extended Client

    """
    logger.info("Checking if the S3 bucket exists")

    try:
        sns_extended_client.s3_client.head_bucket(Bucket=sns_extended_client.large_payload_support)
        return True 
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False 
        raise  

def receive_message_helper(sqs_client, queue_url):
    """

    Acts as a helper for receiving a message via the SQS Client.

    sqs_client: The SQS Client
    queue_url: The URL associated with the SQS Queue

    """

    logger.info("Receiving the message via the SQS Client")

    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
    )

    assert 'Messages' in response.keys()

    return response

def extract_message_body_from_response(sns_extended_client,receive_message_response):
    """

    Responsible for extracting the message body from the response received via the SQS Client.

    receive_message_response: The response received from the SQS Client

    """

    receive_message_response = loads(receive_message_response)
    target_s3_msg_obj = sns_extended_client.s3_client.get_object(Bucket=receive_message_response[1].get("s3BucketName"), Key=receive_message_response[1].get("s3Key"))

    return target_s3_msg_obj['Body'].read().decode()

def check_receive_message_response(sns_extended_client,receive_message_response, message_body, message_attributes):
    """

    Responsible for checking the message received via the SQS Client.

    receive_message_response: The response received from the SQS Client
    message_body: The message body
    message_attributes: The message attributes

    """
    response_msg_body = extract_message_body_from_response(sns_extended_client,receive_message_response['Messages'][0]['Body'])
    assert response_msg_body == message_body

def delete_message_helper(sqs_client,queue_url, receipet_handle):
    """
    
    Acts as a helper for deleting a message via the SQS Client.

    sns_extended_client: The SNS Extended Client
    queue_url: The URL associated with the SQS Queue
    receipet_handle: The receipt handle associated with the message

    """

    logger.info("Deleting the message via the SQS Client")

    response = sqs_client.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipet_handle
    )

    print("Response of delete msg : ")

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

def delete_object_from_s3_helper(sns_extended_client, s3Key):
    """

    Acts as a helper for deleting an object from the S3 bucket.

    sns_extended_client: The SNS Extended Client
    receive_message_response: The receive message from SQS client

    """

    logger.info("Deleting the object from the S3 bucket")

    response = sns_extended_client.s3_client.delete_object(
        Bucket=sns_extended_client.large_payload_support,
        Key=s3Key
    )

    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

def test_publish_receive_small_msg_through_s3(sns_extended_client_with_s3,sqs_client,queue,topic,small_message_body):
    """
    Responsible for replicating a workflow where SQS queue subscribe to sns_extended_client's topic. 
    sns_extended_client publish a message to that topic with the attribute 'always_through_s3' set to true
    which will store in S3 and reference of that object is received by SQS queue by calling the helper functions.

    sns_extedned_client_with_s3 : The SNS Extended Client with S3 Bucket
    sqs_client: The SQS Client
    queue: The SQS Queue
    topic: The SNS Topic
    small_message_body: The Message 
    """ 

    logger.info("Initializing execution of test_publish_receive_small_msg_through_s3")

    initialize_extended_client_attributes_through_s3(sns_extended_client_with_s3)

    queue_url = queue["QueueUrl"]
    sqs_queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"].get("QueueArn")

    kwargs = {
        'sns_extended_client': sns_extended_client_with_s3,
        'topic_arn': topic,
        'queue_url': queue_url,
        'message_body': small_message_body,
    }

    # Adding policy to SQS queue such that SNS topic can publish msg to SQS queue
    policy_json = create_allow_sns_to_write_to_sqs_policy_json(topic, sqs_queue_arn)
    response = sqs_client.set_queue_attributes(
        QueueUrl = queue_url,
        Attributes = {
            'Policy' : policy_json
        }
    )

    # Subscribe SQS queue to SNS topic
    sns_extended_client_with_s3.subscribe(TopicArn=topic,Protocol="sqs",Endpoint=sqs_queue_arn,Attributes={"RawMessageDelivery":"true"})

    publish_message_helper(**kwargs)

    # Message should store into S3 bucket after publishing to the SNS topic
    assert not is_s3_bucket_empty(sns_extended_client_with_s3)

    receive_message_response = receive_message_helper(sqs_client,queue_url)

    # Check the message format - The stored Message in S3 has {"s3BucketName": "<Bucket Name>", "s3Key": "Key Value"}
    json_receive_message_body = loads(receive_message_response['Messages'][0]['Body'])[1]
    message_stored_in_s3_attributes = ['s3BucketName','s3Key']
    for key in json_receive_message_body.keys():
        assert key in message_stored_in_s3_attributes

    # Retrieve the message from s3 Bucket and check value 
    assert retrive_message_from_s3(sns_extended_client_with_s3,json_receive_message_body['s3Key']) == small_message_body
    
    # Delete message from SQS queue
    receipet_handle = receive_message_response['Messages'][0]['ReceiptHandle']
    delete_message_helper(sqs_client, queue_url, receipet_handle)

    # Delete message from S3 bucket
    delete_object_from_s3_helper(sns_extended_client_with_s3, json_receive_message_body['s3Key'])

    # The S3 bucket should be empty
    assert is_s3_bucket_empty(sns_extended_client_with_s3)
    logger.info("Completed execution of test_publish_receive_small_msg_through_s3")

    return

def test_publish_receive_small_msg_not_through_s3(sns_extended_client, sqs_client, queue, topic, small_message_body):
    """
    Responsible for replicating a workflow where SQS queue subscribe to sns_extended_client's topic.
    sns_extended_client publish a message to that topic with the attribute 'always_through_s3' set to false
    which will store in S3 and reference of that object is received by SQS queue by calling the helper functions.

    sns_extended_client: The SNS Extended Client
    sqs_client: The SQS Client
    queue: The SQS Queue
    topic: The SNS Topic
    small_message_body: The Message
    """

    logger.info("Initializing execution of test_publish_receive_small_msg_not_through_s3")

    queue_url = queue["QueueUrl"]
    sqs_queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"].get("QueueArn")

    kwargs = {
        'sns_extended_client': sns_extended_client,
        'topic_arn': topic,
        'queue_url': queue_url,
        'message_body': small_message_body,
    }

    # Adding policy to SQS queue such that SNS topic can publish msg to SQS queue
    policy_json = create_allow_sns_to_write_to_sqs_policy_json(topic, sqs_queue_arn)
    response = sqs_client.set_queue_attributes(
        QueueUrl = queue_url,
        Attributes = {
            'Policy' : policy_json
        }
    )

    # Subscribe SQS queue to SNS topic
    sns_extended_client.subscribe(TopicArn=topic,Protocol="sqs",Endpoint=sqs_queue_arn,Attributes={"RawMessageDelivery":"true"})

    publish_message_helper(**kwargs)

    receive_message_response = receive_message_helper(sqs_client,queue_url)

    # The body of response should have same message body that was being sent to topic by SNS
    assert receive_message_response['Messages'][0]['Body'] == small_message_body

    # Delete message from SQS queue
    receipet_handle = receive_message_response['Messages'][0]['ReceiptHandle']
    delete_message_helper(sqs_client, queue_url, receipet_handle)

    logger.info("Completed execution of test_publish_receive_small_msg_not_through_s3")

    return
    
def test_publish_receive_large_msg_which_passes_threshold_through_s3(sns_extended_client_with_s3,sqs_client,queue,topic,large_message_body):
    """
    Responsible for replicating a workflow where SQS queue subscribe to sns_extended_client's topic. 
    sns_extended_client publish a message to that topic which exceeds the default threshold
    which will store in S3 and reference of that object is received by SQS queue by calling the helper functions.

    sns_extedned_client_with_s3 : The SNS Extended Client with S3 Bucket
    sqs_client: The SQS Client
    queue: The SQS Queue
    topic: The SNS Topic
    large_message_body: The Message 
    """ 

    logger.info("Initializing execution of test_publish_receive_large_msg_which_passes_threshold_through_s3")

    queue_url = queue["QueueUrl"]
    sqs_queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"].get("QueueArn")

    kwargs = {
        'sns_extended_client': sns_extended_client_with_s3,
        'topic_arn': topic,
        'queue_url': queue_url,
        'message_body': large_message_body,
    }

    # Adding policy to SQS queue such that SNS topic can publish msg to SQS queue
    policy_json = create_allow_sns_to_write_to_sqs_policy_json(topic, sqs_queue_arn)
    response = sqs_client.set_queue_attributes(
        QueueUrl = queue_url,
        Attributes = {
            'Policy' : policy_json
        }
    )

    # Subscribe SQS queue to SNS topic
    sns_extended_client_with_s3.subscribe(TopicArn=topic,Protocol="sqs",Endpoint=sqs_queue_arn,Attributes={"RawMessageDelivery":"true"})

    publish_message_helper(**kwargs)

    # Message should store into S3 bucket after publishing to the SNS topic
    assert not is_s3_bucket_empty(sns_extended_client_with_s3)

    receive_message_response = receive_message_helper(sqs_client,queue_url)

    # Check the message format - The stored Message in S3 has {"s3BucketName": "<Bucket Name>", "s3Key": "Key Value"}
    json_receive_message_body = loads(receive_message_response['Messages'][0]['Body'])[1]
    message_stored_in_s3_attributes = ['s3BucketName','s3Key']
    for key in json_receive_message_body.keys():
        assert key in message_stored_in_s3_attributes

    # Retrieve the message from s3 Bucket and check value 
    assert retrive_message_from_s3(sns_extended_client_with_s3,json_receive_message_body['s3Key']) == large_message_body

    # Delete message from SQS queue
    receipet_handle = receive_message_response['Messages'][0]['ReceiptHandle']
    delete_message_helper(sqs_client, queue_url, receipet_handle)

    # Delete message from S3 bucket
    delete_object_from_s3_helper(sns_extended_client_with_s3, json_receive_message_body['s3Key'])

    # The S3 bucket should be empty
    assert is_s3_bucket_empty(sns_extended_client_with_s3)
    logger.info("Completed execution of test_publish_receive_large_msg_which_passes_threshold_through_s3")

    return

def test_publish_receive_msg_with_custom_s3_key(sns_extended_client_with_s3, sqs_client, queue, topic, small_message_body,custom_s3_key_attribute):
    """
    Responsible for replicating a workflow where SQS queue subscribe to sns_extended_client's topic.
    sns_extended_client publish a message to that topic with the custom attribute to store message in s3
    and reference of that object is received by SQS queue by calling the helper functions.

    sns_extended_client_with_s3: The SNS Extended Client with Existed S3 bucket
    sqs_client: The SQS Client
    queue: The SQS Queue
    topic: The SNS Topic
    small_message_body: The Message
    custom_s3_key_attribute: Attribute to set custom Key of message
    """    

    logger.info("Initializing execution of test_publish_receive_small_msg_through_s3")

    initialize_extended_client_attributes_through_s3(sns_extended_client_with_s3)

    queue_url = queue["QueueUrl"]
    sqs_queue_arn = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"].get("QueueArn")

    kwargs = {
        'sns_extended_client': sns_extended_client_with_s3,
        'topic_arn': topic,
        'queue_url': queue_url,
        'message_body': small_message_body,
        'message_attributes': custom_s3_key_attribute
    }

    # Adding policy to SQS queue such that SNS topic can publish msg to SQS queue
    policy_json = create_allow_sns_to_write_to_sqs_policy_json(topic, sqs_queue_arn)
    response = sqs_client.set_queue_attributes(
        QueueUrl = queue_url,
        Attributes = {
            'Policy' : policy_json
        }
    )

    # Subscribe SQS queue to SNS topic
    sns_extended_client_with_s3.subscribe(TopicArn=topic,Protocol="sqs",Endpoint=sqs_queue_arn,Attributes={"RawMessageDelivery":"true"})

    publish_message_helper(**kwargs)

    # Message should store into S3 bucket after publishing to the SNS topic
    assert not is_s3_bucket_empty(sns_extended_client_with_s3)

    receive_message_response = receive_message_helper(sqs_client,queue_url)

    # Check the message format - The stored Message in S3 has {"s3BucketName": "<Bucket Name>", "s3Key": "Key Value"}
    json_receive_message_body = loads(receive_message_response['Messages'][0]['Body'])[1]
    message_stored_in_s3_attributes = ['s3BucketName','s3Key']
    for key in json_receive_message_body.keys():
        assert key in message_stored_in_s3_attributes
    
    # Stored message key should be same as custom_s3_key_attribute datavalue
    assert custom_s3_key_attribute['S3Key']['StringValue'] == json_receive_message_body['s3Key']

    # Retrieve the message from s3 Bucket and check value 
    assert retrive_message_from_s3(sns_extended_client_with_s3,json_receive_message_body['s3Key']) == small_message_body

    # Delete message from SQS queue
    receipet_handle = receive_message_response['Messages'][0]['ReceiptHandle']
    delete_message_helper(sqs_client, queue_url, receipet_handle)

    # Delete message from S3 bucket
    delete_object_from_s3_helper(sns_extended_client_with_s3, json_receive_message_body['s3Key'])

    # The S3 bucket should be empty
    assert is_s3_bucket_empty(sns_extended_client_with_s3)
    logger.info("Completed execution of test_publish_receive_small_msg_through_s3")

    return

def test_session(session):
    assert boto3.session.Session == SNSExtendedClientSession


