import boto3
from src.sns_extended_client import SNSExtendedClientSession
from json import loads

s3_extended_payload_bucket = "extended-client-bucket-store"  # S3 bucket with the given bucket name is a resource which is created and accessible with the given AWS credentials
TOPIC_NAME = "---TOPIC-NAME---"
QUEUE_NAME = "---QUEUE-NAME---"


def get_msg_from_s3(body):
    """Handy Helper to fetch message from S3"""
    json_msg = loads(body)
    s3_client = boto3.client("s3")
    s3_object = s3_client.get_object(
        Bucket=json_msg[1].get("s3BucketName"), Key=json_msg[1].get("s3Key")
    )
    msg = s3_object.get("Body").read().decode()
    return msg


def fetch_and_print_from_sqs(sqs, queue_url):
    message = sqs.receive_message(
        QueueUrl=queue_url, MessageAttributeNames=["All"], MaxNumberOfMessages=1
    ).get("Messages")[0]
    message_body = loads(message.get("Body")).get("Message")
    print("Published Message: {}".format(message_body))
    print("Message Stored in S3 Bucket is: {}\n".format(get_msg_from_s3(message_body)))


sns_extended_client = boto3.client("sns", region_name="us-east-1")
create_topic_response = sns_extended_client.create_topic(Name=TOPIC_NAME)
demo_topic_arn = create_topic_response.get("TopicArn")

# create and subscribe an sqs queue to the sns client
sqs = boto3.client("sqs")
demo_queue_url = sqs.create_queue(QueueName=QUEUE_NAME).get("QueueUrl")
demo_queue_arn = sqs.get_queue_attributes(
    QueueUrl=demo_queue_url, AttributeNames=["QueueArn"]
)["Attributes"].get("QueueArn")

sns_extended_client.subscribe(
    TopicArn=demo_topic_arn, Protocol="sqs", Endpoint=demo_queue_arn
)


sns_extended_client.large_payload_support = s3_extended_payload_bucket
sns_extended_client.always_through_s3 = True
sns_extended_client.publish(
    TopicArn=demo_topic_arn, Message="This message should be published to S3"
)
print("\n\nPublished using SNS extended client:")
fetch_and_print_from_sqs(sqs, demo_queue_url)  # Prints message stored in s3

print("\nUsing decreased message size threshold:")

sns_extended_client.always_through_s3 = False
sns_extended_client.message_size_threshold = 32
sns_extended_client.publish(
    TopicArn=demo_topic_arn,
    Message="This message should be published to S3 as it exceeds the limit of the 32 bytes",
)

fetch_and_print_from_sqs(sqs, demo_queue_url)  # Prints message stored in s3


# publish message using the SNS.Topic resource
sns_extended_client_resource = SNSExtendedClientSession().resource(
    "sns", region_name="us-east-1"
)

topic = sns_extended_client_resource.Topic(demo_topic_arn)
topic.large_payload_support = s3_extended_payload_bucket
topic.always_through_s3 = True
# Can Set custom S3 Keys to be used to store objects in S3
topic.publish(
    Message="This message should be published to S3 using the topic resource",
    MessageAttributes={
        "S3Key": {
            "DataType": "String",
            "StringValue": "347c11c4-a22c-42e4-a6a2-9b5af5b76587",
        }
    },
)
print("\nPublished using Topic Resource:")
fetch_and_print_from_sqs(sqs, demo_queue_url)

"""
PRODUCED OUTPUT:


Published using SNS extended client:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "465d51ea-2c85-4cf8-9ff7-f0a20636ac54"}]
Message Stored in S3 Bucket is: This message should be published to S3


Using decreased message size threshold:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "4e32bc6c-e67e-4e09-982b-66dfbe0c588a"}]
Message Stored in S3 Bucket is: This message should be published to S3 as it exceeds the limit of the 32 bytes


Published using Topic Resource:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "347c11c4-a22c-42e4-a6a2-9b5af5b76587"}]
Message Stored in S3 Bucket is: This message should be published to S3 using the topic resource
"""
