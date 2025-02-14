# Amazon SNS Extended Client Library for Python

### Implements the functionality of [amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib) in Python

## Getting Started

* **Sign up for AWS** -- Before you begin, you need an AWS account. For more information about creating an AWS account, see [create and activate aws account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).
* **Minimum requirements** -- Python 3.x (or later) and pip
* **Download** -- Download the latest preview release or pick it up from pip:
```
pip install amazon-sns-extended-client
```


## Overview
sns-extended-client allows for publishing large messages through SNS via S3. This is the same mechanism that the Amazon library
[amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib) provides.

## Additional attributes available on `boto3` SNS `client`, `Topic` and `PlatformEndpoint` objects.
* large_payload_support -- the S3 bucket name that will store large messages.
* use_legacy_attribute -- if `True`, then all published messages use the Legacy reserved message attribute (SQSLargePayloadSize) instead of the current reserved message attribute (ExtendedPayloadSize).
* message_size_threshold -- the threshold for storing the message in the large messages bucket. Cannot be less than `0` or greater than `262144`. Defaults to `262144`.
* always_through_s3 -- if `True`, then all messages will be serialized to S3. Defaults to `False`
* s3_client -- the boto3 S3 `client` object to use to store objects to S3. Use this if you want to control the S3 client (for example, custom S3 config or credentials). Defaults to `boto3.client("s3")` on first use if not previously set.

## Usage

#### Note:
> The s3 bucket must already exist prior to usage, and be accessible by whatever credentials you have available

### Enabling support for large payloads (>256Kb)

```python
import boto3
import sns_extended_client

# Low level client
sns = boto3.client('sns')
sns.large_payload_support = 'bucket-name'

# boto SNS.Topic resource
resource = boto3.resource('sns')
topic = resource.Topic('topic-arn')

# Or
topic = resource.create_topic(Name='topic-name')

topic.large_payload_support = 'my-bucket-name'

# boto SNS.PlatformEndpoint resource
resource = boto3.resource('sns')
platform_endpoint = resource.PlatformEndpoint('endpoint-arn')

platform_endpoint.large_payload_support = 'my-bucket-name'
```

### Enabling support for large payloads (>64K)
```python
import boto3
import sns_extended_client

# Low level client
sns = boto3.client('sns')
sns.large_payload_support = 'BUCKET-NAME'
sns.message_size_threshold = 65536

# boto SNS.Topic resource
resource = boto3.resource('sns')
topic = resource.Topic('topic-arn')

# Or
topic = resource.create_topic('topic-name')

topic.large_payload_support = 'bucket-name'
topic.message_size_threshold = 65536

# boto SNS.PlatformEndpoint resource
resource = boto3.resource('sns')
platform_endpoint = resource.PlatformEndpoint('endpoint-arn')

platform_endpoint.large_payload_support = 'my-bucket-name'
platform_endpoint.message_size_threshold = 65536
```
### Enabling support for large payloads for all messages
```python
import boto3
import sns_extended_client

# Low level client
sns = boto3.client('sns')
sns.large_payload_support = 'my-bucket-name'
sns.always_through_s3 = True

# boto SNS.Topic resource
resource = boto3.resource('sns')
topic = resource.Topic('topic-arn')

# Or
topic = resource.create_topic(Name='topic-name')

topic.large_payload_support = 'my-bucket-name'
topic.always_through_s3 = True

# boto SNS.PlatformEndpoint resource
resource = boto3.resource('sns')
platform_endpoint = resource.PlatformEndpoint('endpoint-arn')

platform_endpoint.large_payload_support = 'my-bucket-name'
platform_endpoint.always_through_s3 = True
```
### Setting a custom S3 config
```python
import boto3
from botocore.config import Config
import sns_extended_client

# Define Configuration for boto3's S3 Client 
# NOTE - The boto3 version from 1.36.0 to 1.36.6 will throw an error if you enable accelerate_endpoint.
s3_client_config = Config(
    region_name = 'us-east-1',
    signature_version = 's3v4',
    s3={
        "use_accelerate_endpoint":True
    }
)

# Low level client
sns = boto3.client('sns')
sns.large_payload_support = 'my-bucket-name'
sns.s3_client = boto3.client("s3", config=s3_client_config)

# boto SNS.Topic resource
resource = boto3.resource('sns')
topic = resource.Topic('topic-arn')

# Or
topic = resource.topic(Name='topic-name')

topic.large_payload_support = 'my-bucket-name'
topic.s3_client = boto3.client("s3", config=s3_client_config)

# boto SNS.PlatformEndpoint resource
resource = boto3.resource('sns')
platform_endpoint = resource.PlatformEndpoint('endpoint-arn')

platform_endpoint.large_payload_support = 'my-bucket-name'
platform_endpoint.s3_client = boto3.client("s3", config=s3_client_config)
```

### Setting a custom S3 Key
Publish Message Supports user defined S3 Key used to store objects in the specified Bucket.

To use custom keys add the S3 key as a Message Attribute in the MessageAttributes argument with the MessageAttribute.

**Key - "S3Key"**
```python
sns.publish(
    Message="message",
    MessageAttributes={
        "S3Key": {
            "DataType": "String",
            "StringValue": "--S3--Key--",
        }
    },
)
```

### Using SQSLargePayloadSize as reserved message attribute
Initial versions of the Java SNS Extended Client used 'SQSLargePayloadSize' as the reserved message attribute to determine that a message is an S3 message.

In the later versions it was changed to use 'ExtendedPayloadSize'.

To use the Legacy reserved message attribute set use_legacy_attribute parameter to `True`.

```python
import boto3
import sns_extended_client

# Low level client
sns = boto3.client('sns')
sns.large_payload_support = 'bucket-name'

sns.use_legacy_attribute = True

# boto SNS.Topic resource
resource = boto3.resource('sns')
topic = resource.Topic('topic-arn')

# Or
topic = resource.create_topic(Name='topic-name')

topic.large_payload_support = 'my-bucket-name'
topic.use_legacy_attribute = True

# boto SNS.PlatformEndpoint resource
resource = boto3.resource('sns')
platform_endpoint = resource.PlatformEndpoint('endpoint-arn')

platform_endpoint.large_payload_support = 'my-bucket-name'
platform_endpoint.use_legacy_attribute = True 
```

## CODE SAMPLE
Here is an example of using the extended payload utility:

Here we create an SNS Topic and SQS Queue, then subscribe the queue to the topic.

We publish messages to the created Topic and print the published message from the queue along with the original message retrieved from S3.

```python
import boto3
from sns_extended_client import SNSExtendedClientSession
from json import loads

s3_extended_payload_bucket = "extended-client-bucket-store"  # S3 bucket with the given bucket name is a resource which is created and accessible with the given AWS credentials
TOPIC_NAME = "---TOPIC-NAME---"
QUEUE_NAME = "---QUEUE-NAME---"

def allow_sns_to_write_to_sqs(topicarn, queuearn):
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

def get_msg_from_s3(body,sns_extended_client):
    """Handy Helper to fetch message from S3"""
    json_msg = loads(body)
    s3_object = sns_extended_client.s3_client.get_object(
        Bucket=json_msg[1].get("s3BucketName"), Key=json_msg[1].get("s3Key")
    )
    msg = s3_object.get("Body").read().decode()
    return msg


def fetch_and_print_from_sqs(sqs, queue_url,sns_extended_client):
    sqs_msg = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MessageAttributeNames=['All'],
        VisibilityTimeout=0,
        WaitTimeSeconds=0,
        MaxNumberOfMessages=1
    ).get("Messages")[0]
    
    message_body = sqs_msg.get("Body")
    print("Published Message: {}".format(message_body))
    print("Message Stored in S3 Bucket is: {}\n".format(get_msg_from_s3(message_body,sns_extended_client)))

    # Delete the Processed Message
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=sqs_msg['ReceiptHandle']
    )


sns_extended_client = boto3.client("sns", region_name="us-east-1")
create_topic_response = sns_extended_client.create_topic(Name=TOPIC_NAME)
sns_topic_arn = create_topic_response.get("TopicArn")

# create and subscribe an sqs queue to the sns client
sqs = boto3.client("sqs",region_name="us-east-1")
demo_queue_url = sqs.create_queue(QueueName=QUEUE_NAME).get("QueueUrl")
sqs_queue_arn = sqs.get_queue_attributes(
    QueueUrl=demo_queue_url, AttributeNames=["QueueArn"]
)["Attributes"].get("QueueArn")

# Adding policy to SQS queue such that SNS topic can send msg to SQS queue
policy_json = allow_sns_to_write_to_sqs(sns_topic_arn, sqs_queue_arn)
response = sqs.set_queue_attributes(
    QueueUrl = demo_queue_url,
    Attributes = {
        'Policy' : policy_json
    }
)

# Set the RawMessageDelivery subscription attribute to TRUE if you want to use
# SQSExtendedClient to help with retrieving msg from S3
sns_extended_client.subscribe(TopicArn=sns_topic_arn, Protocol="sqs", 
Endpoint=sqs_queue_arn
, Attributes={"RawMessageDelivery":"true"}
)

sns_extended_client.large_payload_support = s3_extended_payload_bucket

# Change default s3_client attribute of sns_extended_client to use 'us-east-1' region
sns_extended_client.s3_client = boto3.client("s3", region_name="us-east-1")


# Below is the example that all the messages will be sent to the S3 bucket
sns_extended_client.always_through_s3 = True
sns_extended_client.publish(
    TopicArn=sns_topic_arn, Message="This message should be published to S3"
)
print("\n\nPublished using SNS extended client:")
fetch_and_print_from_sqs(sqs, demo_queue_url,sns_extended_client)  # Prints message stored in s3

# Below is the example that all the messages larger than 32 bytes will be sent to the S3 bucket
print("\nUsing decreased message size threshold:")

sns_extended_client.always_through_s3 = False
sns_extended_client.message_size_threshold = 32
sns_extended_client.publish(
    TopicArn=sns_topic_arn,
    Message="This message should be published to S3 as it exceeds the limit of the 32 bytes",
)

fetch_and_print_from_sqs(sqs, demo_queue_url,sns_extended_client)  # Prints message stored in s3


# Below is the example to publish message using the SNS.Topic resource
sns_extended_client_resource = SNSExtendedClientSession().resource(
    "sns", region_name="us-east-1"
)

topic = sns_extended_client_resource.Topic(sns_topic_arn)
topic.large_payload_support = s3_extended_payload_bucket

# Change default s3_client attribute of topic to use 'us-east-1' region
topic.s3_client = boto3.client("s3", region_name="us-east-1")

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
fetch_and_print_from_sqs(sqs, demo_queue_url,topic)

# Below is the example to publish message using the SNS.PlatformEndpoint resource
sns_extended_client_resource = SNSExtendedClientSession().resource(
    "sns", region_name="us-east-1"
)

platform_endpoint = sns_extended_client_resource.PlatformEndpoint(sns_topic_arn)
platform_endpoint.large_payload_support = s3_extended_payload_bucket

# Change default s3_client attribute of platform_endpoint to use 'us-east-1' region
platform_endpoint.s3_client = boto3.client("s3", region_name="us-east-1")

platform_endpoint.always_through_s3 = True
# Can Set custom S3 Keys to be used to store objects in S3
platform_endpoint.publish(
    Message="This message should be published to S3 using the PlatformEndpoint resource",
    MessageAttributes={
        "S3Key": {
            "DataType": "String",
            "StringValue": "247c11c4-a22c-42e4-a6a2-9b5af5b76587",
        }
    },
)
print("\nPublished using PlatformEndpoint Resource:")
fetch_and_print_from_sqs(sqs, demo_queue_url,platform_endpoint)
```

PRODUCED OUTPUT:
```
Published using SNS extended client:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "10999f58-c5ae-4d68-9208-f70475e0113d"}]
Message Stored in S3 Bucket is: This message should be published to S3

Using decreased message size threshold:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "2c5cb2c7-e649-492b-85fb-fa9923cb02bf"}]
Message Stored in S3 Bucket is: This message should be published to S3 as it exceeds the limit of the 32 bytes

Published using Topic Resource:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "347c11c4-a22c-42e4-a6a2-9b5af5b76587"}]
Message Stored in S3 Bucket is: This message should be published to S3 using the topic resource

Published using PlatformEndpoint Resource:
Published Message: ["software.amazon.payloadoffloading.PayloadS3Pointer", {"s3BucketName": "extended-client-bucket-store", "s3Key": "247c11c4-a22c-42e4-a6a2-9b5af5b76587"}]
Message Stored in S3 Bucket is: This message should be published to S3 using the PlatformEndpoint resource
```

## DEVELOPMENT

We have built-in Makefile to run test, format check or fix in one command. Please check [Makefile](Makefile) for more information.

Just run below command, and it will do format check and run unit test:
```
make ci
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.