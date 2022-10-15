# Amazon SNS Extended Client Library for Python

### Implements the functionality of [amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib) in Python

## Getting Started

* **Sign up for AWS** -- Before you begin, you need an AWS account. For more information about creating an AWS account, see [create and activate aws account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).
* **Minimum requirements** -- Python 3.x (or later) and pip
* **Download** -- Download the latest preview release or pick it up from pip:
```
under construction
```


## Overview
sns-extended-client allows for publishing large messages through SNS via S3. This is the same mechanism that the Amazon library
[amazon-sns-java-extended-client-lib](https://github.com/awslabs/amazon-sns-java-extended-client-lib) provides.

To do this, this library automatically extends the normal boto3 SNS client and Topic resource classes upon import using the [botoinator](https://github.com/QuiNovas/botoinator) library. This allows for further extension or decoration if desired.

## Additional attributes available on `boto3` SQS `client` and `Queue` objects
* large_payload_support -- the S3 bucket name that will store large messages.
* message_size_threshold -- the threshold for storing the message in the large messages bucket. Cannot be less than `0` or greater than `262144`. Defaults to `262144`.
* always_through_s3 -- if `True`, then all messages will be serialized to S3. Defaults to `False`
* s3 -- the boto3 S3 `resource` object to use to store objects to S3. Use this if you want to control the S3 resource (for example, custom S3 config or credentials). Defaults to `boto3.resource("s3")` on first use if not previously set.

## Usage

#### Note:
> The s3 bucket must already exist prior to usage, and be accessible by whatever credentials you have available

### Enabling support for large payloads (>256Kb)

```python
import boto3
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
```

### Enabling support for large payloads (>64K)
```python
import boto3
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'
sqs.message_size_threshold = 65536

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
queue.message_size_threshold = 65536
```
### Enabling support for large payloads for all messages
```python
import boto3
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'
sqs.always_through_s3 = True

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
queue.always_through_s3 = True
```
### Setting a custom S3 resource
```python
import boto3
from botocore.config import Config
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'
sqs.s3 = boto3.resource(
  's3', 
  config=Config(
    signature_version='s3v4',
    s3={
      "use_accelerate_endpoint": True
    }
  )
)

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
queue.s3 = boto3.resource(
  's3', 
  config=Config(
    signature_version='s3v4',
    s3={
      "use_accelerate_endpoint": True
    }
  )
)
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

