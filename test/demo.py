import boto3
from src.sns_extended_client import SNSExtendedClientSession

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

topic.publish(Message='This message should be published to S3 using the topic resource', MessageAttributes={'S3Key':{'DataType': 'String', 'StringValue': '347c11c4-a22c-42e4-a6a2-9b5af5b76587'}})

topic.always_through_s3 = False

topic.publish(Message='This message should is published using the topic resource.')

topic.message_size_threshold = 32
topic.publish(Message='This message should be published to S3 as it exceeds the limit of the 32 bytes')

