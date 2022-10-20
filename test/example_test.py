import boto3
from sns_extended_client.sns_extended_client import SNSExtendedClient


SMALL_MSG_BODY = 'small message body'
SMALL_MSG_ATTRIBUTES = {'test attr':{'DataType': 'String', 'StringValue': 'str value'}}
ATTRIBUTES_ADDED = ['large_payload_support', 'message_size_threshold', 'always_through_s3', 's3', '_create_s3_put_object_params', '_is_large_message', '_determine_payload', '_get_s3_key']


def demo():

    sns_resource = boto3.resource(service_name='sns', region_name='us-east-1')
    topic = sns_resource.Topic(topicArn='--TOPIC--')

    # sns_extended_client = SNSExtendedClient(topic_resource=topic, large_payload_support='extended-client-bucket-store')
    # sns_extended_client.set_always_through_s3()
    # sns_extended_client.use_legacy_attribute()

    # sns_extended_client.publish(Message='This should be published to s3.')


if __name__ == '__main__':
    demo()
