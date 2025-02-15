import boto3
import pytest
from sns_extended_client.session import SNSExtendedClientSession

@pytest.fixture()
def region_name() -> str:
    region_name = 'us-east-1'
    return region_name

@pytest.fixture()
def session(region_name) -> boto3.Session:

    setattr(boto3.session, "Session", SNSExtendedClientSession)
    # Now take care of the reference in the boto3.__init__ module since the object is being imported there too
    setattr(boto3, "Session", SNSExtendedClientSession)

    # return boto3.session.Session()
    print("This session is fetched")
    return boto3.Session(region_name=region_name)    