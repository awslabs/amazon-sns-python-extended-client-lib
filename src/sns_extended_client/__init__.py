import boto3

from .session import SNSExtendedClientSession

setattr(boto3.session, "Session", SNSExtendedClientSession)

# Now take care of the reference in the boto3.__init__ module
setattr(boto3, "Session", SNSExtendedClientSession)

# Now ensure that even the default session is our SNSExtendedClientSession
if boto3.DEFAULT_SESSION:
    boto3.setup_default_session()