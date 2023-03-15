class SNSExtendedClientException(Exception):
    """Base Class for all SNS Extended client Exceptions"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MissingPayloadOffloadingResource(SNSExtendedClientException):
    def __init__(self, *args, **kwargs):
        error_msg = "Undeclared/Missing S3 bucket name for payload offloading!"
        super().__init__(error_msg, *args, **kwargs)
