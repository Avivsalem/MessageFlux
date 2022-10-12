try:
    from boto3.session import Config, Session  # type: ignore
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[objectstorage]') from ex

_S3_DEFAULT_TIMEOUT = 1
_S3_DEFAULT_RETRIES = 2


class S3Client:
    """
    represents an s3 client object
    """

    def __init__(self,
                 endpoint: str,
                 access_key: str,
                 secret_key: str,
                 timeout: int = _S3_DEFAULT_TIMEOUT,
                 retries: int = _S3_DEFAULT_RETRIES):
        """
        represents an s3 client object

        :param endpoint: the endpoint to s3 (url)
        :param access_key: the access key
        :param secret_key: the secret key
        """
        self._endpoint = endpoint

        session = Session(aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

        self._s3 = session.resource('s3',
                                    endpoint_url=endpoint,
                                    verify=False,
                                    config=Config(
                                        signature_version='s3',
                                        connect_timeout=timeout,
                                        retries={"max_attempts": retries},
                                        s3={'addressing_style': 'path'}))

    @property
    def s3_resource(self):
        """
        returns the actual s3 client

        :return: the actual s3 client
        """
        return self._s3

    @property
    def endpoint(self):
        """
        the endpoint for the s3 client
        """
        return self._endpoint
