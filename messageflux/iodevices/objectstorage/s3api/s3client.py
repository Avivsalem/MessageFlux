try:
    from boto3.session import Config, Session  # type: ignore
    from mypy_boto3_s3 import S3ServiceResource
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[objectstorage]') from ex

_S3_DEFAULT_TIMEOUT = 1
_S3_DEFAULT_RETRIES = 2


class S3Client:
    """
    This class exists for compatibility purposes only.
    normally, it should not be used
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
        self._s3: S3ServiceResource = Session().resource('s3',
                                                         aws_access_key_id=access_key,
                                                         aws_secret_access_key=secret_key,
                                                         endpoint_url=endpoint,
                                                         verify=False,
                                                         config=Config(
                                                             signature_version='s3',
                                                             connect_timeout=timeout,
                                                             retries={
                                                                 "max_attempts": retries},
                                                             s3={'addressing_style': 'path'}))

    @property
    def s3_resource(self) -> S3ServiceResource:
        """
        returns the actual s3 client

        :return: the actual s3 client
        """
        return self._s3
