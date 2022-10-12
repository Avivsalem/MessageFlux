import json
import re
from datetime import datetime
from io import BytesIO
from typing import Optional, Dict, Any, Iterator, TYPE_CHECKING
from urllib.parse import urljoin

try:
    from boto3.s3.inject import ClientError
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[objectstorage]') from ex

from messageflux.iodevices.objectstorage.s3api.s3client import S3Client

BUCKET_NAME_VALIDATOR = re.compile(r'^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$')

if TYPE_CHECKING:
    from _typeshed import SupportsRead, SupportsWrite


class S3BucketException(Exception):
    """
    represents errors thrown from S3 bucket
    """
    pass


class S3NoSuchItem(S3BucketException):
    """
    represents no such item in bucket
    """
    pass


class S3NoSuchBucket(S3BucketException):
    """
    represents no such bucket
    """
    pass


class S3Object:
    """
    represents an s3 object with lazy content get
    """

    def __init__(self, object_summery):
        self._object_summery = object_summery
        self._object_cache = None
        self._body = None
        self._metadata = None

    @property
    def _object_dict(self) -> Dict[str, Any]:
        if self._object_cache is None:
            self._object_cache = self._object_summery.get()
        return self._object_cache

    @property
    def key(self) -> str:
        """
        the object's key
        """
        return self._object_summery.key

    @property
    def body_stream(self) -> 'SupportsRead[bytes]':
        """
        the stream for the object's body (lazy read)
        """
        if self._body is None:
            return self._object_dict['Body']
        return self._body

    @property
    def body(self) -> BytesIO:
        """
        the bytes for the object's body (lazy read)
        """
        if self._body is None:
            self._body = BytesIO(self._object_dict['Body'].read())
        return self._body

    @property
    def metadata(self) -> Dict[str, str]:
        """
        the object's metadata
        """
        if self._metadata is None:
            self._metadata = self._object_dict.get('Metadata', {})

        assert self._metadata is not None
        return self._metadata

    @property
    def last_modified(self) -> datetime:
        """
        the object's last modified time
        """
        return self._object_summery.last_modified

    @property
    def size(self) -> int:
        """
        the object's size
        """
        return self._object_summery.size


class S3Bucket:
    """
    this class represents a single S3 Bucket
    """

    @staticmethod
    def create_bucket(s3_client: S3Client,
                      bucket_name: str,
                      lifetime_in_days: Optional[int] = None,
                      allow_public_access=False) -> 'S3Bucket':
        """
        creates and returns a bucket

        :param s3_client: the s3 client to use
        :param bucket_name: the bucket name to create and get
        :param lifetime_in_days: the lifetime of objects in the bucket to set (None for no change)
        :param allow_public_access: should we make the bucket publicly accessible from web
        :return: the bucket
        """
        bucket = S3Bucket(bucket_name=bucket_name, s3_client=s3_client, auto_create=True)
        if lifetime_in_days is not None:
            bucket.set_lifetime_in_days(lifetime_in_days)

        if allow_public_access:
            bucket.allow_public_access()

        return bucket

    @staticmethod
    def list_buckets(s3_client: S3Client) -> Iterator['S3Bucket']:
        """
        returns a list of all the buckets in this client

        :param s3_client: the s3 client to use
        :return:
        """
        for bucket in s3_client.s3_resource.buckets.all():
            yield S3Bucket(bucket_name=bucket.name, s3_client=s3_client)

    def __init__(self, bucket_name: str, s3_client: S3Client, auto_create: bool = False):
        """
        this class represents a single S3 Bucket

        :param bucket_name: the name of the bucket to work with
        :param s3_client: the s3 client to use
        :param auto_create: should we create the bucket if it doesn't exist?
        """
        if not BUCKET_NAME_VALIDATOR.match(bucket_name):
            raise S3BucketException(f'Invalid Bucket Name:{bucket_name}. check bucket naming rules.')
        try:
            self._bucket_name = bucket_name
            self._s3client = s3_client
            self._s3bucket = s3_client.s3_resource.Bucket(bucket_name)
            s3_client.s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')

            if code in ['NoSuchKey', '404']:
                if auto_create:
                    self._s3bucket.create()
                else:
                    raise S3NoSuchBucket(f'No bucket with name {bucket_name} exists')
            else:
                raise S3BucketException(
                    f'Error While creating S3Bucket "{bucket_name}": {code}') from ex

    def delete_bucket(self, force=False):
        """
        deletes all items in bucket, and then deletes bucket
        :param force: True will delete all objects on bucket, before deleting the bucket
        """
        if force:
            self.clear_objects()
        self._s3bucket.delete()

    @property
    def s3_client(self) -> S3Client:
        """
        the client for this bucket
        """
        return self._s3client

    @property
    def name(self) -> str:
        """
        returns the bucket name
        :return: the bucket name
        """
        return self._bucket_name

    def compute_url(self, key):
        """
        computes the item url, by key

        :param key: the key to item
        :return: the url to item
        """
        return urljoin(self.s3_client.endpoint, f'{self.name}/{key}')

    def allow_public_access(self):
        """
        makes the bucket publicly accessible from web
        """
        policy = {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"urn:sgws:s3:::{self.name}",
                        f"urn:sgws:s3:::{self.name}/*"
                    ]
                }
            ]
        }
        self._s3bucket.Policy().put(Policy=json.dumps(policy))

    def set_lifetime_in_days(self, days: int):
        """
        sets the lifetime of all objects in bucket to 'days' days (or <=0 to cancel expiration)

        :param days: the number of days for an item to live
        """
        try:
            if days <= 0:
                status = 'Disabled'
                days = 1
            else:
                status = 'Enabled'
            lc = {
                'Rules': [{
                    'Status': status,
                    'Prefix': '',
                    'Expiration': {
                        'Days': days
                    },
                    'ID': 'expire'
                }]
            }
            self._s3bucket.Lifecycle().put(LifecycleConfiguration=lc)
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')
            raise S3BucketException(
                f'Error While setting bucket lifecycle: {code}') from ex

    def put_object(self, key: str, buf: 'SupportsRead[bytes]', metadata: Dict[str, str], **kwargs) -> S3Object:
        """
        puts a binary object in the bucket

        :param key: the key of the object to put
        :param buf: the buffer to put in the bucket
        :param metadata: extra metadata
        """
        try:
            obj_summary = self._s3bucket.put_object(Key=key, Metadata=metadata, Body=buf, **kwargs)
            result = S3Object(object_summery=obj_summary)
            return result
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')
            raise S3BucketException(
                f'Error While putting object to key "{key}": {code}') from ex

    def upload_object(self, key: str, stream: 'SupportsRead[bytes]', **kwargs) -> S3Object:
        """
        uploads a binary stream to the bucket

        :param key: the key of the object to put
        :param stream: the stream to put in the bucket
        """
        self._s3client.s3_resource.meta.client.upload_fileobj(stream, self.name, key, **kwargs)
        return S3Object(self._s3bucket.Object(key))

    def download_object(self, key: str, writable_stream: 'SupportsWrite[bytes]', **kwargs):
        """
        downloads an object from the bucket into a writable stream

        :param key: the key of the object to download
        :param writable_stream: the writable stream to write into
        :return:
        """
        self._s3client.s3_resource.meta.client.download_fileobj(self.name, key, writable_stream, **kwargs)

    def get_object(self, key: str) -> S3Object:
        """
        gets a binary from the bucket

        :param key: the key of the binary
        :return: the buffer of the object, and the metadata dict
        """
        try:
            obj_summary = self._s3bucket.Object(key)
            obj_summary.load()  # checks that this item exists
            return S3Object(obj_summary)
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')

            if code in ['NoSuchKey', '404']:
                raise S3NoSuchItem(f'No item with key {key} in bucket {self._bucket_name}')
            else:
                raise S3BucketException(
                    f'Error While getting object from key "{key}": {code}') from ex

    def delete_object(self, key: str):
        """
        deletes a binary from the bucket

        :param key: the key of the binary
        """
        try:
            self._s3bucket.Object(key).delete()
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')
            if code in ['NoSuchKey', '404']:
                raise S3NoSuchItem(f'No item with key {key} in bucket {self._bucket_name}')
            else:
                raise S3BucketException(
                    f'Error While deleting object in key "{key}": {code}') from ex

    def list_objects(self) -> Iterator[S3Object]:
        """
        lists all the objects in this bucket
        :return: a generator of S3Objects
        """
        try:
            response = self._s3bucket.objects.all()
            for item in response:
                yield S3Object(item)
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')
            raise S3BucketException(
                f'Error While listing objects in bucket "{self._bucket_name}": {code}') from ex

    def find_objects(self,
                     prefix: str = None,
                     delimiter: str = None,
                     max_keys: int = 1000, **kwargs) -> Iterator[S3Object]:
        """
        find objects ih the bucket
        :param delimiter: A Delimiter is a character you use to group keys
        :param prefix: Limits the response to keys that begin with the specified prefix
        :param max_keys: Sets the maximum number of keys returned in the response
        :param kwargs: Filters to be used
        :return: dict from object key to last modified
        """
        try:
            if delimiter is not None:
                kwargs['Delimiter'] = delimiter
            if prefix is not None:
                kwargs['Prefix'] = prefix

            response = self._s3bucket.objects.filter(MaxKeys=max_keys,
                                                     **kwargs)
            for item in response:
                yield S3Object(item)
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')
            raise S3BucketException(
                f'Error While finding objects in bucket "{self._bucket_name}": {code}') from ex

    def clear_objects(self):
        """
        clears all the objects in the bucket
        """
        try:
            self._s3bucket.objects.all().delete()
        except ClientError as ex:
            code = ''
            if 'Error' in ex.response:
                code = ex.response['Error'].get('Code', '')
            raise S3BucketException(
                f'Error While removing all objects in bucket "{self._bucket_name}": {code}') from ex
