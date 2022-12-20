import json
import os
import re
from abc import abstractmethod, ABCMeta
from hashlib import md5
from typing import Optional, BinaryIO, Dict, Any, Tuple

from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.message_store_device_wrapper.message_store_base import MessageStoreException, \
    MessageStoreBase
from messageflux.iodevices.objectstorage.s3api.s3bucket import BUCKET_NAME_VALIDATOR, S3Bucket, S3ServiceResource
from messageflux.metadata_headers import MetadataHeaders
from messageflux.utils import get_random_id, json_safe_encoder

KEY_HEADER_CONST = "__KEY__"

_S3_TIMEOUT = int(os.environ.get("S3_TIMEOUT", 1))
_S3_RETRIES = int(os.environ.get("S3_RETRIES", 2))


class BucketNameFormatterBase:
    """
    a base class for formatter to manipulate the bucket name in case it needs to be different then the device name
    """
    BUCKET_SANITATION_RE = re.compile(r'[^a-z0-9.\-]')

    def _sanitize_bucket_name(self, bucket_name: str) -> str:
        sanitized_bucket_name = self.BUCKET_SANITATION_RE.sub('-', bucket_name.lower())
        # Some devices have an underscore ("_") in their name which is not allowed for S3Bucket. So replace it with "-"
        if not BUCKET_NAME_VALIDATOR.match(sanitized_bucket_name):
            raise MessageStoreException(
                'Invalid bucket name. Bucket name must be between 3 and 63 characters long, and '
                'contain only lowercase letters, digits, dots (.) and hyphens(-). '
                f'Bucket name was: {bucket_name}')
        return sanitized_bucket_name

    def _inner_format_name(self, device_name: str, message_bundle: MessageBundle) -> str:
        """
        uses the given parameters to determine what the bucket name should be.
        this should be inherited and implemented in children

        :param device_name: the name of the device
        :param message_bundle: the message that was sent to the device
        :return: the bucket name
        """
        return device_name

    def format_name(self, device_name: str, message_bundle: MessageBundle) -> str:
        """
        uses the given parameters to determine what the bucket name should be

        :param device_name: the name of the device
        :param message_bundle: the message that was sent to the device
        :return: the bucket name
        """
        formatted_name = self._inner_format_name(device_name, message_bundle)
        return self._sanitize_bucket_name(formatted_name)


class _S3MessageStoreBase(MessageStoreBase, metaclass=ABCMeta):
    """
    a message store that uses S3 as it's base
    """
    _ORIGINAL_HEADERS_KEY = "originalheaders"

    def __init__(self,
                 s3_resource: S3ServiceResource,
                 magic: bytes,
                 auto_create_bucket: bool = False,
                 bucket_name_formatter: Optional[BucketNameFormatterBase] = None):
        """
        An S3 based message store

        :param s3_resource: the s3 resource from boto
        :param auto_create_bucket: Whether or not a bucket will be created
                                   when a message is being put in a nonexistent one.
        :param bucket_name_formatter: a formatter to use to manipulate the bucket name.
                                      if none is given the device name will be used
        """
        self.bucket_name_formatter = bucket_name_formatter or BucketNameFormatterBase()
        self._magic = magic

        self._s3_resource = s3_resource
        self._auto_create_bucket = auto_create_bucket
        self._bucket_cache: Dict[str, S3Bucket] = {}

    def _get_bucket(self, bucket_name: str, auto_create=False) -> S3Bucket:
        """
        gets the bucket from cache
        """
        bucket = self._bucket_cache.get(bucket_name, None)
        if bucket is None:
            bucket = S3Bucket(bucket_name, self._s3_resource, auto_create=auto_create)
            self._bucket_cache[bucket_name] = bucket

        return bucket

    @property
    def magic(self) -> bytes:
        """
        return a magic string that is unique and constant for this message store
        """
        return self._magic

    def _serialize_key(self, bucket: S3Bucket, key: str) -> str:
        """
        serializes the key for sending on the wire

        :param bucket: the bucket
        :param key: the key in the bucket
        :return: serialized key to send
        """
        data_dict = {'bucket_name': bucket.name, 'key': key}
        return json.dumps(data_dict)

    def deserialize_key(self, data: str) -> Tuple[str, str]:
        """
        deserializes the key and bucket received from  the wire

        :param data: the data from the wire
        :return: deserialized (bucket name, key)
        """
        data_dict: Dict[str, Any] = json.loads(data)
        bucket_name = str(data_dict['bucket_name'])
        key = str(data_dict['key'])
        return bucket_name, key

    def _serialize_headers(self, headers: Dict[str, Any]) -> Dict[str, str]:
        return {self._ORIGINAL_HEADERS_KEY: json.dumps(headers, default=json_safe_encoder)}

    def _deserialize_headers(self, headers: Dict[str, str]) -> Dict[str, Any]:
        return json.loads(headers.get(self._ORIGINAL_HEADERS_KEY, '{}'))

    @abstractmethod
    def _read_message_from_bucket(self, bucket: S3Bucket, key: str) -> Tuple[BinaryIO, Dict[str, Any]]:
        """
        reads the message from the current s3 api
        """
        pass

    @abstractmethod
    def _put_message_to_bucket(self, bucket: S3Bucket, key: str, message: BinaryIO, metadata: Dict[str, str]):
        pass

    def read_message(self, key: str) -> MessageBundle:
        """
        reads a message according to the key given
        :return: a tuple of the bytes of the message to read, and its metadata
        """
        bucket_name, s3_key = self.deserialize_key(key)
        bucket = self._get_bucket(bucket_name)
        body, headers = self._read_message_from_bucket(bucket, s3_key)

        message_headers = self._deserialize_headers(headers)
        device_headers = {KEY_HEADER_CONST: s3_key}

        return MessageBundle(Message(body, message_headers), device_headers)

    def put_message(self, device_name: str, message_bundle: MessageBundle) -> str:
        """
        puts a message in the message store

        :param device_name: the name of the device putting the item in the store
        :param message_bundle: the Message bundle to write to the store
        :return: the key to the message in the message store
        """
        bucket_name = self.bucket_name_formatter.format_name(device_name, message_bundle)
        bucket = self._get_bucket(bucket_name=bucket_name, auto_create=self._auto_create_bucket)
        data_hash = md5(message_bundle.message.bytes).hexdigest()
        key = \
            message_bundle.message.headers.get(KEY_HEADER_CONST) or \
            message_bundle.device_headers.get(MetadataHeaders.ITEM_ID) or \
            get_random_id()

        key = key + '.' + data_hash
        serialized_headers = self._serialize_headers(message_bundle.message.headers)
        self._put_message_to_bucket(bucket=bucket,
                                    key=key,
                                    message=message_bundle.message.stream,
                                    metadata=serialized_headers)
        return self._serialize_key(bucket, key)

    def delete_message(self, key: str):
        """
        deletes a message from the message store
        :param str key: the key to the message
        """
        bucket_name, s3_key = self.deserialize_key(key)
        bucket = self._get_bucket(bucket_name=bucket_name)
        bucket.delete_object(s3_key)


class S3MessageStore(_S3MessageStoreBase):
    """
    a message store that uses S3 as it's base
    """

    def __init__(self,
                 s3_resource: S3ServiceResource,
                 magic: bytes = b"__S3_MSGSTORE__",
                 auto_create_bucket: bool = False,
                 bucket_name_formatter: Optional[BucketNameFormatterBase] = None,
                 put_object_extra_args: Optional[Dict[str, Any]] = None):
        """
        An S3 based message store

        :param s3_resource: the s3 resource from boto
        :param auto_create_bucket: Whether or not a bucket will be created
                                   when a message is being put in a nonexistent one.
        :param bucket_name_formatter: a formatter to use to manipulate the bucket name.
                                      if none is given the device name will be used
        :param put_object_extra_args: extra args to give to bucket.put_object(). i.e 'StorageClass'
        """
        super().__init__(s3_resource=s3_resource,
                         magic=magic,
                         auto_create_bucket=auto_create_bucket,
                         bucket_name_formatter=bucket_name_formatter)
        self._put_object_extra_args = put_object_extra_args or {}

    def _read_message_from_bucket(self, bucket: S3Bucket, key: str) -> Tuple[BinaryIO, Dict[str, Any]]:
        s3obj = bucket.get_object(key=key)
        headers = s3obj.metadata
        return s3obj.body, headers

    def _put_message_to_bucket(self, bucket: S3Bucket, key: str, message: BinaryIO, metadata: Dict[str, str]):
        bucket.put_object(key=key,
                          buf=message,
                          metadata=metadata,
                          **self._put_object_extra_args)


class S3UploadMessageStore(_S3MessageStoreBase):
    """
    a message store that uses S3 as it's base (using upload_file method)
    """

    def __init__(self,
                 s3_resource: S3ServiceResource,
                 magic: bytes = b"__S3_UPLOAD_MSGSTORE__",
                 auto_create_bucket: bool = False,
                 bucket_name_formatter: Optional[BucketNameFormatterBase] = None,
                 upload_extra_args: Optional[Dict[str, Any]] = None):
        """
        An S3 based message store

        :param s3_resource: the s3 resource from boto
        :param auto_create_bucket: Whether or not a bucket will be created
                                   when a message is being put in a nonexistent one.
        :param bucket_name_formatter: a formatter to use to manipulate the bucket name.
                                      if none is given the device name will be used
        :param upload_extra_args: extra args to give to client.upload_fileobj(). i.e 'StorageClass'
        """
        super().__init__(s3_resource=s3_resource,
                         magic=magic,
                         auto_create_bucket=auto_create_bucket,
                         bucket_name_formatter=bucket_name_formatter)
        self._upload_extra_args = upload_extra_args or {}

    def _read_message_from_bucket(self, bucket: S3Bucket, key: str) -> Tuple[BinaryIO, Dict[str, Any]]:
        s3obj = bucket.get_object(key=key)
        headers = s3obj.metadata
        return s3obj.body, headers

    def _put_message_to_bucket(self, bucket: S3Bucket, key: str, message: BinaryIO, metadata: Dict[str, str]):
        bucket.upload_object(key=key,
                             stream=message,
                             metadata=metadata,
                             **self._upload_extra_args)
