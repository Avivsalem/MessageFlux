import shutil
import threading
from datetime import datetime
from io import BytesIO
from typing import Optional, BinaryIO, Dict, Any

import time

try:
    from boto3.s3.inject import ClientError
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[objectstorage]') from ex

from messageflux.iodevices.objectstorage.s3api.s3client import S3Client


class _MockS3Client:
    class _MockBucket:
        class _MockObjectCollection:
            def __init__(self, bucket: '_MockS3Client._MockBucket', keys):
                self._bucket = bucket
                self._items = [bucket.Object(key=key) for key in keys]

            def __iter__(self):
                return self._items.__iter__()

            def delete(self):
                for item in self._items:
                    item.delete()

        class _MockLifecycle:
            def put(self, **kwargs):
                pass

        class _MockPolicy:
            def put(self, **kwargs):
                pass

        class _MockStreamingBody:
            def __init__(self, buf: BinaryIO):
                data = buf.read()
                self._size = len(data)
                self._buf = BytesIO(data)
                self._buf.seek(0)

            def size(self):
                return self._size

            def read(self, amt=None):
                return self._buf.read(amt)

        class _MockObject:
            def __init__(self, bucket: '_MockS3Client._MockBucket', key: str):
                self._bucket = bucket
                self.key = key

            @property
            def last_modified(self):
                return self._bucket._object_storage[self.key]["LastModified"]

            @property
            def size(self):
                return self._bucket._object_storage[self.key]["ContentLength"]

            def get(self):
                if self.key not in self._bucket._object_storage.keys():
                    raise ClientError({'Error': {'Code': 'NoSuchKey'}}, "get")
                obj = self._bucket._object_storage[self.key]
                return obj

            def load(self):
                pass

            def delete(self):
                if self.key not in self._bucket._object_storage.keys():
                    raise ClientError({'Error': {'Code': 'NoSuchKey'}}, "delete")
                self._bucket._object_storage.pop(self.key)

        def __init__(self, client: '_MockS3Client', bucket_name: str):
            self._bucket_name = bucket_name
            self._object_storage: Dict[str, Dict[str, Any]] = {}
            self.objects = self
            self._client = client

            self._lifecycle = self._MockLifecycle()
            self._policy = self._MockPolicy()

        def Object(self, key: str) -> '_MockS3Client._MockBucket._MockObject':
            return self._MockObject(self, key)

        def Lifecycle(self):
            return self._lifecycle

        def Policy(self):
            return self._policy

        def StreamingBody(self, buf: BinaryIO):
            return self._MockStreamingBody(buf)

        def create(self):
            self._client._buckets[self._bucket_name] = self

        def delete(self):
            self._object_storage.clear()
            self._client._buckets.pop(self._bucket_name)

        def put_object(self, Key, Body, Metadata=None):
            streaming_body = self.StreamingBody(Body)
            obj = {"Body": streaming_body,
                   "LastModified": str(datetime.now()),
                   "ContentLength": streaming_body.size()}
            if Metadata:
                obj["Metadata"] = Metadata
            self._object_storage[Key] = obj
            return self.Object(key=Key)

        def all(self):
            return self._MockObjectCollection(self, self._object_storage.keys())

        def filter(self, **kwargs):
            result_keys = []
            if "Prefix" in kwargs.keys():
                prefix = kwargs["Prefix"]
                for key, obj in self._object_storage.items():
                    if key.startswith(prefix):
                        result_keys.append(key)
            return self._MockObjectCollection(self, result_keys)

        def __iter__(self):
            return self._object_storage.values()

    def __init__(self, port: Optional[int] = None):
        self.client = self
        self._buckets: Dict[str, '_MockS3Client._MockBucket'] = {}
        self._port = port
        if port is not None:
            self._start_web_server(port)

    @property
    def meta(self):
        return self

    def Bucket(self, bucket_name: str):
        """
        create new bucket only if bucket_name doesnt exists
        """
        if bucket_name in self._buckets.keys():
            return self._buckets[bucket_name]

        return self._MockBucket(self, bucket_name)

    def download_fileobj(self, Bucket, Key, Fileobj, **kwargs):
        obj = self.Bucket(Bucket).Object(Key).get()
        stream = obj['Body']
        shutil.copyfileobj(stream, Fileobj)

    def upload_fileobj(self, Fileobj, Bucket, Key, **kwargs):
        self.Bucket(Bucket).put_object(Key, Fileobj)

    def create_bucket(self, Bucket):
        """
        :param Bucket: the name of the bucket
        """
        self._buckets[Bucket] = self.Bucket(bucket_name=Bucket)

    def head_bucket(self, Bucket):
        """
        :param Bucket: the name of the bucket
        """
        if Bucket not in self._buckets.keys():
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, "head_bucket")

    def _start_web_server(self, port: int):
        from flask import make_response, Flask
        flask_app = Flask(__name__)

        @flask_app.route("/<bucket_name>/<key>")
        def return_item(bucket_name, key):
            obj = self.Bucket(bucket_name).Object(key).get()
            data = obj['Body'].read()
            metadata = obj.get('Metadata', "")
            response = make_response(data)
            response.headers['Content-Type'] = 'binary/octet-stream'
            for meta_key, meta_value in metadata.items():
                response.headers[f'x-amz-meta-{meta_key}'] = meta_value
            return response

        t = threading.Thread(target=flask_app.run,
                             daemon=True,
                             kwargs={'host': "localhost",
                                     'port': port})
        t.start()
        time.sleep(1)


class MockS3Client(S3Client):
    """
    this is a mock s3 client, that is used for testing
    """

    # noinspection PyMissingConstructor
    def __init__(self, port: Optional[int] = None):
        self._s3 = _MockS3Client(port=port)
        self._endpoint = f"http://localhost:{port}"
