import random
from io import BytesIO

import mock

from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.objectstorage import S3MessageStore
from messageflux.iodevices.objectstorage.s3api.mock_s3_client import _MockS3Client


def test_s3_message_store():
    port = random.randint(10000, 20000)
    mock_s3_client = _MockS3Client(port)
    with mock.patch("boto3.session.Session.resource", mock.MagicMock(return_value=mock_s3_client)):
        data = BytesIO(b"some data")
        headers = {"header key": "header value"}
        with S3MessageStore(f"http://localhost:{port}", "access key", "secret key",
                            auto_create_bucket=True) as s3_message_store:
            key = s3_message_store.put_message("device-name", MessageBundle(Message(data, headers)))

            assert s3_message_store._s3_client.s3_resource._buckets
            assert s3_message_store._s3_client.s3_resource._buckets["device-name"]._object_storage

            res_bundle = s3_message_store.read_message(key)

            assert res_bundle.message.bytes == b"some data"
            assert "header key" in res_bundle.message.headers
            assert res_bundle.message.headers["header key"] == "header value"

            s3_message_store.delete_message(key)

            assert s3_message_store._s3_client.s3_resource._buckets
            assert not s3_message_store._s3_client.s3_resource._buckets["device-name"]._object_storage
