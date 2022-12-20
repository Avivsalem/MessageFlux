from io import BytesIO

import boto3
import pytest
from moto import mock_s3

from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.objectstorage import S3MessageStore, S3UploadMessageStore, S3NoSuchItem


@mock_s3
def test_s3_message_store():
    data = BytesIO(b"some data")
    headers = {"header key": "header value"}
    s3_resource = boto3.Session().resource('s3')
    with S3MessageStore(s3_resource,
                        auto_create_bucket=True) as s3_message_store:
        key = s3_message_store.put_message("device-name", MessageBundle(Message(data, headers)))

        res_bundle = s3_message_store.read_message(key)

        assert res_bundle.message.bytes == b"some data"
        assert "header key" in res_bundle.message.headers
        assert res_bundle.message.headers["header key"] == "header value"

        s3_message_store.delete_message(key)

        with pytest.raises(S3NoSuchItem):
            res_bundle = s3_message_store.read_message(key)


@mock_s3
def test_s3_upload_message_store():
    data = BytesIO(b"some data")
    headers = {"header key": "header value"}
    s3_resource = boto3.Session().resource('s3')
    with S3UploadMessageStore(s3_resource,
                              auto_create_bucket=True) as s3_message_store:
        key = s3_message_store.put_message("device-name", MessageBundle(Message(data, headers)))

        res_bundle = s3_message_store.read_message(key)

        assert res_bundle.message.bytes == b"some data"
        assert "header key" in res_bundle.message.headers
        assert res_bundle.message.headers["header key"] == "header value"

        s3_message_store.delete_message(key)

        with pytest.raises(S3NoSuchItem):
            res_bundle = s3_message_store.read_message(key)
