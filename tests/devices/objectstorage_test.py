from io import BytesIO

import boto3
import pytest
from moto import mock_s3

from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.objectstorage import BucketNameFormatterBase
from messageflux.iodevices.objectstorage import S3MessageStore, S3UploadMessageStore, S3NoSuchItem


@mock_s3
def test_s3_message_store():
    data = BytesIO(b"some data")
    headers = {"header key": "header value"}
    s3_resource = boto3.resource('s3')
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


@mock_s3
def test_headers():
    from messageflux.iodevices.objectstorage.s3api.s3bucket import S3Bucket
    s3_resource = boto3.resource('s3')
    S3Bucket.create_bucket(s3_resource,
                           BucketNameFormatterBase().format_name('SomeFlow', None))

    message_store = S3MessageStore(s3_resource)
    message_store.connect()

    key = message_store.put_message("SomeFlow",
                                    MessageBundle(Message(BytesIO(b"Message"), {"SOME_HEADER": "some_value"})))
    res_bundle = message_store.read_message(key)
    assert res_bundle.message.headers["SOME_HEADER"] == "some_value"

    key = message_store.put_message("SomeFlow",
                                    MessageBundle(Message(BytesIO(b"Message"), {"some_header": b"some_value"})))
    res_bundle = message_store.read_message(key)
    assert res_bundle.message.headers["some_header"] == "some_value"

    key = message_store.put_message("SomeFlow",
                                    MessageBundle(Message(BytesIO(b"Message"), {"Some_Header": "some_value"})))
    res_bundle = message_store.read_message(key)
    assert res_bundle.message.headers["Some_Header"] == "some_value"

    key = message_store.put_message("SomeFlow", MessageBundle(Message(BytesIO(b"Message"),
                                                                      {"SOME_HEADER": "some_value1",
                                                                       "Some_Header": "some_value2"})))
    res_bundle = message_store.read_message(key)
    assert res_bundle.message.headers["SOME_HEADER"] == "some_value1"
    assert res_bundle.message.headers["Some_Header"] == "some_value2"
