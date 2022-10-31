from io import BytesIO

import boto3
from moto import mock_s3

from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.objectstorage import S3MessageStore, BucketNameFormatterBase


@mock_s3
def test_headers():
    from messageflux.iodevices.objectstorage.s3api.s3bucket import S3Bucket
    s3_resource = boto3.Session().resource('s3')
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
