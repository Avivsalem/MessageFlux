import datetime
from io import BytesIO

from mock import patch

from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.objectstorage import S3MessageStore

mocked_objects = {}


class MockS3Object:
    def __init__(self, key, body, metadata, last_modified):
        self.key = key
        self.last_modified = last_modified
        self.body = body
        self.metadata = metadata


class MockedS3Bucket:
    def __init__(self, *args, **kwargs):
        self.name = "SomeBucketName"

    def put_object(self, key, buf, metadata):
        headers = {header.lower(): value for header, value in metadata.items()}
        s3obj = MockS3Object(key=key, body=buf, metadata=headers, last_modified=datetime.datetime.now())
        mocked_objects[key] = s3obj
        return s3obj

    def get_object(self, key):
        return mocked_objects.pop(key)

    def compute_url(self, key) -> str:
        """
        computes the item url, by key

        :param key: the key to item
        :return: the url to item
        """
        return ""


class MockedS3Client:
    def __init__(self, *args, **kwargs):
        pass


@patch("messageflux.iodevices.objectstorage.s3_message_store.S3Bucket", MockedS3Bucket)
@patch("messageflux.iodevices.objectstorage.s3_message_store.S3Client", MockedS3Client)
def test_headers():
    message_store = S3MessageStore("", "", "")
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
