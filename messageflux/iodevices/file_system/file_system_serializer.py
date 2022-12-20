import json
import zipfile
from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import BinaryIO

from messageflux.iodevices.base import Message
from messageflux.utils import json_safe_encoder


class FileSystemSerializerBase(metaclass=ABCMeta):
    """
    this is a base class for FileSystemSerializer
    """

    @abstractmethod
    def serialize(self, message: Message) -> BinaryIO:
        """
        serializes the message into a stream to write to file

        :param message: the message to serialize
        :return: a stream containing the serialized message
        """
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self, stream: BinaryIO) -> Message:
        """
        deserializes the message from the stream

        :param stream: the stream to deserialize from
        :return: the deserialized message
        """
        raise NotImplementedError()


class ConcatFileSystemSerializer(FileSystemSerializerBase):
    """
    this serializer concats the headers line with the message bytes
    it serializes the stream and headers into a single file
    """

    def serialize(self, message: Message) -> BinaryIO:
        """
        serializes the message into a stream to write to file

        :param message: the message to serialize
        :return: a stream containing the serialized message
        """
        headers_bytes = json.dumps(message.headers, default=json_safe_encoder).encode()
        result = BytesIO()
        result.write(headers_bytes)
        result.write(b'\n')
        result.write(message.bytes)
        result.seek(0)
        return result

    def deserialize(self, stream: BinaryIO) -> Message:
        """
        deserializes the message from the stream

        :param stream: the stream to deserialize from
        :return: the deserialized message
        """
        first_line = stream.readline()
        rest = stream.read()
        headers = json.loads(first_line.decode())
        data = BytesIO(rest)
        return Message(data, headers)


class NoHeadersFileSystemSerializer(FileSystemSerializerBase):
    """
    this is a serializer for filesystem, that ignores the headers, and just puts the stream as the content of the file
    """

    def serialize(self, message: Message) -> BinaryIO:
        """
        serializes the message stream only

        :param message: the message to serialize
        :return: a stream containing the serialized message
        """
        return message.stream

    def deserialize(self, stream: BinaryIO) -> Message:
        """
        deserializes the stream into the stream of the message

        :param stream: the stream to deserialize from
        :return: the deserialized message
        """
        return Message(stream.read(), {})


class ZIPFileSystemSerializer(FileSystemSerializerBase):
    """
    this serializer, creates a zip file, with two inner files: the buffer, and headers
    """
    HEADERS_FILENAME = 'headers'
    BYTES_FILENAME = 'bytes'

    def serialize(self, message: Message) -> BinaryIO:
        """
        serializes the message into a stream to write to file

        :param message: the message to serialize
        :return: a stream containing the serialized message
        """
        headers_bytes = json.dumps(message.headers, default=json_safe_encoder).encode()
        zip_filebuf = BytesIO()

        with zipfile.ZipFile(zip_filebuf, mode='w') as zip_file:
            zip_file.writestr(self.BYTES_FILENAME, message.bytes)
            zip_file.writestr(self.HEADERS_FILENAME, headers_bytes)
        zip_filebuf.seek(0)
        return zip_filebuf

    def deserialize(self, stream: BinaryIO) -> Message:
        """
        deserializes the message from the stream

        :param stream: the stream to deserialize from
        :return: the deserialized message
        """
        with zipfile.ZipFile(stream, mode='r') as zip_file:
            headers_data = zip_file.read(self.HEADERS_FILENAME)
            bytes_data = zip_file.read(self.BYTES_FILENAME)

        headers = json.loads(headers_data.decode())
        data = BytesIO(bytes_data)
        return Message(data, headers)


DefaultFileSystemSerializer = ZIPFileSystemSerializer
