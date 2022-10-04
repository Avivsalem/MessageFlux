import copy
from io import BytesIO
from typing import BinaryIO, Dict, Any, Optional, Union

MessageHeaders = Dict[str, Any]  # this is the type for message metadata


class Message:
    """
    this class is the basic unit that is read from, or sent to devices.
    """
    __slots__ = '_stream', '_headers'

    def __init__(self, data: Union[BinaryIO, bytes], headers: Optional[MessageHeaders] = None):
        """
        :param data: the bytes/stream containing the body of the message
        :param headers: (optional) headers containing metadata about the message
        """
        if isinstance(data, bytes):
            data = BytesIO(data)

        self._stream = data
        self._headers = headers or {}

    @property
    def stream(self) -> BinaryIO:
        """
        the stream for this message. notice that reading the stream, advances its position
        """
        return self._stream

    @property
    def bytes(self) -> bytes:
        """
        :return: the data of the message
        (reads the stream from current position to the end, than resets the position to the original value)
        """
        current_pos = self._stream.tell()
        data = self._stream.read()
        self._stream.seek(current_pos)
        return data

    @property
    def headers(self) -> MessageHeaders:
        """
        the headers for this message
        """
        return self._headers

    def copy(self, new_headers: Optional[MessageHeaders] = None):
        """
        makes a copy of the message, possibly giving it a new headers
        """
        stream_copy = copy.copy(self._stream)
        stream_copy.seek(0)
        if new_headers is None:
            new_headers = self._headers.copy()

        return Message(stream_copy, new_headers)

    def __eq__(self, other):
        if not isinstance(other, Message):
            return False

        return self.bytes == other.bytes and self._headers == other._headers

    def __copy__(self):
        return self.copy()

    def __deepcopy__(self, memo=None):
        stream_copy = copy.deepcopy(self._stream, memo)
        stream_copy.seek(0)
        return Message(stream_copy, copy.deepcopy(self._headers, memo))


DeviceHeaders = Dict[str, Any]  # this is the type for device specific headers, used to pass arguments to/from device


class MessageBundle:
    """
    this class holds a message and device headers (to get from device, or send to device)
    """
    __slots__ = "_message", "_device_headers"

    def __init__(self, message: Message, device_headers: Optional[DeviceHeaders] = None):
        """
        :param message: The Message.
        :param device_headers: Additional Headers that may return data from device, or affect its operation.
        """
        self._message = message
        self._device_headers = device_headers or {}

    @property
    def message(self) -> Message:
        """
        the message in this bundle
        """
        return self._message

    @property
    def device_headers(self) -> DeviceHeaders:
        """
        Additional Headers that may return data from device, or affect its operation.
        """
        return self._device_headers
