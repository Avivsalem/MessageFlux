import copy
from io import BytesIO
from typing import BinaryIO, Dict, Any, Optional, Union

MessageHeaders = Dict[str, Any]  # this is the type for message metadata


class Message:
    """
    this class is the basic unit that is read from, or sent to devices.
    """

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
        return self._stream

    @property
    def bytes(self) -> bytes:
        """
        :return: the data of the message
        """
        current_pos = self._stream.tell()
        data = self._stream.read()
        self._stream.seek(current_pos)
        return data

    @property
    def headers(self) -> MessageHeaders:
        return self._headers

    def copy(self):
        return copy.copy(self)

    def __eq__(self, other):
        if not isinstance(other, Message):
            return False

        return self.bytes == other.bytes and self._headers == other._headers

    def __copy__(self):
        stream_copy = copy.copy(self._stream)
        stream_copy.seek(0)
        return Message(stream_copy, self._headers.copy())

    def __deepcopy__(self, memo=None):
        stream_copy = copy.deepcopy(self._stream, memo)
        stream_copy.seek(0)
        return Message(stream_copy, copy.deepcopy(self._headers, memo))


DeviceHeaders = Dict[str, Any]  # this is the type for device specific headers, used to pass arguments to/from device
