import copy
from typing import BinaryIO, Dict, Any, Optional

MessageHeaders = Dict[str, Any]  # this is the type for message metadata


class Message:
    """
    this class is the basic unit that is read from, or sent to devices.
    """

    def __init__(self, stream: BinaryIO, headers: Optional[MessageHeaders] = None):
        """
        :param stream: the stream containing the body of the message
        :param headers: (optional) headers containing metadata about the message
        """
        self._stream = stream
        self._headers = headers or {}

    @property
    def stream(self) -> BinaryIO:
        return self._stream

    @property
    def headers(self) -> MessageHeaders:
        return self._headers

    def copy(self):
        return copy.copy(self)

    def __eq__(self, other):
        if not isinstance(other, Message):
            return False

        my_data = self._stream.read()
        self._stream.seek(0)
        other_data = other._stream.read()
        other._stream.seek(0)
        return my_data == other_data and self._headers == other._headers

    def __copy__(self):
        stream_copy = copy.copy(self._stream)
        stream_copy.seek(0)
        return Message(stream_copy, self._headers.copy())

    def __deepcopy__(self, memo=None):
        stream_copy = copy.deepcopy(self._stream, memo)
        stream_copy.seek(0)
        return Message(stream_copy, copy.deepcopy(self._headers, memo))


DeviceHeaders = Dict[str, Any]  # this is the type for device specific headers, used to pass arguments to/from device
