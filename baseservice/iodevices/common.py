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


DeviceHeaders = Dict[str, Any]  # this is the type for device specific headers, used to pass arguments to/from device
