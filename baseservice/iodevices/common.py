from typing import BinaryIO, Dict, Any


class Message:
    """
    this class is the basic unit that is read from, or sent to devices.
    """

    def __init__(self, stream: BinaryIO, headers: Dict[str, Any] = None):
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
    def headers(self) -> Dict[str, Any]:
        return self._headers
