import zlib
from typing import Tuple

from baseservice.iodevices.base import Message, DeviceHeaders, InputTransaction
from baseservice.iodevices.transformer_device_wrapper import TransformerBase


class ZLIBTransformer(TransformerBase):
    """
    This class uses zlib to compress data before sending it to the underlying device, and decompress data coming out of
    the underlying device, if necessary.
    """
    ZLIB_TRANSFORMER_MAGIC = b'__ZLIBTRANSFORMER__'

    def __init__(self, level: int = -1):
        self._level = level
        if not (-1 <= self._level <= 9):
            raise ValueError("ZLIBTransformer: level must be between -1 and 9")

    def transform_outgoing_message(self,
                                   message: Message,
                                   device_headers: DeviceHeaders) -> Tuple[Message,
                                                                           DeviceHeaders]:
        compressed_data = self.ZLIB_TRANSFORMER_MAGIC + zlib.compress(message.bytes, level=self._level)
        return Message(compressed_data, message.headers), device_headers

    def transform_incoming_message(self,
                                   message: Message,
                                   device_headers: DeviceHeaders,
                                   transaction: InputTransaction) -> Tuple[Message,
                                                                           DeviceHeaders,
                                                                           InputTransaction]:
        if message.stream.read(len(self.ZLIB_TRANSFORMER_MAGIC)) != self.ZLIB_TRANSFORMER_MAGIC:
            # This is not a zlib-compressed message
            message.stream.seek(0)
            return message, device_headers, transaction

        decompressed_data = zlib.decompress(message.stream.read())
        return Message(decompressed_data, message.headers), device_headers, transaction
