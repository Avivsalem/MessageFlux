import zlib

from baseservice.iodevices.base import Message, ReadResult
from baseservice.iodevices.base.common import MessageBundle
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

    def transform_outgoing_message(self, message_bundle: MessageBundle) -> MessageBundle:

        compressed_data = self.ZLIB_TRANSFORMER_MAGIC + zlib.compress(message_bundle.message.bytes, level=self._level)
        return MessageBundle(message=Message(compressed_data, message_bundle.message.headers),
                             device_headers=message_bundle.device_headers)

    def transform_incoming_message(self, read_result: ReadResult) -> ReadResult:
        if read_result.message.stream.read(len(self.ZLIB_TRANSFORMER_MAGIC)) != self.ZLIB_TRANSFORMER_MAGIC:
            # This is not a zlib-compressed message
            read_result.message.stream.seek(0)
            return read_result

        decompressed_data = zlib.decompress(read_result.message.stream.read())
        return ReadResult(message=Message(decompressed_data, read_result.message.headers),
                          device_headers=read_result.device_headers,
                          transaction=read_result.transaction)
