import logging
from typing import BinaryIO

from messageflux.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase


class _MessageStoreTransformerBase:
    """
    this is a base class for message store transformer
    """
    MAGIC_HEADER = b"__MSGSTORE_WRAPPER__"

    def __init__(self, message_store: MessageStoreBase):
        """
        :param message_store: the message store to use
        """
        self._message_store = message_store
        self._logger = logging.getLogger(__name__)
        self._full_magic = b"|".join([self.MAGIC_HEADER,
                                      self._message_store.magic
                                      ])

    def __enter__(self) -> '_MessageStoreTransformerBase':
        self._message_store.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._message_store.__exit__(exc_type, exc_val, exc_tb)

    def connect(self):
        """
        connects to device message store
        """
        self._message_store.connect()

    def disconnect(self):
        """
        disconnects from the message store
        """
        self._message_store.disconnect()

    def serialize_key(self, key: str) -> bytes:
        """
        serializes the key for sending on the wire

        :param str key: the key
        :return: serialized key to send
        """
        return self._full_magic + key.encode()

    def deserialize_key(self, data: bytes) -> str:
        """
        deserializes the key received from the wire

        :param bytes data: the data from the wire
        :return: deserialized key
        """
        key = data[len(self._full_magic):]
        return key.decode()

    def is_key(self, stream: BinaryIO) -> bool:
        """
        checks if the data from the wire, is a serialized key

        :param stream: the data from the wire
        :return: True if it is a serialized key
        """
        magic = stream.read(len(self._full_magic))
        stream.seek(0)
        return magic == self._full_magic
