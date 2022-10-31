from abc import ABCMeta, abstractmethod
from typing import List

from messageflux.iodevices.base.common import MessageBundle
from messageflux.utils import AggregatedException


class MessageStoreException(AggregatedException):
    """
    base exception for message store exceptions
    """
    pass


class MessageStoreBase(metaclass=ABCMeta):
    """
    base class for a Message Store
    """

    @property
    @abstractmethod
    def magic(self) -> bytes:
        """
        return a magic prefix that is unique and constant for this message store
        """
        pass

    def connect(self):
        """
        connects to Message Store

        It is expected, that the implementation stays connected to the message store since 'connect' is called, and
        up until 'close' is called.

        if the for some reason the connection terminates, the implementation must try to re-connect silently upon
        operation
        """
        pass

    def disconnect(self):
        """
        closes the connection to Message Store
        """
        pass

    def __enter__(self) -> 'MessageStoreBase':
        """
        enters the context for this Message Store

        :return: self
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        exits the context for this Message Store (calls close)

        :param exc_type:
        :param exc_val:
        :param exc_tb:
        """
        self.disconnect()

    @abstractmethod
    def read_message(self, key: str) -> MessageBundle:
        """
        reads a message according to the key given

        :param str key: the key to the message
        :return: a Message from the store
        """
        pass

    @abstractmethod
    def put_message(self, device_name: str, message_bundle: MessageBundle) -> str:
        """
        puts a message in the message store

        :param device_name: the name of the device putting the item in the store
        :param message_bundle: the Message bundle to write to the store
        :return: the key to the message in the message store
        """
        pass

    @abstractmethod
    def delete_message(self, key: str):
        """
        deletes a message from the message store

        :param str key: the key to the message
        """
        pass

    def delete_messages(self, keys: List[str]):
        """
        deletes multiple messages from the message store

        :param list[str] keys: the list of keys to the messages
        """
        failures = []

        for key in keys:
            try:
                self.delete_message(key)
            except Exception as ex:
                failures.append(ex)

        if failures:
            raise MessageStoreException(
                "Error deleting {} out of {} messages from store".format(len(failures), len(keys)),
                inner_exceptions=failures)
