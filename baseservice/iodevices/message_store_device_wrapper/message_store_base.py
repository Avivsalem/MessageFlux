from abc import ABCMeta, abstractmethod
from typing import Tuple

from baseservice.iodevices.base import Message, DeviceHeaders, InputTransaction


class MessageStoreBase(metaclass=ABCMeta):
    @abstractmethod
    def store_message(self,
                      message: Message,
                      device_headers: DeviceHeaders) -> Tuple[Message, DeviceHeaders]:
        """
        Store the message in the message store.

        :param message: the message to store
        :param device_headers: the device headers
        :return: the key message to send through the underlying device, and possibly modified device headers
        """
        pass

    @abstractmethod
    def get_message(self,
                    message: Message,
                    device_headers: DeviceHeaders,
                    transaction: InputTransaction) -> Tuple[Message, DeviceHeaders, InputTransaction]:
        """
        Get the message from the message store.

        if the message is not a valid key message, then original message should be returned.

        :param message: the key_message received from the underlying device
        :param device_headers: the device headers received from the underlying device
        :param transaction: the transaction received from the underlying device
        :return: the message from the message store, possibly modified device headers, and possibly modified transaction
        """
        pass
