from abc import ABCMeta, abstractmethod
from typing import Tuple

from baseservice.iodevices.base import Message, DeviceHeaders, InputTransaction


class TransformerBase(metaclass=ABCMeta):
    def connect(self):
        """
        this method is called when the transformer device manager is connected
        """
        pass

    def disconnect(self):
        """
        this method is called when the transformer device manager is disconnected
        """
        pass

    @abstractmethod
    def transform_outgoing_message(self,
                                   message: Message,
                                   device_headers: DeviceHeaders) -> Tuple[Message, DeviceHeaders]:
        """
        Transform the message before it is sent to the underlying device.

        :param message: the message to transform
        :param device_headers: the device headers
        :return: the transformed message to send through the underlying device, and possibly modified device headers
        """
        pass

    @abstractmethod
    def transform_incoming_message(self,
                                   message: Message,
                                   device_headers: DeviceHeaders,
                                   transaction: InputTransaction) -> Tuple[Message, DeviceHeaders, InputTransaction]:
        """
        Transform the message that was received from the underlying device.


        :param message: the original message received from the underlying device
        :param device_headers: the device headers received from the underlying device
        :param transaction: the transaction received from the underlying device
        :return: the transformed message, possibly modified device headers, and possibly modified transaction
        """
        pass
