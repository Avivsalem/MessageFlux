from abc import ABCMeta, abstractmethod

from baseservice.iodevices.base import ReadResult
from baseservice.iodevices.base.common import MessageBundle


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
    def transform_outgoing_message(self, message_bundle: MessageBundle) -> MessageBundle:
        """
        Transform the message before it is sent to the underlying device.

        :param message_bundle: the message bundle to transform
        :return: the transformed message bundle to send through the underlying device
        """
        pass

    @abstractmethod
    def transform_incoming_message(self, read_result: ReadResult) -> ReadResult:
        """
        Transform the message that was received from the underlying device.


        :param read_result: the original ReadResult received from the underlying device
        :return: the transformed ReadResult
        """
        pass
