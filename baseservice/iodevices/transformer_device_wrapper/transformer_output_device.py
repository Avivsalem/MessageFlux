from abc import ABCMeta, abstractmethod

from baseservice.iodevices.base import OutputDevice, OutputDeviceManager
from baseservice.iodevices.base.common import MessageBundle


class OutputTransformerBase(metaclass=ABCMeta):

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
                                   output_device: 'TransformerOutputDevice',
                                   message_bundle: MessageBundle) -> MessageBundle:
        """
        Transform the message before it is sent to the underlying device.

        :param output_device: the output device that the transformer runs on
        :param message_bundle: the message bundle to transform
        :return: the transformed message bundle to send through the underlying device
        """
        pass


class TransformerOutputDevice(OutputDevice['TransformerOutputDeviceManager']):
    def __init__(self,
                 manager: 'TransformerOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 transformer: OutputTransformerBase):
        super().__init__(manager, name)
        self._transformer = transformer
        self._inner_device = inner_device

    def _send_message(self, message_bundle: MessageBundle):
        message_bundle = self._transformer.transform_outgoing_message(self, message_bundle)
        self._inner_device.send_message(message_bundle.message, message_bundle.device_headers)


class TransformerOutputDeviceManager(OutputDeviceManager[TransformerOutputDevice]):
    def __init__(self, inner_device_manager: OutputDeviceManager, transformer: OutputTransformerBase):
        self._inner_device_manager = inner_device_manager
        self._transformer = transformer

    def connect(self):
        self._transformer.connect()
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()
        self._transformer.disconnect()

    def get_output_device(self, name: str) -> TransformerOutputDevice:
        inner_device = self._inner_device_manager.get_output_device(name)
        return TransformerOutputDevice(self, name, inner_device, self._transformer)
