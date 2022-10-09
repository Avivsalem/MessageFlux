from abc import ABCMeta, abstractmethod

from messageflux.iodevices.base import OutputDevice, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle


class OutputTransformerBase(metaclass=ABCMeta):
    """
    transformer for output devices
    """

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
    """
    a wrapper device that transforms outgoing messages
    """

    def __init__(self,
                 manager: 'TransformerOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 transformer: OutputTransformerBase):
        """

        :param manager: the device manager
        :param name: the name of the device
        :param inner_device: the inner device that this device wraps
        :param transformer: the output transformer to use
        """
        super().__init__(manager, name)
        self._transformer = transformer
        self._inner_device = inner_device

    def _send_message(self, message_bundle: MessageBundle):
        message_bundle = self._transformer.transform_outgoing_message(self, message_bundle)
        self._inner_device.send_message(message_bundle.message, message_bundle.device_headers)

    def close(self):
        """
        closes the inner device
        """
        self._inner_device.close()


class TransformerOutputDeviceManager(OutputDeviceManager[TransformerOutputDevice]):
    """
    a wrapper output device manager, that wraps the devices in transformer output devices
    """

    def __init__(self, inner_device_manager: OutputDeviceManager, transformer: OutputTransformerBase):
        """

        :param inner_device_manager: the inner device manager
        :param transformer: the transformer to use
        """
        self._inner_device_manager = inner_device_manager
        self._transformer = transformer

    def connect(self):
        """
        connects the inner device manager and the transformer
        """
        self._transformer.connect()
        self._inner_device_manager.connect()

    def disconnect(self):
        """
        disconnects the inner device manager and the transformer
        """
        self._inner_device_manager.disconnect()
        self._transformer.disconnect()

    def get_output_device(self, name: str) -> TransformerOutputDevice:
        """
        returns a wrapped output device
        :param name: the name of the device to get
        """
        inner_device = self._inner_device_manager.get_output_device(name)
        return TransformerOutputDevice(self, name, inner_device, self._transformer)
