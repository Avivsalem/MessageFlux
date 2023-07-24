import threading
from abc import ABCMeta, abstractmethod
from typing import Optional

from messageflux.iodevices.base import InputDevice, ReadResult, InputDeviceManager


class InputTransformerBase(metaclass=ABCMeta):
    """
    transformer for input devices
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
    def transform_incoming_message(self,
                                   input_device: 'TransformerInputDevice',
                                   read_result: ReadResult) -> ReadResult:
        """
        Transform the message that was received from the underlying device.

        :param input_device: the input device that the transformer runs on
        :param read_result: the original ReadResult received from the underlying device
        :return: the transformed ReadResult
        """
        pass


class TransformerInputDevice(InputDevice['TransformerInputDeviceManager']):
    """
    an input device that transforms the input
    """

    def __init__(self,
                 manager: 'TransformerInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 transformer: InputTransformerBase):
        """

        :param manager: the input device manager
        :param name: the name of this device
        :param inner_device: the inner device that it wraps
        :param transformer: the transformer to use to transform the incoming messages
        """
        super(TransformerInputDevice, self).__init__(manager, name)
        self._transformer = transformer
        self._inner_device = inner_device

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:
        read_result = self._inner_device.read_message(cancellation_token=cancellation_token,
                                                      timeout=timeout,
                                                      with_transaction=with_transaction)
        if read_result is not None:
            read_result = self._transformer.transform_incoming_message(self, read_result)

        return read_result

    def close(self):
        """
        closes the inner device
        """
        self._inner_device.close()


class TransformerInputDeviceManager(InputDeviceManager[TransformerInputDevice]):
    """
    a wrapper input device manager, that wraps the devices in transformer input devices
    """

    def __init__(self, inner_device_manager: InputDeviceManager, transformer: InputTransformerBase):
        """

        :param inner_device_manager: the inner device manager
        :param transformer: the input transformer to use
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

    def get_input_device(self, name: str) -> TransformerInputDevice:
        """
        returns a wrapped input device

        :param name: the name of the input device to get
        """

        inner_input_device = self._inner_device_manager.get_input_device(name)
        return TransformerInputDevice(self, name, inner_input_device, self._transformer)
