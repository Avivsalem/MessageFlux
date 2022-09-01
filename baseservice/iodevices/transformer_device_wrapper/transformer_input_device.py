from abc import ABCMeta, abstractmethod
from typing import Optional

from baseservice.iodevices.base import InputDevice, ReadResult, InputDeviceManager


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
    def __init__(self,
                 manager: 'TransformerInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 transformer: InputTransformerBase):
        super(TransformerInputDevice, self).__init__(manager, name)
        self._transformer = transformer
        self._inner_device = inner_device

    def _read_message(self, timeout: Optional[float] = None, with_transaction: bool = True) -> Optional[ReadResult]:
        read_result = self._inner_device.read_message(timeout=timeout, with_transaction=with_transaction)
        if read_result is not None:
            read_result = self._transformer.transform_incoming_message(self, read_result)

        return read_result


class TransformerInputDeviceManager(InputDeviceManager[TransformerInputDevice]):
    def __init__(self, inner_device_manager: InputDeviceManager, transformer: InputTransformerBase):
        self._inner_device_manager = inner_device_manager
        self._transformer = transformer

    def connect(self):
        self._transformer.connect()
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()
        self._transformer.disconnect()

    def get_input_device(self, name: str) -> TransformerInputDevice:
        inner_input_device = self._inner_device_manager.get_input_device(name)
        return TransformerInputDevice(self, name, inner_input_device, self._transformer)
