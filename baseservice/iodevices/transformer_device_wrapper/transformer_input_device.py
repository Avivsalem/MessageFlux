from baseservice.iodevices.base import InputDevice, ReadMessageResult,  InputDeviceManager
from baseservice.iodevices.transformer_device_wrapper.transformer_base import TransformerBase


class TransformerInputDevice(InputDevice):
    def __init__(self,
                 manager: 'TransformerInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 transformer: TransformerBase):
        super(TransformerInputDevice, self).__init__(manager, name)
        self._transformer = transformer
        self._inner_device = inner_device

    def _read_message(self, timeout: float = 0, with_transaction: bool = True) -> ReadMessageResult:
        result = self._inner_device.read_message(timeout=timeout, with_transaction=with_transaction)

        if InputDevice.is_non_empty_message_result(result):
            message, headers, transaction = result
            return self._transformer.transform_incoming_message(message, headers, transaction)

        return result


class TransformerInputDeviceManager(InputDeviceManager):
    def __init__(self, inner_device_manager: InputDeviceManager, transformer: TransformerBase):
        self._inner_device_manager = inner_device_manager
        self._transformer = transformer

    def connect(self) -> None:
        self._transformer.connect()
        self._inner_device_manager.connect()

    def disconnect(self) -> None:
        self._inner_device_manager.disconnect()
        self._transformer.disconnect()

    def get_input_device(self, name: str) -> InputDevice:
        inner_input_device = self._inner_device_manager.get_input_device(name)
        return TransformerInputDevice(self, name, inner_input_device, self._transformer)
