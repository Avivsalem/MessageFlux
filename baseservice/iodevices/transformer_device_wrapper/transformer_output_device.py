from baseservice.iodevices.base import OutputDevice, OutputDeviceManager, Message, DeviceHeaders
from baseservice.iodevices.transformer_device_wrapper.transformer_base import TransformerBase


class TransformerOutputDevice(OutputDevice):
    def __init__(self,
                 manager: 'TransformerOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 transformer: TransformerBase):
        super().__init__(manager, name)
        self._transformer = transformer
        self._inner_device = inner_device

    def _send_message(self, message: Message, device_headers: DeviceHeaders):
        new_message, new_device_headers = self._transformer.transform_outgoing_message(message, device_headers)
        self._inner_device.send_message(new_message, new_device_headers)


class TransformerOutputDeviceManager(OutputDeviceManager):
    def __init__(self, inner_device_manager: OutputDeviceManager, transformer: TransformerBase):
        self._inner_device_manager = inner_device_manager
        self._transformer = transformer

    def connect(self):
        self._transformer.connect()
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()
        self._transformer.disconnect()

    def get_output_device(self, name: str) -> OutputDevice:
        inner_device = self._inner_device_manager.get_output_device(name)
        return TransformerOutputDevice(self, name, inner_device, self._transformer)

