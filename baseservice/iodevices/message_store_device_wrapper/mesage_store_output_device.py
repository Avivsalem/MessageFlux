from baseservice.iodevices.base import OutputDevice, OutputDeviceManager, Message, DeviceHeaders
from baseservice.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase


class MessageStoreOutputDevice(OutputDevice):
    def __init__(self,
                 manager: 'MessageStoreOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 message_store: MessageStoreBase):
        super().__init__(manager, name)
        self._message_store = message_store
        self._inner_device = inner_device

    def _send_message(self, message: Message, device_headers: DeviceHeaders):
        new_message, new_device_headers = self._message_store.store_message(message, device_headers)
        self._inner_device.send_message(new_message, new_device_headers)


class MessageStoreOutputDeviceManager(OutputDeviceManager):
    def __init__(self, inner_device_manager: OutputDeviceManager, message_store: MessageStoreBase):
        self._inner_device_manager = inner_device_manager
        self._message_store = message_store

    def connect(self):
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()

    def get_output_device(self, name: str) -> OutputDevice:
        inner_device = self._inner_device_manager.get_output_device(name)
        return MessageStoreOutputDevice(self, name, inner_device, self._message_store)

