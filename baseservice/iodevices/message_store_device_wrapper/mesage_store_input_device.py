from typing import Optional

from baseservice.iodevices.base import InputDevice, ReadMessageResult, EMPTY_RESULT, InputDeviceManager
from baseservice.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase


class MessageStoreInputDevice(InputDevice):
    def __init__(self,
                 manager: 'MessageStoreInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 message_store: MessageStoreBase):
        super(MessageStoreInputDevice, self).__init__(manager, name)
        self._message_store = message_store
        self._inner_device = inner_device

    def _read_message(self, timeout: Optional[float] = 0, with_transaction: bool = True) -> ReadMessageResult:
        message, headers, transaction = self._inner_device.read_message(timeout=timeout,
                                                                        with_transaction=with_transaction)
        if message is None:
            return EMPTY_RESULT

        return self._message_store.get_message(message, headers, transaction)


class MessageStoreInputDeviceManager(InputDeviceManager):
    def __init__(self, inner_device_manager: InputDeviceManager, message_store: MessageStoreBase):
        self._inner_device_manager = inner_device_manager
        self._message_store = message_store

    def connect(self):
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()

    def get_input_device(self, name: str) -> InputDevice:
        inner_input_device = self._inner_device_manager.get_input_device(name)
        return MessageStoreInputDevice(self, name, inner_input_device, self._message_store)
