import uuid
from io import BytesIO
from typing import Tuple

from baseservice.iodevices.base import Message, DeviceHeaders, InputTransaction
from baseservice.iodevices.in_memory_device import InMemoryDeviceManager
from baseservice.iodevices.message_store_device_wrapper import MessageStoreBase, MessageStoreInputDeviceManager, \
    MessageStoreOutputDeviceManager


class TestMessageStore(MessageStoreBase):
    def __init__(self):
        self.messages = {}

    def store_message(self,
                      message: Message,
                      device_headers: DeviceHeaders) -> Tuple[Message, DeviceHeaders]:
        key = uuid.uuid4().bytes
        self.messages[key] = message
        return Message(BytesIO(key)), device_headers

    def get_message(self,
                    message: Message,
                    device_headers: DeviceHeaders,
                    transaction: InputTransaction) -> Tuple[Message, DeviceHeaders, InputTransaction]:
        key = message.stream.read()
        return self.messages[key], device_headers, transaction


def test_sanity():
    memory_device_manager = InMemoryDeviceManager()
    message_store = TestMessageStore()
    input_device_manager = MessageStoreInputDeviceManager(memory_device_manager, message_store)
    output_device_manager = MessageStoreOutputDeviceManager(memory_device_manager, message_store)

    test_device_name = 'test_device'
    output_device = output_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(BytesIO(b'hello1')))
    assert len(message_store.messages) == 1
    output_device.send_message(Message(BytesIO(b'hello2')))
    assert len(message_store.messages) == 2

    input_device = input_device_manager.get_input_device(test_device_name)
    message, device_headers, transaction = input_device.read_message(with_transaction=False)
    assert message.stream.read() == b'hello1'

    message, device_headers, transaction = input_device.read_message(with_transaction=False)
    assert message.stream.read() == b'hello2'
