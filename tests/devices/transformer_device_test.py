import uuid
from typing import Tuple

from baseservice.iodevices.base import Message, DeviceHeaders, InputTransaction
from baseservice.iodevices.in_memory_device import InMemoryDeviceManager
from baseservice.iodevices.transformer_device_wrapper import TransformerBase, TransformerInputDeviceManager, \
    TransformerOutputDeviceManager
from baseservice.iodevices.transformer_device_wrapper.zlib_transformer import ZLIBTransformer


class TestMessageStoreTransformer(TransformerBase):
    def __init__(self):
        self.messages = {}

    def transform_outgoing_message(self,
                                   message: Message,
                                   device_headers: DeviceHeaders) -> Tuple[Message, DeviceHeaders]:
        key = uuid.uuid4().bytes
        self.messages[key] = message
        return Message(key), device_headers

    def transform_incoming_message(self,
                                   message: Message,
                                   device_headers: DeviceHeaders,
                                   transaction: InputTransaction) -> Tuple[Message, DeviceHeaders, InputTransaction]:
        key = message.bytes
        return self.messages[key], device_headers, transaction


def test_sanity():
    memory_device_manager = InMemoryDeviceManager()
    message_store_transformer = TestMessageStoreTransformer()
    input_device_manager = TransformerInputDeviceManager(memory_device_manager, message_store_transformer)
    output_device_manager = TransformerOutputDeviceManager(memory_device_manager, message_store_transformer)

    test_device_name = 'test_device'
    output_device = output_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(b'hello1'))
    assert len(message_store_transformer.messages) == 1
    output_device.send_message(Message(b'hello2'))
    assert len(message_store_transformer.messages) == 2

    input_device = input_device_manager.get_input_device(test_device_name)
    message, _, _ = input_device.read_message(with_transaction=False)
    assert message.bytes == b'hello1'

    message, _, _ = input_device.read_message(with_transaction=False)
    assert message.bytes == b'hello2'


def test_zlib():
    memory_device_manager = InMemoryDeviceManager()
    zlib_transformer = ZLIBTransformer()
    zlib_input_device_manager = TransformerInputDeviceManager(memory_device_manager, zlib_transformer)
    zlib_output_device_manager = TransformerOutputDeviceManager(memory_device_manager, zlib_transformer)

    test_device_name = 'test_device'
    output_device = memory_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(b'hello1'))

    output_device = zlib_output_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(b'hello2'))

    input_device = zlib_input_device_manager.get_input_device(test_device_name)
    message, _, _ = input_device.read_message(with_transaction=False)
    assert message.bytes == b'hello1'

    message, _, _ = input_device.read_message(with_transaction=False)
    assert message.bytes == b'hello2'

    output_device = zlib_output_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(b'X' * 1000))

    input_device = memory_device_manager.get_input_device(test_device_name)
    message, _, transaction = input_device.read_message(with_transaction=True)
    assert len(message.bytes) < 1000
    transaction.rollback()

    input_device = zlib_input_device_manager.get_input_device(test_device_name)
    message, _, _ = input_device.read_message(with_transaction=False)
    assert message.bytes == b'X' * 1000
