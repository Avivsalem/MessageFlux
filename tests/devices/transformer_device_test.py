import uuid

from messageflux.iodevices.base import Message, ReadResult
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager
from messageflux.iodevices.transformer_device_wrapper import TransformerInputDeviceManager, \
    TransformerOutputDeviceManager, InputTransformerBase, OutputTransformerBase
from messageflux.iodevices.transformer_device_wrapper.transformer_input_device import TransformerInputDevice
from messageflux.iodevices.transformer_device_wrapper.transformer_output_device import TransformerOutputDevice
from messageflux.iodevices.transformer_device_wrapper.zlib_transformer import ZLIBTransformer


class MockMessageStoreTransformer(InputTransformerBase, OutputTransformerBase):
    def __init__(self):
        self.messages = {}

    def transform_outgoing_message(self, output_device: TransformerOutputDevice,
                                   message_bundle: MessageBundle) -> MessageBundle:
        key = uuid.uuid4().bytes
        self.messages[key] = message_bundle.message
        return MessageBundle(Message(key), message_bundle.device_headers)

    def transform_incoming_message(self, input_device: TransformerInputDevice, read_result: ReadResult) -> ReadResult:
        key = read_result.message.bytes
        return ReadResult(self.messages[key], read_result.device_headers, read_result.transaction)


def test_sanity():
    memory_device_manager = InMemoryDeviceManager()
    message_store_transformer = MockMessageStoreTransformer()
    input_device_manager = TransformerInputDeviceManager(memory_device_manager, message_store_transformer)
    output_device_manager = TransformerOutputDeviceManager(memory_device_manager, message_store_transformer)

    test_device_name = 'test_device'
    output_device = output_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(b'hello1'))
    assert len(message_store_transformer.messages) == 1
    output_device.send_message(Message(b'hello2'))
    assert len(message_store_transformer.messages) == 2

    input_device = input_device_manager.get_input_device(test_device_name)
    read_result = input_device.read_message(with_transaction=False)
    assert read_result is not None
    assert read_result.message.bytes == b'hello1'

    read_result = input_device.read_message(with_transaction=False)
    assert read_result is not None
    assert read_result.message.bytes == b'hello2'


def test_zlib():
    memory_device_manager = InMemoryDeviceManager()
    zlib_transformer = ZLIBTransformer()
    zlib_input_device_manager = TransformerInputDeviceManager(memory_device_manager, zlib_transformer)
    zlib_output_device_manager = TransformerOutputDeviceManager(memory_device_manager, zlib_transformer)

    test_device_name = 'test_device'
    output_device = memory_device_manager.get_output_device(test_device_name)
    output_device.send_message(Message(b'hello1'))

    output_device1 = zlib_output_device_manager.get_output_device(test_device_name)
    output_device1.send_message(Message(b'hello2'))

    input_device = zlib_input_device_manager.get_input_device(test_device_name)
    read_result = input_device.read_message(with_transaction=False)
    assert read_result is not None
    assert read_result.message.bytes == b'hello1'

    read_result = input_device.read_message(with_transaction=False)
    assert read_result is not None
    assert read_result.message.bytes == b'hello2'

    output_device1 = zlib_output_device_manager.get_output_device(test_device_name)
    output_device1.send_message(Message(b'X' * 1000))

    input_device1 = memory_device_manager.get_input_device(test_device_name)
    read_result = input_device1.read_message(with_transaction=True)
    assert read_result is not None
    assert len(read_result.message.bytes) < 1000
    read_result.rollback()

    input_device = zlib_input_device_manager.get_input_device(test_device_name)
    read_result = input_device.read_message(with_transaction=False)
    assert read_result is not None
    assert read_result.message.bytes == b'X' * 1000
