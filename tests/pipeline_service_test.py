from io import BytesIO
from threading import Thread
from time import sleep
from typing import Union, Tuple

from baseservice.iodevices.base import InputDevice, Message, DeviceHeaders
from baseservice.iodevices.in_memory_device import InMemoryDeviceManager
from baseservice.pipeline_service import PipelineHandlerBase, PipelineService


class TestPipelineHandler(PipelineHandlerBase):
    def handle_message(self, input_device: InputDevice,
                       message: Message,
                       device_headers: DeviceHeaders) -> Union[Tuple[None, None, None],
                                                               Tuple[str, Message, DeviceHeaders]]:
        output_device_name = message.stream.read().decode()
        message.stream.seek(0)
        return output_device_name, message, device_headers


def test_sanity():
    input_device_name = 'test_input_device'
    input_device_manager = InMemoryDeviceManager()
    input_device = input_device_manager.get_output_device(input_device_name)
    input_device.send_message(Message(BytesIO(b'output_device1')))
    input_device.send_message(Message(BytesIO(b'output_device2')))
    input_device.send_message(Message(BytesIO(b'output_device3')))
    output_device_manager = InMemoryDeviceManager()
    service = PipelineService(input_device_manager=input_device_manager,
                              input_device_names=[input_device_name],
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestPipelineHandler())

    try:
        Thread(target=service.start).start()
        sleep(1)
        message, _, _ = output_device_manager.get_input_device('output_device1').read_message(with_transaction=False)
        assert message is not None
        assert message.stream.read() == b'output_device1'

        message, _, _ = output_device_manager.get_input_device('output_device2').read_message(with_transaction=False)
        assert message is not None
        assert message.stream.read() == b'output_device2'

        message, _, _ = output_device_manager.get_input_device('output_device3').read_message(with_transaction=False)
        assert message is not None
        assert message.stream.read() == b'output_device3'
    finally:
        service.stop()