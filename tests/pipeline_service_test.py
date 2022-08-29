from io import BytesIO
from threading import Thread, Event
from time import sleep
from typing import Union, Tuple, Optional

from baseservice.iodevices.base import InputDevice, Message, DeviceHeaders
from baseservice.iodevices.base.common import MessageBundle
from baseservice.iodevices.in_memory_device import InMemoryDeviceManager
from baseservice.pipeline_service import PipelineHandlerBase, PipelineService, PipelineResult


class TestPipelineHandler(PipelineHandlerBase):
    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[PipelineResult]:
        output_device_name = message_bundle.message.bytes.decode()
        return PipelineResult(output_device_name, message_bundle)


def test_sanity():
    input_device_name = 'test_input_device'
    input_device_manager = InMemoryDeviceManager()
    input_device = input_device_manager.get_output_device(input_device_name)
    input_device.send_message(Message(b'output_device1'))
    input_device.send_message(Message(b'output_device2'))
    input_device.send_message(Message(b'output_device3'))
    output_device_manager = InMemoryDeviceManager()
    service = PipelineService(input_device_manager=input_device_manager,
                              input_device_names=[input_device_name],
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestPipelineHandler())
    loop_ended = Event()
    service.loop_ended_event.subscribe(lambda x: loop_ended.set())
    try:
        Thread(target=service.start).start()
        loop_ended.wait(3)
        read_result= output_device_manager.get_input_device('output_device1').read_message(with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'output_device1'

        read_result = output_device_manager.get_input_device('output_device2').read_message(with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'output_device2'

        read_result = output_device_manager.get_input_device('output_device3').read_message(with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'output_device3'
    finally:
        service.stop()
