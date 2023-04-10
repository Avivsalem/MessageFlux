from threading import Event
from threading import Thread
from typing import Optional

from pydantic import BaseModel

from messageflux import InputDevice
from messageflux.iodevices.base import Message
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager
from messageflux.pipeline_service import PipelineService
from messageflux.pydantic_handler import PydanticPipelineHandler, PydanticPipelineResult


class TestInputModel(BaseModel):
    x: int
    y: str


class TestOutputModel(BaseModel):
    a: int
    b: str


class TestPydanticHandler(PydanticPipelineHandler):
    def handle_model(self, input_device: InputDevice, model: TestInputModel) -> Optional[PydanticPipelineResult]:
        return PydanticPipelineResult(output_device_name='test_device', model=TestOutputModel(a=model.x, b=model.y))


def test_sanity():
    input_device_name = 'test_input_device'
    input_model = TestInputModel(x=3, y='test test test')
    input_bytes = input_model.json().encode()
    input_device_manager = InMemoryDeviceManager()
    input_device = input_device_manager.get_output_device(input_device_name)
    input_device.send_message(Message(input_bytes))
    output_device_manager = InMemoryDeviceManager()
    service = PipelineService(input_device_manager=input_device_manager,
                              input_device_names=[input_device_name],
                              max_batch_read_count=3,
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestPydanticHandler())
    loop_ended = Event()
    service.loop_ended_event.subscribe(lambda x: loop_ended.set())
    try:
        Thread(target=service.start, daemon=True).start()
        loop_ended.wait(3)
        read_result = output_device_manager.get_input_device('test_device').read_message(timeout=3,
                                                                                         with_transaction=False)
        assert read_result is not None
        output_model: TestOutputModel = TestOutputModel.parse_raw(read_result.message.bytes)
        assert output_model.a == input_model.x
        assert output_model.b == input_model.y

    finally:
        service.stop()
