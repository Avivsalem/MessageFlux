from threading import Thread, Event
from typing import Optional, Union, List, Generator

from messageflux.iodevices.base import InputDevice, Message
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager
from messageflux.pipeline_service import PipelineHandlerBase, PipelineService, PipelineResult


class TestPipelineHandler(PipelineHandlerBase):
    def __init__(self, filter_header: bool = False):
        self._filter = filter_header

    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[PipelineResult]:
        if self._filter and message_bundle.message.headers.get('filter'):
            return None
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
                              max_batch_read_count=3,
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestPipelineHandler())
    loop_ended = Event()
    service.loop_ended_event.subscribe(lambda x: loop_ended.set())
    try:
        cancellation_token = Event()
        Thread(target=service.start, daemon=True).start()
        loop_ended.wait(3)
        read_result = output_device_manager.get_input_device('output_device1').read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'output_device1'

        read_result = output_device_manager.get_input_device('output_device2').read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'output_device2'

        read_result = output_device_manager.get_input_device('output_device3').read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'output_device3'
    finally:
        service.stop()


def test_filter():
    input_device_name = 'test_input_device'
    output_device_name = 'test_output_device'
    input_device_manager = InMemoryDeviceManager()
    input_device = input_device_manager.get_output_device(input_device_name)
    input_device.send_message(Message(output_device_name.encode(), headers={'num': 1}))
    input_device.send_message(Message(output_device_name.encode(), headers={'num': 2, 'filter': True}))
    input_device.send_message(Message(output_device_name.encode(), headers={'num': 3}))
    output_device_manager = InMemoryDeviceManager()
    service = PipelineService(input_device_manager=input_device_manager,
                              input_device_names=[input_device_name],
                              max_batch_read_count=3,
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestPipelineHandler(filter_header=True))
    loop_ended = Event()
    service.loop_ended_event.subscribe(lambda x: loop_ended.set())
    try:
        cancellation_token = Event()
        Thread(target=service.start, daemon=True).start()
        loop_ended.wait(3)

        read_result = output_device_manager.get_input_device(output_device_name).read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.headers.get('num') == 1

        read_result = output_device_manager.get_input_device(output_device_name).read_message(
            cancellation_token=cancellation_token, timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.headers.get('num') == 3

    finally:
        service.stop()


def test_multiple():
    input_device_name = 'test_input_device'
    input_device_manager = InMemoryDeviceManager()

    class TestMultipleHandler(PipelineHandlerBase):
        def handle_message(self,
                           input_device: InputDevice,
                           message_bundle: MessageBundle) -> Optional[Union[PipelineResult,
                                                                            List[PipelineResult],
                                                                            Generator[PipelineResult, None, None]]]:
            return [PipelineResult("test1", message_bundle),
                    PipelineResult("test2", message_bundle),
                    PipelineResult("test3", message_bundle)]

    input_device = input_device_manager.get_output_device(input_device_name)
    input_device.send_message(Message(b'123'))
    output_device_manager = InMemoryDeviceManager()
    service = PipelineService(input_device_manager=input_device_manager,
                              input_device_names=[input_device_name],
                              max_batch_read_count=3,
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestMultipleHandler())
    loop_ended = Event()
    service.loop_ended_event.subscribe(lambda x: loop_ended.set())
    try:
        cancellation_token = Event()
        Thread(target=service.start, daemon=True).start()
        loop_ended.wait(3)

        read_result = output_device_manager.get_input_device("test1").read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'123'

        read_result = output_device_manager.get_input_device("test2").read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'123'

        read_result = output_device_manager.get_input_device("test3").read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'123'

    finally:
        service.stop()


def test_generator():
    input_device_name = 'test_input_device'
    input_device_manager = InMemoryDeviceManager()

    class TestMultipleHandler(PipelineHandlerBase):
        def handle_message(self,
                           input_device: InputDevice,
                           message_bundle: MessageBundle) -> Optional[Union[PipelineResult,
                                                                            List[PipelineResult],
                                                                            Generator[PipelineResult, None, None]]]:
            yield PipelineResult("test1", message_bundle)
            yield PipelineResult("test2", message_bundle)
            yield PipelineResult("test3", message_bundle)

    input_device = input_device_manager.get_output_device(input_device_name)
    input_device.send_message(Message(b'123'))
    output_device_manager = InMemoryDeviceManager()
    service = PipelineService(input_device_manager=input_device_manager,
                              input_device_names=[input_device_name],
                              max_batch_read_count=3,
                              output_device_manager=output_device_manager,
                              pipeline_handler=TestMultipleHandler())
    loop_ended = Event()
    service.loop_ended_event.subscribe(lambda x: loop_ended.set())
    try:
        cancellation_token = Event()
        Thread(target=service.start, daemon=True).start()
        loop_ended.wait(3)

        read_result = output_device_manager.get_input_device("test1").read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'123'

        read_result = output_device_manager.get_input_device("test2").read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'123'

        read_result = output_device_manager.get_input_device("test3").read_message(
            cancellation_token=cancellation_token,
            timeout=3,
            with_transaction=False)
        assert read_result is not None
        assert read_result.message.bytes == b'123'

    finally:
        service.stop()
