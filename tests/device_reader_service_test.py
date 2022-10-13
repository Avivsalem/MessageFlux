import logging
import sys
import time
from threading import Thread
from typing import List, Optional, Tuple

from messageflux import InputDevice, ReadResult, SingleMessageDeviceReaderService, DeviceReaderService
from messageflux.iodevices.base import Message, InputDeviceManager
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager
from messageflux.service_addons.loop_health_addon import LoopHealthAddon


class MockInputDevice(InputDevice['MockInputDeviceManager']):

    def __init__(self, manager: 'MockInputDeviceManager', name, input_list: List[str]):
        super(MockInputDevice, self).__init__(manager, name)
        self.input_list = input_list

    def _read_message(self, timeout: Optional[float] = None, with_transaction: bool = True) -> Optional[ReadResult]:
        try:
            return ReadResult(Message(self.input_list.pop().encode()), transaction=NULLTransaction(self))
        except IndexError:
            return None


class MockInputDeviceManager(InputDeviceManager[MockInputDevice]):
    def __init__(self, input_list):
        self.input_list = input_list

    def get_input_device(self, device_name):
        return MockInputDevice(self, device_name, self.input_list)


class IncreaserService(SingleMessageDeviceReaderService):
    STOPPED = "STOPPED"

    def __init__(self, output_list, **kwargs):
        super(IncreaserService, self).__init__(**kwargs)
        self.output_list = output_list

    def _handle_single_message(self, input_device: InputDevice, read_result: ReadResult):
        self.output_list.append(int(read_result.message.bytes) + 1)


def test_sanity():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_queue = ["3", "2", "1"]
    input_device_manager = MockInputDeviceManager(input_queue)
    output_queue: List[int] = []
    service = IncreaserService(output_list=output_queue,
                               input_device_manager=input_device_manager,
                               input_device_names=['bla'])

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        time.sleep(1)

        assert output_queue == [2, 3, 4]
    finally:
        service.stop()
        logging.getLogger().removeHandler(stream_handler)


class BatchService(DeviceReaderService):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.batches = []

    def _handle_messages(self, batch: List[Tuple[InputDevice, ReadResult]]):
        self.batches.append(batch)


def test_batch_count():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_device_manager = InMemoryDeviceManager()
    output_device = input_device_manager.get_output_device('bla')
    output_device.send_message(Message(b'1'))
    output_device.send_message(Message(b'2'))
    output_device.send_message(Message(b'3'))
    service = BatchService(input_device_manager=input_device_manager,
                           input_device_names=['bla'],
                           max_batch_read_count=2)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        time.sleep(1)

        assert len(service.batches) == 2
        assert len(service.batches[0]) == 2
        assert len(service.batches[1]) == 1

    finally:
        service.stop()
        logging.getLogger().removeHandler(stream_handler)


def test_dont_wait_for_batch():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_device_manager = InMemoryDeviceManager()
    output_device = input_device_manager.get_output_device('bla')
    output_device.send_message(Message(b'1'))
    output_device.send_message(Message(b'2'))
    service = BatchService(input_device_manager=input_device_manager,
                           input_device_names=['bla'],
                           read_timeout=5,
                           wait_for_batch_count=False,
                           max_batch_read_count=3)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        time.sleep(1)
        output_device.send_message(Message(b'3'))
        time.sleep(1)

        assert len(service.batches) == 2
        assert len(service.batches[0]) == 2
        assert len(service.batches[1]) == 1

    finally:
        service.stop()
        logging.getLogger().removeHandler(stream_handler)


def test_wait_for_batch():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_device_manager = InMemoryDeviceManager()
    output_device = input_device_manager.get_output_device('bla')
    output_device.send_message(Message(b'1'))
    output_device.send_message(Message(b'2'))
    service = BatchService(input_device_manager=input_device_manager,
                           input_device_names=['bla'],
                           read_timeout=5,
                           wait_for_batch_count=True,
                           max_batch_read_count=3)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        time.sleep(2)
        output_device.send_message(Message(b'3'))
        time.sleep(3)

        assert len(service.batches) == 1
        assert len(service.batches[0]) == 3

    finally:
        service.stop()
        logging.getLogger().removeHandler(stream_handler)


class ErrorService(SingleMessageDeviceReaderService):
    def __init__(self, error_after_count: int, **kwargs):
        super(ErrorService, self).__init__(**kwargs)
        self._error_after_count = error_after_count
        self._count = 0

    def _handle_single_message(self, input_device: InputDevice, read_result: ReadResult):
        self._count += 1
        if self._count >= self._error_after_count:
            raise Exception()


def test_fatal():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_queue = ["3", "2", "1"]
    input_device_manager = MockInputDeviceManager(input_queue)
    service = ErrorService(error_after_count=1,
                           input_device_manager=input_device_manager,
                           input_device_names=['bla'])

    addon = LoopHealthAddon(max_consecutive_failures=1).attach(service)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    time.sleep(3)
    try:
        assert not service.is_alive
    finally:
        service.stop()
        addon.detach()
        logging.getLogger().removeHandler(stream_handler)


def test_not_fatal():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_queue = ["3", "2", "1"]
    input_device_manager = MockInputDeviceManager(input_queue)
    service = ErrorService(error_after_count=2,
                           input_device_manager=input_device_manager,
                           input_device_names=['bla'])
    addon = LoopHealthAddon(max_consecutive_failures=3).attach(service)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    time.sleep(3)
    try:
        assert service.is_alive
    finally:
        service.stop()
        addon.detach()
        logging.getLogger().removeHandler(stream_handler)
