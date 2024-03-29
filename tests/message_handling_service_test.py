import logging
import sys
import threading
import time
from threading import Thread
from typing import List, Optional, Tuple

import requests

from messageflux import (InputDevice,
                         ReadResult,
                         MessageHandlingService,
                         MessageHandlerBase,
                         BatchMessageHandlerBase,
                         BatchMessageHandlingService)
from messageflux.base_service import ServiceState
from messageflux.iodevices.base import Message, InputDeviceManager
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager
from messageflux.service_addons.loop_health_addon import LoopHealthAddon
from messageflux.service_addons.webserver_liveness_addon import WebServerLivenessAddon


class MockInputDevice(InputDevice['MockInputDeviceManager']):

    def __init__(self, manager: 'MockInputDeviceManager', name, input_list: List[str]):
        super(MockInputDevice, self).__init__(manager, name)
        self.input_list = input_list

    def _read_message(self, cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:
        try:
            return ReadResult(Message(self.input_list.pop().encode()), transaction=NULLTransaction(self))
        except IndexError:
            return None


class MockInputDeviceManager(InputDeviceManager[MockInputDevice]):
    def __init__(self, input_list, **kwargs):
        super().__init__(**kwargs)
        self.input_list = input_list

    def _create_input_device(self, name):
        return MockInputDevice(self, name, self.input_list)


class IncreaserMessageHandler(MessageHandlerBase):
    STOPPED = "STOPPED"

    def __init__(self, output_list):
        self.output_list = output_list

    def handle_message(self, input_device: InputDevice, read_result: ReadResult):
        self.output_list.append(int(read_result.message.bytes) + 1)


def test_sanity():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_queue = ["3", "2", "1"]
    input_device_manager = MockInputDeviceManager(input_queue)
    output_queue: List[int] = []
    message_handler = IncreaserMessageHandler(output_list=output_queue)
    service = MessageHandlingService(message_handler=message_handler,
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


class MyBatchHandler(BatchMessageHandlerBase):
    def __init__(self):
        self.batches = []
        self.batch_was_read = threading.Event()
        self.batch_was_read.clear()

    def handle_message_batch(self, batch: List[Tuple[InputDevice, ReadResult]]):
        self.batches.append(batch)
        self.batch_was_read.set()


def test_batch_count():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_device_manager = InMemoryDeviceManager()
    output_device = input_device_manager.get_output_device('bla')
    output_device.send_message(Message(b'1'))
    output_device.send_message(Message(b'2'))
    output_device.send_message(Message(b'3'))
    batch_handler = MyBatchHandler()
    service = BatchMessageHandlingService(batch_handler=batch_handler,
                                          input_device_manager=input_device_manager,
                                          input_device_names=['bla'],
                                          max_batch_read_count=2)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        batch_handler.batch_was_read.wait(3)
        time.sleep(1)
        assert len(batch_handler.batches) == 2
        assert len(batch_handler.batches[0]) == 2
        assert len(batch_handler.batches[1]) == 1

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
    batch_handler = MyBatchHandler()
    service = BatchMessageHandlingService(batch_handler=batch_handler,
                                          input_device_manager=input_device_manager,
                                          input_device_names=['bla'],
                                          read_timeout=10,
                                          wait_for_batch_count=False,
                                          max_batch_read_count=3)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        batch_handler.batch_was_read.wait(3)
        assert len(batch_handler.batches) == 1
        batch_handler.batch_was_read.clear()
        output_device.send_message(Message(b'3'))
        batch_handler.batch_was_read.wait(2)

        assert len(batch_handler.batches) == 2
        assert len(batch_handler.batches[0]) == 2
        assert len(batch_handler.batches[1]) == 1

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
    batch_handler = MyBatchHandler()
    service = BatchMessageHandlingService(batch_handler=batch_handler,
                                          input_device_manager=input_device_manager,
                                          input_device_names=['bla'],
                                          read_timeout=5,
                                          wait_for_batch_count=True,
                                          max_batch_read_count=3)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    try:
        batch_handler.batch_was_read.wait(2)
        batch_handler.batch_was_read.clear()
        output_device.send_message(Message(b'3'))
        batch_handler.batch_was_read.wait(3)

        assert len(batch_handler.batches) == 1
        assert len(batch_handler.batches[0]) == 3

    finally:
        service.stop()
        logging.getLogger().removeHandler(stream_handler)


class ErrorMessageHandler(MessageHandlerBase):
    def __init__(self, error_after_count: int):
        self.handle_was_called = threading.Event()
        self.handle_was_called.clear()
        self._error_after_count = error_after_count
        self._count = 0

    def handle_message(self, input_device: InputDevice, read_result: ReadResult):
        self.handle_was_called.set()
        self._count += 1
        if self._count >= self._error_after_count:
            raise Exception()


def test_fatal():
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)
    service_stopping = threading.Event()
    service_stopping.clear()

    def _on_service_state_change(service_state: ServiceState):
        if service_state == ServiceState.STOPPING:
            service_stopping.set()

    input_queue = ["3", "2", "1"]
    input_device_manager = MockInputDeviceManager(input_queue)
    error_handler = ErrorMessageHandler(error_after_count=1)
    service = MessageHandlingService(message_handler=error_handler,
                                     input_device_manager=input_device_manager,
                                     input_device_names=['bla'])
    service.state_changed_event.subscribe(_on_service_state_change)

    loop_addon = LoopHealthAddon(max_consecutive_failures=1).attach(service)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()
    error_handler.handle_was_called.wait(10)
    service_stopping.wait(10)
    try:
        assert not service.is_alive
    finally:
        service.stop()
        loop_addon.detach()
        logging.getLogger().removeHandler(stream_handler)


def test_not_fatal():
    port = 18080
    stream_handler = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(stream_handler)

    input_queue = ["3", "2", "1"]
    input_device_manager = MockInputDeviceManager(input_queue)
    error_handler = ErrorMessageHandler(error_after_count=2)
    service = MessageHandlingService(message_handler=error_handler,
                                     input_device_manager=input_device_manager,
                                     input_device_names=['bla'])
    addon = LoopHealthAddon(max_consecutive_failures=3).attach(service)
    webserver_addon = WebServerLivenessAddon(host="127.0.0.1", port=port).attach(service)

    service_thread = Thread(target=service.start, daemon=True)
    service_thread.start()

    time.sleep(3)
    try:
        assert service.is_alive
        assert requests.get(f"http://127.0.0.1:{port}/").ok
    finally:
        service.stop()
        addon.detach()
        webserver_addon.detach()
        logging.getLogger().removeHandler(stream_handler)
