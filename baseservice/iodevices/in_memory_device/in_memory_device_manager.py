import heapq
from functools import total_ordering
from threading import Condition
from typing import Optional, Dict, List

import time

from baseservice.iodevices.base import (Message,
                                        InputDeviceManager,
                                        OutputDeviceManager,
                                        OutputDevice,
                                        InputDevice,
                                        InputTransaction,
                                        NULL_TRANSACTION,
                                        ReadResult)
from baseservice.iodevices.base.common import MessageBundle

MESSAGE_TIMESTAMP_HEADER = 'message_timestamp'


class InMemoryDevice(InputDevice, OutputDevice):
    @total_ordering
    class _QueueMessage:
        def __init__(self, message: Message, timestamp: Optional[float] = None):
            self.message = message.copy()
            self.timestamp = timestamp or time.time()

        def __eq__(self, other):
            return self.timestamp == other.timestamp

        def __lt__(self, other):
            return self.timestamp < other.timestamp

    class InMemoryTransaction(InputTransaction):
        def __init__(self, device: 'InMemoryDevice', message: 'InMemoryDevice._QueueMessage'):
            super().__init__(device)
            self._message = message
            self._device = device

        def _commit(self):
            pass

        def _rollback(self):
            self._device._push_to_queue(self._message)

    def __init__(self, manager: 'InMemoryDeviceManager', name: str):
        InputDevice.__init__(self, manager, name)
        OutputDevice.__init__(self, manager, name)
        self._queue: List[InMemoryDevice._QueueMessage] = []
        self._queue_not_empty: Condition = Condition()

    def _read_message(self, timeout: Optional[float] = 0, with_transaction: bool = True) -> Optional[ReadResult]:
        with self._queue_not_empty:
            if self._queue_not_empty.wait_for(lambda: any(self._queue), timeout):
                message = heapq.heappop(self._queue)
                transaction = self.InMemoryTransaction(self, message) if with_transaction else NULL_TRANSACTION
                device_headers = {MESSAGE_TIMESTAMP_HEADER: message.timestamp}
                return ReadResult(message=message.message.copy(),
                                  device_headers=device_headers,
                                  transaction=transaction)
            else:
                return None

    def _push_to_queue(self, message: 'InMemoryDevice._QueueMessage'):
        with self._queue_not_empty:
            heapq.heappush(self._queue, message)
            self._queue_not_empty.notify()

    def _send_message(self, message_bundle: MessageBundle):
        self._push_to_queue(InMemoryDevice._QueueMessage(message_bundle.message))


class InMemoryDeviceManager(InputDeviceManager, OutputDeviceManager):
    def __init__(self):
        self._queues: Dict[str, InMemoryDevice] = {}

    def get_input_device(self, name: str) -> InputDevice:
        return self._queues.setdefault(name, InMemoryDevice(self, name))

    def get_output_device(self, name: str) -> OutputDevice:
        return self._queues.setdefault(name, InMemoryDevice(self, name))
