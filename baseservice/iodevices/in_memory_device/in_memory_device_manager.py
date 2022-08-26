import heapq
import time
from functools import total_ordering
from threading import Condition
from typing import Optional, Dict, List, Any

from baseservice.iodevices.base import (Message,
                                        OutputDevice,
                                        InputDevice,
                                        DeviceHeaders,
                                        InputTransaction,
                                        NULL_TRANSACTION,
                                        ReadMessageResult,
                                        EMPTY_RESULT)
from baseservice.iodevices.base.input_output_devices import InputOutputDevice, InputOutputDeviceManager

MESSAGE_TIMESTAMP_HEADER = 'message_timestamp'


class InMemoryDevice(InputOutputDevice):
    @total_ordering
    class _QueueMessage:
        def __init__(self, message: Message, timestamp: Optional[float] = None):
            self.message = message.copy()
            self.timestamp = timestamp or time.time()

        def __eq__(self, other: Any) -> bool:
            if not isinstance(other, InMemoryDevice._QueueMessage):
                return False

            return self.timestamp == other.timestamp

        def __lt__(self, other: 'InMemoryDevice._QueueMessage') -> bool:
            return self.timestamp < other.timestamp

    class InMemoryTransaction(InputTransaction):
        def __init__(self, device: 'InMemoryDevice', message: 'InMemoryDevice._QueueMessage'):
            super().__init__(device)
            self._message = message

        def _commit(self) -> None:
            pass

        def _rollback(self) -> None:
            self._device._push_to_queue(self._message)

    def __init__(self, manager: 'InMemoryDeviceManager', name: str):
        InputDevice.__init__(self, manager, name)
        OutputDevice.__init__(self, manager, name)
        self._queue: List[InMemoryDevice._QueueMessage] = []
        self._queue_not_empty: Condition = Condition()

    def _read_message(self, timeout: Optional[float] = 0, with_transaction: bool = True) -> ReadMessageResult:
        with self._queue_not_empty:
            if self._queue_not_empty.wait_for(lambda: any(self._queue), timeout):
                message = heapq.heappop(self._queue)
                transaction = self.InMemoryTransaction(self, message) if with_transaction else NULL_TRANSACTION
                device_headers = {MESSAGE_TIMESTAMP_HEADER: message.timestamp}
                return message.message.copy(), device_headers, transaction
            else:
                return EMPTY_RESULT

    def _push_to_queue(self, message: 'InMemoryDevice._QueueMessage') -> None:
        with self._queue_not_empty:
            heapq.heappush(self._queue, message)
            self._queue_not_empty.notify()

    def _send_message(self, message: Message, device_headers: DeviceHeaders) -> None:
        self._push_to_queue(InMemoryDevice._QueueMessage(message))


class InMemoryDeviceManager(InputOutputDeviceManager):
    def __init__(self) -> None:
        self._queues: Dict[str, InMemoryDevice] = {}

    def get_input_device(self, name: str) -> InputDevice:
        return self._queues.setdefault(name, InMemoryDevice(self, name))

    def get_output_device(self, name: str) -> OutputDevice:
        return self._queues.setdefault(name, InMemoryDevice(self, name))
