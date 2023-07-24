import heapq
import threading
import time
from functools import total_ordering
from threading import Condition
from typing import Optional, Dict, List, Tuple

from messageflux.iodevices.base import (Message,
                                        InputDeviceManager,
                                        OutputDeviceManager,
                                        OutputDevice,
                                        InputDevice,
                                        InputTransaction,
                                        ReadResult)
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.base.input_transaction import NULLTransaction

MESSAGE_TIMESTAMP_HEADER = 'message_timestamp'


@total_ordering
class _QueueMessage:
    """
    an object containing a message in the queue
    """

    def __init__(self, message: Message, timestamp: Optional[float] = None):
        self.message = message.copy()
        self.timestamp = timestamp or time.time()
        self._monotonic_time = time.perf_counter_ns()

    def __eq__(self, other):
        if not isinstance(other, _QueueMessage):
            return False
        return self._monotonic_time == other._monotonic_time

    def __lt__(self, other):
        assert isinstance(other, _QueueMessage)
        return self._monotonic_time < other._monotonic_time


class InMemoryInputDevice(InputDevice['InMemoryDeviceManager']):
    """
    an input device which is stored in memory
    """

    class InMemoryTransaction(InputTransaction):
        """
        a transaction object for the in memory device
        """

        def __init__(self, device: 'InMemoryInputDevice', message: _QueueMessage):
            super().__init__(device)
            self._message = message
            self._device: InMemoryInputDevice = device

        def _commit(self):
            pass

        def _rollback(self):
            """
            returns the message back to queue
            """
            self._device._push_to_queue(self._message)

    def __init__(self, manager: 'InMemoryDeviceManager',
                 name: str,
                 queue: List[_QueueMessage],
                 queue_not_empty_condition: Condition):
        super().__init__(manager, name)
        self._queue = queue
        self._queue_not_empty = queue_not_empty_condition

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:

        with self._queue_not_empty:
            if self._queue_not_empty.wait_for(lambda: any(self._queue), timeout):
                queue_message = heapq.heappop(self._queue)
                transaction: InputTransaction
                if with_transaction:
                    transaction = self.InMemoryTransaction(self, queue_message)
                else:
                    transaction = NULLTransaction(self)
                device_headers = {MESSAGE_TIMESTAMP_HEADER: queue_message.timestamp}
                return ReadResult(message=queue_message.message.copy(),
                                  device_headers=device_headers,
                                  transaction=transaction)
            else:
                return None

    def _push_to_queue(self, message: _QueueMessage):
        with self._queue_not_empty:
            heapq.heappush(self._queue, message)
            self._queue_not_empty.notify()


class InMemoryOutputDevice(OutputDevice['InMemoryDeviceManager']):
    """
    an output device which is stored in memory
    """

    def __init__(self, manager: 'InMemoryDeviceManager',
                 name: str,
                 queue: List[_QueueMessage],
                 queue_not_empty_condition: Condition):
        super().__init__(manager, name)
        self._queue = queue
        self._queue_not_empty = queue_not_empty_condition

    def _send_message(self, message_bundle: MessageBundle):
        """
        sends a message to the device.

        :param message_bundle: the message bundle to send
        """
        with self._queue_not_empty:
            heapq.heappush(self._queue, _QueueMessage(message_bundle.message))
            self._queue_not_empty.notify()


class InMemoryDeviceManager(InputDeviceManager[InMemoryInputDevice], OutputDeviceManager[InMemoryOutputDevice]):
    """
    the in memory device manager. it serves as both input and output device manager.
    notice that the messages are shared only within the same manager!
    """

    def __init__(self):
        self._queues: Dict[str, Tuple[List[_QueueMessage], Condition]] = {}

    def _get_queue_tuple(self, name: str) -> Tuple[List[_QueueMessage], Condition]:
        res = self._queues.get(name, None)
        if res is not None:
            queue, condition = res
        else:
            queue = []
            condition = Condition()
            self._queues[name] = (queue, condition)

        return queue, condition

    def get_input_device(self, name: str) -> InMemoryInputDevice:
        """
        creates an input device.

        :param name: the name of the input device to create
        :return: the created input device
        """
        queue, condition = self._get_queue_tuple(name)

        return InMemoryInputDevice(self, name, queue, condition)

    def get_output_device(self, name: str) -> InMemoryOutputDevice:
        """
        creates an output device.

        :param name: the name of the output device to create
        :return: the created output device
        """
        queue, condition = self._get_queue_tuple(name)
        return InMemoryOutputDevice(self, name, queue, condition)
