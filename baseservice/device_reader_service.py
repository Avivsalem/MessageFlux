import threading
from abc import abstractmethod, ABCMeta
from time import time
from typing import List, Optional, Tuple

from baseservice.iodevices.base import (InputTransactionScope,
                                        Message,
                                        DeviceHeaders,
                                        InputDeviceManager,
                                        AggregateInputDevice,
                                        InputDevice)
from baseservice.server_loop_service import ServerLoopService


class DeviceReaderService(ServerLoopService, metaclass=ABCMeta):
    def __init__(self, *,
                 input_device_manager: InputDeviceManager,
                 input_device_names: List[str],
                 use_transactions: bool = True,
                 read_timeout: float = 5,
                 max_batch_read_count: int = 1,
                 wait_for_batch_count: bool = False,
                 **kwargs):
        """

        :param input_device_manager: the device manager to read messages from
        :param input_device_names: the device names to read messages from
        :param use_transactions: whether to use transaction when reading from device
        :param read_timeout: the time (in seconds) to wait on device to return a message
        :param max_batch_read_count: the maximum batch size to read from device
        :param wait_for_batch_count: should the service wait the whole read_timeout for batch_count messages to be read.
        'False' means that it will read *up-to* batch_count messages, and process them immediately.
        'True' means that it will wait until read_timeout is passed, or batch_count messages has reached.
        :param kwargs: passed to parent as is
        """
        super().__init__(**kwargs)
        self._input_device_manager = input_device_manager
        self._input_device_names = input_device_names
        self._use_transactions = use_transactions
        self._read_timeout = max(read_timeout, 0)
        self._max_batch_read_count = max(max_batch_read_count, 1)
        self._wait_for_batch_count = wait_for_batch_count
        self._aggregate_input_device: Optional[AggregateInputDevice] = None

    def _prepare_service(self):
        self._input_device_manager.connect()
        self._aggregate_input_device = self._input_device_manager.get_aggregate_device(self._input_device_names)

    def _server_loop(self, cancellation_token: threading.Event):
        with InputTransactionScope(device=self._aggregate_input_device,
                                   with_transaction=self._use_transactions) as transaction_scope:
            batch = []

            # read first message with _read_timeout anyway
            message, device_headers = transaction_scope.read_message(timeout=self._read_timeout)
            if message is not None:
                batch.append((self._aggregate_input_device.last_read_device, message, device_headers))

            end_time = time() + self._read_timeout
            for i in range(self._max_batch_read_count - 1):  # try to read the rest of the batch
                remaining_time = end_time - time()

                if remaining_time <= 0:
                    break

                if self._wait_for_batch_count:
                    timeout = remaining_time  # if wait_for_batch_count, try to read another message with remaining time
                else:
                    timeout = 0  # if not wait_for_batch, try to read another message without waiting at all

                message, device_headers = transaction_scope.read_message(timeout=timeout)
                if message is None:
                    break  # no more messages to read

                batch.append((self._aggregate_input_device.last_read_device, message, device_headers))

            if batch:
                self._handle_messages(batch)

            transaction_scope.commit()

    def _finalize_service(self, exception: Optional[Exception] = None):
        self._input_device_manager.disconnect()

    @abstractmethod
    def _handle_messages(self, batch: List[Tuple[InputDevice, Message, DeviceHeaders]]):
        pass
