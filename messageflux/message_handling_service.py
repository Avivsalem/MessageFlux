import threading
from abc import abstractmethod, ABCMeta
from time import time
from typing import List, Optional, Tuple, Union

from messageflux.iodevices.base import (InputTransactionScope,
                                        InputDeviceManager,
                                        AggregatedInputDevice,
                                        InputDevice, ReadResult)
from messageflux.server_loop_service import ServerLoopService


class MessageHandlingServiceBase(ServerLoopService, metaclass=ABCMeta):
    """
    a service thats reads from input devices and handles the messages
    """

    def __init__(self, *,
                 input_device_manager: InputDeviceManager,
                 input_device_names: Union[List[str], str],
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
        :param **kwargs: passed to parent as is
        """
        super().__init__(**kwargs)
        self._input_device_manager = input_device_manager
        if isinstance(input_device_names, str):
            input_device_names = [input_device_names]
        self._input_device_names = input_device_names
        self._use_transactions = use_transactions
        self._read_timeout = max(read_timeout, 0)
        self._max_batch_read_count = max(max_batch_read_count, 1)
        self._wait_for_batch_count = wait_for_batch_count
        self._aggregate_input_device: Optional[AggregatedInputDevice] = None

    def _prepare_service(self):
        self._input_device_manager.connect()
        self._aggregate_input_device = self._input_device_manager.get_aggregate_device(self._input_device_names)

    def _server_loop(self, cancellation_token: threading.Event):
        assert self._aggregate_input_device is not None
        with InputTransactionScope(device=self._aggregate_input_device,
                                   with_transaction=self._use_transactions) as transaction_scope:
            batch: List[Tuple[InputDevice, ReadResult]] = []

            # read first message with _read_timeout anyway
            read_result = transaction_scope.read_message(cancellation_token=cancellation_token,
                                                         timeout=self._read_timeout)
            if read_result is not None:
                last_read_device = self._aggregate_input_device.last_read_device
                assert last_read_device is not None
                batch.append((last_read_device, read_result))

            end_time = time() + self._read_timeout
            for i in range(self._max_batch_read_count - 1):  # try to read the rest of the batch
                remaining_time = end_time - time()

                if remaining_time <= 0:
                    break

                if self._wait_for_batch_count:
                    timeout = remaining_time  # if wait_for_batch_count, try to read another message with remaining time
                else:
                    timeout = 0  # if not wait_for_batch, try to read another message without waiting at all

                read_result = transaction_scope.read_message(cancellation_token=cancellation_token,
                                                             timeout=timeout)
                if read_result is None:
                    break  # no more messages to read
                last_read_device = self._aggregate_input_device.last_read_device
                assert last_read_device is not None
                batch.append((last_read_device, read_result))

            if batch:
                self._handle_message_batch(batch)

            transaction_scope.commit()

    def _finalize_service(self, exception: Optional[Exception] = None):
        self._input_device_manager.disconnect()

    @abstractmethod
    def _handle_message_batch(self, batch: List[Tuple[InputDevice, ReadResult]]):
        """
        handles a batch of messages that was read from input devices
        :param batch: a list of tuples of input device and the ReadResult object that was read from it
        """
        pass


class BatchMessageHandlerBase(metaclass=ABCMeta):
    """
    a batch message handler base class. used to handle a batch of messages
    """

    def connect(self):
        """
        called when the service starts.
        can be overrided by child class to perform some initialization logic
        """
        pass

    def disconnect(self):
        """
        called when the service stops.
        can be overrided by child class to perform some cleanup logic
        """
        pass

    @abstractmethod
    def handle_message_batch(self, batch: List[Tuple[InputDevice, ReadResult]]):
        """
        handles a batch of messages that was read from input devices
        :param batch: a list of tuples of input device and the ReadResult object that was read from it
        """
        pass


class BatchMessageHandlingService(MessageHandlingServiceBase):
    """
    a service that reads from input devices and handles the messages
    """

    def __init__(self, *,
                 batch_handler: BatchMessageHandlerBase,
                 **kwargs):
        """

        :param batch_handler: the message handler to use.
        :param **kwargs: passed to parent as is
        """
        super().__init__(**kwargs)
        self._message_handler = batch_handler

    def _prepare_service(self):
        super()._prepare_service()
        self._message_handler.connect()

    def _finalize_service(self, exception: Optional[Exception] = None):
        self._message_handler.disconnect()
        super()._finalize_service(exception)

    def _handle_message_batch(self, batch: List[Tuple[InputDevice, ReadResult]]):
        self._message_handler.handle_message_batch(batch)


class MessageHandlerBase(metaclass=ABCMeta):
    """
    a message handler base class. used to handle a single message
    """

    def connect(self):
        """
        called when the service starts.
        can be overrided by child class to perform some initialization logic
        """
        pass

    def disconnect(self):
        """
        called when the service stops.
        can be overrided by child class to perform some cleanup logic
        """
        pass

    @abstractmethod
    def handle_message(self, input_device: InputDevice, read_result: ReadResult):
        """
        handles a single message from the device

        :param input_device: the input device that the message was read from
        :param read_result: the read result returned from the device
        """
        pass


class MessageHandlingService(BatchMessageHandlingService):
    """
    a service that reads a SINGLE message from devices and handles it
    """

    class _BatchMessageHandlerAdapter(BatchMessageHandlerBase):
        def __init__(self, massage_handler: MessageHandlerBase):
            self._message_handler = massage_handler

        def connect(self):
            """
            called when the service starts.
            can be overrided by child class to perform some initialization logic
            """
            self._message_handler.connect()

        def disconnect(self):
            """
            called when the service stops.
            can be overrided by child class to perform some cleanup logic
            """
            self._message_handler.disconnect()

        def handle_message_batch(self, batch: List[Tuple[InputDevice, ReadResult]]):
            """
            handles a batch of messages that was read from input devices
            :param batch: a list of tuples of input device and the ReadResult object that was read from it
            """
            for input_device, read_result in batch:
                self._message_handler.handle_message(input_device=input_device,
                                                     read_result=read_result)

    def __init__(self, *, message_handler: MessageHandlerBase, **kwargs):
        """
        :param **kwargs: passed to parent as is
        """
        super().__init__(batch_handler=self._BatchMessageHandlerAdapter(message_handler), **kwargs)
