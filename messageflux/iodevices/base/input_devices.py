import logging
import threading
from abc import ABCMeta, abstractmethod
from time import perf_counter
from typing import Optional, List, TypeVar, Generic

from messageflux.iodevices.base.common import MessageBundle, Message, DeviceHeaders
from messageflux.iodevices.base.input_transaction import InputTransaction, NULLTransaction
from messageflux.utils import StatefulListIterator, AggregatedException

TManagerType = TypeVar('TManagerType', bound='InputDeviceManager')
TInputDeviceType = TypeVar('TInputDeviceType', bound='InputDevice')


class InputDeviceException(AggregatedException):
    """
    a base exception class for all input device related exceptions
    """
    pass


class InputDevice(Generic[TManagerType], metaclass=ABCMeta):
    """
    this is the base class for input devices
    """
    INPUT_DEVICE_NAME_HEADER = "__INPUT_DEVICE_NAME__"

    def __init__(self, manager: TManagerType, name: str):
        """
        :param manager: the input device manager that created this device
        :param name: the name of this device
        """
        self._manager = manager
        self._name = name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def name(self) -> str:
        """
        :return: the name of this device
        """
        return self._name

    @property
    def manager(self) -> TManagerType:
        """
        :return: the input device manager that created this device
        """
        return self._manager

    def read_message(self,
                     cancellation_token: threading.Event,
                     timeout: Optional[float] = None,
                     with_transaction: bool = True) -> Optional['ReadResult']:
        """
        this method returns a message from the device. and makes sure that the input device name header is present

        :param cancellation_token: the cancellation token for this service. this can be used to know if cancellation
        was requested

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return None
        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a ReadResult object or None if no message was available.
        the device headers can contain extra information about the device that returned the message
        """
        read_result = self._read_message(cancellation_token=cancellation_token,
                                         timeout=timeout,
                                         with_transaction=with_transaction)
        if read_result is not None:
            read_result.device_headers.setdefault(self.INPUT_DEVICE_NAME_HEADER, self.name)

        return read_result

    @abstractmethod
    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        """
        this method returns a message from the device (should be implemented by child classes)

        :param cancellation_token: the cancellation token for this service. this can be used to know if cancellation
        was requested

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return None

        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a ReadResult object or None if no message was available.
        the device headers, can contain extra information about the device that returned the message
        """
        pass

    def close(self):
        """
        and optional method that cleans device resources if necessary
        """
        pass


class AggregatedInputDevice(InputDevice[TManagerType]):
    """
    this class is a round-robin input device, that reads from several underlying input devices in order
    """
    # the amount in seconds to sleep between iterations. otherwise, we perform busy wait if all devices are empty
    _SLEEP_BETWEEN_ITERATIONS: float = 0.1

    def __init__(self, manager: TManagerType, inner_devices: List[InputDevice]):
        """
        :param manager: the input device manager that created this device
        :param inner_devices: the list of input devices to read from
        """
        super().__init__(manager=manager, name="AggregateInputDevice")
        self._inner_devices_iterator: StatefulListIterator[InputDevice] = StatefulListIterator(inner_devices)
        self._last_read_device: Optional[InputDevice] = None
        self._logger = logging.getLogger(__name__)

    @property
    def last_read_device(self) -> Optional[InputDevice]:
        """
        :return: the last device that was read (in case it returned data/raised exception)
        """
        return self._last_read_device

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        """
        this method returns a message from the first device available in inner devices

        :param cancellation_token: the cancellation token for this service. this can be used to know if cancellation
        was requested

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return None

        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a ReadResult object or None if no message was available.
        the device headers, can contain extra information about the device that returned the message
        """

        end_time = 0.0
        if timeout is not None:
            end_time = perf_counter() + timeout

        while True:
            for inner_device in self._inner_devices_iterator:
                self._last_read_device = inner_device
                read_result = inner_device.read_message(cancellation_token=cancellation_token,
                                                        timeout=0,
                                                        with_transaction=with_transaction)
                if read_result is not None:
                    return read_result

                if timeout is not None and perf_counter() >= end_time:
                    break

            if timeout is not None and perf_counter() >= end_time:
                self._last_read_device = None
                return None
            else:
                # if all the devices were empty, wait before performing another iteration.
                cancellation_token.wait(self._SLEEP_BETWEEN_ITERATIONS)

    def close(self):
        """
        tries to close underlying devices
        """
        for inner_device in self._inner_devices_iterator:
            try:
                inner_device.close()
            except Exception:
                self._logger.exception("Error closing underlying device")


class InputDeviceManager(Generic[TInputDeviceType], metaclass=ABCMeta):
    """
    this is the base class for input device managers. this class is used to create input devices.
    """

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        """
        connects to the device manager
        """
        pass

    def disconnect(self):
        """
        disconnects from the device manager
        """
        pass

    @abstractmethod
    def get_input_device(self, name: str) -> TInputDeviceType:
        """
        creates an input device. should be implemented in child classes

        :param name: the name of the input device to create
        :return: the created input device
        """
        pass

    def get_aggregate_device(self, names: List[str]) -> AggregatedInputDevice:
        """
        creates an aggregated input device on all the devices with given names

        :param names: the names of the devices to create and aggregate
        :return: the AggregatedInputDevice
        """
        inner_devices: List[InputDevice] = []
        for name in names:
            inner_devices.append(self.get_input_device(name))

        return AggregatedInputDevice(manager=self, inner_devices=inner_devices)


class _NullInputDeviceManager(InputDeviceManager):
    """
    this is a stub used to create null device
    """

    def get_input_device(self, name: str) -> InputDevice:
        """
        returns a null device
        """
        return _NullDevice()


class _NullDevice(InputDevice):
    """
    this is a stub for creating a null transaction
    """

    def __init__(self):
        super(_NullDevice, self).__init__(_NullInputDeviceManager(), '__NULL__')

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        """
        always returns None immediately
        """
        return None


NULL_TRANSACTION = NULLTransaction(_NullDevice())  # this is a const NullTransaction, for convenience


class ReadResult(MessageBundle):
    """
    this class holds the result for "read_message". adds the transaction to the message bundle
    """
    __slots__ = "_transaction"

    def __init__(self,
                 message: Message,
                 device_headers: Optional[DeviceHeaders] = None,
                 transaction: InputTransaction = NULL_TRANSACTION):
        """
        :param message: The Message.
        :param device_headers: Additional Headers that may return data from device, or affect its operation.
        :param transaction: the transaction returned by the reading device
        """
        super(ReadResult, self).__init__(message=message,
                                         device_headers=device_headers)

        self._transaction = transaction

    @property
    def transaction(self) -> InputTransaction:
        """
        the transaction returned by the reading device
        """
        return self._transaction

    def commit(self) -> None:
        """
        commits this read result
        """
        self.transaction.commit()

    def rollback(self) -> None:
        """
        rolls back this read result
        """
        self.transaction.rollback()
