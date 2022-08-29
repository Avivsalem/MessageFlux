from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Optional, List, TypeVar, Generic

from time import time, sleep

from baseservice.iodevices.base.common import MessageBundle
from baseservice.iodevices.base.input_transaction import InputTransaction, NULLTransaction
from baseservice.utils import KwargsException, StatefulListIterator

TManagerType = TypeVar('TManagerType', bound='InputDeviceManager')
TInputDeviceType = TypeVar('TInputDeviceType', bound='InputDevice')


class InputDeviceException(KwargsException):
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
        ctor
        :param manager: the input device manager that created this device
        :param name: the name of this device
        """
        self._manager = manager
        self._name = name

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
                     timeout: Optional[float] = 0,
                     with_transaction: bool = True) -> Optional['ReadResult']:
        """
        this method returns a message from the device. and makes sure that the input device name header is present
        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return None
        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a ReadResult object or None if no message was available.
        the device headers, can contain extra information about the device that returned the message
        """
        read_result = self._read_message(timeout=timeout, with_transaction=with_transaction)
        if read_result is not None:
            read_result.device_headers.setdefault(self.INPUT_DEVICE_NAME_HEADER, self.name)

        return read_result

    @abstractmethod
    def _read_message(self,
                      timeout: Optional[float] = 0,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        """
        this method returns a message from the device (should be implemented by child classes)

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return (None, None)
        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a ReadResult object or None if no message was available.
        the device headers, can contain extra information about the device that returned the message
        """
        pass


class AggregateInputDevice(InputDevice):
    """
    this class is a round-robin input device, that reads from several underlying input devices in order
    """

    def __init__(self, manager: 'InputDeviceManager', inner_devices: List[InputDevice]):
        """

        :param manager: the input device manager that created this device
        :param inner_devices: the list of input devices to read from
        """
        super().__init__(manager=manager, name="AggregateInputDevice")
        self._inner_devices_iterator: StatefulListIterator[InputDevice] = StatefulListIterator(inner_devices)
        self._last_read_device: Optional[InputDevice] = None

    @property
    def last_read_device(self) -> Optional[InputDevice]:
        """
        :return: the last device that was read (in case it returned data/raised exception)
        """
        return self._last_read_device

    def _read_from_device(self, with_transaction: bool) -> Optional['ReadResult']:
        """
        tries to read from the first device that returns a result.
        upon success, return the result. otherwise, returns None

        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: the result from the first device that returned non-empty result
        """
        for inner_device in self._inner_devices_iterator:
            self._last_read_device = inner_device

            read_result = inner_device.read_message(timeout=0, with_transaction=with_transaction)
            if read_result is not None:
                return read_result

        self._last_read_device = None
        return None

    def _read_message(self,
                      timeout: Optional[float] = 0,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        end_time = 0.0
        if timeout is not None:
            end_time = time() + timeout
        read_result = self._read_from_device(with_transaction=with_transaction)
        while read_result is None and (timeout is None or time() < end_time):
            sleep(0.1)
            read_result = self._read_from_device(with_transaction=with_transaction)

        return read_result


class InputDeviceManager(Generic[TInputDeviceType], metaclass=ABCMeta):
    """
    this is the base class for input device managers. this class is used to create input devices.
    """

    def __enter__(self):
        self.connect()

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

    def get_aggregate_device(self, names: List[str]) -> AggregateInputDevice:
        inner_devices: List[InputDevice] = []
        for name in names:
            inner_devices.append(self.get_input_device(name))

        return AggregateInputDevice(manager=self, inner_devices=inner_devices)


class _NullInputDeviceManager(InputDeviceManager):
    def get_input_device(self, name: str) -> InputDevice:
        return _NullDevice()


class _NullDevice(InputDevice):
    def __init__(self):
        super(_NullDevice, self).__init__(_NullInputDeviceManager(), '__NULL__')

    def _read_message(self, timeout: Optional[float] = 0, with_transaction: bool = True) -> Optional['ReadResult']:
        return None


NULL_TRANSACTION = NULLTransaction(_NullDevice())


@dataclass
class ReadResult(MessageBundle):
    """
    this class holds the result for "read_message". adds the transaction to the message bundle
    """
    transaction: InputTransaction = NULL_TRANSACTION

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
