from abc import ABCMeta, abstractmethod
from time import time, sleep
from typing import Optional, Tuple, List, Union

from baseservice.iodevices.base.common import Message, DeviceHeaders
from baseservice.utils import KwargsException, StatefulListIterator

from baseservice.iodevices.base.input_transaction import InputTransaction, NULL_TRANSACTION

ReadMessageResult = Union[Tuple[Message, DeviceHeaders, InputTransaction], Tuple[None, None, None]]
EMPTY_RESULT = (None, None, None)


class InputDeviceException(KwargsException):
    """
    a base exception class for all input device related exceptions
    """
    pass


class InputDevice(metaclass=ABCMeta):
    """
    this is the base class for input devices
    """
    INPUT_DEVICE_NAME_HEADER = "__INPUT_DEVICE_NAME__"

    def __init__(self, manager: 'InputDeviceManager', name: str):
        """

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
    def manager(self) -> 'InputDeviceManager':
        """
        :return: the input device manager that created this device
        """
        return self._manager

    def read_message(self,
                     timeout: Optional[float] = 0,
                     with_transaction: bool = True) -> ReadMessageResult:
        """
        this method returns a message from the device. and makes sure that the input device name header is present

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return (None, None)
        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a tuple of (Message, DeviceHeaders, Transaction) or EMPTY_RESULT if no message was available.
        the device headers, can contain extra information about the device that returned the message
        """
        message, device_headers, transaction = self._read_message(timeout=timeout,
                                                                  with_transaction=with_transaction)
        if message is not None:
            device_headers = device_headers or {}
            device_headers.setdefault(self.INPUT_DEVICE_NAME_HEADER, self.name)

        if not with_transaction:
            transaction = NULL_TRANSACTION

        return message, device_headers, transaction

    @abstractmethod
    def _read_message(self,
                      timeout: Optional[float] = 0,
                      with_transaction: bool = True) -> ReadMessageResult:
        """
        this method returns a message from the device (should be implemented by child classes)

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return (None, None)
        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a tuple of (Message, DeviceHeaders, Transaction) or EMPTY_RESULT if no message was available.
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

    def _read_from_device(self, with_transaction: bool) -> ReadMessageResult:
        """
        tries to read from the first device that returnes a result.
        upon success, return the result. otherwise, returns (None,None,None)

        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: the result from the first device that returned non-empty result
        """
        for inner_device in self._inner_devices_iterator:
            self._last_read_device = inner_device

            message, device_headers, transaction = inner_device.read_message(timeout=0,
                                                                             with_transaction=with_transaction)
            if message is not None:
                return message, device_headers, transaction

        self._last_read_device = None
        return EMPTY_RESULT

    def _read_message(self,
                      timeout: Optional[float] = 0,
                      with_transaction: bool = True) -> ReadMessageResult:
        end_time = time() + timeout
        message, device_headers, transaction = self._read_from_device(with_transaction=with_transaction)
        while message is None and (time() < end_time):
            sleep(0.1)
            message, device_headers, transaction = self._read_from_device(with_transaction=with_transaction)

        return message, device_headers, transaction


class InputDeviceManager(metaclass=ABCMeta):
    """
    this is the base class for input device managers. this class is used to create input devices.
    """

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
    def get_input_device(self, name: str) -> InputDevice:
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
