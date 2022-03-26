import asyncio
import time
from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple, Union, List, Dict

from baseservice.iodevices.async_devices.async_input_transaction import AsyncInputTransaction, ASYNC_NULL_TRANSACTION
from baseservice.iodevices.base import InputDevice, EMPTY_RESULT
from baseservice.iodevices.base.common import Message, DeviceHeaders

ReadMessageResult = Union[Tuple[Message, DeviceHeaders, AsyncInputTransaction], Tuple[None, None, None]]


class AsyncInputDevice(metaclass=ABCMeta):
    """
    this is the base class for input devices
    """

    def __init__(self, manager: 'AsyncInputDeviceManager', name: str):
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
    def manager(self) -> 'AsyncInputDeviceManager':
        """
        :return: the input device manager that created this device
        """
        return self._manager

    async def read_message(self,
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
        message, device_headers, transaction = await self._read_message(timeout=timeout,
                                                                        with_transaction=with_transaction)
        if message is not None:
            device_headers = device_headers or {}
            device_headers.setdefault(InputDevice.INPUT_DEVICE_NAME_HEADER, self.name)

        if not with_transaction:
            transaction = ASYNC_NULL_TRANSACTION

        return message, device_headers, transaction

    @abstractmethod
    async def _read_message(self,
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


class AsyncAggregateInputDevice(AsyncInputDevice):
    """
    this class is a round-robin input device, that reads from several underlying input devices in order
    """

    def __init__(self, manager: 'AsyncInputDeviceManager', inner_devices: List[AsyncInputDevice]):
        """

        :param manager: the input device manager that created this device
        :param inner_devices: the list of input devices to read from
        """
        super().__init__(manager=manager, name="AggregateInputDevice")
        self._inner_devices = inner_devices
        self._inner_devices_tasks: Dict[asyncio.Task, AsyncInputDevice] = None
        self._last_read_device: Optional[AsyncInputDevice] = None

    @property
    def last_read_device(self) -> Optional[InputDevice]:
        """
        :return: the last device that was read (in case it returned data/raised exception)
        """
        return self._last_read_device

    async def _read_message(self,
                            timeout: Optional[float] = 0,
                            with_transaction: bool = True) -> ReadMessageResult:
        start_time = time.time()
        if self._inner_devices_tasks is None:
            self._inner_devices_tasks = {}
            for inner_device in self._inner_devices:
                task = asyncio.create_task(
                    inner_device.read_message(timeout=timeout, with_transaction=with_transaction))
                self._inner_devices_tasks[task] = inner_device

        remaining_time = None  # this is so the loop will run at least once

        while remaining_time is None or remaining_time > 0:
            if timeout is None:
                remaining_time = None
            else:
                remaining_time = max((start_time + timeout) - time.time(), 0)

            done, _ = await asyncio.wait(self._inner_devices_tasks.keys(),
                                         timeout=remaining_time,
                                         return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                inner_device = self._inner_devices_tasks[task]
                self._inner_devices_tasks.pop(task)
                new_task = asyncio.create_task(
                    inner_device.read_message(timeout=timeout, with_transaction=with_transaction))
                self._inner_devices_tasks[new_task] = inner_device
                result = task.result()
                if result is not EMPTY_RESULT:
                    self._last_read_device = inner_device
                    return result

        return EMPTY_RESULT


class AsyncInputDeviceManager(metaclass=ABCMeta):
    """
    this is the base class for input device managers. this class is used to create input devices.
    """

    async def connect(self):
        """
        connects to the device manager
        """
        pass

    async def disconnect(self):
        """
        disconnects from the device manager
        """
        pass

    @abstractmethod
    async def get_input_device(self, name: str) -> AsyncInputDevice:
        """
        creates an input device. should be implemented in child classes

        :param name: the name of the input device to create
        :return: the created input device
        """
        pass

    async def get_aggregate_device(self, names: List[str]) -> AsyncAggregateInputDevice:
        inner_devices: List[AsyncInputDevice] = []
        for name in names:
            inner_devices.append(await self.get_input_device(name))

        return AsyncAggregateInputDevice(manager=self, inner_devices=inner_devices)
