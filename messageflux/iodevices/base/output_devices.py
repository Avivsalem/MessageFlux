from abc import ABCMeta, abstractmethod
from typing import Optional, TypeVar, Generic

from messageflux.iodevices.base.common import Message, DeviceHeaders, MessageBundle
from messageflux.utils import AggregatedException

TManagerType = TypeVar('TManagerType', bound='OutputDeviceManager')
TOutputDeviceType = TypeVar('TOutputDeviceType', bound='OutputDevice')


class OutputDeviceException(AggregatedException):
    """
    a base exception class for all output device related exceptions
    """
    pass


class OutputDevice(Generic[TManagerType], metaclass=ABCMeta):
    """
    base class for all output devices
    """

    def __init__(self, manager: TManagerType, name: str):
        """

        :param manager: the output device manager that created this device
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

    def send_message(self, message: Message, device_headers: Optional[DeviceHeaders] = None):
        """
        sends a message to the device.

        :param message: the message to send
        :param device_headers: optional headers to send to underlying device.
        those headers are not part of the message, but contains extra data for the device, that can modify its operation
        """
        device_headers = device_headers or {}
        self._send_message(MessageBundle(message=message, device_headers=device_headers))

    @abstractmethod
    def _send_message(self, message_bundle: MessageBundle):
        """
        sends a message to the device. this should be implemented by child classes

        :param message_bundle: the message bundle to send
        """
        pass

    def close(self):
        """
        and optional method that cleans device resources if necessary
        """
        pass


class OutputDeviceManager(Generic[TOutputDeviceType], metaclass=ABCMeta):
    """
    this is a base class for output device managers. it is used to create output devices
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
    def get_output_device(self, name: str) -> TOutputDeviceType:
        """
        creates an output device. this should be implemented by child classes

        :param name: the name of the output device to create
        :return: the created output device
        """
        pass
