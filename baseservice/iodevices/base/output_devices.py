from abc import ABCMeta, abstractmethod
from typing import Optional

from baseservice.iodevices.base.common import Message, DeviceHeaders
from baseservice.utils import KwargsException


class OutputDeviceException(KwargsException):
    """
    a base exception class for all output device related exceptions
    """
    pass


class OutputDevice(metaclass=ABCMeta):
    """
    base class for all output devices
    """

    def __init__(self, manager: 'OutputDeviceManager', name: str):
        """

        :param manager: the output device manager that created this device
        :param name: the name of this device
        """
        self._manager = manager
        self._name = name

    def send_message(self, message: Message, device_headers: Optional[DeviceHeaders] = None):
        """
        sends a message to the device.

        :param message: the message to send
        :param device_headers: optional headers to send to underlying device.
        those headers are not part of the message, but contains extra data for the device, that can modify its operation
        """
        device_headers = device_headers or {}
        self._send_message(message=message, device_headers=device_headers)

    @abstractmethod
    def _send_message(self, message: Message, device_headers: DeviceHeaders):
        """
        sends a message to the device. this should be implemented by child classes

        :param message: the message to send
        :param device_headers: optional headers to send to underlying device.
        those headers are not part of the message, but contains extra data for the device, that can modify its operation
        """
        pass


class OutputDeviceManager(metaclass=ABCMeta):
    """
    this is a base class for output device managers. it is used to create output devices
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
    def get_output_device(self, name: str) -> OutputDevice:
        """
        creates an output device. this should be implemented by child classes

        :param name: the name of the output device to create
        :return: the created output device
        """
        pass
