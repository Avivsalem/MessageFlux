import logging

from messageflux.iodevices.base import OutputDevice, OutputDeviceException, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle
from typing import Optional


class FailoverOutputDevice(OutputDevice['FailoverOutputDeviceManager']):
    """
    this device tries to send to primary output device - if the primary fails, it sends to secondary
    """

    def __init__(self,
                 device_manager: 'FailoverOutputDeviceManager',
                 inner_device: OutputDevice,
                 failover_device: OutputDevice,
                 device_name: Optional[str] = None):
        """
        this device tries to send to primary output device - if the primary fails, it sends to secondary

        :param inner_device: the device to wrap
        :param failover_device: the failover device to send to in case of primary fail
        :param device_name: the name for the device (or None to take from inner_device
        """
        if device_name is None:
            device_name = inner_device.name
        super(FailoverOutputDevice, self).__init__(device_manager, device_name)
        self._inner_device = inner_device
        self._failover_device = failover_device
        self._logger = logging.getLogger(__name__)

    @property
    def inner_device(self) -> OutputDevice:
        """
        the inner device
        :return: the inner device
        """
        return self._inner_device

    @property
    def failover_device(self) -> OutputDevice:
        """
        the failover device
        :return: the failover device
        """
        return self._failover_device

    def close(self):
        """
        closes the underlying devices
        """
        primary_worked = False
        try:
            self._inner_device.close()
            primary_worked = True
        except Exception:
            self._logger.warning(f'Error disconnecting from underlying device {self._inner_device.name}', exc_info=True)

        try:
            self._failover_device.close()
        except Exception:
            if primary_worked:
                self._logger.warning(f'Error disconnecting from FAILOVER underlying device {self._inner_device.name}',
                                     exc_info=True)
            else:
                raise

    def _send_message(self, message_bundle: MessageBundle):
        """
        does the actual sending of a stream to OutputDevice with metadata

        :param stream: the stream to send
        """
        failures = []
        try:
            self._inner_device.send_message(message_bundle.message, message_bundle.device_headers)
            return
        except Exception as ex:
            failures.append(ex)
            self._logger.warning(f'Error sending to underlying device {self._inner_device.name}', exc_info=True)

            try:
                self._failover_device.send_message(message_bundle.message.copy(), message_bundle.device_headers)
                return
            except Exception as ex:
                failures.append(ex)
                raise OutputDeviceException("Couldn't send to device or failover device", inner_exceptions=failures)


class FailoverOutputDeviceManager(OutputDeviceManager[OutputDevice]):
    """
    Output Device Manager that uses a failover if the send fails
    """

    def __init__(self, inner_device_manager: OutputDeviceManager, failover_device_manager: OutputDeviceManager):
        """
        :param inner_device_manager: the device to wrap
        :param failover_device_manager: the failover device_manager to send to in case of primary fail
        """
        self._inner_device_manager = inner_device_manager
        self._failover_device_manager = failover_device_manager
        self._logger = logging.getLogger(__name__)

    def connect(self):
        """
        connects to device manager
        """
        primary_worked = False
        try:
            self._inner_device_manager.connect()
            primary_worked = True
        except Exception:
            self._logger.warning(
                f'Error connecting to underlying manager: {type(self._inner_device_manager).__name__}', exc_info=True)
        try:
            self._failover_device_manager.connect()
        except Exception:
            if primary_worked:
                self._logger.warning(
                    f'Error connecting to underlying manager: {type(self._failover_device_manager).__name__}',
                    exc_info=True)
            else:
                raise

    def disconnect(self):
        """
        closes the connection to IODeviceManager
        """
        primary_worked = False
        try:
            self._inner_device_manager.disconnect()
            primary_worked = True
        except Exception:
            self._logger.warning(
                f'Error disconnecting from underlying manager: {type(self._inner_device_manager).__name__}',
                exc_info=True)
        try:
            self._failover_device_manager.disconnect()
        except Exception:
            if primary_worked:
                self._logger.warning(
                    f'Error disconnecting from underlying manager: {type(self._failover_device_manager).__name__}',
                    exc_info=True)
            else:
                raise

    def get_output_device(self, name: str) -> OutputDevice:
        """
        Returns an output device by name

        :param name: the name of the device to write to
        :return: an output device for 'device_name'
        """
        inner_device = None
        try:
            inner_device = self._inner_device_manager.get_output_device(name)
        except Exception:
            self._logger.warning(
                f'Error creating output device {name} from {type(self._inner_device_manager).__name__} manager',
                exc_info=True)
        try:
            failover_device = self._failover_device_manager.get_output_device(name)
        except Exception:
            if inner_device is not None:
                self._logger.warning(
                    f'Error creating output device {name} from {type(self._failover_device_manager).__name__} '
                    f'manager',
                    exc_info=True)
                return inner_device
            else:
                raise

        if inner_device is None:
            return failover_device

        return FailoverOutputDevice(device_manager=self,
                                    inner_device=inner_device,
                                    failover_device=failover_device,
                                    device_name=name)
