import logging
from typing import Collection, List, Callable

from messageflux.iodevices.base import OutputDeviceManager, OutputDevice, OutputDeviceException
from messageflux.iodevices.base.common import MessageBundle


class CollectionOutputDevice(OutputDevice['CollectionOutputDeviceManager']):
    """
    this class writes to a collection of output devices.

    it iterates over the collection, and sends to the first successful device
    """

    def __init__(self,
                 device_manager: 'CollectionOutputDeviceManager',
                 device_name: str,
                 output_devices: Collection[OutputDevice]):
        """
        :param device_name: the name of this device
        :param output_devices: the outgoing devices to send to
        """
        super(CollectionOutputDevice, self).__init__(device_manager, device_name)
        self._output_devices = output_devices

        self._logger = logging.getLogger(__name__)

    def _send_message(self, message_bundle: MessageBundle):
        """
        sends a message to the device. this should be implemented by child classes

        :param message_bundle: the message bundle to send
        """
        failures = []
        for curr_device in self._output_devices:
            try:
                curr_device.send_message(message_bundle.message, message_bundle.device_headers)
                return
            except Exception as e:
                failures.append(e)
                self._logger.warning(f'underlying device {curr_device.name}', exc_info=True)

        self._logger.error("Couldn't send to any underlying device")
        raise OutputDeviceException("Couldn't send to any underlying device", inner_exceptions=failures)

    def close(self):
        """
        closes the connection to device
        """
        for device in self._output_devices:
            try:
                device.close()
            except Exception:
                self._logger.warning(f'Error closing underlying device {device.name}', exc_info=True)


class CollectionOutputDeviceManager(OutputDeviceManager[CollectionOutputDevice]):
    """
    This class is used to create Collection OutputDevices
    """

    def __init__(self,
                 inner_managers: List[OutputDeviceManager],
                 collection_maker: Callable[[List[OutputDevice]], Collection[OutputDevice]]):
        """
        This class is used to create Collection OutputDevices

        :param inner_managers: the actual OutputDeviceManager instances to generate devices from
        :param collection_maker: the callable to make the iterable collection from list of devices
        """
        self._inner_managers = inner_managers
        self._logger = logging.getLogger(__name__)
        self._collection_maker = collection_maker

    def connect(self):
        """
        connects to device manager
        """
        failures = []
        for manager in self._inner_managers:
            try:
                manager.connect()
            except Exception as e:
                failures.append(e)
                self._logger.warning(
                    f'Error connecting to underlying manager: {type(manager).__name__}', exc_info=True)

        if len(failures) >= len(self._inner_managers):
            self._logger.error("Couldn't connect to any underlying manager")
            raise OutputDeviceException('Error connecting to underlying device managers', inner_exceptions=failures)

    def disconnect(self):
        """
        closes the connection to IODeviceManager
        """
        for manager in self._inner_managers:
            try:
                manager.disconnect()
            except Exception:
                self._logger.warning(
                    f'Error closing underlying manager {type(manager).__name__}', exc_info=True)

    def get_output_device(self, name: str) -> CollectionOutputDevice:
        """
        Returns an output device by name

        :param name: the name of the device to write to
        :return: an output device for 'device_name'
        """
        devices: List[OutputDevice] = []
        failures = []
        for manager in self._inner_managers:
            try:
                device = manager.get_output_device(name)
                devices.append(device)
            except Exception as ex:
                failures.append(ex)
                self._logger.warning(
                    f"Error creating output device {name} from {type(manager).__name__} manager.", exc_info=True)
        if not devices:
            self._logger.error(f"Couldn't create output device '{name}'")
            raise OutputDeviceException(f"Couldn't create output device '{name}'", inner_exceptions=failures)

        return CollectionOutputDevice(self, name, self._collection_maker(devices))
