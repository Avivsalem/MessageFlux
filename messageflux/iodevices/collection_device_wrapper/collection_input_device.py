import logging
import threading
from time import perf_counter
from typing import Optional, Collection, List, Callable

from messageflux import InputDevice, ReadResult
from messageflux.iodevices.base import InputDeviceManager, InputDeviceException


class CollectionInputDevice(InputDevice['CollectionInputDeviceManager']):
    """
    this class reads from a collection of input devices.

    it iterates over the collection, and returns the first successful item read from the device
    """
    _SLEEP_BETWEEN_ITERATIONS: float = 0.1

    def __init__(self,
                 device_manager: 'CollectionInputDeviceManager',
                 device_name: str,
                 input_devices: Collection[InputDevice]):
        """
        ctor

        :param device_name: the name of this device
        :param input_devices: the devices that this aggregate device reads from

        """
        super(CollectionInputDevice, self).__init__(device_manager, device_name)
        self._input_devices = input_devices
        self._logger = logging.getLogger(__name__)

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        """
        reads a message from device

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
            failures = []
            for curr_device in self._input_devices:
                try:
                    read_result = curr_device._read_message(cancellation_token=cancellation_token,
                                                            timeout=0,
                                                            with_transaction=with_transaction)
                    if read_result is not None:
                        return read_result
                    if timeout is not None and perf_counter() >= end_time:
                        break
                except Exception as ex:
                    failures.append(ex)
                    self._logger.warning(f"Error reading from {curr_device.name} device", exc_info=True)
                    if timeout is not None and perf_counter() >= end_time:
                        break

            if len(failures) >= len(self._input_devices):
                self._logger.error('Error reading from CollectionInputDevice - all devices failed')
                raise InputDeviceException('Error reading from CollectionInputDevice - all devices failed',
                                           inner_exceptions=failures)

            if timeout is not None and perf_counter() >= end_time:
                return None
            else:
                cancellation_token.wait(self._SLEEP_BETWEEN_ITERATIONS)

    def close(self):
        """
        closes the connection to device
        """
        for device in self._input_devices:
            try:
                device.close()
            except Exception:
                self._logger.warning(f'Error closing underlying device {device.name}', exc_info=True)


class CollectionInputDeviceManager(InputDeviceManager[CollectionInputDevice]):
    """
    This class is used to create Collection InputDevices
    """

    def __init__(self,
                 inner_managers: List[InputDeviceManager],
                 collection_maker: Callable[[List[InputDevice]], Collection[InputDevice]]):
        """
        This class is used to create Collection InputDevices

        :param inner_managers: the actual InputDeviceManager instances to generate devices from
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
            raise InputDeviceException('Error connecting to underlying device managers', inner_exceptions=failures)

    def disconnect(self):
        """
        disconnects from the device manager
        """
        for manager in self._inner_managers:
            try:
                manager.disconnect()
            except Exception:
                self._logger.warning(
                    f'Error closing underlying manager {type(manager).__name__}', exc_info=True)

    def get_input_device(self, name: str) -> CollectionInputDevice:
        """
        Returns an input device by name

        :param name: the name of the device to read from
        :return: an input device for 'device_name'
        """
        devices: List[InputDevice] = []
        failures = []
        for manager in self._inner_managers:
            try:
                device = manager.get_input_device(name)
                devices.append(device)
            except Exception as ex:
                failures.append(ex)
                self._logger.warning(
                    f"Error creating input device {name} from {type(manager).__name__} manager", exc_info=True)
        if not devices:
            self._logger.error("Couldn't create any input device")
            raise InputDeviceException(f"Couldn't create input device '{name}'", inner_exceptions=failures)

        return CollectionInputDevice(self, name, self._collection_maker(devices))
