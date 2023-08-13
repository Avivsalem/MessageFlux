import threading
from typing import Optional

from messageflux.iodevices.base import InputDevice, ReadResult, InputDeviceManager
from messageflux.iodevices.short_circuit_device_wrapper.common import ShortCircuitDeviceBase


class ShortCircuitInputDevice(ShortCircuitDeviceBase, InputDevice['ShortCircuitInputDeviceManager']):
    """
    this is a wrapper input device for adding the short circuit logic to input devices
    """

    def __init__(self,
                 manager: 'ShortCircuitInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        InputDevice.__init__(self, manager, name)
        ShortCircuitDeviceBase.__init__(self, short_circuit_fail_count, short_circuit_time)
        self._inner_device = inner_device

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:
        self._validate_short_circuit()
        with self._failure_count_context():
            return self._inner_device.read_message(cancellation_token=cancellation_token,
                                                   timeout=timeout,
                                                   with_transaction=with_transaction)

    def close(self):
        """
        closes the inner device
        """
        self._inner_device.close()


class ShortCircuitInputDeviceManager(InputDeviceManager[ShortCircuitInputDevice]):
    """
    this is an input device manager that wraps input devices in short circuit input devices
    """

    def __init__(self,
                 inner_device_manager: InputDeviceManager,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        """

        :param inner_device_manager: the inner device manager
        :param short_circuit_fail_count: the number of consecutive failures after which,
        the device will be short circuited
        :param short_circuit_time: the time that the device will remain short circuited
        """
        self._inner_device_manager = inner_device_manager
        self._short_circuit_fail_count = short_circuit_fail_count
        self._short_circuit_fail_time = short_circuit_time

    def connect(self):
        """
        connects to the device manager
        """
        self._inner_device_manager.connect()

    def disconnect(self):
        """
        disconnects from the device manager
        """
        self._inner_device_manager.disconnect()

    def get_input_device(self, name: str) -> ShortCircuitInputDevice:
        """
        returns a wrapped input device

        :param name: the name of the input device to get
        """
        inner_device = self._inner_device_manager.get_input_device(name)
        return ShortCircuitInputDevice(self,
                                       name,
                                       inner_device,
                                       self._short_circuit_fail_count,
                                       self._short_circuit_fail_time)
