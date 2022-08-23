from typing import Optional

from baseservice.iodevices.base import InputDevice, ReadMessageResult, InputDeviceManager
from baseservice.iodevices.short_circuit_device_wrapper.common import ShortCircuitDeviceBase


class ShortCircuitInputDevice(ShortCircuitDeviceBase, InputDevice):
    def __init__(self,
                 manager: 'ShortCircuitInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        InputDevice.__init__(self, manager, name)
        ShortCircuitDeviceBase.__init__(self, short_circuit_fail_count, short_circuit_time)
        self._inner_device = inner_device

    def _read_message(self, timeout: Optional[float] = 0, with_transaction: bool = True) -> ReadMessageResult:
        self._validate_short_circuit()
        try:
            result = self._inner_device.read_message(timeout=timeout, with_transaction=with_transaction)
        except Exception:
            self._report_failure()
            raise
        else:
            self._report_success()

        return result


class ShortCircuitInputDeviceManager(InputDeviceManager):
    def __init__(self,
                 inner_device_manager: InputDeviceManager,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        self._inner_device_manager = inner_device_manager
        self._short_circuit_fail_count = short_circuit_fail_count
        self._short_circuit_fail_time = short_circuit_time

    def connect(self):
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()

    def get_input_device(self, name: str) -> InputDevice:
        inner_device = self._inner_device_manager.get_input_device(name)
        return ShortCircuitInputDevice(self,
                                       name,
                                       inner_device,
                                       self._short_circuit_fail_count,
                                       self._short_circuit_fail_time)
