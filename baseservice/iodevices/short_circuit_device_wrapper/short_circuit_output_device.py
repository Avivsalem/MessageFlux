from baseservice.iodevices.base import OutputDevice, OutputDeviceManager, Message, DeviceHeaders
from baseservice.iodevices.short_circuit_device_wrapper.common import ShortCircuitDeviceBase


class ShortCircuitOutputDevice(ShortCircuitDeviceBase, OutputDevice):
    def __init__(self,
                 manager: 'ShortCircuitOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        OutputDevice.__init__(self, manager, name)
        ShortCircuitDeviceBase.__init__(self, short_circuit_fail_count, short_circuit_time)
        self._inner_device = inner_device

    def _send_message(self, message: Message, device_headers: DeviceHeaders):
        self._validate_short_circuit()
        try:
            self._inner_device.send_message(message, device_headers)
        except Exception:
            self._report_failure()
            raise
        else:
            self._report_success()


class ShortCircuitOutputDeviceManager(OutputDeviceManager):
    def __init__(self,
                 inner_device_manager: OutputDeviceManager,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        self._inner_device_manager = inner_device_manager
        self._short_circuit_fail_count = short_circuit_fail_count
        self._short_circuit_fail_time = short_circuit_time

    def connect(self):
        self._inner_device_manager.connect()

    def disconnect(self):
        self._inner_device_manager.disconnect()

    def get_output_device(self, name: str) -> OutputDevice:
        inner_device = self._inner_device_manager.get_output_device(name)
        return ShortCircuitOutputDevice(self,
                                        name,
                                        inner_device,
                                        self._short_circuit_fail_count,
                                        self._short_circuit_fail_time)
