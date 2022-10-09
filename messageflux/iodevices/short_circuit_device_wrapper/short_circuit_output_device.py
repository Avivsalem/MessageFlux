from messageflux.iodevices.base import OutputDevice, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.short_circuit_device_wrapper.common import ShortCircuitDeviceBase


class ShortCircuitOutputDevice(ShortCircuitDeviceBase, OutputDevice['ShortCircuitOutputDeviceManager']):
    """
    this is an output device manager that wraps input devices in short circuit input devices
    """

    def __init__(self,
                 manager: 'ShortCircuitOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
        OutputDevice.__init__(self, manager, name)
        ShortCircuitDeviceBase.__init__(self, short_circuit_fail_count, short_circuit_time)
        self._inner_device = inner_device

    def _send_message(self, message_bundle: MessageBundle):
        self._validate_short_circuit()
        with self._failure_count_context():
            self._inner_device.send_message(message_bundle.message, message_bundle.device_headers)

    def close(self):
        """
        closes the inner device
        """
        self._inner_device.close()


class ShortCircuitOutputDeviceManager(OutputDeviceManager[ShortCircuitOutputDevice]):
    """
    this is an output device manager that wraps output devices in short circuit output devices
    """

    def __init__(self,
                 inner_device_manager: OutputDeviceManager,
                 short_circuit_fail_count: int,
                 short_circuit_time: int):
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

    def get_output_device(self, name: str) -> ShortCircuitOutputDevice:
        """
        returns a wrapped output device
        :param name: the name of the output device to get
        """
        inner_device = self._inner_device_manager.get_output_device(name)
        return ShortCircuitOutputDevice(self,
                                        name,
                                        inner_device,
                                        self._short_circuit_fail_count,
                                        self._short_circuit_fail_time)
