from messageflux.iodevices.base import OutputDeviceManager, OutputDevice, MessageBundle
from messageflux.utils import RateLimiter


class RateLimitedOutputDevice(OutputDevice['RateLimitedOutputDeviceManager']):

    def __init__(self,
                 manager: 'RateLimitedOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 rate_limiter: RateLimiter
                 ):
        super().__init__(manager, name)
        self._rate_limiter = rate_limiter
        self._inner_device = inner_device

    def _send_message(self, message_bundle: MessageBundle):
        with self._rate_limiter:
            self._inner_device.send_message(message_bundle.message, message_bundle.device_headers)

    def close(self):
        """
        closes the inner device
        """
        super().close()
        self._inner_device.close()


class RateLimitedOutputDeviceManager(OutputDeviceManager[RateLimitedOutputDevice]):
    def __init__(self,
                 inner_device_manager: OutputDeviceManager,
                 rate_limiter: RateLimiter,
                 **kwargs):
        super().__init__(**kwargs)
        self._rate_limiter = rate_limiter
        self._inner_device_manager = inner_device_manager

    def _create_output_device(self, name: str) -> RateLimitedOutputDevice:
        inner_device = self._inner_device_manager.get_output_device(name)
        return RateLimitedOutputDevice(manager=self,
                                       name=name,
                                       inner_device=inner_device,
                                       rate_limiter=self._rate_limiter)

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
