from typing import Optional

from messageflux.iodevices.base import OutputDeviceManager, OutputDevice, MessageBundle
from messageflux.utils import RateLimiter


class RateLimitedOutputDevice(OutputDevice['RateLimitedOutputDeviceManager']):

    def __init__(self,
                 manager: 'RateLimitedOutputDeviceManager',
                 name: str,
                 inner_device: OutputDevice,
                 rate_limiter: RateLimiter,
                 timeout: Optional[float] = None
                 ):
        super().__init__(manager, name)
        self._rate_limiter = rate_limiter
        self._inner_device = inner_device
        self._timeout = timeout

    def _send_message(self, message_bundle: MessageBundle):
        timed_out = self._rate_limiter.perform_action(timeout=self._timeout)
        if timed_out:
            raise RateLimiter.RateActionTimeoutError('Rate Limited action was timed out')

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
                 timeout: Optional[float] = None,
                 **kwargs):
        """

        :param inner_device_manager: the inner device manager
        :param rate_limiter: the rate limiter to use
        :param timeout: a timeout to fail after if rate limit has reached
        """
        super().__init__(**kwargs)
        self._rate_limiter = rate_limiter
        self._inner_device_manager = inner_device_manager
        self._timeout = timeout

    def _create_output_device(self, name: str) -> RateLimitedOutputDevice:
        inner_device = self._inner_device_manager.get_output_device(name)
        return RateLimitedOutputDevice(manager=self,
                                       name=name,
                                       inner_device=inner_device,
                                       rate_limiter=self._rate_limiter,
                                       timeout=self._timeout)

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
