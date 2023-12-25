import threading
from typing import Optional

from messageflux.iodevices.base import InputDeviceManager, InputDevice, ReadResult
from messageflux.utils import RateLimiter, TimerContext


class RateLimitedInputDevice(InputDevice['RateLimitedInputDeviceManager']):

    def __init__(self,
                 manager: 'RateLimitedInputDeviceManager',
                 name: str,
                 inner_device: InputDevice,
                 rate_limiter: RateLimiter
                 ):
        super().__init__(manager, name)
        self._rate_limiter = rate_limiter
        self._inner_device = inner_device

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:

        with TimerContext() as timer:
            timed_out = self._rate_limiter.perform_action(timeout=timeout)
            if timed_out:
                return None

        if timeout is not None:
            timeout -= timer.elapsed_seconds

        return self._inner_device.read_message(cancellation_token=cancellation_token,
                                               timeout=timeout,
                                               with_transaction=with_transaction)

    def close(self):
        """
        closes the inner device
        """
        super().close()
        self._inner_device.close()


class RateLimitedInputDeviceManager(InputDeviceManager[RateLimitedInputDevice]):
    def __init__(self,
                 inner_device_manager: InputDeviceManager,
                 rate_limiter: RateLimiter,
                 **kwargs):
        super().__init__(**kwargs)
        self._rate_limiter = rate_limiter
        self._inner_device_manager = inner_device_manager

    def _create_input_device(self, name: str) -> RateLimitedInputDevice:
        inner_device = self._inner_device_manager.get_input_device(name)
        return RateLimitedInputDevice(manager=self,
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
