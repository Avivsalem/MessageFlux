import threading
from typing import Optional

from messageflux import InputDevice, ReadResult
from messageflux.iodevices.base import OutputDevice, InputDeviceManager, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle


class MockException(Exception):
    pass


class MockErrorInputDevice(InputDevice):

    def __init__(self, name):
        super(MockErrorInputDevice, self).__init__(MockErrorDeviceManager(), name)

        self.should_fail = True

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:
        if self.should_fail:
            raise MockException()
        else:
            return None


class MockErrorOutputDevice(OutputDevice):

    def __init__(self, name):
        super(MockErrorOutputDevice, self).__init__(MockErrorDeviceManager(), name)

        self.should_fail = True
        self.sent = False

    def _send_message(self, message_bundle: MessageBundle):
        if self.should_fail:
            raise MockException
        self.sent = True


class MockErrorDeviceManager(InputDeviceManager, OutputDeviceManager):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _create_input_device(self, name):
        return MockErrorInputDevice(name)

    def _create_output_device(self, name):
        return MockErrorOutputDevice(name)
