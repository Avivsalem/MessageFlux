import threading
from typing import Optional

import pytest
from time import sleep

from messageflux.iodevices.base import (InputDeviceManager,
                                        OutputDeviceManager,
                                        OutputDevice,
                                        InputDevice,
                                        Message,
                                        ReadResult)
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.short_circuit_device_wrapper import ShortCircuitInputDeviceManager, ShortCircuitException, \
    ShortCircuitOutputDeviceManager


class MyException(Exception):
    pass


# noinspection Mypy
class ErrorInputDevice(InputDevice):
    def __init__(self):
        # noinspection PyTypeChecker
        super(ErrorInputDevice, self).__init__(None, '')

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:
        if with_transaction:
            raise MyException()
        else:
            return None


class ErrorOutputDevice(OutputDevice):
    def __init__(self):
        # noinspection PyTypeChecker
        super(ErrorOutputDevice, self).__init__(None, '')

    def _send_message(self, message_bundle: MessageBundle):
        if message_bundle.device_headers.get("fail", True):
            raise MyException()


class ErrorDeviceManager(InputDeviceManager, OutputDeviceManager):
    def get_input_device(self, name: str) -> InputDevice:
        return ErrorInputDevice()

    def get_output_device(self, name: str) -> OutputDevice:
        return ErrorOutputDevice()


def test_sanity_input():
    cancellation_token = threading.Event()
    error_device_manager = ErrorDeviceManager()
    input_device_mananger = ShortCircuitInputDeviceManager(error_device_manager, 2, 2)
    input_device_mananger.connect()
    input_device = input_device_mananger.get_input_device("test")

    with pytest.raises(MyException):
        input_device.read_message(cancellation_token=cancellation_token)

    with pytest.raises(MyException):
        input_device.read_message(cancellation_token=cancellation_token)

    assert input_device.is_in_short_circuit_state

    with pytest.raises(ShortCircuitException):
        input_device.read_message(cancellation_token=cancellation_token)

    sleep(1)

    with pytest.raises(ShortCircuitException):
        input_device.read_message(cancellation_token=cancellation_token)

    sleep(1.1)

    with pytest.raises(MyException):
        input_device.read_message(cancellation_token=cancellation_token)

    input_device.read_message(cancellation_token=cancellation_token, with_transaction=False)

    with pytest.raises(MyException):
        input_device.read_message(cancellation_token=cancellation_token)

    with pytest.raises(MyException):
        input_device.read_message(cancellation_token=cancellation_token)


def test_sanity_output():
    error_device_manager = ErrorDeviceManager()
    output_device_manager = ShortCircuitOutputDeviceManager(error_device_manager, 2, 2)
    output_device_manager.connect()
    output_device = output_device_manager.get_output_device("test")

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))

    assert output_device.is_in_short_circuit_state

    with pytest.raises(ShortCircuitException):
        output_device.send_message(Message(b'bla'))

    sleep(1)

    with pytest.raises(ShortCircuitException):
        output_device.send_message(Message(b'bla'))

    sleep(1.1)

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))

    output_device.send_message(Message(b'bla'), {"fail": False})

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))
