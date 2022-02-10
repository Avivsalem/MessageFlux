from time import sleep
from typing import Optional

import pytest

from baseservice.iodevices.base import (InputDeviceManager,
                                        OutputDeviceManager,
                                        OutputDevice,
                                        InputDevice,
                                        Message,
                                        DeviceHeaders,
                                        ReadMessageResult, EMPTY_RESULT)
from baseservice.iodevices.short_circuit_device_wrapper import ShortCircuitInputDeviceManager, ShortCircuitException, \
    ShortCircuitOutputDeviceManager


class MyException(Exception):
    pass


class ErrorDevice(InputDevice, OutputDevice):
    def __init__(self):
        InputDevice.__init__(self, None, None)
        OutputDevice.__init__(self, None, None)

    def _read_message(self, timeout: Optional[float] = 0, with_transaction: bool = True) -> ReadMessageResult:
        if with_transaction:
            raise MyException()
        else:
            return EMPTY_RESULT

    def _send_message(self, message: Message, device_headers: DeviceHeaders):
        if device_headers.get("fail", True):
            raise MyException()


class ErrorDeviceManager(InputDeviceManager, OutputDeviceManager):
    def get_input_device(self, name: str) -> InputDevice:
        return ErrorDevice()

    def get_output_device(self, name: str) -> OutputDevice:
        return ErrorDevice()


def test_sanity_input():
    error_device_manager = ErrorDeviceManager()
    input_device_mananger = ShortCircuitInputDeviceManager(error_device_manager, 2, 2)
    input_device_mananger.connect()
    input_device = input_device_mananger.get_input_device("test")

    with pytest.raises(MyException):
        input_device.read_message()

    with pytest.raises(MyException):
        input_device.read_message()

    assert input_device.is_in_short_circuit_state

    with pytest.raises(ShortCircuitException):
        input_device.read_message()

    sleep(1)

    with pytest.raises(ShortCircuitException):
        input_device.read_message()

    sleep(1)

    with pytest.raises(MyException):
        input_device.read_message()

    input_device.read_message(with_transaction=False)

    with pytest.raises(MyException):
        input_device.read_message()

    with pytest.raises(MyException):
        input_device.read_message()


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

    sleep(1)

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))

    output_device.send_message(Message(b'bla'), {"fail": False})

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))

    with pytest.raises(MyException):
        output_device.send_message(Message(b'bla'))
