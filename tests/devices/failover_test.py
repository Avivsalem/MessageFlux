from io import BytesIO

from messageflux.iodevices.base import OutputDeviceException, Message
from messageflux.iodevices.failover_output_device_wrapper.failover_output_device import FailoverOutputDevice
from tests.devices.mocks import MockErrorOutputDevice


def test_sanity():
    stream = BytesIO(b'asdasdasd')
    mockdevice = MockErrorOutputDevice('mock')
    mockdevice.should_fail = False
    failover_device = MockErrorOutputDevice('failover')
    failover_device.should_fail = False
    food = FailoverOutputDevice(None, mockdevice, failover_device)
    food.send_message(Message(stream))
    assert mockdevice.sent is True
    assert failover_device.sent is False


def test_failover():
    stream = BytesIO(b'asdasdasd')
    mockdevice = MockErrorOutputDevice('mock')
    mockdevice.should_fail = True
    failover_device = MockErrorOutputDevice('failover')
    failover_device.should_fail = False
    food = FailoverOutputDevice(None, mockdevice, failover_device)
    food.send_message(Message(stream))
    assert mockdevice.sent is False
    assert failover_device.sent is True


def test_fail():
    stream = BytesIO(b'asdasdasd')
    mockdevice = MockErrorOutputDevice('mock')
    mockdevice.should_fail = True
    failover_device = MockErrorOutputDevice('failover')
    failover_device.should_fail = True
    food = FailoverOutputDevice(None, mockdevice, failover_device)
    try:
        food.send_message(Message(stream))
        assert False
    except OutputDeviceException:
        assert mockdevice.sent is False
        assert failover_device.sent is False
