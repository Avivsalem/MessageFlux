import logging
import os
import sys
import threading
import time
from typing import Optional

from messageflux import InputDevice, ReadResult
from messageflux.iodevices.base import OutputDevice, InputDeviceManager, OutputDeviceManager, InputTransactionScope
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.file_system import FileSystemInputDeviceManager, NoHeadersFileSystemSerializer, \
    FileSystemOutputDeviceManager
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager
from messageflux.iodevices.round_robin_device_wrapper import RoundRobinInputDeviceManager, RoundRobinOutputDeviceManager
from messageflux.utils import AggregatedException

DEFAULT_LOG_FORMATTER = logging.Formatter(u"%(asctime)-15s %(levelname)s %(message)s")
DEFAULT_LOG_HANDLER = logging.StreamHandler(sys.stdout)
DEFAULT_LOG_HANDLER.setFormatter(DEFAULT_LOG_FORMATTER)


class ErrorIODevice(OutputDevice, InputDevice):
    def _send_message(self, message_bundle: MessageBundle):
        raise Exception('MOCK read ERROR')

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        raise Exception('MOCK send ERROR')


class ErrorIODeviceManager(InputDeviceManager, OutputDeviceManager):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _create_input_device(self, name):
        return ErrorIODevice(None, name)

    def _create_output_device(self, name):
        return ErrorIODevice(None, name)


def test_sanity(tmpdir):
    cancellation_token = threading.Event()
    logger = logging.getLogger()
    logger.addHandler(DEFAULT_LOG_HANDLER)
    logger.setLevel(logging.INFO)

    tmpdir = str(tmpdir)
    input_fs_manager = FileSystemInputDeviceManager(tmpdir, serializer=NoHeadersFileSystemSerializer())
    output_fs_manager = FileSystemOutputDeviceManager(tmpdir, serializer=NoHeadersFileSystemSerializer())
    manager2 = ErrorIODeviceManager()
    rri_manager = RoundRobinInputDeviceManager([input_fs_manager, manager2])
    rro_manager = RoundRobinOutputDeviceManager([output_fs_manager, manager2])
    os.makedirs(os.path.join(input_fs_manager.queues_folder, 'MyTest'))
    output_fs_manager.connect()
    od = output_fs_manager.get_output_device('MyTest')
    od.send_message(Message(b'data1'))
    time.sleep(0.1)
    od.send_message(Message(b'data2'))

    with rri_manager as manager:
        input_device = manager.get_input_device('MyTest')
        with InputTransactionScope(input_device) as transaction_scope:
            res = transaction_scope.read_message(cancellation_token=cancellation_token)
            assert res.message.bytes == b'data1'
            res = transaction_scope.read_message(cancellation_token=cancellation_token)
            assert res.message.bytes == b'data2'
            res = transaction_scope.read_message(cancellation_token=cancellation_token, timeout=0)
            assert res is None

        assert not os.listdir(os.path.join(input_fs_manager.queues_folder, 'MyTest'))
        assert not os.listdir(input_fs_manager.tmp_folder)

    with rro_manager as manager:
        output_device = manager.get_output_device('MyTest')
        output_device.send_message(Message(b'test1'))
        output_device.send_message(Message(b'test2'))
        output_device.send_message(Message(b'test3'))
        assert len(os.listdir(os.path.join(output_fs_manager.queues_folder, 'MyTest'))) == 3


def test_order_input():
    cancellation_token = threading.Event()
    device_name = 'TEST'
    manager1 = InMemoryDeviceManager()
    manager2 = InMemoryDeviceManager()
    manager3 = InMemoryDeviceManager()
    rri_manager = RoundRobinInputDeviceManager([manager1, manager2, manager3])

    t = manager1.get_output_device(device_name)
    t.send_message(Message(b'1'))
    t.send_message(Message(b'1a'))
    t.send_message(Message(b'1b'))

    t = manager2.get_output_device(device_name)
    t.send_message(Message(b'2'))
    t.send_message(Message(b'2a'))
    t.send_message(Message(b'2b'))
    t.send_message(Message(b'2c'))

    t = manager3.get_output_device(device_name)
    t.send_message(Message(b'3'))
    t.send_message(Message(b'3a'))
    t.send_message(Message(b'3b'))

    rrt = rri_manager.get_input_device(device_name)
    streams = []
    for i in range(3):
        res = rrt.read_message(cancellation_token=cancellation_token, timeout=0, with_transaction=False)
        if res is None:
            continue
        streams.append(res.message.bytes.decode())

    streams.sort()
    assert streams == ['1', '2', '3']

    streams = []
    for i in range(3):
        res = rrt.read_message(cancellation_token=cancellation_token, timeout=0, with_transaction=False)
        if res is None:
            continue
        streams.append(res.message.bytes.decode())

    streams.sort()
    assert streams == ['1a', '2a', '3a']

    streams = []
    for i in range(3):
        res = rrt.read_message(cancellation_token=cancellation_token, timeout=0, with_transaction=False)
        if res is None:
            continue
        streams.append(res.message.bytes.decode())

    streams.sort()
    assert streams == ['1b', '2b', '3b']

    streams = []
    for i in range(3):
        res = rrt.read_message(cancellation_token=cancellation_token, timeout=0, with_transaction=False)
        if res is None:
            continue
        streams.append(res.message.bytes.decode())

    streams.sort()
    assert streams == ['2c']


def test_order_output():
    device_name = 'TEST'
    manager1 = InMemoryDeviceManager()
    manager2 = InMemoryDeviceManager()
    manager3 = InMemoryDeviceManager()
    rro_manager = RoundRobinOutputDeviceManager([manager1, manager2, manager3])
    rrd = rro_manager.get_output_device(device_name)
    rrd.send_message(Message(b'1'))
    rrd.send_message(Message(b'2'))
    rrd.send_message(Message(b'3'))
    rrd.send_message(Message(b'4'))
    rrd.send_message(Message(b'5'))
    rrd.send_message(Message(b'6'))
    rrd.send_message(Message(b'7'))

    idd = manager1.get_input_device(device_name)
    streams = []
    for i in range(2):
        res = idd.read_message(cancellation_token=threading.Event(), timeout=0, with_transaction=False)
        if res is None:
            continue
        streams.append(res.message.bytes.decode())

    streams.sort()
    assert streams in [['1', '4'], ['2', '5'], ['3', '6']]


def test_all_error():
    logger = logging.getLogger()
    logger.addHandler(DEFAULT_LOG_HANDLER)
    logger.setLevel(logging.INFO)
    manager1 = ErrorIODeviceManager()
    manager2 = ErrorIODeviceManager()
    rri_manager = RoundRobinInputDeviceManager([manager1, manager2])
    rro_manager = RoundRobinOutputDeviceManager([manager1, manager2])

    with rri_manager as manager:
        input_device = manager.get_input_device('MyTest')
        try:
            _ = input_device.read_message(cancellation_token=threading.Event(), timeout=0, with_transaction=False)
        except Exception as ex:
            assert isinstance(ex, AggregatedException)
            assert len(ex.inner_exceptions) == 2

    with rro_manager as manager:
        output_device = manager.get_output_device('MyTest')
        try:
            output_device.send_message(Message(b'test1'))
        except Exception as ex:
            assert isinstance(ex, AggregatedException)
            assert len(ex.inner_exceptions) == 2
