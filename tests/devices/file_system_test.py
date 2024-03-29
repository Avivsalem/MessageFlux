import os
from io import BytesIO
from threading import Event
from uuid import uuid4

from messageflux.iodevices.base import InputTransactionScope, Message
from messageflux.iodevices.file_system import FileSystemInputDeviceManager, FileSystemOutputDeviceManager, \
    NoHeadersFileSystemSerializer, DefaultFileSystemSerializer
from messageflux.iodevices.file_system.file_system_device_manager_base import FileSystemDeviceManagerBase
from messageflux.iodevices.file_system.file_system_input_device import TransactionLog
from messageflux.iodevices.file_system.file_system_serializer import ZIPFileSystemSerializer, ConcatFileSystemSerializer
from tests.devices.common import sanity_test, rollback_test

QUEUE_NAME = "Test"
OUTPUT_NAME = "OUTPUT"


def test_folder_creation(tmpdir):
    tmpdir = str(tmpdir)
    manager = FileSystemInputDeviceManager(tmpdir)
    with manager:
        _ = manager.get_input_device(QUEUE_NAME)
    assert FileSystemDeviceManagerBase.DEFAULT_QUEUES_SUB_DIR in os.listdir(tmpdir)
    assert QUEUE_NAME in os.listdir(manager.queues_folder)


def test_generic_sanity(tmpdir):
    input_manager = FileSystemInputDeviceManager(tmpdir)
    output_manager = FileSystemOutputDeviceManager(tmpdir)
    sanity_test(input_manager, output_manager, sleep_between_sends=1)


def test_generic_rollback(tmpdir):
    input_manager = FileSystemInputDeviceManager(tmpdir)
    output_manager = FileSystemOutputDeviceManager(tmpdir)
    rollback_test(input_manager, output_manager, sleep_between_sends=1)


def test_sanity(tmpdir):
    tmpdir = str(tmpdir)
    serializer = NoHeadersFileSystemSerializer()
    input_manager = FileSystemInputDeviceManager(tmpdir, serializer=serializer)
    output_manager = FileSystemOutputDeviceManager(tmpdir, serializer=serializer)
    os.makedirs(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file.txt'), 'wb') as f:
        f.write(b'data')

    with input_manager:
        input_device = input_manager.get_input_device(QUEUE_NAME)
        read_result = input_device.read_message(cancellation_token=Event(), with_transaction=False)
        assert read_result is not None
        assert read_result.message.stream.read() == b'data'
        read_result.message.stream.seek(0)

    with output_manager:
        output_device = output_manager.get_output_device(OUTPUT_NAME)
        output_device.send_message(read_result.message)

    assert not os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    assert not os.listdir(input_manager.tmp_folder)
    assert len(os.listdir(os.path.join(input_manager.queues_folder, OUTPUT_NAME))) == 1


def test_sanity_unsorted(tmpdir):
    tmpdir = str(tmpdir)
    input_manager = FileSystemInputDeviceManager(tmpdir, fifo=False)
    output_manager = FileSystemOutputDeviceManager(tmpdir)
    serializer = DefaultFileSystemSerializer()

    os.makedirs(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file.txt'), 'wb') as f:
        f.write(serializer.serialize(Message(b'data')).read())

    with input_manager:
        input_device = input_manager.get_input_device(QUEUE_NAME)
        read_result = input_device.read_message(cancellation_token=Event(), with_transaction=False)
        assert read_result is not None
    with output_manager:
        output_device = output_manager.get_output_device(OUTPUT_NAME)
        output_device.send_message(read_result.message)

    assert not os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    assert not os.listdir(input_manager.tmp_folder)
    assert len(os.listdir(os.path.join(input_manager.queues_folder, OUTPUT_NAME))) == 1


def test_rollback(tmpdir):
    tmpdir = str(tmpdir)
    input_manager = FileSystemInputDeviceManager(tmpdir)
    serializer = DefaultFileSystemSerializer()

    os.makedirs(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file1.txt'), 'wb') as f:
        f.write(serializer.serialize(Message(b'data')).read())

    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file2.txt'), 'wb') as f:
        f.write(serializer.serialize(Message(b'data')).read())
    try:
        with input_manager:
            cancellation_token = Event()
            input_device = input_manager.get_input_device(QUEUE_NAME)
            with InputTransactionScope(input_device) as transaction_scope:
                _ = transaction_scope.read_message(cancellation_token=cancellation_token)
                _ = transaction_scope.read_message(cancellation_token=cancellation_token)
                assert len(os.listdir(input_manager.bookkeeping_folder)) == 1
                tran_log = TransactionLog._load_file(
                    os.path.join(input_manager.bookkeeping_folder, os.listdir(input_manager.bookkeeping_folder)[0]))
                assert len(tran_log) == 2
                raise Exception()
    except Exception:
        pass

    assert 'test_input_file1.txt' in os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    assert 'test_input_file2.txt' in os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME))

    assert not os.listdir(input_manager.tmp_folder)


def test_backout(tmpdir):
    tmpdir = str(tmpdir)
    input_manager = FileSystemInputDeviceManager(tmpdir)
    serializer = DefaultFileSystemSerializer()

    os.makedirs(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file.txt'), 'wb') as f:
        f.write(serializer.serialize(Message(b'data')).read())

    with input_manager:
        cancellation_token = Event()
        for i in range(3):
            queue = input_manager.get_input_device(QUEUE_NAME)
            read_result = queue.read_message(cancellation_token=cancellation_token)
            assert read_result is not None
            read_result.rollback()

        queue = input_manager.get_input_device(QUEUE_NAME)
        read_result = queue.read_message(cancellation_token=cancellation_token, timeout=1)
        assert read_result is None

        assert len(os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'POISON'))) == 1


def test_output_file_system_manager_format(tmpdir):
    tmpdir = str(tmpdir)
    output_manager = FileSystemOutputDeviceManager(tmpdir, queue_dir_name='', output_filename_format='{filename}')
    serializer = DefaultFileSystemSerializer()

    with output_manager:
        output_device = output_manager.get_output_device('test')
        output_device.send_message(Message(BytesIO(b'ABC'), {"filename": 'abc.txt'}))

    assert len(os.listdir(os.path.join(tmpdir, 'test'))) == 1
    with open(os.path.join(tmpdir, 'test', 'abc.txt'), "rb") as f:
        assert serializer.deserialize(f).bytes == b'ABC'


def test_zip_serializer():
    serializer = ZIPFileSystemSerializer()
    input_message = Message(uuid4().bytes * 1024, headers={'test': 'foo'})
    stream = serializer.serialize(input_message)
    output_message = serializer.deserialize(stream)

    assert input_message == output_message


def test_concat_serializer():
    serializer = ConcatFileSystemSerializer()
    input_message = Message(uuid4().bytes * 1024, headers={'test': 'foo'})
    stream = serializer.serialize(input_message)
    output_message = serializer.deserialize(stream)

    assert input_message == output_message
