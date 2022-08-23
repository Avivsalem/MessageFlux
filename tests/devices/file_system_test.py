import os
from io import BytesIO

from baseservice.iodevices.base import InputTransactionScope, Message
from baseservice.iodevices.file_system import FileSystemInputDeviceManager, FileSystemOutputDeviceManager, \
    NoHeadersFileSystemSerializer, DefaultFileSystemSerializer
from baseservice.iodevices.file_system.file_system_device_manager_base import FileSystemDeviceManagerBase
from baseservice.iodevices.file_system.file_system_input_device import TransactionLog
from tests.devices.common import sanity_test

QUEUE_NAME = "Test"
OUTPUT_NAME = "OUTPUT"


def test_folder_creation(tmpdir):
    tmpdir = str(tmpdir)
    manager = FileSystemInputDeviceManager(tmpdir)
    with manager:
        device = manager.get_input_device(QUEUE_NAME)
    assert FileSystemDeviceManagerBase.DEFAULT_QUEUES_SUB_DIR in os.listdir(tmpdir)
    assert QUEUE_NAME in os.listdir(manager.queues_folder)


def test_generic_sanity(tmpdir):
    input_manager = FileSystemInputDeviceManager(tmpdir)
    output_manager = FileSystemOutputDeviceManager(tmpdir)
    sanity_test(input_manager, output_manager, sleep_between_sends=1.5)


def rollback_test(tmpdir):
    input_manager = FileSystemInputDeviceManager(tmpdir)
    output_manager = FileSystemOutputDeviceManager(tmpdir)
    sanity_test(input_manager, output_manager, sleep_between_sends=1.5)


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
        msg: Message
        msg, _, _ = input_device.read_message(with_transaction=False)
        assert msg is not None
        assert msg.stream.read() == b'data'
        msg.stream.seek(0)

    with output_manager:
        output_device = output_manager.get_output_device(OUTPUT_NAME)
        output_device.send_message(msg)

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
        f.write(serializer.serialize(data=BytesIO(b'data')).read())

    with input_manager:
        input_device = input_manager.get_input_device(QUEUE_NAME)
        msg, _, _ = input_device.read_message(with_transaction=False)
    with output_manager:
        output_device = output_manager.get_output_device(OUTPUT_NAME)
        output_device.send_message(msg)

    assert not os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    assert not os.listdir(input_manager.tmp_folder)
    assert len(os.listdir(os.path.join(input_manager.queues_folder, OUTPUT_NAME))) == 1


def test_rollback(tmpdir):
    tmpdir = str(tmpdir)
    input_manager = FileSystemInputDeviceManager(tmpdir)
    serializer = DefaultFileSystemSerializer()

    os.makedirs(os.path.join(input_manager.queues_folder, QUEUE_NAME))
    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file1.txt'), 'wb') as f:
        f.write(serializer.serialize(data=BytesIO(b'data')).read())

    with open(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'test_input_file2.txt'), 'wb') as f:
        f.write(serializer.serialize(data=BytesIO(b'data')).read())
    try:
        with input_manager:
            input_device = input_manager.get_input_device(QUEUE_NAME)
            with InputTransactionScope(input_device) as transaction_scope:
                msg, _ = transaction_scope.read_message()
                msg, _ = transaction_scope.read_message()
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
        f.write(serializer.serialize(data=BytesIO(b'data')).read())

    with input_manager:
        for i in range(3):
            queue = input_manager.get_input_device(QUEUE_NAME)
            product, meta, transaction = queue.read_message()
            assert product is not None
            transaction.rollback()

        queue = input_manager.get_input_device(QUEUE_NAME)
        product, meta, _ = queue.read_message()
        assert product is None

        assert len(os.listdir(os.path.join(input_manager.queues_folder, QUEUE_NAME, 'BACKOUT'))) == 1


def test_output_file_system_manager_format(tmpdir):
    tmpdir = str(tmpdir)
    output_manager = FileSystemOutputDeviceManager(tmpdir, queue_dir_name='', output_filename_format='{filename}')
    serializer = DefaultFileSystemSerializer()

    with output_manager:
        output_device = output_manager.get_output_device('test')
        output_device.send_message(Message(BytesIO(b'ABC'), {"filename": 'abc.txt'}))

    assert len(os.listdir(os.path.join(tmpdir, 'test'))) == 1
    with open(os.path.join(tmpdir, 'test', 'abc.txt'), "rb") as f:
        assert serializer.deserialize(f)[0].read() == b'ABC'
