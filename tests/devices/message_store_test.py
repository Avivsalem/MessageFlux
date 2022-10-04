import os

from messageflux.iodevices.base import Message
from messageflux.iodevices.file_system import FileSystemInputDeviceManager, FileSystemOutputDeviceManager
from messageflux.iodevices.message_store_device_wrapper.file_system_message_store import FileSystemMessageStore
from messageflux.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase
from messageflux.iodevices.message_store_device_wrapper.message_store_input_device import \
    MessageStoreInputDeviceManagerWrapper
from messageflux.iodevices.message_store_device_wrapper.message_store_output_device import \
    MessageStoreOutputDeviceManagerWrapper

QUEUE_NAME = "Test"
OUTPUT_NAME = "OUTPUT"


def test_sanity(tmpdir):
    tmpdir = str(tmpdir)
    input_fs_manager = FileSystemInputDeviceManager(tmpdir)
    output_fs_manager = FileSystemOutputDeviceManager(tmpdir)
    msg_store_dir = os.path.join(tmpdir, "MSGSTORE")
    msg_store = FileSystemMessageStore(msg_store_dir)
    input_manager = MessageStoreInputDeviceManagerWrapper(input_fs_manager, msg_store)
    output_manager = MessageStoreOutputDeviceManagerWrapper(output_fs_manager, msg_store, size_threshold=-1)

    assert not os.path.exists(msg_store_dir)
    with output_manager:
        assert not os.listdir(msg_store_dir)
        output_device = output_manager.get_output_device(QUEUE_NAME)
        output_device.send_message(Message(b'BLA'))

    inner_folders = os.listdir(msg_store_dir)
    assert len(inner_folders) == 1
    assert len(os.listdir(os.path.join(msg_store_dir, inner_folders[0]))) == 1

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is not None
        read_result.commit()

    assert read_result.message.bytes == b"BLA"
    assert len(os.listdir(msg_store_dir)) == 0

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is None


class MockMessageStore(MessageStoreBase):

    def connect(self):
        pass

    def disconnect(self):
        pass

    @property
    def magic(self):
        return b"__MOCK_MS__"

    def read_message(self, key):
        raise Exception("ERROR")

    def put_message(self, device_name, message):
        raise Exception("ERROR")

    def delete_message(self, key):
        raise Exception("ERROR")


def test_chained_message_stores(tmpdir):
    tmpdir = str(tmpdir)
    input_fs_manager = FileSystemInputDeviceManager(tmpdir)
    output_fs_manager = FileSystemOutputDeviceManager(tmpdir)
    msg_store_dir = os.path.join(tmpdir, "MSGSTORE")
    msg_store = FileSystemMessageStore(msg_store_dir)
    mock_input_manager = MessageStoreInputDeviceManagerWrapper(input_fs_manager, MockMessageStore())
    mock_output_manager = MessageStoreOutputDeviceManagerWrapper(output_fs_manager, MockMessageStore(),
                                                                 size_threshold=1000)
    input_manager = MessageStoreInputDeviceManagerWrapper(mock_input_manager, msg_store)
    output_manager = MessageStoreOutputDeviceManagerWrapper(mock_output_manager, msg_store, size_threshold=-1)

    with output_manager:
        assert not os.listdir(msg_store_dir)
        output_device = output_manager.get_output_device(QUEUE_NAME)
        output_device.send_message(Message(b'BLA'))

    inner_folders = os.listdir(msg_store_dir)
    assert len(inner_folders) == 1
    assert len(os.listdir(os.path.join(msg_store_dir, inner_folders[0]))) == 1

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is not None
        read_result.commit()

    assert read_result.message.bytes == b"BLA"

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is None


def test_rollback(tmpdir):
    tmpdir = str(tmpdir)
    input_fs_manager = FileSystemInputDeviceManager(tmpdir)
    output_fs_manager = FileSystemOutputDeviceManager(tmpdir)
    msg_store_dir = os.path.join(tmpdir, "MSGSTORE")
    msg_store = FileSystemMessageStore(msg_store_dir)
    input_manager = MessageStoreInputDeviceManagerWrapper(input_fs_manager, msg_store)
    output_manager = MessageStoreOutputDeviceManagerWrapper(output_fs_manager, msg_store, size_threshold=-1)

    with output_manager:
        assert not os.listdir(msg_store_dir)
        output_device = output_manager.get_output_device(QUEUE_NAME)
        output_device.send_message(Message(b'BLA'))

    inner_folders = os.listdir(msg_store_dir)
    assert len(inner_folders) == 1
    assert len(os.listdir(os.path.join(msg_store_dir, inner_folders[0]))) == 1

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is not None
        read_result.rollback()

    assert read_result.message.bytes == b"BLA"
    assert len(os.listdir(msg_store_dir)) == 1
    assert len(os.listdir(os.path.join(msg_store_dir, inner_folders[0]))) == 1

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is not None
        read_result.commit()

    assert read_result.message.bytes == b"BLA"
    assert len(os.listdir(msg_store_dir)) == 0


def test_no_deletion(tmpdir):
    tmpdir = str(tmpdir)
    input_fs_manager = FileSystemInputDeviceManager(tmpdir)
    output_fs_manager = FileSystemOutputDeviceManager(tmpdir)
    msg_store_dir = os.path.join(tmpdir, "MSGSTORE")
    msg_store = FileSystemMessageStore(msg_store_dir)
    input_manager = MessageStoreInputDeviceManagerWrapper(input_fs_manager, msg_store, delete_on_commit=False)
    output_manager = MessageStoreOutputDeviceManagerWrapper(output_fs_manager, msg_store, size_threshold=-1)

    assert not os.path.exists(msg_store_dir)
    with output_manager:
        assert not os.listdir(msg_store_dir)
        output_device = output_manager.get_output_device(QUEUE_NAME)
        output_device.send_message(Message(b'BLA'))

    inner_folders = os.listdir(msg_store_dir)
    assert len(inner_folders) == 1
    assert len(os.listdir(os.path.join(msg_store_dir, inner_folders[0]))) == 1

    with input_manager:
        input_device = input_manager.get_aggregate_device([QUEUE_NAME])
        read_result = input_device.read_message(1)
        assert read_result is not None
        read_result.commit()

    assert read_result.message.bytes == b"BLA"
    assert len(os.listdir(msg_store_dir)) == 1
    assert len(os.listdir(os.path.join(msg_store_dir, inner_folders[0]))) == 1


THRESHOLD = 5


def test_threshold(tmpdir):
    tmpdir = str(tmpdir)
    output_fs_manager = FileSystemOutputDeviceManager(tmpdir)
    msg_store_dir = os.path.join(tmpdir, "MSGSTORE")
    msg_store = FileSystemMessageStore(msg_store_dir, num_of_subdirs=0)
    output_manager = MessageStoreOutputDeviceManagerWrapper(output_fs_manager,
                                                            msg_store,
                                                            size_threshold=THRESHOLD)

    assert not os.path.exists(msg_store_dir)
    with output_manager:
        assert not os.listdir(msg_store_dir)
        output_device = output_manager.get_output_device(QUEUE_NAME)
        # Above the threshold
        output_device.send_message(Message(b'X' * (THRESHOLD + 1)))

        inner_folders = os.listdir(msg_store_dir)
        assert len(inner_folders) == 1

        bucket_dir = os.path.join(msg_store_dir, inner_folders[0])
        assert len(os.listdir(bucket_dir)) == 1  # File Was Written

        # below the threshold - header: True
        output_device.send_message(Message(b'X' * (THRESHOLD - 1)))

        assert len(os.listdir(bucket_dir)) == 1  # File Was Not Written
