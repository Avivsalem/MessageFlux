import logging
import os
import posixpath
import random
from datetime import datetime

from baseservice.iodevices.base import Message
from baseservice.iodevices.message_store_device_wrapper.message_store_base import MessageStoreBase
from baseservice.utils import get_random_id
from baseservice.utils.filesystem import create_dir_if_not_exists


class FileSystemMessageStore(MessageStoreBase):
    """
    used as a message store, backed by filesystem
    """

    DATE_FORMAT = "%Y-%m-%d"

    def __init__(self,
                 root_folder: str,
                 num_of_subdirs: int = 4000):
        """
        :param root_folder: the root folders to use
        :param num_of_subdirs: number of subdirectories to create under each date in root_folder
        """
        self._root_folder = root_folder
        self._logger = logging.getLogger(__name__)
        self._num_of_subdirs = num_of_subdirs

    def connect(self):
        """
        connects to Message Store
        """
        create_dir_if_not_exists(self._root_folder)

    def disconnect(self):
        """
        closes the connection to Message Store
        """
        pass

    @property
    def magic(self) -> bytes:
        """
        return a magic string that is unique and constant for this message store

        """
        return b"__FS_MSGSTORE__"

    def read_message(self, key: str) -> Message:
        """
        reads a message according to the key given

        :param str key: the key to the message
        :return: the Message
        """
        file_path = self.get_absolute_path(key)
        with open(file_path, 'rb') as f:
            data = f.read()

        return Message(data)

    def generate_relative_path(self) -> str:
        """
        Generate a relative path for the file to be saved as well as loaded from a different machine later.
        Notice the path will always be in posix format regardless of the os in order to be consistent.

        :return: The relative path to use together with the root path in order to save the file.
        """
        filename = get_random_id() + ".FSMS"
        current_date = datetime.now().strftime(self.DATE_FORMAT)
        random_str = str(random.randint(0, self._num_of_subdirs))
        subdir = f'{current_date}-{random_str}'
        return posixpath.join(subdir, filename)

    def get_absolute_path(self, relative_path: str) -> str:
        """
        Convert a relative path that was returned by "generate_relative_path" into the path where the file is at.

        :param relative_path: The key that was returned by "generate_relative_path"
        :return: A path where the original file can be written/read/deleted.
        """
        return os.path.join(self._root_folder, os.path.normpath(relative_path))

    def put_message(self, device_name: str, message: Message) -> str:
        """
        puts a message in the message store

        :param str device_name: the name of the device putting the item in the store
        :param message: the Message to write to the store
        :return: the key to the message in the message store
        """
        # TODO: maybe add the device name to path? need to sanitize it so it's legal dir name
        relative_path = self.generate_relative_path()
        file_path = self.get_absolute_path(relative_path)

        create_dir_if_not_exists(os.path.dirname(file_path))

        with open(file_path, 'wb') as f:
            f.write(message.bytes)

        os.chmod(file_path, 0o777)
        return relative_path

    def delete_message(self, key: str):
        """
        deletes a message from the message store

        :param str key: the key to the message
        """
        file_path = self.get_absolute_path(key)
        os.remove(file_path)
        subdir = os.path.dirname(file_path)
        try:
            os.rmdir(subdir)
        except OSError:
            self._logger.warning(f'Could not delete directory {subdir}', exc_info=True)
