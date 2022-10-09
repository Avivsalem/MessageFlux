import os
import socket
from os import scandir
from typing import List, Optional

from messageflux.iodevices.file_system.file_system_serializer import FileSystemSerializerBase, \
    DefaultFileSystemSerializer
from messageflux.utils import get_random_id, KwargsException


class FileSystemDeviceManagerBase:
    """
    this manager is used to create IO Devices over a shared file system
    """
    DEFAULT_QUEUES_SUB_DIR = 'QUEUES'
    DEFAULT_BOOKKEEPING_SUB_DIR = 'BOOKKEEPING'
    DEFAULT_TMPDIR_SUB_DIR = 'TMP'

    def __init__(self,
                 root_folder: str,
                 queue_dir_name: str = DEFAULT_QUEUES_SUB_DIR,
                 tmp_dir_name: str = DEFAULT_TMPDIR_SUB_DIR,
                 bookkeeping_dir_name: str = DEFAULT_BOOKKEEPING_SUB_DIR,
                 serializer: Optional[FileSystemSerializerBase] = None):
        """
        :param root_folder: the root folder to use for the manager
        :param queue_dir_name: the name of the subdirectory under root_folder that holds the queues
        :param tmp_dir_name: the name of the subdirectory under root_folder to use for temp files
        :param bookkeeping_dir_name: the name of the subdirectory under root_folder that holds the book-keeping data
        :param serializer: the serializer to use to write messages to files. None will use the default serializer
        """
        self._root_folder = root_folder
        self._queues_folder = os.path.join(root_folder, queue_dir_name)
        self._tmp_folder = os.path.join(root_folder, tmp_dir_name)
        self._bookkeeping_folder = os.path.join(root_folder, bookkeeping_dir_name)
        self._unique_manager_id = f'{socket.gethostname()}-{get_random_id()}'
        self._serializer = serializer or DefaultFileSystemSerializer()

    @property
    def unique_manager_id(self) -> str:
        """
        this is a unique id for this manager instance
        """
        return self._unique_manager_id

    @property
    def bookkeeping_folder(self) -> str:
        """
        the full path for the book keeping folder
        """
        return self._bookkeeping_folder

    @property
    def queues_folder(self) -> str:
        """
        the full path for the queues folder
        """
        return self._queues_folder

    @property
    def tmp_folder(self) -> str:
        """
        the full path for the temp folder
        """
        return self._tmp_folder

    def get_available_device_names(self) -> List[str]:
        """
        returns a list of available 'queues' in the queue folder
        """
        available_devices = []
        for dir_entry in scandir(self._queues_folder):
            if dir_entry.is_dir():
                available_devices.append(dir_entry.name)
        return available_devices

    def _create_all_directories(self):
        """
        connects to device manager
        """
        try:
            os.makedirs(self._tmp_folder, exist_ok=True)
            os.makedirs(self._queues_folder, exist_ok=True)
            os.makedirs(self._bookkeeping_folder, exist_ok=True)
        except Exception as e:
            raise KwargsException('Error creating directories') from e
