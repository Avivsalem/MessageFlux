from os import scandir

import os
import socket
from typing import List, Optional

from baseservice.iodevices.file_system.file_system_serializer import FileSystemSerializerBase, \
    DefaultFileSystemSerializer
from baseservice.utils import get_random_id, KwargsException
from baseservice.utils.filesystem import create_dir_if_not_exists


class FileSystemDeviceManagerBase:
    """
    this manager is used to create FileSystem IO Devices
    """
    DEFAULT_QUEUES_SUB_DIR = 'QUEUES'

    def __init__(self,
                 root_folder: str,
                 queue_dir_name: str = DEFAULT_QUEUES_SUB_DIR,
                 tmp_dir: Optional[str] = None,
                 bookkeeping_path: Optional[str] = None,
                 serializer: Optional[FileSystemSerializerBase] = None):
        """
        ctor

        :param root_folder: the root folder to read/write from
        :param queue_dir_name: the name of the subdirectory under root_folder that holds the queues
        :param tmp_dir: the full path of directory to use for temp files (None will generate a default under root_path)
        """
        self._root_folder = root_folder
        if tmp_dir is None:
            tmp_dir = os.path.join(root_folder, 'TMP')
        self._tmp_folder = tmp_dir
        if bookkeeping_path is None:
            bookkeeping_path = os.path.join(root_folder, 'BOOKKEEPING')

        self._bookkeeping_folder = bookkeeping_path

        self._queues_folder = os.path.join(root_folder, queue_dir_name)
        self._unique_manager_id = f'{socket.gethostname()}-{get_random_id()}'
        self._serializer = serializer or DefaultFileSystemSerializer()

    @property
    def unique_manager_id(self):
        return self._unique_manager_id

    @property
    def bookkeeping_folder(self):
        return self._bookkeeping_folder

    @property
    def queues_folder(self) -> str:
        """
        the queues folder for this manager
        :return: the queues folder for this manager
        """
        return self._queues_folder

    @property
    def tmp_folder(self) -> str:
        """
        the temp folder for this manager
        :return: the temp folder for this manager
        """
        return self._tmp_folder

    def list_available_devices(self) -> List[str]:
        available_devices = []
        for dir_entry in scandir(self._queues_folder):
            if dir_entry.is_dir():
                available_devices.append(dir_entry.name)
        return available_devices

    def connect(self):
        """
        connects to device manager
        """
        try:
            create_dir_if_not_exists(self._tmp_folder)
            create_dir_if_not_exists(self._queues_folder)
            create_dir_if_not_exists(self._bookkeeping_folder)
        except Exception as e:
            raise KwargsException('Error creating directories') from e  # TODO: raise another type of exception

    def close(self):
        """
        closes the connection to IODeviceManager
        """
        pass
