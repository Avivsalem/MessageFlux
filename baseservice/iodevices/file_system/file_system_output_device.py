import logging
import os
import shutil
from typing import BinaryIO, Optional, Dict, Any

from baseservice.iodevices.base import OutputDeviceManager, OutputDevice, Message, DeviceHeaders, OutputDeviceException
from baseservice.iodevices.file_system.file_system_device_manager_base import FileSystemDeviceManagerBase
from baseservice.iodevices.file_system.file_system_serializer import FileSystemSerializerBase, \
    DefaultFileSystemSerializer
from baseservice.metadata_headers import MetadataHeaders
from baseservice.utils import KwargsException, get_random_id
from baseservice.utils.filesystem import create_dir_if_not_exists


class FileSystemOutputDeviceManager(FileSystemDeviceManagerBase, OutputDeviceManager):
    def __init__(self,
                 root_folder: str,
                 queue_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_QUEUES_SUB_DIR,
                 tmp_dir: str = None,
                 serializer: Optional[FileSystemSerializerBase] = None,
                 output_filename_format: str = None,
                 bookkeeping_path: Optional[str] = None):
        """
        ctor

        :param root_folder: the root folder to read/write from
        :param queue_dir_name: the name of the subdirectory under root_folder that holds the queues
        :param tmp_dir: the full path of directory to use for temp files (None will generate a default under root_path)
        :param output_filename_format: the filename format to save a product, "{filename}-{item_id}"
        :param bookkeeping_path: optional path for the bookkeeping folder (it may be outside root folder)
        """
        super(FileSystemOutputDeviceManager, self).__init__(root_folder=root_folder,
                                                            queue_dir_name=queue_dir_name,
                                                            tmp_dir=tmp_dir,
                                                            bookkeeping_path=bookkeeping_path,
                                                            serializer=serializer)
        self._output_filename_format = output_filename_format

    def get_output_device(self, device_name: str) -> OutputDevice:
        """
        Returns an outgoing device by name

        :param device_name: the name of the device to write to
        :return: an outgoing device for 'device_name'
        """
        try:
            return FileSystemOutputDevice(device_manager=self,
                                          tmp_folder=self._tmp_folder,
                                          queues_folder=self._queues_folder,
                                          device_name=device_name,
                                          format_filename=self._output_filename_format,
                                          serializer=self._serializer)
        except Exception as e:
            raise KwargsException("Error getting output device") from e  # TODO: raise another type of exception


class FileSystemOutputDevice(OutputDevice[FileSystemOutputDeviceManager]):
    """
    And OutputDevice that writes to filesystem
    """

    def __init__(self,
                 device_manager: 'FileSystemOutputDeviceManager',
                 tmp_folder: str,
                 queues_folder: str,
                 device_name: str,
                 format_filename: str = None,
                 serializer: Optional[FileSystemSerializerBase] = None):
        """
        ctor

        :param tmp_folder: the folder to create temporary files in
        :param queues_folder: the base folder for all queues
        :param device_name: the device name (subdirectory)
        :param format_filename: the filename format to save the product, "{filename}-{item_id}"
        """
        super(FileSystemOutputDevice, self).__init__(device_manager, device_name)
        self._tmp_folder = tmp_folder
        self._output_folder = os.path.join(queues_folder, device_name)
        try:
            create_dir_if_not_exists(self._tmp_folder)
            create_dir_if_not_exists(self._output_folder)
        except Exception as e:
            raise OutputDeviceException('Error creating output device') from e

        self._format = format_filename
        self._logger = logging.getLogger(__name__)
        self._serializer = serializer or DefaultFileSystemSerializer()

    def _send_message(self, message: Message, device_headers: DeviceHeaders):
        item_id = device_headers.get(MetadataHeaders.ITEM_ID)
        self._logger.debug('Sending product to {} device'.format(self.name))
        self._save_to_folder(message.stream, item_id, message.headers)

    def _save_to_folder(self, stream: BinaryIO, item_id: str, headers: Dict[str, Any]):
        """
        saves the given product to the appropriate folder

        :param stream: the stream to be saved
        :param item_id: the item_id if exists
        """
        try:
            filename = ''
            try:
                if self._format:
                    filename = self._format.format(**headers)
            except KeyError:
                self._logger.error(
                    'Filename format isn\'t satisfied, format:{}, headers:{}, puts default filename instead'.format(
                        self._format, headers))
            if not filename:
                if item_id:
                    item_id = item_id + '-'
                filename = "{itemid}{uuid}.SBM".format(itemid=item_id if item_id else str(item_id),
                                                       uuid=get_random_id())
            tmp_fullpath = os.path.join(self._tmp_folder, filename)
            with open(tmp_fullpath, 'wb') as f:
                stream_to_write = self._serializer.serialize(data=stream, headers=headers)
                f.write(stream_to_write.read())

            os.chmod(tmp_fullpath, 0o777)
            final_fullpath = os.path.join(self._output_folder, filename)
            shutil.move(tmp_fullpath, final_fullpath)
            self._logger.debug(f'Wrote product to path {final_fullpath}')
        except Exception as e:
            raise OutputDeviceException('Error writing to device') from e
