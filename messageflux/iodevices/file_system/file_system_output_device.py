import logging
import os
import shutil
from typing import Optional

from messageflux.iodevices.base import OutputDeviceManager, OutputDevice, OutputDeviceException
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.iodevices.file_system.file_system_device_manager_base import FileSystemDeviceManagerBase
from messageflux.iodevices.file_system.file_system_serializer import FileSystemSerializerBase, \
    DefaultFileSystemSerializer
from messageflux.metadata_headers import MetadataHeaders
from messageflux.utils import get_random_id


class FileSystemOutputDevice(OutputDevice['FileSystemOutputDeviceManager']):
    """
    And OutputDevice that writes to filesystem
    """

    def __init__(self,
                 manager: 'FileSystemOutputDeviceManager',
                 name: str,
                 tmp_folder: str,
                 queues_folder: str,
                 format_filename: Optional[str] = None,
                 serializer: Optional[FileSystemSerializerBase] = None):
        """
        :param manager: the output device manager that created this device
        :param name: the name of this device
        :param tmp_folder: the folder to create temporary files in
        :param queues_folder: the base folder for all queues
        :param format_filename: the filename format to save the product, "{filename}-{item_id}"
        :param serializer: the serializer to use
        """
        super(FileSystemOutputDevice, self).__init__(manager, name)
        self._tmp_folder = tmp_folder
        self._output_folder = os.path.join(queues_folder, name)
        try:
            os.makedirs(self._tmp_folder, exist_ok=True)
            os.makedirs(self._output_folder, exist_ok=True)
        except Exception as e:
            raise OutputDeviceException('Error creating output device') from e

        self._format = format_filename
        self._logger = logging.getLogger(__name__)
        self._serializer = serializer or DefaultFileSystemSerializer()

    def _send_message(self, message_bundle: MessageBundle):
        """
        sends a message to the device

        :param message_bundle: the message bundle to send
        """
        item_id = message_bundle.device_headers.get(MetadataHeaders.ITEM_ID, '')
        self._logger.debug('Sending product to {} device'.format(self.name))
        self._save_to_folder(item_id, message_bundle.message)

    def _save_to_folder(self, item_id: str, message: Message):
        """
        saves the given message to the appropriate folder

        :param message: the message tp be saved
        """
        try:
            filename = ''
            try:
                if self._format:
                    filename = self._format.format(**message.headers)
            except KeyError:
                self._logger.error(
                    'Filename format isn\'t satisfied, format:{}, headers:{}, puts default filename instead'.format(
                        self._format, message.headers))
            if not filename:
                if item_id:
                    item_id += '-'
                filename = "{itemid}{uuid}.SBM".format(itemid=item_id if item_id else str(item_id),
                                                       uuid=get_random_id())
            tmp_fullpath = os.path.join(self._tmp_folder, filename)
            with open(tmp_fullpath, 'wb') as f:
                stream_to_write = self._serializer.serialize(message=message)
                f.write(stream_to_write.read())

            os.chmod(tmp_fullpath, 0o777)
            final_fullpath = os.path.join(self._output_folder, filename)
            shutil.move(tmp_fullpath, final_fullpath)
            self._logger.debug(f'Wrote product to path {final_fullpath}')
        except Exception as e:
            raise OutputDeviceException('Error writing to device') from e


class FileSystemOutputDeviceManager(FileSystemDeviceManagerBase, OutputDeviceManager[FileSystemOutputDevice]):
    """
    this is a device manager that generates output filesystem devices
    """

    def __init__(self,
                 root_folder: str,
                 queue_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_QUEUES_SUB_DIR,
                 tmp_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_TMPDIR_SUB_DIR,
                 bookkeeping_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_BOOKKEEPING_SUB_DIR,
                 serializer: Optional[FileSystemSerializerBase] = None,
                 output_filename_format: Optional[str] = None):
        """
        :param root_folder: the root folder to use for the manager
        :param queue_dir_name: the name of the subdirectory under root_folder that holds the queues
        :param tmp_dir_name: the name of the subdirectory under root_folder to use for temp files
        :param bookkeeping_dir_name: the name of the subdirectory under root_folder that holds the book-keeping data
        :param serializer: the serializer to use to write messages to files. None will use the default serializer
        :param output_filename_format: the filename format to save a product, "{filename}-{item_id}"
        """
        super(FileSystemOutputDeviceManager, self).__init__(root_folder=root_folder,
                                                            queue_dir_name=queue_dir_name,
                                                            tmp_dir_name=tmp_dir_name,
                                                            bookkeeping_dir_name=bookkeeping_dir_name,
                                                            serializer=serializer)
        self._output_filename_format = output_filename_format

    def connect(self):
        """
        connects to the device manager
        """
        try:
            self._create_all_directories()
        except Exception as ex:
            raise OutputDeviceException('Error connection to Device Manager') from ex

    def get_output_device(self, name: str) -> FileSystemOutputDevice:
        """
        Returns an outgoing device by name

        :param name: the name of the device to write to
        :return: an outgoing device for 'name'
        """
        try:
            return FileSystemOutputDevice(manager=self,
                                          name=name,
                                          tmp_folder=self._tmp_folder,
                                          queues_folder=self._queues_folder,
                                          format_filename=self._output_filename_format,
                                          serializer=self._serializer)
        except Exception as e:
            raise OutputDeviceException("Error getting output device") from e
