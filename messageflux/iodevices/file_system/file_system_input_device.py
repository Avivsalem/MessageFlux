import json
import logging
import os
import random
import threading
import time
from collections import defaultdict
from random import randint
from typing import Optional, Dict, List, Set, Iterator

from messageflux.iodevices.base import InputTransaction, InputDeviceManager, InputDevice, ReadResult, \
    InputDeviceException
from messageflux.iodevices.file_system.file_system_device_manager_base import FileSystemDeviceManagerBase
from messageflux.iodevices.file_system.file_system_serializer import FileSystemSerializerBase, \
    DefaultFileSystemSerializer
from messageflux.metadata_headers import MetadataHeaders
from messageflux.utils import get_random_id
from messageflux.utils.filesystem import atomic_move, AtomicMoveException


class FileSystemInputTransaction(InputTransaction):
    """
    represents an InputTransaction for filesystem
    """

    MAX_POISON_COUNT = 3  # TODO: get this from the user.
    _POISON_COUNTS_PER_FILE: Dict[str, int] = defaultdict(lambda: 0)
    _logger = logging.getLogger(__name__)

    def __init__(self, device: 'FileSystemInputDevice', org_path: str, tmp_path: str):
        """
        :param device: the device that returned this transaction
        :param org_path: the original path of the file we read
        :param tmp_path: the temp path of the file we read
        """
        super(FileSystemInputTransaction, self).__init__(device=device)
        self._org_path = org_path
        self._tmp_path = tmp_path
        self._device_manager = device.manager
        self._device_manager.transaction_log.add_transaction(self)

    @property
    def org_path(self) -> str:
        """
        the original path of the file in this transaction
        """
        return self._org_path

    @property
    def tmp_path(self) -> str:
        """
        the temp (current) path of the file in this transaction
        """
        return self._tmp_path

    @staticmethod
    def _get_poison_filename(org_filename: str) -> str:
        """
        gets a filename for poison messages

        :param org_filename: the original filename
        :return: filepath for the poison message
        """
        basename = os.path.basename(org_filename)
        dirname = os.path.dirname(org_filename)
        poison_folder = os.path.join(dirname, 'POISON')  # TODO: get this from the user
        os.makedirs(poison_folder, exist_ok=True)
        poison_filepath = os.path.join(poison_folder, get_random_id() + basename)
        return poison_filepath

    @staticmethod
    def _calc_lockfile_name(tmp_folder: str, org_path: str) -> str:
        """
        calculates a name for the lockfile

        :param tmp_folder: the tmp folder
        :param org_path: the original file path
        :return: the path for the lockfile
        """
        return os.path.join(tmp_folder, '{}.lockfile'.format(os.path.basename(org_path)))

    @staticmethod
    def read_file(device: 'FileSystemInputDevice',
                  org_path: str,
                  tmp_folder: str,
                  with_transaction: bool,
                  serializer: FileSystemSerializerBase) -> Optional[ReadResult]:
        """
        reads the file from org_path, and adds the file to transaction if it exists, or delete if it doesn't
        :param device: the input device for this transaction
        :param org_path: the original path to this file
        :param tmp_folder: the temporary folder to use while holding files in transaction
        :param with_transaction: if set to 'False' no transaction is made, and the message is acked on read
        :param serializer: the serializer to use to deserialize the file
        """

        tmp_path = os.path.join(tmp_folder, get_random_id())
        try:
            if not atomic_move(org_path,
                               tmp_path,
                               FileSystemInputTransaction._calc_lockfile_name(tmp_folder,
                                                                              org_path)):
                return None
        except AtomicMoveException:
            FileSystemInputTransaction._logger.exception(f'Atomic move could not move the file {org_path}')
            return None

        with open(tmp_path, 'rb') as f:
            message = serializer.deserialize(f)

        if with_transaction:
            return ReadResult(message=message,
                              transaction=FileSystemInputTransaction(device=device,
                                                                     org_path=org_path,
                                                                     tmp_path=tmp_path))
        else:
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass

            return ReadResult(message)

    def _commit(self):
        """
        commits the transaction
        """
        try:
            os.remove(self._tmp_path)
        except FileNotFoundError:
            pass
        self._POISON_COUNTS_PER_FILE.pop(self._org_path, None)
        self._device_manager.transaction_log.remove_transaction(self)

    @staticmethod
    def rollback_path(tmp_path: str, org_path: str, max_poison_count: int):
        """
        rolls back the transaction

        :param tmp_path: the temp (current) path of the file
        :param org_path: the original path of the file
        :param max_poison_count: the maximum time we allow the file to be rolled back
        """
        FileSystemInputTransaction._POISON_COUNTS_PER_FILE[org_path] += 1
        if FileSystemInputTransaction._POISON_COUNTS_PER_FILE[org_path] >= max_poison_count:
            dest = FileSystemInputTransaction._get_poison_filename(org_path)
            FileSystemInputTransaction._POISON_COUNTS_PER_FILE.pop(org_path, None)
        else:
            dest = org_path

        atomic_move(tmp_path, dest, FileSystemInputTransaction._calc_lockfile_name(os.path.dirname(tmp_path), dest))

    def _rollback(self):
        """
        rolls back the transaction
        """
        try:
            self.rollback_path(tmp_path=self._tmp_path,
                               org_path=self._org_path,
                               max_poison_count=self.MAX_POISON_COUNT)
            self._device_manager.transaction_log.remove_transaction(self)
        except AtomicMoveException:
            self._logger.exception(f"Could not rollback file:{self._org_path}")


class TransactionLog:
    """
    this class is used to persist transaction log into filesystem, so it can be rolled back on process termination
    """
    _logger = logging.getLogger(__name__)

    def __init__(self, filepath: str):
        """
        :param filepath: the full path of the file used to persist this log
        """
        self._filepath = filepath
        self._transactions: Dict[str, str] = {}
        self._transactions.update(self._load_file(filepath))

    @staticmethod
    def _load_file(filepath: str) -> Dict[str, str]:
        """
        loads the transaction log from file

        :param filepath: the path of the file to read
        """
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
            return data
        else:
            return {}

    @property
    def filepath(self):
        """
        the path to the file holding this transaction log
        """
        return self._filepath

    def add_transaction(self, transaction: FileSystemInputTransaction):
        """
        adds a transaction to the log

        :param transaction: the transaction to add
        """
        self._transactions[transaction.tmp_path] = transaction.org_path
        try:
            self.write_log()
        except Exception:
            self._logger.warning("Couldn't save transaction log", exc_info=True)

    def remove_transaction(self, transaction: FileSystemInputTransaction):
        """
        removes a transaction from the log

        :param transaction: the transaction to remove
        """
        self._transactions.pop(transaction.tmp_path, None)
        try:
            self.write_log()
        except Exception:
            self._logger.warning("Couldn't save transaction log", exc_info=True)

    def rollback_all(self):
        """
        rolls back all the transaction in the log
        """
        for tmp_path, org_path in self._transactions.items():
            try:
                FileSystemInputTransaction.rollback_path(tmp_path=tmp_path,
                                                         org_path=org_path,
                                                         max_poison_count=FileSystemInputTransaction.MAX_POISON_COUNT)
            except AtomicMoveException:
                self._logger.exception(f"Could not rollback file:{org_path}")
        self._transactions.clear()
        try:
            self.write_log()
        except Exception:
            self._logger.warning("Couldn't save transaction log", exc_info=True)

    def write_log(self):
        """
        writes the transaction log to the file
        """
        if not self._transactions:
            try:
                os.remove(self._filepath)
            except OSError:
                pass
        else:
            with open(self._filepath, 'w') as f:
                json.dump(self._transactions, f)


class FileSystemInputDevice(InputDevice['FileSystemInputDeviceManager']):
    """
    An InputDevice, that reads from folder
    """

    _MAX_BATCH_SIZE = 300
    _MIN_BATCH_SIZE = 8
    _SLEEP_BETWEEN_BATCHES = 1
    STAT_HEADER_NAME = "__STAT__"
    FILENAME_HEADER_NAME = MetadataHeaders.FILENAME

    def __init__(self,
                 manager: 'FileSystemInputDeviceManager',
                 name: str,
                 tmp_folder: str,
                 queues_folder: str,
                 fifo: bool = True,
                 min_file_age: int = 0,
                 serializer: Optional[FileSystemSerializerBase] = None):
        """
        ctor

        :param manager: the device manager which created this input device
        :param name: the name of the queue to read from
        :param tmp_folder: the folder to create temporary files in
        :param queues_folder: the base folder for all queues
        :param fifo: should we read the files sorted by time (Notice! when fifo=false, you might get file starvation)
        :param min_file_age: the minimum time in seconds since last modification to file, before we to try to read it...
        :param serializer: the serializer to use for reading messages from files
        """
        super(FileSystemInputDevice, self).__init__(manager, name)
        self.min_file_age = min_file_age
        self._sorted = fifo
        self._tmp_folder = tmp_folder
        self._input_folder = os.path.join(queues_folder, name)
        try:
            os.makedirs(self._tmp_folder, exist_ok=True)
            os.makedirs(self._input_folder, exist_ok=True)
        except Exception as e:
            raise InputDeviceException('Error creating input device') from e

        self._current_batch_size = self._MIN_BATCH_SIZE
        self._current_generator: Optional[Iterator[Optional[os.DirEntry]]] = None
        self._black_listed_files: Set[str] = set()
        self._logger = logging.getLogger(__name__)
        self._serializer = serializer or DefaultFileSystemSerializer()

    def _get_sorted_filenames(self) -> List[os.DirEntry]:
        """
        returns a list of direntries for files under input_folder, sorted by mtime

        :return: sorted list of direntries
        """

        tmplist = []
        for direntry in os.scandir(self._input_folder):
            try:
                if direntry.path in self._black_listed_files:
                    continue
                mtime = direntry.stat(follow_symlinks=False).st_mtime
            except Exception:
                continue
            tmplist.append((direntry, mtime))
        sorted_list = sorted(tmplist, key=lambda t: t[1])
        result = []
        for tup in sorted_list:
            result.append(tup[0])
        return result

    def _get_generator_files(self) -> Iterator[Optional[os.DirEntry]]:
        """
        returns a generator of direntries for files under input_folder, unsorted
        None when have no more direnties in the scandir

        :return: generator of direntries
        """

        while True:
            for direntry in os.scandir(self._input_folder):
                yield direntry
            yield None

    def _get_unsorted_filenames(self) -> List[os.DirEntry]:
        """
        returns a batch of generator direntries for files under input_folder, unsorted
        None when have no more direnties in the scandir

        :return: generator of direntries
        """
        if not self._current_generator:
            self._current_generator = self._get_generator_files()
        batch = []
        assert self._current_generator is not None
        for direntry in self._current_generator:
            if direntry is None:
                break
            try:
                if direntry.path in self._black_listed_files:
                    continue

                if self.min_file_age > 0:
                    file_age = time.time() - direntry.stat().st_mtime
                    if file_age < self.min_file_age:
                        continue
            except Exception:
                continue
            batch.append(direntry)
            if len(batch) >= self._current_batch_size:
                break

        self._current_batch_size = max(self._MIN_BATCH_SIZE,
                                       min(len(batch), self._current_batch_size))
        random.shuffle(batch)

        return batch

    def _increase_batch_size(self):
        """
        increases the batch size for concurrent reading
        """
        self._current_batch_size = min(self._current_batch_size * 2, self._MAX_BATCH_SIZE)

    def _decrease_batch_size(self):
        """
        decreases the batch size for concurrent reading
        """
        self._current_batch_size = max(self._MIN_BATCH_SIZE, int(self._current_batch_size / 2))

    def _read_file(self, direntry: os.DirEntry, with_transaction: bool = True) -> Optional[ReadResult]:
        """
        tries to read a single filename from queue

        :param direntry: the input file to read

        :return: None, None if the file can't be read or (BytesIO, dict) if it succeeded
        """
        if direntry.path in self._black_listed_files:
            return None

        if not direntry.is_file():
            self._black_listed_files.add(direntry.path)
            return None

        self._logger.debug(f"found input file {direntry.name} in directory {self._input_folder}")

        file_path = direntry.path
        return FileSystemInputTransaction.read_file(self,
                                                    org_path=file_path,
                                                    tmp_folder=self._tmp_folder,
                                                    with_transaction=with_transaction,
                                                    serializer=self._serializer)

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional[ReadResult]:
        """
        this method returns a message from the file system

        :param cancellation_token: the cancellation token for this service. this can be used to know if cancellation
        was requested

        :param timeout: an optional timeout (in seconds) to wait for the device to return a message.
        after 'timeout' seconds, if the device doesn't have a message to return, it will return None

        :param with_transaction: 'True' if the device should read message within transaction,
        or 'False' if the message is automatically committed

        :return: a ReadResult object or None if no message was available.
        the device headers contains the filename, and stat struct for the file
        """
        try:
            deadline = 0.0
            if timeout is not None:
                deadline = time.perf_counter() + timeout
            if self._sorted:
                while True:
                    input_files = self._get_sorted_filenames()
                    for direntry in input_files:
                        try:
                            fs_metadata = {self.FILENAME_HEADER_NAME: direntry.name,
                                           self.STAT_HEADER_NAME: direntry.stat()}
                        except FileNotFoundError:  # file was deleted before we read it
                            continue
                        read_result = self._read_file(direntry, with_transaction=with_transaction)
                        if read_result is not None:
                            read_result.device_headers.update(fs_metadata)
                            return read_result
                        if timeout is not None and time.perf_counter() >= deadline:
                            break

                    if timeout is not None and time.perf_counter() >= deadline:
                        break
                    cancellation_token.wait(self._SLEEP_BETWEEN_BATCHES)
            else:
                while True:
                    got_file = False
                    for direntry in self._get_unsorted_filenames():
                        got_file = True
                        try:
                            fs_metadata = {self.FILENAME_HEADER_NAME: direntry.name,
                                           self.STAT_HEADER_NAME: direntry.stat()}
                        except FileNotFoundError:  # file was deleted before we read it
                            continue
                        read_result = self._read_file(direntry, with_transaction=with_transaction)
                        if read_result is not None:
                            self._decrease_batch_size()
                            read_result.device_headers.update(fs_metadata)
                            return read_result

                    # couldn't read any file from batch. try bigger batch next time
                    self._increase_batch_size()
                    if timeout is not None and time.perf_counter() >= deadline:
                        break
                    if not got_file:
                        cancellation_token.wait(self._SLEEP_BETWEEN_BATCHES)

            return None

        except Exception as e:
            raise InputDeviceException('Error reading product from device') from e


class FileSystemInputDeviceManager(FileSystemDeviceManagerBase, InputDeviceManager[FileSystemInputDevice]):
    """
    this is an input manager that creates file system input devices
    """

    def __init__(self,
                 root_folder: str,
                 queue_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_QUEUES_SUB_DIR,
                 tmp_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_TMPDIR_SUB_DIR,
                 bookkeeping_dir_name: str = FileSystemDeviceManagerBase.DEFAULT_BOOKKEEPING_SUB_DIR,
                 serializer: Optional[FileSystemSerializerBase] = None,
                 fifo: bool = True,
                 min_input_file_age: int = 0,
                 transaction_log_save_interval: int = 10):
        """
        :param root_folder: the root folder to use for the manager
        :param queue_dir_name: the name of the subdirectory under root_folder that holds the queues
        :param tmp_dir_name: the name of the subdirectory under root_folder to use for temp files
        :param bookkeeping_dir_name: the name of the subdirectory under root_folder that holds the book-keeping data
        :param serializer: the serializer to use to write messages to files. None will use the default serializer
        :param fifo: should we read the files sorted by time (Notice! when fifo=false, you might get file starvation)
        :param min_input_file_age: the minimum time in seconds since last modification, before we to try to read it...
        :param transaction_log_save_interval: the interval in seconds to save the transaction log
        """
        super().__init__(root_folder=root_folder,
                         queue_dir_name=queue_dir_name,
                         tmp_dir_name=tmp_dir_name,
                         bookkeeping_dir_name=bookkeeping_dir_name,
                         serializer=serializer)
        self._fifo = fifo
        self._min_input_file_age = min_input_file_age
        transaction_log_filename = os.path.join(self.bookkeeping_folder, f'{self._unique_manager_id}.transactionlog')
        self._transaction_log = TransactionLog(transaction_log_filename)
        self._transaction_log_save_interval = transaction_log_save_interval
        self._should_stop: threading.Event = threading.Event()
        self._transaction_log_thread: Optional[threading.Thread] = None
        self._logger = logging.getLogger(__name__)

    def _do_transaction_log_thread(self):
        """
        a thread that looks for old transaction logs, and rolls them back
        """
        while not self._should_stop.is_set():
            try:
                self._transaction_log.write_log()
                now = time.time()
                for direntry in os.scandir(self.bookkeeping_folder):
                    try:
                        if not direntry.is_file():
                            continue
                        transaction_log_file = direntry.path
                        if not transaction_log_file.endswith(".transactionlog"):
                            continue
                        file_age = now - direntry.stat().st_mtime
                        if file_age < self._transaction_log_save_interval * 3:
                            # don't roll back transaction logs that are too new (might be a file of another pod/process)
                            continue

                        temp_transaction_log_file_name = f'{transaction_log_file}.rolling_back.{randint(0, 10000)}'
                        os.rename(src=transaction_log_file, dst=temp_transaction_log_file_name)
                    except FileNotFoundError:
                        continue  # Couldn't handle the file, because someone else got to it first

                    try:
                        transaction_log = TransactionLog(temp_transaction_log_file_name)
                        transaction_log.rollback_all()
                    except Exception:
                        self._logger.exception(f"Error rolling back {transaction_log_file} file. removing")
                    finally:
                        if os.path.exists(temp_transaction_log_file_name):
                            os.remove(temp_transaction_log_file_name)
            except Exception:
                self._logger.exception('Error in transaction_log thread')
            # sleep for at least a few seconds, so we won't spam
            self._should_stop.wait(self._transaction_log_save_interval)

    @property
    def transaction_log(self) -> TransactionLog:
        """
        the transaction log
        """
        return self._transaction_log

    def get_input_device(self, name: str) -> FileSystemInputDevice:
        """
        Returns an input device by name

        :param name: the name of the device to read from
        :return: an input device for 'device_name'
        """
        try:
            return FileSystemInputDevice(manager=self,
                                         name=name,
                                         tmp_folder=self._tmp_folder,
                                         queues_folder=self._queues_folder,
                                         fifo=self._fifo,
                                         min_file_age=self._min_input_file_age,
                                         serializer=self._serializer)
        except Exception as e:
            raise InputDeviceException("Error getting input device") from e

    def connect(self):
        """
        connects to the device manager
        """
        try:
            self._create_all_directories()
            self._should_stop.clear()
            self._transaction_log_thread = threading.Thread(target=self._do_transaction_log_thread, daemon=True)
            self._transaction_log_thread.start()
        except Exception as ex:
            raise InputDeviceException('Error connection to Device Manager') from ex

    def disconnect(self):
        """
        disconnects from the device manager
        """
        self._should_stop.set()
        self._transaction_log.rollback_all()
