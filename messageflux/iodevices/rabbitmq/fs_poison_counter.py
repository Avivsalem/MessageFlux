import json
import logging
import os
import threading
from hashlib import md5
from threading import Event
from typing import Optional, Dict, Any

from time import time

from messageflux.iodevices.rabbitmq.rabbitmq_poison_counting_input_device import PoisonCounterBase


class FileSystemPoisonCounter(PoisonCounterBase):
    """
    a poison counter that uses a shared folder to coordinate the consumers

    it is a reasonable assumption, that at any given moment, a message is only handled by one consumer,
    since the queue takes care of that.
    therefore, there should be no race on reading/writing the file (not including cases of hash collision...)
    """
    COUNTER_FILE_SUFFIX = '.PMC'
    MESSAGE_ID_PROP_NAME = 'message_id'
    COUNTER_PROP_NAME = 'counter'

    def __init__(self, base_folder: str, file_cleanup_timeout: int = 600):
        """
        :param base_folder: the shared folder path, to use for counter files.
        it is recommended that it will be unique per consumer group
        :param file_cleanup_timeout: the time (in seconds), after which, the counter file is considered old,
        and should be deleted
        """
        self._base_folder = base_folder
        self._file_cleanup_timeout = file_cleanup_timeout

        os.makedirs(self._base_folder, exist_ok=True)

        self._should_stop = Event()
        self._cleanup_thread: Optional[threading.Thread] = None

    def start(self):
        """
        starts the cleanup thread
        """
        self._should_stop.clear()
        if self._cleanup_thread is None:
            self._cleanup_thread = threading.Thread(target=self._do_cleanup_thread, daemon=True)
            self._cleanup_thread.start()

    def stop(self):
        """
        stops the cleanup thread
        """
        self._should_stop.set()
        if self._cleanup_thread is not None:
            self._cleanup_thread.join()
            self._cleanup_thread = None

    def _do_cleanup_thread(self):
        logger = logging.getLogger(__name__)
        while not self._should_stop.is_set():
            try:
                now = time()
                for direntry in os.scandir(self._base_folder):
                    try:
                        if not direntry.is_file():
                            continue
                        counter_file = direntry.path
                        if not counter_file.endswith(self.COUNTER_FILE_SUFFIX):
                            continue
                        file_age = now - direntry.stat().st_mtime
                        if file_age < self._file_cleanup_timeout:
                            # don't remove counter files that are too new (might be a file of another pod/process)
                            continue

                        os.remove(counter_file)
                    except FileNotFoundError:
                        continue  # Couldn't handle the file, probably because someone else got to it first
            except Exception:
                logger.warning('Error in cleanup thread', exc_info=True)
            # sleep for at least a few seconds so we won't spam
            self._should_stop.wait(self._file_cleanup_timeout)

    def _get_counter_filepath(self, message_id: str):
        """
        calculates the full path of the counter file
        """
        filename = md5(message_id.encode('utf8')).hexdigest()
        return os.path.join(self._base_folder, f'{filename}{self.COUNTER_FILE_SUFFIX}')

    def increment_and_return_counter(self, message_id: str) -> int:
        """
        reads the counter file, increments the counter, and write it back to counter file
        """
        counter_filepath = self._get_counter_filepath(message_id)
        data: Dict[str, Any] = {self.MESSAGE_ID_PROP_NAME: message_id,
                                self.COUNTER_PROP_NAME: 0}

        try:
            if os.path.exists(counter_filepath):
                with open(counter_filepath, 'r') as f:
                    data = json.load(f)
        except FileNotFoundError:  # someone deleted the file as we read it... NOT SUPPOSED TO HAPPEN
            pass

        data[self.MESSAGE_ID_PROP_NAME] = message_id  # THIS IS ONLY FOR SAFETY... PROBABLY REDUNDANT
        data[self.COUNTER_PROP_NAME] += 1

        with open(counter_filepath, 'w') as f:
            json.dump(data, f)
        os.chmod(counter_filepath, 0o777)

        return data[self.COUNTER_PROP_NAME]

    def delete_counter(self, message_id: str):
        """
        deletes the counter file
        """
        try:
            counter_filepath = self._get_counter_filepath(message_id)
            if os.path.exists(counter_filepath):
                os.remove(counter_filepath)
        except FileNotFoundError:  # file already deleted... so we don't care
            pass
