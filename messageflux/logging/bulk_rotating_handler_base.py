import os
import random
import socket
import threading
import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime
from logging.handlers import BaseRotatingHandler
from os import scandir
from shutil import move
from typing import Optional, Iterator

import itertools
from time import sleep, time

from messageflux.utils import get_random_id


class BulkRotatingHandlerBase(BaseRotatingHandler, metaclass=ABCMeta):
    """
    Handler for logging to a destination, while using a rotating "live" file, which rotates "into" its destination
    when the current file reaches a certain size or time.
    """
    _MIN_SLEEP_TIME = 5

    def __init__(self,
                 live_log_path: str,
                 bkp_log_path: Optional[str] = None,
                 max_records: int = 1000,
                 max_time: int = 60,
                 live_log_prefix: str = ''):
        """
        Open a log file and use it as the stream for logging.
        when max_records are written, or max_time has passed, a rollover occurs
        rollover copies the live log from live_log_path to the "destination"

        :param live_log_path: the path to write the live log to
        :param bkp_log_path: the path to write to rotated log to, if writing to rotated_log_path fails
        :param max_records: the maximum number of records to write before rotation
        :param max_time: the maximum time (in seconds) to wait before rotation
        :param live_log_prefix: the prefix for live log file
        """
        self._live_log_path = os.path.abspath(live_log_path)
        filename = os.path.join(self._live_log_path,
                                f'{live_log_prefix}{socket.gethostname()}-{str(os.getpid())}-{id(self)}.log')
        os.makedirs(self._live_log_path, exist_ok=True)

        BaseRotatingHandler.__init__(self, filename, 'a', None, True)

        if bkp_log_path is None:
            self._bkp_log_path = self._live_log_path
        else:
            self._bkp_log_path = os.path.abspath(bkp_log_path)
            os.makedirs(self._bkp_log_path, exist_ok=True)

        self._max_records = max_records
        self._max_time = max_time
        self._record_count = 0
        self._run = True
        self._next_rotate = time() + self._max_time
        self._log_thread: Optional[threading.Thread] = None
        self.doRollover()
        self._bkp_copy_thread = threading.Thread(target=self._do_bkp_copy_thread, daemon=True)
        self._bkp_copy_thread.start()

    @abstractmethod
    def _move_log_to_destination(self, src_file):
        """
        this moves the live log from a file, to its destination
        """
        raise NotImplementedError()

    @staticmethod
    def _safe_move(src, dst):
        """
        moves a file from src to dst as atomically possible

        :param src: src filepath
        :param dst: dsf filepath
        """
        try:
            os.chmod(src, 0o777)
            os.rename(src, dst)  # Try to rename - most atomic possible
        except Exception:
            basedir = os.path.dirname(dst)
            tmpdir = os.path.join(basedir, '_TMP_')
            os.makedirs(tmpdir, exist_ok=True)  # create a tmp directory under the destination directory
            tmpfile = os.path.join(tmpdir, "{}.tmp".format(get_random_id()))
            move(src, tmpfile)  # move the src to tmp dir in dest directory
            move(tmpfile, dst)  # move the tmp file to dest file (now they're in the same filesystem: should be atomic)

    @staticmethod
    def _get_unique_log_filename() -> str:
        """
        generates a random filename for log

        :return: a random filename for log
        """
        return "{host}_{time}_{rnd}_{pid}.log".format(
            host=socket.gethostname(),
            time=datetime.now().strftime('%y_%m_%d_%H_%M_%S_%f'),
            rnd=random.randint(0, 100000),
            pid=os.getpid()
        )

    @staticmethod
    def _sleep_until(timestamp):
        now = time()
        delay = timestamp - now
        if delay > 0:
            sleep(delay)

    def _do_logger_rotate_thread(self):
        while self._run:
            try:
                if self.shouldRollover(None):
                    # flush all bulks
                    self.doRollover()
            except Exception:
                print('Error in Logger thread. error:\r\n' + traceback.format_exc())

            self._sleep_until(self._next_rotate)

    def _do_bkp_copy_thread(self):
        while self._run:
            try:
                now = time()
                entries: Iterator[os.DirEntry] = scandir(self._bkp_log_path)
                if self._live_log_path != self._bkp_log_path:
                    entries = itertools.chain(entries, scandir(self._live_log_path))

                for direntry in entries:
                    if not direntry.is_file():
                        continue
                    src = direntry.path
                    if src == self.baseFilename:
                        # don't move the file we use to log right now
                        continue
                    file_age = now - direntry.stat().st_mtime
                    if file_age < self._max_time * 2:
                        # don't move files that are too new (might be a file of another pod/process)
                        continue
                    self._move_log_to_destination(src)
            except Exception:
                print('Error in bkp copy thread. error:\r\n' + traceback.format_exc())
            # sleep for at least a few seconds, so we won't spam
            sleep(max(self._max_time,
                      self._MIN_SLEEP_TIME))  # could be anything - but max_is a good estimate of how long is allowed

    def _ensure_logger_rotate_thread(self):
        """
        ensures that the logger thread starts once if needed
        :return:
        """
        if self._log_thread is None and self._run:
            self._log_thread = threading.Thread(target=self._do_logger_rotate_thread, daemon=True)
            self._log_thread.start()

    def emit(self, record):
        self._ensure_logger_rotate_thread()
        super(BulkRotatingHandlerBase, self).emit(record)
        self._record_count += 1

    def doRollover(self):
        """
        Do a rollover, as described in __init__().
        """
        self.acquire()
        try:
            if self.stream:
                self.stream.close()
                self.stream = None  # type: ignore

            if os.path.exists(self.baseFilename):
                try:
                    self._move_log_to_destination(self.baseFilename)
                except Exception:
                    dst_filename = os.path.join(self._bkp_log_path, self._get_unique_log_filename())
                    self._safe_move(self.baseFilename, dst_filename)
        finally:
            self._record_count = 0
            self._next_rotate = time() + self._max_time
            self.release()

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        Basically, see if the supplied record would cause the file to exceed
        the size limit we have.
        """

        if self._record_count >= self._max_records:
            return True

        if time() >= self._next_rotate:
            return True

        return False

    def close(self):
        """
        stops the logger thread and flushes
        """
        super(BulkRotatingHandlerBase, self).close()
        self._run = False
        self.doRollover()
