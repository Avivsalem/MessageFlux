import os
from typing import Optional

from messageflux.logging.bulk_rotating_handler_base import BulkRotatingHandlerBase


class BulkRotatingFileHandler(BulkRotatingHandlerBase):
    """
    Handler for logging to a set of files, which switches from one file
    to the next when the current file reaches a certain size or time.
    """

    def __init__(self,
                 live_log_path: str,
                 rotated_log_path: str,
                 bkp_log_path: Optional[str] = None,
                 max_records: int = 1000,
                 max_time: int = 60,
                 live_log_prefix: str = ''):
        """
        Open a log file and use it as the stream for logging.
        when max_records are written, or max_time has passed, a rollover occurs
        rollover copies the live log from live_log_path to rotated_log_path

        :param live_log_path: the path to write the live log to
        :param rotated_log_path: the path to write the rotated log to
        :param bkp_log_path: the path to write to rotated log to, if writing to rotated_log_path fails
        :param max_records: the maximum number of records to write before rotation
        :param max_time: the maximum time (in seconds) to wait before rotation
        :param live_log_prefix: the prefix for live log file
        """
        self._rotated_log_path = os.path.abspath(rotated_log_path)
        os.makedirs(self._rotated_log_path, exist_ok=True)

        super(BulkRotatingFileHandler, self).__init__(live_log_path=live_log_path,
                                                      bkp_log_path=bkp_log_path,
                                                      max_records=max_records,
                                                      max_time=max_time,
                                                      live_log_prefix=live_log_prefix)

    def _move_log_to_destination(self, src_file: str):
        """
        this moves the live log from a file, to its destination (the rotated log path)
        """

        dst_filename = os.path.join(self._rotated_log_path, self._get_unique_log_filename())
        self._safe_move(src_file, dst_filename)
