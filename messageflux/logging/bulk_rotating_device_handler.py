import os
from typing import Optional, Dict, Any

from messageflux.iodevices.base import OutputDeviceManager, Message
from messageflux.logging.bulk_rotating_handler_base import BulkRotatingHandlerBase


class BulkRotatingDeviceHandler(BulkRotatingHandlerBase):
    """
    Handler for logging into an output device when the current file reaches a certain size or time.
    """

    def __init__(self,
                 live_log_path: str,
                 output_device_manager: OutputDeviceManager,
                 output_device_name: str,
                 metadata: Optional[Dict[str, Any]] = None,
                 bkp_log_path: Optional[str] = None,
                 max_records: int = 1000,
                 max_time: int = 60,
                 live_log_prefix: str = ''):
        """
        Open a log file and use it as the stream for logging.
        when max_records are written, or max_time has passed, a rollover occurs
        rollover copies the live log from live_log_path to rotated_log_path

        :param live_log_path: the path to write the live log to
        :param output_device_manager: the device manager to send the logs to
        :param output_device_name: the name of the output_device to send to
        :param metadata: a dictionary of metadata to send on the device along with the log
        :param bkp_log_path: the path to write to rotated log to, if writing to rotated_log_path fails
        :param max_records: the maximum number of records to write before rotation
        :param max_time: the maximum time (in seconds) to wait before rotation
        :param live_log_prefix: the prefix for live log file
        """
        self._output_device_manager = output_device_manager
        self._output_device_name = output_device_name

        self._output_device_manager.connect()
        self._output_device = self._output_device_manager.get_output_device(name=self._output_device_name)
        self._metadata = metadata or {}

        super(BulkRotatingDeviceHandler, self).__init__(live_log_path=live_log_path,
                                                        bkp_log_path=bkp_log_path,
                                                        max_records=max_records,
                                                        max_time=max_time,
                                                        live_log_prefix=live_log_prefix)

    def _move_log_to_destination(self, src_file: str):
        """
        this moves the live log from a file, to its destination (the rotated log path)
        """
        with open(src_file, 'rb') as log_file:
            self._output_device.send_message(Message(log_file, self._metadata.copy()))

        os.remove(src_file)

    def close(self):
        """
        closes the handler (disconnects from the output device)
        """
        try:
            super(BulkRotatingDeviceHandler, self).close()
        finally:
            self._output_device_manager.disconnect()
