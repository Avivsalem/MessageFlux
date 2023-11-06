import os
import queue
import traceback
from threading import Thread
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
                 live_log_prefix: str = '',
                 wait_on_queue_timeout: float = 1.0):
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
        :param wait_on_queue_timeout: the timeout in seconds to wait for the publish queue to have messages.
        must be greater then 0. lower number will make the thread busy wait.
        higher number will take more time to kill thread when close is called
        """
        self._output_device_manager = output_device_manager
        self._output_device_name = output_device_name

        self._output_device_manager.connect()
        self._output_device = self._output_device_manager.get_output_device(name=self._output_device_name)
        self._metadata = metadata or {}
        self._queue: Optional[queue.Queue] = None
        self._wait_on_queue_timeout = max(wait_on_queue_timeout, 0.1)
        self._send_to_device_thread: Optional[Thread] = None

        super(BulkRotatingDeviceHandler, self).__init__(live_log_path=live_log_path,
                                                        bkp_log_path=bkp_log_path,
                                                        max_records=max_records,
                                                        max_time=max_time,
                                                        live_log_prefix=live_log_prefix)

    def _do_send_to_device_thread(self):
        if self._queue is None:
            self._queue = queue.Queue()

        while self._run:
            try:
                file_to_send = self._queue.get(timeout=self._wait_on_queue_timeout)
                with open(file_to_send, 'rb') as log_file:
                    self._output_device.send_message(Message(log_file, self._metadata.copy()))

                os.remove(file_to_send)
            except queue.Empty:
                pass
            except Exception:
                print('Error in send to device thread. error:\r\n' + traceback.format_exc())

    def _move_log_to_destination(self, src_file: str):
        """
        this moves the live log from a file, to its destination (the rotated log path)
        """
        if self._send_to_device_thread is None and self._run:
            self._queue = queue.Queue()
            self._send_to_device_thread = Thread(target=self._do_send_to_device_thread)
            self._send_to_device_thread.start()

        if self._queue is not None:
            self._queue.put_nowait(src_file)

    def close(self):
        """
        closes the handler (disconnects from the output device)
        """
        try:
            super(BulkRotatingDeviceHandler, self).close()
        finally:
            self._output_device_manager.disconnect()
