import logging
import os
import threading
import time
from multiprocessing import Process
from typing import List, Optional

from baseservice.base_service import BaseService
from baseservice.multiprocessing.singleprocesshandler import ServiceFactory, SingleProcessHandler

INSTANCE_COUNT_ENV_VAR = 'MULTI_PROCESS_INSTANCE_COUNT'


class MultiProcessRunner(BaseService):
    def __init__(self,
                 service_factory: ServiceFactory,
                 instance_count: int,
                 shutdown_timeout: int = 5,
                 live_check_interval: int = 60,
                 live_check_timeout: int = 10,
                 restart_on_failure: bool = True):

        super().__init__()
        self._service_factory = service_factory
        self._instance_count = instance_count
        self._shutdown_timeout = shutdown_timeout
        self._live_check_interval = live_check_interval
        self._live_check_timeout = live_check_timeout
        self._restart_on_failure = restart_on_failure
        self._logger = logging.getLogger(__name__)

        self._process_handlers: List[SingleProcessHandler] = []

    def _is_alive(self) -> bool:
        for handler in self._process_handlers:
            if handler.is_alive():
                return True

        return False

    def _run_service(self, cancellation_token: threading.Event):
        os.environ[INSTANCE_COUNT_ENV_VAR] = str(self._instance_count)
        for i in range(self._instance_count):
            self._logger.info(f'Starting service instance {i}')
            self._run_service_instance(i)

    @property
    def processes(self) -> List[Process]:
        return [handler.process for handler in self._process_handlers]

    def _run_service_instance(self, instance_index: int):
        handler = SingleProcessHandler(self._service_factory,
                                       instance_index,
                                       self._live_check_interval,
                                       self._live_check_timeout)
        handler.start(self._on_handler_exit)
        self._process_handlers.append(handler)

    def _on_handler_exit(self, handler: SingleProcessHandler):
        if not self._restart_on_failure:
            return

        if self._cancellation_token.is_set():
            return  # no need to restart if we are shutting down

        if handler in self._process_handlers:
            self._process_handlers.remove(handler)

        self._run_service_instance(handler.instance_index)

    def _finalize_service(self, exception: Optional[Exception] = None):
        super()._finalize_service(exception)
        for handler in self._process_handlers:
            handler.stop()
        time.sleep(self._shutdown_timeout)
        still_running = [handler.process for handler in self._process_handlers if handler.is_alive()]
        if still_running:
            self._logger.warning(f'{len(still_running)} processes still running after {self._shutdown_timeout} seconds')
            for handler in still_running:
                handler.terminate()
            time.sleep(self._shutdown_timeout)
            still_running = [handler.process for handler in self._process_handlers if handler.is_alive()]
            if still_running:
                self._logger.warning(
                    f'{len(still_running)} processes still running after {self._shutdown_timeout} seconds')
                for handler in still_running:
                    handler.kill()
                time.sleep(self._shutdown_timeout)
                still_running = [handler.process for handler in self._process_handlers if handler.is_alive()]
                if still_running:
                    self._logger.error(
                        f'{len(still_running)} processes still running after {self._shutdown_timeout} seconds')
                    pass
