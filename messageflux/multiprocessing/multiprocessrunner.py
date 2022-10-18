import logging
import os
import threading
from multiprocessing.process import BaseProcess

import time
from typing import List, Optional

from messageflux.base_service import BaseService
from messageflux.multiprocessing.singleprocesshandler import ServiceFactory, SingleProcessHandler

INSTANCE_COUNT_ENV_VAR = 'MULTI_PROCESS_INSTANCE_COUNT'


class MultiProcessRunner(BaseService):
    """
    a class that runs multiple services in processes. it handles the child process, and check liveness
    """

    def __init__(self, *,
                 service_factory: ServiceFactory,
                 instance_count: int,
                 shutdown_timeout: int = 5,
                 live_check_interval: int = 60,
                 live_check_timeout: int = 10,
                 restart_on_failure: bool = True,
                 **kwargs):
        """
            :param service_factory: a factory class that creates the service to run in a different process
            :param instance_count: the number of processes to run
            :param shutdown_timeout: the time (seconds) to wait after calling 'stop' to violently stop the subprocesses
            :param live_check_interval: the interval in seconds to send the liveness message to queue
            :param live_check_timeout: the time to wait for the process answer to liveness test
            :param restart_on_failure: should we restart a process if it fails?
        """

        super().__init__(**kwargs)
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
    def processes(self) -> List[BaseProcess]:
        """
        the list of child processes that runs the service instances
        """
        return [handler.process for handler in self._process_handlers if handler.process is not None]

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
        still_running = [handler.process for handler in self._process_handlers if
                         handler.is_alive() and handler.process is not None]
        if still_running:
            self._logger.warning(f'{len(still_running)} processes still running after {self._shutdown_timeout} seconds')
            for handler_process in still_running:
                handler_process.terminate()
            time.sleep(self._shutdown_timeout)
            still_running = [handler.process for handler in self._process_handlers if
                             handler.is_alive() and handler.process is not None]
            if still_running:
                self._logger.warning(
                    f'{len(still_running)} processes still running after {self._shutdown_timeout} seconds')
                for handler_process in still_running:
                    handler_process.kill()
                time.sleep(self._shutdown_timeout)
                still_running = [handler.process for handler in self._process_handlers if
                                 handler.is_alive() and handler.process is not None]
                if still_running:
                    self._logger.error(
                        f'{len(still_running)} processes still running after {self._shutdown_timeout} seconds')
                    pass


def get_service_runner(*,
                       service_factory: ServiceFactory,
                       instance_count: int,
                       shutdown_timeout: int = 5,
                       live_check_interval: int = 60,
                       live_check_timeout: int = 10,
                       restart_on_failure: bool = True,
                       **kwargs) -> BaseService:
    """
    a helper method, that the creates the MultiProcessRunner if instance_count is greater then 1.
    otherwise, it just returns the service itself.

    :param service_factory: a factory class that creates the service to run in a different process
    :param instance_count: the number of processes to run
    :param shutdown_timeout: the time (seconds) to wait after calling 'stop' to violently stop the subprocesses
    :param live_check_interval: the interval in seconds to send the liveness message to queue
    :param live_check_timeout: the time to wait for the process answer to liveness test
    :param restart_on_failure: should we restart a process if it fails?
    """
    if instance_count <= 1:
        return service_factory.create_service()
    else:
        return MultiProcessRunner(service_factory=service_factory,
                                  instance_count=instance_count,
                                  shutdown_timeout=shutdown_timeout,
                                  live_check_interval=live_check_interval,
                                  live_check_timeout=live_check_timeout,
                                  restart_on_failure=restart_on_failure,
                                  **kwargs)
