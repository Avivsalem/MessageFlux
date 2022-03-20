import logging
import multiprocessing
import os
import threading
from abc import ABCMeta, abstractmethod
from multiprocessing.connection import Connection
from typing import Optional, Callable

from baseservice.base_service import BaseService


class ServiceFactory(metaclass=ABCMeta):
    """
    This class is used to create a service instance.
    """

    @abstractmethod
    def create_service(self) -> BaseService:
        pass


_STOP_MESSAGE = 'STOP'
_TEST_ALIVE_MESSAGE = 'TEST_ALIVE'

INSTANCE_INDEX_ENV_VAR = 'MULTI_PROCESS_INSTANCE_INDEX'
_logger = logging.getLogger(__name__)


def _start_service_and_listen_queue(service_factory: ServiceFactory,
                                    child_pipe: Connection,
                                    instance_index: int):
    try:
        service = service_factory.create_service()
        os.environ[INSTANCE_INDEX_ENV_VAR] = str(instance_index)

        def _listen_to_pipe(inner_service: BaseService, pipe: Connection):
            try:
                while True:
                    message = pipe.recv()
                    if message == _TEST_ALIVE_MESSAGE:
                        pipe.send(inner_service.is_alive)
                    elif message == _STOP_MESSAGE:
                        _logger.info(f'Stopping Service #{instance_index}')
                        break
                    else:
                        _logger.error(f'Unknown message received: {message}')
                        pass
            except Exception:
                _logger.exception('Error in pipe listener')
                pass
            finally:
                inner_service.stop()
                pipe.close()

        t = threading.Thread(target=_listen_to_pipe, args=(service, child_pipe), daemon=True)
        t.start()
        _logger.info(f'Starting Service #{instance_index}')
        service.start()
    except Exception as ex:
        _logger.exception(f'Error starting service #{instance_index}')
        raise


class SingleProcessHandler:
    """
    This class is used to handle a single process.
    """

    def __init__(self,
                 service_factory: ServiceFactory,
                 instance_index: int,
                 live_check_interval: int = 60,
                 live_check_timeout: int = 10):
        self._service_factory = service_factory
        self._live_check_interval = live_check_interval
        self._live_check_timeout = live_check_timeout
        self._liveness_thread: Optional[threading.Thread] = None
        self._stop_was_called = threading.Event()
        self._parent_pipe: Optional[Connection] = None
        self._process: Optional[multiprocessing.Process] = None
        self._instance_index = instance_index

    @property
    def instance_index(self) -> int:
        return self._instance_index

    def _liveness_check(self):
        while not self._stop_was_called.wait(self._live_check_interval):
            alive = False
            if self._parent_pipe is not None and not self._parent_pipe.closed:
                self._parent_pipe.send(_TEST_ALIVE_MESSAGE)
                has_answer = self._parent_pipe.poll(self._live_check_timeout)
                if has_answer:
                    alive = self._parent_pipe.recv()

            if not alive:
                _logger.error(f'Service #{self._instance_index} is not alive. Stopping.')
                self.stop()
                self.kill()
                break

    def start(self, exit_callback: Callable[['SingleProcessHandler'], None]) -> threading.Thread:
        """
        Start the service.

        :param exit_callback:
        :return:
        """

        context = multiprocessing.get_context('spawn')
        self._parent_pipe, child_pipe = context.Pipe()
        self._stop_was_called.clear()

        self._process = context.Process(target=_start_service_and_listen_queue,
                                        args=(self._service_factory, child_pipe, self._instance_index))

        def _run_process():
            self._process.start()
            self._process.join()
            self._cleanup()

            exit_callback(self)

        run_process_thread = threading.Thread(target=_run_process, daemon=True)
        run_process_thread.start()

        self._liveness_thread = threading.Thread(target=self._liveness_check, daemon=True)
        self._liveness_thread.start()

        return run_process_thread

    def stop(self):
        self._stop_was_called.set()
        if self._parent_pipe is not None:
            self._parent_pipe.send(_STOP_MESSAGE)

    def is_alive(self) -> bool:
        return self._process is not None and self._process.is_alive()

    @property
    def pid(self) -> Optional[int]:
        if self._process is None:
            return None
        return self._process.pid

    @property
    def process(self) -> Optional[multiprocessing.Process]:
        return self._process

    def terminate(self):
        if self.is_alive():
            self._process.terminate()

    def kill(self):
        if self.is_alive():
            self._process.kill()

    def _cleanup(self):
        if self._parent_pipe is not None:
            self._parent_pipe.close()
            self._parent_pipe = None
