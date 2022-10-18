from multiprocessing import process

import logging
import multiprocessing
import os
import threading
from abc import ABCMeta, abstractmethod
from multiprocessing.process import BaseProcess
from typing import Optional, Callable, TYPE_CHECKING

from messageflux.base_service import BaseService

if TYPE_CHECKING:
    from multiprocessing.connection import _ConnectionBase


class ServiceFactory(metaclass=ABCMeta):
    """
    This class is used to create a service instance.
    """

    @abstractmethod
    def create_service(self) -> BaseService:
        """
        creates the service instance. this will run in the child service
        :return: an instance of BaseService
        """
        pass


_STOP_MESSAGE = 'STOP'
_TEST_ALIVE_MESSAGE = 'TEST_ALIVE'

INSTANCE_INDEX_ENV_VAR = 'MULTI_PROCESS_INSTANCE_INDEX'
_logger = logging.getLogger(__name__)


def _start_service_and_listen_queue(service_factory: ServiceFactory,
                                    child_pipe: '_ConnectionBase',
                                    instance_index: int):
    try:
        os.environ[INSTANCE_INDEX_ENV_VAR] = str(instance_index)
        service = service_factory.create_service()

        def _listen_to_pipe(inner_service: BaseService, pipe: '_ConnectionBase'):
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
    except Exception:
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
        self._parent_pipe: Optional['_ConnectionBase'] = None
        self._process: Optional[process.BaseProcess] = None
        self._instance_index = instance_index

    @property
    def instance_index(self) -> int:
        """
        the index of this service instance
        """
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

        :param exit_callback: a callback to call when the process has exited
        :return: a thread that runs that instance.
        """

        context = multiprocessing.get_context('spawn')
        child_pipe: '_ConnectionBase'
        self._parent_pipe, child_pipe = context.Pipe()
        self._stop_was_called.clear()

        self._process = context.Process(target=_start_service_and_listen_queue,
                                        args=(self._service_factory, child_pipe, self._instance_index))

        def _run_process():
            assert self._process is not None
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
        """
        requests the child process to stop
        """
        self._stop_was_called.set()
        if self._parent_pipe is not None:
            self._parent_pipe.send(_STOP_MESSAGE)

    def is_alive(self) -> bool:
        """
        returns whether the child process is alive
        :return: True if the child process is alive, False otherwise
        """
        return self._process is not None and self._process.is_alive()

    @property
    def pid(self) -> Optional[int]:
        """
        the pid of the child process (or None if no child process exists)
        """
        if self._process is None:
            return None
        return self._process.pid

    @property
    def process(self) -> Optional[BaseProcess]:
        """
        the child process object for this instance (if exists.)
        """
        return self._process

    def terminate(self):
        """
        tries to forcefully terminate the child process
        """
        if self.is_alive():
            assert self._process is not None
            self._process.terminate()

    def kill(self):
        """
        tries to forcefully KILL the child process
        """
        if self.is_alive():
            assert self._process is not None
            self._process.kill()

    def _cleanup(self):
        if self._parent_pipe is not None:
            self._parent_pipe.close()
            self._parent_pipe = None
