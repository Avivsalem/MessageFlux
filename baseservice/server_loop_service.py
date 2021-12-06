import threading
from abc import ABCMeta, abstractmethod
from time import time
from typing import Optional

from baseservice.base_service import BaseService
from baseservice.utils import Event


class LoopMetrics:
    def __init__(self, loop_duration: float, exception: Optional[Exception] = None):
        """

        :param loop_duration: the duration (in seconds) that it took the loop to run
        :param exception: the exception (if any) that the loop raised
        """
        self._loop_duration = loop_duration
        self._exception = exception

    @property
    def loop_duration(self) -> float:
        """
        :return: the duration (in seconds) that it took the loop to run
        """
        return self._loop_duration

    @property
    def exception(self) -> Optional[Exception]:
        """
        :return: the exception (if any) that the loop raised
        """
        return self._exception


class ServerLoopService(BaseService, metaclass=ABCMeta):
    """
    this is a base class for services that uses a 'loop' as their running method
    """

    def __init__(self, *,
                 duration_after_loop_success: float = 0,
                 duration_after_loop_failure: float = 0, **kwargs):
        """

        :param duration_after_loop_success: the duration (in seconds) to wait after successful run of the loop.
        :param duration_after_loop_failure: the duration (in seconds) to wait after failed run of the loop.
        :param kwargs: the init args for base classes
        """
        super().__init__(**kwargs)
        self._duration_after_loop_success = duration_after_loop_success
        self._duration_after_loop_failure = duration_after_loop_failure
        self._loop_ended_event: Event[LoopMetrics] = Event()

    @property
    def loop_ended_event(self) -> Event[LoopMetrics]:
        return self._loop_ended_event

    def _run_service(self, cancellation_token: threading.Event):
        """
        runs the server loop continuously, until the cancellation token is set.

        :param cancellation_token: the cancellation token for this service
        """
        while not cancellation_token.is_set():
            loop_exception = None
            start_time = time()
            try:
                self._server_loop(cancellation_token)
            except Exception as ex:
                self._logger.exception(f'Server loop raised an exception')
                loop_exception = ex

            loop_duration = time() - start_time
            self._loop_ended_event.fire(LoopMetrics(loop_duration=loop_duration, exception=loop_exception))

            wait_duration = self._duration_after_loop_success
            if loop_exception is not None:
                wait_duration = self._duration_after_loop_failure

            cancellation_token.wait(wait_duration)

    @abstractmethod
    def _server_loop(self, cancellation_token: threading.Event):
        """
        this method performs a single server loop. it should be implemented by the child classes

        :param cancellation_token: the cancellation token for this service
        """
        pass