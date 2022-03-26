import asyncio
import threading
from abc import ABCMeta, abstractmethod
from time import time

from baseservice.base_service import BaseService
from baseservice.server_loop_service import LoopMetrics
from baseservice.utils import ObservableEvent


class AsyncServerLoopService(BaseService, metaclass=ABCMeta):
    """
    this is a base class for services that uses a 'loop' as their running method
    """

    def __init__(self, *,
                 duration_after_loop_success: float = 0,
                 duration_after_loop_failure: float = 0,
                 concurrent_loops: int = 1,
                 **kwargs):
        """

        :param duration_after_loop_success: the duration (in seconds) to wait after successful run of the loop.
        :param duration_after_loop_failure: the duration (in seconds) to wait after failed run of the loop.
        :param kwargs: the init args for base classes
        """
        super().__init__(**kwargs)
        self._duration_after_loop_success = duration_after_loop_success
        self._duration_after_loop_failure = duration_after_loop_failure
        self._loop_ended_event: ObservableEvent[LoopMetrics] = ObservableEvent()
        self._concurrent_loops = concurrent_loops
        self._async_cancelation_token: asyncio.Event = asyncio.Event()
        self._async_cancelation_token.set()

    def start(self):
        self._async_cancelation_token.clear()
        super().start()

    def stop(self):
        super().stop()
        self._async_cancelation_token.set()

    @property
    def loop_ended_event(self) -> ObservableEvent[LoopMetrics]:
        return self._loop_ended_event

    def _run_service(self, cancellation_token: threading.Event):
        """
        runs the server loop continuously, until the cancellation token is set.

        :param cancellation_token: the cancellation token for this service
        """
        asyncio.run(self._async_run_service())

    async def _async_prepare_service(self):
        pass

    async def _async_finalize_service(self):
        pass

    async def _async_run_service(self):
        await self._async_prepare_service()
        try:
            loops = [self._single_server_loop_runner(self._async_cancelation_token) for _ in range(self._concurrent_loops)]
            await asyncio.gather(*loops)
        finally:
            await self._async_finalize_service()

    async def _single_server_loop_runner(self, cancellation_token: asyncio.Event):
        """
        runs a single server loop, until the cancellation token is set.

        :param cancellation_token: the cancellation token for this service
        """
        while not cancellation_token.is_set():
            loop_exception = None
            start_time = time()
            try:
                await self._async_server_loop(cancellation_token)
            except Exception as ex:
                self._logger.exception(f'Server loop raised an exception')
                loop_exception = ex

            loop_duration = time() - start_time
            self._loop_ended_event.fire(LoopMetrics(loop_duration=loop_duration, exception=loop_exception))

            wait_duration = self._duration_after_loop_success
            if loop_exception is not None:
                wait_duration = self._duration_after_loop_failure

            if wait_duration > 0:
                try:
                    await asyncio.wait_for(cancellation_token.wait(), wait_duration)
                except asyncio.TimeoutError:
                    pass

    @abstractmethod
    async def _async_server_loop(self, cancellation_token: asyncio.Event):
        """
        this method performs a single server loop. it should be implemented by the child classes

        :param cancellation_token: the cancellation token for this service
        """
        pass
