import logging
import threading
from time import time
from typing import Optional

from messageflux.base_service import ServiceState
from messageflux.server_loop_service import ServerLoopService, LoopMetrics
from messageflux.utils import KwargsException


class LoopHealthAddonException(KwargsException):
    """
    an exception that is raised on LoopHealthAddon errors
    """
    pass


class LoopHealthAddon:
    """
    an addon that can be attached to a service, to make sure he is healthy.
    if the service gets stuck (service loop inactivity) or has to many consecutive failures,
    the addons stops the service
    """

    def __init__(self, *,
                 max_consecutive_failures: Optional[int] = None,
                 max_inactivity_timeout: Optional[float] = None):
        """

        :param max_consecutive_failures: the number of consecutive service loop failures,
        after which the service is stopped. None means to allow any number of failures
        :param max_inactivity_timeout: the number of seconds of inactivity (no loop ended events...) after which
        the service is stopped. None means to ignore inactivity.
        """

        self._max_consecutive_failures = max_consecutive_failures
        self._max_inactivity_timeout = max_inactivity_timeout
        self._cancellation_token = threading.Event()
        self._logger = logging.getLogger(__name__)
        self._service: Optional[ServerLoopService] = None

        self._consecutive_failures = 0
        self._last_loop_time = 0.0

    @property
    def service(self) -> Optional[ServerLoopService]:
        """
        the attached service (or None if no service is attached)
        """
        return self._service

    def attach(self, service: ServerLoopService) -> 'LoopHealthAddon':
        """
        attached the addon to a service, and returns the addon
        :param service: the service to attach to
        """
        if self._service is not None:
            raise LoopHealthAddonException('Cannot attach an already attached addon')

        self._consecutive_failures = 0
        self._last_loop_time = 0.0

        if self._max_inactivity_timeout is not None:
            if service.service_state in [ServiceState.STARTED, ServiceState.STARTING]:
                raise LoopHealthAddonException('Cannot monitor inactivity on an already started service')

            service.state_changed_event.subscribe(self._on_service_state_change)

        service.loop_ended_event.subscribe(self._on_loop_ended)
        self._service = service

        return self

    def detach(self):
        """
        detaches from a service
        """
        self._cancellation_token.set()
        service = self._service
        if service is None:
            return
        service.state_changed_event.unsubscribe(self._on_service_state_change)
        service.loop_ended_event.unsubscribe(self._on_loop_ended)
        self._service = None

    @property
    def consecutive_failures(self) -> int:
        """
        the number of consecutive failures the service has had
        """
        return self._consecutive_failures

    @property
    def last_loop_time(self) -> float:
        """
        the last time that the service loop has ended
        """
        return self._last_loop_time

    def _inactivity_watchdog_thread(self, service: ServerLoopService, max_inactivity_timeout: float):
        while not self._cancellation_token.is_set():
            timeout_time = self._last_loop_time + max_inactivity_timeout
            if time() >= timeout_time:
                self._logger.warning(
                    f'Service {service.name} exceeded {max_inactivity_timeout} '
                    f'seconds from last loop finished and will be stopped')
                service.stop()
                return

            time_to_sleep = timeout_time - time()
            self._cancellation_token.wait(time_to_sleep)

    def _on_service_state_change(self, service_state: ServiceState):
        if service_state == ServiceState.STARTED:
            self._on_service_started()
        elif service_state == ServiceState.STOPPING:
            self._on_service_stopping()

    def _on_service_started(self):
        if self._max_inactivity_timeout is not None:
            self._cancellation_token.clear()
            self._last_loop_time = time()
            t = threading.Thread(target=self._inactivity_watchdog_thread,
                                 args=[self._service, self._max_inactivity_timeout],
                                 daemon=True)
            t.start()

    def _on_service_stopping(self):
        self._cancellation_token.set()

    def _on_loop_ended(self, loop_metrics: LoopMetrics):
        self._last_loop_time = time()
        if loop_metrics.exception is None:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1

        if self._max_consecutive_failures is not None and self._consecutive_failures >= self._max_consecutive_failures:
            if self._service is not None:
                self._logger.warning(
                    f'Service {self._service.name} reached {self._consecutive_failures} and will be stopped')
                self._service.stop()
