import logging
import threading
from time import time

from baseservice.base_service import ServiceState
from baseservice.server_loop_service import ServerLoopService, LoopMetrics


class LoopHealthAddon:
    def __init__(self, service: ServerLoopService, *,
                 max_consecutive_failures: int = -1,
                 max_inactivity_timeout: float = -1):
        self._service = service
        if max_inactivity_timeout > 0:
            self._service.state_changed_event.subscribe(self._on_service_state_change)

        self._service.loop_ended_event.subscribe(self._on_loop_ended)

        self._max_consecutive_failures = max_consecutive_failures
        self._max_inactivity_timeout = max_inactivity_timeout
        self._cancellation_token = threading.Event()
        self._logger = logging.getLogger(__name__)

        self._consecutive_failures = 0
        self._last_loop_time = None

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    @property
    def last_loop_time(self) -> float:
        return self._last_loop_time

    def _inactivity_watchdog_thread(self):
        while not self._cancellation_token.is_set():
            timeout_time = self._last_loop_time + self._max_inactivity_timeout
            if time() >= timeout_time:
                self._logger.warning(
                    f'Service {self._service.name} exceeded {self._max_inactivity_timeout} '
                    f'seconds from last loop finished and will be stopped')
                self._service.stop()
                return

            time_to_sleep = timeout_time - time()
            self._cancellation_token.wait(time_to_sleep)

    def _on_service_state_change(self, service_state: ServiceState):
        if service_state == ServiceState.STARTED:
            self._on_service_started()
        elif service_state == ServiceState.STOPPING:
            self._on_service_stopping()

    def _on_service_started(self):
        if self._max_inactivity_timeout >= 0:
            self._cancellation_token.clear()
            self._last_loop_time = time()
            t = threading.Thread(target=self._inactivity_watchdog_thread, daemon=True)
            t.start()

    def _on_service_stopping(self):
        self._cancellation_token.set()

    def _on_loop_ended(self, loop_metrics: LoopMetrics):
        self._last_loop_time = time()
        if loop_metrics.exception is None:
            self._consecutive_failures = 0
        else:
            self._consecutive_failures += 1

        if self._consecutive_failures >= self._max_consecutive_failures > 0:
            self._logger.warning(
                f'Service {self._service.name} reached {self._consecutive_failures} and will be stopped')
            self._service.stop()
