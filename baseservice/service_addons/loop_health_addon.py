import logging
import threading
from time import time

from baseservice.base_service import ServiceState
from baseservice.server_loop_service import ServerLoopService, LoopMetrics


class LoopHealthAddon:
    def __init__(self, service: ServerLoopService, *,
                 stop_after_consecutive_failures: int = -1,
                 stop_after_inactivity_timeout: float = -1):
        self._service = service
        if stop_after_inactivity_timeout > 0:
            self._service.state_event.register_handler(self._on_service_state_change)

        self._service.loop_ended_event.register_handler(self._on_loop_ended)

        self._stop_after_consecutive_failures = stop_after_consecutive_failures
        self._stop_after_inactivity_timeout = stop_after_inactivity_timeout
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
            time_from_last_loop = time() - self._last_loop_time
            if time_from_last_loop >= self._stop_after_inactivity_timeout:
                self._logger.warning(
                    f'Service {self._service.name} exceeded {self._stop_after_inactivity_timeout} '
                    f'seconds from last loop finished and will be stopped')
                self._service.stop()
                return

            time_to_sleep = self._stop_after_inactivity_timeout - time_from_last_loop
            self._cancellation_token.wait(time_to_sleep)

    def _on_service_state_change(self, service_state: ServiceState):
        if service_state == ServiceState.STARTING:
            self._on_service_starting()
        elif service_state == ServiceState.STOPPING:
            self._on_service_stopping()

    def _on_service_starting(self):
        if self._stop_after_inactivity_timeout >= 0:
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

        if self._consecutive_failures >= self._stop_after_consecutive_failures > 0:
            self._logger.warning(
                f'Service {self._service.name} reached {self._consecutive_failures} and will be stopped')
            self._service.stop()
