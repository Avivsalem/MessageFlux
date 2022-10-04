import contextlib

from time import perf_counter

from messageflux.utils import KwargsException


class ShortCircuitException(KwargsException):
    """
    this exception is raised when the device is in short circuit state
    """
    pass


class ShortCircuitDeviceBase:
    """
    a base logic for short circuit devices
    """

    def __init__(self, short_circuit_fail_count: int, short_circuit_time: float):
        """

        :param short_circuit_fail_count: the consecutive number of failures that will trigger a short circuit
        :param short_circuit_time: the time in seconds that the device will be in short circuit state
        """
        self._short_circuit_fail_count = short_circuit_fail_count
        self._short_circuit_time = short_circuit_time
        self._short_circuit_end_time: float = -1.0
        self._consecutive_failures_count = 0

    @property
    def is_in_short_circuit_state(self) -> bool:
        """
        returns whether this device is still short circuited.

        :return: True if the device is short-circuited, False otherwise
        """
        return self._short_circuit_end_time >= 0 and perf_counter() < self._short_circuit_end_time

    @property
    def consecutive_failures_count(self) -> int:
        """
        returns the current consecutive failures count
        """
        return self._consecutive_failures_count

    def _validate_short_circuit(self):
        if self.is_in_short_circuit_state:
            raise ShortCircuitException("Device is in short circuited state")

        self._short_circuit_end_time = -1.0

    def _report_failure(self):
        self._consecutive_failures_count += 1
        if self._consecutive_failures_count >= self._short_circuit_fail_count:
            self._short_circuit_end_time = perf_counter() + self._short_circuit_time
            self._consecutive_failures_count = 0  # we are now in short circuit state, so reset the fail count

    def _report_success(self):
        self._consecutive_failures_count = 0

    @contextlib.contextmanager
    def _failure_count_context(self):
        try:
            yield self
        except Exception:
            self._report_failure()
            raise
        else:
            self._report_success()
