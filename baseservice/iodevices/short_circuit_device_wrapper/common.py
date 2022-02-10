import time

from baseservice.utils import KwargsException


class ShortCircuitException(KwargsException):
    """
    this exception is raised when the device is in short circuit state
    """
    pass


class ShortCircuitDeviceBase(object):
    def __init__(self, short_circuit_fail_count: int, short_circuit_time: int):
        """

        :param short_circuit_fail_count: the consecutive number of failures that will trigger a short circuit
        :param short_circuit_time: the time in seconds that the device will be in short circuit state
        """
        self._short_circuit_fail_count = short_circuit_fail_count
        self._short_circuit_time = short_circuit_time
        self._short_circuit_end_time = None
        self._current_fail_count = 0

    @property
    def is_in_short_circuit_state(self) -> bool:
        return self._short_circuit_end_time is not None and time.time() < self._short_circuit_end_time

    @property
    def current_fail_count(self) -> int:
        return self._current_fail_count

    def _validate_short_circuit(self):
        if self.is_in_short_circuit_state:
            raise ShortCircuitException("Device is in short circuit state")

        self._short_circuit_end_time = None

    def _report_failure(self):
        self._current_fail_count += 1
        if self._current_fail_count >= self._short_circuit_fail_count:
            self._short_circuit_end_time = time.time() + self._short_circuit_time
            self._current_fail_count = 0  # we are now in short circuit state, so reset the fail count

    def _report_success(self):
        self._current_fail_count = 0
