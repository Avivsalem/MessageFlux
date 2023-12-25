import time
from collections import deque
from contextlib import ContextDecorator
from typing import Optional


class RateLimiter(ContextDecorator):
    """
    a class used to limit the rate of actions
    """
    SECOND = 1
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    _DEFAULT = float()

    class RateLimitedError(Exception):
        """
        this is raised when we are rate limited (and raise_on_limited is True)
        """
        pass

    class RateActionTimeoutError(Exception):
        """
        this is raised when perform_action is timed out
        """
        pass

    def __init__(self,
                 number_of_actions: int,
                 amount_of_seconds: int = 1,
                 default_timeout: Optional[float] = None,
                 raise_on_timeout=False,
                 raise_on_limited=False):
        """
        ctor

        :param number_of_actions: the number of actions allowed within a window of time
        :param amount_of_seconds: the length of the time window in seconds
        :param default_timeout: the default timeout to use, if none was given to perform_action
        :param raise_on_timeout: should we raise an exception when timed_out
        :param raise_on_limited: should we raise an exception when rate limited (instead of block)
        """

        self._number_of_actions = number_of_actions
        self._amount_of_seconds = amount_of_seconds
        self._action_queue: deque[float] = deque()
        self._default_timeout = default_timeout
        self._raise_on_timeout = raise_on_timeout
        self._raise_on_limited = raise_on_limited

    def _get_time(self) -> float:
        """
        returns a monotonic time in seconds
        """
        return time.monotonic()

    def expected_block_time(self) -> float:
        """
        returns the time that 'perform_action' will block if called now
        """
        if self._amount_of_seconds <= 0:
            return 0
        time_to_sleep: float = 0
        now = self._get_time()
        self._trim_queue(now=now)
        if len(self._action_queue) >= self._number_of_actions:
            last_action_time = self._action_queue.popleft()
            time_to_sleep = self._amount_of_seconds - (now - last_action_time)

        return time_to_sleep

    def _trim_queue(self, now: float):
        while self._action_queue and now - self._action_queue[0] > self._amount_of_seconds:
            self._action_queue.popleft()

    def perform_action(self, timeout: Optional[float] = _DEFAULT) -> bool:
        """
        signals the rate limiter, that you want to perform an action.
        the method will block for the right amount of time if rate limiting is needed

        :param timeout: Optional timeout in seconds. (None means no timeout)
        :return: 'True' if the block was timed out, 'False' otherwise
        """
        if timeout is self._DEFAULT:
            timeout = self._default_timeout

        timed_out = False
        time_to_sleep = self.expected_block_time()
        if time_to_sleep > 0:
            if self._raise_on_limited:

                raise self.RateLimitedError(f'Action is limited for another {time_to_sleep} seconds')
            if timeout is not None and timeout < time_to_sleep:
                time_to_sleep = timeout
                timed_out = True
            time.sleep(time_to_sleep)

        if not timed_out:
            self._action_queue.append(self._get_time())

        elif self._raise_on_timeout:
            raise self.RateActionTimeoutError('Rate Limited action was timed out')

        return timed_out

    def __enter__(self):
        self.perform_action()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
