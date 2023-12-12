import time
from collections import deque
from contextlib import contextmanager
from functools import wraps
from typing import Optional


def limit_rate(number_of_actions: int, amount_of_seconds: int = 1, max_block: Optional[float] = None):
    """
    a decorator to limit rate of a method

    :param number_of_actions: the number of actions allowed within a window of time
    :param amount_of_seconds: the length of the time window in seconds
    :param max_block: the maximum time to block if rate limit has reached
    """

    def _decorator_func(func):
        """
        the actual decorator
        """
        rate_limiter = RateLimiter(number_of_actions=number_of_actions,
                                   amount_of_seconds=amount_of_seconds)

        @wraps(func)
        def _wrapper(*args, **kwargs):
            rate_limiter.perform_action(max_block=max_block)
            return func(*args, **kwargs)

        return _wrapper

    return _decorator_func


class RateLimiter:
    """
    a class used to limit the rate of actions
    """
    SECOND = 1
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    def __init__(self, number_of_actions: int, amount_of_seconds: int = 1):
        """
        ctor

        :param number_of_actions: the number of actions allowed within a window of time
        :param amount_of_seconds: the length of the time window in seconds
        """

        self._number_of_actions = number_of_actions
        self._amount_of_seconds = amount_of_seconds
        self._action_queue: deque[float] = deque()

    def _get_time(self) -> float:
        """
        returns a monotonic time in seconds
        """
        return time.monotonic()

    def _trim_queue(self, now: float):
        while self._action_queue and now - self._action_queue[0] > self._amount_of_seconds:
            self._action_queue.popleft()

    def perform_action(self, max_block: Optional[float] = None) -> float:
        """
        signals the rate limiter, that you want to perform an action.
        the method will block for the right amount of time if rate limiting is needed

        :param max_block: the maximum amount of seconds to block if rate limit is needed. None means no maximum
        :return: the actual time blocked in seconds (between 0 and max_block)
        """
        if self._amount_of_seconds <= 0:
            return 0
        time_to_sleep: float = 0
        now = self._get_time()
        self._trim_queue(now=now)
        if len(self._action_queue) >= self._number_of_actions:
            last_action_time = self._action_queue.popleft()
            time_to_sleep = self._amount_of_seconds - (now - last_action_time)
            if max_block is not None:
                time_to_sleep = min(max_block, time_to_sleep)

            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
                now = self._get_time()

        self._action_queue.append(now)
        return time_to_sleep

    @contextmanager
    def limit(self, max_block: Optional[float] = None):
        """
        a context manager that rate limits its context

        :param max_block: the maximum amount of seconds to block if rate limit is needed. None means no maximum
        :return: the actual time blocked in seconds (between 0 and max_block)
        """
        actual_blocked = self.perform_action(max_block=max_block)
        try:
            yield actual_blocked
        finally:
            pass
