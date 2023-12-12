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

    def __init__(self, number_of_actions: int, amount_of_seconds: int = 1, max_block: Optional[float] = None):
        """
        ctor

        :param number_of_actions: the number of actions allowed within a window of time
        :param amount_of_seconds: the length of the time window in seconds
        :param max_block: the maximum amount of seconds to block if rate limit is needed. None means no maximum
        """

        self._number_of_actions = number_of_actions
        self._amount_of_seconds = amount_of_seconds
        self._max_block = max_block
        self._action_queue: deque[float] = deque()
        self._last_block_time: float = 0

    @property
    def last_block_time(self) -> float:
        """
        returns the last amount of time that the rate limiter was blocked
        """
        return self._last_block_time

    def _get_time(self) -> float:
        """
        returns a monotonic time in seconds
        """
        return time.monotonic()

    def _trim_queue(self, now: float):
        while self._action_queue and now - self._action_queue[0] > self._amount_of_seconds:
            self._action_queue.popleft()

    def perform_action(self):
        """
        signals the rate limiter, that you want to perform an action.
        the method will block for the right amount of time if rate limiting is needed
        """
        self._last_block_time = 0
        if self._amount_of_seconds <= 0:
            return
        time_to_sleep: float = 0
        now = self._get_time()
        self._trim_queue(now=now)
        if len(self._action_queue) >= self._number_of_actions:
            last_action_time = self._action_queue.popleft()
            time_to_sleep = self._amount_of_seconds - (now - last_action_time)
            if self._max_block is not None:
                time_to_sleep = min(self._max_block, time_to_sleep)

            if time_to_sleep > 0:
                time.sleep(time_to_sleep)
                now = self._get_time()

        self._action_queue.append(now)
        self._last_block_time = time_to_sleep

    def __enter__(self):
        self.perform_action()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
