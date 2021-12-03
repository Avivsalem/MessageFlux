from abc import ABCMeta, abstractmethod
from threading import Event
from typing import Optional

from baseservice.base_service import BaseService


class ServerLoopService(BaseService, metaclass=ABCMeta):
    """
    this is a base class for services that uses a 'loop' as their running method
    """

    def __init__(self, *, interval_between_loops: float = 0, **kwargs):
        """

        :param interval_between_loops: the interval (in seconds) to wait between each run of the loop.
        :param kwargs: the init args for base classes
        """
        super().__init__(**kwargs)
        self._interval_between_loops = interval_between_loops

    def _run_service(self, cancellation_token: Event):
        """
        runs the server loop continuously, until the cancellation token is set.

        :param cancellation_token: the cancellation token for this service
        """
        while not cancellation_token.is_set():
            self._server_loop(cancellation_token)
            cancellation_token.wait(self._interval_between_loops)

    @abstractmethod
    def _server_loop(self, cancellation_token: Event):
        """
        this method performs a single server loop. it should be implemented by the child classes

        :param cancellation_token: the cancellation token for this service
        """
        pass
