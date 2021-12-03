import logging
import threading
from abc import ABCMeta, abstractmethod
import signal
from threading import Event
from typing import Optional


class BaseService(metaclass=ABCMeta):
    """
    this class is the base class for all services
    """

    def __init__(self, *, should_stop_on_signal=True):
        """

        :param should_stop_on_signal: if True, the service will try to register SIGTERM and SIGINT on stop method.
        """
        self._should_stop_on_signal: bool = should_stop_on_signal
        self._cancellation_token: Event = Event()
        self._cancellation_token.set()  # service starts as not_running
        self._logger = logging.getLogger(__name__)

    @property
    def is_alive(self) -> bool:
        """
        this property is True, if the service is running.
        """
        return self._is_alive()

    def _is_alive(self) -> bool:
        """
        this may be overridden by child classes to change the 'is_alive' behaviour
        """
        return not self._cancellation_token.is_set()

    def start(self):
        """
        starts the service, and blocks until 'stop' is called
        """
        self._cancellation_token.clear()
        if self._should_stop_on_signal:
            self._register_signals()
        try:
            self._logger.info(f"Starting {self.__name__}")
            self._prepare_service()
            self._run_service(cancellation_token=self._cancellation_token)
            self._cancellation_token.wait()
        except Exception as ex:
            self._cancellation_token.set()
            self._finalize_service(exception=ex)
        else:
            self._finalize_service()

    def _register_signals(self):
        if threading.current_thread() is threading.main_thread():
            self._logger.info("Registering Terminate Signals...")
            signal.signal(signal.SIGINT, lambda s, f: self.stop())
            signal.signal(signal.SIGTERM, lambda s, f: self.stop())
        else:
            self._logger.warning("Service doesn't run on main thread - can't register signals")

    def _prepare_service(self):
        """
        this method may be implemented by child classes, to perform some initialization logic
        before actually starting the service
        """
        pass

    @abstractmethod
    def _run_service(self, cancellation_token: Event):
        """
        this method should be implemented by child classes, and actually run the service

        :param cancellation_token: the cancellation token that will be set when 'stop' is called
        """
        pass

    def _finalize_service(self, exception: Optional[Exception] = None):
        """
        this method may be implemented by child classes, to perfom some cleanup logic
        after the service has finished running.

        :param exception: the exception (if any) that _run_service raised
        """
        pass

    def stop(self):
        """
        stops the service (sets the cancellation token, so the service will stop gracefully)
        """
        self._cancellation_token.set()
