import logging
import threading
from abc import ABCMeta, abstractmethod
import signal
from threading import Event
from typing import Optional


class BaseService(metaclass=ABCMeta):
    def __init__(self, should_stop_on_signal=True):
        self._should_stop_on_signal: bool = should_stop_on_signal
        self._cancellation_token: Event = Event()
        self._cancellation_token.set()  # service starts as not_running
        self._logger = logging.getLogger(__name__)

    def start(self):
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
        pass

    @abstractmethod
    def _run_service(self, cancellation_token: Event):
        pass

    def _finalize_service(self, exception: Optional[Exception] = None):
        pass

    def stop(self):
        self._cancellation_token.set()

