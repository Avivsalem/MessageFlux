from abc import ABCMeta, abstractmethod
from threading import Event


class BaseService(metaclass=ABCMeta):
    def __init__(self, should_stop_on_signal=True):
        self._should_stop_on_signal: bool = should_stop_on_signal
        self._cancellation_token: Event = Event()
        self._cancellation_token.set()  # service starts as not_running

    def start(self):
        if self._should_stop_on_signal:
            self._register_signals()
        self._cancellation_token.clear()
        try:
            self._run_service(cancellation_token=self._cancellation_token)
            self._cancellation_token.wait()
        finally:
            self._finalize_service()

    def _register_signals(self):
        pass  # TODO: implement

    @abstractmethod
    def _run_service(self, cancellation_token: Event):
        pass

    def _finalize_service(self):
        pass

    def stop(self):
        self._cancellation_token.set()
