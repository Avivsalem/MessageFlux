import logging
import signal
import threading
from abc import ABCMeta, abstractmethod
from enum import Enum, unique
from typing import Optional

from messageflux.utils import ObservableEvent


@unique
class ServiceState(Enum):
    """
    the states of the service:
    INITIALIZING->(start)->STARTING->(prepare_service)->STARTED->(run_service)->STOPPING->(finalize_service)->STOPPED
    """
    INITIALIZED = 'INITIALIZED'
    """The service has been created, but has not been started yet"""

    STARTING = 'STARTING'
    """The service is starting, but is not running yet"""

    STARTED = 'STARTED'
    """The service is running."""

    STOPPING = 'STOPPING'
    """The service is in the process of stopping"""

    STOPPED = 'STOPPED'
    """The service is not running anymore"""


class BaseService(metaclass=ABCMeta):
    """
    this class is the base class for all services
    """

    def __init__(self, *,
                 name: Optional[str] = None,
                 should_stop_on_signal: bool = True):
        """

        :param name: the name of this service. if None, the name of the type will be used
        :param should_stop_on_signal: if True, the service will try to register SIGTERM and SIGINT on stop method.
        """
        self._should_stop_on_signal: bool = should_stop_on_signal
        self._cancellation_token: threading.Event = threading.Event()
        self._cancellation_token.set()  # service starts as not_running
        self._logger = logging.getLogger(__name__)
        self._state_changed_event: ObservableEvent[ServiceState] = ObservableEvent()
        self._service_state = ServiceState.INITIALIZED

        if not name:
            name = type(self).__name__

        self._name = name

    @property
    def name(self) -> str:
        """
        the name of the service
        """
        return self._name

    @property
    def is_alive(self) -> bool:
        """
        this property is True, if the service is running.
        """
        return self._is_alive()

    @property
    def service_state(self) -> ServiceState:
        """
        this is the current service state
        """
        return self._service_state

    @property
    def state_changed_event(self) -> ObservableEvent[ServiceState]:
        """
        this is an Event, that can be used to register on server state changes
        """
        return self._state_changed_event

    def _is_alive(self) -> bool:
        """
        this may be overridden by child classes to change the 'is_alive' behaviour
        """
        return not self._cancellation_token.is_set()

    def _set_service_state(self, new_service_state: ServiceState):
        old_service_state = self._service_state
        self._service_state = new_service_state

        if old_service_state != new_service_state:
            self._state_changed_event.fire(new_service_state)

    def start(self):
        """
        starts the service, and blocks until 'stop' is called
        """
        self._cancellation_token.clear()
        if self._should_stop_on_signal:
            self._register_signals()
        server_exception = None
        try:
            self._set_service_state(ServiceState.STARTING)
            self._logger.info(f"Starting {self._name}")
            self._prepare_service()
            self._set_service_state(ServiceState.STARTED)
            self._run_service(cancellation_token=self._cancellation_token)

            # this loop is because wait() prevents signal handling on some systems.
            # otherwise, we'd just use wait() without the loop (and no timeout)
            while not self._cancellation_token.is_set():
                self._cancellation_token.wait(0.5)
        except Exception as ex:
            self._logger.exception(f'Service raised an exception: {str(ex)}')
            self._cancellation_token.set()
            server_exception = ex

        self._set_service_state(ServiceState.STOPPING)
        self._finalize_service(exception=server_exception)
        self._set_service_state(ServiceState.STOPPED)

    def _register_signals(self):
        if threading.current_thread() is threading.main_thread():
            self._logger.info("Registering Terminate Signals...")
            for signame in [signal.SIGINT, signal.SIGTERM]:
                signal.signal(signame, lambda s, f: self.stop())
        else:
            self._logger.warning("Service doesn't run on main thread - can't register signals")

    def _prepare_service(self):
        """
        this method may be implemented by child classes, to perform some initialization logic
        before actually starting the service
        """
        pass

    @abstractmethod
    def _run_service(self, cancellation_token: threading.Event):
        """
        this method should be implemented by child classes, and actually run the service

        :param cancellation_token: the cancellation token that will be set when 'stop' is called
        """
        pass

    def _finalize_service(self, exception: Optional[Exception] = None):
        """
        this method may be implemented by child classes, to perform some cleanup logic
        after the service has finished running.

        :param exception: the exception (if any) that _run_service raised
        """
        pass

    def stop(self):
        """
        stops the service (sets the cancellation token, so the service will stop gracefully)
        """
        self._logger.info(f"Stopping {self._name}")
        if self._cancellation_token.is_set():
            return
        self._set_service_state(ServiceState.STOPPING)
        self._cancellation_token.set()
