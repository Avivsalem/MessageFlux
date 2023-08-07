import logging
import threading
from typing import Optional

from flask import Flask
from messageflux.base_service import ServiceState
from messageflux.server_loop_service import ServerLoopService
from messageflux.utils import KwargsException
from waitress import serve


class IsAliveAddonException(KwargsException):
    """
    An exception that is raised on LoopHealthAddon errors
    """
    pass


class IsAliveAddon:
    """
    An addon that can be attached to a service, this will add is_alive http endpoint to the service.
    You can use this addon for health checks.
    """

    def __init__(self,
                 is_alive_endpoint: str = '/is_alive',
                 host: str = '0.0.0.0',
                 port: int = 8080):
        """
        :param is_alive_endpoint: The endpoint that will answer the request
        :param host: The host to server will listen on
        :param port: The port to server will listen on
        """

        self.is_alive_endpoint = is_alive_endpoint
        self._host = host
        self._port = port
        self._logger = logging.getLogger(__name__)
        self._service: Optional[ServerLoopService] = None

        self._app = Flask(__name__)
        self._app.add_url_rule(self.is_alive_endpoint, None, self._is_alive)

    @property
    def service(self) -> Optional[ServerLoopService]:
        """
        the attached service (or None if no service is attached)
        """
        return self._service

    def attach(self, service: ServerLoopService) -> 'IsAliveAddon':
        """
        attached the addon to a service, and returns the addon
        :param service: the service to attach to
        """
        if self._service is not None:
            raise IsAliveAddonException('Cannot attach an already attached addon')

        if service.service_state in [ServiceState.STARTED, ServiceState.STARTING]:
            raise IsAliveAddonException('Cannot monitor inactivity on an already started service')

        service.state_changed_event.subscribe(self._on_service_state_change)
        self._service = service

        return self

    def detach(self):
        """
        detaches from a service
        """
        service = self._service
        if service is None:
            return

        service.state_changed_event.unsubscribe(self._on_service_state_change)
        self._service = None

    def _on_service_state_change(self, service_state: ServiceState):
        if service_state == ServiceState.STARTED:
            self._on_service_started()

        elif service_state == ServiceState.STOPPING:
            self._on_service_stopping()

    def _is_alive(self):
        if self.service is None:
            return "Service is not attached", 503

        if self.service.service_state == ServiceState.STARTED:
            return "Running", 200

        return "Not running", 503

    def _start_server(self, host: str, port: int):
        serve(self._app, host=host, port=port)

    def _on_service_started(self):
        logging.info("Starting IsAliveAddon")
        t = threading.Thread(target=self._start_server,
                             kwargs={
                                 'host': self._host,
                                 'port': self._port,
                             },
                             daemon=True)
        t.start()

    def _on_service_stopping(self):
        logging.info("Stopping IsAliveAddon")
