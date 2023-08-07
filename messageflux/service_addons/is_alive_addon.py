import logging
import threading
from typing import Optional

import uvicorn
from fastapi import FastAPI
from messageflux.base_service import ServiceState
from messageflux.server_loop_service import ServerLoopService
from messageflux.utils import KwargsException


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
                 port: int = 8080):
        """
        :param is_alive_endpoint: The endpoint that will answer the request
        :param port: The port to server will listen on
        """

        self._is_alive_endpoint = is_alive_endpoint
        self._port = port
        self._logger = logging.getLogger(__name__)
        self._service: Optional[ServerLoopService] = None

        self._app = FastAPI()

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

    def _start_server(self):
        @self._app.get(self._is_alive_endpoint)
        def is_alive():
            return "Running"

        server_configuration = uvicorn.Config(self._app, host="0.0.0.0", port=self._port, log_config=None)
        self._server = uvicorn.Server(server_configuration)
        self._server.run()

    def _on_service_started(self):
        logging.info("Starting IsAliveAddon")
        t = threading.Thread(target=self._start_server, daemon=True)
        t.start()

    def _on_service_stopping(self):
        logging.info("Stopping IsAliveAddon")
        self._server.should_exit = True
