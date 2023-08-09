import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional

from messageflux.base_service import BaseService, ServiceState


class _MessageFluxHTTPServer(ThreadingHTTPServer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.messageflux_base_service: Optional[BaseService] = None


class WebServerLivenessAddonException(Exception):
    """
    the exception type for this addon
    """
    pass


class WebServerLivenessAddon:
    """
    an addon that adds a small webserver that listens on all routes and returns 200 if the service is alive
    or 500 otherwise.
    useful for liveness checks in deployments
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8080):
        """

        :param host: the host to listen on
        :param port: the port to listen on
        """
        self._host = host
        self._port = port
        self._service: Optional[BaseService] = None
        self._web_server: Optional[_MessageFluxHTTPServer] = None

    @property
    def service(self) -> Optional[BaseService]:
        """
        the attached service (or None if no service is attached)
        """
        return self._service

    def attach(self, service: BaseService) -> 'WebServerLivenessAddon':
        """
        attached the addon to a service, and returns the addon

        :param service: the service to attach to
        """
        if self._service is not None:
            raise WebServerLivenessAddonException('Cannot attach an already attached addon')

        if service.service_state in [ServiceState.STARTED, ServiceState.STARTING]:
            raise WebServerLivenessAddonException('Cannot monitor inactivity on an already started service')

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
        if self._web_server is not None:
            self._web_server.messageflux_base_service = None

    def _on_service_state_change(self, service_state: ServiceState):
        if service_state == ServiceState.STARTED:
            t = threading.Thread(target=self._run_web_server,
                                 daemon=True)
            t.start()

        elif service_state == ServiceState.STOPPING:
            if self._web_server is not None:
                web_server = self._web_server
                self._web_server = None
                web_server.shutdown()

    def _run_web_server(self):
        class LivenessHandler(BaseHTTPRequestHandler):
            """
            the actual http request handler
            """

            def do_GET(self):
                assert isinstance(self.server, _MessageFluxHTTPServer)
                base_service = self.server.messageflux_base_service
                if base_service is not None and base_service.is_alive:
                    self.send_response(200)
                else:
                    self.send_response(500)
                self.end_headers()

        self._web_server = _MessageFluxHTTPServer((self._host, self._port), LivenessHandler)
        self._web_server.messageflux_base_service = self._service
        self._web_server.serve_forever()
