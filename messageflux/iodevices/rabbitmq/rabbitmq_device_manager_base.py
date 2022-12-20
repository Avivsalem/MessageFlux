import logging
import socket
import ssl
from random import shuffle
from typing import TYPE_CHECKING, List, Optional, Dict, Any, Union, Type

import sys

from messageflux.utils import ThreadLocalMember, KwargsException

DEFAULT_CLIENT_ARGS = {'hostname': socket.gethostname()}
CONNECTION_TYPE_ARG_NAME = '__CONNECTION_TYPE__'

pika_logger = logging.getLogger('pika')

pika_logger.propagate = False
pika_logger.setLevel(logging.WARNING)

for handler in pika_logger.handlers:
    pika_logger.removeHandler(handler)
pika_logger.addHandler(logging.StreamHandler(sys.stdout))

if TYPE_CHECKING:
    from pika import BlockingConnection, PlainCredentials
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.frame import Method as PikaMethod


class RabbitMQDeviceManagerBase:
    """
    base class for rabbitmq device managers

    Notice that pika is imported inside the methods here,
    since it causes trouble when using this device in multiprocess.
    """
    _connection: Union[ThreadLocalMember[Optional['BlockingConnection']],
                       Optional['BlockingConnection']] = ThreadLocalMember(init_value=None)
    _maintenance_channel: Union[ThreadLocalMember[Optional['BlockingChannel']],
                                Optional['BlockingChannel']] = ThreadLocalMember(init_value=None)

    def __init__(self,
                 hosts: Union[List[str], str],
                 user: str,
                 password: str,
                 port: Optional[int] = None,
                 ssl_context: Optional[ssl.SSLContext] = None,
                 virtual_host: Optional[str] = None,
                 client_args: Optional[Dict[str, str]] = None,
                 connection_type: str = "None",
                 heartbeat: int = 300,
                 connection_attempts: int = 5,
                 blocked_connection_timeout: Optional[float] = None):
        """
        This manager used to create RabbitMQ devices (direct queues)

        :param hosts: the hostname or a list of hostnames of the manager
        :param user: the username for the rabbitMQ manager
        :param password: the password for the rabbitMQ manager
        :param port: the port to connect the hosts to
        :param ssl_context: the ssl context to use. None means don't use ssl at all
        :param virtual_host: the virtual host to connect to
        :param client_args: the arguments to create the client with
        :param heartbeat: heartbeat interval for the connection (between 0 and 65536
        :param connection_attempts: Maximum number of retry attempts
        :param blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection:
            passing `ConnectionBlockedTimeout` exception to on_close_callback
            in asynchronous adapters or raising it in `BlockingConnection`.
        """
        try:
            import pika
        except ImportError as ex:
            raise ImportError('Please Install the required extra: messageflux[rabbitmq]') from ex
        if isinstance(hosts, str):
            hosts = [hosts]
        self._hosts = hosts
        self._user = user
        self._password = password
        self._ssl_context = ssl_context
        if port is None:
            if self._ssl_context is not None:
                port = pika.ConnectionParameters.DEFAULT_SSL_PORT
            else:
                port = pika.ConnectionParameters.DEFAULT_PORT
        self._port = port
        if virtual_host is None:
            virtual_host = pika.ConnectionParameters.DEFAULT_VIRTUAL_HOST
        self._virtual_host = virtual_host
        self._logger = logging.getLogger(__name__)

        self._client_args = (client_args or DEFAULT_CLIENT_ARGS).copy()
        self._client_args[CONNECTION_TYPE_ARG_NAME] = connection_type
        self._client_args["product"] = f"{socket.gethostname()}(Pika)"

        self._heartbeat = max(0, min(heartbeat, 65535))
        self._connection_attempts = connection_attempts
        self._blocked_connection_timeout = blocked_connection_timeout

    def _create_connection(self,
                           credentials: Optional['PlainCredentials'] = None,
                           heartbeat: Optional[int] = None,
                           connection_attempts: Optional[int] = None,
                           client_properties: Optional[Dict[str, str]] = None,
                           virtual_host: Optional[str] = None,
                           blocked_connection_timeout: Optional[float] = None) -> 'BlockingConnection':
        """
        creates a BlockingConnection object

        :param credentials: possible alternate credentials
        :param heartbeat: heartbeat interval for the connection (between 0 and 65536
        :param connection_attempts: Maximum number of retry attempts
        :param client_properties: the client properties for the connection
        :param virtual_host: the virtual host to connect to
        :param blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection:
            passing `ConnectionBlockedTimeout` exception to on_close_callback
            in asynchronous adapters or raising it in `BlockingConnection`.

        :return: a newly created connection
        """
        try:
            import pika
            from pika.exceptions import AMQPConnectionError
        except ImportError as ex:
            raise ImportError('Please Install the required extra: messageflux[rabbitmq]') from ex

        class SelfClosingBlockingConnection(pika.BlockingConnection):

            @property
            def is_open(self):
                """
                Returns a boolean reporting the current connection state.
                """
                try:
                    self.process_data_events()
                except Exception:
                    pass

                return super().is_open

            def __del__(self):
                """
                Tries to close the connection when it goes out of scope (other thread for example)
                """
                try:
                    if self.is_open:
                        self.close()
                except Exception:
                    pass

        shuffle(self._hosts)
        exceptions = []
        for host in self._hosts:
            try:
                ssl_options: Union[pika.SSLOptions, Type[pika.ConnectionParameters._DEFAULT]]
                if self._ssl_context is not None:
                    ssl_options = pika.SSLOptions(self._ssl_context, host)
                else:
                    ssl_options = pika.ConnectionParameters._DEFAULT

                return SelfClosingBlockingConnection(pika.ConnectionParameters(
                    host=host,
                    port=self._port,
                    ssl_options=ssl_options,
                    credentials=credentials or pika.PlainCredentials(username=self._user, password=self._password),

                    heartbeat=heartbeat or self._heartbeat,
                    connection_attempts=connection_attempts or self._connection_attempts,
                    client_properties=client_properties or self._client_args,
                    virtual_host=virtual_host or self._virtual_host,
                    blocked_connection_timeout=(blocked_connection_timeout or self._blocked_connection_timeout)))

            except AMQPConnectionError as ex:
                message = f"RabbitMQ on host {host} is not available"
                io_device_exception = KwargsException(message)
                if len(self._hosts) == 1:
                    self._logger.exception(message)
                    raise io_device_exception from ex
                else:
                    io_device_exception.__cause__ = ex  # this adds ex as the cause, to use in the aggregate exception
                    exceptions.append(io_device_exception)
                    self._logger.warning(message, exc_info=True)

        raise KwargsException("Couldn't connect to any of the hosts for RabbitMQ.",
                              inner_exceptions=exceptions)

    @property
    def connection(self) -> 'BlockingConnection':
        """
        returns an open and ready connection
        """
        if self._connection is None or not self._connection.is_open:
            self._connection = self._create_connection()

        return self._connection

    def _connect(self):
        if self._connection is None or not self._connection.is_open:
            self._connection = self._create_connection()

    def _disconnect(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                self._logger.warning('Error Closing Device', exc_info=True)

    @property
    def maintenance_channel(self) -> 'BlockingChannel':
        """
        returns a channel used for maintenance (create/bind etc...)
        """
        if (self._maintenance_channel is None or
                not self._maintenance_channel.is_open or not self._maintenance_channel.connection.is_open):
            self._maintenance_channel = self.connection.channel()

        assert self._maintenance_channel is not None
        return self._maintenance_channel

    def create_queue(self,
                     queue_name: str,
                     passive: bool = False,
                     durable: bool = True,
                     exclusive: bool = False,
                     auto_delete: bool = False,
                     arguments: Optional[Dict[str, Any]] = None,
                     direct_bind_to_exchange: Optional[str] = None) -> 'PikaMethod':
        """
        Declare queue, create if needed. This method creates or checks a
        queue. When creating a new queue the client can specify various
        properties that control the durability of the queue and its contents,
        and the level of sharing for the queue.

        Leave the queue name empty for a auto-named queue in RabbitMQ

        :param queue_name: The queue name
        :param passive: Only check to see if the queue exists
        :param durable: Survive reboots of the broker
        :param exclusive: Only allow access by the current connection
        :param auto_delete: Delete after consumer cancels or disconnects
        :param arguments: Custom key/value arguments for the queue
        :param direct_bind_to_exchange: if not None - a 'direct' exchange name to bind to with the queue name
        :returns: Method frame from the Queue.Declare-ok response
        """
        res = self.maintenance_channel.queue_declare(queue=queue_name,
                                                     passive=passive,
                                                     durable=durable,
                                                     exclusive=exclusive,
                                                     auto_delete=auto_delete,
                                                     arguments=arguments)
        if direct_bind_to_exchange is not None:
            self.bind_queue(queue_name, direct_bind_to_exchange, queue_name)
        return res

    def delete_queue(self, queue_name: str, only_if_empty: bool = True) -> 'PikaMethod':
        """
        Deletes a queue from the mq broker.

        :param queue_name: The queue name
        :param only_if_empty: if True will delete the queue only if it is empty (no messages inside)
        :returns: Method frame from the Queue.Declare-ok response
        """
        return self.maintenance_channel.queue_delete(queue_name, if_empty=only_if_empty)

    def get_queue_message_count(self, queue_name: str) -> int:
        """
        returns the current number of messages in queue

        :param queue_name: the queue name to check
        :return: current number of messages in queue
        """
        return self.create_queue(queue_name, passive=True).method.message_count

    def bind_queue(self,
                   queue_name: str,
                   exchange: str,
                   routing_key: Optional[str] = None,
                   arguments: Optional[Dict[str, Any]] = None) -> 'PikaMethod':
        """
        Bind the queue to the specified exchange

        :param queue_name: The queue to bind to the exchange
        :param exchange: The source exchange to bind to
        :param routing_key: The routing key to bind on
        :param arguments: Custom key/value pair arguments for the binding

        :returns: Method frame from the Queue.Bind-ok response
        """

        return self.maintenance_channel.queue_bind(queue_name, exchange, routing_key, arguments)

    def unbind_queue(self,
                     queue_name: str,
                     exchange: str,
                     routing_key: Optional[str] = None,
                     arguments: Optional[Dict[str, Any]] = None) -> 'PikaMethod':
        """
        Unbind the queue from the specified exchange

        :param queue_name: The queue to unbind to the exchange
        :param exchange: The source exchange to unbind from
        :param routing_key: The routing key to unbind
        :param arguments: Custom key/value pair arguments for the unbinding

        :returns: Method frame from the Queue.Unbind-ok response
        """

        return self.maintenance_channel.queue_unbind(queue_name, exchange, routing_key, arguments)
