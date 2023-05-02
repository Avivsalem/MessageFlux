import logging
import ssl
from typing import BinaryIO, Dict, Any, Union, Optional, List, TYPE_CHECKING

import time

from messageflux.iodevices.base import OutputDevice, OutputDeviceException, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.rabbitmq.rabbitmq_device_manager_base import RabbitMQDeviceManagerBase
from messageflux.metadata_headers import MetadataHeaders
from messageflux.utils import get_random_id, ThreadLocalMember

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


class RabbitMQOutputDevice(OutputDevice['RabbitMQOutputDeviceManager']):
    """
    represents an RabbitMQ output devices
    """

    def __init__(self, device_manager: 'RabbitMQOutputDeviceManager', routing_key: str, exchange: str = ''):
        """
        constructs a new output RabbitMQ device

        :param device_manager: the RabbitMQ device Manager that holds this device
        :param routing_key: the routing key for this queue
        :param exchange: the exchange name in RabbitMQ for this output device
        """
        super(RabbitMQOutputDevice, self).__init__(device_manager, routing_key)
        self._routing_key = routing_key
        self._exchange = exchange
        self._logger = logging.getLogger(__name__)

    def _send_message(self, message_bundle: MessageBundle):
        rabbit_headers = self.manager.default_rabbit_headers.copy()
        custom_headers = message_bundle.device_headers
        # if specific rabbitmq headers we want them to override other headers
        rabbit_headers.update(custom_headers)
        self.manager.publish_message(
            routing_key=self._routing_key,
            data=message_bundle.message.stream,
            exchange=self._exchange,
            headers=message_bundle.message.headers,
            app_id=rabbit_headers.get('app_id', "BASESERVICE"),
            message_id=rabbit_headers.get('message_id',
                                          message_bundle.message.headers.get(MetadataHeaders.ITEM_ID, get_random_id())),
            persistent=rabbit_headers.get('persistent', True),
            mandatory=rabbit_headers.get('mandatory', self.manager.publish_confirm),
            priority=rabbit_headers.get('priority', None),
            expiration=rabbit_headers.get('expiration', None)
        )


class LengthValidationException(OutputDeviceException):
    """
    this exception is used when a length validation error occurs in IODevice
    """
    pass


class RabbitMQOutputDeviceManager(RabbitMQDeviceManagerBase, OutputDeviceManager[RabbitMQOutputDevice]):
    """
    this manager is used to create RabbitMQ devices
    """
    _PUBLISH_CONFIRM_HEADER = "__RABBITMQ_PUBLISH_CONFIRM__"

    _outgoing_channel: Union[ThreadLocalMember[Optional['BlockingChannel']],
                             Optional['BlockingChannel']] = ThreadLocalMember(init_value=None)

    def __init__(self,
                 hosts: Union[List[str], str],
                 user: str,
                 password: str,
                 port: Optional[int] = None,
                 ssl_context: Optional[ssl.SSLContext] = None,
                 virtual_host: Optional[str] = None,
                 default_output_exchange: str = '',
                 publish_confirm: bool = True,
                 client_args: Optional[Dict[str, str]] = None,
                 heartbeat: int = 300,
                 connection_attempts: int = 5,
                 max_message_length: int = -1,
                 max_header_value_length: int = 2048,
                 max_header_name_length: int = 1024,
                 default_rabbit_headers: Optional[Dict[str, Any]] = None,
                 blocked_connection_timeout: Optional[float] = None,
                 ):
        """
        This manager used to create RabbitMQ devices (direct queues)

        :param hosts: the hostname or a list of hostnames of the manager
        :param user: the username for the rabbitMQ manager
        :param password: the password for the rabbitMQ manager
        :param port: the port to connect the hosts to
        :param ssl_context: the ssl context to use. None means don't use ssl at all
        :param virtual_host: the virtual host to connect to
        :param default_output_exchange: the default exchange used for output devices
        :param publish_confirm: should the send fail if the message is unroutable?
        :param client_args: the arguments to create the client with
        :param int heartbeat: heartbeat interval for the connection (between 0 and 65536
        :param int connection_attempts: Maximum number of retry attempts
        :param int max_message_length: the maximum size (in bytes) of a message to send (0 and below = no limit)
        :param int max_header_value_length: the maximum length of a header's value (0 and below = no limit)
        :param int max_header_name_length: the maximum length of a header's name (0 and below = no limit)
        :param dict default_rabbit_headers: The default headers to use when sending
        :param blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection:
            passing `ConnectionBlockedTimeout` exception to on_close_callback
            in asynchronous adapters or raising it in `BlockingConnection`.
        """
        super(RabbitMQOutputDeviceManager, self).__init__(hosts=hosts,
                                                          user=user,
                                                          password=password,
                                                          port=port,
                                                          ssl_context=ssl_context,
                                                          virtual_host=virtual_host,
                                                          client_args=client_args,
                                                          connection_type="Output",
                                                          heartbeat=heartbeat,
                                                          connection_attempts=connection_attempts,
                                                          blocked_connection_timeout=blocked_connection_timeout)

        self._default_output_exchange = default_output_exchange
        self._publish_confirm = publish_confirm
        self._max_message_length = max_message_length
        self._max_header_value_length = max_header_value_length
        self._max_header_name_length = max_header_name_length
        self._default_rabbit_headers = {
            'persistent': True,
            'mandatory': self.publish_confirm,
            'priority': None,
            'expiration': None
        }

        if default_rabbit_headers is None:
            default_rabbit_headers = {}

        self._default_rabbit_headers.update(default_rabbit_headers)

    @property
    def default_rabbit_headers(self) -> Dict[str, Any]:
        """
        returns the default rabbit headers
        """
        return self._default_rabbit_headers

    @property
    def publish_confirm(self) -> bool:
        """
        returns the publish_confirm flag
        """
        return self._publish_confirm

    def publish_message(self,
                        routing_key: str,
                        data: BinaryIO,
                        exchange: str = "",
                        headers: Optional[Dict[str, Any]] = None,
                        app_id: Optional[str] = None,
                        message_id: Optional[str] = None,
                        persistent: bool = True,
                        mandatory: bool = False,
                        priority: Optional[int] = None,
                        expiration: Optional[int] = None):
        """
        publishes a message, and reconnects if there's an error

        :param routing_key: The routing key to bind on
        :param data: the stream to send
        :param exchange: The exchange to publish to
        :param headers: the headers to send to queue
        :param str app_id: the app_id to send with the message
        :param str message_id: the id to five the message
        :param bool persistent: should the message be persisted to disk
        :param bool mandatory: The mandatory flag
        :param int priority: the message priority (0-9)
        :param expiration: ttl (in ms) of message
        """
        try:
            from pika.exceptions import (AMQPConnectionError,
                                         UnroutableError,
                                         AMQPChannelError,
                                         ConnectionBlockedTimeout,
                                         NackError)
        except ImportError as ex:
            raise ImportError('Please Install the required extra: messageflux[rabbitmq]') from ex

        if headers is not None:
            # Safety measure for removing input device headers
            for header_name, header_value in headers.items():
                if hasattr(header_name, '__len__') and len(header_name) > self._max_header_name_length > 0:
                    raise LengthValidationException(
                        f"Header name ({header_name}) is longer than {self._max_header_name_length}")
                if hasattr(header_value, '__len__') and len(header_value) > self._max_header_value_length > 0:
                    raise LengthValidationException(
                        f"Value of header {header_name} is longer than {self._max_header_value_length} "
                        f"(header value is {header_value})")

        length = data.seek(0, 2)
        if length >= self._max_message_length > 0:
            raise LengthValidationException(f"file is too big for rabbitmq: {routing_key}, length: {length}")
        data.seek(0)

        try:
            self._inner_publish(routing_key, data, exchange, headers, app_id, message_id,
                                persistent, mandatory, priority, expiration)
        except NackError:
            self._logger.exception(
                f'Publish on routing key "{routing_key}" was NACKed. (possibly the queue reached its max size)')
            raise
        except UnroutableError:
            self._logger.exception(f"could not route message. routing key: {routing_key}")
            raise
        except ConnectionBlockedTimeout:
            self._logger.exception(
                f"could not publish message, due to ConnectionBlocked (possibly Memory Alarm). "
                f"routing key: {routing_key}")
            raise

        except (AMQPConnectionError, AMQPChannelError):
            self._logger.warning(f"failed to send message to queue. routing key: {routing_key}", exc_info=True)
            try:
                self._inner_publish(routing_key, data, exchange, headers, app_id, message_id,
                                    persistent, mandatory, priority, expiration)
            except (AMQPConnectionError, AMQPChannelError):
                self._logger.exception(f"failed to send message to queue. routing key: {routing_key}")
                raise

        except Exception as ex:
            raise OutputDeviceException("Error while sending to RabbitMQ") from ex

    def _inner_publish(self,
                       routing_key: str,
                       data: BinaryIO,
                       exchange: str = "",
                       headers: Optional[Dict[str, Any]] = None,
                       app_id: Optional[str] = None,
                       message_id: Optional[str] = None,
                       persistent: bool = True,
                       mandatory: bool = False,
                       priority: Optional[int] = None,
                       expiration: Optional[int] = None):
        """
        does the actual publish to queue

        :param routing_key: The routing key to bind on
        :param data: the stream to send
        :param exchange: The exchange to publish to
        :param headers: the headers to send to queue
        :param str app_id: the app_id to send with the message
        :param str message_id: the id to five the message
        :param bool persistent: should the message be persisted to disk
        :param bool mandatory: The mandatory flag
        :param int priority: the message priority (0-9)
        :param int expiration: ttl (in ms) of message
        """
        try:
            import pika
        except ImportError as ex:
            raise ImportError('Please Install the required extra: messageflux[rabbitmq]') from ex

        if headers is None:
            headers = {}
        else:
            headers = headers.copy()
        headers[self._PUBLISH_CONFIRM_HEADER] = self.publish_confirm

        data.seek(0)

        str_expiration: Optional[str] = None
        if expiration is not None:
            str_expiration = str(expiration)

        properties = pika.BasicProperties(delivery_mode=2 if persistent else 1,
                                          headers=headers,
                                          app_id=app_id,
                                          message_id=message_id,
                                          timestamp=int(time.time()),
                                          priority=priority,
                                          expiration=str_expiration)

        channel = self.get_outgoing_channel()
        channel.basic_publish(exchange, routing_key, data.read(), properties,
                              mandatory=mandatory)

    def get_outgoing_channel(self) -> 'BlockingChannel':
        """
        returns a rabbitmq channel for publishing messages

        :return: a rabbit mq channel
        """
        try:
            if self._outgoing_channel is None or not self._outgoing_channel.is_open or not self.connection.is_open:
                self._outgoing_channel = self.connection.channel()
                assert self._outgoing_channel is not None
                if self._publish_confirm:
                    self._outgoing_channel.confirm_delivery()

            return self._outgoing_channel
        except Exception as ex:
            raise OutputDeviceException('Could not connect to rabbitmq.') from ex

    def get_output_device(self, name: str, exchange: Optional[str] = None) -> RabbitMQOutputDevice:
        """
        Returns and outgoing device by name

        :param name: the name of the device to write to
        :param exchange: the exchange name in RabbitMQ for this output device
        :return: an output device for 'device_name'
        """
        try:
            if exchange is None:
                exchange = self._default_output_exchange

            return RabbitMQOutputDevice(self, name, exchange)
        except Exception as e:
            message = f"Couldn't create output device '{name}'"
            self._logger.exception(message)
            raise OutputDeviceException(message) from e

    def connect(self):
        """
        connects to the device manager
        """
        try:
            self._connect()
        except Exception as ex:
            raise OutputDeviceException('Could not connect to rabbitmq.') from ex

    def disconnect(self):
        """
        disconnects from the device manager
        """
        self._disconnect()
