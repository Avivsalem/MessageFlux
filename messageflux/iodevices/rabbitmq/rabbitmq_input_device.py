import logging
import os
import socket
import ssl
import threading
from io import BytesIO
from typing import TYPE_CHECKING, Dict, Optional, Tuple, Union, List, Any

from messageflux.iodevices.base import InputDevice, InputTransaction, ReadResult, InputDeviceException, Message, \
    InputDeviceManager
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.rabbitmq.rabbitmq_device_manager_base import RabbitMQDeviceManagerBase
from messageflux.utils import ThreadLocalMember

try:
    from pika import spec
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[rabbitmq]') from ex

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


class RabbitMQInputTransaction(InputTransaction):
    """
    represents a InputTransaction for RabbitMQ
    """

    def __init__(self,
                 cancellation_token: threading.Event,
                 device: 'RabbitMQInputDevice',
                 channel: 'BlockingChannel',
                 delivery_tag: int):
        """

        :param cancellation_token: the cancellation token, to check before acking
        :param device: the device that returned this transaction
        :param channel: the BlockingChannel that the item was read from
        :param delivery_tag: the delivery tag for this item
        """
        super(RabbitMQInputTransaction, self).__init__(device=device)
        self._cancellation_token = cancellation_token
        self._channel = channel
        self._delivery_tag = delivery_tag
        self._logger = logging.getLogger(__name__)

    @property
    def channel(self) -> 'BlockingChannel':
        """
        the channel that the item was read from
        """
        return self._channel

    @property
    def delivery_tag(self) -> int:
        """
        the delivery tag for this item
        """
        return self._delivery_tag

    def _commit(self):
        try:
            if self._cancellation_token.is_set():
                try:
                    self._channel.cancel()  # cancel the consumer before acking
                except Exception:
                    pass
            self._channel.basic_ack(self._delivery_tag)
        except Exception:
            self._logger.warning('commit failed', exc_info=True)

    def _rollback(self):
        try:
            if self._cancellation_token.is_set():
                try:
                    self._channel.cancel()  # cancel the consumer before nacking
                except Exception:
                    pass
            self._channel.basic_nack(self._delivery_tag, requeue=True)
        except Exception:
            self._logger.warning('rollback failed', exc_info=True)


class RabbitMQInputDevice(InputDevice['RabbitMQInputDeviceManager']):
    """
    represents an RabbitMQ input device
    """

    _channel: Union[ThreadLocalMember[Optional['BlockingChannel']], Optional['BlockingChannel']] = ThreadLocalMember(
        init_value=None)

    @staticmethod
    def _get_rabbit_headers(method_frame, header_frame):
        return {
            "exchange": method_frame.exchange,
            "routing_key": method_frame.routing_key,
            "content_type": header_frame.content_type,
            "content_encoding": header_frame.content_encoding,
            "priority": header_frame.priority,
            "correlation_id": header_frame.correlation_id,
            "reply_to": header_frame.reply_to,
            "expiration": header_frame.expiration,
            "message_id": header_frame.message_id,
            "timestamp": header_frame.timestamp,
            "type": header_frame.type,
            "user_id": header_frame.user_id,
            "app_id": header_frame.app_id
        }

    def __init__(self,
                 device_manager: 'RabbitMQInputDeviceManager',
                 queue_name: str,
                 consumer_args: Optional[Dict[str, str]] = None,
                 prefetch_count: int = 1,
                 use_consumer: bool = True):
        """
        constructs a new input RabbitMQ device

        :param device_manager: the RabbitMQ device Manager that holds this device
        :param queue_name: the name for the queue
        :param consumer_args: the arguments to create the consumer with
        only relevent if "use_consumer" is True
        :param int prefetch_count: the number of unacked messages that can be consumed
        only relevent if "use_consumer" is True
        :param bool use_consumer: True to use the 'consume' method, False to use 'basic_get'
        """
        super().__init__(device_manager, queue_name)
        self._device_manager = device_manager
        self._queue_name = queue_name
        self._logger = logging.getLogger(__name__)
        if consumer_args is None:
            consumer_args = {'hostname': socket.gethostname(), 'PID': str(os.getpid())}

        self._consumer_args = consumer_args
        self._prefetch_count = max(1, prefetch_count)
        self._use_consumer = use_consumer
        self._last_consumer_auto_ack: Optional[bool] = None

    def _reconnect_device_manager(self):
        """
        reconnects the RabbitMQ device manager
        """
        try:
            if self._channel is not None and self._channel.is_open:
                assert self._channel is not None
                if self._use_consumer:
                    try:
                        self._channel.cancel()
                    except Exception:
                        pass
                self._channel.close()
            self._channel = self._device_manager.connection.channel()

            assert self._channel is not None
            self._channel.basic_qos(prefetch_count=self._prefetch_count)
        except Exception as e:
            raise InputDeviceException('Could not connect to rabbitmq.') from e

    def _get_channel(self) -> 'BlockingChannel':
        """
        gets a channel
        """
        if self._channel is None or not self._channel.is_open:
            self._reconnect_device_manager()

        assert self._channel is not None
        return self._channel

    def _get_data_from_queue(self,
                             cancellation_token: threading.Event,
                             timeout: Optional[float],
                             with_transaction: bool) -> Optional['ReadResult']:
        """
        performs a single read from queue

        :param cancellation_token: the cancellation token, to pass to transaction
        :param timeout: the timeout in seconds to block. negative number means no blocking
        :param with_transaction: does this read is to be done with transaction?
        :return: the stream and metadata, or None,None if no message in queue
        """
        channel = self._get_channel()
        get_timeout: Optional[float] = None
        if timeout is not None:
            get_timeout = max(0.01, timeout)
        body: Optional[bytes]
        header_frame: Optional[spec.BasicProperties]
        method_frame: Optional[Union[spec.Basic.Deliver, spec.Basic.GetOk]]

        body, header_frame, method_frame = self._get_frames_from_queue(channel,
                                                                       get_timeout,
                                                                       with_transaction=with_transaction)
        if method_frame is None:  # no message in queue
            return None

        assert body is not None
        assert header_frame is not None

        return self._create_response_from_frames(cancellation_token=cancellation_token,
                                                 body=body,
                                                 header_frame=header_frame,
                                                 method_frame=method_frame,
                                                 channel=channel,
                                                 with_transaction=with_transaction)

    def _create_response_from_frames(self,
                                     cancellation_token: threading.Event,
                                     body: bytes,
                                     header_frame: spec.BasicProperties,
                                     method_frame: Union[spec.Basic.Deliver, spec.Basic.GetOk],
                                     channel: 'BlockingChannel',
                                     with_transaction: bool) -> ReadResult:
        """
        creates the read result from the data returned from rabbitmq

        :param cancellation_token: the cancellation token, to pass to transaction
        :param body: the body of the message
        :param header_frame: the header frame of the message
        :param method_frame: the method frame of the message
        :param channel: the channel we read the message from
        :param with_transaction: should we use a transaction

        :return: ReadResult object
        """

        delivery_tag = method_frame.delivery_tag
        assert delivery_tag is not None
        if with_transaction:
            transaction: InputTransaction = RabbitMQInputTransaction(cancellation_token=cancellation_token,
                                                                     device=self,
                                                                     channel=channel,
                                                                     delivery_tag=delivery_tag)
        else:
            transaction = NULLTransaction(self)
        headers: Dict[str, Any] = header_frame.headers or {}  # type: ignore
        # get the rabbitmq headers as for device headers
        rabbit_headers = self._get_rabbit_headers(method_frame, header_frame)

        buf = BytesIO(body)
        return ReadResult(message=Message(buf, headers),
                          device_headers=rabbit_headers,
                          transaction=transaction)

    def _get_frames_from_queue(self,
                               channel: 'BlockingChannel',
                               timeout: Optional[float],
                               with_transaction: bool) -> Tuple[
        Optional[bytes],
        Optional[spec.BasicProperties],
        Optional[Union[spec.Basic.Deliver, spec.Basic.GetOk]]
    ]:

        """
        gets the actual frame from queue. use_consumer effects this method

        :param BlockingChannel channel: the channel
        :param float timeout: the timeout
        :param bool with_transaction: do we operate within a transaction scope
        """
        # this could be auto_ack = not has_transaction, but it's less clear... so it's verbose here...
        if with_transaction:
            auto_ack = False
        else:
            auto_ack = True

        if self._use_consumer:
            if self._last_consumer_auto_ack is None:
                self._last_consumer_auto_ack = auto_ack

            if self._last_consumer_auto_ack != auto_ack:
                channel.cancel()
                self._last_consumer_auto_ack = auto_ack

            method_frame: Optional[Union[spec.Basic.Deliver, spec.Basic.GetOk]]
            header_frame: Optional[spec.BasicProperties]
            body: Optional[bytes]

            method_frame, header_frame, body = next(channel.consume(queue=self._queue_name,
                                                                    inactivity_timeout=timeout,
                                                                    arguments=self._consumer_args, auto_ack=auto_ack))
        else:
            method_frame, header_frame, body = channel.basic_get(queue=self._queue_name,  # type: ignore
                                                                 auto_ack=auto_ack)

        return body, header_frame, method_frame

    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        """
        reads a stream from InputDevice (tries getting a message. if it fails, reconnects and tries again once)

        :param timeout: the timeout in seconds to block. negative number means no blocking
        :return: a tuple of stream and metadata from InputDevice, or (None, None) if no message is available
        """
        try:
            from pika.exceptions import AMQPConnectionError, AMQPChannelError
        except ImportError as exc:
            raise ImportError('Please Install the required extra: messageflux[rabbitmq]') from exc

        try:
            return self._get_data_from_queue(cancellation_token=cancellation_token,
                                             timeout=timeout,
                                             with_transaction=with_transaction)
        except (AMQPConnectionError, AMQPChannelError):
            self._reconnect_device_manager()
            try:
                return self._get_data_from_queue(cancellation_token=cancellation_token,
                                                 timeout=timeout,
                                                 with_transaction=with_transaction)
            except Exception:
                self._logger.exception(f"AMQError thrown. failed to get message. device name: {self._queue_name}")
                raise
        except Exception as e:
            raise InputDeviceException('Error reading from device') from e

    def close(self):
        """
        closes the connection to device
        """
        try:
            if self._channel is not None and self._channel.is_open:
                if self._use_consumer:
                    self._channel.cancel()
                self._channel.close()
        except Exception:
            self._logger.warning('Error Closing Device', exc_info=True)

        self._channel = None


class RabbitMQInputDeviceManager(RabbitMQDeviceManagerBase, InputDeviceManager[RabbitMQInputDevice]):
    """
    rabbitmq input device manager
    """

    def __init__(self,
                 hosts: Union[List[str], str],
                 user: str,
                 password: str,
                 port: Optional[int] = None,
                 ssl_context: Optional[ssl.SSLContext] = None,
                 virtual_host: Optional[str] = None,
                 client_args: Optional[Dict[str, str]] = None,
                 heartbeat: int = 300,
                 connection_attempts: int = 5,
                 prefetch_count: int = 1,
                 use_consumer: bool = True,
                 blocked_connection_timeout: Optional[float] = None,
                 default_direct_exchange: Optional[str] = None
                 ):
        """
        This manager used to create RabbitMQ devices (direct queues)

        :param hosts: the hostname or a list of hostnames of the manager
        :param user: the username for the rabbitMQ manager
        :param password: the password for the rabbitMQ manager
        :param port: the port to connect the hosts to
        :param ssl_context: the ssl context to use. None means don't use ssl at all
        :param virtual_host: the virtual host to connect to
        :param client_args: the arguments to create the client with
        :param int heartbeat: heartbeat interval for the connection (between 0 and 65536
        :param int connection_attempts: Maximum number of retry attempts
        (-1 means not to handle poison messages at all, 0 means reject all redelivered messages right away)
        :param int prefetch_count: the number of unacked messages that can be consumed
        :param bool use_consumer: True to use the 'consume' method, False to use 'basic_get'
        :param blocked_connection_timeout: If not None,
            the value is a non-negative timeout, in seconds, for the
            connection to remain blocked (triggered by Connection.Blocked from
            broker); if the timeout expires before connection becomes unblocked,
            the connection will be torn down, triggering the adapter-specific
            mechanism for informing client app about the closed connection:
            passing `ConnectionBlockedTimeout` exception to on_close_callback
            in asynchronous adapters or raising it in `BlockingConnection`.

        :param default_direct_exchange: optional direct exchange to bind all the queues to (None means no bind)
        """
        super().__init__(hosts=hosts,
                         user=user,
                         password=password,
                         port=port,
                         ssl_context=ssl_context,
                         virtual_host=virtual_host,
                         client_args=client_args,
                         connection_type="Input",
                         heartbeat=heartbeat,
                         connection_attempts=connection_attempts,
                         blocked_connection_timeout=blocked_connection_timeout)

        self._prefetch_count = prefetch_count
        self._use_consumer = use_consumer
        self._default_direct_exchange = default_direct_exchange

    def _device_factory(self, device_name: str) -> RabbitMQInputDevice:

        return RabbitMQInputDevice(device_manager=self,
                                   queue_name=device_name,
                                   consumer_args=self._client_args,
                                   prefetch_count=self._prefetch_count,
                                   use_consumer=self._use_consumer)

    def get_input_device(self, name: str) -> RabbitMQInputDevice:
        """
        Returns an incoming device by name

        :param name: the name of the device to read from
        :return: an input device for 'device_name'
        """
        try:
            self.create_queue(queue_name=name,
                              passive=True,
                              direct_bind_to_exchange=self._default_direct_exchange)

            return self._device_factory(name)

        except Exception as e:
            message = f"Couldn't create input device '{name}'"
            self._logger.exception(message)
            raise InputDeviceException(message) from e

    def connect(self):
        """
        connects to the device manager
        """
        try:
            self._connect()
        except Exception as e:
            raise InputDeviceException('Could not connect to rabbitmq.') from e

    def disconnect(self):
        """
        disconnects from the device manager
        """
        self._disconnect()
