import ssl
import threading
from abc import ABCMeta, abstractmethod
from time import perf_counter
from typing import List, Dict, Optional

from messageflux.iodevices.base import InputTransaction, ReadResult
from messageflux.iodevices.rabbitmq.rabbitmq_input_device import RabbitMQInputDevice, RabbitMQInputDeviceManager


class PoisonCounterBase(metaclass=ABCMeta):
    """
    abstract class for a persistent counter store for message ids
    """

    def start(self):
        """
        gets called before the first invocation
        """
        pass

    def stop(self):
        """
        gets called after the last invocation
        """
        pass

    @abstractmethod
    def increment_and_return_counter(self, message_id: str) -> int:
        """
        this methods increments and returns the counter for message_id.
        if there is no counter for message_id it should return '1'

        :param message_id: the message id to check the counter for
        :returns: the incremented counter for message_id
        """
        pass

    @abstractmethod
    def delete_counter(self, message_id: str):
        """
        deletes the counter for message_id

        :param message_id: the message id to delete the counter for
        """
        pass


class RabbitMQNoPoisonInputTransactionWrapper(InputTransaction):
    """
    represents a wrapper for InputTransaction for RabbitMQ with poison counter
    """

    def __init__(self,
                 inner_transaction: InputTransaction,
                 poison_counter: PoisonCounterBase,
                 message_id: str):
        """

        :param inner_transaction: the inner transaction
        :param poison_counter: the poison counter
        :param message_id: the message id in this transaction
        """
        super().__init__(inner_transaction.device)
        self._inner_transaction = inner_transaction
        self._poison_counter = poison_counter
        self._message_id = message_id

    def _commit(self):
        self._inner_transaction.commit()
        self._poison_counter.delete_counter(message_id=self._message_id)

    def _rollback(self):
        self._inner_transaction.rollback()


class RabbitMQPoisonCountingInputDevice(RabbitMQInputDevice):
    """
    represents an RabbitMQ input device, that adds handling with poison messages (for classic queues)
    """

    def __init__(self,
                 device_manager: 'RabbitMQPoisonCountingInputDeviceManager',
                 queue_name: str,
                 poison_counter: PoisonCounterBase,
                 max_poison_count=3,
                 consumer_args: Optional[Dict[str, str]] = None,
                 prefetch_count: int = 1,
                 use_consumer: bool = True):
        """
        constructs a new input RabbitMQ device

        :param device_manager: the RabbitMQ device Manager that holds this device
        :param queue_name: the name for the queue
        :param PoisonCounterBase poison_counter: the poison counter to use
        :param int max_poison_count: the number of times to requeue a message before backout
        (-1 means not to handle poison messages at all, 0 means reject all redelivered messages right away)
        :param consumer_args: the arguments to create the consumer with
        :param int prefetch_count: the number of un-acked messages that can be consumed
        :param bool use_consumer: True to use the 'consume' method, False to use 'basic_get'
        """
        super().__init__(device_manager=device_manager,
                         queue_name=queue_name,
                         consumer_args=consumer_args,
                         prefetch_count=prefetch_count,
                         use_consumer=use_consumer)
        self._max_poison_count = max_poison_count
        self._poison_counter = poison_counter

    def _get_data_from_queue(self,
                             cancellation_token: threading.Event,
                             timeout: Optional[float],
                             with_transaction: bool) -> Optional['ReadResult']:
        """
        performs a single read from queue

        :param cancellation_token: the cancellation token, to pass to transaction
        :param timeout: the timeout in seconds to block. None means no blocking
        :param with_transaction: does this read is to be done with transaction?
        :return: the stream and metadata, or None,None if no message in queue
        """
        start_time = perf_counter()

        while True:
            get_timeout: Optional[float] = None
            if timeout is not None:
                get_timeout = max(0.01, start_time + timeout - perf_counter())
            channel = self._get_channel()
            body, header_frame, method_frame = self._get_frames_from_queue(channel,
                                                                           get_timeout,
                                                                           with_transaction=with_transaction)
            if method_frame is None:  # no message in queue
                return None
            assert body is not None
            assert header_frame is not None

            redelivered = method_frame.redelivered

            if not redelivered or self._max_poison_count < 0:  # no need to protect from poison messages:
                break

            else:  # this message was redelivered. protect from poison message
                assert header_frame is not None
                message_id = f'{self.name}-{header_frame.message_id}-{header_frame.timestamp}'
                redeliver_count = self._poison_counter.increment_and_return_counter(message_id)
                if redeliver_count < self._max_poison_count:  # this message is below the max_poison_threshold
                    read_result = self._create_response_from_frames(cancellation_token=cancellation_token,
                                                                    body=body,
                                                                    header_frame=header_frame,
                                                                    method_frame=method_frame,
                                                                    channel=channel,
                                                                    with_transaction=with_transaction)
                    return ReadResult(message=read_result.message,
                                      device_headers=read_result.device_headers,
                                      transaction=RabbitMQNoPoisonInputTransactionWrapper(
                                          inner_transaction=read_result.transaction,
                                          poison_counter=self._poison_counter,
                                          message_id=message_id))

                # this point in code, means the message is a poison message.
                self._logger.error("Received message with backout count of more than {}, perhaps we are in a loop. "
                                   "handling backout. BackoutCount was {}. "
                                   "device name: {}".format(self._max_poison_count,
                                                            redeliver_count,
                                                            self._queue_name))

                self._poison_counter.delete_counter(message_id)
                if with_transaction:
                    assert method_frame is not None
                    assert method_frame.delivery_tag is not None
                    channel.basic_reject(method_frame.delivery_tag, requeue=False)

                # if the message was poison message, we want to skip it and read the next one.
                # but only if we're not exceeding timeout
                if timeout is not None:
                    if perf_counter() >= start_time + max(timeout, 1):
                        return None

                continue

        return self._create_response_from_frames(cancellation_token=cancellation_token,
                                                 body=body,
                                                 header_frame=header_frame,
                                                 method_frame=method_frame,
                                                 channel=channel,
                                                 with_transaction=with_transaction)


class RabbitMQPoisonCountingInputDeviceManager(RabbitMQInputDeviceManager):
    """
    rabbitmq input device manager, that adds handling with poison messages (for classic queues)
    """

    def __init__(self,
                 hosts: List[str],
                 user: str,
                 password: str,
                 poison_counter: PoisonCounterBase,
                 max_poison_count: int = 3,
                 port: Optional[int] = None,
                 ssl_context: Optional[ssl.SSLContext] = None,
                 virtual_host: Optional[str] = None,
                 client_args: Optional[Dict[str, str]] = None,
                 heartbeat: int = 300,
                 connection_attempts: int = 5,
                 prefetch_count: int = 1,
                 use_consumer: bool = True,
                 blocked_connection_timeout: Optional[float] = None,
                 default_direct_exchange: Optional[str] = None,
                 ):
        """
        This manager used to create RabbitMQ devices (direct queues)

        :param hosts: the list of hostnames of the manager
        :param user: the username for the rabbitMQ manager
        :param password: the password for the rabbitMQ manager
        :param PoisonCounterBase poison_counter: the poison counter to use
        :param int max_poison_count: the number of times to try to handle a message before backout
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
                         heartbeat=heartbeat,
                         connection_attempts=connection_attempts,
                         prefetch_count=prefetch_count,
                         use_consumer=use_consumer,
                         blocked_connection_timeout=blocked_connection_timeout,
                         default_direct_exchange=default_direct_exchange)

        self._max_poison_count = max_poison_count
        self._poison_counter = poison_counter

    def connect(self):
        """
        connects to the device manager
        """
        self._poison_counter.start()
        super().connect()

    def disconnect(self):
        """
        disconnects from the device manager
        """
        self._poison_counter.stop()
        super().disconnect()

    def _device_factory(self, device_name: str) -> RabbitMQPoisonCountingInputDevice:
        return RabbitMQPoisonCountingInputDevice(device_manager=self,
                                                 queue_name=device_name,
                                                 poison_counter=self._poison_counter,
                                                 max_poison_count=self._max_poison_count,
                                                 consumer_args=self._client_args,
                                                 prefetch_count=self._prefetch_count,
                                                 use_consumer=self._use_consumer)
