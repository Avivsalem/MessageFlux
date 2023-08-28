import logging
import threading
from io import BytesIO
from typing import Optional, Union, TYPE_CHECKING, Dict, List, Any

from messageflux.iodevices.base import (
    InputDevice,
    InputTransaction,
    ReadResult,
    InputDeviceException,
    Message,
    InputDeviceManager,
)
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.sqs.message_attributes import decode_message_attributes
from messageflux.iodevices.sqs.sqs_manager_base import SQSManagerBase

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Message as SQSMessage, SQSServiceResource


class SQSInputTransaction(InputTransaction):
    """
    represents a InputTransaction for SQS
    """

    _device: "SQSInputDevice"

    def __init__(self, device: "SQSInputDevice", message: 'SQSMessage'):
        """

        :param device: the device that returned this transaction
        :param message: the received message
        """
        super(SQSInputTransaction, self).__init__(device=device)
        self._message = message
        self._logger = logging.getLogger(__name__)

    def _commit(self):
        try:
            self._message.delete()
        except Exception:
            self._logger.exception("commit failed")

    def _rollback(self):
        try:
            self._message.change_visibility(VisibilityTimeout=0)
        except Exception:
            self._logger.warning("rollback failed", exc_info=True)


class SQSInputDevice(InputDevice["SQSInputDeviceManager"]):
    """
    represents an SQS input device
    """

    def __init__(
            self,
            device_manager: "SQSInputDeviceManager",
            queue_name: str,
            max_messages_per_request: int = 1,
            included_message_attributes: Optional[Union[str, List[str]]] = None,
    ):
        """
        constructs a new input SQS device

        :param device_manager: the SQS device Manager that holds this device
        :param queue_name: the name for the queue
        :param max_messages_per_request: maximum messages to retrieve from the queue (max 10)
        :param included_message_attributes: list of message attributes to get for the message. defaults to ALL

        """
        super().__init__(device_manager, queue_name)

        if included_message_attributes is None:
            included_message_attributes = ["All"]

        self._included_message_attributes = included_message_attributes
        self._max_messages_per_request = min(max_messages_per_request, 10)
        self._queue = self.manager.get_queue(queue_name)
        self._message_cache: List['SQSMessage'] = []

    def _get_sqs_message(self, timeout: Optional[float]) -> 'Optional[SQSMessage]':
        if not self._message_cache:
            additional_args: Dict[str, Any] = {}

            if timeout is not None:
                additional_args = dict(WaitTimeSeconds=int(timeout))

            sqs_messages = self._queue.receive_messages(
                MessageAttributeNames=self._included_message_attributes,
                MaxNumberOfMessages=self._max_messages_per_request,
                **additional_args
            )
            if not sqs_messages:
                return None
            self._message_cache.extend(sqs_messages)

        return self._message_cache.pop(0)

    def _read_message(
            self,
            cancellation_token: threading.Event,
            timeout: Optional[float] = None,
            with_transaction: bool = True,
    ) -> Optional["ReadResult"]:
        """
        reads a stream from InputDevice (tries getting a message. if it fails, reconnects and tries again once)

        :param timeout: the timeout in seconds to block. negative number means no blocking
        :return: a tuple of stream and metadata from InputDevice, or (None, None) if no message is available
        """

        sqs_message = self._get_sqs_message(timeout=timeout)
        if sqs_message is None:
            return None

        transaction: InputTransaction
        if with_transaction:
            transaction = SQSInputTransaction(device=self,
                                              message=sqs_message)
        else:
            transaction = NULLTransaction(self)
            sqs_message.delete()

        message_attributes = sqs_message.message_attributes or {}

        return ReadResult(
            message=Message(
                headers=decode_message_attributes(message_attributes),
                data=BytesIO(sqs_message.body.encode()),
            ),
            transaction=transaction
        )


class SQSInputDeviceManager(SQSManagerBase, InputDeviceManager[SQSInputDevice]):
    """
    SQS input device manager
    """

    def __init__(self, *,
                 sqs_resource: Optional['SQSServiceResource'] = None,
                 max_messages_per_request: int = 1,
                 included_message_attributes: Optional[Union[str, List[str]]] = None, ) -> None:
        """
        :param sqs_resource: the boto sqs service resource. Defaults to creating from env vars
        :param max_messages_per_request: maximum messages to retrieve from the queue (max 10)
        :param included_message_attributes: list of message attributes to get for the message. defaults to ALL
        """
        super().__init__(sqs_resource=sqs_resource)
        self._device_cache: Dict[str, SQSInputDevice] = {}
        self._max_messages_per_request = max_messages_per_request
        self._included_message_attributes = included_message_attributes

    def get_input_device(self, name: str) -> SQSInputDevice:
        """
        Returns an incoming device by name

        :param name: the name of the device to read from
        :return: an input device for 'device_name'
        """
        try:
            device = self._device_cache.get(name, None)
            if device is None:
                device = SQSInputDevice(device_manager=self,
                                        queue_name=name,
                                        max_messages_per_request=self._max_messages_per_request,
                                        included_message_attributes=self._included_message_attributes)

                self._device_cache[name] = device

            return device
        except Exception as e:
            message = f"Couldn't create input device '{name}'"
            self._logger.exception(message)
            raise InputDeviceException(message) from e
