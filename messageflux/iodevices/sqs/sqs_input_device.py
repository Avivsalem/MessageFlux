import logging
import threading
from io import BytesIO
from typing import Optional, Union, TYPE_CHECKING

from messageflux.iodevices.base import (
    InputDevice,
    InputTransaction,
    ReadResult,
    InputDeviceException,
    Message,
    InputDeviceManager,
)
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.sqs.sqs_manager_base import SQSManagerBase

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Message as SQSMessage


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
            included_message_attributes: Optional[Union[str, list]] = None,  # TODO: what's this?
    ):
        """
        constructs a new input SQS device

        :param device_manager: the SQS device Manager that holds this device
        :param queue_name: the name for the queue

        """
        super().__init__(device_manager, queue_name)

        if included_message_attributes is None:
            included_message_attributes = ["All"]

        self._included_message_attributes = included_message_attributes
        self._max_messages_per_request = 1  # TODO: get this in manager
        self._queue = self.manager.get_queue(queue_name)

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
        if timeout is None:
            sqs_messages = self._queue.receive_messages(
                MessageAttributeNames=self._included_message_attributes,
                MaxNumberOfMessages=self._max_messages_per_request,
            )  # TODO: what's the visibility timeout? should we extend it?
        else:
            sqs_messages = self._queue.receive_messages(
                MessageAttributeNames=self._included_message_attributes,
                MaxNumberOfMessages=self._max_messages_per_request,
                WaitTimeSeconds=int(timeout)
            )  # TODO: what's the visibility timeout? should we extend it?

        if not sqs_messages:
            return None

        assert (len(sqs_messages) == 1), "SQSInputDevice should only return one message at a time"

        sqs_message = sqs_messages[0]

        transaction: InputTransaction
        if with_transaction:
            transaction = SQSInputTransaction(device=self,
                                              message=sqs_message)
        else:
            transaction = NULLTransaction(self)
            sqs_message.delete()

        return ReadResult(
            message=Message(
                headers={
                    key: value["BinaryValue"] if value['DataType'] == "Binary" else value['StringValue']
                    for key, value in sqs_message.message_attributes.items()
                },
                data=BytesIO(sqs_message.body.encode()),
            ),
            transaction=transaction
        )


class SQSInputDeviceManager(SQSManagerBase, InputDeviceManager[SQSInputDevice]):
    """
    SQS input device manager
    """

    def get_input_device(self, device_name: str) -> SQSInputDevice:
        """
        Returns an incoming device by name

        :param device_name: the name of the device to read from
        :return: an input device for 'device_name'
        """
        try:
            return SQSInputDevice(self, device_name)
        except Exception as e:
            message = f"Couldn't create input device '{device_name}'"
            self._logger.exception(message)
            raise InputDeviceException(message) from e
