import logging
import threading

from io import BytesIO
from typing import Dict, Optional, Union

from messageflux.iodevices.base import InputDevice, InputTransaction, ReadResult, InputDeviceException, Message, \
    InputDeviceManager
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.sqs.sqs_manager_base import SQSManagerBase
from messageflux.utils import get_random_id


try:
    import boto3
    from mypy_boto3_sqs.service_resource import Queue
    from mypy_boto3_sqs.client import SQSClient
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[sqs]') from ex


class SQSInputTransaction(InputTransaction):
    """
    represents a InputTransaction for SQS
    """
    _device: 'SQSInputDevice'

    def __init__(self,
                 device: 'SQSInputDevice',
                 receipt_handle: str):
        """

        :param device: the device that returned this transaction
        :param receipt_handle: the receipt handle for this item
        """
        super(SQSInputTransaction, self).__init__(device=device)
        self._receipt_handle = receipt_handle
        self._logger = logging.getLogger(__name__)

    def _commit(self):
        try:
            self._device.delete_message(self._receipt_handle)
        except Exception:
            self._logger.exception('commit failed')

    def _rollback(self):
        try:
            self._device.change_visability_timeout(self._receipt_handle, 0)
        except Exception:
            self._logger.warning('rollback failed', exc_info=True)
    

class SQSInputDevice(InputDevice['SQSInputDeviceManager']):
    """
    represents an SQS input device
    """

    def __init__(self,
                 device_manager: 'SQSInputDeviceManager',
                 queue_name: str,
                 included_message_attributes: Optional[Union[str, list]] = None):
        """
        constructs a new input SQS device

        :param device_manager: the SQS device Manager that holds this device
        :param queue_name: the name for the queue

        """
        super().__init__(device_manager, queue_name)
        self._included_message_attributes = (
            included_message_attributes
            if included_message_attributes is not None
            else ["All"]
        )
        self._max_messages_per_request = 1
        self._queue = self.manager.get_queue(queue_name)

    def _read_message(self, cancellation_token: threading.Event, 
                            timeout: Optional[float] = None, 
                            with_transaction: bool = True) -> Optional['ReadResult']:
        """
        reads a stream from InputDevice (tries getting a message. if it fails, reconnects and tries again once)

        :param timeout: the timeout in seconds to block. negative number means no blocking
        :return: a tuple of stream and metadata from InputDevice, or (None, None) if no message is available
        """
        
        sqs_messages = self._queue.receive_messages(
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=self._max_messages_per_request
        )

        if not sqs_messages:
            return None
        
        assert len(sqs_messages) == 1, "SQSInputDevice should only return one message at a time"

        sqs_message = sqs_messages[0]

        return ReadResult(
            message=Message(
                headers={
                    key: value["StringValue"]
                    for key, value in sqs_message.message_attributes.items()
                },
                data=BytesIO(sqs_message.body.encode()),
            ),
            transaction = SQSInputTransaction(
                device=self,
                receipt_handle=sqs_message.receipt_handle,
            ) if with_transaction else NULLTransaction(self),
        )
    
    def delete_message(self, receipt_handle: str):
        """
        deletes a message from the queue

        :param receipt_handle: the receipt handle of the message
        """
        self._queue.delete_messages(
            Entries=[
                {
                    "Id": get_random_id(),
                    "ReceiptHandle": receipt_handle
                }
            ]
        )

    def change_visability_timeout(self, receipt_handle: str, timeout: int):
        """
        changes the visibility timeout of a message

        :param receipt_handle: the receipt handle of the message
        :param timeout: the new timeout in seconds
        """
        self._queue.change_message_visibility_batch(
            Entries=[
                {
                    "Id": get_random_id(),
                    "ReceiptHandle": receipt_handle,
                    "VisibilityTimeout": timeout
                }
            ]
        )

class SQSInputDeviceManager(SQSManagerBase, InputDeviceManager[SQSInputDevice]):
    """
    SQS input device manager
    """

    def __init__(self):
        super(SQSManagerBase, self).__init__()


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
