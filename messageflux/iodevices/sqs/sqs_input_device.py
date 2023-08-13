import logging

from io import BytesIO
from typing import Dict, Optional, Union, Any

from messageflux.iodevices.base import InputDevice, InputTransaction, ReadResult, InputDeviceException, Message, \
    InputDeviceManager
from messageflux.iodevices.base.input_transaction import NULLTransaction
from messageflux.iodevices.rabbitmq.rabbitmq_device_manager_base import RabbitMQDeviceManagerBase
from messageflux.iodevices.sqs.sqs_base import SQSManagerBase

try:
    from mypy_boto3_sqs.service_resource import Queue
    from mypy_boto3_sqs.client import SQSClient
    import boto3
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[sqs]') from ex


class SQSInputTransaction(InputTransaction):
    """
    represents a InputTransaction for SQS
    """
    _device: 'SQSInputDevice'

    def __init__(self,
                 device: 'SQSInputDevice',
                 delivery_tag: int):
        """

        :param device: the device that returned this transaction
        :param delivery_tag: the delivery tag for this item
        """
        super(SQSInputTransaction, self).__init__(device=device)
        self._delivery_tag = delivery_tag
        self._logger = logging.getLogger(__name__)

    @property
    def delivery_tag(self) -> int:
        """
        the delivery tag for this item
        """
        return self._delivery_tag

    def _commit(self):
        try:
            self._device.delete_message(self._delivery_tag)
        except Exception:
            self._logger.exception('commit failed')

    def _rollback(self):
        try:
            self._device.change_visability_timeout(self._delivery_tag, 0)
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
        self._queue_url = self.manager.client.get_queue_url(queue_name)
        self._included_message_attributes = (
            included_message_attributes
            if included_message_attributes is not None
            else ["All"]
        )
        self._max_messages_per_request = 1

    def _read_message(self, timeout: Optional[float] = None, with_transaction: bool = True) -> Optional['ReadResult']:
        """
        reads a stream from InputDevice (tries getting a message. if it fails, reconnects and tries again once)

        :param timeout: the timeout in seconds to block. negative number means no blocking
        :return: a tuple of stream and metadata from InputDevice, or (None, None) if no message is available
        """

        
        response = self.manager.client.receive_message(
            QueueUrl=self._queue_url,
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=self._max_messages_per_request
        )

        sqs_messages = response["Messages"]


        if sqs_messages:
            assert len(sqs_messages) == 1, "SQSDevice should only return one message at a time"
            sqs_message = sqs_messages[0]

            return ReadResult(
                message=Message(
                    headers=sqs_message["MessageAttributes"],
                    data=BytesIO(sqs_message["Body"].encode()),
                ),
                transaction = SQSInputTransaction(
                    device=self,
                    delivery_tag=sqs_message["ReceiptHandle"],
                ) if with_transaction else NULLTransaction(),
            )
        

    def delete_message(self, receipt_handle: str):
        """
        deletes a message from the queue

        :param receipt_handle: the receipt handle of the message
        """
        self.manager.client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle
        )

    def change_visability_timeout(self, receipt_handle: str, timeout: int):
        """
        changes the visibility timeout of a message

        :param receipt_handle: the receipt handle of the message
        :param timeout: the new timeout in seconds
        """
        self.manager.client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=timeout
        )

class SQSInputDeviceManager(SQSManagerBase, InputDeviceManager[SQSInputDevice]):
    """
    SQS input device manager
    """

    def __init__(self):
        self._sqs_client = boto3.client('sqs')
        self._queue_cache: Dict[str, Queue] = {}
        self._logger = logging.getLogger(__name__)


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

    @property
    def client(self) -> SQSClient:
        """
        returns the sqs client
        """
        return self._sqs_client