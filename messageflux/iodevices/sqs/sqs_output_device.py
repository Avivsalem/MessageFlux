import logging
from typing import Dict

from messageflux.iodevices.base import OutputDevice, OutputDeviceException, OutputDeviceManager
from messageflux.iodevices.base.common import MessageBundle
from messageflux.metadata_headers import MetadataHeaders
from messageflux.utils import get_random_id

try:
    from mypy_boto3_sqs.service_resource import SQSServiceResource, Queue
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[sqs]') from ex


class SQSOutputDevice(OutputDevice['SQSOutputDeviceManager']):
    """
    represents an SQS output devices
    """

    def __init__(self, device_manager: 'SQSOutputDeviceManager', queue_name: str):
        """
        constructs a new output SQS device

        :param device_manager: the SQS device Manager that holds this device
        :param queue_name: the name of the queue
        """
        super(SQSOutputDevice, self).__init__(device_manager, queue_name)
        self._queue_name = queue_name
        self._logger = logging.getLogger(__name__)

    def _send_message(self, message_bundle: MessageBundle):
        sqs_queue = self.manager.get_queue(self._queue_name)

        sqs_queue.send_message(
            MessageBody=message_bundle.message.bytes.decode(),
            MessageAttributes=message_bundle.message.headers,
            MessageDeduplicationId=message_bundle.device_headers.get('message_id',
                                                                     message_bundle.message.headers.get(
                                                                         MetadataHeaders.ITEM_ID, get_random_id())),
        )


class SQSOutputDeviceManager(OutputDeviceManager[SQSOutputDevice]):
    """
    this manager is used to create SQS devices
    """

    def __init__(self, sqs_resource: SQSServiceResource):
        """
        This manager used to create SQS devices

        :param sqs_resource: the SQS resource from boto
        """
        self._logger = logging.getLogger(__name__)

        self._sqs_resource = sqs_resource
        self._queue_cache: Dict[str, Queue] = {}

    def get_output_device(self, queue_name: str) -> SQSOutputDevice:
        """
        Returns and outgoing device by name

        :param queue_name: the name of the queue
        :return: an output device for 'queue_url'
        """
        try:
            return SQSOutputDevice(self, queue_name)
        except Exception as e:
            message = f"Couldn't create output device '{queue_name}'"
            self._logger.exception(message)
            raise OutputDeviceException(message) from e

    def get_queue(self, queue_name: str, auto_create=False) -> Queue:
        """
        gets the bucket from cache
        """
        queue = self._queue_cache.get(queue_name, None)
        if queue is None:
            queue = self._sqs_resource.get_queue_by_name(QueueName=queue_name)
            self._queue_cache[queue_name] = queue

        return queue
