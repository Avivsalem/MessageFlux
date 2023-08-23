import logging

from messageflux.iodevices.base import (
    OutputDevice,
    OutputDeviceException,
    OutputDeviceManager,
)
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.sqs.message_attributes import generate_message_attributes
from messageflux.iodevices.sqs.sqs_manager_base import SQSManagerBase
from messageflux.utils import get_random_id


class SQSOutputDevice(OutputDevice["SQSOutputDeviceManager"]):
    """
    represents an SQS output devices
    """

    def __init__(self, device_manager: "SQSOutputDeviceManager", queue_name: str):
        """
        constructs a new output SQS device

        :param device_manager: the SQS device Manager that holds this device
        :param queue_name: the name of the queue
        """
        super(SQSOutputDevice, self).__init__(device_manager, queue_name)
        self._sqs_queue = self.manager.get_queue(queue_name)

        # https://awscli.amazonaws.com/v2/documentation/api/latest/reference/sqs/get-queue-attributes.html#get-queue-attributes
        self._is_fifo = queue_name.endswith(".fifo")
        self._logger = logging.getLogger(__name__)

    def _send_message(self, message_bundle: MessageBundle):
        if self._is_fifo:
            response = self._sqs_queue.send_message(
                MessageBody=message_bundle.message.bytes.decode(),
                MessageAttributes=generate_message_attributes(
                    message_bundle.message.headers
                ),
                MessageGroupId=get_random_id(),
            )
        else:
            response = self._sqs_queue.send_message(
                MessageBody=message_bundle.message.bytes.decode(),
                MessageAttributes=generate_message_attributes(
                    message_bundle.message.headers
                ),
            )

        if "MessageId" not in response:
            raise OutputDeviceException("Couldn't send message to SQS")


class SQSOutputDeviceManager(SQSManagerBase, OutputDeviceManager[SQSOutputDevice]):
    """
    this manager is used to create SQS devices
    """

    def get_output_device(self, queue_name: str) -> SQSOutputDevice:
        """
        Returns and outgoing device by name

        :param queue_name: the name of the queue
        :return: an output device for 'queue_name'
        """
        try:
            return SQSOutputDevice(self, queue_name)
        except Exception as e:
            message = f"Couldn't create output device '{queue_name}'"
            self._logger.exception(message)
            raise OutputDeviceException(message) from e
