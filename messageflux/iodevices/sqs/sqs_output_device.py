import logging
from typing import Optional, Dict, TYPE_CHECKING, Any

from messageflux.iodevices.base import (
    OutputDevice,
    OutputDeviceException,
    OutputDeviceManager,
)
from messageflux.iodevices.base.common import MessageBundle
from messageflux.iodevices.sqs.message_attributes import generate_message_attributes
from messageflux.iodevices.sqs.sqs_manager_base import SQSManagerBase
from messageflux.utils import get_random_id

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import SQSServiceResource


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
        self._message_group_id = get_random_id()

        # https://awscli.amazonaws.com/v2/documentation/api/latest/reference/sqs/get-queue-attributes.html#get-queue-attributes
        self._is_fifo = queue_name.endswith(".fifo")
        self._logger = logging.getLogger(__name__)

    def _send_message(self, message_bundle: MessageBundle):
        additional_args: Dict[str, Any] = {}
        if self._is_fifo:
            additional_args = dict(MessageGroupId=self._message_group_id)

        response = self._sqs_queue.send_message(
            MessageBody=message_bundle.message.bytes.decode(),
            MessageAttributes=generate_message_attributes(
                message_bundle.message.headers
            ),
            **additional_args,
        )

        if "MessageId" not in response:
            raise OutputDeviceException("Couldn't send message to SQS")


class SQSOutputDeviceManager(SQSManagerBase, OutputDeviceManager[SQSOutputDevice]):
    """
    this manager is used to create SQS devices
    """

    def __init__(self, sqs_resource: Optional['SQSServiceResource'] = None) -> None:
        """
        :param sqs_resource: the boto sqs service resource. Defaults to creating from env vars
        """
        super().__init__(sqs_resource=sqs_resource)
        self._device_cache: Dict[str, SQSOutputDevice] = {}

    def get_output_device(self, name: str) -> SQSOutputDevice:
        """
        Returns and outgoing device by name

        :param name: the name of the queue
        :return: an output device for 'queue_name'
        """
        try:
            device = self._device_cache.get(name, None)
            if device is None:
                device = SQSOutputDevice(self, name)
                self._device_cache[name] = device

            return device

        except Exception as e:
            message = f"Couldn't create output device '{name}'"
            self._logger.exception(message)
            raise OutputDeviceException(message) from e
