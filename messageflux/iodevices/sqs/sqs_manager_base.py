import logging

from typing import Dict, TYPE_CHECKING, Optional

try:
    import boto3
except ImportError as ex:
    raise ImportError("Please Install the required extra: messageflux[sqs]") from ex

if TYPE_CHECKING:
    from mypy_boto3_sqs.service_resource import Queue, SQSServiceResource


class SQSManagerBase:
    """
    base class for sqs device managers
    """

    def __init__(self, *,
                 sqs_resource: Optional['SQSServiceResource'] = None) -> None:
        """
        :param sqs_resource: the boto sqs service resource. Defaults to creating from env vars
        """
        if sqs_resource is None:
            sqs_resource = boto3.resource('sqs')

        self._sqs_resource = sqs_resource
        self._queue_cache: Dict[str, 'Queue'] = {}
        self._logger = logging.getLogger(__name__)

    def get_queue(self, queue_name: str) -> 'Queue':
        """
        gets the queue from cache
        """
        queue = self._queue_cache.get(queue_name, None)
        if queue is None:
            queue = self._sqs_resource.get_queue_by_name(QueueName=queue_name)
            self._queue_cache[queue_name] = queue

        return queue

    def create_queue(self, queue_name: str, **kwargs) -> 'Queue':
        """
        creates a queue

        :param queue_name: the queue name to create
        :return: the newly created queue
        """

        return self._sqs_resource.create_queue(QueueName=queue_name, **kwargs)
