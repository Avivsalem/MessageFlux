import logging

from typing import Dict

try:
    import boto3
    from mypy_boto3_sqs.service_resource import Queue
except ImportError as ex:
    raise ImportError("Please Install the required extra: messageflux[sqs]") from ex


class SQSManagerBase:
    def __init__(self) -> None:
        self._sqs_resource = boto3.resource("sqs")
        self._queue_cache: Dict[str, Queue] = {}
        self._logger = logging.getLogger(__name__)

    def get_queue(self, queue_name: str) -> Queue:
        """
        gets the queue from cache
        """
        queue = self._queue_cache.get(queue_name, None)
        if queue is None:
            queue = self._sqs_resource.get_queue_by_name(QueueName=queue_name)
            self._queue_cache[queue_name] = queue

        return queue
