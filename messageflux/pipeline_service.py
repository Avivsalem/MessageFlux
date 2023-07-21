import logging
from abc import ABCMeta, abstractmethod
from typing import List, Tuple, Optional, Union

from messageflux.iodevices.base import InputDevice, OutputDeviceManager, ReadResult
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.message_handling_service import MessageHandlingServiceBase

_logger = logging.getLogger(__name__)


class PipelineResult:
    """
    a result from pipeline handler
    """

    def __init__(self,
                 output_device_name: str,
                 message_bundle: Union[MessageBundle, Message]):
        self.output_device_name = output_device_name
        if isinstance(message_bundle, Message):
            message_bundle = MessageBundle(message=message_bundle)

        self.message_bundle = message_bundle


class PipelineHandlerBase(metaclass=ABCMeta):
    """
    a pipeline handler base class. used to handle a single message that passes through the pipeline
    """

    @abstractmethod
    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[Union[PipelineResult, List[PipelineResult]]]:
        """
        Handles a message from an input device. and returns a tuple of the output device name, message and headers to
        send to.

        :param input_device: The input device that sent the message.
        :param message_bundle: The message that was received.

        :return: None if the message should not be sent to any output device.
        PipelineResult if a message should be sent to the output device with the given name.
        """
        pass


class FixedRouterPipelineHandler(PipelineHandlerBase):
    """
    A pipeline handler that routes all messages to a single output device. without any processing.
    """

    def __init__(self, output_device_name: str):
        self._output_device_name = output_device_name

    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[PipelineResult]:
        """
        routes the message to the fixed output

        :param input_device: The input device that sent the message.
        :param message_bundle: The message that was received.

        :return: a pipeline result that has the input message bundle, and the fixed output device name
        """
        output_message_bundle = MessageBundle(message=message_bundle.message.copy(), device_headers={})
        return PipelineResult(output_device_name=self._output_device_name,
                              message_bundle=output_message_bundle)


class PipelineService(MessageHandlingServiceBase):
    """
    a service that uses a PipelineHandler object to process messages from input, and send to output
    """

    def __init__(self, *,
                 pipeline_handler: PipelineHandlerBase,
                 output_device_manager: Optional[OutputDeviceManager] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self._output_device_manager = output_device_manager
        self._pipeline_handler = pipeline_handler

    def _handle_message_batch(self, batch: List[Tuple[InputDevice, ReadResult]]):
        for input_device, read_result in batch:
            pipeline_handler_result = self._pipeline_handler.handle_message(input_device, read_result)
            if pipeline_handler_result is not None:
                if not isinstance(pipeline_handler_result, List):
                    pipeline_handler_result = [pipeline_handler_result]
                for pipeline_result in pipeline_handler_result:
                    if self._output_device_manager is None:
                        _logger.warning("pipeline handler returned a result to output to device: "
                                        f"'{pipeline_result.output_device_name}', "
                                        f"but no output_device_manager was given")
                        continue

                    output_device = self._output_device_manager.get_output_device(pipeline_result.output_device_name)
                    output_device.send_message(message=pipeline_result.message_bundle.message,
                                               device_headers=pipeline_result.message_bundle.device_headers)
