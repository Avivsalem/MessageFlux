from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import List, Tuple, Optional

from messageflux.device_reader_service import DeviceReaderService
from messageflux.iodevices.base import InputDevice, OutputDeviceManager, ReadResult
from messageflux.iodevices.base.common import MessageBundle


@dataclass
class PipelineResult:
    """
    a result from pipeline handler
    """
    output_device_name: str
    message_bundle: MessageBundle


class PipelineHandlerBase(metaclass=ABCMeta):
    """
    a pipeline handler base class. used to handle a single message that passes through the pipeline
    """

    @abstractmethod
    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[PipelineResult]:
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


class PipelineService(DeviceReaderService):
    """
    a service that uses a PipelineHandler object to process messages from input, and send to output
    """
    def __init__(self, *, output_device_manager: OutputDeviceManager, pipeline_handler: PipelineHandlerBase, **kwargs):
        super().__init__(**kwargs)
        self._output_device_manager = output_device_manager
        self._pipeline_handler = pipeline_handler

    def _handle_messages(self, batch: List[Tuple[InputDevice, ReadResult]]):
        for input_device, read_result in batch:
            pipeline_result = self._pipeline_handler.handle_message(input_device, read_result)
            if pipeline_result is not None:
                output_device = self._output_device_manager.get_output_device(pipeline_result.output_device_name)
                output_device.send_message(message=pipeline_result.message_bundle.message,
                                           device_headers=pipeline_result.message_bundle.device_headers)
