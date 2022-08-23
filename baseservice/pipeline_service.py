from abc import ABCMeta, abstractmethod
from typing import List, Tuple, Union

from baseservice.device_reader_service import DeviceReaderService
from baseservice.iodevices.base import InputDevice, Message, DeviceHeaders, OutputDeviceManager


class PipelineHandlerBase(metaclass=ABCMeta):

    @abstractmethod
    def handle_message(self,
                       input_device: InputDevice,
                       message: Message,
                       device_headers: DeviceHeaders) -> Union[Tuple[None, None, None],
                                                               Tuple[str, Message, DeviceHeaders]]:
        """
        Handles a message from an input device. and returns a tuple of the output device name, message and headers. to send to.

        :param input_device: The input device that sent the message.
        :param message: The message that was sent.
        :param device_headers: The headers of the device that sent the message.
        :return: (None, None, None) if the message should not be sent to any output device.
        (output_device_name, message, device_headers) if a message should be sent to the output device with the given name.
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
                       message: Message,
                       device_headers: DeviceHeaders) -> Union[Tuple[None, None, None],
                                                               Tuple[str, Message, DeviceHeaders]]:
        return self._output_device_name, message, device_headers


class PipelineService(DeviceReaderService):
    def __init__(self, *, output_device_manager: OutputDeviceManager, pipeline_handler: PipelineHandlerBase, **kwargs):
        super().__init__(**kwargs)
        self._output_device_manager = output_device_manager
        self._pipeline_handler = pipeline_handler

    def _handle_messages(self, batch: List[Tuple[InputDevice, Message, DeviceHeaders]]):
        for input_device, message, device_headers in batch:
            output_device_name, new_message, new_device_headers = self._pipeline_handler.handle_message(input_device,
                                                                                                        message,
                                                                                                        device_headers)
            if output_device_name is not None and new_message is not None:
                output_device = self._output_device_manager.get_output_device(output_device_name)
                output_device.send_message(new_message, new_device_headers)
