from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from typing import Optional, TypeVar

try:
    from pydantic import BaseModel
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[pydantic]') from ex

from messageflux import InputDevice
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.pipeline_service import PipelineHandlerBase, PipelineResult


@dataclass
class PydanticPipelineResult:
    """
    a result from pipeline handler
    """
    output_device_name: str
    model: BaseModel


T = TypeVar('T', bound=BaseModel)


class PydanticPipelineHandler(PipelineHandlerBase, metaclass=ABCMeta):
    def __init__(self):
        self._model_annotation = self.handle_model.__annotations__.get('model', None)
        if (
                not self._model_annotation or
                not isinstance(self._model_annotation, type) or
                not issubclass(self._model_annotation, BaseModel)
        ):
            # TODO: better exception type
            raise ValueError(
                f"'model' is annotated as {self._model_annotation} which is not a pydantic BaseModel")

    def handle_message(self, input_device: InputDevice, message_bundle: MessageBundle) -> Optional[PipelineResult]:
        model = self._model_annotation.parse_raw(message_bundle.message.bytes)
        result = self.handle_model(input_device=input_device, model=model)
        if result is None:
            return result

        return PipelineResult(output_device_name=result.output_device_name,
                              message_bundle=MessageBundle(message=Message(data=result.model.json().encode())))

    @abstractmethod
    def handle_model(self, input_device: InputDevice, model: T) -> Optional[PydanticPipelineResult]:
        """
        handles a pydantic model.
        param 'model' must be type annotated with a type that inherits BaseModel



        :param input_device: The input device that sent the message.
        :param model: the model that was serialized from the stream

        :return: None if the message should not be sent to any output device.
        PydanticPipelineResult if a message should be sent to the output device with the given name.
        """
        pass
