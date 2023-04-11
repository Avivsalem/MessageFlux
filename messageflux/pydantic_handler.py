import inspect
import json
from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from typing import Optional, TypeVar, Generic, Any

try:
    from pydantic import BaseModel, parse_raw_as
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
    pydantic_object: Any


T = TypeVar('T')


class PydanticPipelineHandler(PipelineHandlerBase, Generic[T], metaclass=ABCMeta):
    def __init__(self):
        param = inspect.signature(self.handle_object).parameters.get('pydantic_object', None)
        if param is None:
            # TODO: better exception type
            raise ValueError("'pydantic_object' param is missing")

        self._model_annotation = param.annotation
        if self._model_annotation is inspect.Parameter.empty:
            # TODO: better exception type
            raise ValueError("'pydantic_object' param is not type annotated")

    def handle_message(self, input_device: InputDevice, message_bundle: MessageBundle) -> Optional[PipelineResult]:
        model: Any
        if inspect.isclass(self._model_annotation) and issubclass(self._model_annotation, MessageBundle):
            model = message_bundle
        elif inspect.isclass(self._model_annotation) and issubclass(self._model_annotation, Message):
            model = message_bundle.message
        else:
            model = parse_raw_as(self._model_annotation, message_bundle.message.bytes)

        result = self.handle_object(input_device=input_device, pydantic_object=model)
        if result is None:
            return result
        json_encoder = getattr(result.pydantic_object, '__json_encoder__', BaseModel.__json_encoder__)

        if isinstance(result.pydantic_object, MessageBundle):
            output_bundle = result.pydantic_object
        elif isinstance(result.pydantic_object, Message):
            output_bundle = MessageBundle(message=result.pydantic_object)
        else:
            output_data = json.dumps(result.pydantic_object, default=json_encoder).encode()
            output_bundle = MessageBundle(message=Message(data=output_data))

        return PipelineResult(output_device_name=result.output_device_name,
                              message_bundle=output_bundle)

    @abstractmethod
    def handle_object(self, input_device: InputDevice, pydantic_object: T) -> Optional[PydanticPipelineResult]:
        """
        handles a pydantic serializable object.
        param 'pydantic_object' must be type annotated with a type that inherits BaseModel

        :param input_device: The input device that sent the message.
        :param pydantic_object: the object that was serialized from the stream

        :return: None if the message should not be sent to any output device.
        PydanticPipelineResult if a message should be sent to the output device with the given name.
        """
        pass
