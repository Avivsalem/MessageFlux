import inspect
import json
import logging
from dataclasses import dataclass
from typing import Optional, Callable, Dict, List, Any, TypeVar, Union

from messageflux import InputDevice
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.pipeline_service import PipelineHandlerBase, PipelineResult

try:
    from pydantic.typing import get_all_type_hints
    from pydantic import BaseModel, parse_raw_as, create_model, ValidationError, Extra
    from pydantic.config import get_config
except ImportError as ex:
    raise ImportError('Please Install the required extra: messageflux[fastmessage]') from ex


class FastMessageException(Exception):
    pass


class DuplicateCallbackException(FastMessageException):
    pass


class NotAllowedParamKindException(FastMessageException):
    pass


class MissingCallbackException(FastMessageException):
    pass


class SpecialDefaultValueException(FastMessageException):
    pass


class InputDeviceName(str):
    """
    a place holder class for input_device name
    """
    pass


class MultipleReturnValues(list):
    """
    a value that indicates that multiple output values should be returned
    """
    pass


@dataclass
class FastMessageOutput:
    """
    a result that contains the output device name to send the value to
    """
    output_device: str
    value: Any


class _DefaultClass(str):
    pass


_DEFAULT = _DefaultClass()
_CALLABLE_TYPE = TypeVar('_CALLABLE_TYPE', bound=Callable[..., Any])


@dataclass
class _ParamInfo:
    annotation: Any
    default: Any


_logger = logging.getLogger(__name__)


class _CallbackWrapper:
    def __init__(self, callback: Callable,
                 input_device: str,
                 output_device: Optional[str] = None):
        self._callback = callback
        self._input_device = input_device
        self._output_device = output_device
        self._special_params: Dict[str, _ParamInfo] = dict()
        self._params: Dict[str, _ParamInfo] = dict()
        type_hints = get_all_type_hints(self._callback)
        extra = Extra.ignore
        for param_name, param in inspect.signature(self._callback).parameters.items():
            if param.kind in (param.POSITIONAL_ONLY, param.VAR_POSITIONAL):
                raise NotAllowedParamKindException(
                    f"param '{param_name}' is of '{param.kind}' kind. this is now allowed")

            if param.kind == param.VAR_KEYWORD:  # there's **kwargs param
                extra = Extra.allow
                continue

            annotation = Any if param.annotation is param.empty else type_hints[param_name]
            default = ... if param.default is param.empty else param.default

            param_info = _ParamInfo(annotation=annotation, default=default)

            if param_info.annotation in (MessageBundle, Message, InputDeviceName,
                                         Optional[MessageBundle], Optional[Message], Optional[InputDeviceName]):
                if param_info.default is not ...:
                    raise SpecialDefaultValueException(
                        f"param '{param_name}' is of special type '{param.annotation.__name__}' "
                        f"but has a default value")
                self._special_params[param_name] = param_info

            else:
                self._params[param_name] = param_info

        self._model = None
        if self._params:
            model_name = self._get_model_name()
            model_params: Dict[str, Any] = {}
            for param_name, param_info in self._params.items():
                model_params[param_name] = (param_info.annotation, param_info.default)
            self._model = create_model(model_name,
                                       __config__=get_config(dict(extra=extra)),
                                       **model_params)

    def _get_model_name(self) -> str:
        return f"model_{self._callback.__name__}_{self._input_device}"

    def __call__(self,
                 input_device: InputDevice,
                 message_bundle: MessageBundle) -> Optional[Union[PipelineResult, List[PipelineResult]]]:
        kwargs: Dict[str, Any] = {}
        for param_name, param_info in self._special_params.items():
            if param_info.annotation is InputDeviceName:
                kwargs[param_name] = input_device.name
            elif param_info.annotation is MessageBundle:
                kwargs[param_name] = message_bundle
            elif param_info.annotation is Message:
                kwargs[param_name] = message_bundle.message

        if self._model:
            model = parse_raw_as(self._model, message_bundle.message.bytes)
            kwargs.update(dict(model))

        callback_return = self._callback(**kwargs)
        if callback_return is None:
            return None

        return self._get_pipeline_results(value=callback_return,
                                          default_output_device=self._output_device)

    def _get_pipeline_results(self, value: Any, default_output_device: Optional[str]) -> List[PipelineResult]:
        results = []
        if isinstance(value, MultipleReturnValues):
            for item in value:
                results.extend(self._get_pipeline_results(value=item,
                                                          default_output_device=default_output_device))
        elif isinstance(value, FastMessageOutput):
            results.extend(self._get_pipeline_results(value=value.value,
                                                      default_output_device=value.output_device))
        else:
            pipeline_result = self._get_single_pipeline_result(value=value,
                                                               output_device=default_output_device)
            if pipeline_result is not None:
                results.append(pipeline_result)

        return results

    def _get_single_pipeline_result(self, value: Any, output_device: Optional[str]) -> Optional[PipelineResult]:
        if output_device is None:
            _logger.warning(f"callback for input device '{self._input_device}' returned value, "
                            f"but is not mapped to output device")
            return None

        if isinstance(value, MessageBundle):
            output_bundle = value
        elif isinstance(value, Message):
            output_bundle = MessageBundle(message=value)
        else:
            json_encoder = getattr(value, '__json_encoder__', BaseModel.__json_encoder__)
            output_data = json.dumps(value, default=json_encoder).encode()
            output_bundle = MessageBundle(message=Message(data=output_data))

        return PipelineResult(output_device_name=output_device, message_bundle=output_bundle)


class FastMessage(PipelineHandlerBase):
    def __init__(self, default_output_device: Optional[str] = None,
                 validation_error_handler: Optional[Callable[
                     [InputDevice, MessageBundle, ValidationError],
                     Optional[Union[PipelineResult, List[PipelineResult]]]]] = None):
        """

        :param default_output_device: an optional default output device to send callaback results to,
        unless mapped otherwise
        :param validation_error_handler: an optional handler that will be called on validation errors,
        in order to give the user a chance to handle them gracefully
        """
        self._default_output_device = default_output_device
        self._validation_error_handler = validation_error_handler
        self._wrappers: Dict[str, _CallbackWrapper] = {}

    @property
    def input_devices(self) -> List[str]:
        """
        returns all the input device names that has callbacks
        """
        return list(self._wrappers.keys())

    def register_validation_error_handler(self,
                                          handler: Callable[
                                              [InputDevice, MessageBundle, ValidationError],
                                              Optional[PipelineResult]]):
        """
        registers optional handler that will be called on validation errors,
        in order to give the user a chance to handle them gracefully
        :param handler: the handler to register
        """
        self._validation_error_handler = handler

    def register_callback(self,
                          callback: Callable,
                          input_device: str,
                          output_device: Optional[str] = _DEFAULT):
        """
        registers a callback to a device

        :param callback: the callback to register
        :param input_device: the input device to register the callback to
        :param output_device:  optional output device to route the return value of the callback to.
        None means no output routing.
        if callback returns None, no routing will be made even if 'output_device' is not None
        """
        if input_device in self._wrappers:
            raise DuplicateCallbackException(f"Can't register more than one callback on device '{input_device}'")

        if output_device is _DEFAULT:
            output_device = self._default_output_device

        self._wrappers[input_device] = _CallbackWrapper(callback=callback,
                                                        input_device=input_device,
                                                        output_device=output_device)

    def map(self,
            input_device: str,
            output_device: Optional[str] = _DEFAULT) -> Callable[[_CALLABLE_TYPE], _CALLABLE_TYPE]:
        """
        this is the decorator method

        :param input_device: the input device to register the decorated method on
        :param output_device: optional output device to route the return value of the callback to.
        if callback returns None, no routing will be made even if 'output_device' is not None
        None means no output routing
        """

        def _register_callback_decorator(callback: _CALLABLE_TYPE) -> _CALLABLE_TYPE:
            self.register_callback(callback=callback,
                                   input_device=input_device,
                                   output_device=output_device)
            return callback

        return _register_callback_decorator

    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[Union[PipelineResult, List[PipelineResult]]]:
        callback_wrapper = self._wrappers.get(input_device.name)
        if callback_wrapper is None:
            raise MissingCallbackException(f"No callback registered for device '{input_device.name}'")
        try:
            return callback_wrapper(input_device=input_device, message_bundle=message_bundle)
        except ValidationError as ve:
            if self._validation_error_handler is None:
                raise

            return self._validation_error_handler(input_device, message_bundle, ve)
