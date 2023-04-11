import concurrent.futures
import logging

from typing import Optional, Callable, TypeVar, List, get_args
from pydantic import BaseModel, ValidationError

from messageflux.iodevices.base import InputDevice, OutputDevice, Message


_logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Callable[[BaseModel], Optional[BaseModel]])

class FastMessageWorker:


    def __init__(
        self,
        name: str = "FastMessage",
        *,
        input_device: Optional[InputDevice] = None,
        output_device: Optional[OutputDevice] = None,
        callback: Optional[Callable[[BaseModel], Optional[BaseModel]]] = None,
        read_timeout: Optional[int] = None,
    ) -> None:
        self._name = name
        self._callback = callback
        self.input_device = input_device
        self.output_device = output_device
        self.read_timeout = read_timeout

    def run_message_processing(self):
        assert self._callback is not None, "Callback is not set"
        assert self.input_device is not None, "Subscriber is not set"
        assert self.output_device is not None, "Publisher is not set"

        _logger.debug("Trying to read messages")

        message = self.input_device.read_message(timeout=self.read_timeout, with_transaction=True)

        _logger.debug("Successfully read messages")

        if message is None:
            _logger.debug("No messages to process")
            return
        

        input_model: BaseModel = self._get_message_type(self._callback)

        try:
            _logger.debug("Parsing messages into input model")

            message_model_object = input_model.parse_raw(message.message.bytes)
            _logger.debug("Successfully parsed messages into input model")

        except ValidationError as ex:
            _logger.exception(
                "Failed to parse messages", extra={"exception_details": repr(ex)}
            )
            message.rollback()
            return

        try:
            _logger.debug("Processing messages")

            result = self._callback(message_model_object)
           
            _logger.debug("Successfully processed messages")

        except Exception:
            _logger.exception("Failed to process messages")
            message.rollback()
            return

        try:
            _logger.debug("Publishing results")

            output_message = Message(data=result.json().encode("utf-8"))
            self.output_device.send_message(output_message)

            _logger.debug("Successfully published results")

            message.commit()

        except Exception:
            _logger.exception("Failed to publish results")
            message.rollback()
            return

    def _get_message_type(self, callback: Callable) -> BaseModel:
        base_model_param = [k for k, v in callback.__annotations__.items() if isinstance(v, BaseModel)]

        assert len(base_model_param) == 1, "Callback must have exactly one BaseModel parameter"

        return callback.__annotations__["message"]
    
    def validate_callback(self, callback: Callable) -> None:
        base_model_param = [k for k, v in callback.__annotations__.items() if isinstance(v, BaseModel)]

        assert len(base_model_param) == 1, "Callback must have exactly one BaseModel parameter"

    def register_callback(self, callback: T) -> T:
        self.validate_callback(callback)

        self._callback = callback

        return callback

    def set_input_device(self, input_device: InputDevice) -> None:
        self.input_device = input_device

    def set_output_device(self, output_device: OutputDevice) -> None:
        self.output_device = output_device

    def run(self):
        _logger.info("Starting worker: %s", self._name)

        try:
            while True:
                self.run_message_processing()
        except KeyboardInterrupt:
            _logger.info("Exiting worker")
        except Exception:
            _logger.exception("Unhandled exception in worker")
            raise



# Scrap

class InputMessageModel(BaseModel):
    a: str
    b: int

class OutputMessageModel(BaseModel):
    c: str
    d: int


worker = FastMessageWorker(name="MockWorker")


@worker.register_callback
def process_message(message: InputMessageModel) -> OutputMessageModel:
    return OutputMessageModel(c=message.a, d=message.b)


# In the runtime section of the service

config = {
    "input_device": {
        "type": "kafka",
    },
    "output_device": {
        "type": "kafka",
    }
}

worker.set_input_device(InputDevice.from_config(config["input_device"]))
worker.set_output_device(OutputDevice.from_config(config["output_device"]))
worker.run()
