# Config

from typing import Literal
from pydantic import BaseSettings


class RabbitMQConfig(BaseSettings):
    type: Literal["rabbitmq"]
    host: str = "localhost"
    port: int = 5672

    def get_manager(self):
        from messageflux.iodevices.rabbitmq import RabbitMQInputDeviceManager
        return RabbitMQInputDeviceManager(**self.dict())


class FileSystemConfig(BaseSettings):
    type: Literal["filesystem"]
    path: str

    def get_manager(self):
        from messageflux.iodevices.file_system import FileSystemInputDeviceManager
        return FileSystemInputDeviceManager(**self.dict())


class ServiceConfig(BaseSettings):
    input_device_manager: FileSystemConfig | RabbitMQConfig = FileSystemConfig(path="/tmp", type="filesystem")
    output_device_manager: FileSystemConfig | RabbitMQConfig = FileSystemConfig(path="/tmp", type="filesystem")
    input_device_names: list[str] = []

    def init_service_params(self):
        return {
            "input_device_manager": self.input_device_manager.get_manager(),
            "output_device_manager": self.output_device_manager.get_manager(),
            "input_device_names": self.queue_names,
        }
    


# Running the app

from messageflux import FastMessage, MessageHandlingService


def run(app: FastMessage):
    config = ServiceConfig()
    params = config.init_service_params()
    
    service = MessageHandlingService(app, **params)
    service.start()

