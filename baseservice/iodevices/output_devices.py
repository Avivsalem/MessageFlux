from abc import ABCMeta, abstractmethod
from typing import BinaryIO, Dict, Any


class OutputDevice(metaclass=ABCMeta):
    def __init__(self, manager: 'OutputDeviceManager', name: str):
        self._manager = manager
        self._name = name

    @abstractmethod
    def send_stream(self, stream: BinaryIO, headers: Dict[str, Any]):
        pass


class OutputDeviceManager(metaclass=ABCMeta):
    @abstractmethod
    def get_output_device(self, name: str) -> OutputDevice:
        pass
