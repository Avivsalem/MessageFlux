from abc import ABCMeta

from baseservice.iodevices.base import InputDeviceManager, OutputDeviceManager, InputDevice, OutputDevice


class InputOutputDeviceManager(InputDeviceManager, OutputDeviceManager, metaclass=ABCMeta):
    pass


class InputOutputDevice(InputDevice, OutputDevice, metaclass=ABCMeta):
    _manager: InputOutputDeviceManager

    def __init__(self, manager: InputOutputDeviceManager, name: str):
        InputDevice.__init__(self, manager, name)
        OutputDevice.__init__(self, manager, name)
