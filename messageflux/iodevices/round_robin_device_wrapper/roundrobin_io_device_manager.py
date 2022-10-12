from random import shuffle
from typing import List

from messageflux.iodevices.base import InputDeviceManager, OutputDeviceManager
from messageflux.iodevices.collection_device_wrapper import CollectionInputDeviceManager, CollectionOutputDeviceManager
from messageflux.utils import StatefulListIterator


def _create_round_robin_collection(devices: List):
    shuffle(devices)
    return StatefulListIterator(devices)


class RoundRobinInputDeviceManager(CollectionInputDeviceManager):
    """
    This class is used to create RoundRobin InputDevices
    """

    def __init__(self, inner_managers: List[InputDeviceManager]):
        """
        This class is used to create RoundRobin IODevices

        :param inner_managers: the actual InputDeviceManager instances to generate devices from
        """

        super(RoundRobinInputDeviceManager, self).__init__(inner_managers=inner_managers,
                                                           collection_maker=_create_round_robin_collection)


class RoundRobinOutputDeviceManager(CollectionOutputDeviceManager):
    """
    This class is used to create RoundRobin IODevices
    """

    def __init__(self, inner_managers: List[OutputDeviceManager]):
        """
        This class is used to create RoundRobin IODevices

        :param inner_managers: the actual OutputDeviceManager instances to generate devices from
        """
        super(RoundRobinOutputDeviceManager, self).__init__(inner_managers=inner_managers,
                                                            collection_maker=_create_round_robin_collection)
