from baseservice.iodevices.in_memory_device import InMemoryDeviceManager
from .common import sanity_test, rollback_test


def test_sanity():
    in_memory_device_manager = InMemoryDeviceManager()
    sanity_test(in_memory_device_manager, in_memory_device_manager)


def test_rollback():
    in_memory_device_manager = InMemoryDeviceManager()
    rollback_test(in_memory_device_manager, in_memory_device_manager)
