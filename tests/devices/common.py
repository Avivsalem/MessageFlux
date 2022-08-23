import time

import uuid
from typing import Optional

from baseservice.iodevices.base import InputDeviceManager, OutputDeviceManager, Message


def sanity_test(input_device_manager: InputDeviceManager,
                output_device_manager: OutputDeviceManager,
                device_name: Optional[str] = None,
                sleep_between_sends=0.01):
    """
    Common test for all devices.
    """
    device_name = device_name or str(uuid.uuid4())
    test_message_1 = Message(str(uuid.uuid4()).encode(), headers={'test': 'test1'})
    test_message_2 = Message(str(uuid.uuid4()).encode(), headers={'test': 'test2'})

    output_device_manager.connect()
    try:
        output_device = output_device_manager.get_output_device(device_name)
        output_device.send_message(test_message_1.copy())
        time.sleep(sleep_between_sends)
        output_device.send_message(test_message_2.copy())
    finally:
        output_device_manager.disconnect()

    input_device_manager.connect()
    try:
        input_device = input_device_manager.get_input_device(device_name)
        msg, headers, transaction = input_device.read_message()
        assert msg == test_message_1
        transaction.commit()

        msg, headers, transaction = input_device.read_message()
        assert msg == test_message_2
        transaction.commit()
    finally:
        input_device_manager.disconnect()


def rollback_test(input_device_manager: InputDeviceManager,
                  output_device_manager: OutputDeviceManager,
                  device_name: Optional[str] = None,
                  sleep_between_sends=0.01):
    """
    Common test for all devices.
    """
    device_name = device_name or str(uuid.uuid4())
    test_message_1 = Message(str(uuid.uuid4()).encode(), headers={'test': 'test1'})
    test_message_2 = Message(str(uuid.uuid4()).encode(), headers={'test': 'test2'})

    output_device_manager.connect()
    try:
        output_device = output_device_manager.get_output_device(device_name)
        output_device.send_message(test_message_1.copy())
        time.sleep(sleep_between_sends)
        output_device.send_message(test_message_2.copy())
    finally:
        output_device_manager.disconnect()

    input_device_manager.connect()
    try:
        input_device = input_device_manager.get_input_device(device_name)
        msg, headers, transaction1 = input_device.read_message()
        assert msg == test_message_1
        msg, headers, transaction2 = input_device.read_message()
        assert msg == test_message_2
        transaction1.rollback()
        transaction2.rollback()

        msg, headers, transaction = input_device.read_message()
        assert msg == test_message_1
        transaction.commit()

        msg, headers, transaction = input_device.read_message()
        assert msg == test_message_2
        transaction.commit()
    finally:
        input_device_manager.disconnect()
