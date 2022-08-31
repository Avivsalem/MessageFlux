import os

from baseservice.iodevices.base import Message
from baseservice.iodevices.rabbitmq.rabbitmq_input_device import RabbitMQInputDeviceManager
from baseservice.iodevices.rabbitmq.rabbitmq_output_device import RabbitMQOutputDeviceManager
from tests.devices.common import sanity_test, rollback_test

NULL_PASSWORD = "NULL_PASSWORD"
RABBIT_HOST = "rattlesnake.rmq.cloudamqp.com"
RABBIT_PORT = 5672
RABBIT_USERNAME = "uiwayyvm"
RABBIT_VHOST = RABBIT_USERNAME
RABBIT_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", NULL_PASSWORD)
assert RABBIT_PASSWORD != NULL_PASSWORD


def _sanity(in_mgr: RabbitMQInputDeviceManager, out_mgr: RabbitMQOutputDeviceManager):
    res = out_mgr.create_queue('', auto_delete=True)
    queue_name = res.method.queue
    try:
        with out_mgr:
            with in_mgr:
                inqueue = in_mgr.get_input_device(queue_name)
                outqueue = out_mgr.get_output_device(queue_name)
                headers = {
                    'test_header': 'test_value',
                }
                device_headers = {'priority': 3, 'message_id': 'msg_123'}
                outqueue.send_message(Message(b"TEST_DATA", headers), device_headers)

                read_result = inqueue.read_message(1, with_transaction=True)
                assert read_result is not None
                read_result.commit()

                assert read_result.message.bytes == b"TEST_DATA"
                assert headers['test_header'] == 'test_value'
                assert read_result.device_headers['routing_key'] == queue_name
                assert read_result.device_headers['priority'] == 3
                assert read_result.device_headers['message_id'] == 'msg_123'

                assert inqueue.read_message(1) is None

    finally:
        out_mgr.delete_queue(queue_name, only_if_empty=False)


def test_sanity():
    in_mgr = RabbitMQInputDeviceManager(hosts=[RABBIT_HOST],
                                        user=RABBIT_USERNAME,
                                        password=RABBIT_PASSWORD,
                                        port=RABBIT_PORT,
                                        virtual_host=RABBIT_VHOST)

    out_mgr = RabbitMQOutputDeviceManager(hosts=[RABBIT_HOST],
                                          user=RABBIT_USERNAME,
                                          password=RABBIT_PASSWORD,
                                          port=RABBIT_PORT,
                                          virtual_host=RABBIT_VHOST)
    _sanity(in_mgr, out_mgr)


def test_sanity_non_consumer():
    in_mgr = RabbitMQInputDeviceManager(hosts=[RABBIT_HOST],
                                        user=RABBIT_USERNAME,
                                        password=RABBIT_PASSWORD,
                                        port=RABBIT_PORT,
                                        virtual_host=RABBIT_VHOST,
                                        use_consumer=False)

    out_mgr = RabbitMQOutputDeviceManager(hosts=[RABBIT_HOST],
                                          user=RABBIT_USERNAME,
                                          password=RABBIT_PASSWORD,
                                          port=RABBIT_PORT,
                                          virtual_host=RABBIT_VHOST)
    _sanity(in_mgr, out_mgr)


def test_generic_sanity():
    input_manager = RabbitMQInputDeviceManager(hosts=[RABBIT_HOST],
                                               user=RABBIT_USERNAME,
                                               password=RABBIT_PASSWORD,
                                               port=RABBIT_PORT,
                                               virtual_host=RABBIT_VHOST,
                                               prefetch_count=5)
    output_manager = RabbitMQOutputDeviceManager(
        hosts=[RABBIT_HOST],
        user=RABBIT_USERNAME,
        password=RABBIT_PASSWORD,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST)

    res = output_manager.create_queue('', auto_delete=False)
    queue_name = res.method.queue
    try:
        sanity_test(input_device_manager=input_manager,
                    output_device_manager=output_manager,
                    device_name=queue_name)
    finally:
        output_manager.delete_queue(queue_name, only_if_empty=False)


def test_generic_rollback():
    input_manager = RabbitMQInputDeviceManager(hosts=[RABBIT_HOST],
                                               user=RABBIT_USERNAME,
                                               password=RABBIT_PASSWORD,
                                               port=RABBIT_PORT,
                                               virtual_host=RABBIT_VHOST,
                                               prefetch_count=5)
    output_manager = RabbitMQOutputDeviceManager(
        hosts=[RABBIT_HOST],
        user=RABBIT_USERNAME,
        password=RABBIT_PASSWORD,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST)

    res = output_manager.create_queue('', auto_delete=False)
    queue_name = res.method.queue
    try:
        rollback_test(input_device_manager=input_manager,
                      output_device_manager=output_manager,
                      device_name=queue_name)
    finally:
        output_manager.delete_queue(queue_name, only_if_empty=False)
