import os

import pytest

from baseservice.iodevices.base import Message, InputTransactionScope
from baseservice.iodevices.rabbitmq.fs_poison_counter import FileSystemPoisonCounter
from baseservice.iodevices.rabbitmq.rabbitmq_input_device import RabbitMQInputDeviceManager
from baseservice.iodevices.rabbitmq.rabbitmq_output_device import RabbitMQOutputDeviceManager, LengthValidationException
from baseservice.iodevices.rabbitmq.rabbitmq_poison_counting_input_device import PoisonCounterBase, \
    RabbitMQPoisonCountingInputDeviceManager
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


def test_message_and_headers_size():
    mgr = RabbitMQOutputDeviceManager(hosts=[RABBIT_HOST],
                                      user=RABBIT_USERNAME,
                                      password=RABBIT_PASSWORD,
                                      port=RABBIT_PORT,
                                      virtual_host=RABBIT_VHOST,
                                      max_message_length=5,
                                      max_header_name_length=100,
                                      max_header_value_length=150)

    res = mgr.create_queue('', auto_delete=True, exclusive=True)
    queue_name = res.method.queue
    with mgr:
        outqueue = mgr.get_output_device(queue_name)
        headers = {'test_header': 'test_value'}
        with pytest.raises(LengthValidationException):
            outqueue.send_message(Message(b"TEST_DATA", headers=headers))
        with pytest.raises(LengthValidationException):
            outqueue.send_message(Message(b"TEST", headers={"1" * 101: "1" * 151}))
        with pytest.raises(LengthValidationException):
            outqueue.send_message(Message(b"TEST", headers={"1" * 101: "1"}))
        with pytest.raises(LengthValidationException):
            outqueue.send_message(Message(b"TEST", headers={"1": "1" * 151}))
        outqueue.send_message(Message(b"TEST", headers={"1" * 100: "1" * 150}))
        outqueue.send_message(Message(b"TEST", headers={"1": "1"}))


def _no_poison_rollback(poison_counter: PoisonCounterBase):
    in_mgr = RabbitMQPoisonCountingInputDeviceManager(hosts=[RABBIT_HOST],
                                                      user=RABBIT_USERNAME,
                                                      password=RABBIT_PASSWORD,
                                                      port=RABBIT_PORT,
                                                      virtual_host=RABBIT_VHOST,
                                                      prefetch_count=5,
                                                      max_poison_count=2,
                                                      poison_counter=poison_counter)

    out_mgr = RabbitMQOutputDeviceManager(hosts=[RABBIT_HOST],
                                          user=RABBIT_USERNAME,
                                          password=RABBIT_PASSWORD,
                                          port=RABBIT_PORT,
                                          virtual_host=RABBIT_VHOST)

    res = out_mgr.create_queue('', auto_delete=True)
    queue_name = res.method.queue
    with out_mgr:
        outqueue = out_mgr.get_output_device(queue_name)
        outqueue.send_message(Message(data=b"TEST_DATA", headers={'test_header': 'test_value'}))
        with in_mgr:
            inqueue = in_mgr.get_input_device(queue_name)
            with InputTransactionScope(inqueue) as transaction_scope:
                read_result = transaction_scope.read_message(5)
                assert read_result is not None
                assert read_result.message.bytes == b"TEST_DATA"
                assert read_result.message.headers['test_header'] == 'test_value'
                transaction_scope.rollback()

            with InputTransactionScope(inqueue) as transaction_scope:
                read_result = transaction_scope.read_message(5)
                assert read_result is not None
                assert read_result.message.bytes == b"TEST_DATA"
                assert read_result.message.headers['test_header'] == 'test_value'
                transaction_scope.rollback()

            with InputTransactionScope(inqueue) as transaction_scope:
                read_result = transaction_scope.read_message(1)
                assert read_result is None
                read_result = transaction_scope.read_message(1)
                assert read_result is None


def test_no_poison_rollback():
    class InMemoryPoisonCounter(PoisonCounterBase):
        def __init__(self):
            self.poison_counter = dict()

        def increment_and_return_counter(self, message_id: str) -> int:
            old_value = self.poison_counter.get(message_id, 0)
            new_value = old_value + 1
            self.poison_counter[message_id] = new_value
            return new_value

        def delete_counter(self, message_id: str):
            self.poison_counter.pop(message_id, None)

    poison_counter = InMemoryPoisonCounter()
    _no_poison_rollback(poison_counter)
    assert len(poison_counter.poison_counter) == 0


def test_no_poison_rollback_fs_counter(tmpdir):
    tmpdir = str(tmpdir)
    poison_counter = FileSystemPoisonCounter(tmpdir)
    _no_poison_rollback(poison_counter)
    assert len(os.listdir(tmpdir)) == 0
