import uuid
from threading import Event

import boto3
from moto import mock_sqs

from messageflux.iodevices.sqs import SQSInputDeviceManager
from messageflux.iodevices.sqs import SQSOutputDeviceManager
from tests.devices.common import sanity_test, rollback_test


@mock_sqs
def test_generic_sanity():
    sqs_resource = boto3.resource('sqs', region_name='us-west-2')
    input_manager = SQSInputDeviceManager(sqs_resource=sqs_resource, max_messages_per_request=4)
    output_manager = SQSOutputDeviceManager(sqs_resource=sqs_resource)
    queue_name = str(uuid.uuid4())
    with input_manager, output_manager:
        q = output_manager.create_queue(queue_name)
        try:
            sanity_test(input_device_manager=input_manager,
                        output_device_manager=output_manager,
                        device_name=queue_name,
                        extra_headers={'test_bytes': b'bytes'})
        finally:
            q.delete()


@mock_sqs
def test_generic_rollback():
    sqs_resource = boto3.resource('sqs', region_name='us-west-2')
    input_manager = SQSInputDeviceManager(sqs_resource=sqs_resource, max_messages_per_request=4)
    output_manager = SQSOutputDeviceManager(sqs_resource=sqs_resource)
    queue_name = str(uuid.uuid4())
    with input_manager, output_manager:
        q = output_manager.create_queue(queue_name)
        try:
            rollback_test(input_device_manager=input_manager,
                          output_device_manager=output_manager,
                          device_name=queue_name)
        finally:
            q.delete()


@mock_sqs
def test_empty_headers():
    sqs_resource = boto3.resource('sqs', region_name='us-west-2')
    input_manager = SQSInputDeviceManager(sqs_resource=sqs_resource)
    output_manager = SQSOutputDeviceManager(sqs_resource=sqs_resource)
    queue_name = str(uuid.uuid4())
    test_message = str(uuid.uuid4())
    with input_manager, output_manager:
        q = output_manager.create_queue(queue_name)
        q.send_message(MessageBody=test_message)
        try:
            id = input_manager.get_input_device(queue_name)
            rr = id.read_message(cancellation_token=Event())
            assert rr.message.bytes.decode() == test_message
        finally:
            q.delete()
