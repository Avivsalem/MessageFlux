import json
import threading
import uuid
from typing import Optional, List

import pytest
from pydantic import BaseModel, ValidationError

from messageflux import ReadResult
from messageflux.fastmessage_handler import FastMessage, MissingCallbackException, DuplicateCallbackException, \
    InputDeviceName, SpecialDefaultValueException, NotAllowedParamKindException, MultipleReturnValues, FastMessageOutput
from messageflux.iodevices.base import InputDevice
from messageflux.iodevices.base.common import MessageBundle, Message
from messageflux.pipeline_service import PipelineResult


class FakeInputDevice(InputDevice):
    def _read_message(self,
                      cancellation_token: threading.Event,
                      timeout: Optional[float] = None,
                      with_transaction: bool = True) -> Optional['ReadResult']:
        return None

    def __init__(self, name: str):
        super().__init__(None, name)


class SomeModel(BaseModel):
    x: int


class SomeOtherModel(BaseModel):
    y: str


def test_sanity():
    default_output_device = str(uuid.uuid4()).replace('-', '')
    fm: FastMessage = FastMessage(default_output_device=default_output_device)

    @fm.map()
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    result = fm.handle_message(FakeInputDevice('do_something1'), MessageBundle(Message(b'{"x": {"x":1}, "y": "a", "F":3}')))
    assert result is not None
    result = result[0]
    assert result.output_device_name == default_output_device
    json_result = json.loads(result.message_bundle.message.bytes.decode())
    assert json_result['y'] == 'x=1, y=a, z=None'

    result = fm.handle_message(FakeInputDevice('do_something1'),
                               MessageBundle(Message(b'{"x": {"x":1}, "y": "a", "z":[1,2]}')))
    assert result is not None
    result = result[0]
    assert result.output_device_name == default_output_device
    json_result = json.loads(result.message_bundle.message.bytes.decode())
    assert json_result['y'] == 'x=1, y=a, z=[1, 2]'


def test_root_model():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1', output_device='output')
    def do_something1(__root__: SomeModel):
        return SomeOtherModel(y=f'x={__root__.x}')

    result = fm.handle_message(FakeInputDevice('input1'), MessageBundle(Message(b'{"x": 1}')))
    assert result is not None
    json_result = json.loads(result[0].message_bundle.message.bytes.decode())
    assert json_result['y'] == 'x=1'


def test_kwargs():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1', output_device='output')
    def do_something1(x: int, **kwargs):
        return kwargs

    result = fm.handle_message(FakeInputDevice('input1'), MessageBundle(Message(b'{"x": 1, "y":"hello", "z":3}')))
    assert result is not None
    json_result = json.loads(result[0].message_bundle.message.bytes.decode())
    assert json_result['y'] == 'hello'
    assert json_result['z'] == 3


def test_duplicate_register():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1')
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    with pytest.raises(DuplicateCallbackException):
        @fm.map(input_device='input1')
        def do_something2(x: SomeModel, y: str, z: List[int] = None):
            pass


def test_missing_callback():
    fm: FastMessage = FastMessage()

    @fm.map()
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    with pytest.raises(MissingCallbackException):
        _ = fm.handle_message(FakeInputDevice('input2'),
                              MessageBundle(Message(b'{"x": {"x":1}, "y": "a", "z":[1,2]}')))


def test_validation_error():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1')
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    with pytest.raises(ValidationError):
        _ = fm.handle_message(FakeInputDevice('input1'), MessageBundle(Message(b'{"y": "a", "z":[1,2]}')))


def test_handled_validation_error():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1', output_device='output')
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    def handle_error(i: InputDevice, m: MessageBundle, e: ValidationError) -> PipelineResult:
        return PipelineResult('ERROR', m)

    fm.register_validation_error_handler(handle_error)

    input_bundle = MessageBundle(Message(b'{"y": "a", "z":[1,2]}'))
    result = fm.handle_message(FakeInputDevice('input1'), input_bundle)

    assert result is not None
    assert result.output_device_name == "ERROR"
    assert result.message_bundle == input_bundle


def test_default_value_on_special_param():
    fm: FastMessage = FastMessage()

    with pytest.raises(SpecialDefaultValueException):
        @fm.map(input_device='input1')
        def do_something1(x: Message = None):
            pass


def test_var_args():
    fm: FastMessage = FastMessage()
    with pytest.raises(NotAllowedParamKindException):
        @fm.map(input_device='input1')
        def do_something1(x: int, *args):
            pass


def test_special_args():
    default_output_device = str(uuid.uuid4()).replace('-', '')
    fm: FastMessage = FastMessage(default_output_device=default_output_device)

    @fm.map(input_device='input1')
    def do_something1(m: Message, b: MessageBundle, d: InputDeviceName, y: int):
        return SomeOtherModel(y=f'd={d}, b.test={b.device_headers["test"]}, m.test={m.headers["test"]}, y={y}')

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(message=Message(data=b'{"y": 10}',
                                                             headers={'test': 'mtest'}),
                                             device_headers={'test': 'btest'}))
    assert result is not None
    result = result[0]
    assert result.output_device_name == default_output_device
    json_result = json.loads(result.message_bundle.message.bytes.decode())
    assert json_result['y'] == 'd=input1, b.test=btest, m.test=mtest, y=10'


def test_list_single_result():
    default_output_device = str(uuid.uuid4()).replace('-', '')
    fm: FastMessage = FastMessage(default_output_device=default_output_device)

    @fm.map(input_device='input1')
    def do_something1(m: Message, b: MessageBundle, d: InputDeviceName, y: int):
        return [1, 2, 3]

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(message=Message(data=b'{"y": 10}',
                                                             headers={'test': 'mtest'}),
                                             device_headers={'test': 'btest'}))
    assert result is not None
    assert isinstance(result, List)
    assert len(result) == 1
    assert result[0].message_bundle.message.bytes == b'[1, 2, 3]'


def test_list_multiple_result():
    default_output_device = str(uuid.uuid4()).replace('-', '')
    fm: FastMessage = FastMessage(default_output_device=default_output_device)

    @fm.map(input_device='input1')
    def do_something1(m: Message, b: MessageBundle, d: InputDeviceName, y: int):
        return MultipleReturnValues([1, 2, 3])

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(message=Message(data=b'{"y": 10}',
                                                             headers={'test': 'mtest'}),
                                             device_headers={'test': 'btest'}))
    assert result is not None
    assert isinstance(result, List)
    assert len(result) == 3
    assert result[0].message_bundle.message.bytes == b'1'
    assert result[1].message_bundle.message.bytes == b'2'
    assert result[2].message_bundle.message.bytes == b'3'


def test_custom_output_device_result():
    default_output_device = str(uuid.uuid4()).replace('-', '')
    fm: FastMessage = FastMessage(default_output_device=default_output_device)

    @fm.map(input_device='input1')
    def do_something1(m: Message, b: MessageBundle, d: InputDeviceName, y: int):
        return MultipleReturnValues([
            FastMessageOutput(value=1, output_device='test1'),
            FastMessageOutput(value=2, output_device='test2'),
            FastMessageOutput(value=3, output_device='test3'),
            4
        ])

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(message=Message(data=b'{"y": 10}',
                                                             headers={'test': 'mtest'}),
                                             device_headers={'test': 'btest'}))
    assert result is not None
    assert isinstance(result, List)
    assert len(result) == 4
    assert result[0].message_bundle.message.bytes == b'1'
    assert result[0].output_device_name == "test1"
    assert result[1].message_bundle.message.bytes == b'2'
    assert result[1].output_device_name == "test2"
    assert result[2].message_bundle.message.bytes == b'3'
    assert result[2].output_device_name == "test3"
    assert result[3].message_bundle.message.bytes == b'4'
    assert result[3].output_device_name == default_output_device


def test_no_output_device():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1')
    def do_something1(m: Message, b: MessageBundle, d: InputDeviceName, y: int):
        return MultipleReturnValues([
            FastMessageOutput(value=1, output_device='test1'),
            FastMessageOutput(value=2, output_device='test2'),
            FastMessageOutput(value=3, output_device='test3'),
            4
        ])

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(message=Message(data=b'{"y": 10}',
                                                             headers={'test': 'mtest'}),
                                             device_headers={'test': 'btest'}))
    assert result is not None
    assert isinstance(result, List)
    assert len(result) == 3
    assert result[0].message_bundle.message.bytes == b'1'
    assert result[0].output_device_name == "test1"
    assert result[1].message_bundle.message.bytes == b'2'
    assert result[1].output_device_name == "test2"
    assert result[2].message_bundle.message.bytes == b'3'
    assert result[2].output_device_name == "test3"

# add tests for no output devices, etc...
