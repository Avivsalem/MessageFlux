import json
import uuid
from typing import Optional, List

import pytest
from pydantic import BaseModel, ValidationError

from messageflux import ReadResult
from messageflux.fastmessage_handler import FastMessage, MissingCallbackException, DuplicateCallbackException, \
    InputDeviceName
from messageflux.iodevices.base import InputDevice
from messageflux.iodevices.base.common import MessageBundle, Message


class FakeInputDevice(InputDevice):
    def _read_message(self, timeout: Optional[float] = None, with_transaction: bool = True) -> Optional['ReadResult']:
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

    @fm.map(input_device='input1')
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    result = fm.handle_message(FakeInputDevice('input1'), MessageBundle(Message(b'{"x": {"x":1}, "y": "a"}')))
    assert result is not None
    assert result.output_device_name == default_output_device
    json_result = json.loads(result.message_bundle.message.bytes.decode())
    assert json_result['y'] == 'x=1, y=a, z=None'

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(Message(b'{"x": {"x":1}, "y": "a", "z":[1,2]}')))
    assert result is not None
    assert result.output_device_name == default_output_device
    json_result = json.loads(result.message_bundle.message.bytes.decode())
    assert json_result['y'] == 'x=1, y=a, z=[1, 2]'


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

    @fm.map(input_device='input1')
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    with pytest.raises(MissingCallbackException):
        result = fm.handle_message(FakeInputDevice('input2'),
                                   MessageBundle(Message(b'{"x": {"x":1}, "y": "a", "z":[1,2]}')))


def test_validation_error():
    fm: FastMessage = FastMessage()

    @fm.map(input_device='input1')
    def do_something1(x: SomeModel, y: str, z: List[int] = None):
        return SomeOtherModel(y=f'x={x.x}, y={y}, z={z}')

    with pytest.raises(ValidationError):
        result = fm.handle_message(FakeInputDevice('input1'), MessageBundle(Message(b'{"y": "a", "z":[1,2]}')))


def test_special_args():
    default_output_device = str(uuid.uuid4()).replace('-', '')
    fm: FastMessage = FastMessage(default_output_device=default_output_device)

    @fm.map(input_device='input1')
    def do_something1(m: Message, b: MessageBundle, d: InputDeviceName, y: int):
        return SomeOtherModel(y=f'b.test={b.device_headers["test"]}, m.test={m.headers["test"]}, y={y}')

    result = fm.handle_message(FakeInputDevice('input1'),
                               MessageBundle(message=Message(data=b'{"y": 10}',
                                                             headers={'test': 'mtest'}),
                                             device_headers={'test': 'btest'}))
    assert result is not None
    assert result.output_device_name == default_output_device
    json_result = json.loads(result.message_bundle.message.bytes.decode())
    assert json_result['y'] == 'b.test=btest, m.test=mtest, y=10'

# add tests for no output devices, etc...
