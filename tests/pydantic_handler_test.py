import json
from typing import Optional, List

from pydantic import BaseModel, parse_raw_as

from messageflux import InputDevice
from messageflux.iodevices.base import Message
from messageflux.iodevices.base.common import MessageBundle
from messageflux.pydantic_handler import PydanticPipelineHandler, PydanticPipelineResult


class TestInputModel(BaseModel):
    x: int
    y: str


class TestOutputModel(BaseModel):
    a: int
    b: str


class TestPydanticHandler(PydanticPipelineHandler[TestInputModel]):
    def handle_object(self,
                      input_device: InputDevice,
                      pydantic_object: TestInputModel) -> Optional[PydanticPipelineResult]:
        return PydanticPipelineResult(output_device_name='test_device',
                                      pydantic_object=TestOutputModel(a=pydantic_object.x, b=pydantic_object.y))


def test_sanity():
    input_model = TestInputModel(x=3, y='test test test')
    input_bytes = input_model.json().encode()
    handler = TestPydanticHandler()
    result = handler.handle_message(None, MessageBundle(Message(input_bytes)))
    assert result is not None
    output_model: TestOutputModel = TestOutputModel.parse_raw(result.message_bundle.message.bytes)
    assert output_model.a == input_model.x
    assert output_model.b == input_model.y


class TestListPydanticHandler(PydanticPipelineHandler[List[TestInputModel]]):
    def handle_object(self,
                      input_device: InputDevice,
                      pydantic_object: List[TestInputModel]) -> Optional[PydanticPipelineResult]:
        results = []
        for input_model in pydantic_object:
            results.append(TestOutputModel(a=input_model.x, b=input_model.y))
        return PydanticPipelineResult(output_device_name='test_device', pydantic_object=results)


def test_complex_type():
    input_models = [TestInputModel(x=3, y='test test test'), TestInputModel(x=5, y='asdasd')]
    input_bytes = json.dumps(input_models, default=BaseModel.__json_encoder__).encode()
    handler = TestListPydanticHandler()
    result = handler.handle_message(None, MessageBundle(Message(input_bytes)))
    assert result is not None
    output_models: List[TestOutputModel] = parse_raw_as(List[TestOutputModel], result.message_bundle.message.bytes)
    assert len(output_models) == len(input_models)
    for i in range(len(output_models)):
        assert output_models[i].a == input_models[i].x
        assert output_models[i].b == input_models[i].y


class TestMessagePydanticHandler(PydanticPipelineHandler[Message]):
    def handle_object(self,
                      input_device: InputDevice,
                      pydantic_object: Message) -> Optional[PydanticPipelineResult]:
        return PydanticPipelineResult(output_device_name='test_device',
                                      pydantic_object=Message(data=pydantic_object.bytes))


def test_message_type():
    input_bytes = b'1234'
    handler = TestMessagePydanticHandler()
    result = handler.handle_message(None, MessageBundle(Message(input_bytes)))
    assert result is not None
    assert result.message_bundle.message.bytes == input_bytes
