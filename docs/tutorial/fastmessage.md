# Using FastMessage

FastMessage is an easy way to create a ```PipelineHandlerBase``` and put it in a ```PipelineService```

## Limitations

TBD

## Examples

### Creating a FastMessage

```python
from messageflux.fastmessage_handler import FastMessage
from pydantic import BaseModel
fm = FastMessage()


@fm.map(input_device='some_queue')
def do_something(x: int, y: str):
    pass  # do something with x and y

class SomeModel(BaseModel):
    x:int
    y:str
    
@fm.map(input_device='some_other_queue')
def do_something_else(m:SomeModel, a:int):
    pass # do somthing with m and a
```

### Creating the service runner

```python
from pydantic import BaseModel

from messageflux.fastmessage_handler import FastMessage
from messageflux.iodevices.rabbitmq.rabbitmq_input_device import RabbitMQInputDeviceManager
from messageflux.iodevices.rabbitmq.rabbitmq_output_device import RabbitMQOutputDeviceManager
from messageflux.pipeline_service import PipelineService

fm = FastMessage()


@fm.map(input_device='some_queue')
def do_something(x: int, y: str):
    pass  # do something with x and y


class SomeModel(BaseModel):
    x: int
    y: str


@fm.map(input_device='some_other_queue', output_device='some_output_queue')
def do_something_else(m: SomeModel, a: int):
    return "some_value"  # do somthing with m and a


input_device_manager = RabbitMQInputDeviceManager(hosts='my.rabbit.host',
                                                  user='username',
                                                  password='password')

output_device_manager = RabbitMQOutputDeviceManager(hosts='my.rabbit.host',
                                                    user='username',
                                                    password='password')

service = PipelineService(input_device_manager=input_device_manager,
                          input_device_names=fm.input_devices,
                          output_device_manager=output_device_manager,
                          pipeline_handler=fm)

service.start()
```
