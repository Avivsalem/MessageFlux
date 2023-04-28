# PipelineService

The ```PipelineService``` Service is used to Process incoming messages, and transfer the results to an output device.

It gets an instance of ```PipelineHandlerBase``` class, which handles the messages.
It also gets the input and output device managers that will be used to create the devices to receive and send messages

The list of device names to listen on, is also given in the constructor.



## Examples

### Custom Handler

```python
from typing import Optional

from messageflux.iodevices.base import InputDevice, Message, MessageBundle
from messageflux.iodevices.file_system import FileSystemOutputDeviceManager
from messageflux.iodevices.rabbitmq.rabbitmq_input_device import RabbitMQInputDeviceManager
from messageflux.pipeline_service import PipelineService, PipelineHandlerBase, PipelineResult


class MyPipelineHandler(PipelineHandlerBase):  # This is the handler class
    def handle_message(self,
                       input_device: InputDevice,
                       message_bundle: MessageBundle) -> Optional[PipelineResult]:
        """
        Handles a message from an input device. and returns a tuple of the output device name, message and headers to
        send to.

        :param input_device: The input device that sent the message.
        :param message_bundle: The message that was received.

        :return: None if the message should not be sent to any output device.
        PipelineResult if a message should be sent to the output device with the given name.
        """
        message_bytes = message_bundle.message.bytes
        # do something with message
        return PipelineResult(output_device_name='OUTPUT',  # the output device to send the message to
                              message_bundle=Message(b'test'))  # the message to send


input_device_manager = RabbitMQInputDeviceManager(hosts='my.rabbit.host',
                                                  user='username',
                                                  password='password')  # can be some other type of manager

output_device_manager = FileSystemOutputDeviceManager(
    root_folder='/mnt/fsdevices/')  # can be some other type of manager

service = PipelineService(input_device_manager=input_device_manager,
                          input_device_names=['INPUT_QUEUE_1', 'INPUT_QUEUE_2'],  # these are the devices to read from
                          output_device_manager=output_device_manager,
                          pipeline_handler=MyPipelineHandler())

service.start()  # starts the service
```

### Fixed Router

Sometimes, we just need to read from one device (i.e, a RabbitMQ queue) and forward the messages to another device
(i.e, A File-System folder).

In that case, we can use the ```FixedRouterPipelineHandler``` instead of writing a custom handler 

```python
from typing import Optional

from messageflux.iodevices.rabbitmq.rabbitmq_input_device import RabbitMQInputDeviceManager
from messageflux.iodevices.file_system import FileSystemOutputDeviceManager
from messageflux.pipeline_service import PipelineService, FixedRouterPipelineHandler



input_device_manager = RabbitMQInputDeviceManager(hosts='my.rabbit.host',
                                                  user='username',
                                                  password='password') # can be some other type of manager

output_device_manager = FileSystemOutputDeviceManager(root_folder='/mnt/fsdevices/') # can be some other type of manager

# forward all the messages to 'OUTPUT' device on the output manager
fixed_router = FixedRouterPipelineHandler(output_device_name='OUTPUT') 

service = PipelineService(input_device_manager=input_device_manager,
                          input_device_names=['INPUT_QUEUE_1', 'INPUT_QUEUE_2'], # these are the devices to read from
                          output_device_manager=output_device_manager,
                          pipeline_handler=fixed_router)

service.start() # starts the service
```

In this example, the service will read from 'INPUT_QUEUE_1' and 'INPUT_QUEUE_2' queues on the RabbitMQ server, 
and forward all the messages to the 'OUTPUT' queue on the File-System device.
this is a useful usecase for backup for example.


