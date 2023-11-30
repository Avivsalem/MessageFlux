# IO Devices

the heart of this package is the io devices

## Basics

Devices are never created directly. They are created by a ```DeviceManager```

An ```InputDeviceManager``` creates a ```InputDevice``` by name.

An ```OutputDeviceManager``` creates a ```OutputDevice``` by name

### Examples

#### creating RabbitMQ input devices

```python
from messageflux.iodevices.rabbitmq import RabbitMQInputDeviceManager

rabbitmq_device_manager = RabbitMQInputDeviceManager(hosts=['my_rabbit_host'],
                                                     user='USERNAME',
                                                     password='PASSWORD')

rabbitmq_queue = rabbitmq_device_manager.get_input_device('MY_QUEUE_NAME')  # get the input device (rabbitmq queue)

result = rabbitmq_queue.read_message()  # reads a message from queue
# do something with the message...
result.commit()  # tells the device the message was handled successfully.
```

#### creating RabbitMQ output devices

```python
from messageflux.iodevices.rabbitmq import RabbitMQOutputDeviceManager
from messageflux.iodevices.base import Message

rabbitmq_device_manager = RabbitMQOutputDeviceManager(hosts=['my_rabbit_host'],
                                                      user='USERNAME',
                                                      password='PASSWORD')

rabbitmq_queue = rabbitmq_device_manager.get_output_device(
  'MY_QUEUE_NAME')  # get the output device (rabbitmq queue)
message = Message(b'data to send')
rabbitmq_queue.send_message(message)  # sends the message to the queue
```

## Wrapper Devices

Wrapper devices are special devices that wraps other devices and adds some functionality to them, for example:

- ```FailoverOutputDevice``` allows you to define an output device that tries to send to a primary output device,
  and if it failes, it sends the message to the failover device
- ```TransformerInputDevice``` allows you to add some transformation logic to the message that is read from the device.

there are several wrapper devices. read the reference for a complete list

### Examples

#### FailoverOutputDevice

```python
from messageflux.iodevices.rabbitmq import RabbitMQOutputDeviceManager
from messageflux.iodevices.failover_output_device_wrapper import FailoverOutputDeviceManager
from messageflux.iodevices.base import Message

rabbitmq_primary = RabbitMQOutputDeviceManager(hosts=['my_rabbit_primary_host'],
                                               user='USERNAME',
                                               password='PASSWORD')

rabbitmq_failover = RabbitMQOutputDeviceManager(hosts=['my_rabbit_failover_host'],
                                                user='USERNAME',
                                                password='PASSWORD')

device_manager = FailoverOutputDeviceManager(inner_device_manager=rabbitmq_primary,
                                             failover_device_manager=rabbitmq_failover)

rabbitmq_queue = device_manager.get_output_device('MY_QUEUE_NAME')  # this is now a FailoverOutputDevice
message = Message(b'data to send')
rabbitmq_queue.send_message(
  message)  # sends the message to the primary queue. if there's an error, then send to the failover queue
```
