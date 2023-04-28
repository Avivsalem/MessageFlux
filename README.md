# MessageFlux

[![stars](https://badgen.net/github/stars/Avivsalem/MessageFlux)](https://github.com/Avivsalem/MessageFlux/stargazers)
[![license](https://badgen.net/github/license/Avivsalem/MessageFlux/)](https://github.com/Avivsalem/MessageFlux/blob/main/LICENSE)
[![last commit](https://badgen.net/github/last-commit/Avivsalem/MessageFlux/main)](https://github.com/Avivsalem/MessageFlux/commit/main)
[![tests](https://github.com/AvivSalem/MessageFlux/actions/workflows/tests.yml/badge.svg)](https://github.com/AvivSalem/MessageFlux/actions/workflows/tests.yml?query=branch%3Amain)
[![Documentation Status](https://readthedocs.org/projects/messageflux/badge/?version=latest)](https://messageflux.readthedocs.io/en/latest/?badge=latest)
[![pypi version](https://badgen.net/pypi/v/MessageFlux)](https://pypi.org/project/messageflux/)
[![python compatibility](https://badgen.net/pypi/python/MessageFlux)](https://pypi.org/project/messageflux/)
[![downloads](https://img.shields.io/pypi/dm/messageflux)](https://pypi.org/project/messageflux/)

messageflux is a framework for creating long-running services, that read messages from devices, and handles them.

Devices are composable components - meaning that added functionality can come from wrapping devices
in other devices.

You can find the full documentation [here](https://messageflux.readthedocs.io/)

## Requirements

Python 3.7+

## Installation

```console
$ pip install messageflux
```

### Extra Requirements (Example)

```console
$ pip install messageflux[rabbitmq]
```

## Example

### Create it

* Create a file `main.py` with:

```Python
from messageflux import MessageHandlingService, MessageHandlerBase, InputDevice, ReadResult
from messageflux.iodevices.in_memory_device import InMemoryDeviceManager


class MyMessageHandler(MessageHandlerBase):
    def handle_message(self, input_device: InputDevice, read_result: ReadResult):
        message = read_result.message
        print(f'message bytes: {message.bytes}')  # Do somthing with the message...


input_device_manager = InMemoryDeviceManager()
# write messages to devices here...

my_example_service = MessageHandlingService(message_handler=MyMessageHandler(),
                                            input_device_manager=input_device_manager,
                                            input_device_names=['MY_QUEUE'])
if __name__ == '__main__':
    my_example_service.start()  # this blocks indefinitely (until CTRL-C or sigterm)

```

### Run it

```console
python main.py 
```

### Using Multi Processing for concurrency

```python
from messageflux import MessageHandlingService, MessageHandlerBase, InputDevice, ReadResult
from messageflux.multiprocessing import get_service_runner, ServiceFactory


class MyMessageHandler(MessageHandlerBase):
    def handle_message(self, input_device: InputDevice, read_result: ReadResult):
        message = read_result.message
        print(f'message bytes: {message.bytes}')  # Do somthing with the message...


class MyServiceFactory(ServiceFactory):

    def create_service(self) -> MessageHandlingService:
        """
        we import the devices in 'create_service' so that all the imports will be in the child process.
        this is only a precaution, but recommended
        """
        from messageflux.iodevices.in_memory_device import InMemoryDeviceManager

        input_device_manager = InMemoryDeviceManager()
        # write messages to devices here...

        my_example_service = MessageHandlingService(message_handler=MyMessageHandler(),
                                                    input_device_manager=input_device_manager,
                                                    input_device_names=['MY_QUEUE'])
        return my_example_service


if __name__ == '__main__':  # you must do this in multiprocess running
    service_to_run = get_service_runner(service_factory=MyServiceFactory(),
                                        instance_count=5)  # this will run 5 child processes

    service_to_run.start()  # this starts the child processes and blocks indefinitely (until CTRL-C or sigterm)
```

## Optional Requirements

* ```messageflux[fastmessage]``` - for using the FastMessage pipeline handler 
* ```messageflux[rabbitmq]``` - for using the rabbitmq device
* ```messageflux[objectstorage]``` - for using the s3 device wrappers
* ```messageflux[dev]``` - for running tests and developing for this package
* ```messageflux[all]``` - all extras required for all devices