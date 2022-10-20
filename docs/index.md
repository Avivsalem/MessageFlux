# Welcome to Message Flux!

# Specific

::: messageflux.base_service.BaseService
    options:
      members: [__init__, start, stop]


::: messageflux.iodevices.base.input_devices.InputDevice
    options:
      members: [__init__, read_message]


::: messageflux.iodevices.base.output_devices.OutputDevice
    options:
      members: [__init__, send_message]




# Recursive 

::: messageflux.iodevices.file_system
    options:
      show_submodules: true
