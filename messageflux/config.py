import logging

from typing import Dict

_logger = logging.getLogger("messageflux.runner")


PIPELINE_SERVICE_CONFIG = {
    'input_device_manager': {
        'class': 'messageflux.iodevices.file_system.FileSystemInputDeviceManager',
        'root_folder': '/tmp'

    },
    'output_device_manager': {
        'class': 'messageflux.iodevices.file_system.FileSystemOutputDeviceManager',
        'root_folder': '/tmp'
    },
}


def _init_from_config(device_manager_config: dict):
    class_string = device_manager_config.pop('class')
    class_parts = class_string.split('.')
    module_name = '.'.join(class_parts[:-1])
    class_name = class_parts[-1]
    module = __import__(module_name, fromlist=[class_name])
    class_ = getattr(module, class_name)
    return class_(**device_manager_config)


class Config:
    def __init__(self, config_dict: Dict[str, dict] | None = None) -> None:
        if config_dict is None:
            _logger.warning("No config provided, using default config")
            config_dict = PIPELINE_SERVICE_CONFIG

        self._config_dict = config_dict
        self._input_device_manager = _init_from_config(config_dict['input_device_manager'])
        self._output_device_manager = _init_from_config(config_dict['output_device_manager'])
    
    def get_input_device_manager(self):
        return self._input_device_manager
    
    def get_output_device_manager(self):
        return self._output_device_manager

        