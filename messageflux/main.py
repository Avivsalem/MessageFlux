import typer

from messageflux.config import Config
from messageflux.pipeline_service import PipelineHandlerBase, PipelineService
from messageflux.importer import import_from_string


def run(*, pipeline_hanlder: PipelineHandlerBase | str, config_dict: dict | None = None):
    config = Config(config_dict=config_dict)

    if isinstance(pipeline_hanlder, str):
        pipeline_hanlder: PipelineHandlerBase = import_from_string(pipeline_hanlder)

    service = PipelineService(pipeline_handler=pipeline_hanlder,
                              input_device_names=pipeline_hanlder.input_devices, # missing
                              output_device_manager=config.get_output_device_manager(),
                              input_device_manager=config.get_input_device_manager())
    service.start()




if __name__ == '__main__':
    
    run()