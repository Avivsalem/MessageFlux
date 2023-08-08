import typer
import json

from typing import Optional

from messageflux.main import run


def run_pipeline(pipeline_hanlder: str, config_str: Optional[str] = None):
    if config_str:
        config_dict = json.loads(config_str)
    else:
        config_dict = None

    run(pipeline_hanlder=pipeline_hanlder, config_dict=config_dict)


if __name__ == '__main__':
    typer.run(run_pipeline)