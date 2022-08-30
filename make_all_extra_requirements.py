import os
from pathlib import Path
from typing import List, Set

this_directory = Path(__file__).parent


def load_requirements(path: Path , filename) -> List[str]:
    return (path / filename).read_text().splitlines()


def load_all_extra_requirements(path: Path = this_directory) -> List[str]:
    import re
    extra_requirement_pattern = re.compile(r'^requirements[_\-](?P<name>.+?)\.txt$')
    files = [f for f in path.iterdir() if f.is_file()]
    all_requirements: Set[str] = set()

    for file in files:
        match = extra_requirement_pattern.match(file.name)
        if match:
            all_requirements.update((path / file.name).read_text().splitlines())

    return list(all_requirements)


(this_directory / "all-extra-requirements.txt").write_text(os.linesep.join(load_all_extra_requirements()))
