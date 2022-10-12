import os
from pathlib import Path
from typing import List, Set


def load_requirements(path: Path, filename) -> List[str]:
    return (path / filename).read_text().splitlines()


def load_all_extra_requirements(path: Path) -> List[str]:
    import re
    extra_requirement_pattern = re.compile(r'^requirements[_\-](?P<name>.+?)\.txt$')
    files = [f for f in path.iterdir() if f.is_file()]
    all_requirements: Set[str] = set()

    for file in files:
        match = extra_requirement_pattern.match(file.name)
        if match:
            all_requirements.update((path / file.name).read_text().splitlines())

    return sorted([line.strip() for line in all_requirements])


def generate_all_requirements(path: Path, output_filename: str = "requirements-all.txt"):
    all_extras = os.linesep.join(load_all_extra_requirements(path))
    output_file = (path/output_filename)

    print(f'---------------> Writing the requirements to {output_file}:')
    print(all_extras)
    output_file.write_text(all_extras)
    print()
    print('---------------> Done!!!')
