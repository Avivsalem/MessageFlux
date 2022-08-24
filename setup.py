from pathlib import Path
from setuptools import setup, find_packages

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def load_requirements(path: Path = this_directory, filename="requirements.txt"):
    return (path / filename).read_text().splitlines()


def load_extra_requirements(path: Path = this_directory):
    import re
    extra_requirement_pattern = re.compile(r'^requirements[_\-](?P<name>.+?)\.txt$')
    files = [f for f in path.iterdir() if f.is_file()]
    extras_dict = {}
    for file in files:
        match = extra_requirement_pattern.match(file.name)
        if match:
            extras_dict[match['name']] = load_requirements(path, file.name)

    if extras_dict:
        all_reqs = set()
        for req in extras_dict.values():
            all_reqs.update(req)
        extras_dict['all'] = list(all_reqs)

    return extras_dict


pkg_name = 'baseservice'

setup(name=pkg_name,
      version='0.1a',
      description="a package for creating event driven services",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Aviv Salem',
      author_email='avivsalem@gmail.com',
      url='https://github.com/Avivsalem/BaseService',
      packages=find_packages(include=[pkg_name, f'{pkg_name}.*']),
      python_requires='>=3.7',
      classifiers=[
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
      ],
      include_package_data=True,
      install_requires=load_requirements(),
      extra_require=load_extra_requirements()
      )
