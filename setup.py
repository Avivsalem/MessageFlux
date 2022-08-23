from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

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
      install_requires=[],
      )
