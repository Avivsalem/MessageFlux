from setuptools import setup, find_packages

pkg_name = 'baseservice'

setup(name=pkg_name,
      version='0.1a',
      author='Aviv Salem',
      author_email='avivsalem@gmail.com',
      url='https://github.com/Avivsalem/BaseService',
      packages=find_packages(include=[pkg_name, f'{pkg_name}.*']),
      install_requires=[],
      extras_require={
            "dev": [
                  "tox",
                  "pytest",
                  "mypy",
                  "lxml",  # lxml is a mypy requirement in order to generate xml reports.
                  "flake8"
            ]
      }
)
