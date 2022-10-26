# Welcome to MessageFlux

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
