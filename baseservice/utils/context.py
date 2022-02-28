from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, List, Any
import threading


class _StateProvider(metaclass=ABCMeta):
    @abstractmethod
    def get_state(self) -> Dict[str, List]:
        pass


_global: Dict[str, List] = {}


class _GlobalStateProvider(_StateProvider):
    def get_state(self) -> Dict[str, List]:
        return _global


class _ThreadLocalStateProvider(threading.local, _StateProvider):
    def __init__(self):
        self._state: Dict[str, List] = {}

    def get_state(self) -> Dict[str, List]:
        return self._state


_context_var = ContextVar('__asyncio_context__', default={})


class _AsyncioContextStateProvider(_StateProvider):
    def get_state(self) -> Dict[str, List]:
        return _context_var.get()


class Context:
    def __init__(self, state_provider: _StateProvider):
        self._state_provider = state_provider

    def push(self, key: str, value: Any):
        stack = self._state_provider.get_state().setdefault(key, [])
        stack.append(value)

    def pop(self, key: str, default=None) -> Any:
        state = self._state_provider.get_state()
        stack = state.get(key, [default])
        value = stack.pop()
        if not stack:
            state.pop(key, None)
        return value

    def head(self, key: str, default=None) -> Any:
        stack = self._state_provider.get_state().get(key, [default])
        return stack[-1]

    def get_current_context(self) -> Dict[str, Any]:
        state = self._state_provider.get_state()
        return {key: stack[-1] for key, stack in state.items()}

    @contextmanager
    def start_context(self, **kwargs):
        for key, value in kwargs.items():
            self.push(key, value)
        try:
            yield
        finally:
            for key in kwargs.keys():
                self.pop(key)


global_context = Context(_GlobalStateProvider())
thread_local_context = Context(_ThreadLocalStateProvider())
asyncio_context = Context(_AsyncioContextStateProvider())
