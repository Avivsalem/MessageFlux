import threading
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict, Any

_context_var = ContextVar('__asyncio_context__', default={})


class Context(metaclass=ABCMeta):
    @abstractmethod
    def get(self, key: str, default=None) -> Any:
        pass

    @abstractmethod
    @contextmanager
    def start_context(self, **kwargs):
        pass

    @abstractmethod
    def get_current_context(self) -> Dict[str, Any]:
        pass


class _GlobalContext(Context):
    def __init__(self):
        self._state = {}

    def _push(self, key: str, value: Any):
        stack = self._state.setdefault(key, [])
        stack.append(value)

    def _pop(self, key: str) -> Any:
        stack = self._state.get(key)
        value = stack.pop()
        if not stack:
            self._state.pop(key, None)
        return value

    def get(self, key: str, default=None) -> Any:
        stack = self._state.get(key, [default])
        return stack[-1]

    def get_current_context(self) -> Dict[str, Any]:
        return {key: stack[-1] for key, stack in self._state.items()}

    @contextmanager
    def start_context(self, **kwargs):
        for key, value in kwargs.items():
            self._push(key, value)
        try:
            yield self
        finally:
            for key in kwargs.keys():
                self._pop(key)


class _ThreadLocalContext(threading.local, _GlobalContext):
    pass


class _AsyncioContext(Context):
    def get(self, key: str, default=None) -> Any:
        return _context_var.get().get(key, default)

    def get_current_context(self) -> Dict[str, Any]:
        return _context_var.get().copy()

    @contextmanager
    def start_context(self, **kwargs):
        new_context = _context_var.get().copy()
        new_context.update(kwargs)
        token = _context_var.set(new_context)
        try:
            yield self
        finally:
            _context_var.reset(token)


global_context: Context = _GlobalContext()
thread_local_context: Context = _ThreadLocalContext()
asyncio_context: Context = _AsyncioContext()
