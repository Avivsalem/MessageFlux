import threading
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
# noinspection PyPackageRequirements
from contextvars import ContextVar
from typing import Dict, Any

_context_var: ContextVar = ContextVar('__asyncio_context__', default={})


class Context(metaclass=ABCMeta):
    """
    a stack like context object. allows to store some key-value data and access it
    """

    @abstractmethod
    def get(self, key: str, default=None) -> Any:
        """
        gets the current value for 'key' in the context. returns default if key does not exist

        :param key: the key to get
        :param default: the default value to return if key does not exist
        :return: the current value for 'key' in the context. returns 'default' if key does not exist
        """
        pass

    @abstractmethod
    @contextmanager
    def start_context(self, **kwargs):
        """
        starts the context manager

        :param **kwargs: kwargs to push to the context
        """
        pass

    @abstractmethod
    def get_current_context(self) -> Dict[str, Any]:
        """
        gets the complete current context
        """
        pass


class _GlobalContext(Context):
    """
    this class is a global context. same for everyone
    """

    def __init__(self):
        self._state = {}

    def _push(self, key: str, value: Any):
        stack = self._state.setdefault(key, [])
        stack.append(value)

    def _pop(self, key: str) -> Any:
        stack = self._state.get(key)
        assert stack is not None
        value = stack.pop()
        if not stack:
            self._state.pop(key, None)
        return value

    def get(self, key: str, default=None) -> Any:
        """
        gets the current value for 'key' in the context. returns default if key does not exist

        :param key: the key to get
        :param default: the default value to return if key does not exist
        :return: the current value for 'key' in the context. returns 'default' if key does not exist
        """
        stack = self._state.get(key, [default])
        return stack[-1]

    def get_current_context(self) -> Dict[str, Any]:
        """
        gets the complete current context
        """
        return {key: stack[-1] for key, stack in self._state.items()}

    @contextmanager
    def start_context(self, **kwargs):
        """
        starts the context manager

        :param kwargs: kwargs to push to the context
        """
        for key, value in kwargs.items():
            self._push(key, value)
        try:
            yield self
        finally:
            for key in kwargs.keys():
                self._pop(key)


class _ThreadLocalContext(threading.local, _GlobalContext):
    """
    this class is a thread local context. each thread sees a different context stack
    """

    pass


class _AsyncioContext(Context):
    """
    this context is for asyncio. each asyncio chain sees a different context stack
    """

    def get(self, key: str, default=None) -> Any:
        """
        gets the current value for 'key' in the context. returns default if key does not exist

        :param key: the key to get
        :param default: the default value to return if key does not exist
        :return: the current value for 'key' in the context. returns 'default' if key does not exist
        """
        return _context_var.get().get(key, default)

    def get_current_context(self) -> Dict[str, Any]:
        """
        gets the complete current context
        """
        return _context_var.get().copy()

    @contextmanager
    def start_context(self, **kwargs):
        """
        starts the context manager

        :param kwargs: kwargs to push to the context
        """

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
