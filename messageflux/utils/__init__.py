import datetime
import os
import threading
from typing import Collection, TypeVar, List, Iterator, Generic, Callable, Optional, Any, Union

from itertools import cycle, islice
from time import perf_counter

EllipsisType = type(...)


class KwargsException(Exception):
    """
    the basic exception type we use.
    adds the ability to add KWARGS to exception
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.kwargs = kwargs


class AggregatedException(KwargsException):
    """
    an exception that can hold multiple inner exceptions
    """

    def __init__(self, *args, inner_exceptions: Optional[List[Exception]] = None, **kwargs):
        super(AggregatedException, self).__init__(*args, **kwargs)
        self.inner_exceptions = inner_exceptions


TItemType = TypeVar('TItemType')


class StatefulListIterator(Collection[TItemType]):
    """
    An list, that can be iterated from the last stopped location, in a circular fashion.
    every iteration starts from the last position iterated position, and iterates over all the list
    """

    def __init__(self, items: List[TItemType]) -> None:
        self._items = items
        self._item_count = len(items)
        self._item_cycle = cycle(self._items)

    def __contains__(self, __x: object) -> bool:
        return self._items.__contains__(__x)

    def __len__(self) -> int:
        return self._item_count

    def __iter__(self) -> Iterator[TItemType]:
        return islice(self._item_cycle, self._item_count)


TEventType = TypeVar('TEventType')


class ObservableEvent(Generic[TEventType]):
    """
    an event that can be subscribed to and fired.
    """

    def __init__(self) -> None:
        self._handlers: List[Callable[[TEventType], None]] = []

    def subscribe(self, handler: Callable[[TEventType], None]) -> None:
        """
        subscribes a callback to the event

        :param handler: the callback to subscribe
        """
        self._handlers.append(handler)

    def unsubscribe(self, handler: Callable[[TEventType], None]) -> bool:
        """
        unsubscribes a callback to the event

        :param handler: the callback to unsubscribe
        :return: True if the callback existed and unsubscribed. False if the callback didn't exist
        """
        if handler in self._handlers:
            self._handlers.remove(handler)
            return True
        return False

    def fire(self, event: TEventType, continue_after_failure: bool = True) -> bool:
        """
        fires the event

        :param event: the event to fire
        :param continue_after_failure: True means to call all the callbacks even if one fails.
        False means to stop if one of the callbacks fails
        :return: True if all the callbacks succeeded, False otherwise
        """
        ret_val = True
        for handler in self._handlers:
            try:
                handler(event)
            except Exception:
                ret_val = False
                if not continue_after_failure:
                    raise
        return ret_val


TValueType = TypeVar('TValueType')


class ThreadLocalValue(threading.local, Generic[TValueType]):
    """
    this class holds a single, thread-local value

    x = ThreadLocalValue(5)
    x.value # 5
    x.value = 7 # changes only for this thread
    """

    def __init__(self, init_value: Optional[TValueType] = None):
        self.value = init_value


class ThreadLocalMember(Generic[TValueType]):
    """
    this is a descriptor, for making thread local members for class:

    class MyClass:
        x = ThreadLocalMember()
        y = ThreadLocalMember(1)
        z = ThreadLocalMember()
        w = ThreadLocalMember(1)
        def __init__():
            self.x = 5
            self.w = 2

    a = MyClass():
    a.x # 5
    a.x = 7 # changes only for this thread
    a.y # 1 - the default init value for this
    a.z # raises AttributeError (z was not set on class). once it's set, then value is the init value for each thread
    a.w # 2 for this thread, 1 for any other thread (because of the default init value)
    """

    def __init__(self, init_value: Union[TValueType, EllipsisType] = ...):  # type: ignore
        self._init_value = init_value

    def __set_name__(self, owner, name):
        self._public_name = name
        self._private_name = f'_thread_local_{name}'

    def _get_thread_local_value(self, instance, init_value):
        try:
            lv = getattr(instance, self._private_name)
        except AttributeError:
            lv = ThreadLocalValue(init_value)
            setattr(instance, self._private_name, lv)
        return lv

    def __get__(self, instance, owner) -> Optional[TValueType]:
        if self._init_value is not ...:
            lv = self._get_thread_local_value(instance, self._init_value)
        else:
            if not hasattr(instance, self._private_name):
                raise AttributeError(f"'{owner.__name__}' object has no attribute '{self._public_name}'")
            lv = getattr(instance, self._private_name)

        return lv.value

    def __set__(self, instance, value):
        if instance is None:
            return
        if self._init_value is ...:
            init_value = value
        else:
            init_value = self._init_value
        lv = self._get_thread_local_value(instance, init_value)
        lv.value = value


def get_random_id() -> str:
    """
    generates a random 16 bytes hex string
    """
    return os.urandom(16).hex()


class ContextDidNotEndException(KwargsException):
    """
    this exception is raised when elapsed seconds is called on a context that was not run
    """
    pass


class TimerContext:
    """
    a context object that records how much time did the context last
    """

    def __init__(self):
        self._start: float = perf_counter()
        self._elapsed: Optional[float] = None

    @property
    def elapsed_seconds(self) -> float:
        """
        returns the elapsed time for this context (between __enter__ and __exit__)

        :return: the elapsed time (in seconds) for the context.
        if __enter__ or __exit__ weren't called, a ContextDidNotEndException exception is raised
        """
        if self._elapsed is None:
            raise ContextDidNotEndException("Context did not end")
        return self._elapsed

    def __enter__(self):
        self._start = perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._elapsed = perf_counter() - self._start


def json_safe_encoder(obj: Any):
    """
    Convert non-serializable types to strings.

    :param obj: The object to make serializable.
    :return: Serializable version of the object (string-form).
    """

    try:
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.time):
            return obj.strftime('%H:%M')
        elif isinstance(obj, bytes):
            return obj.decode()
        else:
            return str(obj)
    except Exception:
        return "UNENCODABLE"
