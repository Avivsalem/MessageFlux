import threading
from typing import Collection, TypeVar, List, Iterator, Generic, Callable, Optional
from itertools import cycle, islice


class KwargsException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.kwargs = kwargs


TItemType = TypeVar('TItemType')


class StatefulListIterator(Collection[TItemType]):
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


class Event(Generic[TEventType]):
    def __init__(self) -> None:
        self._handlers: List[Callable[[TEventType], None]] = []

    def register_handler(self, handler: Callable[[TEventType], None]) -> None:
        self._handlers.append(handler)

    def unregister_handler(self, handler: Callable[[TEventType], None]) -> bool:
        if handler in self._handlers:
            self._handlers.remove(handler)
            return True
        return False

    def fire(self, event: TEventType, continue_after_failure: bool = True) -> bool:
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
    this is a descriptor, for making thread local memebers for class:

    class MyClass:
        x = ThreadLocalMember(5)

    a = MyClass():
    a.x # 5
    a.x = 7 # changes only for this thread
    """
    def __init__(self, init_value: Optional[TValueType] = None):
        self._init_value = init_value

    def __set_name__(self, owner, name):
        self._private_name = f'_thread_local_{name}'

    def _get_local_value(self, instance) -> ThreadLocalValue:
        if hasattr(instance, self._private_name):
            lv = getattr(instance, self._private_name)
        else:
            lv = ThreadLocalValue(self._init_value)
            setattr(instance, self._private_name, lv)

        return lv

    def __get__(self, instance, owner) -> Optional[TValueType]:
        lv = self._get_local_value(instance)
        return lv.value

    def __set__(self, instance, value):
        lv = self._get_local_value(instance)
        lv.value = value
