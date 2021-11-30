from typing import Collection, TypeVar,List, Iterator,Generic,Callable
from itertools import cycle, islice


class KwargsException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.kwargs = kwargs

TItemType = TypeVar('TItemType')

class StatefulListIterator(Collection[TItemType]):
    def __init__(self, items:List[TItemType]) -> None:
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
    
    def fire(self, event: TEventType, continue_after_failure:bool=True) -> bool:
        ret_val = True
        for handler in self._handlers:
            try:
                handler(event)
            except Exception:
                ret_val = False
                if not continue_after_failure:
                    raise
        return ret_val
        

        