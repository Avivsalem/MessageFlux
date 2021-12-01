from concurrent.futures import ThreadPoolExecutor

import pytest

from baseservice.utils import ThreadLocalMember

executor = ThreadPoolExecutor()


def test_thread_local_member():
    class MyTestClass:
        prop1: ThreadLocalMember[int] = ThreadLocalMember(1)
        prop2: ThreadLocalMember[str] = ThreadLocalMember()
        prop3: ThreadLocalMember[int] = ThreadLocalMember(3)
        prop4: ThreadLocalMember[str] = ThreadLocalMember()

        def __init__(self):
            self.prop1 = 11
            self.prop2 = "this is the init"
            self.not_local = 7

    a = MyTestClass()
    assert a.prop1 == 11
    assert a.prop2 == "this is the init"
    assert a.prop3 == 3
    with pytest.raises(AttributeError):
        x = a.prop4
    a.prop4 = "prop4"  # this is the first set, so from now on, all the threads will see this value as the default
    assert a.prop4 == "prop4"
    assert a.not_local == 7
    a.not_local = 77

    b = MyTestClass()
    assert b.prop1 == 11
    assert b.prop2 == "this is the init"
    assert b.prop3 == 3
    with pytest.raises(AttributeError):
        x = b.prop4
    assert b.not_local == 7
    b.not_local = 77

    def thread_func(p: MyTestClass):
        assert p.prop1 == 1
        assert p.prop2 == "this is the init"
        assert p.prop3 == 3
        assert p.not_local == 77
        p.prop1 = 1111
        try:
            return p.prop4
        except AttributeError:
            return "Exception"

    fut_a = executor.submit(thread_func, a)
    assert fut_a.result() == "prop4"
    fut_b = executor.submit(thread_func, b)
    assert fut_b.result() == "Exception"

    assert a.prop1 == 11
    assert b.prop1 == 11
