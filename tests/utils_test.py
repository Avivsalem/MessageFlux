from concurrent.futures import ThreadPoolExecutor

from baseservice.utils import ThreadLocalMember

executor = ThreadPoolExecutor()


def test_thread_local_member():
    class MyTestClass:
        prop1: ThreadLocalMember[int] = ThreadLocalMember(5)
        prop2: ThreadLocalMember[str] = ThreadLocalMember()

    a = MyTestClass()
    assert a.prop1 == 5
    assert a.prop2 is None

    a.prop1 = 4
    a.prop2 = "Bla"

    b = MyTestClass()
    assert b.prop1 == 5
    assert b.prop2 is None

    def func():
        b.prop2 = "this is thread"
        return a.prop1, a.prop2, b.prop1, b.prop2

    fut = executor.submit(func)
    assert fut.result() == (5, None, 5, "this is thread")
    assert b.prop2 is None
