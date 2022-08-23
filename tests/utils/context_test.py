import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from baseservice.utils.context import global_context, thread_local_context, asyncio_context

executor = ThreadPoolExecutor()


def test_sanity():
    test_key = str(uuid.uuid4())

    def func(tst_key):
        org_vals = (global_context.get(tst_key), thread_local_context.get(tst_key), asyncio_context.get(tst_key))
        with global_context.start_context(**{tst_key: 'new_val'}):
            with thread_local_context.start_context(**{tst_key: 'new_val'}):
                with asyncio_context.start_context(**{tst_key: 'new_val'}):
                    new_vals = (global_context.get(tst_key),
                                thread_local_context.get(tst_key),
                                asyncio_context.get(tst_key))
                    sleep(1)
        return org_vals, new_vals

    with global_context.start_context(**{test_key: 'old_val'}):
        with thread_local_context.start_context(**{test_key: 'old_val'}):
            with asyncio_context.start_context(**{test_key: 'old_val'}):
                fut_a = executor.submit(func, test_key)
                sleep(0.5)
                assert (global_context.get(test_key), thread_local_context.get(test_key),
                        asyncio_context.get(test_key)) == ('new_val', 'old_val', 'old_val')
                org_vals, new_vals = fut_a.result()
                assert org_vals == ('old_val', None, None)
                assert new_vals == ('new_val', 'new_val', 'new_val')
                assert (global_context.get(test_key), thread_local_context.get(test_key),
                        asyncio_context.get(test_key)) == ('old_val', 'old_val', 'old_val')


def test_asyncio():
    test_key = str(uuid.uuid4())

    async def return_value(tst_key):
        return asyncio_context.get(tst_key)

    async def func(tst_key, index: int):
        org_val = await return_value(tst_key)
        with asyncio_context.start_context(**{tst_key: f'new_val - {index}'}):
            new_val = await return_value(tst_key)
        return org_val, new_val

    async def run_test():
        with asyncio_context.start_context(**{test_key: 'old_val'}):
            task1 = asyncio.create_task(func(test_key, 1))
            task2 = asyncio.create_task(func(test_key, 2))
            await asyncio.wait([task1, task2])
            org_val, new_val = task1.result()
            assert org_val == 'old_val'
            assert new_val == f'new_val - 1'
            org_val, new_val = task2.result()
            assert org_val == 'old_val'
            assert new_val == f'new_val - 2'

            assert asyncio_context.get(test_key) == 'old_val'

    asyncio.run(run_test())
