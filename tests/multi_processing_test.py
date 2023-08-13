import logging
import os
import sys
import threading
import time

from messageflux.base_service import BaseService
from messageflux.multiprocessing import ServiceFactory
from messageflux.multiprocessing.multiprocessrunner import MultiProcessRunner

DEFAULT_LOG_FORMATTER = logging.Formatter(
    u"%(asctime)-15s [%(levelname)s] [PID:%(process)d] %(message)s")
DEFAULT_LOG_HANDLER = logging.StreamHandler(sys.stdout)
DEFAULT_LOG_HANDLER.setFormatter(DEFAULT_LOG_FORMATTER)


class MockServiceFactory(ServiceFactory):
    def __init__(self, tmpdir, fail=False, should_exit=True):
        self._tmpdir = tmpdir
        self._fail = fail
        self._should_exit = should_exit

    def create_service(self, instance_index: int, total_instances: int) -> BaseService:
        return MockService(self._tmpdir,
                           instance_index=instance_index,
                           total_instances=total_instances,
                           fail=self._fail,
                           should_exit=self._should_exit)


class MockService(BaseService):
    def __init__(self, temp_dir, instance_index: int, total_instances: int, fail=False, should_exit=True):
        super(MockService, self).__init__()
        self._instance_index = instance_index
        self._total_instances = total_instances
        self._temp_dir = temp_dir
        self._fail = fail
        self._exit = should_exit

    def _run_service(self, cancellation_token: threading.Event):
        print(f'Started Process {self._instance_index} out of {self._total_instances}')
        try:
            filename = os.path.join(self._temp_dir, str(os.getpid()))
            with open(filename, 'w') as f:
                f.write('data')
            print(f"{self._instance_index}: file written")
            if self._fail:
                raise Exception()
            else:
                cancellation_token.wait()

            os.remove(filename)
        except Exception as ex:
            print(str(ex))
            raise

    def stop(self):
        if not self._exit:
            return
        super(MockService, self).stop()


# @pytest.mark.skip(reason="Is statistical currently, must be fixed")
def test_sanity(tmpdir):
    logger = logging.getLogger()
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(DEFAULT_LOG_HANDLER)

    tmpdir = str(tmpdir)
    multi_runner = MultiProcessRunner(service_factory=MockServiceFactory(tmpdir), instance_count=3)

    threading.Thread(target=multi_runner.start, daemon=True).start()
    for i in range(90):
        num_of_dirs = len(os.listdir(tmpdir))
        print(f'found {num_of_dirs} files in dir')
        if num_of_dirs == 3:
            break
        time.sleep(1)
    assert len(os.listdir(tmpdir)) == 3
    multi_runner.stop()

    print("Stopped MultiProcessRunner")

    for i in range(10):
        num_of_dirs = len(os.listdir(tmpdir))
        print(f'found {num_of_dirs} files in dir')
        if num_of_dirs == 0:
            break
        time.sleep(1)

    assert len(os.listdir(tmpdir)) == 0


# @pytest.mark.skip(reason="Is statistical currently, must be fixed")
def test_force_kill(tmpdir):
    tmpdir = str(tmpdir)
    multi_runner = MultiProcessRunner(service_factory=MockServiceFactory(tmpdir, should_exit=False),
                                      instance_count=3,
                                      shutdown_timeout=1)

    threading.Thread(target=multi_runner.start, daemon=True).start()

    for i in range(60):
        processes = multi_runner.processes
        alive = [p for p in processes if p.is_alive()]
        num_of_dirs = len(os.listdir(tmpdir))
        print(f'found {num_of_dirs} files in dir and {len(alive)} alive processes')
        if len(alive) == 3 and num_of_dirs == 3:
            break
        time.sleep(1)

    processes = multi_runner.processes
    alive = [p for p in processes if p.is_alive()]
    assert len(alive) == 3
    assert len(os.listdir(tmpdir)) == 3

    multi_runner.stop()

    print("Stopped MultiProcessRunner")
    for i in range(5):
        alive = [p for p in processes if p.is_alive()]
        if len(alive) == 0:
            break
        time.sleep(1)
    alive = [p for p in processes if p.is_alive()]
    assert len(alive) == 0
    assert len(os.listdir(tmpdir)) == 3
