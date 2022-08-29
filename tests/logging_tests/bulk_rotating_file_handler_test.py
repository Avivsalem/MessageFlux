import logging
import os

import time

from baseservice.logging import BulkRotatingFileHandler


def test_sanity(tmpdir):
    live_path = os.path.join(tmpdir.strpath, "live_path")
    rotating_path = os.path.join(tmpdir.strpath, "rotated_path")
    handler = BulkRotatingFileHandler(live_path, rotating_path,
                                      max_records=1, max_time=1)
    logger = logging.getLogger("TestHandler")
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.info("random_message")
    time.sleep(2)

    log_files = os.listdir(rotating_path)
    assert len(log_files) == 1

    with open(os.path.join(rotating_path, log_files[0]), "r") as log_file:
        assert log_file.read() == "random_message\n"
