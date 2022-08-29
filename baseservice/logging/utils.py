import logging

import sys

DEFAULT_LOG_FORMATTER = logging.Formatter(
    "%(asctime)-15s [%(levelname)s] [PID:%(process)d] %(message)s")
DEFAULT_LOG_HANDLER = logging.StreamHandler(sys.stdout)
DEFAULT_LOG_HANDLER.setFormatter(DEFAULT_LOG_FORMATTER)
