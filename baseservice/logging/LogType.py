from enum import Enum

LOG_TYPE_EXTRA = 'log_type'


class LogType(Enum):
    """
    this enum defines log_types
    """
    INPUT_LOG_TYPE = 'input_log'
    OUTPUT_LOG_TYPE = 'output_log'
    FINISH_LOG_TYPE = 'finish_log'
