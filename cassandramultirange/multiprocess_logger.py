import logging
import os
import sys
from multiprocessing import get_logger


def multiprocess_logger(dunder_file: str = None, *, filename: str = None):
    new_logger = get_logger()
    new_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s| %(levelname)s| %(processName)s %(threadName)s] %(message)s')
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    new_logger.addHandler(handler)

    if dunder_file:
        module = os.path.basename(dunder_file).split(".")[0]
    else:
        module = filename
    new_logger.addFilter(lambda r: r.module == module)
    return new_logger
