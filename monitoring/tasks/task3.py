import logging
from datetime import datetime

LOG = logging.getLogger()


def task3():
    LOG.critical(f"Executing task3, {datetime.now()}")

    return "Task 3 result"
