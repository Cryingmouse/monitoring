import logging
from datetime import datetime

LOG = logging.getLogger()


def task2():
    LOG.critical(f"Executing task2, {datetime.now()}")

    return "Task 2 result"

def callback_task2(task_name, result, is_error):
    print(f"callback_task1 is triggered, {datetime.now()}")
    print(f"the args are {task_name}, {result}, {is_error}")