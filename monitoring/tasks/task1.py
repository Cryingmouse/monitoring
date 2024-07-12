import logging
import time
from datetime import datetime

LOG = logging.getLogger()


def task1():
    LOG.critical(f"Executing task1, {datetime.now()}")

    time.sleep(10)  # 模拟长时间运行的任务

    LOG.critical(f"Executing task1, {datetime.now()} after sleep")

    return "Task 1 result"


def callback_task1(task_name, result, is_error):
    print(f"callback_task1 is triggered, {datetime.now()}")
    print(f"the args are {task_name}, {result}, {is_error}")