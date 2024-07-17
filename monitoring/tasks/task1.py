import logging
import time
from datetime import datetime
from typing import Any

from monitoring.tasks.base import AbstractTask, AbstractSubscriber

LOG = logging.getLogger()


class Task1(AbstractTask):
    @classmethod
    def execute(cls, *args, **kwargs):
        LOG.critical(f"Executing Job1, {datetime.now()}")

        time.sleep(10)  # 模拟长时间运行的任务

        LOG.critical(f"Executing Job1, {datetime.now()} after sleep")

        return "Job1 1 result"


class Subscriber1(AbstractSubscriber):
    @classmethod
    def execute(cls, task_name: str, result: Any, is_error: bool = False):
        LOG.error(f"Executing Subscriber1, {result}, {datetime.now()}")

        return "Subscriber1 result"


class Subscriber2(AbstractSubscriber):
    @classmethod
    def execute(cls, task_name: str, result: Any, is_error: bool = False):
        LOG.error(f"Executing Subscriber2, {result}, {datetime.now()}")

        return "Subscriber2 result"
