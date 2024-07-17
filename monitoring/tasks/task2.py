import logging
from datetime import datetime
from typing import Any

from monitoring.tasks.base import AbstractTask, AbstractSubscriber

LOG = logging.getLogger()


class Task2(AbstractTask):
    @classmethod
    def execute(cls):
        LOG.critical(f"Executing Task2, {datetime.now()}")

        LOG.critical(f"Executing Task2, {datetime.now()} after sleep")

        return "Task2 result"


class Subscriber1(AbstractSubscriber):
    @classmethod
    def execute(cls, job_name: str, result: Any, is_error: bool = False):
        LOG.error(f"Executing Subscriber1, {result}, {datetime.now()}")

        return "Subscriber1 result"


class Subscriber2(AbstractSubscriber):
    @classmethod
    def execute(cls, job_name: str, result: Any, is_error: bool = False):
        LOG.error(f"Executing Subscriber2, {result}, {datetime.now()}")

        return "Subscriber2 result"
