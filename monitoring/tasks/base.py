from typing import Any


class AbstractTask:
    @classmethod
    def execute(cls):
        raise NotImplemented


class AbstractSubscriber:
    @classmethod
    def execute(cls, job_name: str, result: Any, is_error: bool):
        raise NotImplemented
