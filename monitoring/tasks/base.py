from typing import Any


class AbstractTask:
    @classmethod
    def execute(cls, *args, **kwargs):
        raise NotImplemented


class AbstractSubscriber:
    @classmethod
    def execute(cls, task_name: str, result: Any, is_error: bool):
        raise NotImplemented
