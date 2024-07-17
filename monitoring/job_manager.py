import logging
from collections import defaultdict

from monitoring.tasks.base import AbstractTask

LOG = logging.getLogger()


class JobManager:
    subscribers = defaultdict(list)

    @classmethod
    def execute_and_publish(cls, task: AbstractTask):
        task_name = f"{task.__module__}.{task.__name__}"
        try:
            result = task.execute()
            cls.publish(task_name, result)
        except Exception as e:
            LOG.error(f"An exception {e} during the task {task}")
            cls.publish(task_name, str(e), is_error=True)

    @classmethod
    def publish(cls, task_name, result, is_error=False):
        """发布任务结果"""
        for subscriber in cls.subscribers[task_name]:
            subscriber.execute(task_name, result, is_error)

    @classmethod
    def subscribe(cls, task_name, callback):
        """订阅任务结果"""
        if isinstance(callback, list):
            cls.subscribers[task_name].extend(callback)
        else:
            cls.subscribers[task_name].append(callback)

    @classmethod
    def unsubscribe(cls, task_name, callback):
        """取消订阅任务结果"""
        if callback in cls.subscribers[task_name]:
            cls.subscribers[task_name].remove(callback)
