import logging
from collections import defaultdict

from monitoring.tasks.base import AbstractTask

LOG = logging.getLogger()


class TaskManager:
    def __init__(self):
        self.subscribers = defaultdict(list)

    def execute_and_publish(self, task: AbstractTask, *args, **kwargs):
        task_name = f"{task.__module__}.{task.__name__}"
        try:
            result = task.execute(args, kwargs)
            self.publish(task_name, result)
        except Exception as e:
            LOG.error(f"An exception {e} during the task {task}")
            self.publish(task_name, str(e), is_error=True)

    def publish(self, task_name, result, is_error=False):
        """发布任务结果"""
        for subscriber in self.subscribers[task_name]:
            subscriber.execute(task_name, result, is_error)

    def subscribe(self, task_name, callback):
        """订阅任务结果"""
        if isinstance(callback, list):
            self.subscribers[task_name].extend(callback)
        else:
            self.subscribers[task_name].append(callback)

    def unsubscribe(self, task_name, callback):
        """取消订阅任务结果"""
        if callback in self.subscribers[task_name]:
            self.subscribers[task_name].remove(callback)
