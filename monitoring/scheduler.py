import importlib
import logging
import os
from collections import defaultdict
from pathlib import Path

import yaml
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, framework):
        self.framework = framework

    def on_modified(self, event):
        if event.src_path == self.framework.config_file:
            logger.info("Config file changed. Reloading...")
            self.framework.reload_config()


class TimedTaskScheduler:
    def __init__(self, config_file):
        self.config_file = self.find_config(config_file)
        self.scheduler = BackgroundScheduler()
        self.subscribers = defaultdict(list)
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        self.load_config()
        self.setup_config_watcher()

    @staticmethod
    def find_config(config_file):
        if config_file and os.path.exists(config_file):
            return config_file

        # 查找顺序：当前目录 -> 用户主目录 -> /etc/timed_task_framework
        search_paths = [
            os.path.join(os.path.dirname(__file__), "resources", "task_config.yaml"),
            os.path.join(os.getcwd(), 'task_config.yaml'),
            os.path.expanduser('~/.task_config.yaml'),
            '/etc/monitoring/task_config.yaml',
        ]

        for path in search_paths:
            if os.path.exists(path):
                return path

        raise FileNotFoundError("Config file not found. Please create a config file or specify its location.")

    def setup_config_watcher(self):
        self.observer = Observer()
        handler = ConfigFileHandler(self)
        self.observer.schedule(handler, Path(self.config_file).parent, recursive=False)
        self.observer.start()

    def load_config(self):
        """从配置文件加载任务"""
        with open(self.config_file, 'r') as file:
            self.config = yaml.safe_load(file)

        self.apply_config()

    def apply_config(self):
        """应用配置，添加新任务，删除不再需要的任务"""
        current_jobs = {job.id: job for job in self.scheduler.get_jobs()}
        new_job_ids = set()

        for task_config in self.config['tasks']:
            module_name, func_name = task_config['task'].rsplit('.', 1)
            module = importlib.import_module(module_name)
            task = getattr(module, func_name)
            job_id = f"{module_name}.{func_name}"

            if job_id in current_jobs:
                # 更新现有任务
                self.scheduler.reschedule_job(
                    job_id,
                    trigger=IntervalTrigger(seconds=task_config['interval'])
                )
            else:
                # 添加新任务
                self.register(task, task_config['interval'])

            new_job_ids.add(job_id)

        # 删除不再需要的任务
        for job_id in set(current_jobs.keys()) - new_job_ids:
            self.unregister(job_id)

    def reload_config(self):
        """重新加载配置文件并应用更改"""
        self.load_config()

    def register(self, task, interval):
        """注册一个新任务及其执行间隔"""
        job_id = f"{task.__module__}.{task.__name__}"
        job = self.scheduler.add_job(
            self._execute_and_publish,
            trigger=IntervalTrigger(seconds=interval),
            args=[task],
            id=job_id,
            max_instances=1,
            coalesce=True
        )
        return job.id

    def unregister(self, job_id):
        """取消注册一个任务"""
        self.scheduler.remove_job(job_id)

    def _execute_and_publish(self, task):
        """执行任务并发布结果"""
        try:
            result = task()
            self.publish(task.__name__, result)
        except Exception as e:
            self.publish(task.__name__, str(e), is_error=True)

    def publish(self, task_name, result, is_error=False):
        """发布任务结果"""
        for subscriber in self.subscribers[task_name]:
            subscriber(task_name, result, is_error)

    def subscribe(self, task_name, callback):
        """订阅任务结果"""
        self.subscribers[task_name].append(callback)

    def unsubscribe(self, task_name, callback):
        """取消订阅任务结果"""
        if callback in self.subscribers[task_name]:
            self.subscribers[task_name].remove(callback)

    def start(self):
        """启动定时任务框架"""
        self.scheduler.start()

    def stop(self):
        """停止定时任务框架"""
        self.scheduler.shutdown()
        self.observer.stop()
        self.observer.join()

    def _job_listener(self, event):
        """监听任务执行事件"""
        if event.code == EVENT_JOB_MISSED:
            logger.warning(f"Job {event.job_id} missed its execution time")

        if event.code == EVENT_JOB_EXECUTED:
            logger.critical(f"Job {event.job_id} was executed")

    def result_handler(task_name, result, is_error):
        if is_error:
            logger.error(f"Error in {task_name}: {result}")
        else:
            logger.info(f"Result from {task_name}: {result}")
