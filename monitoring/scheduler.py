import importlib
import logging
import os
from datetime import datetime
from pathlib import Path

import yaml
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from pytz import timezone, utc
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from monitoring.job_manager import JobManager

LOG = logging.getLogger()


class ConfigFileHandler(FileSystemEventHandler):
    def __init__(self, scheduler):
        self.scheduler = scheduler

    def on_modified(self, event):
        if event.src_path == self.scheduler.task_config_file:
            LOG.info("Config file changed. Reloading...")
            self.scheduler.reload_config()


class Scheduler:
    def __init__(self, config_file, job_store_url):
        job_stores = {"default": MemoryJobStore(), "mariadb": SQLAlchemyJobStore(url=job_store_url)}
        executors = {
            "default": ThreadPoolExecutor(),
            "thread_pool": ThreadPoolExecutor(max_workers=20),
            "process_pool": ProcessPoolExecutor(max_workers=20),
        }
        job_defaults = {"coalesce": True, "max_instances": 1}
        self.scheduler = BackgroundScheduler(
            jobstores=job_stores, executors=executors, job_defaults=job_defaults, timezone=utc
        )
        self.scheduler.add_listener(self._job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)

        self.task_config_file = self.find_config(config_file)
        self.config = None
        self.load_task_config()

        handler = ConfigFileHandler(self)
        self.observer = Observer()
        self.observer.schedule(handler, str(Path(self.task_config_file).parent), recursive=False)
        self.observer.start()

    @staticmethod
    def find_config(config_file):
        if config_file and os.path.exists(config_file):
            return config_file

        search_paths = [
            os.path.join(os.path.dirname(__file__), "resources", "task_config.yaml"),
            os.path.join(os.getcwd(), "task_config.yaml"),
            os.path.expanduser("~/.task_config.yaml"),
            "/etc/monitoring/task_config.yaml",
        ]

        for path in search_paths:
            if os.path.exists(path):
                return path

        raise FileNotFoundError("Config file not found. Please create a config file or specify its location.")

    def load_task_config(self):
        """从配置文件加载任务"""
        with open(self.task_config_file, "r") as file:
            self.config = yaml.safe_load(file)

        self.apply_task_config()

    def apply_task_config(self):
        """应用配置，添加新任务，删除不再需要的任务"""
        current_jobs = {job.id: job for job in self.scheduler.get_jobs()}
        new_job_ids = set()

        for task_config in self.config["tasks"]:
            module_name, job_name = task_config["task"].rsplit(".", 1)
            module = importlib.import_module(module_name)
            job = getattr(module, job_name)
            job_id = f"{module_name}.{job_name}"

            trigger = self._create_trigger(task_config)
            executor = self._get_executor(task_config)
            subscribes = self._get_subscribes(task_config)

            if job_id in current_jobs:
                # 更新现有任务
                self.scheduler.reschedule_job(job_id, trigger=trigger)
            else:
                # 添加新任务
                self.register(job=job, trigger=trigger, executor=executor, subscribes=subscribes)

            new_job_ids.add(job_id)

        # 删除不再需要的任务
        for job_id in set(current_jobs.keys()) - new_job_ids:
            self.unregister(job_id)

    def reload_config(self):
        """重新加载配置文件并应用更改"""
        self.load_task_config()

    def register(self, job, trigger, executor, subscribes):
        """注册一个新任务及其执行间隔"""
        job_name = f"{job.__module__}.{job.__name__}"
        job = self.scheduler.add_job(
            func=JobManager.execute_and_publish,
            trigger=trigger,
            executor=executor,
            args=[job],
            id=job_name,
            jobstore="mariadb",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        JobManager.subscribe(job_name, subscribes)
        return job_name

    def unregister(self, job_id):
        """取消注册一个任务"""
        self.scheduler.remove_job(job_id)

    def start(self):
        """启动定时任务框架"""
        self.scheduler.start()

    def stop(self):
        """停止定时任务框架"""
        self.scheduler.shutdown()
        self.observer.stop()
        self.observer.join()

    @staticmethod
    def _create_trigger(task_config):
        """根据配置创建相应的触发器"""
        trigger_type = task_config["trigger_type"].lower()

        if trigger_type == "interval":
            return IntervalTrigger(seconds=task_config["interval"])
        elif trigger_type == "cron":
            cron_config = task_config["cron"]
            cron_expression = cron_config["expression"]
            cron_parts = cron_expression.split()

            if len(cron_parts) == 5:
                # Standard cron format：Min Hour Day Month Week
                cron_kwargs = {
                    "minute": cron_parts[0],
                    "hour": cron_parts[1],
                    "day": cron_parts[2],
                    "month": cron_parts[3],
                    "day_of_week": cron_parts[4],
                }
            elif len(cron_parts) == 6:
                # Extend cron format：Sec Min Hour Day Month Week
                cron_kwargs = {
                    "second": cron_parts[0],
                    "minute": cron_parts[1],
                    "hour": cron_parts[2],
                    "day": cron_parts[3],
                    "month": cron_parts[4],
                    "day_of_week": cron_parts[5],
                }
            else:
                raise ValueError(
                    "Invalid cron expression. Expected either 5 parts (standard cron) or 6 parts (extended cron)"
                )

            # 处理可选参数
            for field in ["start_date", "end_date", "timezone", "jitter"]:
                if field in cron_config:
                    if field in ["start_date", "end_date"]:
                        cron_kwargs[field] = datetime.fromisoformat(cron_config[field])
                    elif field == "timezone":
                        cron_kwargs[field] = timezone(cron_config[field])
                    else:
                        print(field)
                        cron_kwargs[field] = cron_config[field]

            return CronTrigger(**cron_kwargs)
        elif trigger_type == "date":
            return DateTrigger(run_date=datetime.fromisoformat(task_config["run_date"]))
        else:
            raise ValueError(f"Unsupported trigger type: {trigger_type}")

    @staticmethod
    def _get_executor(task_config):
        """获取执行器"""
        executor_name = task_config.get("executor", "default")
        if executor_name == "ProcessPoolExecutor":
            return "process_pool"
        elif executor_name == "ThreadPoolExecutor":
            return "thread_pool"
        else:
            return "default"

    @staticmethod
    def _get_subscribes(task_config):
        """获取回调任务（如果存在）"""
        subscribes = []

        if "subscribers" in task_config:
            for subscribe in task_config["subscribers"]:
                module_name, callback_name = subscribe.rsplit(".", 1)
                module = importlib.import_module(module_name)
                subscribes.append(getattr(module, callback_name))

        return subscribes

    @staticmethod
    def _job_listener(event):
        """监听任务执行事件"""
        if event.code == EVENT_JOB_MISSED:
            LOG.error(f"Job {event.job_id} missed its execution time")

        if event.code == EVENT_JOB_EXECUTED:
            LOG.info(f"Job {event.job_id} was executed")
