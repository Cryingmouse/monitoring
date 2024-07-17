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

from monitoring.task_manager import TaskManager

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

        self.task_manager = TaskManager()

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
        current_tasks = {job.id: job for job in self.scheduler.get_jobs()}
        new_task_names = set()

        for task_config in self.config["tasks"]:
            module_name, task_name = task_config["task"].rsplit(".", 1)
            module = importlib.import_module(module_name)
            task = getattr(module, task_name)
            task_name = f"{module_name}.{task_name}"

            trigger = self._create_trigger(task_config)
            executor = self._get_executor(task_config)
            subscribes = self._get_subscribes(task_config)
            store = self._get_store(task_config)

            if task_name in current_tasks:
                # 更新现有任务
                self.scheduler.reschedule_job(task_name, trigger=trigger)
            else:
                # 添加新任务
                self.register(task=task, trigger=trigger, executor=executor, store=store, subscribes=subscribes)

            new_task_names.add(task_name)

        # 删除不再需要的任务
        for task_name in set(current_tasks.keys()) - new_task_names:
            self.unregister(task_name)

    def reload_config(self):
        """重新加载配置文件并应用更改"""
        self.load_task_config()

    def register(self, task, trigger, executor, store, subscribes, *args, **kwargs):
        task_name = f"{task.__module__}.{task.__name__}"

        self.scheduler.add_job(
            func=self.task_manager.execute_and_publish,
            trigger=trigger,
            executor=executor,
            args=[task, args, kwargs],
            id=task_name,
            jobstore=store,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

        self.task_manager.subscribe(task_name, subscribes)

        return task_name

    def unregister(self, task_name):
        """Unregister a task from scheduler."""
        self.scheduler.remove_job(task_name)

    def start(self):
        """Start scheduler"""
        self.scheduler.start()

    def stop(self):
        """Stop scheduler"""
        self.scheduler.shutdown()
        self.observer.stop()
        self.observer.join()

    @staticmethod
    def _create_trigger(task_config):
        """
        Create a trigger based on the provided task configuration.

        This function creates and returns an appropriate trigger object based on the
        trigger type specified in the task configuration. It supports three types of
        triggers: interval, cron, and date.

        Args:
            task_config (dict): A dictionary containing the task configuration.
                It must include a 'trigger_type' key and additional keys based on the trigger type:
                - For 'interval': 'interval' (in seconds)
                - For 'cron': 'cron' dict with 'expression' and optional 'start_date', 'end_date', 'timezone', 'jitter'
                - For 'date': 'run_date' (ISO format datetime string)

        Returns:
            apscheduler.triggers.base.BaseTrigger: An instance of the appropriate trigger class.

        Raises:
            ValueError: If an unsupported trigger type is specified or if the cron expression is invalid.

        Examples:
            >>> config = {"trigger_type": "interval", "interval": 60}
            >>> trigger = _create_trigger(config)
            >>> type(trigger)
            <class 'apscheduler.triggers.interval.IntervalTrigger'>

            >>> config = {"trigger_type": "cron", "cron": {"expression": "0 0 * * *"}}
            >>> trigger = _create_trigger(config)
            >>> type(trigger)
            <class 'apscheduler.triggers.cron.CronTrigger'>

            >>> config = {"trigger_type": "date", "run_date": "2023-01-01T00:00:00"}
            >>> trigger = _create_trigger(config)
            >>> type(trigger)
            <class 'apscheduler.triggers.date.DateTrigger'>
        """
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
                        cron_kwargs[field] = cron_config[field]

            return CronTrigger(**cron_kwargs)
        elif trigger_type == "date":
            return DateTrigger(run_date=datetime.fromisoformat(task_config["run_date"]))
        else:
            raise ValueError(f"Unsupported trigger type: {trigger_type}")

    @staticmethod
    def _get_executor(task_config):
        """Get the executor type for a task.

        Determines and returns the appropriate executor identifier based on the
        executor name specified in the task configuration. Supports process pool
        executor, thread pool executor, and default executor.

        Args:
            task_config (dict): Task configuration dictionary. Should contain an
                                optional 'executor' key to specify the executor type.

        Returns:
            str: Executor identifier. Possible return values:
                 - 'process_pool': Corresponds to ProcessPoolExecutor
                 - 'thread_pool': Corresponds to ThreadPoolExecutor
                 - 'default': For default executor or when no executor is specified

        Examples:
            >>> config = {'executor': 'ProcessPoolExecutor'}
            >>> _get_executor(config)
            'process_pool'

            >>> config = {'executor': 'ThreadPoolExecutor'}
            >>> _get_executor(config)
            'thread_pool'

            >>> config = {}
            >>> _get_executor(config)
            'default'

        Note:
            If the 'executor' key is not specified in task_config, or if an unknown executor name is provided, the
            function will return 'default'.
        """
        executor_name = task_config.get("executor", "default")
        if executor_name == "ProcessPoolExecutor":
            return "process_pool"
        elif executor_name == "ThreadPoolExecutor":
            return "thread_pool"
        else:
            return "default"

    @staticmethod
    def _get_store(task_config):
        task_store = task_config.get("task_store", "default")

        return task_store if task_store in ("default", "mariadb") else "default"

    @staticmethod
    def _get_subscribes(task_config):
        """Retrieve and return a list of subscriber callback functions for a task.

        This function parses the 'subscribers' field in the task configuration,
        imports the specified modules, and retrieves the callback functions.

        Args:
            task_config (dict): A dictionary containing the task configuration.
                                It should have a 'subscribers' key with a list
                                of strings in the format 'module.callback_function'.

        Returns:
            list: A list of callable objects (functions) that are subscribed to the task.

        Raises:
            ImportError: If a specified module cannot be imported.
            AttributeError: If a specified callback function cannot be found in the module.

        Example:
            >>> config = {
            ...     'subscribers': ['mymodule.callback1', 'anothermodule.callback2']
            ... }
            >>> callbacks = _get_subscribes(config)
            >>> len(callbacks)
            2
            >>> all(callable(func) for func in callbacks)
            True

        Note:
            The function uses `importlib.import_module` to dynamically import modules,
            and `getattr` to retrieve the callback functions from these modules.
            Ensure that all specified modules and functions exist and are importable.
        """
        subscribes = []

        if "subscribers" in task_config:
            for subscribe in task_config["subscribers"]:
                module_name, callback_name = subscribe.rsplit(".", 1)
                module = importlib.import_module(module_name)
                subscribes.append(getattr(module, callback_name))

        return subscribes

    @staticmethod
    def _job_listener(event):
        """Listen for and log job execution events.

        This function serves as an event listener for scheduler job events.
        It specifically handles missed job executions and successful job executions,
        logging appropriate messages for each case.

        Args:
            event (apscheduler.events.JobEvent): An event object from APScheduler
                containing information about the job event.

        Returns:
            None

        Logs:
            - ERROR: When a job misses its execution time.
            - INFO: When a job is successfully executed.

        Example:
            This function is typically used as a callback for APScheduler events:

            >>> from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_MISSED
            >>> scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED)

        Note:
            This function assumes the existence of a global `LOG` object
            for logging. Ensure that `LOG` is properly configured before
            using this function.
        """
        if event.code == EVENT_JOB_MISSED:
            LOG.error(f"Job {event.job_id} missed its execution time")

        if event.code == EVENT_JOB_EXECUTED:
            LOG.info(f"Job {event.job_id} was executed")
