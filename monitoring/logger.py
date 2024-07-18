import datetime as dt
import json
import logging
import os
import sys
from logging import config
from threading import current_thread
from typing import Dict
from typing import Union

LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "parentThread",
    "taskName",
}


class JSONFormatter(logging.Formatter):
    def __init__(self, *, fmt_keys: Union[Dict[str, str], None] = None):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: msg_val if (msg_val := always_fields.pop(val, None)) is not None else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class ParentThreadFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.parentThread = current_thread().ident

        return True


def get_log_directory(default_path):
    """Determine log directory based on platform."""
    platform = sys.platform
    if platform in ["win32", "darwin"]:
        return os.path.join(os.environ["TEMP"], "log")
    return default_path


def setup_logging():
    config_file = os.path.join(os.path.dirname(__file__), "resources", "logging.json")
    if not os.path.isfile(config_file):
        raise FileNotFoundError("Failed to find the logging.json.")

    with open(config_file) as f_in:
        config_json = json.load(f_in)

    # Update file handlers
    for handler_key in ("file_handler", "clean_core_dump_file_handler"):
        if handler_key in config_json["handlers"]:
            log_dir = get_log_directory(config_json["handlers"][handler_key].get("filepath"))
            config_json["handlers"][handler_key]["filename"] = os.path.join(
                log_dir, config_json["handlers"][handler_key]["filename"]
            )
            config_json["handlers"][handler_key].pop("filepath", None)

    config.dictConfig(config_json)
