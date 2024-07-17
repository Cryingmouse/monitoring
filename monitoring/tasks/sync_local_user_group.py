import logging
import time
from datetime import datetime
from typing import Any

from monitoring.tasks.base import AbstractTask, AbstractSubscriber

LOG = logging.getLogger()


class SyncLocalUserAndGroup(AbstractTask):
    @classmethod
    def execute(cls, *args, **kwargs):
        LOG.info(f"Sync local user and group.")
