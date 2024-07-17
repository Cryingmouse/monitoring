import logging
import os

from monitoring.tasks.base import AbstractTask

LOG = logging.getLogger("core_dump")


class CleanCoreDumpTask(AbstractTask):
    @classmethod
    def execute(cls, *args, **kwargs):
        clean_core_dump()


CLEAR_THRESHOLD = 0.2
CORE_DUMP_DIR = "/var/log/systemd/coredump/"
CLEAR_LOG_FILE = "clean_core_dump.log"


def reverse_core_files(dir_path):
    """
    Get all files and directories in the directory specified by the dir_path parameter.
    Sort them in reverse chronological order by creation time.
    """
    # Get the list of all files and folders in the directory
    file_list = os.listdir(dir_path)

    # Return the sorted list of files
    return sorted(
        file_list,
        key=lambda x: os.path.getmtime(os.path.join(dir_path, x)),
        reverse=False,
    )


def clean_core_dump():
    """
    Monitor the /var/log/systemd/coredump/ directory, and clear old core dump files according to the following rules:
    1. When the directory usage exceeds 80%, old core files will be cleared in chronological order of creation until the directory usage is less than 80%;
    2. No distinction is made between data and control plane cores;
    3. When clearing, log the file name, size, and deletion time to the clear_coredump.log file;
    4. Log files will not be cleared.
    """
    LOG.critical("Start to clean core dump file!")

    for core_file_name in reverse_core_files(CORE_DUMP_DIR):
        fs_stat = os.statvfs(CORE_DUMP_DIR)
        if min(fs_stat.f_bfree, fs_stat.f_bavail) / fs_stat.f_blocks > CLEAR_THRESHOLD:
            break

        if core_file_name.startswith(CLEAR_LOG_FILE):
            continue

        abs_core_file_name = os.path.join(CORE_DUMP_DIR, core_file_name)
        if not os.path.isfile(abs_core_file_name):
            continue

        core_file = os.stat(abs_core_file_name)
        os.remove(abs_core_file_name)

        LOG.info(f"{core_file_name} {core_file.st_size} bytes")
