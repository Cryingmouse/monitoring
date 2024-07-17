import logging
import os

from monitoring.tasks.base import AbstractTask

LOG = logging.getLogger("core_dump")

CLEAR_THRESHOLD = 0.2
CORE_DUMP_DIR = "/var/log/systemd/coredump/"
CLEAR_LOG_FILE = "clean_core_dump.log"


class CleanCoreDumpTask(AbstractTask):
    @classmethod
    def execute(cls, *args, **kwargs):
        """Monitors and clears old core dump files in the /var/log/systemd/coredump/ directory.

        This method implements a cleanup strategy for core dump files based on the following rules:
        1. When directory usage exceeds 80%, old core files are cleared in chronological order until usage falls below 80%.
        2. No distinction is made between data and control plane cores.
        3. Deleted file names, sizes, and deletion times are logged to clean_core_dump.log.
        4. Log files are not cleared.

        Raises:
            OSError: If there are issues accessing or modifying files and directories.
        """
        LOG.info("Start to clean core dump file!")

        for core_file_name in get_filtered_core_files(CORE_DUMP_DIR):
            fs_stat = os.statvfs(CORE_DUMP_DIR)
            if min(fs_stat.f_bfree, fs_stat.f_bavail) / fs_stat.f_blocks > CLEAR_THRESHOLD:
                break

            abs_core_file_name = os.path.join(CORE_DUMP_DIR, core_file_name)
            core_file = os.stat(abs_core_file_name)
            os.remove(abs_core_file_name)

            LOG.info(f"{core_file_name} {core_file.st_size} bytes")


def get_filtered_core_files(dir_path):
    """Get filtered core dump files in the given directory and sort by modification time.

    This function retrieves core dump files in the specified directory, filters out log files and non-file entries, and
    sorts them in chronological order based on their modification time, with the oldest items first.

    Args:
        dir_path (str): The path to the directory to be processed.

    Returns:
        list: A list of core dump file names sorted by modification time in ascending order (oldest first), excluding
        log files and non-file entries.

    Raises:
        OSError: If the directory cannot be accessed or read.
    """
    # Get the list of all files and folders in the directory
    file_list = os.listdir(dir_path)

    # Filter and sort the list of files
    filtered_files = [
        f for f in file_list if os.path.isfile(os.path.join(dir_path, f)) and not f.startswith(CLEAR_LOG_FILE)
    ]

    return sorted(
        filtered_files,
        key=lambda x: os.path.getmtime(os.path.join(dir_path, x)),
        reverse=False,
    )
