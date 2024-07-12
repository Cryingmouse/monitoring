import click

from monitoring.logger import setup_logging
from monitoring.scheduler import TimedTaskScheduler
from monitoring.tasks.task1 import callback_task1

setup_logging()


@click.command()
@click.option(
    "-c", "--config_file", "config_file", help="Path to the configuration file", required=True, type=str)
def main(config_file):
    scheduler = TimedTaskScheduler(config_file=config_file)
    scheduler.start()

    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping the scheduler...")
    finally:
        scheduler.stop()


if __name__ == "__main__":
    main()
