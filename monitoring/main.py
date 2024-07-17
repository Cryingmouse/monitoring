import click

from monitoring.logger import setup_logging
from monitoring.scheduler import Scheduler

setup_logging()


@click.command()
@click.option(
    "-c", "--config_file", "config_file", help="Path to the configuration file", required=True, type=str)
def main(config_file):
    job_store_url = "mysql+pymysql://root:NAS_PASS@127.0.0.1:3306/nas"

    scheduler = Scheduler(config_file=config_file, job_store_url=job_store_url)
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
