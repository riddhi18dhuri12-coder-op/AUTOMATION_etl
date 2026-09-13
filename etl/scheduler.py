"""
Scheduler: runs the ETL pipeline automatically on a cron or
interval schedule, defined in config.yaml under `schedule:`.
Run this as a long-lived process (e.g. via systemd, Docker, or
a background worker) for full automation.
"""
import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from etl.pipeline import ETLPipeline
from etl.utils.config_loader import load_config


def run_pipeline_job(config_path: str):
    pipeline = ETLPipeline(config_path)
    pipeline.run()


def start_scheduler(config_path: str = "./config/config.yaml"):
    config = load_config(config_path)
    schedule_cfg = config.get("schedule", {})

    if not schedule_cfg.get("enabled"):
        print("Scheduling is disabled in config.yaml (schedule.enabled: false). Exiting.")
        sys.exit(0)

    scheduler = BlockingScheduler()

    if schedule_cfg["type"] == "cron":
        trigger = CronTrigger.from_crontab(schedule_cfg["cron"])
        print(f"Scheduling pipeline with cron expression: {schedule_cfg['cron']}")
    elif schedule_cfg["type"] == "interval":
        trigger = IntervalTrigger(minutes=schedule_cfg["interval_minutes"])
        print(f"Scheduling pipeline every {schedule_cfg['interval_minutes']} minutes")
    else:
        raise ValueError(f"Unknown schedule type: {schedule_cfg['type']}")

    scheduler.add_job(run_pipeline_job, trigger, args=[config_path], id="etl_pipeline_job")

    def shutdown(signum, frame):
        print("\nShutting down scheduler gracefully...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print("Scheduler started. Press Ctrl+C to stop.")
    scheduler.start()


if __name__ == "__main__":
    start_scheduler()
