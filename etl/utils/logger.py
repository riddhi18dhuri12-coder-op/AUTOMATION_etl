"""
Centralized logging setup for the ETL pipeline.
Logs to both console and a rotating file, tagged with run_id so
individual pipeline runs can be traced end-to-end.
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def get_logger(name: str, log_dir: str = "./logs", run_id: str = "") -> logging.Logger:
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured (avoid duplicate handlers on repeated calls)
        return logger

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt=f"%(asctime)s | %(levelname)-8s | run={run_id} | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(
        filename=f"{log_dir}/etl_pipeline.log",
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
