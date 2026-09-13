"""
CLI entry point.

Usage:
    python main.py run                         # run the pipeline once, now
    python main.py schedule                    # start the automated scheduler (long-running)
    python main.py run --config path/to.yaml   # use a specific config file
"""
import argparse
import sys

from etl.pipeline import ETLPipeline
from etl.scheduler import start_scheduler


def main():
    parser = argparse.ArgumentParser(description="ETL Pipeline CLI")
    parser.add_argument(
        "command", choices=["run", "schedule"],
        help="'run' executes the pipeline once; 'schedule' starts automated recurring runs"
    )
    parser.add_argument(
        "--config", default="./config/config.yaml",
        help="Path to config YAML (default: ./config/config.yaml)"
    )
    args = parser.parse_args()

    if args.command == "run":
        pipeline = ETLPipeline(args.config)
        results = pipeline.run()
        failed = [k for k, v in results.items() if v != "success"]
        if failed:
            print(f"\nPipeline completed with failures in: {failed}")
            sys.exit(1)
        print("\nPipeline completed successfully.")

    elif args.command == "schedule":
        start_scheduler(args.config)


if __name__ == "__main__":
    main()
