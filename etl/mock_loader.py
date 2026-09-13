"""
Mock loader used only for local testing/demo purposes — writes to
CSV in the staging dir instead of a real warehouse. Not part of
the production LOADER_REGISTRY; swap in real Snowflake/BigQuery
config for actual runs.
"""
from pathlib import Path

import pandas as pd


class MockLoader:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def load(self, source_name: str, df: pd.DataFrame) -> None:
        out_dir = Path("./data/staging")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{source_name}.csv"
        df.to_csv(out_path, index=False)
        self.logger.info(f"[{source_name}] (MOCK) Wrote {len(df)} rows to {out_path}")
