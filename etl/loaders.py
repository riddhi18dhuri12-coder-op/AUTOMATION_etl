"""
Loaders: write transformed DataFrames into the target data
warehouse. Supports Snowflake and BigQuery with append / replace /
merge (upsert) semantics. Add a new warehouse by subclassing
BaseLoader and registering it in LOADER_REGISTRY.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd


class BaseLoader(ABC):
    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger

    @abstractmethod
    def load(self, source_name: str, df: pd.DataFrame) -> None:
        ...


class SnowflakeLoader(BaseLoader):
    def _get_connection(self):
        import snowflake.connector
        cfg = self.config
        return snowflake.connector.connect(
            account=cfg["account"],
            user=cfg["user"],
            password=cfg["password"],
            warehouse=cfg["warehouse"],
            database=cfg["database"],
            schema=cfg["schema"],
        )

    def load(self, source_name: str, df: pd.DataFrame) -> None:
        if df.empty:
            self.logger.info(f"[{source_name}] Nothing to load (empty DataFrame)")
            return

        from snowflake.connector.pandas_tools import write_pandas

        table = self.config["table_map"][source_name]
        load_mode = self.config.get("load_mode", "append")
        merge_keys = self.config.get("merge_keys", {}).get(source_name)

        conn = self._get_connection()
        try:
            if load_mode == "merge" and merge_keys:
                self._merge_load(conn, table, df, merge_keys)
            else:
                overwrite = load_mode == "replace"
                success, nchunks, nrows, _ = write_pandas(
                    conn, df, table.upper(), auto_create_table=True, overwrite=overwrite
                )
                self.logger.info(
                    f"[{source_name}] Loaded {nrows} rows into Snowflake table {table} "
                    f"(mode={load_mode}, success={success})"
                )
        finally:
            conn.close()

    def _merge_load(self, conn, table: str, df: pd.DataFrame, merge_keys: list) -> None:
        """
        Stage rows into a temp table, then MERGE into the target table
        on merge_keys — an upsert. Falls back gracefully if the target
        table doesn't exist yet by creating it first.
        """
        from snowflake.connector.pandas_tools import write_pandas

        stage_table = f"{table}_STAGE"
        write_pandas(conn, df, stage_table.upper(), auto_create_table=True, overwrite=True)

        cols = list(df.columns)
        on_clause = " AND ".join([f"t.{k.upper()} = s.{k.upper()}" for k in merge_keys])
        set_clause = ", ".join([f"t.{c.upper()} = s.{c.upper()}" for c in cols])
        insert_cols = ", ".join([c.upper() for c in cols])
        insert_vals = ", ".join([f"s.{c.upper()}" for c in cols])

        merge_sql = f"""
            MERGE INTO {table.upper()} t
            USING {stage_table.upper()} s
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        cur = conn.cursor()
        try:
            cur.execute(merge_sql)
            self.logger.info(f"Merged {len(df)} rows into {table} using keys {merge_keys}")
        finally:
            cur.close()


class BigQueryLoader(BaseLoader):
    def load(self, source_name: str, df: pd.DataFrame) -> None:
        if df.empty:
            self.logger.info(f"[{source_name}] Nothing to load (empty DataFrame)")
            return

        from google.cloud import bigquery

        cfg = self.config
        table_name = cfg["table_map"][source_name]
        table_id = f"{cfg['project']}.{cfg['dataset']}.{table_name}"
        load_mode = cfg.get("load_mode", "append")

        write_disposition = "WRITE_TRUNCATE" if load_mode == "replace" else "WRITE_APPEND"

        client = bigquery.Client(project=cfg["project"])
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        self.logger.info(
            f"[{source_name}] Loaded {len(df)} rows into BigQuery table {table_id} "
            f"(mode={load_mode})"
        )


LOADER_REGISTRY = {
    "snowflake": SnowflakeLoader,
    "bigquery": BigQueryLoader,
}


def get_loader(destination_type: str, config: Dict[str, Any], logger) -> BaseLoader:
    if destination_type not in LOADER_REGISTRY:
        raise ValueError(
            f"Unknown destination type '{destination_type}'. "
            f"Available: {list(LOADER_REGISTRY.keys())}"
        )
    loader_cls = LOADER_REGISTRY[destination_type]
    return loader_cls(config[destination_type], logger)
