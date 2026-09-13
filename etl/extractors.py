"""
Extractors: pull raw data from heterogeneous sources into pandas
DataFrames using a uniform interface. Add a new source type by
subclassing BaseExtractor and registering it in EXTRACTOR_REGISTRY.
"""
import glob
from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_exponential

from etl.utils.state import StateStore


class BaseExtractor(ABC):
    def __init__(self, name: str, config: Dict[str, Any], logger, state: StateStore):
        self.name = name
        self.config = config
        self.logger = logger
        self.state = state

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        ...


class CSVExtractor(BaseExtractor):
    """Handles CSV files, including glob patterns for multi-file loads."""

    def extract(self) -> pd.DataFrame:
        path_pattern = self.config["path"]
        options = self.config.get("options", {})
        files = sorted(glob.glob(path_pattern))

        if not files:
            self.logger.warning(f"[{self.name}] No files matched pattern: {path_pattern}")
            return pd.DataFrame()

        frames = []
        for f in files:
            self.logger.info(f"[{self.name}] Reading {f}")
            frames.append(pd.read_csv(f, **options))

        df = pd.concat(frames, ignore_index=True)
        self.logger.info(f"[{self.name}] Extracted {len(df)} rows from {len(files)} file(s)")
        return df


class ExcelExtractor(BaseExtractor):
    def extract(self) -> pd.DataFrame:
        path = self.config["path"]
        options = self.config.get("options", {})
        df = pd.read_excel(path, **options)
        self.logger.info(f"[{self.name}] Extracted {len(df)} rows from {path}")
        return df


class SQLExtractor(BaseExtractor):
    """
    Handles arbitrary SQL sources via SQLAlchemy. Supports simple
    incremental extraction using a cursor column (e.g. updated_at)
    tracked in the state store between runs.
    """

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def extract(self) -> pd.DataFrame:
        conn_str = self.config["connection_string"]
        query = self.config["query"]
        incremental_cfg = self.config.get("incremental", {})

        engine = create_engine(conn_str)
        params = {}

        if incremental_cfg.get("enabled"):
            state_key = incremental_cfg["state_key"]
            since = self.state.get(state_key, "1970-01-01T00:00:00")
            params["since"] = since
            self.logger.info(f"[{self.name}] Incremental extract since {since}")

        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params)

        if incremental_cfg.get("enabled") and not df.empty:
            cursor_field = incremental_cfg["cursor_field"]
            if cursor_field in df.columns:
                new_max = df[cursor_field].max()
                self.state.set(incremental_cfg["state_key"], str(new_max))

        self.logger.info(f"[{self.name}] Extracted {len(df)} rows from SQL source")
        return df


class APIExtractor(BaseExtractor):
    """
    Handles REST API sources, including simple offset/page-based
    pagination and dot-path extraction into the JSON payload.
    """

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def _fetch_page(self, url: str, method: str, params: dict, headers: dict) -> dict:
        resp = requests.request(method=method, url=url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _resolve_path(payload: Any, dot_path: str) -> Any:
        if not dot_path:
            return payload
        node = payload
        for part in dot_path.split("."):
            node = node[part]
        return node

    def extract(self) -> pd.DataFrame:
        url = self.config["url"]
        method = self.config.get("method", "GET")
        params = dict(self.config.get("params", {}))
        headers = self.config.get("headers", {})
        response_path = self.config.get("response_path", "")
        pagination = self.config.get("pagination", {})

        all_records = []

        if not pagination.get("enabled"):
            payload = self._fetch_page(url, method, params, headers)
            records = self._resolve_path(payload, response_path)
            if isinstance(records, dict):
                # e.g. {"USD": 1.0, "EUR": 0.9} style payloads -> long format
                records = [{"key": k, "value": v} for k, v in records.items()]
            all_records.extend(records)
        else:
            page_param = pagination.get("page_param", "page")
            page = pagination.get("start_page", 1)
            max_pages = pagination.get("max_pages", 50)
            while page <= max_pages:
                params[page_param] = page
                payload = self._fetch_page(url, method, params, headers)
                records = self._resolve_path(payload, response_path)
                if not records:
                    break
                all_records.extend(records)
                page += 1

        df = pd.DataFrame(all_records)
        self.logger.info(f"[{self.name}] Extracted {len(df)} records from API")
        return df


EXTRACTOR_REGISTRY = {
    "csv": CSVExtractor,
    "excel": ExcelExtractor,
    "sql": SQLExtractor,
    "api": APIExtractor,
}


def get_extractor(source_type: str):
    if source_type not in EXTRACTOR_REGISTRY:
        raise ValueError(
            f"Unknown source type '{source_type}'. "
            f"Available: {list(EXTRACTOR_REGISTRY.keys())}"
        )
    return EXTRACTOR_REGISTRY[source_type]
