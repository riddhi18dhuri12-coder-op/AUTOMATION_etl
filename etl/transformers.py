"""
Transformers: a small library of composable, config-driven
DataFrame operations. Each transform step in config.yaml maps to
a function here via the OP_REGISTRY, so new logic (e.g. business
rules, joins, enrichment) can be added without touching pipeline
control flow.
"""
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd


def rename_columns(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    return df.rename(columns=step["mapping"])


def drop_duplicates(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=step.get("subset"))
    logger.info(f"drop_duplicates: {before - len(df)} rows removed")
    return df


def drop_nulls(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    before = len(df)
    df = df.dropna(subset=step.get("subset"))
    logger.info(f"drop_nulls: {before - len(df)} rows removed")
    return df


def cast_types(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    for col, dtype in step["columns"].items():
        if col in df.columns:
            df[col] = df[col].astype(dtype, errors="ignore")
    return df


def filter_rows(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    before = len(df)
    df = df.query(step["condition"])
    logger.info(f"filter_rows ('{step['condition']}'): {before - len(df)} rows removed")
    return df


def add_column(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    value = step["value"]
    if value == "{{now}}":
        value = datetime.now(timezone.utc).isoformat()
    df[step["column"]] = value
    return df


def standardize_text(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    mode = step.get("mode", "upper")
    for col in step["columns"]:
        if col not in df.columns:
            continue
        if mode == "upper":
            df[col] = df[col].str.upper()
        elif mode == "lower":
            df[col] = df[col].str.lower()
        elif mode == "title":
            df[col] = df[col].str.title()
        df[col] = df[col].str.strip()
    return df


def flatten_json(df: pd.DataFrame, step: Dict[str, Any], logger) -> pd.DataFrame:
    return pd.json_normalize(df.to_dict(orient="records"))


OP_REGISTRY = {
    "rename_columns": rename_columns,
    "drop_duplicates": drop_duplicates,
    "drop_nulls": drop_nulls,
    "cast_types": cast_types,
    "filter_rows": filter_rows,
    "add_column": add_column,
    "standardize_text": standardize_text,
    "flatten_json": flatten_json,
}


def apply_transformations(df: pd.DataFrame, steps: list, logger) -> pd.DataFrame:
    for step in steps or []:
        op = step.get("op")
        if op not in OP_REGISTRY:
            logger.warning(f"Unknown transform op '{op}', skipping")
            continue
        if df.empty:
            continue
        df = OP_REGISTRY[op](df, step, logger)
    return df
