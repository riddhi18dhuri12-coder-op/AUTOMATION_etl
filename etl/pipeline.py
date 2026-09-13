"""
Pipeline orchestrator: reads config, runs extract -> transform ->
load for each configured source, and reports a summary. Designed
to be called directly (main.py) or on a schedule (scheduler.py).
"""
import time
import traceback
from datetime import datetime
from typing import Any, Dict

import requests

from etl.extractors import get_extractor
from etl.loaders import get_loader
from etl.transformers import apply_transformations
from etl.utils.config_loader import load_config
from etl.utils.logger import get_logger
from etl.utils.state import StateStore


class ETLPipeline:
    def __init__(self, config_path: str = "./config/config.yaml"):
        self.config = load_config(config_path)
        self.run_id = datetime.now().strftime(
            self.config["pipeline"].get("run_id_format", "%Y%m%d_%H%M%S")
        )
        self.logger = get_logger(
            name=self.config["pipeline"]["name"],
            log_dir="./logs",
            run_id=self.run_id,
        )
        self.state = StateStore()
        self.results: Dict[str, Any] = {}

    def run(self) -> Dict[str, Any]:
        start = time.time()
        self.logger.info(f"=== Starting pipeline run: {self.run_id} ===")

        fail_fast = self.config["pipeline"].get("fail_fast", False)
        destination_type = self.config["destination"]["type"]
        loader = get_loader(destination_type, self.config["destination"], self.logger)

        for source_cfg in self.config["sources"]:
            name = source_cfg["name"]
            try:
                self._run_source(name, source_cfg, loader)
                self.results[name] = "success"
            except Exception as e:
                self.results[name] = f"failed: {e}"
                self.logger.error(f"[{name}] FAILED: {e}\n{traceback.format_exc()}")
                if fail_fast:
                    self._notify_failure(name, e)
                    raise
                self._notify_failure(name, e)

        elapsed = round(time.time() - start, 2)
        self.logger.info(f"=== Pipeline run {self.run_id} complete in {elapsed}s ===")
        self.logger.info(f"Results: {self.results}")

        if all(v == "success" for v in self.results.values()):
            self._notify_success()

        return self.results

    def _run_source(self, name: str, source_cfg: Dict[str, Any], loader) -> None:
        self.logger.info(f"--- Processing source: {name} ---")

        # EXTRACT
        extractor_cls = get_extractor(source_cfg["type"])
        extractor = extractor_cls(name, source_cfg, self.logger, self.state)
        df = extractor.extract()

        # TRANSFORM
        steps = self.config.get("transformations", {}).get(name, [])
        df = apply_transformations(df, steps, self.logger)

        # LOAD
        loader.load(name, df)

    def _notify_failure(self, source_name: str, error: Exception) -> None:
        cfg = self.config.get("notifications", {}).get("on_failure", {})
        if not cfg.get("enabled"):
            return
        webhook = cfg.get("webhook_url")
        if not webhook:
            return
        try:
            requests.post(webhook, json={
                "run_id": self.run_id,
                "status": "failure",
                "source": source_name,
                "error": str(error),
            }, timeout=10)
        except Exception as e:
            self.logger.warning(f"Failed to send failure notification: {e}")

    def _notify_success(self) -> None:
        cfg = self.config.get("notifications", {}).get("on_success", {})
        if not cfg.get("enabled"):
            return
        webhook = cfg.get("webhook_url")
        if not webhook:
            return
        try:
            requests.post(webhook, json={
                "run_id": self.run_id,
                "status": "success",
                "results": self.results,
            }, timeout=10)
        except Exception as e:
            self.logger.warning(f"Failed to send success notification: {e}")
