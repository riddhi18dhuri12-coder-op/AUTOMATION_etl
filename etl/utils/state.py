"""
Tiny JSON-backed state store used for incremental extraction
(e.g. remembering the last 'updated_at' pulled from a source so
the next run only fetches new/changed rows).
"""
import json
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, path: str = "./data/state.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            with open(self.path, "r") as f:
                self._state = json.load(f)
        else:
            self._state = {}

    def get(self, key: str, default: Any = None) -> Any:
        return self._state.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._state[key] = value
        self._flush()

    def _flush(self) -> None:
        with open(self.path, "w") as f:
            json.dump(self._state, f, indent=2, default=str)
