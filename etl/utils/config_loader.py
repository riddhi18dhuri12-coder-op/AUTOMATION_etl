"""
Loads YAML config and interpolates ${ENV_VAR} placeholders with
values from the environment (or a .env file via python-dotenv).
This keeps secrets out of version control while keeping the
pipeline fully config-driven.
"""
import os
import re
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

_ENV_PATTERN = re.compile(r"\$\{([^}^{]+)\}")


def _interpolate(value: Any) -> Any:
    if isinstance(value, str):
        def replace(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def load_config(path: str = "./config/config.yaml", env_file: str = ".env") -> Dict[str, Any]:
    if os.path.exists(env_file):
        load_dotenv(env_file)

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    return _interpolate(raw)
