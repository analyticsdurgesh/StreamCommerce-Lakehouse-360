from __future__ import annotations

import os
from pathlib import Path
from string import Template
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_config(environment: str | None = None) -> dict[str, Any]:
    env = environment or os.getenv("ENVIRONMENT", "local")
    path = PROJECT_ROOT / "configs" / f"{env}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    raw = Template(path.read_text()).safe_substitute(os.environ)
    return yaml.safe_load(raw)


def path_for(layer: str, table: str | None = None, environment: str | None = None) -> Path | str:
    config = load_config(environment)
    base = config["paths"][layer]
    return str(Path(base) / table) if table else base
