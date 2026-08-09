from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

DEFAULT_ENDPOINT = "https://api.openai.com/v1"
DEFAULT_HOME = "~/.agni"


class ConfigError(Exception): ...


@dataclass(frozen=True)
class Config:
    api_key: str
    endpoint: str
    models: tuple[str, ...]
    home: Path

    @property
    def model(self) -> str:
        return self.models[0]

    @property
    def database(self) -> Path:
        return self.home / "data" / "agni.db"

    @property
    def memory_root(self) -> Path:
        return self.home / "memory"


def load_config(env: Mapping[str, str] | None = None) -> Config:
    source = os.environ if env is None else env
    api_key = (source.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise ConfigError("OPENAI_API_KEY is not set")
    models = tuple(name.strip() for name in (source.get("AGNI_MODELS") or "").split(",") if name.strip())
    if not models:
        raise ConfigError("AGNI_MODELS is not set; give it a comma-separated list, the first is the default")
    endpoint = (source.get("OPENAI_ENDPOINT") or "").strip() or DEFAULT_ENDPOINT
    home = Path((source.get("AGNI_HOME") or "").strip() or DEFAULT_HOME).expanduser()
    return Config(api_key=api_key, endpoint=endpoint, models=models, home=home)
