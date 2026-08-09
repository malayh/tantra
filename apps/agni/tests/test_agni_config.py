from __future__ import annotations

import sys
from pathlib import Path

import pytest

from agni.config import DEFAULT_ENDPOINT, ConfigError, load_config
from agni.main import main

BASE = {"OPENAI_API_KEY": "sk-test", "AGNI_MODELS": "a/one, b/two "}


def test_the_config_reads_every_setting_from_the_environment() -> None:
    config = load_config({**BASE, "OPENAI_ENDPOINT": "https://router.test/v1", "AGNI_HOME": "/tmp/agni-test"})

    assert config.api_key == "sk-test"
    assert config.endpoint == "https://router.test/v1"
    assert config.models == ("a/one", "b/two")
    assert config.model == "a/one"
    assert config.home == Path("/tmp/agni-test")
    assert config.database == Path("/tmp/agni-test/data/agni.db")
    assert config.memory_root == Path("/tmp/agni-test/memory")


def test_the_endpoint_and_home_fall_back_to_their_defaults() -> None:
    config = load_config(BASE)

    assert config.endpoint == DEFAULT_ENDPOINT
    assert config.home == Path.home() / ".agni"


def test_agni_home_expands_a_tilde() -> None:
    config = load_config({**BASE, "AGNI_HOME": "~/scratch/agni"})

    assert config.home == Path.home() / "scratch" / "agni"


def test_a_missing_api_key_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="OPENAI_API_KEY"):
        load_config({"AGNI_MODELS": "a/one"})


def test_an_empty_model_list_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="AGNI_MODELS"):
        load_config({"OPENAI_API_KEY": "sk-test", "AGNI_MODELS": " , "})


def test_main_reports_a_missing_api_key_on_stderr_and_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(sys, "argv", ["agni"])
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("AGNI_MODELS", "a/one")

    assert main() == 1
    assert "OPENAI_API_KEY" in capsys.readouterr().err
