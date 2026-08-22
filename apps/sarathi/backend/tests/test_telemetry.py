import os
from collections.abc import Iterator

import pytest

from sarathi.agent import Researcher, Sarathi, _wire_tools, make_harness
from sarathi.config import get_settings
from sarathi.telemetry import get_telemetry, shutdown_telemetry
from tantra.telemetry import Telemetry

ENDPOINT = "http://ingest.example.invalid"


@pytest.fixture
def unwired() -> Iterator[None]:
    yield
    if get_telemetry.cache_info().currsize:
        shutdown_telemetry()
    Sarathi.tools = []
    Researcher.tools = []
    _wire_tools.cache_clear()
    get_telemetry.cache_clear()
    get_settings.cache_clear()


@pytest.fixture
def uninstalled(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    installed: list[object] = []
    monkeypatch.setattr("tantra.telemetry.trace.set_tracer_provider", installed.append)
    return installed


def test_telemetry_is_off_without_an_endpoint(monkeypatch: pytest.MonkeyPatch, unwired: None) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    get_settings.cache_clear()
    get_telemetry.cache_clear()

    assert get_telemetry() is None
    shutdown_telemetry()


def test_an_endpoint_wires_an_env_configured_tracer_onto_the_harness(
    monkeypatch: pytest.MonkeyPatch, uninstalled: list[object], unwired: None
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    monkeypatch.setenv("TELEMETRY_CAPTURE_CONTENT", "true")
    get_settings.cache_clear()
    get_telemetry.cache_clear()
    _wire_tools.cache_clear()

    configured = get_telemetry()

    assert isinstance(configured, Telemetry)
    assert configured.capture_content is True
    assert len(uninstalled) == 1
    assert configured._provider.resource.attributes["service.name"] == "sarathi"
    assert make_harness().tracer is configured


def test_dotenv_settings_are_bridged_into_the_environment(
    monkeypatch: pytest.MonkeyPatch, uninstalled: list[object], unwired: None
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_HEADERS", raising=False)
    monkeypatch.delenv("OTEL_RESOURCE_ATTRIBUTES", raising=False)
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    monkeypatch.setenv("TELEMETRY_CAPTURE_CONTENT", "false")
    get_settings.cache_clear()
    get_telemetry.cache_clear()
    settings = get_settings()
    monkeypatch.setattr(settings, "OTEL_EXPORTER_OTLP_HEADERS", "x-token=secret")
    monkeypatch.setattr(settings, "OTEL_RESOURCE_ATTRIBUTES", "service.environment=test")
    monkeypatch.setattr(settings, "OTEL_SERVICE_NAME", "from-dotenv")

    configured = get_telemetry()

    assert isinstance(configured, Telemetry)
    assert configured.capture_content is False
    assert os.environ["OTEL_EXPORTER_OTLP_HEADERS"] == "x-token=secret"
    assert os.environ["OTEL_RESOURCE_ATTRIBUTES"] == "service.environment=test"
    assert configured._provider.resource.attributes["service.name"] == "from-dotenv"
    assert configured._provider.resource.attributes["service.environment"] == "test"


def test_a_real_environment_variable_beats_the_dotenv_value(
    monkeypatch: pytest.MonkeyPatch, uninstalled: list[object], unwired: None
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", ENDPOINT)
    monkeypatch.setenv("OTEL_SERVICE_NAME", "from-environ")
    get_settings.cache_clear()
    get_telemetry.cache_clear()
    settings = get_settings()
    monkeypatch.setattr(settings, "OTEL_SERVICE_NAME", "from-dotenv")

    configured = get_telemetry()

    assert isinstance(configured, Telemetry)
    assert os.environ["OTEL_SERVICE_NAME"] == "from-environ"
    assert configured._provider.resource.attributes["service.name"] == "from-environ"
