import os
from functools import cache

from sarathi.config import get_settings
from tantra.telemetry import Telemetry

_OTEL_VARS = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
)


@cache
def get_telemetry() -> Telemetry | None:
    settings = get_settings()
    if not settings.OTEL_EXPORTER_OTLP_ENDPOINT.strip():
        return None
    for key in _OTEL_VARS:
        value = getattr(settings, key).strip()
        if value:
            os.environ.setdefault(key, value)
    return Telemetry.from_env(capture_content=settings.TELEMETRY_CAPTURE_CONTENT)


def shutdown_telemetry() -> None:
    telemetry = get_telemetry()
    if telemetry is not None:
        telemetry.shutdown()
