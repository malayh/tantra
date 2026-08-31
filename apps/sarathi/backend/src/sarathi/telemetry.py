import logging
import os
from functools import cache
from typing import Any

from fastapi import FastAPI

from sarathi.config import get_settings
from tantra.telemetry import Telemetry

_OTEL_VARS = (
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_RESOURCE_ATTRIBUTES",
    "OTEL_SERVICE_NAME",
)

_logger_provider: Any = None


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


def setup_instrumentation(app: FastAPI) -> None:
    global _logger_provider
    if get_telemetry() is None:
        return
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.resources import Resource

    from sarathi.db import get_engine

    _logger_provider = LoggerProvider(resource=Resource.create())
    _logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(_logger_provider)
    handler = LoggingHandler(logger_provider=_logger_provider)
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    for name in ("uvicorn", "uvicorn.access"):
        logging.getLogger(name).addHandler(handler)
    FastAPIInstrumentor.instrument_app(app, excluded_urls="api/health")
    SQLAlchemyInstrumentor().instrument(engine=get_engine().sync_engine)


def shutdown_telemetry() -> None:
    telemetry = get_telemetry()
    if telemetry is not None:
        telemetry.shutdown()
    if _logger_provider is not None:
        _logger_provider.shutdown()
