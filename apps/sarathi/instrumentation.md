# Osuite instrumentation (sarathi backend)

## Required environment variables

Set wherever the backend runs (local `.env`, compose already forwards it via `env_file: .env`, k8s/deploy targets need them added manually):

```bash
OTEL_SERVICE_NAME="sarathi"
OTEL_RESOURCE_ATTRIBUTES="service.environment=production"
OTEL_EXPORTER_OTLP_ENDPOINT="https://ingest.<region>.osuite.io:443"
OTEL_EXPORTER_OTLP_HEADERS="x-osuite-ingest-token=<ingestkey>"
```

- `<region>` and `<ingestkey>` come from Osuite — telemetry is disabled until the endpoint is set.
- `service.environment` separates prod/staging/dev in Osuite (Osuite-specific key; not `deployment.environment`).
- Optional: `TELEMETRY_CAPTURE_CONTENT=true` includes prompts/completions/tool args in agent spans.

## What was changed

- Installed `opentelemetry-instrumentation-fastapi` + `opentelemetry-instrumentation-sqlalchemy` (SDK and OTLP HTTP exporter were already present via `tantra-harness[telemetry]`).
- `backend/src/sarathi/telemetry.py` — added `setup_instrumentation()`: OTLP log export bridged into stdlib `logging` (root + uvicorn loggers, trace IDs auto-attached, stdout logging unchanged), FastAPI request tracing (`/api/health` excluded), SQLAlchemy query spans; shutdown flushes logs.
- `backend/src/sarathi/main.py` — `create_app()` calls `setup_instrumentation(app)`; lifespan shuts telemetry down.
- `.env.example` — OTEL block updated to Osuite placeholders.
- Agent/LLM/tool spans were already emitted by `tantra.telemetry.Telemetry` — untouched.

## How to verify

1. Fill in endpoint + token in `.env`.
2. Start the stack (`docker compose up` or `just runserver`) and hit a few endpoints / run a chat.
3. In Osuite, filter by the service name under Traces (HTTP + `chat`/`execute_tool` spans) and Logs.
4. Nothing appearing? Check backend stderr for OTLP export errors, and confirm the token and endpoint region.
