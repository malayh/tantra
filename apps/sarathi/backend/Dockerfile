FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/tantra ./packages/tantra
COPY apps/agni/pyproject.toml ./apps/agni/pyproject.toml
COPY apps/sarathi/backend ./apps/sarathi/backend

RUN uv sync --frozen --package sarathi --no-dev

WORKDIR /app/apps/sarathi/backend

EXPOSE 8000

CMD ["uv", "run", "--no-sync", "uvicorn", "sarathi.main:app", "--host", "0.0.0.0", "--port", "8000"]
