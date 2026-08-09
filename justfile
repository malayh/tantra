default:
    @just --list

sync:
    uv sync --all-packages

test *args:
    uv run pytest {{args}}

stress *args:
    uv run pytest stress {{args}}

lint:
    uv run ruff check .
    uv run ruff format --check .

fmt:
    uv run ruff format .
