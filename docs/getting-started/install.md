# Install

Requires **Python 3.13 or newer**.

The install name is `tantra-harness`; the import name is `tantra`.

## Extras matrix

| Command | Adds |
|---|---|
| `pip install tantra-harness` | core + `tantra.extratools.shell` (`bash`, `ShellGuard`) |
| `pip install "tantra-harness[web]"` | `web_search` (Brave) + `web_fetch` |
| `pip install "tantra-harness[doc]"` | `read_doc` for PDF / docx |
| `pip install "tantra-harness[postgres]"` | the psycopg driver that `PostgresStore` needs at use time; the class itself imports without it |
| `pip install "tantra-harness[web,doc]"` | combine freely |

The base install pulls only `pydantic`, `httpx` and `openai`. The shell tools are stdlib and need no extra.

!!! warning "Import-name collision"
    The unrelated PyPI project `tantra` also installs an `import tantra`. Installing both into one environment clobbers the import silently. Do not co-install them.

## Stores need setup

`SQLiteStore` and `PostgresStore` create their schema in `await store.setup()`. Call it once before use — nothing in `Harness` calls it for you.

```python
store = SQLiteStore("sessions.db")
await store.setup()
```

`MemoryStore` and `FileSystemStore` work without it.

## Next

[Quickstart](quickstart.md) — a working turn in about thirty lines.
