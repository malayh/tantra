# Install

The distribution name is `tantra-harness`; the Python import is `tantra`.

| Command | Capability |
|---|---|
| `pip install tantra-harness` | Core runtime and shell tools |
| `pip install "tantra-harness[web]"` | Web search and fetch |
| `pip install "tantra-harness[doc]"` | PDF and Word reading |
| `pip install "tantra-harness[postgres]"` | PostgreSQL store driver |
| `pip install "tantra-harness[telemetry]"` | OpenTelemetry implementation |

Do not co-install the unrelated PyPI distribution named `tantra`; it uses the same import name.

Python 3.13 or newer is required. `SQLiteStore` and `PostgresStore` require `await store.setup()` before the Runtime uses them.
