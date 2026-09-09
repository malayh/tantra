# Telemetry

Install `tantra-harness[telemetry]`, then pass a `Telemetry` tracer to Runtime.

```python
from tantra.telemetry import Telemetry

runtime = Runtime(
    provider,
    store,
    [Researcher],
    telemetry=Telemetry.from_env(capture_content=False),
)
```

`Telemetry.from_env()` returns `None` when no OTLP trace endpoint is configured, so the same construction supports traced and untraced deployments. Tantra does not configure exporters unless this helper is called.

Turn, generation, and tool spans record outcomes and token usage. Independent actor turns are separate observations. Content capture is opt-in because prompts, tool arguments, and results may contain sensitive data.
