# Telemetry

`Tracer` is the dependency-free protocol. `NullTracer` is the default. `tantra.telemetry.Telemetry` is the OpenTelemetry implementation installed by `tantra-harness[telemetry]`.

Pass a tracer with `Runtime(telemetry=...)`. Spans cover actor turns, provider generations, tools, compaction, outcomes, and token usage. Content attributes are disabled unless explicitly enabled.
