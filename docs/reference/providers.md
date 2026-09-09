# Providers

A `Provider` supplies `limits(model)` and streams `ProviderEvent` values for `SampleRequest`. Text, reasoning, tool-call deltas, complete tool calls, and `StreamEnd` form the streaming protocol.

`OpenAICompatible` maps OpenAI-compatible APIs. `FakeProvider` replays deterministic `Sample` objects for tests. Runtime owns model precedence and retry handling.
