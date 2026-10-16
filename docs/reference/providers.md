# Providers

A `Provider` supplies `limits(model)` and streams `ProviderEvent` values for `SampleRequest`. Text, reasoning, tool-call deltas, complete tool calls, and `StreamEnd` form the streaming protocol.

Tool call IDs are unique within a session. Before journaling a sample, the turn engine rewrites any complete tool call whose ID was already used in the turn or the loaded history to a deterministic `call_<24 hex>`; IDs without a collision are kept byte for byte. Repeats come from providers that reuse IDs and from the `OpenAICompatible` fallback `call_{index}` for calls without one. `ToolCallDelta` and the sample's telemetry span keep the raw provider IDs.

`Provider.limits(model) -> ModelLimits | Awaitable[ModelLimits]` supplies `context_window` and `max_output`. Runtime resolves synchronous and asynchronous implementations.

`OpenAICompatible` maps OpenAI-compatible APIs. Its limit precedence is:

1. The constructor's explicit `limits` entry for the model.
2. Lazily cached model-catalogue metadata from positive `context_length` and `max_completion_tokens` fields, either top-level or under `top_provider`.
3. `ModelLimits(context_window=128_000, max_output=4_096)` per missing or invalid field.

Catalogue success and failure are both cached for the provider instance. `FakeProvider` replays deterministic `Sample` objects for tests. Runtime owns model precedence and retry handling.
