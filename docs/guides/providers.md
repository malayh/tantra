# Providers

A provider implements `limits(model)` and an async `stream(SampleRequest)` method. `Agent.model` wins over the root session model, which wins over `Runtime.default_model`. Children inherit the root session model unless their declaration overrides it.

Provider deltas are appended before subscribers see them. A complete sample ends with `StreamEnd`, after which the turn engine executes any tool batch.

Configure retry policy on the Runtime:

```python
runtime = Runtime(
    provider,
    store,
    [Bot],
    retry=RetryConfig(max_attempts=3, base_delay=0.5, max_delay=8.0),
)
```

Retryable provider failures include explicit retryable errors, HTTP 429, and server errors. Exhaustion records `TurnFailed`; it does not retry the accepted input in another process.

`OpenAICompatible` is the production adapter and `FakeProvider` is the deterministic test adapter.

## Model limits

`limits(model)` may return `ModelLimits` directly or an awaitable. The turn engine resolves either form before preparing the request.

`OpenAICompatible` accepts authoritative per-model limits:

```python
provider = OpenAICompatible(
    base_url,
    api_key,
    limits={
        "vendor/model": ModelLimits(context_window=200_000, max_output=8_192),
    },
)
```

An explicit entry bypasses discovery. Otherwise the provider requests the model catalogue lazily and at most once per provider instance. It reads positive `context_length` and `max_completion_tokens` fields either at the model's top level or under `top_provider`. Unknown models, standard catalogues without those extensions, malformed fields, and catalogue failures fall back independently to `128_000` context tokens and `4_096` output tokens.

Limit discovery never blocks a turn with a catalogue error. Configure explicit values when a proxy reports incomplete or inaccurate metadata.
