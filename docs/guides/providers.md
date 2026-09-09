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
