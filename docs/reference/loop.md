# Loop & events flow

The turn loop itself (`tantra.loop.TurnLoop`) is internal — it is built by `Harness` and has no supported constructor contract. Its public faces are the event envelope it yields, the retry policy it takes, and the adapter that drains it.

```python
from tantra import Emitted, RetryConfig, collect
```

## `Emitted`

Every item yielded by `Harness.run`, `Harness.resume` and `Harness.replay`.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `session_id` | `str` | — | Session the event belongs to. Child-session events keep the **child's** id. |
| `depth` | `int` | `0` | Depth of that session. Root = 0, its children = 1. |
| `seq` | `int \| None` | `None` | Store sequence number. `None` means the event was never persisted — a live delta. |
| `event` | `SessionEvent \| TextDelta \| ReasoningDelta \| ToolCallDelta` | — | The payload. |

Two things follow from the shape:

- **`seq is None` is the live/persisted test.** `TextDelta`, `ReasoningDelta` and `ToolCallDelta` stream through for rendering and are never written to the log; everything with a `seq` is in the store and comes back from `replay`.
- **A parent turn forwards its children's events live**, tagged with their own `session_id` and `depth`. Filter on `emitted.session_id == sid` to show only the session you asked for.

```python
async for emitted in harness.run(session.id, "how is p99?"):
    if isinstance(emitted.event, TextDelta):
        print(emitted.event.text, end="", flush=True)
```

## `RetryConfig`

Frozen dataclass. Governs **provider sampling only** — tool failures are never retried; they become error results the model sees.

| Field | Default | Meaning |
|---|---|---|
| `max_attempts` | `3` | Total attempts per sample, including the first. |
| `base_delay` | `0.5` | Seconds before the second attempt. |
| `max_delay` | `8.0` | Ceiling on the backoff. |

After the attempt with 0-based index *n* fails, the loop sleeps `min(base_delay * 2 ** n, max_delay)` before the next one — so the defaults give 0.5s, then 1.0s, and the third failure gives up.

A `ProviderError` is retried when `retryable is True`, or when `status_code` is 429 or ≥ 500. Anything else — and the last attempt — fails the turn with `TurnFailed`. Note that a stream ending without a `StreamEnd` raises a `ProviderError` carrying neither flag, so it is **not** retryable: the first occurrence fails the turn.

`DEFAULT_RETRY = RetryConfig()` is what `Harness` uses when you pass nothing. Pass your own with `Harness(retry=RetryConfig(max_attempts=5))`.

## `collect(stream) -> list[Emitted]`

```python
from tantra import collect

events = await collect(harness.run(sid, "go"))
```

Drains an `Emitted` stream into a list. It exists because **the turn advances only while someone consumes the iterator** — see [Sharp edges](../sharp-edges.md). Use it in tests and in fire-and-forget callers; iterate directly when you want to stream.

Also importable as `from tantra.adapters.collect import collect`.

## See also

- [Events](events.md) — every persisted event class.
- [Harness](harness.md) — the generators that yield `Emitted`.
- [Providers](providers.md) — where the deltas come from.
