# Errors

```python
from tantra import ProviderError, SeqConflict, SessionBusy, SessionNotFound, TantraError, TurnIncomplete
from tantra.errors import CorruptLog, SessionExists
```

Every exception the library raises derives from `TantraError`, so one `except TantraError` catches the lot.

```text
Exception
└── TantraError
    ├── SeqConflict
    ├── SessionNotFound
    ├── SessionExists
    ├── CorruptLog
    ├── SessionBusy
    ├── TurnIncomplete
    └── ProviderError
```

| Exception | Raised when |
|---|---|
| `TantraError` | Base class, and the catch-all for misconfiguration: unknown agent or skill, a non-`Tool` in `Agent.tools`, an invalid permission value, `max_steps < 1`, duplicate agent or tool names, `max_depth` exceeded, `ctx.spawn` outside a tool call, a resume with a mismatched `ask_id`/`response`, a lease lost mid-turn. |
| `SeqConflict` | A store `append` was given an `expect_seq` that is not the session's current last seq — another writer got there first. |
| `SessionNotFound` | The session id is unknown to the store. Raised by `run`, `resume`, `cancel`, `replay`, and by store `append` / `put_header` / `patch_header` / `acquire_lease`. |
| `SessionExists` | `Store.create` was given an id that is already taken. |
| `CorruptLog` | A store read hit a stored event it cannot decode. Stores raise rather than skip: a gap in the suffix would silently rewrite history. |
| `SessionBusy` | `run` or `resume` could not take the single-writer lease — another process holds a live one. Carries `.sid`. |
| `TurnIncomplete` | `run` was called on a session whose previous turn never reached `TurnCompleted` / `TurnFailed`. Call `resume(sid)` instead. |
| `ProviderError` | The model transport failed. Carries `.status_code: int \| None` and `.retryable: bool \| None`, which drive [retry](loop.md#retryconfig). A turn that exhausts its attempts ends in `TurnFailed`, not an exception. |

!!! note "Two are not re-exported at the top level"
    `SessionExists` and `CorruptLog` are store-facing and live only in `tantra.errors`. Import them from there; the rest are available directly from `tantra`.

## Where errors surface

`run`, `resume` and `replay` are async generators. Their argument checks — `SessionNotFound`, `SessionBusy`, `TurnIncomplete` — run inside the generator body, so they are raised on the **first iteration**, not at the call. See [Sharp edges](../sharp-edges.md).

Exceptions raised *inside a tool* never propagate: the loop catches them and writes `str(exc)` as an error result the model reads. See [Tools](tools.md).
