# Errors

| Error | Meaning |
|---|---|
| `TantraError` | Base error or invalid configuration/call |
| `SessionNotFound` | Unknown root or actor UUID |
| `InvalidCommandReuse` | One command UUID was reused with different content |
| `WriterRequired` | Mutation attempted without an entered writable connection |
| `WriterReplaced` | A newer writable connection owns the root tree |
| `AskExpired` | The ask is not live in this Runtime |
| `MaxDepthExceeded` | A spawn would exceed `Runtime.max_depth` |
| `SeqConflict` | A low-level optimistic append used a stale sequence |
| `ProviderError` | Provider request failed |

Agent failures represented by terminal journal events return as `TurnResult`. Invalid calls, storage failures, and programming errors raise.
