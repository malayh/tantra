# Errors

| Error | Meaning |
|---|---|
| `TantraError` | Base error or invalid configuration/call |
| `SessionNotFound` | Unknown root or actor UUID |
| `SessionBusy` | Deletion refused because the session tree has active or uncertain work |
| `InvalidCommandReuse` | One command UUID was reused with different content |
| `WriterRequired` | Mutation attempted without an entered writable connection |
| `WriterReplaced` | A newer writable connection owns the root tree |
| `LeaseLost` | The Runtime lost its fenced execution generation |
| `CoordinatorUnavailable` | Coordinator state cannot be read or changed safely |
| `CommandTimeout` | A remote command reply timed out; the command may have been accepted |
| `RemoteExecutionError` | The owner returned a definitive remote execution or validation failure |
| `AskExpired` | The ask is not live in this Runtime |
| `MaxDepthExceeded` | A spawn would exceed `Runtime.max_depth` |
| `ProviderError` | Provider request failed |

Agent failures represented by terminal journal events return as `TurnResult`. Invalid calls, storage failures, and programming errors raise.
