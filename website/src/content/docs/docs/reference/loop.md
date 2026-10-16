# Turn engine and retry

`TurnEngine` is internal. Runtime activates it for one accepted actor input and persists its events before publication.

`RetryConfig` controls provider sampling retries. `LoggedEvent` is the public streaming envelope, and `TurnResult` is the attached terminal result. Event consumption does not drive execution.
