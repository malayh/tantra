class TantraError(Exception): ...


class SessionNotFound(TantraError): ...


class SessionExists(TantraError): ...


class SessionBusy(TantraError): ...


class CorruptLog(TantraError): ...


class InvalidCommandReuse(TantraError): ...


class WriterReplaced(TantraError): ...


class WriterRequired(TantraError): ...


class LeaseLost(TantraError): ...


class CoordinatorUnavailable(TantraError): ...


class ModelChangeBusy(TantraError): ...


class CommandTimeout(TantraError):
    def __init__(self, message: str, *, command_id: object | None = None) -> None:
        super().__init__(message)
        self.command_id = command_id


class RemoteExecutionError(TantraError): ...


class AskExpired(TantraError): ...


class MaxDepthExceeded(TantraError): ...


class ProviderError(TantraError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retryable: bool | None = None,
        context_overflow: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.context_overflow = context_overflow
