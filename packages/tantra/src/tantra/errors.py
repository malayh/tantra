class TantraError(Exception): ...


class SessionNotFound(TantraError): ...


class SessionExists(TantraError): ...


class CorruptLog(TantraError): ...


class InvalidCommandReuse(TantraError): ...


class WriterReplaced(TantraError): ...


class WriterRequired(TantraError): ...


class AskExpired(TantraError): ...


class MaxDepthExceeded(TantraError): ...


class ProviderError(TantraError):
    def __init__(self, message: str, *, status_code: int | None = None, retryable: bool | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
