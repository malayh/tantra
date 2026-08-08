class TantraError(Exception): ...


class SeqConflict(TantraError): ...


class SessionNotFound(TantraError): ...


class SessionExists(TantraError): ...


class CorruptLog(TantraError): ...


class SessionBusy(TantraError): ...


class TurnIncomplete(TantraError): ...


class ProviderError(TantraError):
    def __init__(self, message: str, *, status_code: int | None = None, retryable: bool | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
