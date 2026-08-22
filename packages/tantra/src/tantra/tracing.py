from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from tantra.context import TurnContext
    from tantra.events import CompactionApplied, ToolCallRequested
    from tantra.providers.base import Provider, SampleRequest, StreamEnd
    from tantra.tools import Tool


class Tracer(Protocol):
    """Span sink for the turn loop. `Harness(telemetry=...)` takes one; the default records nothing.

    Every `start_*` returns an opaque handle the loop hands back to the matching `end_*`; the loop
    never inspects it and `None` is a valid handle. Implementations must be total: a raising tracer
    call would break the turn, so errors belong inside the implementation, not in the loop.
    """

    def start_turn(self, turn: TurnContext, *, resumed: bool, ask_id: str | None, parent: Any) -> Any:
        """Open the span covering one `run()` or `resume()` segment of `turn`.

        `parent` is the handle of the tool span that spawned this turn, or `None` for a root turn.
        `resumed` marks a segment picked up by `resume()`, `ask_id` the answered ask that drove it.
        """

    def end_turn(
        self,
        span: Any,
        *,
        outcome: str,
        stop_reason: str | None,
        output: Any,
        final_text: str | None,
        error: BaseException | str | None,
        ask_id: str | None,
    ) -> None:
        """Close a turn span. `outcome` is one of
        `completed | max_steps | cancelled | output | failed | suspended | aborted`.
        """

    def start_sample(
        self, parent: Any, req: SampleRequest, *, sample_id: str | None, provider: Provider, compacted: bool
    ) -> Any:
        """Open the span covering one model call, retries included. `sample_id` is `None` for calls
        made by a compactor rather than by the loop; `compacted` marks the first call after a
        compaction landed."""

    def end_sample(self, span: Any, *, end: StreamEnd | None, error: BaseException | None, attempts: int) -> None:
        """Close a sample span. `attempts` counts every try, so a retried call reports more than one."""

    def start_tool(
        self, parent: Any, call: ToolCallRequested, *, args: dict[str, Any], tool: Tool | None, replayed: bool
    ) -> Any:
        """Open the span covering one tool call. `args` are the effective arguments — hooks may have
        rewritten what `call` carries. `tool` is `None` when the model named a tool that is unknown,
        `replayed` marks a call whose execution restarts after a suspend."""

    def end_tool(
        self,
        span: Any,
        *,
        result: Any,
        is_error: bool,
        outcome: str,
        error_type: str | None,
        ask_id: str | None,
    ) -> None:
        """Close a tool span. `outcome` is one of `completed | error | suspended | aborted`."""

    def start_compaction(self, parent: Any) -> Any:
        """Open the span covering a compactor consultation, whether or not it compacts anything."""

    def end_compaction(self, span: Any, *, applied: CompactionApplied | None, error: BaseException | None) -> None:
        """Close a compaction span. `applied` is `None` when nothing was summarized."""


class NullTracer:
    def start_turn(self, turn: TurnContext, *, resumed: bool, ask_id: str | None, parent: Any) -> Any:
        return None

    def end_turn(
        self,
        span: Any,
        *,
        outcome: str,
        stop_reason: str | None,
        output: Any,
        final_text: str | None,
        error: BaseException | str | None,
        ask_id: str | None,
    ) -> None:
        return None

    def start_sample(
        self, parent: Any, req: SampleRequest, *, sample_id: str | None, provider: Provider, compacted: bool
    ) -> Any:
        return None

    def end_sample(self, span: Any, *, end: StreamEnd | None, error: BaseException | None, attempts: int) -> None:
        return None

    def start_tool(
        self, parent: Any, call: ToolCallRequested, *, args: dict[str, Any], tool: Tool | None, replayed: bool
    ) -> Any:
        return None

    def end_tool(
        self,
        span: Any,
        *,
        result: Any,
        is_error: bool,
        outcome: str,
        error_type: str | None,
        ask_id: str | None,
    ) -> None:
        return None

    def start_compaction(self, parent: Any) -> Any:
        return None

    def end_compaction(self, span: Any, *, applied: CompactionApplied | None, error: BaseException | None) -> None:
        return None


NULL_TRACER = NullTracer()

current_span: ContextVar[Any] = ContextVar("tantra_current_span", default=None)
