from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tantra.context import TurnContext
from tantra.events import SessionEvent, ToolCallRequested

if TYPE_CHECKING:
    from tantra.loop import Emitted


@dataclass(frozen=True)
class Denial:
    reason: str


class Hook:
    """Lifecycle callbacks on the turn loop. Subclass and override only what you need."""

    async def before_turn(self, turn: TurnContext) -> None:
        """Called once per `run()`, after `TurnStarted` is persisted. Not called on `resume()`."""

    async def before_sample(self, turn: TurnContext) -> None:
        """Called before every model call, including retried turns resumed in another process."""

    async def before_tool(self, call: ToolCallRequested, turn: TurnContext) -> ToolCallRequested | Denial | None:
        """Gate a tool call: `None` passes it through, a `ToolCallRequested` replaces the executed
        arguments (the log keeps what the model asked for), and `Denial` turns it into an error
        result the model can adapt to without ever invoking the tool.
        """

    async def after_tool(self, call: ToolCallRequested, result: Any, is_error: bool, turn: TurnContext) -> Any:
        """Transform an executed tool's result before it is persisted. `None` keeps it unchanged."""

    async def after_turn(self, turn: TurnContext, event: SessionEvent) -> None:
        """Called with the terminal `TurnCompleted` or `TurnFailed` once it is persisted."""

    async def on_event(self, emitted: Emitted) -> None:
        """Called for every emitted event, including the live deltas that are never persisted."""
