from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, get_type_hints

from pydantic import BaseModel, create_model

from tantra.ask import AskRequest, AskResponse
from tantra.errors import TantraError
from tantra.providers.base import ToolSchema

if TYPE_CHECKING:
    from tantra.agent import Agent
    from tantra.memory import Memory
    from tantra.stores.base import Store


@dataclass(frozen=True)
class TaskRef:
    task_id: str
    agent: str


class Context:
    """Runtime handle for a tool. Annotate a parameter `ctx: Context` to receive it.

    The annotated parameter is stripped from the model-facing schema and injected at call time.
    """

    def __init__(
        self,
        *,
        session_id: str,
        turn_id: str,
        call_id: str,
        depth: int,
        deps: Any,
        store: Store,
        emit: Callable[[str], Awaitable[None]],
        ask: Callable[[AskRequest], Awaitable[AskResponse]] | None = None,
        spawn: Callable[[type[Agent] | str, str], Awaitable[TaskRef]] | None = None,
        task_status: Callable[[str, int | None, int], Awaitable[dict[str, Any]]] | None = None,
        task_messages: Callable[[str, int], Awaitable[list[dict[str, Any]]]] | None = None,
        task_result: Callable[[str], Awaitable[dict[str, Any]]] | None = None,
        task_wait: Callable[[list[str] | None], Awaitable[dict[str, Any]]] | None = None,
        memory: Memory | None = None,
    ) -> None:
        self.session_id = session_id
        self.turn_id = turn_id
        self.call_id = call_id
        self.depth = depth
        self.deps = deps
        self.store = store
        self.memory = memory
        self._emit = emit
        self._ask = ask
        self._spawn = spawn
        self._task_status = task_status
        self._task_messages = task_messages
        self._task_result = task_result
        self._task_wait = task_wait

    async def emit(self, message: str) -> None:
        """Record progress for the running tool call as a persisted `ToolProgress` event."""
        await self._emit(message)

    async def ask(self, request: AskRequest) -> AskResponse:
        """Suspend the turn until a human answers, then return their response.

        The process may die while suspended: on resume the tool is re-executed from the start and
        every already-answered `ask` returns its recorded response without prompting again. Nothing
        may be captured in a closure across the suspend.
        """
        if self._ask is None:
            raise TantraError("ctx.ask is only available inside a running tool call")
        return await self._ask(request)

    async def spawn(self, agent: type[Agent] | str, input: str) -> TaskRef:
        """Create an attached child task and return its durable reference immediately."""
        if self._spawn is None:
            raise TantraError("ctx.spawn is only available inside a running tool call")
        return await self._spawn(agent, input)

    async def task_status(self, task_id: str, after_seq: int | None, limit: int) -> dict[str, Any]:
        if self._task_status is None:
            raise TantraError("task_status is only available inside a running tool call")
        return await self._task_status(task_id, after_seq, limit)

    async def task_messages(self, task_id: str, limit: int) -> list[dict[str, Any]]:
        if self._task_messages is None:
            raise TantraError("task_messages is only available inside a running tool call")
        return await self._task_messages(task_id, limit)

    async def task_result(self, task_id: str) -> dict[str, Any]:
        if self._task_result is None:
            raise TantraError("task_result is only available inside a running tool call")
        return await self._task_result(task_id)

    async def task_wait(self, task_ids: list[str] | None) -> dict[str, Any]:
        if self._task_wait is None:
            raise TantraError("task_wait is only available inside a running tool call")
        return await self._task_wait(task_ids)


def _args_model(fn: Callable[..., Any], name: str) -> tuple[str | None, type[BaseModel]]:
    hints = get_type_hints(fn)
    ctx_param: str | None = None
    fields: dict[str, Any] = {}
    for param_name, param in inspect.signature(fn).parameters.items():
        annotation = hints.get(param_name, Any)
        if isinstance(annotation, type) and issubclass(annotation, Context):
            ctx_param = param_name
            continue
        default = ... if param.default is inspect.Parameter.empty else param.default
        fields[param_name] = (annotation, default)
    return ctx_param, create_model(f"{name}_args", **fields)


class Tool:
    def __init__(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        description: str | None = None,
        permission: str | None = None,
    ) -> None:
        self.fn = fn
        self.name = name or fn.__name__
        self.description = description if description is not None else (inspect.getdoc(fn) or "")
        self.permission = permission
        self.ctx_param, self.args_model = _args_model(fn, self.name)
        self.takes_ctx = self.ctx_param is not None
        self.schema = ToolSchema(
            name=self.name,
            description=self.description,
            parameters=self.args_model.model_json_schema(),
        )

    async def invoke(self, args: dict[str, Any], ctx: Context) -> Any:
        validated = self.args_model.model_validate(args)
        kwargs: dict[str, Any] = {field: getattr(validated, field) for field in self.args_model.model_fields}
        if self.ctx_param is not None:
            kwargs[self.ctx_param] = ctx
        result = self.fn(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    def __repr__(self) -> str:
        return f"Tool({self.name})"


def tool(
    fn: Callable[..., Any] | None = None,
    *,
    name: str | None = None,
    description: str | None = None,
    permission: str | None = None,
) -> Any:
    def wrap(target: Callable[..., Any]) -> Tool:
        return Tool(target, name=name, description=description, permission=permission)

    return wrap(fn) if fn is not None else wrap
