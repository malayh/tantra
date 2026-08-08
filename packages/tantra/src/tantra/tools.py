from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any, get_type_hints

from pydantic import BaseModel, create_model

from tantra.ask import AskRequest, AskResponse
from tantra.errors import TantraError
from tantra.providers.base import ToolSchema

if TYPE_CHECKING:
    from tantra.agent import Agent
    from tantra.memory import Memory
    from tantra.stores.base import Store


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
        spawn: Callable[[type[Agent] | str, str], Awaitable[Any]] | None = None,
        fan_out: Callable[[Sequence[tuple[type[Agent] | str, str]], int], Awaitable[list[Any]]] | None = None,
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
        self._fan_out = fan_out

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

    async def spawn(self, agent: type[Agent] | str, input: str) -> Any:
        """Run `agent` as a child session and return its final text, or its parsed `output_schema`.

        Re-entrant: a resumed turn re-executes the tool and attaches to the child already recorded
        for this call rather than creating a second one. A child that asks suspends this turn too.
        """
        if self._spawn is None:
            raise TantraError("ctx.spawn is only available inside a running tool call")
        return await self._spawn(agent, input)

    async def fan_out(self, tasks: Sequence[tuple[type[Agent] | str, str]], max_concurrency: int = 4) -> list[Any]:
        """Run `(agent, input)` pairs as concurrent child sessions.

        Results are positionally aligned with `tasks`; a task that fails contributes its exception
        in place of a result instead of failing the turn.
        """
        if self._fan_out is None:
            raise TantraError("ctx.fan_out is only available inside a running tool call")
        return await self._fan_out(tasks, max_concurrency)


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
