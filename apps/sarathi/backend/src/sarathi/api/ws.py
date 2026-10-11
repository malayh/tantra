import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs
from uuid import UUID

import jwt
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sarathi.agent import ATTACHMENT_MARKER, RuntimeResources
from sarathi.auth import SocketTicket, decode_socket_ticket
from sarathi.config import get_settings
from sarathi.models import User
from sarathi.schemas import HeaderNotice, SessionInput
from tantra import (
    ApprovalResponse,
    ChoiceResponse,
    FreeTextResponse,
    InputCodec,
    LoggedEvent,
    SessionSocket,
    SocketAccess,
    SocketAuth,
    SocketBinding,
    SocketViewContext,
)


@dataclass(frozen=True)
class SocketPrincipal:
    user_id: str
    capability: str


def _query_ticket(scope: Mapping[str, Any]) -> str:
    query = parse_qs(scope.get("query_string", b"").decode("ascii"), keep_blank_values=True)
    if set(query) != {"ticket"} or len(query["ticket"]) != 1 or not query["ticket"][0]:
        raise PermissionError("socket ticket required")
    return query["ticket"][0]


def _ticket(scope: Mapping[str, Any]) -> SocketTicket:
    try:
        ticket = decode_socket_ticket(_query_ticket(scope))
        root_id = UUID(hex=scope["path_params"]["session_id"])
    except (KeyError, TypeError, ValueError, UnicodeError, jwt.PyJWTError) as exc:
        raise PermissionError("invalid socket ticket") from exc
    if ticket.root_id != root_id:
        raise PermissionError("invalid socket ticket")
    return ticket


def _owns_attachments(user_id: str, body: SessionInput) -> bool:
    root = (Path(get_settings().UPLOAD_DIR) / user_id).resolve()
    return all(Path(item.path).resolve().is_relative_to(root) for item in body.attachments)


def _encode_input(body: SessionInput) -> str:
    lines = [body.text]
    lines.extend(f"{ATTACHMENT_MARKER}{item.name} path={item.path}]" for item in body.attachments)
    return "\n".join(lines)


def _permission_arguments(resources: RuntimeResources, access: SocketAccess) -> bool:
    assert access.ask is not None
    extra = access.ask.event.request.extra
    permission = extra.get("permission")
    arguments = extra.get("arguments")
    if not isinstance(permission, str) or not isinstance(arguments, dict):
        return False
    tool = resources.runtime.tools.get(access.root.agent, {}).get(permission)
    if tool is None:
        return False
    try:
        validated = tool.args_model.model_validate(arguments, strict=True)
    except ValidationError:
        return False
    return validated.model_dump(mode="json", exclude_unset=True) == arguments


def build_session_socket(
    resources: RuntimeResources,
    session_factory: async_sessionmaker[AsyncSession],
) -> SessionSocket:
    async def bind(scope: Mapping[str, Any]) -> SocketBinding:
        ticket = _ticket(scope)

        async def revalidate() -> bool:
            async with session_factory() as db:
                return await db.get(User, int(ticket.user_id)) is not None

        return SocketBinding(
            ticket.root_id,
            SocketAuth(
                principal=SocketPrincipal(ticket.user_id, ticket.capability),
                submitted_by=ticket.user_id,
                expires_at=ticket.login_expires_at,
                revalidate=revalidate,
            ),
        )

    async def policy(access: SocketAccess) -> bool:
        principal = access.binding.auth.principal
        if not isinstance(principal, SocketPrincipal):
            return False
        if access.root.parent_id is not None or access.root.metadata.get("user") != principal.user_id:
            return False
        if access.actor.metadata.get("user") != principal.user_id:
            return False
        if access.operation == "subscribe":
            return True
        if principal.capability != "write" or access.actor.id != access.root.id:
            return False
        if access.operation == "send":
            return isinstance(access.input, SessionInput) and _owns_attachments(principal.user_id, access.input)
        if access.operation != "answer":
            return True
        if access.ask is None or access.response is None or access.ask.actor_id.hex != access.root.id:
            return False
        request = access.ask.event.request
        if request.kind != access.response.kind:
            return False
        if isinstance(access.response, ApprovalResponse):
            return _permission_arguments(resources, access)
        if isinstance(access.response, ChoiceResponse):
            return access.response.selected in request.options
        return isinstance(access.response, FreeTextResponse)

    async def views(_context: SocketViewContext):
        async def view(item: LoggedEvent) -> Any:
            return item.event.model_dump(mode="json")

        return view

    async def notices(binding: SocketBinding) -> AsyncIterator[Any]:
        header = await resources.store.header(binding.root_id.hex)
        if header is None:
            return
        current = (header.title, header.model)
        yield HeaderNotice(title=header.title, model=header.model).model_dump(mode="json")
        coordinator = resources.runtime.coordinator
        if coordinator is None:
            await asyncio.Future()
        else:
            async for notice in coordinator.watch(binding.root_id.hex):
                if notice.kind != "header" or notice.actor_id != binding.root_id.hex:
                    continue
                header = await resources.store.header(binding.root_id.hex)
                if header is None:
                    return
                changed = (header.title, header.model)
                if changed == current:
                    continue
                current = changed
                yield HeaderNotice(title=header.title, model=header.model).model_dump(mode="json")

    return SessionSocket(
        resources.runtime,
        bind=bind,
        policy=policy,
        input_codec=InputCodec(SessionInput, _encode_input),
        views=views,
        notices=notices,
        allowed_origins=set(get_settings().cors_origins),
    )
