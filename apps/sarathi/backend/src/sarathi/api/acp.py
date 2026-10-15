import re
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import jwt
from fastapi import APIRouter, Response
from pydantic import ValidationError

from sarathi.agent import ATTACHMENT_MARKER, RuntimeResources, Sarathi
from sarathi.auth import CurrentLogin, create_acp_ticket, decode_acp_ticket
from sarathi.config import get_settings
from sarathi.schemas import AcpTicketOut, AcpTicketRequest
from tantra import ApprovalResponse, ChoiceResponse, FreeTextResponse
from tantra.acp import AcpAccess, AcpAuth, AcpServer, AcpWebSocket
from tantra.acp.wire import ContentBlock, text_block

ATTACHMENT_LINE = re.compile(re.escape(ATTACHMENT_MARKER) + r"(.+)\]")
PATH_MARKER = " path="

router = APIRouter(prefix="/api/acp", tags=["acp"])


@dataclass(frozen=True)
class AcpPrincipal:
    user_id: str
    capability: str


@router.post("/ticket")
async def issue_acp_ticket(body: AcpTicketRequest, login: CurrentLogin, response: Response) -> AcpTicketOut:
    ticket, expires_at = create_acp_ticket(login.user.id, body.mode, login.expires_at)
    response.headers["Cache-Control"] = "no-store"
    return AcpTicketOut(ticket=ticket, expires_at=expires_at)


def _query_ticket(scope: Mapping[str, Any]) -> str:
    query = parse_qs(scope.get("query_string", b"").decode("ascii"), keep_blank_values=True)
    if set(query) != {"ticket"} or len(query["ticket"]) != 1 or not query["ticket"][0]:
        raise PermissionError("ACP ticket required")
    return query["ticket"][0]


async def bind(scope: Mapping[str, Any]) -> AcpAuth:
    try:
        ticket = decode_acp_ticket(_query_ticket(scope))
    except (KeyError, TypeError, ValueError, UnicodeError, jwt.PyJWTError) as exc:
        raise PermissionError("invalid ACP ticket") from exc
    return AcpAuth(
        AcpPrincipal(ticket.user_id, ticket.capability),
        submitted_by=ticket.user_id,
        expires_at=ticket.login_expires_at,
    )


async def scope(auth: AcpAuth, meta: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(auth.principal, AcpPrincipal):
        raise PermissionError("unknown principal")
    return {"user": auth.principal.user_id, "kind": "root"}


def _permission_arguments(resources: RuntimeResources, access: AcpAccess) -> bool:
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


async def policy(resources: RuntimeResources, access: AcpAccess) -> bool:
    principal = access.auth.principal
    if not isinstance(principal, AcpPrincipal):
        return False
    if access.root.parent_id is not None or access.root.metadata.get("user") != principal.user_id:
        return False
    if access.actor.metadata.get("user") != principal.user_id:
        return False
    if access.operation == "resume":
        return True
    if principal.capability != "write" or access.actor.id != access.root.id:
        return False
    if access.operation != "answer":
        return True
    if access.ask is None or access.ask.actor_id.hex != access.root.id:
        return False
    if access.response is None:
        return True
    request = access.ask.event.request
    if request.kind != access.response.kind:
        return False
    if isinstance(access.response, ApprovalResponse):
        return _permission_arguments(resources, access)
    if isinstance(access.response, ChoiceResponse):
        return access.response.selected in request.options
    return isinstance(access.response, FreeTextResponse)


def _marker(root: Path, block: ContentBlock) -> str:
    name, uri = block.get("name"), block.get("uri")
    if not isinstance(name, str) or not isinstance(uri, str) or "\n" in name or PATH_MARKER in name:
        raise ValueError("invalid resource link")
    parts = urlsplit(uri)
    if parts.scheme != "file" or parts.netloc or parts.query or parts.fragment:
        raise ValueError("resource links must be local file URIs")
    path = unquote(parts.path)
    if not path.startswith("/"):
        raise ValueError("resource links must be absolute")
    try:
        resolved = Path(path).resolve(strict=True)
    except OSError:
        raise ValueError("resource link not found") from None
    if not resolved.is_relative_to(root) or "\n" in str(resolved) or PATH_MARKER in str(resolved):
        raise ValueError("invalid resource link path")
    return f"{ATTACHMENT_MARKER}{name}{PATH_MARKER}{resolved}]"


async def encode_prompt(auth: AcpAuth, blocks: list[ContentBlock], meta: Mapping[str, Any]) -> str:
    root = (Path(get_settings().UPLOAD_DIR) / auth.principal.user_id).resolve()
    texts: list[str] = []
    markers: list[str] = []
    for block in blocks:
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            texts.append(block["text"])
        elif block.get("type") == "resource_link":
            markers.append(_marker(root, block))
        else:
            raise ValueError("unsupported content block")
    body = "\n".join(texts)
    if any(ATTACHMENT_LINE.fullmatch(line.strip()) for line in body.split("\n")):
        raise ValueError("text must not contain attachment markers")
    return "\n".join([body, *markers])


def decode_input(input: str) -> list[ContentBlock]:
    lines = input.split("\n")
    links: list[ContentBlock] = []
    while lines:
        match = ATTACHMENT_LINE.fullmatch(lines[-1].strip())
        if match is None:
            break
        name, found, path = match[1].rpartition(PATH_MARKER)
        if not found or not path.startswith("/"):
            break
        links.insert(0, {"type": "resource_link", "name": name, "uri": Path(path).as_uri()})
        lines.pop()
    return [text_block("\n".join(lines)), *links]


def build_acp_endpoint(resources: RuntimeResources) -> AcpWebSocket:
    settings = get_settings()
    server = AcpServer(
        resources.runtime,
        agent=Sarathi,
        scope=scope,
        policy=partial(policy, resources),
        encode_prompt=encode_prompt,
        decode_input=decode_input,
        models=settings.models,
        name="sarathi",
    )
    return server.websocket(bind=bind, allowed_origins=set(settings.cors_origins))
