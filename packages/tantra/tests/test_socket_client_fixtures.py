from __future__ import annotations

import json
from pathlib import Path

from pydantic import TypeAdapter

from tantra._socket_protocol import AnswerFrame, ServerFrame

FIXTURES = Path(__file__).parents[2] / "tantra-client" / "test" / "fixtures" / "protocol.json"
SUBSCRIPTION_ID = "12345678-1234-1234-1234-123456789abc"
COMMAND_ID = "32345678-1234-1234-1234-123456789abc"
ASK_ID = "42345678-1234-1234-1234-123456789abc"


def test_typescript_server_frame_fixtures_match_python_models() -> None:
    fixtures = json.loads(FIXTURES.read_text())
    adapter = TypeAdapter(ServerFrame)
    for payload in fixtures["server_frames"]:
        frame = adapter.validate_python(payload, strict=True)
        assert frame.model_dump(mode="json") == payload


def test_typescript_answer_fixtures_match_python_models() -> None:
    fixtures = json.loads(FIXTURES.read_text())
    for response in fixtures["answers"]:
        frame = AnswerFrame(
            subscription_id=SUBSCRIPTION_ID,
            command_id=COMMAND_ID,
            ask_id=ASK_ID,
            response=response,
        )
        assert frame.response.model_dump(mode="json") == response
