from typing import Annotated, Literal

from pydantic import BaseModel, Discriminator, Field

from tantra import Emitted


class Attachment(BaseModel):
    path: str
    name: str


class UserMessageFrame(BaseModel):
    type: Literal["user_message"] = "user_message"
    text: str
    attachments: list[Attachment] = Field(default_factory=list)


class AskResponseFrame(BaseModel):
    type: Literal["ask_response"] = "ask_response"
    ask_id: str
    response: str


class CancelFrame(BaseModel):
    type: Literal["cancel"] = "cancel"


ClientFrame = Annotated[UserMessageFrame | AskResponseFrame | CancelFrame, Discriminator("type")]


class ReplayDoneFrame(BaseModel):
    type: Literal["replay_done"] = "replay_done"


class BusyFrame(BaseModel):
    type: Literal["busy"] = "busy"
    retry_in: float


class TitleUpdatedFrame(BaseModel):
    type: Literal["title_updated"] = "title_updated"
    title: str


class ServerErrorFrame(BaseModel):
    type: Literal["server_error"] = "server_error"
    message: str


ServerFrame = ReplayDoneFrame | BusyFrame | TitleUpdatedFrame | ServerErrorFrame | Emitted
