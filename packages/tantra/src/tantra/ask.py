from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class AskBase(BaseModel):
    model_config = ConfigDict(extra="allow")


class Approval(AskBase):
    kind: Literal["approval"] = "approval"
    title: str
    body: str = ""
    extra: dict[str, Any] = Field(default_factory=dict)


class Choice(AskBase):
    kind: Literal["choice"] = "choice"
    title: str
    options: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)


class FreeText(AskBase):
    kind: Literal["free_text"] = "free_text"
    prompt: str
    extra: dict[str, Any] = Field(default_factory=dict)


class ApprovalResponse(AskBase):
    kind: Literal["approval"] = "approval"
    allow: bool


class ChoiceResponse(AskBase):
    kind: Literal["choice"] = "choice"
    selected: str


class FreeTextResponse(AskBase):
    kind: Literal["free_text"] = "free_text"
    text: str


AskRequest = Annotated[Approval | Choice | FreeText, Field(discriminator="kind")]

AskResponse = Annotated[ApprovalResponse | ChoiceResponse | FreeTextResponse, Field(discriminator="kind")]
