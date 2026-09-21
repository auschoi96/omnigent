"""SDK-only models layered on the upstream OmniGent wire schemas.

Server-owned request, response, and stream-event models come directly from
``omnigent.server.schemas``.  This module contains only public client input
conveniences and response shapes for routes whose upstream annotations are
currently untyped dictionaries.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field


class _OutputModel(BaseModel):
    model_config = ConfigDict(extra="allow")

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        return self.model_dump(**kwargs)

    def to_json(self, **kwargs: Any) -> str:
        return self.model_dump_json(**kwargs)


class _StrictRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _SessionMessageData(_StrictRequestModel):
    role: Literal["user"] = "user"
    content: list[dict[str, Any]]


class SessionMessage(_StrictRequestModel):
    """Public user-message input for the existing session event route."""

    type: Literal["message"] = "message"
    data: _SessionMessageData
    model_override: str | None = None
    tools: list[dict[str, Any]] | None = None

    @classmethod
    def text(
        cls,
        text: str,
        *,
        model_override: str | None = None,
        tools: Sequence[Mapping[str, Any]] | None = None,
    ) -> Self:
        return cls.from_content(
            [{"type": "input_text", "text": text}],
            model_override=model_override,
            tools=tools,
        )

    @classmethod
    def from_content(
        cls,
        content: Sequence[Mapping[str, Any]],
        *,
        model_override: str | None = None,
        tools: Sequence[Mapping[str, Any]] | None = None,
    ) -> Self:
        return cls(
            data=_SessionMessageData(content=[dict(block) for block in content]),
            model_override=model_override,
            tools=[dict(tool) for tool in tools] if tools is not None else None,
        )


class _FunctionCallOutputData(_StrictRequestModel):
    call_id: str
    output: str


class FunctionCallOutput(_StrictRequestModel):
    """Public function-result input for the existing session event route."""

    type: Literal["function_call_output"] = "function_call_output"
    data: _FunctionCallOutputData


class _InterruptData(_StrictRequestModel):
    pass


class Interrupt(_StrictRequestModel):
    """Public interrupt input for the existing session event route."""

    type: Literal["interrupt"] = "interrupt"
    data: _InterruptData = Field(default_factory=_InterruptData)


PublicSessionEventInput = Annotated[
    SessionMessage | FunctionCallOutput | Interrupt,
    Field(discriminator="type"),
]


class EventAcknowledgement(_OutputModel):
    queued: bool
    item_id: str | None = None
    pending_id: str | None = None
    denied: bool | None = None
    reason: str | None = None
    elicitation_id: str | None = None
    child_session_id: str | None = None
    recovered: bool | None = None
    recovery: str | None = None


class ElicitationResolutionAcknowledgement(_OutputModel):
    queued: bool


class SessionResourceDeleted(_OutputModel):
    id: str
    object: Literal["session.resource.deleted"] = "session.resource.deleted"
    deleted: bool = True


class _PendingElicitationState(_OutputModel):
    status: Literal["pending"]
    message: str
    phase: str
    policy_name: str
    content_preview: str


class _ResolvedElicitationState(_OutputModel):
    status: Literal["resolved"]


ElicitationState = Annotated[
    _PendingElicitationState | _ResolvedElicitationState,
    Field(discriminator="status"),
]


class UnknownEvent(_OutputModel):
    """A newer output event whose discriminator is unknown to this client."""

    type: str
    raw: dict[str, Any]


class _SessionItemBase(_OutputModel):
    id: str
    response_id: str
    status: str
    created_at: int
    created_by: str | None = None


class _MessageSessionItem(_SessionItemBase):
    type: Literal["message"]
    role: Literal["user", "assistant"]
    content: list[dict[str, Any]]
    model: str | None = None
    is_meta: bool = False
    interrupted: bool = False
    stream_message_id: str | None = None


class _FunctionCallSessionItem(_SessionItemBase):
    type: Literal["function_call"]
    model: str
    name: str
    arguments: str
    call_id: str


class _FunctionCallOutputSessionItem(_SessionItemBase):
    type: Literal["function_call_output"]
    call_id: str
    output: str


class _ErrorSessionItem(_SessionItemBase):
    type: Literal["error"]
    source: Literal["llm", "execution", "tool", "harness"]
    code: str
    message: str
    level: Literal["error", "info"] | None = None


class _ReasoningSessionItem(_SessionItemBase):
    type: Literal["reasoning"]
    model: str
    summary: list[dict[str, str]]
    content: list[dict[str, str]] | None = None
    encrypted_content: str | None = None


class _CompactionSessionItem(_SessionItemBase):
    type: Literal["compaction"]
    summary: str
    last_item_id: str
    token_count: int
    model: str | None = None
    compacted_messages: list[dict[str, Any]] | None = None
    window_id: int | str | None = None


class _NativeToolSessionItem(_SessionItemBase):
    type: Literal["native_tool"]
    item: dict[str, Any]


class _ResourceEventSessionItem(_SessionItemBase):
    type: Literal["resource_event"]
    event_type: str
    resource_id: str
    resource_type: str
    resource: dict[str, Any] | None = None


class _RoutingDecisionSessionItem(_SessionItemBase):
    type: Literal["routing_decision"]
    model: str
    applied: bool
    rationale: str
    agent: str | None = None
    harness: str | None = None
    scope: Literal["session", "turn", "child_session", "native_subagent"] = "turn"
    decision_id: str | None = None
    raw_model: str | None = None
    attempted_override: str | None = None
    router_source: str | None = None
    task_description: str | None = None


class _SlashCommandSessionItem(_SessionItemBase):
    type: Literal["slash_command"]
    model: str
    kind: Literal["skill", "command"] = "skill"
    name: str
    arguments: str
    output: str | None = None


class _TerminalCommandSessionItem(_SessionItemBase):
    type: Literal["terminal_command"]
    kind: Literal["input", "output"]
    input: str | None = None
    stdout: str | None = None
    stderr: str | None = None


SessionItem = Annotated[
    _MessageSessionItem
    | _FunctionCallSessionItem
    | _FunctionCallOutputSessionItem
    | _ErrorSessionItem
    | _ReasoningSessionItem
    | _CompactionSessionItem
    | _NativeToolSessionItem
    | _ResourceEventSessionItem
    | _RoutingDecisionSessionItem
    | _SlashCommandSessionItem
    | _TerminalCommandSessionItem,
    Field(discriminator="type"),
]
