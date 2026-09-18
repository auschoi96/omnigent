"""Portable Pydantic contracts for the sessions REST API and SSE stream."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from typing import Annotated, Any, Literal, Self, TypeVar, cast, get_args

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

USER_SESSION_TITLE_MAX_CHARS = 200
FRAMEWORK_NOTICE_BLOCK_TYPE = "_omnigent_framework_notice"


def _omit_explicit_additional_properties(schema: dict[str, Any]) -> None:
    schema.pop("additionalProperties", None)


class _OutputModel(BaseModel):
    model_config = ConfigDict(
        extra="allow", json_schema_extra=_omit_explicit_additional_properties
    )

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        """Return the same representation as :meth:`model_dump`."""
        return self.model_dump(**kwargs)

    def to_json(self, **kwargs: Any) -> str:
        """Return the same representation as :meth:`model_dump_json`."""
        return self.model_dump_json(**kwargs)


class _StrictRequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


def reject_authored_framework_notices(content: object) -> object:
    """Reject framework-owned context blocks in authored content."""
    if isinstance(content, dict):
        if content.get("type") == FRAMEWORK_NOTICE_BLOCK_TYPE:
            raise ValueError("Framework notice blocks are reserved for attachment resolution")
        for value in content.values():
            reject_authored_framework_notices(value)
    elif isinstance(content, list):
        for value in content:
            reject_authored_framework_notices(value)
    return content


_INLINE_BASE64_DATA_URI = re.compile(
    r"data:([^;,\s]*)(?:;[^;,\r\n]*)*;base64,[ \t]?([A-Za-z0-9+/=_-]+)", re.IGNORECASE
)
_BASE64_ALPHABET_ONLY = re.compile(r"[A-Za-z0-9+/=_-]+")
_NON_BINARY_SOURCE_TYPES = frozenset({"text", "url", "content", "file"})
_BINARY_BLOCK_TYPES = frozenset({"image", "document", "file"})
_MIN_UNTELLED_BLOB_CHARS = 64
_MIN_REDACTABLE_URI_PAYLOAD = 128
_Value = TypeVar("_Value")


def redact_inline_data_uris(value: _Value, marker: Callable[[str, int], str]) -> _Value:
    """Recursively replace sizeable inline base64 data URIs."""
    if isinstance(value, str):
        return cast(
            _Value,
            _INLINE_BASE64_DATA_URI.sub(
                lambda match: (
                    marker(match.group(1), len(match.group(2)))
                    if len(match.group(2)) >= _MIN_REDACTABLE_URI_PAYLOAD
                    else match.group(0)
                ),
                value,
            ),
        )
    if isinstance(value, list):
        return cast(_Value, [redact_inline_data_uris(item, marker) for item in value])
    if isinstance(value, dict):
        return cast(
            _Value,
            {key: redact_inline_data_uris(item, marker) for key, item in value.items()},
        )
    return value


def _is_base64_payload(value: str) -> bool:
    compact = "".join(value.split())
    if not compact or not _BASE64_ALPHABET_ONLY.fullmatch(compact):
        return False
    return "=" in compact or len(compact) % 4 == 0 or len(compact) >= _MIN_UNTELLED_BLOB_CHARS


def _redact_data_field(block: dict[str, object], marker: Callable[[str, int], str]) -> None:
    data = block.get("data")
    if not isinstance(data, str) or not data or not _is_base64_payload(data):
        return
    media_type = block.get("media_type")
    block["data"] = marker(media_type if isinstance(media_type, str) else "", len(data))


def redact_binary_payloads(value: _Value, marker: Callable[[str, int], str]) -> _Value:
    """Recursively replace base64 payloads in structured content blocks."""
    if isinstance(value, str):
        return redact_inline_data_uris(value, marker)
    if isinstance(value, list):
        return cast(_Value, [redact_binary_payloads(item, marker) for item in value])
    if isinstance(value, dict):
        block: dict[str, object] = dict(value)
        block_type = block.get("type")
        if isinstance(block_type, str) and block_type in _BINARY_BLOCK_TYPES:
            _redact_data_field(block, marker)
            source = block.get("source")
            if isinstance(source, dict):
                source = dict(source)
                source_type = source.get("type")
                if not (isinstance(source_type, str) and source_type in _NON_BINARY_SOURCE_TYPES):
                    _redact_data_field(source, marker)
                block["source"] = source
        return cast(
            _Value,
            {key: redact_binary_payloads(item, marker) for key, item in block.items()},
        )
    return value


class MessageData(_OutputModel):
    """
    Data for a message item (user or assistant).

    :param role: ``"user"`` or ``"assistant"``.
    :param content: Heterogeneous content blocks, e.g.
        ``[{"type": "input_text", "text": "Hello"}]``.
    :param agent: Agent name (required for assistant messages,
        absent for user). Serialized as ``"model"`` in JSON.
    :param is_meta: ``True`` for durable context that must be
        replayed to agents but hidden from user-facing transcripts,
        e.g. injected skill instructions. Defaults to ``False``
        and is omitted from serialized payloads in that case.
    :param interrupted: ``True`` when an assistant message is a
        durable partial response from an interrupted external-native
        turn, e.g. Codex ``turn/completed`` with status
        ``"interrupted"``. Defaults to ``False`` and is omitted from
        serialized payloads in that case.
    :param stream_message_id: Native live-preview stream finalized by
        this assistant message. Persisted so reconnect snapshots can
        suppress delayed preview chunks after the authoritative item.
    """

    role: Literal["user", "assistant"]
    content: list[dict[str, Any]]
    agent: str | None = Field(
        default=None,
        validation_alias=AliasChoices("agent", "model"),
        serialization_alias="model",
    )
    is_meta: bool = Field(default=False, exclude_if=lambda value: value is False)
    interrupted: bool = Field(default=False, exclude_if=lambda value: value is False)
    stream_message_id: str | None = None

    @field_validator("content")
    @classmethod
    def reject_framework_blocks(cls, content: list[dict[str, Any]]) -> list[dict[str, Any]]:
        reject_authored_framework_notices(content)
        return content

    @model_validator(mode="after")
    def check_agent_for_assistant(self) -> MessageData:
        if self.role == "assistant" and self.agent is None:
            raise ValueError("assistant messages require 'agent'")
        return self


class FunctionCallData(_OutputModel):
    """
    Data for a function_call item.

    :param agent: Agent name. Serialized as ``"model"`` in JSON.
    :param name: Tool function name, e.g. ``"search.web"``.
    :param arguments: JSON-encoded arguments string.
    :param call_id: Unique call identifier from the LLM,
        e.g. ``"call_abc123"``.
    """

    agent: str = Field(
        validation_alias=AliasChoices("agent", "model"),
        serialization_alias="model",
    )
    name: str
    arguments: str
    call_id: str


class FunctionCallOutputData(_OutputModel):
    """
    Data for a function_call_output item.

    :param call_id: The call_id this output corresponds to,
        e.g. ``"call_abc123"``.
    :param output: The tool's string result.
    """

    call_id: str
    output: str


class ErrorData(_OutputModel):
    """
    Data for a persisted error banner item.

    These items mirror ``response.error`` events so clients can render
    the same error banner after reconnect / refresh. They are listed in
    :data:`NON_CONTENT_ITEM_TYPES` because they are operator-visible
    transcript metadata, not content the next agent turn should receive.

    :param source: Error source, e.g. ``"execution"``.
    :param code: Stable error classifier, e.g.
        ``"native_terminal_start_failed"``.
    :param message: Human-readable error message, e.g.
        ``"Native Codex requires the 'codex' CLI on PATH."``.
    :param level: Rendering level. ``"info"`` renders the banner as a neutral
        notice (e.g. codex started a fresh thread) rather than a failure;
        ``None`` / ``"error"`` is the destructive default and is omitted from
        the wire so existing error items are unchanged.
    """

    source: Literal["llm", "execution", "tool", "harness"]
    code: str
    message: str
    level: Literal["error", "info"] | None = None

    @field_validator("code", "message")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("error code and message must be non-empty")
        return value


class ReasoningData(_OutputModel):
    """
    Data for a reasoning item.

    :param agent: Agent name. Serialized as ``"model"`` in JSON.
    :param summary: Summary text blocks,
        e.g. ``[{"type": "summary_text", "text": "..."}]``.
    :param content: Raw reasoning content blocks, or ``None`` if
        redacted.
    :param encrypted_content: Encrypted reasoning content, or
        ``None``.
    """

    agent: str = Field(
        validation_alias=AliasChoices("agent", "model"),
        serialization_alias="model",
    )
    summary: list[dict[str, str]]
    content: list[dict[str, str]] | None = None
    encrypted_content: str | None = None


def _binary_payload_omitted(media_type: str, _payload_length: int) -> str:
    return f"[{media_type or 'binary'} content omitted from the compaction snapshot]"


class CompactionData(_OutputModel):
    """
    Data payload for a compaction summary item.

    Stored as a conversation item of ``type="compaction"``.
    The summary covers all items from the start of the
    conversation (or the previous compaction item) through
    the item identified by ``last_item_id``.

    :param summary: The LLM-generated summary text covering
        all conversation items up through ``last_item_id``,
        e.g. ``"User asked to analyze a dataset. Agent loaded
        data.csv and computed statistics."``.
    :param last_item_id: The item ID (inclusive) of the last
        conversation item covered by this summary, e.g.
        ``"msg_abc123"``. Items at positions <= this item are
        summarized and do not need to be loaded for prompt
        construction.
    :param model: The model used to generate the summary,
        e.g. ``"openai/gpt-4o"``.
    :param token_count: Approximate token count of the summary
        text, for budget tracking, e.g. ``342``.
    :param window_id: Opaque vendor compaction-window identifier. Current
        Codex writes a UUID string to ``payload.window_id`` on its
        ``type == "compacted"`` rollout JSONL record; older Codex rollouts
        used integer counters there.
    """

    summary: str
    last_item_id: str
    model: str | None = None
    token_count: int
    compacted_messages: list[dict[str, Any]] | None = None
    window_id: int | str | None = None

    @field_validator("compacted_messages")
    @classmethod
    def strip_binary_payloads(
        cls, value: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]] | None:
        reject_authored_framework_notices(value)
        return redact_binary_payloads(value, _binary_payload_omitted)


class NativeToolData(_OutputModel):
    """
    A provider-native tool output item (e.g. ``web_search_call``).

    These are executed server-side by the LLM provider and returned
    as opaque dicts. Agent-plane persists and replays them so the
    LLM sees its own tool results on subsequent iterations.

    :param item: The raw dict from the Responses API output, e.g.
        ``{"type": "web_search_call", "id": "ws_abc",
        "status": "completed", "action": {...}}``.
    """

    item: dict[str, Any]


class ResourceEventData(_OutputModel):
    """Data payload for a persisted resource lifecycle event.

    These items are written to the conversation store when a
    session resource is created or deleted, so reconnecting
    clients can discover resource history without replaying the
    live SSE stream.  The agent loop filters them out of the
    LLM's message context (they are metadata, not conversation
    content).

    :param event_type: The SSE event type literal, e.g.
        ``"session.resource.created"`` or
        ``"session.resource.deleted"``.
    :param resource_id: Opaque id of the affected resource,
        e.g. ``"terminal_bash_s1"`` or ``"file_abc123"``.
    :param resource_type: Kind of resource, e.g.
        ``"terminal"``, ``"file"``, ``"environment"``.
    :param resource: Full resource object dict for ``created``
        events. ``None`` for ``deleted`` events.
    """

    event_type: str
    resource_id: str
    resource_type: str
    resource: dict[str, Any] | None = None


class RoutingDecisionData(_OutputModel):
    """
    Data payload for an intelligent model-router decision item.

    Emitted by the server-side smart routing path at the START of an
    advised turn and persisted
    as a display-only transcript item so the model the router chose shows
    in the conversation flow the moment the turn begins. Listed in
    :data:`NON_CONTENT_ITEM_TYPES` so the agent loop's history filter
    skips it — the brain never sees (or answers) its own router note. The
    runner's harness-input builder also drops every non
    message/function_call type, a second guarantee it stays out of the
    model's context.

    :param model: The concrete brain model the router chose, e.g.
        ``"databricks-claude-opus-4-8"``.
    :param applied: ``True`` when the brain actually ran on
        :attr:`model` this turn (optimize mode, no user pin); ``False``
        when the router only WOULD have picked it (advise/shadow mode, or
        a user model pin won) — the UI renders "would have picked".
    :param rationale: The router's one-line explanation, shown as muted
        secondary text, e.g. ``"Multi-file refactor needs deep
        reasoning."``.
    :param harness: Harness the decision applies to, e.g.
        ``"claude-native"`` or ``"codex"``. ``None`` when the decision
        picked a model only (no harness dimension).
    :param scope: What the decision governs — ``"session"`` (auto-harness
        session routing), ``"turn"`` (per-turn routing), ``"child_session"``
        (an Omnigent-spawned sub-agent) or ``"native_subagent"`` (a Task /
        ``spawn_agent`` spawn routed inside the harness). Defaults to
        ``"turn"`` so rows persisted before this field deserialize.
    :param decision_id: Router decision identifier, e.g.
        ``"3f1c…"``. Correlates the transcript item with the routing
        telemetry event and the child-sessions API row. ``None`` for
        decisions made before decision ids existed.
    :param raw_model: The router-vocabulary pick before resolution to a
        servable catalog id, e.g. ``"gpt-5-6-sol"``. ``None`` when the
        pick needed no resolution.
    :param attempted_override: Model the spawning agent asked for and the
        router overrode, e.g. ``"databricks-gpt-5-5"`` — an LLM-supplied
        ``args.model`` on a child session, or a native spawn's own
        ``requested_model``. ``None`` when nothing was asked for, or when
        the router's pick names the same arm as the ask.
    :param router_source: Which router produced the decision —
        ``"databricks-aigw"`` for the external AI-Gateway ``task_v1``
        service, ``"oss-llm"`` for the built-in judge. Deliberately a
        plain ``str`` rather than a ``Literal``: a source added later
        must still round-trip through stored rows and the wire instead
        of failing validation. ``None`` on rows written before the
        field existed.
    :param task_description: Human label of the task/spawn this decision
        governed, e.g. ``"Research auth flows"`` — what ties a fan-out's
        decision to its sub-agent when every spawn shares one
        :attr:`agent` type. ``None`` when the spawn carried none, and on
        rows written before the field existed.
    """

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

    @field_validator("model")
    @classmethod
    def require_non_empty_model(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("routing_decision model must be non-empty")
        return value


class SlashCommandData(_OutputModel):
    """
    Data payload for a slash-command invocation observed in a
    harness transcript (today: Claude Code's embedded TUI).

    Listed in :data:`NON_CONTENT_ITEM_TYPES` so the agent loop's
    history filter skips it — a downstream LLM never sees this as a
    phantom tool call. Field names mirror ``function_call`` so the
    web renderer can reuse the tool-card layout.

    :param agent: Harness/agent name, e.g. ``"claude-native-ui"``.
        Serialized as ``"model"`` for parity with other items.
    :param kind: ``"skill"`` for plugin/Skill invocations,
        ``"command"`` for surfaced CLI built-ins (``/effort``,
        ``/clear``, ``/compact``, ``/model``, ``/ultrareview``).
        The web renderer uses this to pick the prefix label and
        icon. Defaults to ``"skill"`` so persisted items predating
        this field deserialize without backfill.
    :param name: Command name with leading ``/`` stripped, e.g.
        ``"dev-productivity:simplify"``.
    :param arguments: Raw ``<command-args>`` text. Empty when none.
    :param output: ``<local-command-stdout>`` text when present,
        else ``None`` (the common case — Skills act via the next
        assistant turn, not stdout).
    """

    agent: str = Field(
        validation_alias=AliasChoices("agent", "model"),
        serialization_alias="model",
    )
    kind: Literal["skill", "command"] = "skill"
    name: str
    arguments: str
    output: str | None = None


class TerminalCommandData(_OutputModel):
    """
    Data payload for a runner-side terminal command (``!cmd``) observed
    in a harness transcript (today: Claude Code's embedded TUI).

    Listed in :data:`NON_CONTENT_ITEM_TYPES` so the agent loop never
    injects this as phantom content into the LLM's message history.

    Claude Code writes two sibling transcript records per ``!cmd``
    invocation: one ``<bash-input>`` record and one combined
    ``<bash-stdout>``/``<bash-stderr>`` record. Each maps to one
    ``terminal_command`` item with ``kind="input"`` or
    ``kind="output"`` respectively.

    :param kind: ``"input"`` for the command text, ``"output"`` for
        the combined stdout/stderr result.
    :param input: The raw command string, e.g. ``"pwd"``. Present when
        ``kind="input"``, ``None`` otherwise.
    :param stdout: Captured stdout text. Present when ``kind="output"``,
        ``None`` otherwise.
    :param stderr: Captured stderr text. Present when ``kind="output"``,
        ``None`` otherwise.
    """

    kind: Literal["input", "output"]
    input: str | None = None
    stdout: str | None = None
    stderr: str | None = None


ItemData = (
    MessageData
    | FunctionCallData
    | FunctionCallOutputData
    | ErrorData
    | ReasoningData
    | CompactionData
    | NativeToolData
    | ResourceEventData
    | RoutingDecisionData
    | SlashCommandData
    | TerminalCommandData
)

ITEM_TYPE_TO_DATA_CLS: dict[str, type[BaseModel]] = {
    "message": MessageData,
    "function_call": FunctionCallData,
    "function_call_output": FunctionCallOutputData,
    "error": ErrorData,
    "reasoning": ReasoningData,
    "compaction": CompactionData,
    "native_tool": NativeToolData,
    "resource_event": ResourceEventData,
    "routing_decision": RoutingDecisionData,
    "slash_command": SlashCommandData,
    "terminal_command": TerminalCommandData,
}


def parse_item_data(item_type: str, raw: dict[str, Any]) -> ItemData:
    cls = ITEM_TYPE_TO_DATA_CLS.get(item_type)
    if cls is None:
        raise ValueError(f"unknown item type: {item_type!r}")
    return cls(**raw)  # type: ignore[return-value]


def _validate_type_matches_data(item_type: str, data: ItemData) -> None:
    expected = ITEM_TYPE_TO_DATA_CLS.get(item_type)
    if expected is None:
        raise ValueError(f"unknown item type: {item_type!r}")
    if not isinstance(data, expected):
        raise ValueError(
            f"item type {item_type!r} requires {expected.__name__}, got {type(data).__name__}"
        )


class NewConversationItem(BaseModel):
    """
    An item that has not yet been persisted. No ID or timestamp.

    :param type: Item type, e.g. ``"message"``,
        ``"function_call"``.
    :param response_id: The task/response ID this item belongs to.
    :param data: The typed data payload (MessageData, etc.).
    :param created_by: Identity of the human actor who authored this
        item (e.g. ``"alice@example.com"``), or ``None`` for
        agent/tool/system-generated items and single-user mode.
        Mirrors the comment ``created_by`` contract.
    """

    type: str
    response_id: str
    data: ItemData
    created_by: str | None = None
    stable_id: str | None = None

    @model_validator(mode="after")
    def check_type_matches_data(self) -> NewConversationItem:
        _validate_type_matches_data(self.type, self.data)
        if self.stable_id is not None and not re.fullmatch(r"[0-9a-f]{32}", self.stable_id):
            raise ValueError("stable_id must be a 32-char lowercase hex string")
        return self


class ConversationItem(_OutputModel):
    """
    A persisted item with a store-assigned ID.

    :param id: Store-assigned item ID, e.g. ``"msg_abc123"``.
    :param type: Item type, e.g. ``"message"``,
        ``"function_call"``.
    :param status: Item status, e.g. ``"completed"``.
    :param response_id: The task/response ID this item belongs to.
    :param created_at: Unix epoch timestamp of creation.
    :param data: The typed data payload (MessageData, etc.).
    :param created_by: Identity of the human actor who authored this
        item, or ``None`` for agent/tool/system items and single-user
        mode. Lets owner and collaborator messages be distinguished.
    """

    id: str
    type: str
    status: str
    response_id: str
    created_at: int
    data: ItemData
    created_by: str | None = None
    deduplicated: bool = Field(default=False, exclude=True)

    @model_validator(mode="after")
    def check_type_matches_data(self) -> ConversationItem:
        _validate_type_matches_data(self.type, self.data)
        return self

    def to_api_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "response_id": self.response_id,
            "type": self.type,
            "status": self.status,
            "created_at": self.created_at,
            **self.data.model_dump(exclude_none=True, by_alias=True),
            **({"created_by": self.created_by} if self.created_by is not None else {}),
        }


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


class PaginatedList(_OutputModel):
    """
    A paginated list response following cursor-based pagination.

    :param object: Fixed resource type, always ``"list"``.
    :param data: Page of results. Items are heterogeneous
        (``ResponseObject``, ``ConversationObject``, ``FileObject``,
        or dicts) and list is invariant, so no single concrete type
        satisfies all callers.
    :param first_id: ID of the first item in the page, or ``None``
        if the page is empty, e.g. ``"resp_abc123"``.
    :param last_id: ID of the last item in the page, or ``None``
        if the page is empty, e.g. ``"resp_xyz789"``.
    :param has_more: Whether more items exist beyond this page.
    """

    object: str = "list"
    data: list[Any] = Field(default_factory=list)
    first_id: str | None = None
    last_id: str | None = None
    has_more: bool = False


class MCPServerSummary(_OutputModel):
    """
    Safe subset of an MCP server's configuration for API exposure.

    Header values are redacted (``"[REDACTED]"``) so callers can see
    which headers are configured without leaking the actual secrets.
    ``env`` is still fully excluded.

    :param name: Server name as declared in the agent spec,
        e.g. ``"github"``.
    :param transport: Transport type — ``"stdio"`` or ``"http"``.
    :param description: Optional free-text description from the
        spec, e.g. ``"GitHub MCP server"``. ``None`` when unset.
    :param url: HTTP(S) endpoint URL for ``transport="http"``
        servers, e.g. ``"https://mcp.example.com/sse"``. ``None``
        for stdio servers.
    :param headers: HTTP headers for ``transport="http"`` servers.
        Values are always ``"[REDACTED]"``; only the key names are
        exposed.
    :param command: Executable path for ``transport="stdio"``
        servers, e.g. ``"uvx"``. ``None`` for http servers.
    :param args: Command-line arguments for ``transport="stdio"``
        servers, e.g. ``["mcp-server-github"]``. Empty list
        when unset.
    """

    name: str
    transport: str
    description: str | None = None
    url: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    command: str | None = None
    args: list[str] = Field(default_factory=list)


class SkillSummary(_OutputModel):
    """
    Safe subset of a discovered skill for API exposure.

    Surfaces the skill name and one-line description so clients
    (e.g. the web composer's slash-command menu) can list which
    skills the session has access to. The full skill ``content``
    is intentionally omitted — it's only loaded server-side when
    the harness invokes the skill, and it can be large.

    :param name: Skill identifier as parsed from the SKILL.md
        frontmatter, e.g. ``"triage-issues"``. Lowercase
        kebab-case.
    :param description: One-line summary from the SKILL.md
        frontmatter, e.g. ``"Triage open GitHub issues in the
        repo."``.
    """

    name: str
    description: str


class NativeReasoningEffortOption(_OutputModel):
    """Reasoning-effort metadata advertised by a native model catalog."""

    reasoningEffort: str
    description: str | None = None
    model_config = ConfigDict(extra="allow", json_schema_extra=None)


class NativeModelOption(_OutputModel):
    """One runner-owned native model-picker row."""

    id: str
    model: str | None = None
    displayName: str | None = None
    defaultReasoningEffort: str | None = None
    supportedReasoningEfforts: list[NativeReasoningEffortOption] = Field(default_factory=list)
    isDefault: bool | None = None
    model_config = ConfigDict(extra="allow", json_schema_extra=None)


class PolicySummary(_OutputModel):
    """
    Safe subset of a policy's spec for API exposure.

    Exposes the policy name, type, and phases so the UI can
    display which guardrails are active on an agent. The full
    policy body (prompt text, callable path, label conditions)
    is intentionally excluded — this is a summary for display,
    not a full spec.

    :param name: Policy name as declared in the agent spec,
        e.g. ``"block_long_sleep"``.
    :param type: Policy type discriminator — ``"function"``
        or ``"prompt"``.
    :param on: List of phase selectors the policy fires on,
        e.g. ``["tool_call"]`` or ``["request", "response"]``.
    :param description: Short detail string about the policy
        implementation. For function policies: the callable
        dotted path. For prompt policies: the first line of
        the prompt. ``None`` when not available.
    """

    name: str
    type: str
    on: list[str]
    description: str | None = None


class AgentObject(_OutputModel):
    """
    API representation of a registered agent.

    :param id: Unique agent identifier, e.g. ``"ag_abc123"``.
    :param object: Fixed resource type, always ``"agent"``.
    :param name: Human-readable agent name,
        e.g. ``"research-agent"``.
    :param version: Monotonic version counter. Starts at 1,
        incremented on each update.
    :param description: Optional free-text description of the
        agent's purpose.
    :param created_at: Unix epoch timestamp of creation.
    :param updated_at: Unix epoch timestamp of the last update,
        or ``None`` if never updated.
    :param harness: The agent's harness/kind, e.g. ``"codex"``,
        ``"codex-native"``, or ``"claude-native"`` for
        ``executor.type: omnigent`` agents, otherwise the executor
        type (``"claude_sdk"``, ``"agents_sdk"``). ``None`` when the
        bundle cannot be loaded. Lets the Web UI Add Agent picker
        recognise an agent's kind (Codex vs Claude) without
        hardcoding by name slug.
    :param mcp_servers: MCP servers the agent is connected to
        (secret fields omitted). Empty list when the spec
        declares no MCP servers or when the bundle cannot be
        loaded.
    :param mcp_servers_editable: Whether the MCP list can be edited
        through the session UI. Built-in template agents are read-only;
        session-scoped uploaded agents are editable.
    :param policies: Guardrails policies declared on the agent.
        Each entry summarises the policy name, type, and
        phases. Empty list when the spec declares no policies
        or when the bundle cannot be loaded.
    :param skills: Skills bundled in the agent spec
        (``skills/<dir>/SKILL.md``). Lets the Web UI's
        new-session composer offer a slash-command menu before a
        session exists. ``GET /skills`` discovers host skills and,
        when given ``session_id``, merges the session's bundled skills.
        Empty list when the spec
        bundles no skills or when the bundle cannot be loaded.
    :param terminals: Terminal names declared in the spec's
        ``terminals:`` block, in declaration order, e.g.
        ``["shell"]``. The Web UI gates its "new terminal"
        affordance on this list (creation is only offered for
        agents with terminal access) and offers these names as
        the launchable choices. Empty list when the spec
        declares no terminals or when the bundle cannot be
        loaded.
    :param builtin: Whether this is a server-*seeded* built-in
        agent (deterministic, name-derived id) as opposed to an
        operator/user-registered template (random id, e.g. via
        ``omnigent server --agent``) or a session-scoped upload.
        The Web UI's new-session picker uses this to decide
        whether a same-named ``omnigent run`` upload may shadow
        the catalog entry: seeded built-ins are protected, while
        a user-registered template is superseded by a newer
        same-named upload. Always ``False`` for session-scoped
        agents.
    """

    id: str
    object: str = "agent"
    name: str
    version: int = 1
    description: str | None = None
    created_at: int
    updated_at: int | None = None
    harness: str | None = None
    mcp_servers: list[MCPServerSummary] = Field(default_factory=list)
    mcp_servers_editable: bool = False
    policies: list[PolicySummary] = Field(default_factory=list)
    skills: list[SkillSummary] = Field(default_factory=list)
    terminals: list[str] = Field(default_factory=list)
    builtin: bool = False


class CopyFilesRequest(_StrictRequestModel):
    """
    Request to copy files from a lineage ancestor into a session.

    The destination session is the path parameter; ``source_session_id``
    must be a STRICT ancestor of the destination up its
    ``parent_conversation_id`` chain (spawn lineage) — the destination may
    not name itself as the source. The copy creates new child-scoped rows —
    it does not grant cross-session read access.

    :param source_session_id: Session that owns the source files, e.g.
        ``"conv_parent"``. Must be a strict ancestor of the destination.
    :param file_ids: Non-empty, unique ids of the source-owned files to
        copy, e.g. ``["file_abc123"]``.
    """

    source_session_id: str
    file_ids: list[Annotated[str, Field(min_length=1)]] = Field(
        min_length=1, json_schema_extra={"uniqueItems": True}
    )

    @field_validator("file_ids")
    @classmethod
    def require_unique_file_ids(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("file_ids must be unique")
        return value


class CopiedFile(_OutputModel):
    """
    A single copied file's new identity and preserved metadata.

    :param new_id: The new child-scoped file id, e.g. ``"file_def456"``.
    :param filename: The copied file's name, carried over from the source.
    :param content_type: The copied file's MIME type, preserved from the
        source row so the caller need not re-fetch it or guess from the
        filename. ``None`` when the source row had no recorded type.
    """

    new_id: str
    filename: str
    content_type: str | None = None


class CopyFilesResponse(_OutputModel):
    """
    Result of a lineage-scoped file copy.

    :param object: Fixed type, always ``"session.files.copied"``.
    :param session_id: Destination session that now owns the copies.
    :param mapping: Map of source ``file_id`` to the copied file's new
        identity and preserved metadata (id, filename, content type), so a
        caller can attach the copy without a follow-up metadata fetch.
    """

    object: str = "session.files.copied"
    session_id: str
    mapping: dict[str, CopiedFile]


class SessionResourceObject(_OutputModel):
    """
    API representation of a session-scoped resource handle.

    :param id: Opaque resource identifier, e.g. ``"default"`` or
        ``"terminal_bash_s1"``.
    :param object: Fixed resource type, always ``"session.resource"``.
    :param type: Resource kind, initially ``"environment"``,
        ``"terminal"``, or ``"file"``.
    :param session_id: Owning session/conversation id.
    :param name: Human-readable display name. Not required to be
        globally unique.
    :param metadata: Resource-type-specific metadata.
    :param environment: For terminal resources, the environment id the
        terminal actually runs in. Omitted for non-terminal resources.
    """

    id: str
    object: Literal["session.resource"]
    type: Literal["environment", "terminal", "file"]
    session_id: str
    name: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    environment: str | None = None
    model_config = ConfigDict(
        extra="allow",
        strict=True,
        json_schema_extra=_omit_explicit_additional_properties,
    )


class SessionResourcePaginatedList(_OutputModel):
    """Public paginated list of session resources."""

    object: Literal["list"] = "list"
    data: list[SessionResourceObject] = Field(default_factory=list)
    first_id: str | None = None
    last_id: str | None = None
    has_more: bool = False


class ConversationDeleted(_OutputModel):
    """
    Confirmation payload returned after deleting a conversation.

    :param id: ID of the deleted conversation,
        e.g. ``"conv_abc123"``.
    :param object: Fixed resource type, always
        ``"conversation.deleted"``.
    :param deleted: Always ``True``.
    """

    id: str
    object: str = "conversation.deleted"
    deleted: bool = True


class ConversationRef(_OutputModel):
    """
    Lightweight reference to a conversation, used in request and
    response bodies where only the conversation ID is needed.

    :param id: Conversation identifier, e.g. ``"conv_abc123"``.
    """

    id: str


class ChildSessionSummary(_OutputModel):
    """
    Summary of a sub-agent (child) session under a parent session.

    Powers ``GET /v1/sessions/{id}/child_sessions``. Lets the web /
    REPL debug surface enumerate sub-agent calls spawned from a
    parent session without parsing parent ``function_call_output``
    JSON handles (the legacy TUI Ctrl+O path). The endpoint is the
    canonical "historical truth" source; the existing transient
    ``session.created`` SSE event handles live incremental updates.

    Fields are derived from the child :class:`Conversation` plus its
    latest :class:`Task` (newest by ``created_at``).

    :param id: Child conversation/session identifier,
        e.g. ``"conv_child123"``.
    :param object: Fixed resource type, always
        ``"child_session"``.
    :param parent_session_id: Parent conversation id (echo of the
        route's ``session_id`` path parameter), e.g.
        ``"conv_parent987"``. Stable join key for clients that
        cache child rows across multiple parents.
    :param title: Sub-agent title, ``"{agent_type}:{session_name}"``
        as written by :func:`omnigent.tools.builtins.spawn._spawn_one`,
        e.g. ``"researcher:auth"``. ``None`` only for legacy /
        malformed rows; the spawn path always sets it.
    :param tool: UI-facing sub-agent label. For Omnigent-spawned
        children this is derived from the prefix of ``title`` before
        the first ``":"``, e.g. ``"researcher"``. For Codex-native
        children this is the Codex-assigned ``agent_nickname`` when
        available, then ``agent_role``, then ``"Codex"``. Falls back
        to the raw title for legacy / malformed rows; ``None`` only
        when ``title`` itself is ``None`` or empty.
    :param session_name: Sub-agent instance name, the suffix of
        ``title`` after the first ``":"``, e.g. ``"auth"``. ``None``
        if ``title`` is ``None`` or missing a colon.
    :param kind: Conversation kind discriminator, always
        ``"sub_agent"`` for rows surfaced by this endpoint.
    :param created_at: Unix epoch timestamp of child creation.
    :param updated_at: Unix epoch timestamp of the child's most
        recent update.
    :param agent_id: Agent id recorded on the latest task,
        e.g. ``"ag_abc123"``. ``None`` if the child has no tasks
        yet (rare — ``_spawn_one`` creates a task atomically with
        the conversation).
    :param agent_name: Agent type recorded on the latest task,
        e.g. ``"researcher"``. Mirrors the ``tool`` prefix in
        ``title`` and is provided alongside it because the title
        is a denormalized string while ``agent_name`` is the
        durable per-task value.
    :param current_task_id: Latest task id for the child
        (newest by ``created_at``), e.g. ``"task_abc123"``.
        ``None`` if no tasks exist.
    :param current_task_status: Status of the latest task,
        e.g. ``"completed"``, ``"in_progress"``, ``"failed"``.
        ``None`` if no tasks exist.
    :param busy: ``True`` when the child's session loop is live.
        Mirrors the algorithm used by ``GET /v1/sessions/{id}`` to
        compute ``status``: read the live in-memory cache first
        (``"running"``/``"waiting"`` → busy), and fall back to the
        latest task's status on cache miss (``"queued"`` /
        ``"in_progress"`` → busy). For NO_DBOS sessions the tasks
        table is not populated during active runs, so the cache
        consult is what keeps the rail's "Working" badge correct.
    :param labels: Session-scoped guardrails labels on the child
        conversation (mirrors :class:`ConversationObject.labels`).
    :param last_task_error: Error details from the child's most recent
        failed run, e.g.
        ``{"code": "required_terminal_exited", "message": "..."}``.
        ``None`` when the child has no durable failure detail. This is
        the typed projection of runner-owned failure labels; clients
        should not parse those labels directly.
    :param last_message_preview: Single-line preview of the most
        recent message item in the child's conversation, truncated
        to ~150 chars with a trailing ellipsis when longer. ``None``
        when the child has no message items yet (rare — the spawn
        tool immediately commits a user message). Lets the UI
        render a real-time "what's the sub-agent saying right now"
        line without fetching the child's full item history.
    :param pending_elicitations_count: Number of approval / input
        prompts the child is currently blocked on, read from the
        server's :mod:`omnigent.runtime.pending_elicitations`
        index. ``> 0`` means the sub-agent is parked awaiting user
        input — the Agents rail renders an "awaiting input" badge so
        a fanned-out sub-agent that needs attention is visible
        without opening its chat. Mirrors
        :attr:`SessionListItem.pending_elicitations_count`.
    :param routed_model: Model this sub-agent runs on when one was pinned
        for it, e.g. ``"databricks-claude-opus-4-8"``. Read from the
        child's ``model_override`` — the field intelligent routing writes
        when it picks a model for a spawned child. ``None`` when the child
        inherits the parent/spec model.
    :param routing_decision_id: Identifier of the routing decision that
        produced :attr:`routed_model`, mirroring
        ``RoutingDecisionData.decision_id``. Read from the child's
        ``omnigent.routing.decision_id`` label, stamped when routing pins
        the model. ``None`` when the child was not routed.
    """

    id: str
    object: str = "child_session"
    parent_session_id: str
    title: str | None = None
    task_summary: str | None = None
    tool: str | None = None
    session_name: str | None = None
    kind: str = "sub_agent"
    created_at: int
    updated_at: int
    agent_id: str | None = None
    agent_name: str | None = None
    current_task_id: str | None = None
    current_task_status: str | None = None
    busy: bool = False
    labels: dict[str, str] = Field(default_factory=dict)
    last_task_error: dict[str, str] | None = None
    last_message_preview: str | None = None
    pending_elicitations_count: int = 0
    routed_model: str | None = None
    routing_decision_id: str | None = None


class UsageDetails(_OutputModel):
    """
    Breakdown of output token usage.

    :param reasoning_tokens: Number of tokens consumed by
        chain-of-thought reasoning.
    """

    reasoning_tokens: int = 0


class Usage(_OutputModel):
    """
    Token usage statistics for a response.

    :param input_tokens: Number of input (prompt) tokens consumed.
    :param output_tokens: Number of output (completion) tokens
        generated.
    :param output_tokens_details: Breakdown of output token usage
        (e.g. reasoning tokens).
    :param total_tokens: Sum of input and output tokens across all
        LLM sub-calls for this turn (billing total).
    :param context_tokens: Context-fill estimate for the next turn —
        set only by executors that make multiple LLM sub-calls per
        turn (e.g. ``openai-agents``).  For single-call executors
        this is absent and ``total_tokens`` serves the same purpose.
        The toolbar context ring and ``/context`` command use this
        field when present, falling back to ``total_tokens``.
    :param cache_read_input_tokens: Prompt tokens served from a
        provider prompt cache (cache hit), billed at a reduced rate.
        Reported by Anthropic-style providers as a count *separate*
        from ``input_tokens`` (which carries only the non-cached
        portion); ``0`` when the provider does not break out cache
        usage. Consumed by the cache-aware server-side cost path.
    :param cache_creation_input_tokens: Prompt tokens written to the
        provider prompt cache (cache creation), billed at a premium
        rate. Like ``cache_read_input_tokens``, this is separate from
        ``input_tokens``; ``0`` when not reported.
    :param model: The LLM model the harness actually used for this
        turn, e.g. ``"claude-opus-4-8"`` or ``"databricks-gpt-5-5"``.
        Reported by relay executors so the server-side cost path can
        price the turn even when the agent spec pins no ``llm.model``
        (e.g. supervisors that delegate / use the harness default).
        ``None`` when the executor doesn't report it; the cost path
        then falls back to the session override / spec model.
    :param cost_usd: Authoritative per-turn cost in USD reported
        directly by the harness/provider (e.g. GitHub Copilot's
        AI-credit total). When present, the server-side cost path uses
        it in preference to the catalog token-price estimate; ``None``
        when the harness doesn't report a cost (the common case, where
        cost is computed from token counts x catalog pricing).
    """

    input_tokens: int = 0
    output_tokens: int = 0
    output_tokens_details: UsageDetails = Field(default_factory=UsageDetails)
    total_tokens: int = 0
    context_tokens: int | None = None
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    model: str | None = None
    cost_usd: float | None = None


class ErrorDetail(_OutputModel):
    """
    Machine-readable error information attached to a failed response.

    :param code: Error code string, e.g. ``"server_error"``,
        ``"invalid_input"``.
    :param message: Human-readable error description. Always populated; older
        clients render this verbatim.
    :param title: Optional short headline naming what went wrong, e.g.
        ``"Claude Code can't run as root"``. Present when the runner
        recognized the failure (see ``omnigent.runner.launch_failure``); lets
        the UI show a clear card title instead of the raw ``code``.
    :param cause: Optional one/two-sentence explanation of why it failed.
        Paired with ``title``.
    :param remediation: Optional concrete next step to fix it, e.g. a command
        to run. ``None`` when there is no single clear fix.
    """

    code: str
    message: str
    title: str | None = None
    cause: str | None = None
    remediation: str | None = None


class IncompleteDetails(_OutputModel):
    """
    Details explaining why a response is incomplete.

    :param reason: Reason the response stopped early, e.g.
        ``"max_output_tokens"``, ``"max_tool_calls"``.
    """

    reason: str


class ResponseObject(_OutputModel):
    """
    API representation of a response (task execution result).

    :param id: Unique response identifier, e.g.
        ``"resp_abc123"``.
    :param object: Fixed resource type, always ``"response"``.
    :param status: Lifecycle status, one of ``"queued"``,
        ``"in_progress"``, ``"completed"``, ``"failed"``,
        ``"incomplete"``, ``"cancelled"``.
    :param model: Agent name that produced this response,
        e.g. ``"research-agent"``.
    :param created_at: Unix epoch timestamp of creation.
    :param completed_at: Unix epoch timestamp of completion, or
        ``None`` if not yet complete.
    :param output: Heterogeneous output items (messages,
        reasoning, function_calls) serialized as dicts; shape
        varies by item type. Empty for non-completed responses.
    :param background: Whether this response was created as a
        background task.
    :param store: Whether this response is persisted. Always
        ``True``.
    :param usage: Token usage statistics, or ``None`` if not
        yet available.
    :param previous_response_id: ID of the prior response in
        the conversation thread, or ``None`` for the first turn.
    :param conversation: Reference to the owning conversation.
    :param instructions: Per-request system instructions composed
        additively with the agent's own, or ``None``.
    :param reasoning: Reasoning configuration,
        e.g. ``{"effort": "medium"}``.
    :param error: Error details if the response failed.
    :param incomplete_details: Details if the response is
        incomplete (e.g. hit token limit).
    """

    id: str
    object: str = "response"
    status: str
    model: str
    created_at: int
    completed_at: int | None = None
    output: list[dict[str, Any]] = Field(default_factory=list)
    background: bool = False
    store: bool = True
    usage: Usage | None = None
    previous_response_id: str | None = None
    conversation: ConversationRef | None = None
    instructions: str | None = None
    reasoning: dict[str, str] | None = None
    error: ErrorDetail | None = None
    incomplete_details: IncompleteDetails | None = None


class FailedResponseObject(_OutputModel):
    """Response payload for failures raised before response allocation.

    Transport and setup failures can terminate a turn before the harness has
    assigned the metadata required by :class:`ResponseObject`. The remaining
    fields mirror that model so fully allocated failures retain their complete
    wire representation.

    :param id: Unique response identifier, e.g. ``"resp_abc123"``, or
        ``None`` when allocation did not complete.
    :param object: Fixed resource type, always ``"response"``.
    :param status: Lifecycle status, normally ``"failed"``.
    :param model: Agent name that produced the response, e.g.
        ``"research-agent"``, or ``None`` when resolution did not complete.
    :param created_at: Unix epoch timestamp of creation, or ``None`` when the
        response failed before creation.
    :param completed_at: Unix epoch timestamp of completion, or ``None``.
    :param output: Heterogeneous serialized output items accumulated before
        failure.
    :param background: Whether the response was created as a background task.
    :param store: Whether the response is persisted.
    :param usage: Token usage statistics, or ``None`` when unavailable.
    :param previous_response_id: ID of the prior response, or ``None``.
    :param conversation: Reference to the owning conversation, or ``None``.
    :param instructions: Per-request instructions override, or ``None``.
    :param reasoning: Reasoning configuration, or ``None``.
    :param error: Error details describing the failure, or ``None``.
    :param incomplete_details: Incomplete-response details, or ``None``.
    """

    id: str | None = Field(default=None, exclude_if=lambda value: value is None)
    object: str = "response"
    status: str
    model: str | None = Field(default=None, exclude_if=lambda value: value is None)
    created_at: int | None = Field(default=None, exclude_if=lambda value: value is None)
    completed_at: int | None = None
    output: list[dict[str, Any]] = Field(default_factory=list)
    background: bool = False
    store: bool = True
    usage: Usage | None = None
    previous_response_id: str | None = None
    conversation: ConversationRef | None = None
    instructions: str | None = None
    reasoning: dict[str, str] | None = None
    error: ErrorDetail | None = None
    incomplete_details: IncompleteDetails | None = None


class ElicitationResult(_StrictRequestModel):
    """
    Consumer reply to an outstanding elicitation.

    Field names + semantics mirror MCP's ``ElicitResult`` verbatim.
    Omnigent clients deliver this shape inside the session-scoped
    ``approval`` event body, alongside the ``elicitation_id``
    correlation key.

    :param action: User action per MCP semantics. ``"accept"`` =
        approved (form submitted / confirmation given).
        ``"decline"`` = explicit refusal. ``"cancel"`` = dismissed
        without an explicit choice (also the verdict the server
        synthesizes on elicitation timeout).
    :param content: Form data when ``action == "accept"`` and the
        ``requestedSchema`` had fields. ``None`` (or omitted) for
        binary approve/reject elicitations and for ``decline`` /
        ``cancel`` actions. Values are restricted to JSON scalars
        and string lists per the MCP spec.
    :param meta: Optional MCP result metadata. Codex uses
        ``_meta.persist`` to distinguish one-time, session-scoped,
        and persistent MCP tool approvals.
    """

    action: Literal["accept", "decline", "cancel"]
    content: dict[str, str | int | float | bool | list[str] | None] | None = None
    meta: dict[str, Any] | None = Field(default=None, alias="_meta")
    model_config = ConfigDict(extra="forbid", serialize_by_alias=True)

    @model_serializer(mode="wrap")
    def _omit_unset_meta(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = handler(self)
        if self.meta is None:
            data.pop("_meta", None)
            data.pop("meta", None)
        return data


class ServerSessionEventInputBase(BaseModel):
    type: str
    data: dict[str, Any] = Field(default_factory=dict)
    model_override: str | None = None
    tools: list[dict[str, Any]] | None = None
    created_by: str | None = None

    @field_validator("data")
    @classmethod
    def reject_framework_blocks(cls, data: dict[str, Any]) -> dict[str, Any]:
        reject_authored_framework_notices(data)
        return data


class _SessionMessageData(_StrictRequestModel):
    role: Literal["user"] = "user"
    content: list[dict[str, Any]]

    @field_validator("content")
    @classmethod
    def reject_framework_blocks(cls, content: list[dict[str, Any]]) -> list[dict[str, Any]]:
        reject_authored_framework_notices(content)
        return content


class SessionMessage(_StrictRequestModel):
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
        """Build a user message containing one ``input_text`` block."""
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
        """Build a user message from existing OmniGent content blocks."""
        return cls(
            data=_SessionMessageData(content=[dict(block) for block in content]),
            model_override=model_override,
            tools=[dict(tool) for tool in tools] if tools is not None else None,
        )


class _FunctionCallOutputInputData(_StrictRequestModel):
    call_id: str
    output: str


class FunctionCallOutput(_StrictRequestModel):
    type: Literal["function_call_output"] = "function_call_output"
    data: _FunctionCallOutputInputData


class _InterruptData(_StrictRequestModel):
    pass


class Interrupt(_StrictRequestModel):
    type: Literal["interrupt"] = "interrupt"
    data: _InterruptData = Field(default_factory=_InterruptData)


PublicSessionEventInput = Annotated[
    SessionMessage | FunctionCallOutput | Interrupt, Field(discriminator="type")
]


class SessionGitOptions(_StrictRequestModel):
    """
    Git worktree options for ``POST /v1/sessions``.

    Requires ``host_id`` to be set (and therefore ``workspace``, which
    is interpreted as the source repository directory). Two modes,
    selected by ``existing_worktree``:

    - **create** (default): the server creates a git worktree on the
      host for a new branch and starts the runner in that worktree
      instead of the picked directory.
    - **bind** (``existing_worktree=True``): ``workspace`` already IS a
      pre-existing worktree; no worktree is created. ``branch_name`` is
      recorded as the session's ``git_branch`` for display and opt-in
      cleanup, and ``base_branch`` must not be set.

    See designs/SESSION_GIT_WORKTREE.md.

    :param branch_name: In create mode, the new branch to create and
        check out, e.g. ``"feature/login"``. In bind mode, the branch
        already checked out in the existing worktree. Validated against
        git ref-format rules; invalid names fail with ``invalid_input``.
    :param base_branch: Optional base ref to branch from, e.g.
        ``"main"`` or ``"origin/main"``. ``None`` branches from the
        source repository's current ``HEAD``. Create mode only —
        invalid with ``existing_worktree``.
    :param existing_worktree: When ``True``, bind to the pre-existing
        worktree at ``workspace`` instead of creating one (see above).
    :param existing_branch: When ``True``, ``branch_name`` already
        exists and the host checks it out into a fresh worktree (the
        deleted-worktree recreate path) instead of creating a new
        branch. Create mode only; invalid with ``existing_worktree``
        and with ``base_branch`` (an existing branch has no base to
        fork).
    """

    branch_name: str
    base_branch: str | None = None
    existing_worktree: bool = False
    existing_branch: bool = False

    @model_validator(mode="after")
    def _check_existing_worktree(self) -> SessionGitOptions:
        if self.existing_worktree and self.base_branch is not None:
            raise ValueError("base_branch cannot be set when existing_worktree is true")
        if self.existing_branch and self.base_branch is not None:
            raise ValueError("base_branch cannot be set when existing_branch is true")
        if self.existing_branch and self.existing_worktree:
            raise ValueError("existing_branch and existing_worktree cannot both be true")
        return self


class SessionCreateRequestBase(_StrictRequestModel):
    agent_id: Any
    project_id: str | None = None
    title: str | None = Field(default=None, max_length=USER_SESSION_TITLE_MAX_CHARS)
    labels: dict[str, str] = Field(default_factory=dict)
    parent_session_id: str | None = None
    sub_agent_name: str | None = None
    host_type: Literal["external", "managed"] = "external"
    host_id: str | None = None
    sandbox_provider: str | None = None
    workspace: str | None = None
    workspaces: list[str] | None = None
    git: SessionGitOptions | None = None
    terminal_launch_args: list[str] | None = None
    model_override: str | None = None
    reasoning_effort: str | None = None
    cost_control_mode_override: str | None = None
    subagent_routing_override: str | None = None
    harness_override: str | None = None
    smart_routing_message: str | None = None

    @model_validator(mode="after")
    def _check_git_requires_host(self) -> Self:
        if self.git is not None and self.host_id is None and self.project_id is None:
            raise ValueError("git worktree creation requires host_id")
        return self

    def managed_repo_workspaces(self) -> list[str]:
        if self.workspaces:
            return list(self.workspaces)
        if self.workspace is not None:
            return [self.workspace]
        return []


class SessionCreateRequest(SessionCreateRequestBase):
    """Legacy create shape, preserving required-string ``agent_id``."""

    agent_id: str
    initial_items: list[PublicSessionEventInput] = Field(default_factory=list)


class ProjectSessionCreateRequest(SessionCreateRequestBase):
    """Project-opted create shape whose agent may be filled by the server.

    The public legacy :class:`SessionCreateRequest` deliberately keeps
    ``agent_id`` required so requests without ``project_id`` retain their exact
    validation and OpenAPI contract.
    """

    agent_id: str | None = None
    initial_items: list[PublicSessionEventInput] = Field(default_factory=list)


class SessionCreateMetadataBase(_StrictRequestModel):
    title: str | None = Field(default=None, max_length=USER_SESSION_TITLE_MAX_CHARS)
    project_id: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    reasoning_effort: str | None = None
    host_id: str | None = None
    workspace: str | None = None
    terminal_launch_args: list[str] | None = None
    parent_session_id: str | None = None
    host_type: Literal["external", "managed"] = "external"
    sandbox_provider: str | None = None


class SessionCreateMetadata(SessionCreateMetadataBase):
    """
    Metadata JSON part for multipart ``POST /v1/sessions``.

    The uploaded agent tarball supplies the agent spec. This JSON
    part carries only session-level metadata so request metadata
    cannot disagree with the agent bundle.

    :param title: Optional human-readable title for the session,
        e.g. ``"debugging auth flow"``.
    :param labels: Initial guardrails labels to set on the
        session. Empty dict (the default) starts with no labels.
    :param reasoning_effort: Optional per-session reasoning-effort
        hint. Accepted metadata values are ``"none"``,
        ``"minimal"``, ``"low"``, ``"medium"``, ``"high"``,
        ``"xhigh"``, ``"max"``, and ``"ultra"``. Provider-specific support is
        validated when a turn executes. ``None`` means use the agent
        default.
    :param host_id: Optional host to launch the runner on, e.g.
        ``"host_a1b2c3d4..."``. When set, the server generates a
        binding token, writes the expected runner_id to the session
        row, and sends a ``host.launch_runner`` frame to the host.
        ``None`` for CLI-initiated sessions where the caller
        manages runner spawning.
    :param workspace: Absolute path on the host where the runner
        should start, e.g. ``"/Users/corey/universe/src/foo"``.
        Required when ``host_id`` is set; validated against the
        uploaded agent's ``os_env.cwd`` boundary at session create
        (per designs/SESSION_WORKSPACE_SELECTION.md). Optional
        otherwise.
    :param terminal_launch_args: Optional pass-through CLI args for a
        native terminal wrapper (claude / codex), e.g.
        ``["--dangerously-skip-permissions"]``. Set at create-time so
        the runner has them before it boots. Bounds (count / length)
        are validated server-side. ``None`` for non-native sessions.
        See designs/NATIVE_RUNNER_SERVER_LAUNCH.md.
    :param parent_session_id: Optional parent session id, e.g.
        ``"conv_abc123"``. When set, the new session is created as a
        sub-agent child of that session (``kind="sub_agent"``) and
        inherits the parent's runner binding for co-location. The
        caller must have READ access to the parent. ``None``
        creates a top-level session.
    :param host_type: How the session's host is obtained — ``"external"``
        (the default: the caller manages the runner, e.g. a local
        ``omnigent run``) or ``"managed"`` (the server provisions a
        sandbox host). The uploaded bundle's session-scoped agent runs
        on the provisioned sandbox; its spec is fetched by the managed
        runner over its tunnel, same as any session-scoped agent.
    :param sandbox_provider: Which configured sandbox provider to
        provision on ``host_type: "managed"`` (one of the server's
        ``sandbox_providers``); ``None`` takes the server's first. Only
        valid with ``host_type: "managed"``.
    """


class SessionForkRequestBase(_StrictRequestModel):
    title: str | None = Field(default=None, max_length=USER_SESSION_TITLE_MAX_CHARS)
    agent_id: str | None = None
    up_to_response_id: str | None = None
    model_override: str | None = None
    reasoning_effort: str | None = None
    terminal_launch_args: list[str] | None = None
    codex_bypass_sandbox: bool = False
    host_type: Literal["external", "managed"] = "external"
    sandbox_provider: str | None = None
    workspace: str | None = None
    side_chat: bool = False


class SessionForkRequest(SessionForkRequestBase):
    """
    Request body for ``POST /v1/sessions/{source_id}/fork``.

    Creates a deep copy of an existing session's items into a new
    session. All fields are optional.

    :param title: Title for the forked session. When ``None``, the
        server derives ``"Fork of <source_title>"``.
    :param agent_id: Built-in agent to bind the fork to, switching it
        away from the source's agent/harness (e.g. fork a Claude session
        into a Codex one, or a Claude-SDK session into Claude Code). When
        ``None``, the fork keeps the source's agent. Must be a built-in
        agent (one listed by ``GET /v1/agents``).
    :param up_to_response_id: Truncation point for the copied history,
        e.g. ``"resp_abc123"``. When set, only items up to and including
        the last item of that response are copied — items after it are
        dropped from the fork. When ``None`` (default), the full history
        is copied.
    :param model_override: Per-session LLM model override to apply to the
        fork, e.g. ``"claude-opus-4-7"``. **Omitting** the field inherits
        the source's model (same as today, within the same provider
        family); an explicit value overrides it, and a clear alias
        (``"default"``, ``"off"``, ``"reset"``) resets the fork to the
        bound agent's default. Validated against a conservative model-id
        charset. Set by the web fork dialog's model picker.
    :param reasoning_effort: Per-session reasoning-effort override to apply
        to the fork, e.g. ``"high"``. **Omitting** the field inherits the
        source's effort; an explicit value overrides it, and a clear alias
        (``"default"``, ``"off"``, ``"reset"``) resets it. Validated
        against the shared effort vocabulary; provider support is enforced
        at launch. Set by the web fork dialog's effort picker.
    :param terminal_launch_args: Per-session native-terminal pass-through
        args to apply to the fork, e.g. ``["--permission-mode", "auto"]``
        (the fork dialog's permission-/approval-mode selector).
        **Omitting** the field keeps today's behavior — the source's args
        are carried on a same-agent fork and dropped on an agent switch. A
        list (including ``[]``, which clears them) replaces them wholesale.
        Bounds (count / length) are validated server-side.
    :param codex_bypass_sandbox: Opt-in for the DANGEROUS codex-native
        full-bypass (``--dangerously-bypass-approvals-and-sandbox``) on the
        fork. The source's bypass label is ALWAYS dropped on a fork (a
        bypass-armed source can never silently re-arm its clone), so this is
        the only way a fork enables it — an explicit, banner-gated opt-in
        from the dialog, mirroring the new-session approval selector. ``True``
        stamps ``omnigent.codex_native.bypass_sandbox`` on the fork; ``False``
        / omitted leaves the fork in Codex's normal approval/sandbox stance.
        Only meaningful for a codex-native target; ignored otherwise.
    :param host_type: How the fork's host is obtained — ``"external"``
        (the default: the caller binds one afterwards, via
        ``POST /v1/hosts/{host_id}/runners`` from the web dialog or
        ``PATCH /v1/sessions/{id}`` from the REPL) or ``"managed"`` (the
        server provisions a sandbox host for the fork, the same
        background launch a ``host_type: "managed"`` create schedules).
    :param sandbox_provider: Which configured sandbox provider to
        provision on ``host_type: "managed"`` (one of the server's
        ``sandbox_providers``); ``None`` takes the server's first. Only
        valid with ``host_type: "managed"``.
    :param workspace: Git repository URL (optionally ``#<branch>``) the
        server clones into the fork's sandbox as its working directory,
        e.g. ``"https://github.com/org/repo#release-1.2"``. **Omitting**
        the field inherits the repository the source session recorded, so
        cloning a sandbox session lands the fork in the same checkout; an
        explicit value overrides it and an explicit ``null`` gives the
        fork an empty sandbox. Only valid with ``host_type: "managed"`` —
        an external fork's directory is chosen when it binds a host.
    """


class CreatedSessionResponse(_OutputModel):
    """
    Response body for multipart ``POST /v1/sessions``.

    :param session_id: Identifier of the newly created session,
        e.g. ``"conv_abc123"``.
    :param agent_id: Identifier of the session-scoped agent created
        from the uploaded bundle, e.g. ``"ag_abc123"``.
    :param agent_name: Agent name loaded from the uploaded bundle's
        spec, e.g. ``"code-assistant"``.
    """

    session_id: str
    agent_id: str
    agent_name: str


SandboxLaunchStage = Literal[
    "provisioning", "cloning", "starting", "connecting", "ready", "failed"
]


class SandboxStatus(_OutputModel):
    """
    Managed-sandbox launch progress for a ``host_type="managed"`` session.

    Carried on the session snapshot only while the session's
    background sandbox launch is in flight or has failed; ``None``
    for sessions without a managed launch and once the launch
    succeeds (the session then looks like any host-bound session).

    :param stage: Current launch stage, e.g. ``"provisioning"`` —
        one of :data:`SandboxLaunchStage`, in pipeline order:
        ``provisioning`` (creating the sandbox) → ``cloning``
        (cloning the repository workspace; skipped when the session
        has none) → ``starting`` (starting the in-sandbox host) →
        ``connecting`` (launching the agent runner) → ``ready`` /
        ``failed``.
    :param error: Failure detail when ``stage == "failed"``, e.g.
        ``"managed sandbox launch failed: spend limit reached"``.
        ``None`` otherwise.
    """

    stage: SandboxLaunchStage
    error: str | None = None


class ModelUsage(_OutputModel):
    """
    Cumulative token/cost usage attributed to a single LLM model.

    One value in the ``usage_by_model`` map on :class:`SessionResponse` /
    :class:`SessionUsageEvent`, keyed by the raw harness-reported model id
    (e.g. ``"claude-sonnet-4-6"``, ``"databricks-gpt-5-5"``). Counts are
    summed over the session's subtree (itself + sub-agent descendants), so a
    parent folds in sub-agents that ran a different model. Token buckets
    mirror the flat per-session breakdown.

    :param input_tokens: Cumulative non-cached input (prompt) tokens for this
        model over the subtree, e.g. ``12000``. ``None`` when not recorded.
    :param output_tokens: Cumulative output (completion) tokens, e.g.
        ``3400``. ``None`` when not recorded.
    :param total_tokens: Cumulative total tokens (counts cache buckets too,
        as the harness reports), e.g. ``15400``. ``None`` when not recorded.
    :param cache_read_input_tokens: Cumulative tokens read from the prompt
        cache, e.g. ``8000``. ``None`` when not recorded.
    :param cache_creation_input_tokens: Cumulative tokens written to the
        prompt cache, e.g. ``2000``. ``None`` when not recorded.
    :param total_cost_usd: Cumulative USD spend attributed to this model,
        e.g. ``0.42``. Present **only when this model's turns were priced**
        (same "priced ⟺ key present" contract as the session total); ``None``
        when the model is unpriced, so the sum of priced per-model costs
        equals the session ``total_cost_usd``.
    """

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    cache_read_input_tokens: int | None = None
    cache_creation_input_tokens: int | None = None
    total_cost_usd: float | None = None


class BackgroundTaskInfo(_OutputModel):
    """
    One still-running background shell from the claude-native ``Stop`` hook.

    Claude Code leaves finished shells in the hook's ``background_tasks``
    array, so the list surfaced here is filtered to the non-terminal ones —
    its length matches the ``background_task_count`` tally. Every field is
    best-effort: the hook shape is external, so an entry missing one field
    still yields a usable row (e.g. a ``description`` with no ``command``).

    :param id: Opaque per-shell identifier, e.g. ``"abc123"``.
    :param type: Task kind, e.g. ``"shell"``.
    :param status: Per-task status, e.g. ``"running"``.
    :param description: Human-readable label, e.g. ``"Wait for CI"``.
    :param command: Command the shell is running, e.g. ``"sleep 120"``.
    """

    id: str | None = None
    type: str | None = None
    status: str | None = None
    description: str | None = None
    command: str | None = None


class McpServerStartup(_OutputModel):
    """
    One MCP server's startup state within a ``session.mcp_startup`` event.

    :param status: Latest startup state reported by the harness, mirroring
        Codex's ``McpServerStartupState`` enum.
    :param error: Failure detail when ``status == "failed"``, e.g.
        ``"handshaking with MCP server failed"``. ``None`` otherwise.
    """

    status: Literal["starting", "ready", "failed", "cancelled"]
    error: str | None = None


class SessionResponse(_OutputModel):
    """
    API representation of a session.

    Returned by ``POST /v1/sessions``, ``GET /v1/sessions/{id}``,
    and ``PATCH /v1/sessions/{id}``.

    :param id: Unique session identifier (also the underlying
        conversation ID), e.g. ``"conv_abc123"``.
    :param agent_id: Durable identifier of the bound agent,
        e.g. ``"ag_abc123"``. Stable across renames of the
        agent.
    :param agent_name: Human-readable name of the bound agent,
        e.g. ``"research-agent"``. Loaded from the agent row at
        snapshot-build time. ``None`` when the agent row cannot
        be found (deleted or orphaned session).
    :param status: Session lifecycle status. One of
        ``"idle"`` (no loop running), ``"running"`` (loop
        executing), ``"waiting"`` (loop parked on background
        work / sub-agents), or ``"failed"`` (terminal failure).
        Current read paths collapse ``"waiting"`` -> ``"running"``
        before building this snapshot; the literal stays a superset
        of what the runtime can produce so a server that forwards
        the raw status never 500s on serialization.
    :param background_task_count: Background shells (claude-native) still
        running as of the last status edge, so a reload re-shows "N shells
        still running" even though the session has settled to ``"idle"``.
        ``None`` (the default / omitted) when no shells are tracked.
    :param background_tasks: Per-shell detail for the running tally above,
        so a reload can restore each shell's description/command. ``None`` when
        none are tracked (or when an older runner reported only the count).
    :param created_at: Unix epoch seconds of creation.
    :param title: Optional human-readable title, e.g.
        ``"debugging auth flow"``. ``None`` when unset.
    :param labels: Session-scoped guardrails labels. Empty dict
        when no labels have been written.
    :param runner_id: Runner currently bound to this session, e.g.
        ``"runner_abc123"``. ``None`` until a client binds one via
        ``PATCH /v1/sessions/{id}``.
    :param host_id: Host that launched (or should launch) the
        runner for this session, e.g. ``"host_a1b2c3d4..."``.
        ``None`` for CLI-initiated sessions.
    :param runner_online: Strict runner liveness — ``True`` iff a
        runner tunnel is currently registered for this session.
        This is the sole reachability signal: ``True`` means the
        client can chat normally. It does **not** fold in
        host-relaunch optimism (a dead runner on a live host reads
        ``False`` here, not ``True``) — the open-session view pairs
        it with ``host_online`` to decide what to show. ``None``
        when the server has no runner liveness lookup wired.
    :param host_online: Whether the session's host tunnel is live
        (status online and fresh within the host liveness TTL).
        ``None`` when the session has no ``host_id`` (CLI/local).
        Used only to choose what the open view shows when
        ``runner_online`` is ``False`` — host alive ⇒ "send a
        message to wake the runner"; host dead ⇒ "reconnect /
        fork". Never participates in the reachability decision.
    :param host_resumable: Whether this session is bound to a dormant
        managed host the server can wake in place (its provider sets
        :attr:`SandboxLauncher.can_resume`). The open view reads it only
        when ``host_online`` is ``False``, to split a confirmed host-down
        into a recoverable "asleep" state (send a message — the relaunch
        path resumes the sandbox) versus the terminal ``host_offline``
        dead-end (reconnect from your machine / fork). ``False`` for
        non-managed or non-resumable hosts.
    :param reasoning_effort: Per-session reasoning-effort hint.
        Accepted metadata values are ``"none"``, ``"minimal"``,
        ``"low"``, ``"medium"``, ``"high"``, ``"xhigh"``, and
        ``"max"``. Provider-specific support is validated when a
        turn executes. ``None`` means use the agent default.
    :param items: Committed conversation items in chronological
        order. Empty for a freshly created session.
    :param sub_agent_name: For sub-agent sessions, the sub-agent
        type name within the parent's spec tree, e.g.
        ``"summarizer"``. ``None`` for top-level sessions.
    :param parent_session_id: For sub-agent sessions, the parent
        conversation's id, e.g. ``"conv_parent987"``. ``None`` for
        top-level sessions. Lets clients identify a session as a
        child and link back to its parent without an extra
        round-trip — the same conversation row exposes this via
        ``parent_conversation_id`` internally.
    :param root_conversation_id: The id of this session's spawn-tree
        root, e.g. ``"conv_root1"``. Equals ``id`` for top-level
        sessions; for sub-agents it points at the top-level ancestor.
        Lets orchestration tools (e.g. ``sys_session_close``) confirm
        a target shares the caller's spawn tree over the REST path.
        ``None`` only when the underlying row predates the
        ``root_conversation_id`` column (not expected post-migration).
    :param permission_level: The requesting user's numeric
        permission level on this session: ``1`` = read, ``2`` =
        edit, ``3`` = manage. ``None`` when permissions are
        disabled (single-user mode without a permission store).
    :param llm_model: The model this session is actually on. When the
        harness has reported one (``reported_model``, written by
        ``external_model_change``), that verbatim value serves here
        and is the only model value clients display; otherwise the
        bound agent spec's model, e.g.
        ``"anthropic/claude-sonnet-4-6"``. ``None`` when neither
        exists.
    :param harness: The bound agent's canonical harness, e.g.
        ``"claude-sdk"`` or ``"openai-agents"``. Lets the client
        render the active credential for the correct provider
        family instead of inferring it from the model string (which
        is wrong when the agent declares no model). ``None`` when
        the agent cannot be looked up.
    :param model_override: Per-session LLM model override,
        e.g. ``"claude-opus-4-7"``. ``None`` means no override is
        active (the agent's ``llm_model`` applies). Set via
        ``PATCH /v1/sessions/{id}`` or the REPL's ``/model``
        command; both write the same column so the web UI and
        the TUI stay in sync.
    :param cost_control_mode_override: Per-session cost-control
        switch: ``"on"`` activates the spec's configured cost-control
        mode, ``"off"`` disables cost control for this session.
        ``None`` means no override is active (the spec default
        applies). Set at create time or via
        ``PATCH /v1/sessions/{id}`` (the web "Cost Optimized"
        toggle); read by the cost-control advisor pipeline.
    :param subagent_routing_override: Per-session subagent-routing
        switch, two-state: ``"on"`` routes subagent spawns, and ``"off"``
        or ``None`` (unset) both leave them unrouted — the in-session
        "Subagent routing" row renders either as "Default". ``None`` on
        a row created before this became explicit inherits nothing.
        Stamped ``"on"`` at create for Smart Routing sessions; also set
        via ``PATCH /v1/sessions/{id}``.
    :param share_workspace_files: Whether the owner opted into letting
        view-level collaborators browse the workspace (Files/Changes/GitHub
        surfaces). ``False`` by default — read grants share the conversation
        only. The web share dialog reads this to render the toggle, and the
        rail reads it to decide whether to mount the file surfaces for a
        view-only viewer.
    :param context_window: The model's context window size in tokens
        as looked up server-side from litellm's registry (or from the
        ``AP_CONTEXT_WINDOW_OVERRIDE`` env var), e.g. ``200_000``.
        ``None`` when the model is not in litellm's registry and no
        override is set.
    :param last_total_tokens: Total token count (input + output) from
        the most recently completed task's ``usage``, e.g. ``45231``.
        ``None`` when no task has completed yet. Lets clients seed
        their context-ring on conversation resume without waiting for
        the next ``response.completed`` SSE event.
    :param total_cost_usd: Cumulative LLM spend for this session in
        USD, e.g. ``0.42``. ``None`` when the session is **unpriced**
        — no turn has been priced yet (the model is absent from the
        pricing catalog, or no usage has been recorded) — so clients
        render "—" rather than a misleading ``$0.00``. Server-computed
        (cache-aware for relay/codex, exact billing for claude-native),
        the same total the cost-budget policy gates on. Lets clients
        seed their cost indicator on resume without waiting for the
        next ``session.usage`` SSE event.
    :param usage_by_model: Per-model breakdown of the same subtree usage,
        keyed by the raw harness model id, e.g.
        ``{"claude-sonnet-4-6": ModelUsage(input_tokens=12000, ...)}``.
        ``None`` when no per-model usage has been recorded (older sessions
        recorded before this field existed, or before the first turn). Lets
        the UI show which models a session spent its tokens / budget on.
    :param last_task_error: Error details from the most recently
        failed task. Only present when ``status == "failed"`` and
        the task stored an error. Lets clients display the failure
        reason on historical load without relying on the transient
        ``response.error`` SSE event (which may have been emitted
        before the web client subscribed). Format mirrors the
        ``RetryErrorDetail`` SSE shape:
        ``{"code": "executor_error", "message": "..."}``.
        ``None`` in all other cases.
    :param external_session_id: Runtime-native session id this
        conversation wraps, e.g. a Claude Code session uuid for
        ``omnigent claude`` sessions. ``None`` for regular
        AP-only conversations. Populated by the wrapper bridge.
    :param terminal_launch_args: Pass-through CLI args the native
        terminal wrapper (claude / codex) was launched with, e.g.
        ``["--dangerously-skip-permissions"]``. ``None`` for
        non-native sessions or a native session launched with none.
        Lets the launcher reproduce the command on resume.
    :param pending_elicitations: Outstanding approval prompts on
        this session at the moment the snapshot was built — the
        original ``response.elicitation_request`` event dicts.
        Lets the UI render the ApprovalCard on cold load, since
        the live SSE stream has no replay and a prompt emitted
        before the user opened the chat would otherwise vanish.
        Empty list when no prompts are outstanding. Sourced from
        the Omnigent server's in-memory
        :mod:`omnigent.runtime.pending_elicitations` index.
    :param pending_inputs: Un-consumed web-composer user messages on
        native-terminal (claude-native / codex-native) sessions at
        snapshot time, each ``{"pending_id", "content"}``. Native
        sessions don't persist a web message at POST time (the
        transcript forwarder is the single writer), so a client that
        posted then navigated away / rebound would lose its optimistic
        bubble; replaying these re-hydrates it. Empty list otherwise.
        Sourced from the in-memory
        :mod:`omnigent.runtime.pending_inputs` index.
    :param workspace: Absolute path on disk where the runner cd's,
        e.g. ``"/Users/corey/universe/src/foo"``. Set when the
        session was bound to a host workspace at create-time, or
        when the CLI captured ``os.getcwd()`` at session-create.
        Always ``None`` when not yet validated against a host. When a
        git worktree was created for the session, this is the
        worktree directory path.
    :param git_branch: Git branch checked out in the session's
        worktree, e.g. ``"feature/login"``. Set only when the
        session was created with a server-created git worktree;
        ``None`` otherwise. The Web UI uses a non-``None`` value to
        offer the "delete local branch" cleanup checkbox on session
        delete. See designs/SESSION_GIT_WORKTREE.md.
    :param archived: Whether the session is archived. Archived
        sessions are hidden from the default sidebar listing and
        surface only behind the "Show archived" toggle. ``False``
        for normal sessions. Toggled via ``PATCH /v1/sessions/{id}``.
    :param todos: Current native Plan items reported by a harness. Each has
        ``content``, ``status``, and ``activeForm``. Persisted in conversation
        metadata; empty before the first report or after an explicit clear.
    :param model_options: Runner-owned model-picker options for native
        sessions. Claude supplies launch-time gateway aliases; Codex includes
        each model's supported reasoning efforts. Empty while unavailable.
    :param terminal_pending: ``True`` while the runner is auto-creating
        a terminal-first session's terminal (claude-native /
        codex-native), so the Web UI shows a spinner on the Terminal
        pill instead of a silent greyed-out button. Cleared to
        ``False`` once the terminal lands or auto-create fails; from
        then on the client relies purely on whether a terminal resource
        exists. Sourced from the Omnigent server's in-memory
        ``_session_terminal_pending_cache`` at snapshot build time, so a
        client connecting mid-spin-up still sees the spinner.
    :param sandbox_status: Managed-sandbox launch progress while the
        session's background sandbox launch is in flight or has
        failed — see :class:`SandboxStatus`. ``None`` for sessions
        without a managed launch and once the launch succeeds.
        Sourced from the Omnigent server's in-memory
        ``_session_sandbox_status_cache`` at snapshot build time, so
        a client opening the session mid-launch sees the current
        stage.
    :param active_response_id: Response id of the turn currently in
        flight, or ``None`` when the session is idle. Sourced from the
        server's ``_session_active_response_cache`` at snapshot build
        time so a client connecting mid-turn can reopen a streaming
        ``activeResponse`` — the SSE stream is snapshot + live tail with
        no replay, so the turn-start ``running`` edge that carried this
        id is not re-sent on reconnect. Today only native-terminal
        forwarders (claude-native) stamp a turn id on their status
        edges; other harnesses leave this ``None``.
    :param updated_at: Unix epoch timestamp of the last persisted session
        activity. Advances when conversation items are appended and on session
        metadata edits (rename, agent switch, archive); a mid-stall rename
        therefore resets the clock, so an orchestrator treating this as a pure
        item-append heartbeat should account for that. Can be compared across
        snapshots independently of lifecycle status.
    """

    id: str
    agent_id: str
    agent_name: str | None = None
    status: Literal["idle", "running", "waiting", "failed"]
    background_task_count: int | None = None
    background_tasks: list[BackgroundTaskInfo] | None = None
    created_at: int
    updated_at: int | None = None
    title: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    runner_id: str | None = None
    host_id: str | None = None
    runner_online: bool | None = None
    host_online: bool | None = None
    host_resumable: bool = False
    reasoning_effort: str | None = None
    items: list[ConversationItem] = Field(default_factory=list)
    permission_level: int | None = None
    sub_agent_name: str | None = None
    kind: str = "default"
    parent_session_id: str | None = None
    root_conversation_id: str | None = None
    llm_model: str | None = None
    harness: str | None = None
    model_override: str | None = None
    cost_control_mode_override: str | None = None
    subagent_routing_override: str | None = None
    share_workspace_files: bool = False
    context_window: int | None = None
    last_total_tokens: int | None = None
    total_cost_usd: float | None = None
    usage_by_model: dict[str, ModelUsage] | None = None
    last_task_error: dict[str, str] | None = None
    external_session_id: str | None = None
    terminal_launch_args: list[str] | None = None
    pending_elicitations: list[dict[str, Any]] = Field(default_factory=list)
    pending_inputs: list[dict[str, Any]] = Field(default_factory=list)
    workspace: str | None = None
    git_branch: str | None = None
    archived: bool = False
    todos: list[dict[str, Any]] = Field(default_factory=list)
    model_options: list[NativeModelOption] = Field(default_factory=list)
    terminal_pending: bool = False
    sandbox_status: SandboxStatus | None = None
    mcp_startup: dict[str, McpServerStartup] | None = None
    active_response_id: str | None = None
    project_id: str | None = None


class UpdateSessionRequest(_StrictRequestModel):
    """
    Request body for ``PATCH /v1/sessions/{id}``.

    The Alpha runner-state pivot makes this endpoint the mutable
    session affinity primitive when ``runner_id`` is provided. The
    server validates that the runner is online, then replaces
    ``conversations.runner_id``. Existing session metadata updates
    remain supported for clients that update title, labels, or
    reasoning effort through the sessions API.

    :param runner_id: Identifier of a registered runner, e.g.
        ``"runner_abc123"``. ``None`` leaves runner binding
        unchanged.
    :param title: New title, e.g. ``"debugging auth flow"``.
        ``None`` leaves unchanged.
    :param labels: Guardrails labels to upsert. Merges with existing
        labels; keys not present are left untouched.
    :param reasoning_effort: Per-session reasoning-effort hint.
        Accepted metadata values are ``"none"``, ``"minimal"``,
        ``"low"``, ``"medium"``, ``"high"``, ``"xhigh"``, and
        ``"max"``. Provider-specific support is validated when a
        turn executes. Clear aliases such as ``"default"`` remove
        the session override. ``None`` leaves unchanged.
    :param model_override: Per-session LLM model override, e.g.
        ``"claude-opus-4-7"``. The value is forwarded as-is to the
        executor at turn start; the server does not enumerate valid
        models. Clear aliases such as ``"default"``, ``"off"``, or
        ``"reset"`` remove the override (matching the REPL's
        ``/model`` semantics). ``None`` leaves unchanged.
    :param collaboration_mode: Codex-native collaboration-mode string.
        ``"plan"`` enters Plan mode and ``"default"`` returns to Default
        mode for subsequent Codex turns. Only valid for sessions stamped
        with the codex-native wrapper label. Omitted leaves unchanged.
    :param permission_mode: Claude-native permission mode to switch a
        running session to, e.g. ``"auto"``. Only the modes Claude Code's
        shift+tab cycle can reach are accepted (``default``,
        ``acceptEdits``, ``plan``, ``auto``) — ``dontAsk`` and
        ``bypassPermissions`` are launch-only. Only valid for sessions
        stamped with the claude-native wrapper label. Unlike the other
        fields here the switch is applied by the live TUI, so a failure
        to reach the mode is surfaced as an error rather than persisted.
        Omitted leaves unchanged.
    :param approval_mode: Codex-native approval mode to switch a running
        session to, one of ``"ask-for-approval"``, ``"approve-for-me"``,
        ``"full-access"``, ``"read-only"`` — Codex's own ``/permissions``
        presets (the set is codex-version-dependent, so an older build may not
        offer every one). Only valid for sessions stamped with the codex-native
        wrapper label. The runner applies it by driving Codex's ``/permissions``
        popup and confirms the switch echoed before returning, so a failure to
        reach the mode surfaces as an error. The confirmed mode is stored on the
        read-back label only (Codex owns the durable approval state), so it is
        not written to ``terminal_launch_args``. Omitted leaves unchanged.
    :param cost_control_mode_override: Per-session cost-control
        switch: ``"on"`` activates the spec's configured cost-control
        mode, ``"off"`` disables cost control for this session.
        Explicit JSON ``null`` clears the override back to the spec
        default; omitting the field leaves it unchanged (``"off"`` is
        a real value here, so the field's *presence* — not a clear
        alias — is the clear signal, unlike ``model_override``).
    :param subagent_routing_override: Per-session subagent-routing
        switch: ``"on"`` routes subagent spawns, ``"off"`` leaves them
        unrouted. Explicit JSON ``null`` clears the override, which lands
        the session on Default (the same behavior as ``"off"`` — nothing
        is inherited); omitting the field leaves it unchanged (same
        presence-is-the-clear-signal rule as
        ``cost_control_mode_override``). Effective on the next spawn, so
        it can be changed at any point in a session.
    :param share_workspace_files: Opt-in that lets people with *view*
        (read-only) access browse the session's workspace files. ``True``
        turns sharing on, ``False`` turns it off (back to edit-only, the
        default), ``None`` leaves it unchanged. Manage-gated — it sits with
        the grant/revoke and public-access controls that decide who can see
        the session. Never widens absolute-path browsing, which stays
        owner-only.
    :param external_session_id: Runtime-native session id captured
        by a wrapper bridge (e.g. Claude Code's session uuid for
        ``omnigent claude`` sessions). Idempotent on same-value
        writes; the server rejects attempts to overwrite an
        already-set different value with ``invalid_input`` to
        surface programmer errors. ``None`` leaves unchanged.
    :param terminal_launch_args: Per-session native-terminal
        pass-through args, e.g. ``["--dangerously-skip-permissions"]``.
        A list (including ``[]``) replaces the stored value wholesale
        — resume is last-write-wins, never an append. Bounds (count /
        length) are validated server-side. ``None`` leaves unchanged.
    :param silent: When ``True``, persist metadata changes but skip
        the runner-side side effects — specifically the
        native ``/effort`` / ``/model`` / Codex collaboration-mode
        forwards into the live runtime. Used by automatic bind-time
        handoffs (web's sticky-pref apply on session switch, the
        REPL's pre-create ``/model`` snapshot) where injecting a
        visible slash command into a freshly-spawned pane would
        render as an unexpected "Command model X" item before the
        user has sent anything. Default ``False`` preserves the
        user-driven picker / ``/model`` behaviour where the live
        forward IS the desired feedback.
    :param archived: New archived state. ``True`` archives (hides the
        session from the default sidebar listing), ``False`` unarchives,
        ``None`` leaves unchanged. Owner-only (unlike ``title``, which
        needs only edit access).
    :param project_id: File this session into a first-class project (see
        ``designs/PROJECTS_PRD.md``). A non-empty id moves the session into
        that project; the empty string ``""`` unfiles it. **Omitting** the
        field leaves membership unchanged; an explicit ``null`` is rejected
        (400) so it can't silently unfile. Owner-only: because projects are
        owner-private, only the session owner may file it, and only into a
        project they own — the server verifies both. Independent of the
        legacy ``omni_project`` label, which is set via ``labels``.
    """

    runner_id: str | None = None
    title: str | None = Field(default=None, max_length=USER_SESSION_TITLE_MAX_CHARS)
    labels: dict[str, str] | None = None
    reasoning_effort: str | None = None
    model_override: str | None = None
    collaboration_mode: str | None = None
    permission_mode: str | None = None
    approval_mode: str | None = None
    cost_control_mode_override: str | None = None
    subagent_routing_override: str | None = None
    share_workspace_files: bool | None = None
    external_session_id: str | None = None
    terminal_launch_args: list[str] | None = None
    archived: bool | None = None
    project_id: str | None = None
    silent: bool = False


class SessionListItem(_OutputModel):
    """
    Lightweight session summary for ``GET /v1/sessions`` list responses.

    Same shape as :class:`SessionResponse` minus ``items``.

    :param id: Session/conversation identifier,
        e.g. ``"conv_abc123"``.
    :param agent_id: Durable identifier of the bound agent.
    :param agent_name: Human-readable name of the bound agent,
        e.g. ``"research-agent"``. ``None`` when the agent row
        cannot be found.
    :param status: Derived session lifecycle status.
    :param created_at: Unix epoch seconds of creation.
    :param updated_at: Unix epoch seconds of last update.
    :param title: Optional human-readable title.
    :param labels: Session-scoped guardrails labels.
    :param runner_id: Runner currently bound to the session.
    :param host_id: Host that launched the runner for this session.
    :param runner_online: Strict runner liveness — ``True`` iff a
        runner tunnel is currently registered for this session.
        Matches ``GET /health``'s ``runner_online`` value. Strict:
        a dead runner on a live host reads ``False`` here (no
        host-relaunch optimism folded in), unlike the legacy
        conflated value. ``None`` when the server has no runner
        liveness lookup wired.
    :param host_online: Whether the session's host tunnel is live
        (status online and fresh within the host liveness TTL).
        ``None`` when the session has no ``host_id`` (CLI/local).
        Distinguishes "runner down but host can relaunch" from
        "host offline" for the open-session view; not used by the
        sidebar.
    :param reasoning_effort: Per-session reasoning-effort hint.
    :param permission_level: The requesting user's numeric
        permission level on this session: ``1`` = read, ``2`` =
        edit, ``3`` = manage. ``None`` when permissions are
        disabled.
    :param owner: The user_id of the session owner, or ``None``
        when permissions are disabled. Included so the sidebar
        can display the owner without a separate API call.
    :param external_session_id: Runtime-native session id this
        conversation wraps, e.g. a Claude Code session uuid for
        ``omnigent claude`` sessions. ``None`` for regular
        AP-only conversations. Lets the sidebar / picker render
        a runtime badge without a follow-up GET.
    :param pending_elicitations_count: Number of approval prompts
        currently waiting on this session. Powers the sidebar's
        "needs attention" badge so a user with several sessions
        running can tell which ones are blocked on them without
        opening each chat. Sourced from the Omnigent server's in-memory
        :mod:`omnigent.runtime.pending_elicitations` index,
        which mirrors every ``response.elicitation_request`` event
        passing through ``session_stream`` and decrements when a
        verdict is dispatched. ``0`` when the session has no
        outstanding elicitations.
    :param workspace: Absolute path on disk where the runner cd's,
        e.g. ``"/Users/corey/universe/src/foo"``. ``None`` for
        sessions that haven't been bound to a host workspace.
    :param git_branch: Git branch checked out in the session's
        worktree, e.g. ``"feature/login"``. Set only when the
        session was created with a server-created git worktree;
        ``None`` otherwise. The Web UI uses a non-``None`` value to
        offer the "delete local branch" cleanup checkbox on session
        delete. See designs/SESSION_GIT_WORKTREE.md.
    :param archived: Whether the session is archived. Archived
        sessions are returned by ``GET /v1/sessions`` only when the
        request passes ``include_archived=true``; the sidebar groups
        them into a dedicated "Archived" section. ``False`` for
        normal sessions.
    :param comments_count: Total number of review comments (any
        status) on this session. Together with
        ``comments_updated_at`` it forms a change fingerprint: an
        add or edit bumps the timestamp, a delete changes the count,
        so the web client can invalidate its cached comment list
        when either field changes in a ``WS /v1/sessions/updates``
        frame. ``0`` when the session has no comments or the server
        has no comment store wired.
    :param comments_updated_at: Unix epoch **microseconds** of the
        most recently mutated comment on this session (max
        ``updated_at`` across its comments). Microsecond precision
        keeps back-to-back mutations within one second
        distinguishable while staying an exact integer in JavaScript;
        clients only compare it for change. ``None`` when the session
        has no comments or the server has no comment store wired.
    :param viewer_last_seen: The *requesting user's* "last seen"
        wall-clock baseline in seconds for this session, or ``None``
        when they have never seen it. Per-viewer (built from the
        server's in-memory per-user read-state, written by
        ``PUT /v1/sessions/{id}/read-state``); the unread dot shows
        when ``updated_at > viewer_last_seen`` and the session is
        finished. In-memory only — resets on a server restart.
    :param viewer_unread: Whether the *requesting user* explicitly
        marked this session unread. Per-viewer; lifts the active-row
        dot suppression on the client. ``False`` by default.
    :param search_snippet: Excerpt of the chat content that matched the
        request's ``search_query``, centered on the match with ``…``
        marking elided ends, so the search UI can show *where* a session
        matched in its body. Present whenever the query hit an item body
        (even if the title also matched); ``None`` on non-search reads and
        when only the title matched.
    """

    id: str
    agent_id: str
    agent_name: str | None = None
    status: Literal["idle", "running", "waiting", "failed"]
    created_at: int
    updated_at: int
    title: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    runner_id: str | None = None
    host_id: str | None = None
    runner_online: bool | None = None
    host_online: bool | None = None
    reasoning_effort: str | None = None
    permission_level: int | None = None
    owner: str | None = None
    external_session_id: str | None = None
    pending_elicitations_count: int = 0
    workspace: str | None = None
    git_branch: str | None = None
    archived: bool = False
    comments_count: int = 0
    comments_updated_at: int | None = None
    viewer_last_seen: int | None = None
    viewer_unread: bool = False
    search_snippet: str | None = None
    parent_session_id: str | None = None
    project_id: str | None = None


class SessionList(_OutputModel):
    """Paginated list of sessions; ``data`` is a page of ``SessionListItem``."""

    object: Literal["list"] = "list"
    data: list[SessionListItem] = Field(default_factory=list)
    first_id: str | None = None
    last_id: str | None = None
    has_more: bool = False


class ChildSessionList(_OutputModel):
    """Paginated list of child sessions; ``data`` is a page of ``ChildSessionSummary``."""

    object: Literal["list"] = "list"
    data: list[ChildSessionSummary] = Field(default_factory=list)
    first_id: str | None = None
    last_id: str | None = None
    has_more: bool = False


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
    _PendingElicitationState | _ResolvedElicitationState, Field(discriminator="status")
]


class _SSEEventBase(_OutputModel):
    """
    Common base for every SSE event payload model.

    All events share two ambient fields:

    - ``type``: the event-type discriminator literal (defined per
      subclass so :data:`ServerStreamEvent` can dispatch).
    - ``sequence_number``: monotonic per-stream sequence number
      assigned by the SSE serializer at emit time
      (``_format_sse`` in ``omnigent/server/routes/sessions.py``).
      Producers leave it ``None``; the route serializer populates
      it on the wire. ``None`` on session-scoped events emitted
      directly by the runtime (the session stream does not number
      events).

    Subclasses MUST declare ``type`` as ``Literal[...]`` so the
    discriminated-union machinery can route incoming dicts. The
    ``model_config`` is forward-compatible — see the module
    docstring for the rationale.

    :param sequence_number: Per-stream monotonic counter assigned
        by the SSE serializer, e.g. ``42``. ``None`` on the
        producer side (before serialization) and on session-scoped
        events that the runtime publishes directly without
        sequencing.
    """

    sequence_number: int | None = None


class SessionStatusEvent(_SSEEventBase):
    """
    Session lifecycle status transition.

    Emitted by the runtime / session route handler at every
    transition between ``launching`` / ``running`` / ``waiting`` /
    ``idle`` / ``failed``. Wire shape is
    FLAT (not enveloped): ``{"type": "session.status",
    "conversation_id": "...", "status": "...",
    "sequence_number": null}``.

    The ``waiting`` value is emitted by the runtime's parent agent
    loop when it parks on the ``async_work_complete`` drain
    (``_drain_async_completions(block_for_one=True)`` in
    ``omnigent/runtime/workflow.py``) — i.e. while the parent
    turn is suspended waiting for background tools or sub-agents
    to complete. Per the session-rearchitecture spec §3
    ("Event types and direction"), ``waiting`` is the
    session-status companion of the spec's ``turn.waiting``
    transient — clients should render the session as actively
    blocked-on-async-work, distinct from ``running``. When the
    drain wakes (a child completed), the runtime emits a follow-up
    ``running`` to resume.

    :param type: Always ``"session.status"``.
    :param conversation_id: The conversation/session identifier
        whose status changed, e.g. ``"conv_abc123"``.
    :param status: New session status. ``"launching"`` (session or
        child task created, but no concrete harness start observed),
        ``"idle"`` (no loop running), ``"running"`` (loop executing),
        ``"waiting"`` (parent turn parked on the async-work drain), or
        ``"failed"`` (terminal failure).
    :param response_id: Optional active response id for terminal-backed
        integrations, e.g. ``"codex_turn_abc123"``. Clients use it to
        associate coarse session status edges with the assistant bubble
        they describe. ``None`` for ordinary in-process runtime edges.
    :param error: Machine-readable failure detail, present only
        when ``status == "failed"``. Carries the message the
        runner attached when a turn died — most importantly a
        SETUP-phase failure (spec resolution, spawn-env build)
        that ends the turn before any ``response.failed`` event
        is emitted. ``None`` for every non-failed transition.
        Clients render ``error.message`` as the terminal error
        line; without it a setup failure shows as a silent end.
    :param background_task_count: Background shells still running at this
        edge (claude-native ``Stop`` hook). ``None`` when the edge carries no
        information (leave the sticky tally untouched); ``0`` clears it.
    :param background_tasks: Per-shell detail backing that tally, so the UI
        can name each running shell. ``None`` when the edge reports no detail
        (an older runner may send only the count).
    :param blocked_on: Short human phrase naming what a still-``running``
        session is parked on, e.g. ``"permission prompt"`` or
        ``"dialog open"``. Set by terminal-backed integrations whose agent
        can block on a dialog the web UI does not mirror, so the client can
        say *why* nothing is moving instead of showing a bare spinner.
        ``None`` whenever the session is not parked. Unrelated to the
        ``waiting`` status above, which means the turn has ended and only
        background work remains.

    Category: **transient** (SSE-only). Status is rederived on
    reconnect from the cached last-relayed turn lifecycle event
    or by re-querying the runner; not persisted by the runtime.
    """

    type: Literal["session.status"]
    conversation_id: str
    status: Literal["idle", "launching", "running", "waiting", "failed"]
    response_id: str | None = None
    error: ErrorDetail | None = None
    background_task_count: int | None = None
    background_tasks: list[BackgroundTaskInfo] | None = None
    blocked_on: str | None = None


class SessionUsageEvent(_SSEEventBase):
    """
    Token-usage update from a terminal-backed integration.

    Emitted after an ``external_session_usage`` POST from an
    out-of-AP runtime (e.g. the ``omnigent claude`` transcript
    forwarder). Either field may be absent; clients should leave
    cached values untouched for missing fields.

    :param type: Always ``"session.usage"``.
    :param conversation_id: Session identifier.
    :param context_tokens: ``input + cache_creation + cache_read``
        from the latest assistant ``message.usage``. ``None`` on a
        window-only broadcast.
    :param context_window: Resolved window in tokens (e.g. 200_000
        normally, 1_000_000 with ``opus[1m]`` / ``sonnet[1m]``).
        ``None`` on a tokens-only broadcast.
    :param total_cost_usd: Cumulative session spend in USD after this
        update, e.g. ``0.42`` — the server-computed total the
        cost-budget policy gates on. Present **only when the session
        is priced**; omitted (``None``, stripped by ``exclude_none``)
        when unpriced or on a broadcast that carries no cost change,
        so the client keeps its prior value (the snapshot seeds the
        initial "—" for an unpriced session). Once a session is priced
        the total only grows, so it never reverts to unpriced.
    :param usage_by_model: Per-model breakdown of the same subtree usage
        after this update, keyed by raw harness model id, e.g.
        ``{"claude-sonnet-4-6": ModelUsage(input_tokens=12000, ...)}``.
        ``None`` (stripped by ``exclude_none``) on a broadcast that carries
        no per-model change, so the client keeps its cached map.

    Category: **transient** (SSE-only). On reconnect, clients seed
    the ring from the session snapshot's ``last_total_tokens`` and
    ``context_window``, the cost indicator from ``total_cost_usd``,
    and the per-model token breakdown from ``usage_by_model``.
    """

    type: Literal["session.usage"]
    conversation_id: str
    context_tokens: int | None = None
    context_window: int | None = None
    total_cost_usd: float | None = None
    usage_by_model: dict[str, ModelUsage] | None = None


class SessionModelEvent(_SSEEventBase):
    """
    Active-model report from a harness integration.

    Emitted after an ``external_model_change`` POST from a native
    forwarder or when an SDK relay reports its concrete model in terminal
    response usage. Every surface re-renders its model display from this.

    :param type: Always ``"session.model"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param model: The model the harness reports the session is on,
        VERBATIM in the harness's own spelling, e.g.
        ``"claude-opus-4-8[1m]"`` or ``"gpt-5.6-luna"`` — never
        collapsed to a picker alias.

    Category: **transient** (SSE-only). The server also writes
    ``reported_model`` on the conversation (served on the snapshot's
    ``llm_model``), so on reconnect clients restore the display from
    the snapshot rather than from a replayed event.
    """

    type: Literal["session.model"]
    conversation_id: str
    model: str


class SessionTitleEvent(_SSEEventBase):
    """
    Session-title update from a terminal-backed integration.

    Emitted after an ``external_session_title`` POST from the
    ``omnigent claude`` transcript forwarder when the operator renames
    the session inside the Claude Code pane (``/rename``). Lets the web
    session list show the new name without a reload.

    :param type: Always ``"session.title"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param title: Title the session is now on, e.g. ``"auth-refactor"``.

    Category: **transient** (SSE-only). The server also writes ``title``
    on the conversation, so on reconnect clients restore the name from
    the session snapshot rather than from a replayed event.
    """

    type: Literal["session.title"]
    conversation_id: str
    title: str


class SessionReasoningEffortEvent(_SSEEventBase):
    """
    Active reasoning-effort update from a terminal-backed integration.

    Emitted after an ``external_reasoning_effort_change`` POST from a native
    terminal forwarder when the user changes the thinking level inside the
    terminal UI. Lets the web effort picker reflect a TUI-side switch without
    a reload.

    :param type: Always ``"session.reasoning_effort"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param reasoning_effort: Reasoning effort now active for the session, e.g.
        ``"medium"``, or ``None`` when Codex cleared to its default.

    Category: **transient** (SSE-only). The server also writes
    ``reasoning_effort`` on the conversation, so on reconnect clients restore
    the selection from the session snapshot rather than from a replayed event.
    """

    type: Literal["session.reasoning_effort"]
    conversation_id: str
    reasoning_effort: str | None = None


class SessionCollaborationModeEvent(_SSEEventBase):
    """
    Active collaboration-mode update from a Codex-native session.

    Emitted after the web UI toggles Codex collaboration mode, and after the
    Codex forwarder observes a ``thread/settings/updated`` notification from
    the native Codex TUI. Lets connected clients show a clear Plan-mode
    indicator without a reload.

    :param type: Always ``"session.collaboration_mode"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param mode: The active collaboration mode string, e.g. ``"plan"`` or
        ``"default"``.

    Category: **transient** (SSE-only). The server also writes
    ``omnigent.codex_native.collaboration_mode`` on the conversation labels,
    so reconnect clients restore the same state from the session snapshot.
    """

    type: Literal["session.collaboration_mode"]
    conversation_id: str
    mode: str


class SessionPermissionModeEvent(_SSEEventBase):
    """
    Active permission-mode update from a claude-native session.

    Emitted after the web UI switches the mode, and after the Claude forwarder
    observes a different mode in the pane footer — a shift+tab pressed inside
    the TUI, which Omnigent has no other way to see. Lets the composer's mode
    picker track the pane without a reload.

    :param type: Always ``"session.permission_mode"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param permission_mode: The active mode, e.g. ``"auto"`` or ``"plan"``.

    Category: **transient** (SSE-only). The server also writes
    ``omnigent.claude_native.permission_mode`` on the conversation labels, so
    reconnecting clients restore the same state from the session snapshot.
    """

    type: Literal["session.permission_mode"]
    conversation_id: str
    permission_mode: str


class SessionCodexApprovalModeEvent(_SSEEventBase):
    """
    Active approval/sandbox-mode update from a codex-native session.

    Emitted after the web UI switches the mode, and after the Codex forwarder
    observes a ``thread/settings/updated`` notification — an approval change the
    user made inside Codex's ``/permissions`` popup, which Omnigent has no other
    way to see. Lets the composer's approval picker track the thread without a
    reload.

    :param type: Always ``"session.codex_approval_mode"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param approval_mode: The active mode, one of ``"ask-for-approval"``,
        ``"approve-for-me"``, ``"full-access"``, ``"read-only"``.

    Category: **transient** (SSE-only). The server also writes
    ``omnigent.codex_native.approval_mode`` on the conversation labels (and
    ``terminal_launch_args``), so reconnecting clients restore the same state
    from the session snapshot.
    """

    type: Literal["session.codex_approval_mode"]
    conversation_id: str
    approval_mode: str


class SessionAgentChangedEvent(_SSEEventBase):
    """
    Bound-agent change on a live session.

    Emitted by the switch-agent route after the session's agent binding
    is rewritten in place. Connected clients re-derive their cached
    session state (harness presentation labels, bound agent id/name)
    from a fresh snapshot — the chat UI's native-vs-SDK message
    lifecycle depends on those labels, so a stale cache drops the first
    post-switch message (it reappears only when the transcript
    round-trip lands).

    :param type: Always ``"session.agent_changed"``.
    :param conversation_id: Session identifier, e.g. ``"conv_abc123"``.
    :param agent_id: The session-scoped clone now bound to the session,
        e.g. ``"ag_abc123"``.
    :param agent_name: Display name of the agent the session now runs,
        e.g. ``"claude-native-ui"``. Deliberately the clean target-agent
        name — not the clone row's ``"… (switch ag_…)"`` disambiguation
        name — because clients render it verbatim.

    Category: **transient** (SSE-only). The switch is persisted on the
    conversation row, so on reconnect clients read the new binding from
    the session snapshot rather than from a replayed event.
    """

    type: Literal["session.agent_changed"]
    conversation_id: str
    agent_id: str
    agent_name: str


class SessionTodosEvent(_SSEEventBase):
    """
    Plan/TODO update from a native terminal-backed session.

    Emitted after an ``external_session_todos`` POST from a native
    harness forwarder, which captures structured Plan updates from
    Claude or Codex and forwards them to the Omnigent server. Lets web
    render a live todo panel in the right column without polling.

    :param type: Always ``"session.todos"``.
    :param conversation_id: Session identifier,
        e.g. ``"conv_abc123"``.
    :param todos: Current native Plan/TODO items from a harness.
        Each entry is a raw dict with ``content`` (str),
        ``status`` (``"pending"`` | ``"in_progress"`` |
        ``"completed"``), and ``activeForm`` (str, display activity)
        keys, e.g. ``[{"content": "Fix the bug", "status":
        "in_progress", "activeForm": "Fixing the bug"}]``.

    Category: **transient** (SSE-only). On reconnect, clients seed
    the panel from the session snapshot's ``todos`` field, which is
    restored from persisted metadata at snapshot build time.
    """

    type: Literal["session.todos"]
    conversation_id: str
    todos: list[dict[str, Any]]


class SessionTerminalPendingEvent(_SSEEventBase):
    """
    Terminal spin-up status for a terminal-first session.

    Two sources emit this event:

    1. The Omnigent server at ``POST /v1/sessions`` for host-launched
       terminal-first sessions — the earliest possible point, before
       the runner even starts, so the spinner appears immediately on
       session create rather than after the runner boots.
    2. The Omnigent relay when the runner's ``session.terminal_pending`` frame
       arrives — covers non-host-launched sessions (e.g. server-dispatched
       sub-agents) and carries the authoritative ``pending=False`` clear
       emitted by the runner's ``finally`` block.

    Together they allow web to show a spinner on the Terminal pill
    while the backend boots the terminal instead of a silent greyed-out
    button, and to distinguish "still starting up" from "no terminal"
    (killed or never created).

    :param type: Always ``"session.terminal_pending"``.
    :param conversation_id: Session identifier,
        e.g. ``"conv_abc123"``.
    :param pending: ``True`` while the terminal is being created;
        ``False`` once it lands or auto-create fails.

    Category: **transient** (SSE-only). On reconnect, clients seed the
    spinner from the session snapshot's ``terminal_pending`` field,
    which is populated by ``_session_terminal_pending_cache`` at
    snapshot build time.
    """

    type: Literal["session.terminal_pending"]
    conversation_id: str
    pending: bool


class SessionSandboxStatusEvent(_SSEEventBase):
    """
    Managed-sandbox launch progress for a ``host_type="managed"`` session.

    A managed create returns before its sandbox exists; the Omnigent
    server emits this event as the background launch pipeline advances
    so the Web UI can show live provisioning progress on the session
    page instead of a silent dead chat: sandbox provision → repository
    clone → host startup → runner connect → ready, or a terminal
    failure with the reason.

    :param type: Always ``"session.sandbox_status"``.
    :param conversation_id: Session identifier,
        e.g. ``"conv_abc123"``.
    :param stage: The launch stage just entered, e.g.
        ``"provisioning"`` — see :class:`SandboxStatus` for the full
        pipeline order.
    :param error: Failure detail when ``stage == "failed"``, e.g.
        ``"managed sandbox launch failed: spend limit reached"``.
        ``None`` otherwise.

    Category: **transient** (SSE-only). On reconnect, clients seed the
    progress indicator from the session snapshot's ``sandbox_status``
    field, which is populated by ``_session_sandbox_status_cache`` at
    snapshot build time.
    """

    type: Literal["session.sandbox_status"]
    conversation_id: str
    stage: SandboxLaunchStage
    error: str | None = None


class SessionMcpStartupEvent(_SSEEventBase):
    """
    Per-MCP-server startup progress for a native harness session.

    A codex-native session brings up its configured MCP servers when its
    Codex thread starts; slow or failing servers previously left the web
    session looking hung with no signal. The native forwarder mirrors
    Codex's ``mcpServer/startupStatus/updated`` notifications as
    ``external_mcp_startup`` posts, republished here so the web UI can
    show which servers are still starting and which failed or were
    cancelled.

    :param type: Always ``"session.mcp_startup"``.
    :param conversation_id: Session identifier,
        e.g. ``"conv_abc123"``.
    :param servers: Latest per-server startup map, e.g.
        ``{"safe": {"status": "starting", "error": None}}``.

    Category: **transient** (SSE + snapshot cache). Not persisted; a
    client connecting mid-startup seeds from the session snapshot's
    ``mcp_startup`` field and updates live off this event.
    """

    type: Literal["session.mcp_startup"]
    conversation_id: str
    servers: dict[str, McpServerStartup]


class SessionModelOptionsEvent(_SSEEventBase):
    """
    Signal that a native session's model catalog has resolved.

    Model options are fetched from the bound runner and cached on the session
    snapshot. The initial snapshot can return an empty list while this
    background fetch is in flight; this event tells connected clients to
    re-read the snapshot and apply its now-populated ``model_options``.

    Carries no payload beyond the conversation id. The snapshot's
    ``model_options`` field remains the source of truth.

    :param type: Always ``"session.model_options"``.
    :param conversation_id: Session identifier,
        e.g. ``"conv_abc123"``.

    Category: **transient** (SSE-only). On reconnect, clients seed
    Native model / effort controls from the session snapshot.
    """

    type: Literal["session.model_options"]
    conversation_id: str


class SessionInputConsumedPayload(_OutputModel):
    """
    Inner payload of a :class:`SessionInputConsumedEvent`.

    Emitted by the sessions route handler at the moment a client
    input is persisted into ``conversation_items``. Carries the
    persisted-item shape so clients can render the input (e.g.
    the user's message bubble) at the moment of acceptance.

    :param item_id: Stable identifier of the conversation item
        just persisted, e.g. ``"item_abc123"``.
    :param type: The item type discriminator — ``"message"`` for
        user messages, ``"function_call_output"`` for tool
        results, etc. Mirrors
        :class:`omnigent.server.schemas.SessionEventInput`'s
        ``type`` field.
    :param data: Decoded item payload, e.g.
        ``{"role": "user", "content": [{"type": "input_text",
        "text": "Hello"}]}``. Heterogeneous and ``type``-specific.
    :param created_by: Email of the human actor who posted the item,
        e.g. ``"alice@example.com"``. ``None`` for agent/tool/system
        items and single-user mode. Mirrors
        :meth:`ConversationItem.to_api_dict` for live attribution.
    :param cleared_pending_id: When this consumed message drains a
        :mod:`omnigent.runtime.pending_inputs` entry (a native-
        terminal web message round-tripping back from the transcript),
        the drained entry's id, e.g. ``"pending_a1b2c3"``. Lets a
        client drop the matching optimistic bubble by id instead of
        by position. ``None`` for non-native messages and for messages
        that matched no pending entry (e.g. typed directly in the TUI).
    """

    item_id: str
    type: str
    data: dict[str, Any]
    created_by: str | None = None
    cleared_pending_id: str | None = None


class SessionInputConsumedEvent(_SSEEventBase):
    """
    A queued input item was materialized into conversation history.

    Emitted by ``POST /v1/sessions/{id}/events`` once per accepted
    input item at the moment it is persisted into conversation
    history (either onto a steered active turn or as the seed item
    of a freshly-started one). Wire shape uses the NESTED envelope:
    ``{"type": "session.input.consumed", "data":
    <:class:`SessionInputConsumedPayload`>, "sequence_number":
    null}``.

    The event name is **provisional** — it may be renamed in a
    future revision. Consumers should reference
    :data:`SessionInputConsumedEvent` (or its ``type`` literal)
    rather than hardcoding the wire string.

    :param type: Always ``"session.input.consumed"``.
    :param data: The decoded queued-item payload — see
        :class:`SessionInputConsumedPayload`.
    """

    type: Literal["session.input.consumed"]
    data: SessionInputConsumedPayload


class SessionInterruptedPayload(_OutputModel):
    """
    Inner payload of a :class:`SessionInterruptedEvent`.

    Built by ``_publish_interrupted`` in
    ``omnigent/server/routes/sessions.py``.

    :param requested_at: Unix epoch seconds when the interrupt
        request reached the server, e.g. ``1704067200``.
    :param response_id: Optional active response id for terminal-backed
        integrations, e.g. ``"codex_turn_abc123"``.
    """

    requested_at: int
    response_id: str | None = None


class SessionInterruptedEvent(_SSEEventBase):
    """
    User-triggered cancel reached the loop.

    Emitted by ``_publish_interrupted`` in
    ``omnigent/server/routes/sessions.py`` when a client posts
    a ``{"type": "interrupt"}`` to ``POST
    /v1/sessions/{id}/events``. Co-emitted with
    :class:`IncompleteEvent` (with the underlying response carrying
    ``incomplete_details.reason == "user_interrupt"``) so off-the-
    shelf Responses parsers still close cleanly. Wire shape uses
    the NESTED envelope verbatim from the existing emit site.

    :param type: Always ``"session.interrupted"``.
    :param data: The interrupt metadata — see
        :class:`SessionInterruptedPayload`.
    """

    type: Literal["session.interrupted"]
    data: SessionInterruptedPayload


class SessionCreatedEvent(_SSEEventBase):
    """
    A child (sub-agent) session was spawned from this session.

    Emitted by ``omnigent/tools/builtins/spawn.py:_spawn_one``
    onto the **parent** session's conversation stream after the
    child conversation row is created and the child task has been
    started. Per the session-rearchitecture spec §3 ("Event types
    and direction") and §7 ("Flow: client interacts with
    sub-agent"), this lets clients watching the parent session's
    SSE subscribe directly to the child's stream without polling
    history for the tunneled ``function_call`` item.

    The wire shape is FLAT (not enveloped):
    ``{"type": "session.created", "conversation_id": <parent>,
    "child_session_id": <child>, "agent_id": <agent or None>,
    "parent_session_id": <parent>, "sequence_number": null}``.

    The existing tunneled ``function_call`` ConversationItem
    (carried inside :class:`OutputItemDoneEvent`) is retained
    for compatibility — clients that don't yet implement the
    "subscribe to child stream" pattern can keep rendering sub-
    agent calls from the parent's persistent history.

    :param type: Always ``"session.created"``.
    :param conversation_id: The PARENT session/conversation id —
        this event rides the parent's stream, e.g.
        ``"conv_parent123"``.
    :param child_session_id: The newly-created child session id,
        e.g. ``"conv_child456"``. Same as ``conversation_id`` on
        the child's own stream when consumers pivot to it.
    :param agent_id: Registered agent id the child runs as,
        e.g. ``"agent_xyz"``. ``None`` is permitted only for
        legacy spawn paths that did not record an agent id;
        new code MUST set it.
    :param parent_session_id: Echo of ``conversation_id`` for
        consumers that key on a dedicated "parent" field rather
        than the carrier ``conversation_id``. Always equal to
        ``conversation_id``; included for forward-compat with
        clients that may relay these events across stream
        boundaries.

    Category: **transient** (SSE-only). The corresponding durable
    record of "a child session exists" lives in the conversation
    store as the child conversation row itself
    (``parent_conversation_id`` foreign key) and the parent's
    tunneled ``function_call`` item — reconnecting clients
    discover children by walking the parent's persisted history,
    not by replaying this event.
    """

    type: Literal["session.created"]
    conversation_id: str
    child_session_id: str
    agent_id: str | None = None
    parent_session_id: str | None = None


class SessionSupersededEvent(_SSEEventBase):
    """
    This conversation was superseded by another and clients should
    follow to it.

    Emitted by ``_publish_session_superseded`` in
    ``omnigent/server/routes/sessions.py`` when the claude-native
    forwarder rotates a session away on a Claude ``/clear`` (the old
    conversation keeps its history but the live terminal moves to a
    fresh conversation — see ``_post_clear_supersession`` in
    ``omnigent/claude_native_forwarder.py``). A client actively viewing
    the superseded conversation auto-redirects to ``target_conversation_id``.

    Category: **transient** (SSE-only), live-only by design. There is no
    SSE replay: a client that connects after the rotation does not get
    this event. The durable counterpart is the persisted notice message
    appended to the old conversation (a ``message`` item linking to the
    new conversation), which a reloading client renders instead of being
    force-redirected.

    The wire shape is FLAT (not enveloped):
    ``{"type": "session.superseded", "conversation_id": <old>,
    "target_conversation_id": <new>, "reason": "clear"}``.

    :param type: Always ``"session.superseded"``.
    :param conversation_id: The superseded (old) conversation id this
        event rides the stream of, e.g. ``"conv_old"``.
    :param target_conversation_id: The conversation to follow to, e.g.
        ``"conv_new"``.
    :param reason: Why the session was superseded. Currently always
        ``"clear"`` (a Claude Code ``/clear``); kept as a field so the
        client can branch on future supersession causes.
    """

    type: Literal["session.superseded"]
    conversation_id: str
    target_conversation_id: str
    reason: Literal["clear"] = "clear"


class SessionBtwSidechatEvent(_SSEEventBase):
    """
    A Claude Code ``/btw`` side-chat exchange to show transiently.

    ``/btw`` opens an ephemeral side conversation whose answer Claude Code
    keeps only in its in-TUI overlay — it is never written to the session
    transcript. The claude-native forwarder scrapes the settled overlay
    from the pane and emits this event so the managed web UI can show the
    same ephemeral overlay (dismissed with Escape), WITHOUT adding a
    persisted side-chat turn to the main conversation.

    Category: **transient** (SSE-only), live-only by design. Nothing is
    persisted and there is no SSE replay, so a reload drops the overlay —
    matching the terminal, where Escape closes it and leaves no history.

    The wire shape is FLAT (not enveloped):
    ``{"type": "session.btw_sidechat", "conversation_id": <id>,
    "question": <str>, "answer": <str>, "truncated": <bool>}``.

    :param type: Always ``"session.btw_sidechat"``.
    :param conversation_id: The conversation whose stream this rides.
    :param question: The ``/btw`` request line as typed, e.g.
        ``"/btw is this backward compatible?"``.
    :param answer: The side-chat answer text.
    :param truncated: True when the pane clipped a longer answer; the web
        overlay notes it and points at the terminal for the full text.
    """

    type: Literal["session.btw_sidechat"]
    conversation_id: str
    question: str
    answer: str
    truncated: bool = False


class SessionPresenceEvent(_SSEEventBase):
    """
    The session's viewer list changed — full state, not a delta.

    Emitted on ``GET /v1/sessions/{id}/stream`` whenever a user
    joins, leaves (after the server-side grace window absorbs
    reconnect churn), or flips their idle aggregate, and once to
    each newly-connected stream as a snapshot-on-connect. Every
    event carries the COMPLETE viewer list so clients replace their
    state wholesale — missed events self-heal on the next event or
    reconnect. Viewers are scoped to the session *tree* (the root
    conversation and every sub-agent conversation under it), so a
    user on a sub-agent page and a user on the root page appear in
    each other's lists. See ``omnigent/server/presence.py`` and
    ``designs/UI/PRESENCE.md``.

    :param type: Always ``"session.presence"``.
    :param conversation_id: The conversation whose stream delivered
        this event — the root or a sub-agent conversation, e.g.
        ``"conv_abc123"``. Matches the streamed conversation (not
        necessarily the tree's root) so clients can guard events by
        the conversation they are viewing.
    :param viewers: All users currently viewing any conversation in
        the session tree (including the receiving user — the web
        filters self out for display), ordered by join time.
    """

    type: Literal["session.presence"]
    conversation_id: str
    viewers: list[PresenceViewer]


class SessionResourceCreatedEvent(_SSEEventBase):
    """
    A session resource was created.

    Emitted when a terminal is launched, a file is uploaded, or
    any other resource is materialized under a session. Wire shape
    is FLAT: ``{"type": "session.resource.created",
    "resource": <SessionResourceObject-like dict>}``.

    :param type: Always ``"session.resource.created"``.
    :param resource: The newly created resource object.
    """

    type: Literal["session.resource.created"]
    resource: dict[str, Any]


class SessionResourceDeletedEvent(_SSEEventBase):
    """
    A session resource was deleted.

    Emitted when a terminal is closed, a file is deleted, or
    any other resource is removed from a session.

    :param type: Always ``"session.resource.deleted"``.
    :param resource_id: Opaque id of the deleted resource.
    :param resource_type: Type of the deleted resource,
        e.g. ``"terminal"``, ``"file"``.
    :param session_id: Owning session/conversation id.
    """

    type: Literal["session.resource.deleted"]
    resource_id: str
    resource_type: str
    session_id: str


class SessionChildSessionUpdatedEvent(_SSEEventBase):
    """
    A child (sub-agent) session's status changed — pushed to the PARENT.

    Lets the parent's resource rail update a child's status without
    polling ``GET …/child_sessions``. Carries the full
    :class:`ChildSessionSummary` so the web patches its cache directly.

    :param type: Always ``"session.child_session.updated"``.
    :param conversation_id: The PARENT (carrier) session id.
    :param child_session_id: The child session id, e.g.
        ``"conv_child_abc123"``.
    :param child: A PARTIAL :class:`ChildSessionSummary` — the
        snapshot-on-connect sends the full summary, while live runner
        deltas carry only the fields that changed (a status delta omits
        ``last_message_preview``; a preview delta carries only it). The
        web merges present fields over the cached row, so the payload is
        an open dict rather than the strict model.
    """

    type: Literal["session.child_session.updated"]
    conversation_id: str
    child_session_id: str
    child: dict[str, Any]


class SessionChangedFilesInvalidatedEvent(_SSEEventBase):
    """
    The session's changed-files list may have changed — refetch it.

    A coarse "something changed" signal (per-file events aren't available
    for git-mode workspaces) emitted by the runner after a file-mutating
    tool. The web treats it as a refetch trigger for the changed-files
    panel; transient (not persisted — the REST list is source of truth).

    :param type: Always ``"session.changed_files.invalidated"``.
    :param session_id: Owning session/conversation id.
    :param environment_id: Environment whose changes were invalidated,
        e.g. ``"default"``.
    """

    type: Literal["session.changed_files.invalidated"]
    session_id: str
    environment_id: str = "default"


class SessionTerminalActivityEvent(_SSEEventBase):
    """
    A terminal's pane produced output (runner-determined activity pulse).

    Powers the web "active" badge for any terminal without a client PTY
    attach — the runner's per-terminal pane watcher emits this when the
    pane content changes. Transient (a live pulse; not persisted, not in
    the connect snapshot).

    :param type: Always ``"session.terminal.activity"``.
    :param session_id: Owning session/conversation id.
    :param terminal_id: Opaque terminal resource id, e.g.
        ``"terminal_zsh_s1"``.
    """

    type: Literal["session.terminal.activity"]
    session_id: str
    terminal_id: str


class OutputTextDeltaEvent(_SSEEventBase):
    """
    Incremental assistant-text token emitted during streaming.

    Wire shape matches the existing raw-dict emit at
    ``omnigent/runtime/workflow.py:1352-1356``.

    :param type: Always ``"response.output_text.delta"``.
    :param delta: The text fragment for this chunk, e.g.
        ``"Hello"``.
    :param message_id: For native terminal streaming, the provider's stable
        per-message id, e.g. ``"2ca51d97-2f0f-493a-aed7-85a5b56c5747"``.
        ``None`` for ordinary in-process task streaming, where deltas group
        by the active response.
    :param index: 0-based chunk order within the message, e.g. ``3``.
        Used to suppress repeated chunks; ``None`` for in-process streaming.
    :param final: Optional provider completion marker for the message.
    """

    type: Literal["response.output_text.delta"]
    delta: str
    message_id: str | None = None
    index: int | None = None
    final: bool | None = None


class ToolOutputDeltaEvent(_SSEEventBase):
    """Incremental output from an in-progress function call.

    :param type: Always ``"response.function_call_output.delta"``.
    :param call_id: Function-call correlation id.
    :param delta: Command stdout/stderr fragment.
    """

    type: Literal["response.function_call_output.delta"]
    call_id: str
    delta: str


class ReasoningStartedEvent(_SSEEventBase):
    """
    Marker emitted once when a reasoning block begins.

    Fired even when the reasoning content itself is encrypted /
    redacted (so no delta events follow), letting clients render
    a "thinking…" indicator regardless of provider verification
    status. Wire shape matches ``omnigent/runtime/workflow.py:1350``.

    :param type: Always ``"response.reasoning.started"``.
    """

    type: Literal["response.reasoning.started"]


class ReasoningTextDeltaEvent(_SSEEventBase):
    """
    Incremental reasoning-text token (full chain-of-thought).

    Only emitted by providers that surface reasoning content
    (e.g. OpenAI o-series with appropriate verification). Wire
    shape matches ``omnigent/runtime/workflow.py:1358-1364``.

    :param type: Always ``"response.reasoning_text.delta"``.
    :param delta: The reasoning text fragment, e.g.
        ``"Considering the user's intent..."``.
    """

    type: Literal["response.reasoning_text.delta"]
    delta: str


class ReasoningSummaryTextDeltaEvent(_SSEEventBase):
    """
    Incremental reasoning-summary token.

    Emitted when ``reasoning.summary`` is configured on the
    request. Wire shape matches
    ``omnigent/runtime/workflow.py:1370-1373``.

    :param type: Always ``"response.reasoning_summary_text.delta"``.
    :param delta: The summary text fragment, e.g. ``"Will use
        the search tool to gather context."``.
    """

    type: Literal["response.reasoning_summary_text.delta"]
    delta: str


class OutputItemDoneEvent(_SSEEventBase):
    """
    A conversation output item completed during the turn.

    Carries any item type the conversation persists (message,
    function_call, function_call_output, reasoning, compaction,
    native_tool, …). The ``item`` payload's wire shape merges
    common fields (``id``, ``type``, ``status``) with the
    type-specific data fields — it is NOT nested as
    ``{type, data}``.

    :param type: Always ``"response.output_item.done"``.
    :param item: The completed item dict. Heterogeneous and
        item-type-specific; see
        ``omnigent/entities/conversation.py`` for the
        per-type ``*Data`` shapes that drive serialization.
        Example for a function_call item: ``{"id": "fc_abc123",
        "type": "function_call", "status": "action_required",
        "name": "search.web", "arguments": "{\\"q\\": \\"foo\\"}",
        "call_id": "call_xyz"}``.
    """

    type: Literal["response.output_item.done"]
    item: dict[str, Any]


class OutputFileDoneEvent(_SSEEventBase):
    """
    A streamed file output completed materializing.

    Emitted by ``_emit_file_annotation_events`` in
    ``omnigent/runtime/workflow.py`` once per file annotation in
    the assistant's output. ``filename`` and ``content_type`` are
    only populated when the originating annotation carried them.

    :param type: Always ``"response.output_file.done"``.
    :param file_id: Identifier of the materialized file,
        e.g. ``"file_abc123"``.
    :param filename: Original filename if the annotation supplied
        one, e.g. ``"report.pdf"``. ``None`` otherwise.
    :param content_type: MIME content type if the annotation
        supplied one, e.g. ``"application/pdf"``. ``None``
        otherwise.
    """

    type: Literal["response.output_file.done"]
    file_id: str
    filename: str | None = None
    content_type: str | None = None


class HeartbeatEvent(_SSEEventBase):
    """
    Keepalive event emitted on a fixed cadence during streaming.

    Lets consumers detect stalled producers via missed-interval
    timing. Cadence is set by ``_HEARTBEAT_INTERVAL_S`` in
    ``omnigent/runtime/workflow.py`` (15 seconds at the time of
    writing). Wire shape matches the existing emit at
    ``omnigent/runtime/workflow.py:4636-4639``.

    Per ``designs/SERVER_HARNESS_CONTRACT.md`` §Heartbeats, the
    event MAY carry timing metadata so consumers can do richer
    dead-detection than "did anything arrive":

    - ``server_time`` is the producer's wall-clock at emission,
      letting consumers detect clock drift between producer and
      consumer.
    - ``last_event_seq`` is the ``sequence_number`` of the most
      recent NON-heartbeat event (or ``None`` when this is the
      first heartbeat before any user-visible event), letting
      consumers detect dropped events on reconnect.

    Both fields are optional on the wire (``None`` round-trips as
    omitted) so older AP→harness pairs that pre-date the field
    addition still parse cleanly.

    :param type: Always ``"response.heartbeat"``.
    :param server_time: ISO 8601 UTC timestamp at emission, e.g.
        ``"2026-04-27T15:30:00Z"``. ``None`` when the producer
        chose not to populate it (legacy emitters).
    :param last_event_seq: Sequence number of the last non-
        heartbeat event seen on the same stream, e.g. ``42``.
        ``None`` before any user-visible event has fired (first
        heartbeat of the turn, before deltas land), or when the
        producer chose not to populate it.
    """

    type: Literal["response.heartbeat"]
    server_time: str | None = None
    last_event_seq: int | None = None


class SessionHeartbeatEvent(_SSEEventBase):
    """
    Idle-stream keepalive on ``GET /v1/sessions/{id}/stream``.

    Emitted by the session-stream route on a fixed cadence whenever
    the underlying publish queue has been quiet (no turn in flight,
    no resource events). Distinct from :class:`HeartbeatEvent`
    (``response.heartbeat``), which is per-turn and is driven by
    the runtime workflow while a response is producing output.

    Why this exists: the session stream stays open across many turns
    and through idle periods (waiting for the user to type). Without
    a periodic emit, intermediate proxies, OS-level sockets, and the
    client's SSE read-timeout can leave a half-open stream
    undetected for minutes after a network event (laptop sleep,
    Wi-Fi handoff). The heartbeat puts a regular byte on the wire
    so the client's read-timeout and the server's
    ``request.is_disconnected()`` check both fire promptly.

    Consumers MAY ignore the payload entirely (the bytes crossing
    the wire are sufficient). The optional ``server_time`` mirrors
    :class:`HeartbeatEvent` for symmetry and debugging.

    :param type: Always ``"session.heartbeat"``.
    :param server_time: ISO 8601 UTC timestamp at emission, e.g.
        ``"2026-05-25T10:30:00Z"``. ``None`` when the producer
        chose not to populate it.
    """

    type: Literal["session.heartbeat"]
    server_time: str | None = None


class PresenceViewer(_OutputModel):
    """
    One user currently viewing a session (holding its SSE stream open).

    :param user_id: The viewer's authenticated identity,
        e.g. ``"alice@example.com"``. Never the reserved single-user
        ``"local"`` sentinel — presence only tracks distinct human
        actors (see ``attribution_user``).
    :param joined_at: ISO 8601 UTC timestamp of when the user joined,
        e.g. ``"2026-06-10T17:00:00Z"``. Stable across reconnects
        within the server's leave-grace window.
    :param idle: Whether every stream the user holds reports an idle
        (backgrounded) tab. The web greys idle viewers' avatars.
    """

    user_id: str
    joined_at: str
    idle: bool = False


class ElicitationRequestParams(_OutputModel):
    """
    Inner ``params`` block of a :class:`ElicitationRequestEvent`.

    The standard fields (``mode``, ``message``, ``requestedSchema``,
    ``url``) mirror MCP's ``ElicitRequestFormParams`` /
    ``ElicitRequestUrlParams`` byte-for-byte (Principle 8 — adopt
    MCP's wire shape verbatim where it overlaps). The
    AP-specific extensions (``phase``, ``policy_name``,
    ``content_preview``, ``target_session_id``) carry policy-engine
    context and mirrored-child routing for the consumer's renderer;
    MCP's ``extra="allow"`` config permits them under the same params
    block. Wire shape matches
    ``omnigent/runtime/policies/approval.py:175``.

    :param mode: MCP-standard discriminator. ``"form"`` collects
        structured input via ``requestedSchema``; ``"url"``
        directs upstream to an external URL for OAuth /
        out-of-band interaction.
    :param message: Human-readable prompt the consumer renders,
        e.g. ``"Approve running 'rm -rf /tmp/cache'?"``.
    :param requestedSchema: JSON-Schema dict for form mode (or
        ``None`` for url mode). camelCase preserved per MCP
        spec, e.g.
        ``{"type": "object", "properties": {"approve":
        {"type": "boolean"}}}``.
    :param url: External URL for url mode (or ``None`` for form
        mode), e.g. ``"https://oauth.example.com/authorize?..."``.
    :param phase: Omnigent policy-engine phase the elicitation
        belongs to, e.g. ``"pre_tool_use"``.
    :param policy_name: Omnigent policy that triggered the
        elicitation, e.g. ``"approve_shell_commands"``.
    :param content_preview: Truncated preview of the underlying
        request payload (≤1024 chars in current AP), for the
        consumer's renderer.
    :param target_session_id: AP session whose resolve endpoint owns
        this elicitation, e.g. ``"conv_child123"``. Present when a
        child/sub-agent prompt is mirrored into an ancestor stream;
        ``None`` means resolve against the current session.
    """

    mode: Literal["form", "url"] = "form"
    message: str
    requestedSchema: dict[str, Any] | None = None
    url: str | None = None
    phase: str | None = None
    policy_name: str | None = None
    content_preview: str | None = None
    target_session_id: str | None = None
    model_config = ConfigDict(extra="allow", json_schema_extra=None)


class ElicitationRequestEvent(_SSEEventBase):
    """
    Synchronous request for a decision from upstream.

    Emitted by Omnigent (or, under the new contract, by a harness)
    when the LLM / a tool / a policy needs a verdict before
    proceeding. The consumer replies via
    ``POST /v1/sessions/{session_id}/events`` with
    ``type == "approval"`` and
    :class:`omnigent.server.schemas.ElicitationResult` fields in
    ``data``. This preserves MCP request/reply correlation by id
    without threading elicitations through PATCH.

    Wire shape matches the existing emit at
    ``omnigent/runtime/policies/approval.py:175``.

    :param type: Always ``"response.elicitation_request"``.
    :param elicitation_id: Unique correlation id for this
        request — appears in the consumer's approval event payload,
        e.g. ``"elicit_abc123"``.
    :param method: MCP method literal — always
        ``"elicitation/create"`` (the value of
        ``_MCP_ELICITATION_METHOD`` in
        ``omnigent/runtime/policies/approval.py``).
    :param params: The MCP-shaped params block carrying the
        prompt and (form-mode only) the requested schema.
    """

    type: Literal["response.elicitation_request"]
    elicitation_id: str
    method: Literal["elicitation/create"] = "elicitation/create"
    params: ElicitationRequestParams


class ElicitationResolvedEvent(_SSEEventBase):
    """
    Signal that a previously-published elicitation is no longer
    outstanding, even though no UI ``approval`` verdict was
    delivered through ``POST /v1/sessions/{id}/events``.

    Emitted by the runner when its own ``_pending_approvals``
    Future is popped without a verdict (the runner's wait timed
    out, the turn was cancelled, the harness exited) so the AP
    server's :mod:`omnigent.runtime.pending_elicitations`
    index can decrement the sidebar badge in lockstep with the
    underlying awaiter's lifecycle. Without this signal, the AP
    server has no way to learn that the prompt is dead and the
    badge stays stuck.

    Idempotent on the consumer side: the Omnigent server's index
    decrement is a no-op when the id isn't tracked, so the
    runner can fire-and-forget on every Future cleanup.

    :param type: Always ``"response.elicitation_resolved"``.
    :param elicitation_id: Correlation id of the elicitation
        being cleared, e.g. ``"elicit_abc123"``. Must match the
        id of a prior :class:`ElicitationRequestEvent`.
    :param action: Optional MCP verdict recorded when the
        resolution carried a human decision (``"accept"`` /
        ``"decline"`` / ``"cancel"``). ``None`` for resolutions
        without a verdict — timeout, severed wait, or a runner
        that predates verdict carriage — so consumers can say "no
        verdict was recorded" rather than guessing one.
    :param reason: Why a verdict-less resolution happened, when
        known. ``"unanswered"``: the hook stopped waiting (a severed
        poll never re-parked, the ask timed out) before anyone
        answered, so the prompt is gone rather than decided and the
        UI can say so instead of implying it was resolved elsewhere.
        ``None`` when a verdict is present or the reason is unknown.
    """

    type: Literal["response.elicitation_resolved"]
    elicitation_id: str
    action: Literal["accept", "decline", "cancel"] | None = None
    reason: Literal["unanswered"] | None = None


class BrowserActionRequestEvent(_SSEEventBase):
    """
    Request that the desktop renderer perform one browser action.

    Emitted by the server ``POST /v1/sessions/{id}/browser/action_request``
    route when a runner-side ``browser_*`` tool dispatch needs the
    Omnigent desktop app's embedded browser to act. The event fans out
    on the session stream to every subscribed renderer; each renderer
    first POSTs ``/browser/action_claim/{action_id}`` and only the
    winning claimant executes the action and POSTs the result back to
    ``/browser/action_result/{action_id}``. The claim lease prevents
    double execution when more than one renderer is subscribed.

    :param type: Always ``"browser.action_request"``.
    :param action_id: Unique correlation id for this request, e.g.
        ``"baction_abc123"``. Echoed on the claim and result routes.
    :param action: The browser action to perform — the ``browser_``
        tool name with the prefix stripped, e.g. ``"navigate"``,
        ``"snapshot"``, ``"click"``, ``"type"``, ``"screenshot"``.
    :param args: Action arguments forwarded from the tool call, e.g.
        ``{"url": "https://example.com"}``.
    """

    type: Literal["browser.action_request"]
    action_id: str
    action: str
    args: dict[str, Any]


class PolicyDeniedEvent(_SSEEventBase):
    """
    Signal that a policy DENY was enforced on a native harness turn.

    A native harness (Claude Code, Codex, ...) routes each tool call and
    prompt through Omnigent's policy engine via the vendor command-hook
    (``POST /v1/sessions/{id}/policies/evaluate``). The DENY verdict is
    returned synchronously to that hook, so unlike the SDK/wrap path there is
    no stream-visible signal that a native action was blocked — only the
    *effect* (the blocked tool never runs). This event surfaces the decision
    itself on the session stream so observers (the web UI, the capability
    bench) can see a native DENY as a positive signal rather than infer it
    from an absence.

    Fire-and-forget and observational: it does not gate the turn (the hook
    response already did that) and carries no correlation id.

    :param type: Always ``"response.policy_denied"``.
    :param conversation_id: Session/conversation id the DENY applies to,
        e.g. ``"conv_abc123"``.
    :param reason: Human-readable deny reason from the deciding policy, e.g.
        ``"Blocked by policy."``.
    :param phase: The policy phase the DENY landed on, e.g. ``"tool_call"``.
    """

    type: Literal["response.policy_denied"]
    conversation_id: str
    reason: str = ""
    phase: str = ""


class CreatedEvent(_SSEEventBase):
    """
    Initial event emitted at the start of every streaming response.

    Carries the freshly-allocated
    :class:`omnigent.server.schemas.ResponseObject` (status will
    be ``"queued"`` or ``"in_progress"`` depending on whether the
    task started immediately).

    :param type: Always ``"response.created"``.
    :param response: The newly-allocated response object.
    """

    type: Literal["response.created"]
    response: ResponseObject


class QueuedEvent(_SSEEventBase):
    """
    Optional event emitted between ``created`` and ``in_progress``
    for background tasks that are queued before they start.

    Foreground streaming responses skip this event.

    :param type: Always ``"response.queued"``.
    :param response: The response object with
        ``status="queued"``.
    """

    type: Literal["response.queued"]
    response: ResponseObject


class InProgressEvent(_SSEEventBase):
    """
    Event emitted once the task transitions to in-progress.

    Always follows ``response.created`` (and ``response.queued``
    for background tasks).

    :param type: Always ``"response.in_progress"``.
    :param response: The response object with
        ``status="in_progress"``.
    """

    type: Literal["response.in_progress"]
    response: ResponseObject


class CompletedEvent(_SSEEventBase):
    """
    Terminal event for a successfully completed turn.

    Carries the final
    :class:`omnigent.server.schemas.ResponseObject`.

    :param type: Always ``"response.completed"``.
    :param response: The final response object with
        ``status="completed"``.
    """

    type: Literal["response.completed"]
    response: ResponseObject


class FailedEvent(_SSEEventBase):
    """
    Terminal event for a turn that ended with an error.

    Carries a :class:`omnigent.server.schemas.FailedResponseObject`
    whose ``error`` field describes the failure. Response metadata may
    be absent when the failure occurs before response allocation.

    :param type: Always ``"response.failed"``.
    :param source: Where the fault originated -- ``"llm"`` for
        inference/context errors, ``"harness"`` for Claude Code/harness
        process failures, ``"execution"`` for runner configuration
        or infrastructure failures.
    :param response: The failure response object with ``status="failed"``
        and ``error`` populated.
    """

    type: Literal["response.failed"]
    source: Literal["llm", "execution", "tool", "harness"] = "execution"
    response: ResponseObject | FailedResponseObject


class CancelledEvent(_SSEEventBase):
    """
    Terminal event for a turn cancelled before completion.

    :param type: Always ``"response.cancelled"``.
    :param response: The final response object with
        ``status="cancelled"``.
    """

    type: Literal["response.cancelled"]
    response: ResponseObject


class IncompleteEvent(_SSEEventBase):
    """
    Terminal event for a turn that ended without completing
    (e.g. hit the iteration cap or token budget).

    :param type: Always ``"response.incomplete"``.
    :param response: The final response object with
        ``status="incomplete"`` and ``incomplete_details``
        populated describing the reason.
    """

    type: Literal["response.incomplete"]
    response: ResponseObject


class RetryErrorDetail(_OutputModel):
    """
    Error block carried by :class:`RetryEvent` and :class:`ErrorEvent`.

    Mirrors the shape that ``llm_retry.py`` and ``tool_retry.py``
    emit today — flat ``code`` / ``message`` plus an optional
    ``detail`` for provider-specific structured fields.

    :param code: Stable error classifier, e.g. ``"timeout"``,
        ``"rate_limit"``.
    :param message: Human-readable summary, e.g.
        ``"Connection timed out after 30s"``.
    :param detail: Optional provider-specific structured fields
        (e.g. ``{"status_code": 429, "retry_after": 5}``);
        ``None`` when the classifier had no extra context.
    """

    code: str
    message: str
    detail: dict[str, Any] | None = None


class RetryEvent(_SSEEventBase):
    """
    A retryable failure was caught and a retry is scheduled.

    Emitted by ``omnigent/runtime/llm_retry.py`` (LLM calls)
    and ``omnigent/runtime/tool_retry.py`` (tool calls) before
    sleeping for the backoff delay. Wire shape matches
    ``llm_retry.py:329-340`` and ``tool_retry.py:168-180``.

    :param type: Always ``"response.retry"``.
    :param source: Origin of the retried failure — ``"llm"`` for
        LLM-call retries, ``"tool"`` for tool-call retries.
    :param tool_name: Tool identifier when ``source == "tool"``,
        e.g. ``"search.web"``. ``None`` for LLM retries.
    :param attempt: 1-based count of the upcoming attempt
        (i.e. attempt that will run AFTER this delay), e.g.
        ``2`` for the first retry.
    :param max_attempts: Total tries allowed by the retry policy,
        e.g. ``3``. Lets clients render "attempt 2 of 3".
    :param delay_seconds: Seconds the producer will sleep before
        retrying, rounded to two decimals, e.g. ``1.5``.
    :param error: Classified error description for the failure
        being retried.
    """

    type: Literal["response.retry"]
    source: Literal["llm", "tool"]
    tool_name: str | None = None
    attempt: int
    max_attempts: int
    delay_seconds: float
    error: RetryErrorDetail


class ErrorEvent(_SSEEventBase):
    """
    Non-recoverable error reported during the turn.

    Emitted from multiple sites in
    ``omnigent/runtime/workflow.py`` — terminal LLM failures
    (``_emit_llm_error_event``), execution timeouts
    (``_handle_execution_timeout``), and the agent-loop catch-all
    (``except Exception``). Wire shape matches those emits.

    :param type: Always ``"response.error"``.
    :param source: Origin of the error -- ``"llm"`` for LLM-call
        failures, ``"execution"`` for timeouts, ``"tool"`` for
        tool failures, ``"harness"`` for harness process failures.
    :param tool_name: Tool identifier when ``source == "tool"``;
        ``None`` for the other sources.
    :param error: Classified error description.
    """

    type: Literal["response.error"]
    source: Literal["llm", "execution", "tool", "harness"]
    tool_name: str | None = None
    error: RetryErrorDetail


class CompactionInProgressEvent(_SSEEventBase):
    """
    Conversation history is being compacted.

    Emitted by ``omnigent/runtime/compaction.py`` while a
    compaction step runs so clients can render a "summarizing
    history…" indicator. Wire shape matches ``compaction.py:765``.

    A long compaction is announced repeatedly (once per status poll), so
    clients must treat every event carrying the same ``started_at`` as one
    compaction — refreshing their indicator rather than stacking another.

    :param type: Always ``"response.compaction.in_progress"``.
    :param started_at: Unix epoch timestamp (seconds) when this compaction
        was first reported in progress. Stable across repeated progress
        events for the same compaction, so clients can anchor an elapsed
        counter to the true start — including after a page reload. ``None``
        when the emitter does not track it.
    """

    type: Literal["response.compaction.in_progress"]
    started_at: int | None = None


class CompactionCompletedEvent(_SSEEventBase):
    """
    Conversation history compaction has finished.

    Emitted after compaction completes — either by the server after
    ``compact_conversation_now()`` (explicit ``/compact``), or by a
    harness that compacted its own internal context. Clients that
    rendered a "Compacting…" spinner on
    :class:`CompactionInProgressEvent` should upgrade it to the
    permanent "Conversation compacted" marker on this event.

    When emitted by a harness, ``summary`` and ``summary_model``
    are populated so the runner can persist a compaction item for
    session resume. When emitted by the server's explicit
    ``/compact`` path, those fields are ``None``.

    :param type: Always ``"response.compaction.completed"``.
    :param total_tokens: Tiktoken estimate of the post-compaction
        message context size, e.g. ``8421``. Used by clients to
        update the context-ring immediately without waiting for the
        next ``response.completed`` usage report. ``None`` when
        token counting is unavailable.
    :param summary: Text summary of the compacted conversation,
        or ``None`` for server-side compaction (already persisted).
    :param summary_model: Model used for summarization, or ``None``
        if truncation-based or server-side.
    """

    type: Literal["response.compaction.completed"]
    total_tokens: int | None = None
    summary: str | None = None
    summary_model: str | None = None
    compacted_messages: list[dict[str, Any]] | None = None


class CompactionFailedEvent(_SSEEventBase):
    """
    Conversation history compaction failed.

    Emitted by ``omnigent/server/routes/sessions.py`` when
    ``compact_conversation_now()`` raises. Clients that rendered a
    "Compacting…" spinner on :class:`CompactionInProgressEvent`
    should dismiss it without leaving a permanent marker, since the
    conversation history was not modified.

    :param type: Always ``"response.compaction.failed"``.
    """

    type: Literal["response.compaction.failed"]


class ClientTaskCancelEvent(_SSEEventBase):
    """
    Server-side request that the client cancel a tunneled tool call.

    Emitted by ``omnigent/runtime/workflow.py`` when a parent
    cancellation needs to propagate to a long-running async client
    tool. Wire shape matches ``workflow.py:4258-4266``.

    :param type: Always ``"response.client_task.cancel"``.
    :param task_id: Identifier of the client-side task being
        cancelled, e.g. ``"resp_async_abc"``.
    :param call_id: Synthetic ``call_id`` the SDK uses to
        reconcile the local task; ``None`` when no pending tool
        call row exists for the task.
    """

    type: Literal["response.client_task.cancel"]
    task_id: str
    call_id: str | None = None


class TurnStartedEvent(_SSEEventBase):
    """
    Emitted when the runner starts a new turn for a session.

    :param type: Fixed literal ``"turn.started"``.
    :param session_id: Session/conversation identifier,
        e.g. ``"conv_abc123"``.
    """

    type: Literal["turn.started"]
    session_id: str


class TurnCompletedEvent(_SSEEventBase):
    """
    Emitted when a turn finishes successfully with no pending work.

    :param type: Fixed literal ``"turn.completed"``.
    :param session_id: Session/conversation identifier,
        e.g. ``"conv_abc123"``.
    """

    type: Literal["turn.completed"]
    session_id: str


class TurnFailedEvent(_SSEEventBase):
    """
    Emitted when a turn fails due to an LLM error, timeout, or crash.

    :param type: Fixed literal ``"turn.failed"``.
    :param session_id: Session/conversation identifier,
        e.g. ``"conv_abc123"``.
    :param error: Error details, e.g.
        ``{"message": "LLM timeout", "type": "TimeoutError"}``.
    """

    type: Literal["turn.failed"]
    session_id: str
    error: dict[str, Any] = Field(default_factory=dict)


class TurnCancelledEvent(_SSEEventBase):
    """
    Emitted when a turn is interrupted by the user or system.

    :param type: Fixed literal ``"turn.cancelled"``.
    :param session_id: Session/conversation identifier,
        e.g. ``"conv_abc123"``.
    """

    type: Literal["turn.cancelled"]
    session_id: str


ServerStreamEvent = Annotated[
    SessionStatusEvent
    | SessionUsageEvent
    | SessionModelEvent
    | SessionTitleEvent
    | SessionReasoningEffortEvent
    | SessionCollaborationModeEvent
    | SessionPermissionModeEvent
    | SessionCodexApprovalModeEvent
    | SessionAgentChangedEvent
    | SessionTodosEvent
    | SessionTerminalPendingEvent
    | SessionSandboxStatusEvent
    | SessionMcpStartupEvent
    | SessionModelOptionsEvent
    | SessionInputConsumedEvent
    | SessionInterruptedEvent
    | SessionCreatedEvent
    | SessionSupersededEvent
    | SessionBtwSidechatEvent
    | SessionPresenceEvent
    | SessionResourceCreatedEvent
    | SessionResourceDeletedEvent
    | SessionChildSessionUpdatedEvent
    | SessionChangedFilesInvalidatedEvent
    | SessionTerminalActivityEvent
    | OutputTextDeltaEvent
    | ToolOutputDeltaEvent
    | ReasoningStartedEvent
    | ReasoningTextDeltaEvent
    | ReasoningSummaryTextDeltaEvent
    | OutputItemDoneEvent
    | OutputFileDoneEvent
    | HeartbeatEvent
    | SessionHeartbeatEvent
    | ElicitationRequestEvent
    | ElicitationResolvedEvent
    | BrowserActionRequestEvent
    | PolicyDeniedEvent
    | CreatedEvent
    | QueuedEvent
    | InProgressEvent
    | CompletedEvent
    | FailedEvent
    | CancelledEvent
    | IncompleteEvent
    | RetryEvent
    | ErrorEvent
    | CompactionInProgressEvent
    | CompactionCompletedEvent
    | CompactionFailedEvent
    | ClientTaskCancelEvent
    | TurnStartedEvent
    | TurnCompletedEvent
    | TurnFailedEvent
    | TurnCancelledEvent,
    Field(discriminator="type"),
]

SERVER_STREAM_EVENT_TYPES: frozenset[str] = frozenset(
    cls.model_fields["type"].annotation.__args__[0]
    for cls in get_args(get_args(ServerStreamEvent)[0])
)
_KNOWN_EVENT_TYPES = SERVER_STREAM_EVENT_TYPES


def is_known_event(name: str) -> bool:
    return name in SERVER_STREAM_EVENT_TYPES


class UnknownEvent(_OutputModel):
    """A newer output event whose discriminator is unknown to this client."""

    type: str
    raw: dict[str, Any]
