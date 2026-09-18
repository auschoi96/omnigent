"""Server-only API schemas and compatibility imports for portable models.

Portable session request, response, and SSE models are canonically defined in
``omnigent.protocol`` and re-exported here for existing server callers. This
module retains server validation, internal event, and non-session API schemas.
"""

from __future__ import annotations

import re
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    Strict,
    field_validator,
    model_validator,
)

from omnigent.entities import DEFAULT_GENERATED_TITLE_MAX_CHARS
from omnigent.protocol import (  # noqa: F401 - compatibility re-exports
    SERVER_STREAM_EVENT_TYPES,
    AgentObject,
    BackgroundTaskInfo,
    BrowserActionRequestEvent,
    CancelledEvent,
    ChildSessionList,
    ChildSessionSummary,
    ClientTaskCancelEvent,
    CompactionCompletedEvent,
    CompactionFailedEvent,
    CompactionInProgressEvent,
    CompletedEvent,
    ConversationDeleted,
    ConversationItem,
    ConversationRef,
    CopiedFile,
    CopyFilesRequest,
    CopyFilesResponse,
    CreatedEvent,
    CreatedSessionResponse,
    ElicitationRequestEvent,
    ElicitationRequestParams,
    ElicitationResolutionAcknowledgement,
    ElicitationResolvedEvent,
    ElicitationResult,
    ElicitationState,
    ErrorDetail,
    ErrorEvent,
    EventAcknowledgement,
    FailedEvent,
    FailedResponseObject,
    HeartbeatEvent,
    IncompleteDetails,
    IncompleteEvent,
    InProgressEvent,
    McpServerStartup,
    MCPServerSummary,
    ModelUsage,
    NativeModelOption,
    NativeReasoningEffortOption,
    OutputFileDoneEvent,
    OutputItemDoneEvent,
    OutputTextDeltaEvent,
    PaginatedList,
    PolicyDeniedEvent,
    PolicySummary,
    PresenceViewer,
    QueuedEvent,
    ReasoningStartedEvent,
    ReasoningSummaryTextDeltaEvent,
    ReasoningTextDeltaEvent,
    ResponseObject,
    RetryErrorDetail,
    RetryEvent,
    SandboxLaunchStage,
    SandboxStatus,
    ServerSessionEventInputBase,
    ServerStreamEvent,
    SessionAgentChangedEvent,
    SessionBtwSidechatEvent,
    SessionChangedFilesInvalidatedEvent,
    SessionChildSessionUpdatedEvent,
    SessionCodexApprovalModeEvent,
    SessionCollaborationModeEvent,
    SessionCreatedEvent,
    SessionCreateMetadataBase,
    SessionCreateRequestBase,
    SessionForkRequestBase,
    SessionGitOptions,
    SessionHeartbeatEvent,
    SessionInputConsumedEvent,
    SessionInputConsumedPayload,
    SessionInterruptedEvent,
    SessionInterruptedPayload,
    SessionItem,
    SessionList,
    SessionListItem,
    SessionMcpStartupEvent,
    SessionModelEvent,
    SessionModelOptionsEvent,
    SessionPermissionModeEvent,
    SessionPresenceEvent,
    SessionReasoningEffortEvent,
    SessionResourceCreatedEvent,
    SessionResourceDeleted,
    SessionResourceDeletedEvent,
    SessionResourceObject,
    SessionResourcePaginatedList,
    SessionResponse,
    SessionSandboxStatusEvent,
    SessionStatusEvent,
    SessionSupersededEvent,
    SessionTerminalActivityEvent,
    SessionTerminalPendingEvent,
    SessionTitleEvent,
    SessionTodosEvent,
    SessionUsageEvent,
    SkillSummary,
    ToolOutputDeltaEvent,
    TurnCancelledEvent,
    TurnCompletedEvent,
    TurnFailedEvent,
    TurnStartedEvent,
    UnknownEvent,
    UpdateSessionRequest,
    Usage,
    UsageDetails,
    _SSEEventBase,
    is_known_event,
)

_KNOWN_EVENT_TYPES = SERVER_STREAM_EVENT_TYPES

# ── Shared ──────────────────────────────────────────────────────


# ── Agents ──────────────────────────────────────────────────────


_MCP_SERVER_NAME_RE = r"^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,127}$"


class UpsertMCPServerRequest(BaseModel):
    """
    Request body for creating or updating a session agent MCP server.

    ``env`` is still excluded. ``headers`` is accepted for HTTP servers;
    when omitted, existing headers in the bundle are preserved unchanged.
    """

    name: str = Field(min_length=1, max_length=128, pattern=_MCP_SERVER_NAME_RE)
    transport: Literal["http", "stdio"]
    description: str | None = Field(default=None, max_length=512)
    url: str | None = None
    headers: dict[str, str] | None = None
    command: str | None = None
    args: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("name")
    @classmethod
    def _reject_dot_names(cls, value: str) -> str:
        """Reject names that would make unsafe or confusing YAML filenames."""
        if value in {".", ".."}:
            raise ValueError("name cannot be '.' or '..'")
        return value

    @field_validator("args")
    @classmethod
    def _string_args_only(cls, value: list[str]) -> list[str]:
        """Keep args as a small list of strings."""
        return [str(item) for item in value]

    @model_validator(mode="after")
    def _validate_transport_fields(self) -> UpsertMCPServerRequest:
        """Enforce the same transport shape as the agent spec parser."""
        if self.transport == "http":
            if not self.url:
                raise ValueError("url is required when transport is 'http'")
            if not (self.url.startswith("http://") or self.url.startswith("https://")):
                raise ValueError("url must start with http:// or https://")
            if self.command:
                raise ValueError("command is not allowed when transport is 'http'")
            if self.args:
                raise ValueError("args are not allowed when transport is 'http'")
        if self.transport == "stdio":
            if not self.command:
                raise ValueError("command is required when transport is 'stdio'")
            if self.url:
                raise ValueError("url is not allowed when transport is 'stdio'")
        return self


# ── Session Policies ───────────────────────────────────────────


class SessionPolicyObject(BaseModel):
    """
    API representation of a session-scoped policy.

    Returned by all CRUD endpoints under
    ``/v1/sessions/{session_id}/policies``.

    :param id: Opaque policy identifier, e.g. ``"spol_abc123"``.
        ``None`` for spec-declared policies that are not
        store-persisted.
    :param object: Fixed resource type, always
        ``"session.policy"``.
    :param name: Human-readable policy name,
        e.g. ``"block_non_feature_branch_push"``.
    :param type: Handler discriminator: ``"python"`` or
        ``"url"``.
    :param handler: Dotted import path (python) or HTTP URL
        (url), e.g. ``"github_mcp_policy.block_push"`` or
        ``"https://example.com/policies/eval"``.
    :param factory_params: Dict of kwargs passed to the handler
        when it is a factory function. ``None`` for direct
        callables and ``type="url"`` handlers.
    :param enabled: Whether the engine consults this policy.
    :param source: Origin of the policy: ``"session"`` for
        CRUD-created policies, ``"spec"`` for policies
        declared in the agent YAML. Spec policies cannot be
        patched or deleted.
    :param created_at: Unix epoch timestamp of creation.
    :param updated_at: Unix epoch timestamp of the last
        update, or ``None`` if never updated.
    """

    id: str | None
    object: str = "session.policy"
    name: str
    type: str
    handler: str
    factory_params: dict[str, Any] | None = None
    enabled: bool = True
    source: str = "session"
    created_at: int
    updated_at: int | None = None


_DOTTED_PATH_RE = r"^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)+$"


class CreateSessionPolicyRequest(BaseModel):
    """
    Request body for ``POST /v1/sessions/{session_id}/policies``.

    :param name: Human-readable policy name. Must be unique
        within the session, e.g.
        ``"block_non_feature_branch_push"``.
    :param type: Handler discriminator: ``"python"`` or
        ``"url"``.
    :param handler: Dotted import path (python) or HTTPS URL
        (url), e.g.
        ``"github_mcp_policy.block_non_misc_push"``
        or ``"https://example.com/policies/eval"``.
    :param factory_params: Optional dict of kwargs passed to the
        handler when it is a factory function. Only valid for
        ``type="python"``, e.g. ``{"limit": 10}``.
    """

    name: str
    type: str
    handler: str
    factory_params: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_type_and_handler(self) -> CreateSessionPolicyRequest:
        """Reject unknown policy types and validate handler format.

        For ``type="url"``, requires an ``https://`` URL.
        For ``type="python"``, requires a valid dotted import path
        (at least two segments, e.g. ``"pkg.module"``).

        :returns: The validated request unchanged.
        :raises ValueError: If ``type`` is invalid, or ``handler``
            does not match the expected format for the type.
        """
        if self.type not in ("python", "url"):
            raise ValueError(f"type must be 'python' or 'url', got '{self.type}'")
        if self.type == "url":
            if not self.handler.startswith("https://"):
                raise ValueError("handler must be an https:// URL for type 'url'")
        elif self.type == "python":
            if not re.match(_DOTTED_PATH_RE, self.handler):
                raise ValueError(
                    "handler must be a valid dotted import path "
                    "(e.g. 'pkg.module.func') for type 'python'"
                )
        return self


class UpdateSessionPolicyRequest(BaseModel):
    """
    Request body for ``PATCH /v1/sessions/{session_id}/policies/{policy_id}``.

    All fields are optional; ``None`` fields are left unchanged.
    Unknown fields (including ``type``, which is immutable) are
    rejected with ``422``.

    :param name: New policy name. ``None`` leaves it unchanged.
    :param handler: New handler path or URL. ``None`` leaves it
        unchanged.
    :param enabled: New enabled flag. ``None`` leaves it
        unchanged.
    """

    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    handler: str | None = None
    enabled: bool | None = None


# ── Default Policies ──────────────────────────────────────────────


class DefaultPolicyObject(BaseModel):
    """
    API representation of a server-wide default policy.

    Returned by all CRUD endpoints under ``/v1/policies``.

    :param id: Opaque policy identifier, e.g. ``"dpol_abc123"``.
    :param object: Fixed resource type, always
        ``"default_policy"``.
    :param name: Human-readable policy name,
        e.g. ``"block_non_feature_branch_push"``.
    :param type: Handler discriminator: ``"python"`` or
        ``"url"``.
    :param handler: Dotted import path (python) or HTTP URL
        (url), e.g. ``"github_mcp_policy.block_push"`` or
        ``"https://example.com/policies/eval"``.
    :param factory_params: Dict of kwargs passed to the handler
        when it is a factory function. ``None`` for direct
        callables and ``type="url"`` handlers.
    :param enabled: Whether the engine consults this policy.
    :param created_at: Unix epoch timestamp of creation.
    :param updated_at: Unix epoch timestamp of the last
        update, or ``None`` if never updated.
    :param created_by: User ID of the admin who created this
        policy, or ``None`` in single-user mode.
    """

    id: str
    object: str = "default_policy"
    name: str
    type: str
    handler: str
    factory_params: dict[str, Any] | None = None
    enabled: bool = True
    created_at: int
    updated_at: int | None = None
    created_by: str | None = None


class CreateDefaultPolicyRequest(BaseModel):
    """
    Request body for ``POST /v1/policies``.

    :param name: Human-readable policy name. Must be globally
        unique, e.g. ``"block_non_feature_branch_push"``.
    :param type: Handler discriminator: ``"python"``, ``"url"``,
    :param handler: Dotted import path (python) or HTTPS URL
        (url), e.g.
        ``"github_mcp_policy.block_non_misc_push"``
        or ``"https://example.com/policies/eval"``.
    :param factory_params: Optional dict of kwargs passed to the
        handler when it is a factory function. Only valid for
        ``type="python"``, e.g. ``{"limit": 10}``.
    """

    name: str
    type: str
    handler: str
    factory_params: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_type_and_handler(self) -> CreateDefaultPolicyRequest:
        """Reject unknown policy types and validate handler format.

        Same validation rules as :class:`CreateSessionPolicyRequest`.

        :returns: The validated request unchanged.
        :raises ValueError: If ``type`` is invalid, or ``handler``
            does not match the expected format for the type.
        """
        if self.type not in ("python", "url"):
            raise ValueError(f"type must be 'python' or 'url', got '{self.type}'")
        if self.type == "url":
            if not self.handler.startswith("https://"):
                raise ValueError("handler must be an https:// URL for type 'url'")
        elif self.type == "python":
            if not re.match(_DOTTED_PATH_RE, self.handler):
                raise ValueError(
                    "handler must be a valid dotted import path "
                    "(e.g. 'pkg.module.func') for type 'python'"
                )
        return self


class UpdateDefaultPolicyRequest(BaseModel):
    """
    Request body for ``PATCH /v1/policies/{policy_id}``.

    All fields are optional; ``None`` fields are left unchanged.
    Unknown fields (including ``type``, which is immutable) are
    rejected with ``422``.

    :param name: New policy name. ``None`` leaves it unchanged.
    :param handler: New handler path or URL. ``None`` leaves it
        unchanged.
    :param enabled: New enabled flag. ``None`` leaves it
        unchanged.
    """

    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    handler: str | None = None
    enabled: bool | None = None


# ── Files ───────────────────────────────────────────────────────


class FileObject(BaseModel):
    """
    API representation of an uploaded file.

    :param id: Unique file identifier, e.g. ``"file_abc123"``.
    :param object: Fixed resource type, always ``"file"``.
    :param filename: Original filename, e.g. ``"report.pdf"``.
    :param bytes: File size in bytes.
    :param created_at: Unix epoch timestamp of upload.
    """

    id: str
    object: str = "file"
    filename: str
    bytes: int
    created_at: int


class SessionResourceListPage(BaseModel):
    """Strict runner resource-list wire contract."""

    object: Literal["list"]
    data: list[SessionResourceObject]
    first_id: str | None
    last_id: str | None
    has_more: bool

    model_config = ConfigDict(extra="forbid", strict=True)


# ── Conversations ───────────────────────────────────────────────


class ConversationObject(BaseModel):
    """
    API representation of a conversation.

    :param id: Unique conversation identifier,
        e.g. ``"conv_abc123"``.
    :param object: Fixed resource type, always
        ``"conversation"``.
    :param title: Optional user-assigned conversation title.
    :param created_at: Unix epoch timestamp of creation.
    :param updated_at: Unix epoch timestamp of the last
        update, e.g. ``1774118400``.
    :param labels: Session-scoped guardrails labels, mirroring
        the runtime ``Conversation.labels`` dict. Empty dict when
        the PolicyEngine hasn't written any labels yet. Exposed so
        the REPL's Ctrl+O debug overlay can render them at parity
        with the legacy ``omnigent run`` Ctrl+G overview.
    """

    id: str
    object: str = "conversation"
    title: str | None = None
    created_at: int
    updated_at: int
    labels: dict[str, str] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    """
    The body every failed request carries: a single ``error`` object.

    Mirrors what the FastAPI exception handler emits for an
    :class:`~omnigent.errors.OmnigentError`, so documented error responses
    and the runtime envelope stay the same shape.

    :param error: Machine-readable detail about the failure.
    """

    error: ErrorDetail


class CreateResponseRequest(BaseModel):
    """
    Internal request body the harness scaffold builds for each turn.

    Originally the ``POST /v1/responses`` request schema; that route
    was removed but the harness scaffold still synthesizes this shape
    internally to drive an executor turn.

    :param model: Agent name to invoke, e.g.
        ``"research-agent"``. Must match a registered agent.
    :param input: User input — either a plain string (converted
        to a single ``input_text`` block) or a list of content
        blocks, e.g.
        ``[{"type": "input_text", "text": "Hello"}]``.
    :param stream: If ``True``, return an SSE stream instead of
        blocking until completion.
    :param background: If ``True``, the task runs in the
        background and the caller may poll for results.
    :param store: Must be ``True`` (persisted responses). The
        server rejects ``False``.
    :param instructions: Per-request system instructions. Composed
        ADDITIVELY with the agent's own instructions rather than
        replacing them — appended after the agent's text, matching how
        ``omnigent/runtime/prompt.py`` assembles the system prompt and
        what ``docs/AGENT_YAML_SPEC.md`` documents.
    :param previous_response_id: ID of the prior response in the
        conversation thread, e.g. ``"resp_abc123"``. Enables
        multi-turn continuation and steering.
    :param conversation: Explicit conversation reference for
        fork validation. Must match the conversation that owns
        ``previous_response_id``.
    :param reasoning: Reasoning configuration,
        e.g. ``{"effort": "medium"}``.
    :param model_override: Optional per-request LLM model override,
        e.g. ``"openai/gpt-5.4-mini"``. Distinct from ``model``
        (agent name). Substitutes for the spec's ``llm.model`` for
        this single request. Drives the REPL's ``/model`` command.
    :param context_management: Compaction strategy objects,
        e.g. ``[{"type": "compaction", ...}]``.
    :param temperature: Ignored — agent controls this. Silently
        dropped.
    :param top_p: Ignored — agent controls this. Silently
        dropped.
    :param tools: Optional list of client-specified tools in standard
        OpenAI function format. When the LLM invokes one, the
        ``function_call`` output items are returned to the caller (the
        response completes) rather than being executed server-side. The
        caller handles execution and continues via
        ``previous_response_id``. Returns 400 if any entry is malformed
        or missing ``function.name``, e.g.
        ``[{"type": "function", "function": {"name": "get_weather",
        "description": "...", "parameters": {...}}}]``.
    :param tool_choice: Ignored — agent controls this. Silently
        dropped.
    :param max_output_tokens: Ignored — agent controls this.
        Silently dropped.
    :param frequency_penalty: Ignored — agent controls this.
        Silently dropped.
    :param presence_penalty: Ignored — agent controls this.
        Silently dropped.
    :param parallel_tool_calls: Ignored — agent controls this.
        Silently dropped.
    :param max_tool_calls: Ignored — agent controls this.
        Silently dropped.
    :param top_logprobs: Ignored — agent controls this. Silently
        dropped.
    """

    # Optional when previous_response_id is set; server resolves the agent
    # from the prior task. Required for fresh conversations (no prior task).
    model: str | None = None
    # Heterogeneous content blocks (input_text, input_image, input_file)
    # or a plain string shorthand; shape varies by block type.
    input: str | list[dict[str, Any]]
    stream: bool = False
    background: bool = False
    store: bool = True
    instructions: str | None = None
    previous_response_id: str | None = None
    # Correlation id for a mid-turn injection (RUNNER_MESSAGE_INGEST.md
    # Part B). Stamped by the runner when it forwards a buffered message
    # as a live injection; echoed back by the executor adapter in an
    # ``injection.consumed`` marker once the executor actually consumes
    # the message, so the runner can drop the buffered copy and not
    # re-deliver it in a continuation turn. ``None`` for fresh turns.
    injection_id: str | None = None
    conversation: ConversationRef | None = None
    # Reasoning config, e.g. {"effort": "low"|"medium"|"high"}
    reasoning: dict[str, str] | None = None
    # Per-request LLM model override (distinct from ``model``, which
    # carries the agent name). See class docstring for semantics.
    model_override: str | None = None
    # Compaction strategy objects, e.g. [{"type": "compaction", ...}]
    context_management: list[dict[str, Any]] | None = None
    # Ignored fields — agent controls these; silently dropped.
    # Typed loosely because we only need to accept and discard them.
    temperature: float | None = None
    top_p: float | None = None
    tools: list[dict[str, Any]] | None = None
    tool_choice: dict[str, Any] | str | None = None
    max_output_tokens: int | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None
    parallel_tool_calls: bool | None = None
    max_tool_calls: int | None = None
    top_logprobs: int | None = None

    @model_validator(mode="after")
    def _require_model_for_new_conversations(self) -> CreateResponseRequest:
        """
        Enforce that ``model`` is provided when starting a fresh conversation.

        When ``previous_response_id`` is not set the server has no prior task
        from which to resolve the agent, so ``model`` is required. Omitting it
        produces a 422 at the API boundary rather than a cryptic runtime error
        deep in the route handler.

        :returns: ``self`` unchanged when the invariant holds.
        :raises ValueError: When ``model`` is ``None`` and
            ``previous_response_id`` is not set.
        """
        if self.model is None and not self.previous_response_id:
            raise ValueError("model is required when previous_response_id is not set")
        return self


class ToolResult(BaseModel):
    """
    A single tool result submitted by the client via PATCH.

    :param call_id: The tool call ID that this result
        corresponds to, e.g. ``"call_abc123"``.
    :param output: The tool's string output,
        e.g. ``'["paper1.pdf", "paper2.pdf"]'``.
    """

    call_id: str
    output: str


# ── Sessions (/v1/sessions) ────────────────────────────────────


class SessionEventInput(ServerSessionEventInputBase):
    """Server ingress envelope with runner-only attribution."""

    created_by: str | None = None


# Upper bound on repositories a single managed session may clone. Generous for
# the realistic multi-repo case (a handful) while bounding three things a larger
# set would grow: the sandbox's parallel git-clone fan-out on a small node, the
# per-repo relaunch labels (one each, carried in every session snapshot), and an
# abusive request.
# ponytail: bump this one constant if larger repo sets ever need supporting.
_MAX_MANAGED_WORKSPACES = 10


class _SessionCreateRequestBase(SessionCreateRequestBase):
    """Portable create fields plus server-managed host validation."""

    initial_items: list[SessionEventInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_managed_host_fields(self) -> Self:
        """
        Enforce the per-``host_type`` workspace and host-id contract.

        A managed session's host is chosen by the server (sandbox
        provisioning), so a caller-supplied ``host_id`` is a
        contradiction. Its ``workspace``, when given, must be a git
        repository URL (optionally ``#<branch>``) the server clones
        into the sandbox — a path points at nothing in a sandbox that
        doesn't exist yet. Conversely, a repository-URL workspace on
        an external host is rejected: there, ``workspace`` is an
        absolute path on the host. Failing at validation returns a
        422 with the field named instead of silently ignoring the
        caller's intent.

        :returns: The validated instance.
        :raises ValueError: On ``"managed"`` + ``host_id``, a managed
            workspace/workspaces that isn't a valid repository URL,
            ``workspace`` and ``workspaces`` set together, too many
            ``workspaces``, or an external repository-URL workspace.
        """
        # Lazy import: schemas is imported by nearly every module, so
        # pulling the (FastAPI/click-importing) managed-hosts module in
        # at module scope would risk import cycles.
        from omnigent.server.managed_hosts import is_repo_workspace, parse_repo_workspace

        if self.host_type == "managed":
            if self.host_id is not None:
                raise ValueError(
                    "host_type 'managed' lets the server provision the host; "
                    "host_id must not be set"
                )
            if self.workspace is not None and self.workspaces is not None:
                raise ValueError(
                    "set either 'workspace' (one repository) or 'workspaces' "
                    "(several) for host_type 'managed', not both"
                )
            if self.workspaces is not None and len(self.workspaces) > _MAX_MANAGED_WORKSPACES:
                raise ValueError(
                    f"host_type 'managed' takes at most {_MAX_MANAGED_WORKSPACES} "
                    f"repositories in 'workspaces' (got {len(self.workspaces)})"
                )
            for candidate in self.managed_repo_workspaces():
                try:
                    parse_repo_workspace(candidate)
                except ValueError as exc:
                    raise ValueError(
                        "host_type 'managed' takes git repository URLs "
                        f"(optionally '#<branch>') as workspace(s): {exc}"
                    ) from exc
            return self
        if self.sandbox_provider is not None:
            raise ValueError(
                "sandbox_provider only applies to host_type 'managed' — "
                "external hosts are not server-provisioned"
            )
        if self.workspaces:
            raise ValueError(
                "'workspaces' (multi-repo clone) requires host_type 'managed' — "
                "external hosts take a single absolute path in 'workspace'"
            )
        if self.workspace is not None and is_repo_workspace(self.workspace):
            raise ValueError(
                "a repository-URL workspace requires host_type 'managed' — "
                "external hosts take an absolute path on the host"
            )
        return self

    def managed_repo_workspaces(self) -> list[str]:
        """
        The managed session's repository workspaces as a normalized list.

        ``workspaces`` when given, else the single ``workspace`` as a
        one-element list, else empty. ``workspace`` and ``workspaces``
        are mutually exclusive (:meth:`_check_managed_host_fields`), so at
        most one source is populated. Meaningful only for
        ``host_type: "managed"`` (an external ``workspace`` is a host path,
        not a repo URL); callers gate on that.

        :returns: Raw repository-URL strings (each optionally
            ``#<branch>``); empty for an empty sandbox workspace.
        """
        if self.workspaces:
            return list(self.workspaces)
        if self.workspace is not None:
            return [self.workspace]
        return []


class SessionCreateRequest(_SessionCreateRequestBase):
    """Legacy create shape, preserving required-string ``agent_id``."""

    agent_id: str


class ProjectSessionCreateRequest(_SessionCreateRequestBase):
    """Project-opted create shape whose agent may be filled by the server.

    The public legacy :class:`SessionCreateRequest` deliberately keeps
    ``agent_id`` required so requests without ``project_id`` retain their exact
    validation and OpenAPI contract.
    """

    agent_id: str | None = None


SessionCreateInput = SessionCreateRequest | ProjectSessionCreateRequest


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

    @model_validator(mode="after")
    def _check_managed_bundle_fields(self) -> Self:
        """
        Enforce the multipart per-``host_type`` contract.

        Mirrors :meth:`SessionCreateRequest._check_managed_host_fields`
        for the bundle-upload path: a managed session's host is
        server-provisioned, so a caller-supplied ``host_id`` contradicts
        it, and a managed ``workspace`` (when given) must be a git
        repository URL (optionally ``#<branch>``) the server clones into
        the sandbox — a filesystem path points at nothing in a sandbox
        that doesn't exist yet. ``sandbox_provider`` only applies to a
        managed host. A repository-URL workspace on an external host is
        rejected symmetrically.

        :returns: The validated instance.
        :raises ValueError: On ``"managed"`` + ``host_id``, a managed
            workspace that isn't a valid repository URL, ``sandbox_provider``
            without ``"managed"``, or an external repository-URL workspace.
        """
        # Lazy import: schemas is imported nearly everywhere, so pulling
        # the FastAPI/click-importing managed-hosts module in at module
        # scope would risk import cycles.
        from omnigent.server.managed_hosts import is_repo_workspace, parse_repo_workspace

        if self.host_type == "managed":
            if self.host_id is not None:
                raise ValueError(
                    "host_type 'managed' lets the server provision the host; "
                    "host_id must not be set"
                )
            if self.workspace is not None:
                try:
                    parse_repo_workspace(self.workspace)
                except ValueError as exc:
                    raise ValueError(
                        "host_type 'managed' takes a git repository URL "
                        f"(optionally '#<branch>') as workspace: {exc}"
                    ) from exc
            return self
        if self.sandbox_provider is not None:
            raise ValueError(
                "sandbox_provider only applies to host_type 'managed' — "
                "external hosts are not server-provisioned"
            )
        if self.workspace is not None and is_repo_workspace(self.workspace):
            raise ValueError(
                "a repository-URL workspace requires host_type 'managed' — "
                "external hosts take an absolute path on the host"
            )
        return self


class SessionLabelsResponse(BaseModel):
    """
    Lightweight response body for ``GET /v1/sessions/{id}/labels``.

    :param id: Session identifier, e.g. ``"conv_abc123"``.
    :param labels: Session-scoped guardrails labels. Empty dict when
        no labels have been written.
    """

    id: str
    labels: dict[str, str] = Field(default_factory=dict)


# Stages of a managed-sandbox launch, in pipeline order: the sandbox
# is provisioned, the repository workspace is cloned into it (skipped
# when the session has no repo workspace), the in-sandbox host starts
# and registers, and the agent runner is launched on it. ``ready`` and
# ``failed`` are terminal.


class AutomaticSessionRenameRequest(BaseModel):
    """Proposed title for a framework or agent-initiated rename."""

    title: str = Field(min_length=2, max_length=DEFAULT_GENERATED_TITLE_MAX_CHARS)

    model_config = ConfigDict(extra="forbid")


class AutomaticSessionRenameResponse(BaseModel):
    """Result of a conditional automatic session rename."""

    renamed: bool
    title: str | None = None
    reason: Literal["not_top_level", "no_seed", "title_changed", "generation_failed"] | None = None


class ResetSessionModelOverrideRequest(BaseModel):
    """Reset a launch-time model selection only while that selection is current."""

    expected_model_override: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class ResetSessionModelOverrideResponse(BaseModel):
    """Whether the launch-time selection was still current and was cleared."""

    reset: bool


class BackgroundSessionTitleRequest(BaseModel):
    """Private runner request for isolated background title inference."""

    prompt: str = Field(min_length=1, max_length=20_000)
    additional_instructions: str | None = Field(default=None, max_length=4_000)
    agent_id: str | None = None
    model_override: str | None = None
    harness_override: str | None = None
    sub_agent_name: str | None = None

    model_config = ConfigDict(extra="forbid")


class BackgroundSessionTitleResponse(BaseModel):
    """Private runner result for background title inference."""

    status: Literal["generated", "unsupported"]
    title: str | None = None


class CodexGoalObject(BaseModel):
    """
    Current Codex goal state for a Codex-native session.

    Mirrors Codex app-server's ``ThreadGoal`` shape using Omnigent's
    snake-case API convention. ``created_at`` and ``updated_at`` are optional
    because older app-server documentation examples omit them even though the
    current protocol includes them.

    :param thread_id: Codex app-server thread id, e.g. ``"thr_123"``.
    :param objective: Goal objective text, e.g.
        ``"Finish the migration and keep tests green"``.
    :param status: Raw Codex goal lifecycle status, e.g. ``"active"``.
    :param token_budget: Optional token budget, e.g. ``40000``.
        ``None`` means no explicit budget is set.
    :param tokens_used: Tokens spent while pursuing this goal, e.g. ``1024``.
    :param time_used_seconds: Wall-clock seconds spent on this goal,
        e.g. ``60``.
    :param created_at: Unix timestamp when the goal was created, e.g.
        ``1776272400``. ``None`` when not provided by Codex.
    :param updated_at: Unix timestamp when the goal was last updated, e.g.
        ``1776272460``. ``None`` when not provided by Codex.
    """

    thread_id: str
    objective: str
    status: str
    token_budget: Annotated[int, Strict(), Field(gt=0)] | None = None
    tokens_used: Annotated[int, Strict(), Field(ge=0)]
    time_used_seconds: Annotated[int, Strict(), Field(ge=0)]
    created_at: int | None = None
    updated_at: int | None = None


class CodexGoalResponse(BaseModel):
    """
    Response body for reading or setting a Codex-native session goal.

    :param goal: Current goal state, or ``None`` when the session has no
        persisted Codex goal.
    """

    goal: CodexGoalObject | None


class SetCodexGoalRequest(BaseModel):
    """
    Request body for ``PUT /v1/sessions/{id}/codex_goal``.

    :param objective: Goal objective text, e.g.
        ``"Finish the migration and keep tests green"``. Must be non-empty
        after trimming and no longer than 4000 characters, matching Codex
        app-server's goal contract.
    :param token_budget: Optional positive token budget, e.g. ``40000``.
        Explicit JSON ``null`` clears the Codex goal budget; omitting the
        field leaves it absent from the forwarded request.
    :param status: Optional user-selected goal status. ``"active"`` starts or
        resumes the goal, and ``"paused"`` stores it paused. Omit this field
        to preserve Codex's current lifecycle state.
    """

    objective: str = Field(min_length=1, max_length=4000)
    token_budget: Annotated[int, Strict(), Field(gt=0)] | None = None
    status: Literal["active", "paused"] | None = None

    model_config = ConfigDict(extra="forbid")


class UpdateCodexGoalStatusRequest(BaseModel):
    """
    Request body for ``PATCH /v1/sessions/{id}/codex_goal/status``.

    Codex app-server represents pause/resume as ``thread/goal/set`` status
    updates. Omnigent exposes the two user-driven transitions explicitly:
    ``"paused"`` pauses an active goal, and ``"active"`` resumes a paused,
    blocked, or usage-limited goal.

    :param status: Target Codex goal status, either ``"paused"`` or
        ``"active"``.
    """

    status: Literal["active", "paused"]

    model_config = ConfigDict(extra="forbid")


class ClearCodexGoalResponse(BaseModel):
    """
    Response body for ``DELETE /v1/sessions/{id}/codex_goal``.

    :param cleared: ``True`` when Codex removed an existing goal; ``False``
        when no goal was present.
    """

    cleared: bool


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

    @model_validator(mode="after")
    def _check_managed_fork_fields(self) -> Self:
        """
        Enforce the per-``host_type`` contract for a fork.

        Mirrors :meth:`_SessionCreateRequestBase._check_managed_host_fields`:
        a managed fork's ``workspace``, when given, must be a git repository
        URL (optionally ``#<branch>``) the server clones into the sandbox —
        a filesystem path points at nothing in a sandbox that doesn't exist
        yet. Both ``sandbox_provider`` and ``workspace`` are meaningless on
        an external fork, which picks its host and directory afterwards.
        Failing at validation returns a 422 with the field named instead of
        silently ignoring the caller's intent.

        :returns: The validated instance.
        :raises ValueError: On a managed workspace that isn't a valid
            repository URL, or ``sandbox_provider`` / ``workspace`` without
            ``host_type: "managed"``.
        """
        # Lazy import: schemas is imported by nearly every module, so
        # pulling the (FastAPI/click-importing) managed-hosts module in
        # at module scope would risk import cycles.
        from omnigent.server.managed_hosts import parse_repo_workspace

        if self.host_type == "managed":
            if self.workspace is not None:
                try:
                    parse_repo_workspace(self.workspace)
                except ValueError as exc:
                    raise ValueError(
                        "host_type 'managed' takes a git repository URL "
                        f"(optionally '#<branch>') as workspace: {exc}"
                    ) from exc
            return self
        if self.sandbox_provider is not None:
            raise ValueError(
                "sandbox_provider only applies to host_type 'managed' — "
                "external hosts are not server-provisioned"
            )
        if self.workspace is not None:
            raise ValueError(
                "workspace only applies to host_type 'managed' — an external "
                "fork picks its directory when it binds a host"
            )
        return self


class ReadStatePutRequest(BaseModel):
    """
    Request body for ``PUT /v1/sessions/{session_id}/read-state``.

    Sets the *calling user's* read tracking for one session. Mirrors the
    two values the web client keeps per session: a "last seen" wall-clock
    baseline (seconds since epoch) and an explicit "marked unread"
    override. The unread dot shows when ``updated_at > last_seen`` and the
    session is finished; ``unread`` separately pins the override so the
    thread the user is *viewing* (or a running one) still surfaces the dot
    where the automatic "seen" logic would otherwise suppress it.

    :param last_seen: Wall-clock baseline in seconds, e.g. ``1717000000``.
        Marking seen sets this to "now"; marking unread pins it to
        ``updated_at - 1`` so the row reads unseen.
    :param unread: Whether this session is explicitly flagged unread for
        the caller.
    """

    last_seen: int
    unread: bool

    model_config = ConfigDict(extra="forbid")


class SessionSwitchAgentRequest(BaseModel):
    """
    Request body for ``POST /v1/sessions/{id}/switch-agent``.

    Rebinds an existing session in place to a different agent/harness,
    keeping the same session (transcript, comments, files, workspace).
    Unlike fork, no new session is created.

    :param agent_id: Built-in agent to switch the session to, e.g.
        ``"ag_builtin_codex"``. Must be a built-in agent (one listed by
        ``GET /v1/agents``) and different from the session's current
        agent.
    """

    agent_id: str

    model_config = ConfigDict(extra="forbid")


class SessionUsage(BaseModel):
    """
    One session's rolled-up LLM spend for the ``GET /v1/usage`` report.

    ``cost_usd`` is the subtree total — the session plus every sub-agent it
    spawned — read from ``session_usage`` via
    :func:`omnigent.runtime.policies.builder.load_session_usage`. It is the
    authoritative session figure (the same value the web session sidebar
    shows as "Session cost" and the daily rollup records).

    ``models`` is the per-model cost breakdown, mirroring the web session
    sidebar's per-model list. Deliberately **not guaranteed to sum to
    ``cost_usd``**: native harnesses report a single cumulative session
    total and the server attributes it to the currently-active model, so on
    a session that switched models mid-run each model's bucket is a snapshot
    of the running total rather than that model's own spend. The header
    ``cost_usd`` stays authoritative; the per-model values are shown
    faithfully as recorded (same convention as the web UI).

    :param id: Session/conversation identifier, e.g. ``"conv_abc123"``.
    :param created_at: Unix epoch seconds of creation.
    :param updated_at: Unix epoch seconds of last activity.
    :param title: Optional human-readable title.
    :param cost_usd: Authoritative cumulative USD spend for this session's
        subtree.
    :param models: Per-model recorded cost, keyed by the raw harness model
        id (e.g. ``{"claude-opus-4-8": 14.03}``). Empty when no per-model
        cost was recorded. May not sum to ``cost_usd`` (see above).
    """

    id: str
    created_at: int
    updated_at: int
    title: str | None = None
    cost_usd: float = 0.0
    models: dict[str, float] = Field(default_factory=dict)
    harness: str | None = None
    other_harnesses: list[str] | None = None
    llm_model: str | None = None
    agent_name: str | None = None


class DailyCost(BaseModel):
    """One day's LLM spend for the daily timeline chart."""

    day: str
    cost_usd: float = 0.0


class UsageReport(BaseModel):
    """
    Aggregated LLM usage for the calling user, powering ``omni usage``.

    The cost summary is sourced from the per-user daily-cost rollup
    (``user_daily_cost``), which attributes spend to the UTC calendar day it
    occurred on. Windows are therefore calendar-day buckets summed back from
    today — ``cost_today`` / ``cost_last_7d`` / ``cost_last_30d`` — not
    rolling wall-clock hours, so a weeks-old session touched today is not
    counted wholly in "today".

    The ``sessions`` list is a separate detail view built from each session's
    cumulative ``session_usage`` (newest activity first), so the summary and
    the per-session list come from different sources and are not guaranteed
    to tie out to the cent (the summary counts every priced turn ever
    recorded for the user; the list only covers the user's currently-listed
    top-level sessions).

    :param cost_today: Total spend on the current UTC day.
    :param cost_last_7d: Total spend over the last 7 UTC days (incl. today).
    :param cost_last_30d: Total spend over the last 30 UTC days (incl. today).
    :param total_cost_usd: All-time total spend from the daily rollup.
    :param sessions: Per-session detail, newest activity first.
    """

    object: Literal["usage_report"] = "usage_report"
    cost_today: float = 0.0
    cost_last_7d: float = 0.0
    cost_last_30d: float = 0.0
    total_cost_usd: float = 0.0
    daily_costs: list[DailyCost] = Field(default_factory=list)
    sessions: list[SessionUsage] = Field(default_factory=list)


# ── Permissions ────────────────────────────────────────────────────


class GrantPermissionRequest(BaseModel):
    """
    Request body for ``PUT /v1/sessions/{id}/permissions``.

    :param user_id: The user to grant access to, e.g.
        ``"alice@example.com"`` or ``"__public__"`` for public
        read access.
    :param level: Numeric permission level: ``1`` = read,
        ``2`` = edit, ``3`` = manage.
    """

    user_id: str
    level: int = Field(ge=1, le=3)


class PermissionObject(BaseModel):
    """
    API representation of a session permission grant.

    :param user_id: The grantee, e.g. ``"alice@example.com"``.
    :param conversation_id: The session, e.g.
        ``"conv_abc123"``.
    :param level: Numeric permission level (1=read, 2=edit,
        3=manage).
    """

    user_id: str
    conversation_id: str
    level: int


# ─────────────────────────────────────────────────────────────────────
# STREAM EVENTS — typed Pydantic union for SSE event boundary
# ─────────────────────────────────────────────────────────────────────
#
# The public SSE event source of truth is ``omnigent.protocol``. This section
# retains server-internal event variants that never enter ``ServerStreamEvent``.
#
# The SSE endpoint is:
#
# * ``GET /v1/sessions/{id}/stream`` — session live-tail (multiplexes
#   the underlying response stream and surfaces queue/interrupt
#   semantics).
#
# Two event families coexist:
#
# * ``session.*`` — session-scoped lifecycle events
#   (:class:`SessionStatusEvent`, :class:`SessionInputConsumedEvent`,
#   :class:`SessionInterruptedEvent`, :class:`SessionCreatedEvent`).
# * ``response.*`` — pass-through Responses-API events emitted by the
#   executor; the session stream multiplexes them unchanged.
#
# Channel split (per ``designs/session_rearchitecture.md`` §3 "Two
# channels"). Each event variant is conceptually either *transient*
# or *persistent*:
#
# * Transient (SSE-only) — text/reasoning deltas, turn lifecycle
#   events, ``session.*`` lifecycle events, retry/heartbeat/error
#   signals, ``approval_required``. Fire-and-forget on the SSE
#   stream — NOT persisted.
# * Persistent (POST + SSE replay) — assistant messages, tool calls,
#   tool results, and compaction summaries. Persist-then-publish is
#   enforced inside ``_persist_and_stream``.
#
# Wire-shape note: the server today emits some events with a flat
# shape (``{"type": ..., <fields>}``) and others with a nested
# ``{"type": ..., "data": {...}}`` envelope. The Pydantic models
# below match the wire shapes verbatim — see each model's docstring
# for the emit site reference.
# ─────────────────────────────────────────────────────────────────────


# ── Module-level constants (rule 34) ──────────────────────────────

# Public and internal stream models inherit the protocol event base, which
# retains additive fields from newer producers during version skew.


class InjectionConsumedEvent(_SSEEventBase):
    """
    Runner-internal marker: a mid-turn injection was consumed.

    Emitted by the executor adapter (``_watch_injections``) once the
    inner executor accepts a live mid-turn injection into the running
    turn. It rides the harness→runner turn stream and is intercepted by
    the runner's proxy_stream relay: the runner drops the buffered copy
    of the matching message so it is NOT re-delivered as a continuation
    turn (RUNNER_MESSAGE_INGEST.md Part B). This event is **never**
    published to the client session stream or relayed upstream — it is
    purely a runner-internal exactly-once handshake.

    :param type: Always ``"injection.consumed"``.
    :param injection_id: Correlation id the runner stamped on the
        forwarded injection, e.g. ``"inj_ab12cd34ef56"``. Matches the
        ``injection_id`` on the buffered message the runner drops.
    """

    type: Literal["injection.consumed"]
    injection_id: str


# Events the harness may emit on its per-turn SSE stream that are
# runner-internal: the runner intercepts and consumes them (matching by
# ``type`` on the raw frame, see ``omnigent/runner/app.py`` proxy_stream)
# and never relays them to clients. They are deliberately NOT part of the
# public :data:`ServerStreamEvent` union / openapi. This alias types the
# scaffold's per-turn event queue, which carries both the public events and
# these internal markers. See ``designs/RUNNER_MESSAGE_INGEST.md`` Part B.


class PolicyEvaluationRequestEvent(_SSEEventBase):
    """
    Runner-internal marker: harness requests policy evaluation.

    Emitted by the executor adapter before or after an LLM call so
    the runner can evaluate ``LLM_REQUEST`` / ``LLM_RESPONSE``
    policies on the Omnigent server. The runner intercepts this event in
    ``proxy_stream``, calls the Omnigent server's
    ``POST /sessions/{id}/policies/evaluate`` endpoint, and posts
    the verdict back to the harness as a ``policy_verdict`` inbound
    event. This event is **never** relayed to external clients —
    it is purely a runner↔harness handshake.

    :param type: Always ``"policy_evaluation.requested"``.
    :param evaluation_id: Unique correlation id for this
        evaluation, e.g. ``"poleval_abc123"``. The runner echoes
        it back in the ``policy_verdict`` inbound event so the
        scaffold can resolve the correct parked Future.
    :param phase: Proto-style phase string, e.g.
        ``"PHASE_LLM_REQUEST"`` or ``"PHASE_LLM_RESPONSE"``.
    :param data: Event data dict passed to the Omnigent server's
        policy evaluate endpoint, e.g.
        ``{"model": "gpt-4o", "messages_count": 42}``.
    """

    type: Literal["policy_evaluation.requested"]
    evaluation_id: str
    phase: str
    data: dict[str, Any]


class SubagentStartedEvent(_SSEEventBase):
    """
    Runner-internal marker: the harness agent spawned a sub-agent.

    ACP agents that delegate report it in their own dialect (see
    :mod:`omnigent.inner.acp_subagents`); the executor normalizes that to a
    :class:`~omnigent.inner.executor.SubAgentStarted`, which the adapter emits as
    this event. The runner intercepts it in ``proxy_stream`` and POSTs
    ``external_subagent_start`` to the Omnigent server, minting a child session so
    the web "Subagents" panel lists one row per child. **Never** relayed to
    external clients — the client learns of the child via ``session.created``.

    :param type: Always ``"subagent.started"``.
    :param child_key: Stable, per-turn-unique id for the sub-agent — the
        idempotency key when the child is minted and the correlation key for the
        later :class:`SubagentCompletedEvent`.
    :param title: Short human label for the row, e.g. ``"mathutils"``.
    :param task: The instruction the sub-agent was given.
    """

    type: Literal["subagent.started"]
    child_key: str
    title: str
    task: str = ""


class SubagentCompletedEvent(_SSEEventBase):
    """
    Runner-internal marker: a previously-announced sub-agent finished.

    Emitted by the adapter from a
    :class:`~omnigent.inner.executor.SubAgentCompleted`. The runner records the
    outcome on the child session minted for the matching ``child_key`` (status +
    the summary as its output). **Never** relayed to external clients.

    :param type: Always ``"subagent.completed"``.
    :param child_key: Matches the :attr:`SubagentStartedEvent.child_key`.
    :param ok: Whether the sub-agent reported success.
    :param summary: The sub-agent's closing summary.
    """

    type: Literal["subagent.completed"]
    child_key: str
    ok: bool = True
    summary: str = ""


class SubagentToolCallEvent(_SSEEventBase):
    """
    Runner-internal marker: an ACP sub-agent ran a tool call.

    Emitted by the adapter from a
    :class:`~omnigent.inner.executor.SubAgentToolCall` — a call the sub-agent
    made inside its delegated work, which belongs in the child's transcript, not
    the parent stream. The runner appends it as a ``function_call`` conversation
    item on the child session minted for the matching ``child_key``. **Never**
    relayed to external clients.

    :param type: Always ``"subagent.tool_call"``.
    :param child_key: Matches the :attr:`SubagentStartedEvent.child_key`.
    :param call_id: The tool call's id, used as the child item's ``call_id``.
    :param name: Human tool label for the card, e.g. ``"Wrote mathutils.py"``.
    :param arguments: JSON-encoded arguments string (the tool's raw input).
    """

    type: Literal["subagent.tool_call"]
    child_key: str
    call_id: str
    name: str
    arguments: str = ""


HarnessStreamEvent = (
    ServerStreamEvent
    | InjectionConsumedEvent
    | PolicyEvaluationRequestEvent
    | SubagentStartedEvent
    | SubagentCompletedEvent
    | SubagentToolCallEvent
)


# ── Projects ──────────────────────────────────────────────────────


class ProjectOrderRequest(BaseModel):
    """Rank owned project IDs; unranked projects append in discovery order.

    Null selects alphabetical mode without erasing the remembered manual IDs.
    """

    ordered_project_ids: (
        list[Annotated[str, Field(min_length=32, max_length=32, pattern="^[0-9a-f]{32}$")]] | None
    ) = Field(..., max_length=10000)


class ProjectOrderResponse(BaseModel):
    """Current sorting mode and the manual order retained in either mode."""

    sort_mode: Literal["alphabetical", "manual"]
    ordered_project_ids: list[str] | None


class ProjectObject(BaseModel):
    """
    A first-class project (see ``designs/PROJECTS_PRD.md``).

    :param id: Project id (bare 32-char hex).
    :param object: Discriminator; always ``"project"``.
    :param name: Human-readable project name, unique per owner.
    :param created_at: Unix epoch seconds at creation.
    :param updated_at: Unix epoch seconds of the last write, or ``None``.
    :param config: Default session settings as an opaque JSON object (host,
        workspace, harness, model, reasoning effort, git base-branch, …). Empty
        when the project stores no defaults. The key vocabulary is owned by the
        client; the server persists and returns it whole. Values are hints the
        new-chat dialog pre-fills, not enforced requirements.
    """

    id: str
    object: Literal["project"] = "project"
    name: str
    created_at: int
    updated_at: int | None = None
    config: dict[str, Any] = Field(default_factory=dict)


class ProjectList(BaseModel):
    """Response for ``GET /v1/projects``.

    :param object: Discriminator; always ``"list"``.
    :param data: The caller's projects.
    """

    object: Literal["list"] = "list"
    data: list[ProjectObject]


class SessionProjectSummary(BaseModel):
    """One entry of ``GET /v1/sessions/projects`` — a sidebar project folder.

    Dual-read union of first-class projects and legacy ``omni_project``
    label-projects, keyed by name.

    :param id: First-class project id when one exists, or ``None`` for a
        label-only project not yet promoted to the ``projects`` table.
    :param name: Project name (the folder's display name and union key).
    :param icon: The project's chosen emoji icon (a unicode grapheme), read
        from its ``config``; ``None`` when unset or for a label-only folder,
        so the sidebar falls back to the default folder glyph.
    """

    id: str | None = None
    name: str
    icon: str | None = None


class CreateProjectRequest(BaseModel):
    """
    Request body for ``POST /v1/projects``.

    :param name: Human-readable project name. Trimmed; must be non-empty and
        at most 100 characters; unique among the caller's projects.
    :param config: Optional default session settings (opaque JSON object).
        Omitted / empty stores no defaults.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        """Trim and bound the project name.

        :param value: The raw name from the request.
        :returns: The trimmed name.
        :raises ValueError: If empty/whitespace-only or over 100 chars.
        """
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("name must not be empty")
        if len(trimmed) > 100:
            raise ValueError("name must be at most 100 characters")
        return trimmed


class UpdateProjectRequest(BaseModel):
    """
    Request body for ``PATCH /v1/projects/{project_id}``.

    All fields optional; ``None`` leaves a field unchanged.

    :param name: New project name. ``None`` leaves it unchanged; otherwise
        trimmed, non-empty, at most 100 characters.
    :param config: New config object to replace the stored one. ``None`` leaves
        it unchanged; an empty object ``{}`` clears the stored defaults.
    """

    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    config: dict[str, Any] | None = None

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str | None) -> str | None:
        """Trim and bound the project name when provided.

        :param value: The raw name from the request, or ``None``.
        :returns: The trimmed name, or ``None``.
        :raises ValueError: If provided but empty/whitespace-only or over 100.
        """
        if value is None:
            return None
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("name must not be empty")
        if len(trimmed) > 100:
            raise ValueError("name must be at most 100 characters")
        return trimmed
