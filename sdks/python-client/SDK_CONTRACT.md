# OmniGent Python SDK contract ledger

Status: PR 0 implementation contract
Source baseline: `b3308f9f852617e6dd5be1c823f440785c40f26a`
SDK baseline: `omnigent-client==0.15.0.dev0`
Reviewed: 2026-09-17

## Purpose

This ledger defines the REST behavior that the mature Python SDK may expose.
The SDK is a Python interface to existing OmniGent routes; it does not add a
runtime, persistence model, replay protocol, turn resource, or orchestration
layer.

The OpenAI Agents API is a directional interaction reference, not the product
goal and not a wire-compatibility or global parity promise. The relevant
official documentation is the [Agents API architecture][openai-architecture],
[quickstart][openai-quickstart], [sessions][openai-sessions], and [session
events][openai-events]. We adopt only the Python ergonomics that map truthfully
to existing OmniGent behavior: nested resources, sync and async clients, typed
models, context-managed streams, durable-item recovery, and explicit deletion.

[openai-architecture]: https://developers.openai.com/api/docs/guides/agents-api/architecture
[openai-quickstart]: https://developers.openai.com/api/docs/guides/agents-api/quickstart
[openai-sessions]: https://developers.openai.com/api/docs/guides/agents-api/sessions
[openai-events]: https://developers.openai.com/api/docs/guides/agents-api/sessions/events

## Classification

- **Stable**: published in `openapi.json` and suitable for the primary API.
- **Preview**: implemented today but hidden from OpenAPI, so the SDK must label
  it as preview and avoid a stability promise.
- **Compatibility**: an existing SDK surface retained during migration.
- **Deferred**: an existing route that is outside the first mature surface.
- **Unsupported**: no matching OmniGent route or durable semantic exists.

## Route-to-method ledger

Normal error statuses are translated through the shared SDK error hierarchy.
The table records successful statuses and capability caveats.

| Classification | Target SDK method | Existing route or composition | Success | Notes |
|---|---|---|---:|---|
| Stable | `agents.list(...)` | `GET /v1/agents` | 200 | `limit`, `after`, `before`, `order`; no top-level agent CRUD is invented. |
| Stable | `agents.sessions.create(agent_id=...)` | JSON `POST /v1/sessions` | 201 | Returns a full `SessionResponse`. |
| Stable composition | `agents.sessions.create(bundle=...)` | multipart `POST /v1/sessions`, then `retrieve` | 201, 200 | The first response has only `session_id`, `agent_id`, and `agent_name`. |
| Preview composition | `agents.sessions.create(input=..., stream=False)` | create, `events.create`, `retrieve` | 201, 202, 200 | Returns without waiting for turn completion. Never uses `initial_items` as a start-work shortcut. |
| Preview composition | `agents.sessions.create(input=..., stream=True)` | create, open stream through heartbeat, `events.create` | 201, 200, 202 | The create composition consumes `session.heartbeat` before posting input or returning its already-open stream; failure must not post input. |
| Stable | `agents.sessions.retrieve(id, ...)` | `GET /v1/sessions/{id}` | 200 | Supports `include_items`, `include_liveness`, and `refresh_state`. |
| Stable | `agents.sessions.list(...)` | `GET /v1/sessions` | 200 | Preserve every current filter and cursor field. |
| Stable | `agents.sessions.update(id, ...)` | `PATCH /v1/sessions/{id}` | 200 | Strict request; returns a full session snapshot with `items=[]`. |
| Stable | `agents.sessions.delete(id, ...)` | `DELETE /v1/sessions/{id}` | 200 | Explicit remote deletion only; client/stream close never deletes. Preserve delete query options. |
| Stable | `agents.sessions.fork(id, ...)` | `POST /v1/sessions/{id}/fork` | 201 | Existing truncation, run-configuration, managed-host, `side_chat`, and referenced-file carry-forward behavior. |
| Preview | `agents.sessions.events.create(id, events=...)` | `POST /v1/sessions/{id}/events` | 202 | One event or ordered batch of 1–100; batches are not atomic. Public allowlist is below. |
| Preview | `agents.sessions.events.cancel(id)` | same event route with `interrupt` | 202 | Convenience spelling only; no new cancellation semantics. |
| Stable | `agents.sessions.events.stream(id)` | `GET /v1/sessions/{id}/stream` | 200 | Readiness heartbeat and live tail, with limited current-state resource/presence events; no history replay or `Last-Event-ID`. Context-managed local ownership. |
| Stable | `agents.sessions.items.list(id, ...)` | `GET /v1/sessions/{id}/items` | 200 | `limit`, `after`, `before`, `order`; retain all page metadata. |
| Stable | `agents.sessions.subagents.list(id, ...)` | `GET /v1/sessions/{id}/child_sessions` | 200 | `limit`, `after`, `before`, `order`, `tool`, `session_name`. Children are ordinary sessions. |
| Stable | `agents.sessions.files.list(id, ...)` | `GET /v1/sessions/{id}/resources/files` | 200 | Returns 501 when the deployment has no file store. |
| Stable | `agents.sessions.files.upload(id, ...)` | multipart `POST /v1/sessions/{id}/resources/files` | 201 | Session-scoped only. |
| Stable | `agents.sessions.files.retrieve(id, file_id)` | `GET /v1/sessions/{id}/resources/files/{file_id}` | 200 | Metadata, not bytes. |
| Stable | `agents.sessions.files.content(id, file_id)` | `GET /v1/sessions/{id}/resources/files/{file_id}/content` | 200/304 | `download(...)` writes these bytes locally. |
| Stable | `agents.sessions.files.delete(id, file_id)` | `DELETE /v1/sessions/{id}/resources/files/{file_id}` | 200 | Returns the server deletion object. |
| Stable | `agents.sessions.files.copy(id, ...)` | `POST /v1/sessions/{id}/resources/files:copy` | 200 | Existing strict-ancestor lineage rules apply. |
| Stable | `agents.sessions.agent.retrieve(id)` | `GET /v1/sessions/{id}/agent` | 200 | Session-bound agent only. |
| Stable | `agents.sessions.agent.contents(id)` | `GET /v1/sessions/{id}/agent/contents` | 200 | Raw gzip bytes. |
| Stable | `agents.sessions.agent.update(id, bundle=...)` | multipart `PUT /v1/sessions/{id}/agent` | 200 | Existing name-immutability and session-scoped-agent rules apply. |
| Preview | `agents.sessions.elicitations.retrieve(id, elicitation_id)` | `GET /v1/sessions/{id}/elicitations/{elicitation_id}` | 200 | Process-memory state; not durable required actions. |
| Preview | `agents.sessions.elicitations.resolve(id, elicitation_id, ...)` | `POST /v1/sessions/{id}/elicitations/{elicitation_id}/resolve` | 202 | Existing MCP-shaped result; never defaults to approval. |
| Preview | `agents.sessions.compact(id)` | event route with `compact` | 202 | Existing control, not part of the stable public event-input union. |

`client.sessions` remains an alias to the same target resource object during
migration. It must not be a second implementation.

### Complete selected operation signatures

The exact synchronous target signatures are below. `SyncCursorPage[T]` and
`AsyncCursorPage[T]` are the two I/O views over one cursor-page implementation
and the canonical wire values. Both are `list[T]` subclasses for the already
loaded page, preserving the current `SessionsNamespace.list()` contract for
truthiness, iteration, `len()`, integer/slice indexing, and `isinstance(page,
list)`. Their `data` property exposes that same loaded list; `first_id`,
`last_id`, `has_more`, and next-page helpers carry pagination. List operations
never fetch another page. One shared compatibility contract covers these
behaviors instead of repeating tests for each list method. This is not a
second protocol model tree.

```text
agents.list(
    *,
    limit: int = 20,
    after: str | None = None,
    before: str | None = None,
    order: Literal["asc", "desc"] = "desc",
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SyncCursorPage[AgentObject]

sessions.retrieve(
    session_id: str,
    *,
    include_items: bool = True,
    include_liveness: bool = True,
    refresh_state: bool = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResponse

sessions.list(
    *,
    limit: int = 20,
    after: str | None = None,
    before: str | None = None,
    agent_id: str | None = None,
    agent_name: str | None = None,
    order: Literal["asc", "desc"] = "desc",
    sort_by: Literal["created_at", "updated_at"] = "created_at",
    search_query: str | None = None,
    include_archived: bool = False,
    kind: Literal["default", "sub_agent", "any"] = "default",
    project: str | None = None,
    pinned: bool = False,
    visibility: Literal["all", "mine", "shared", "archived"] = "all",
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SyncCursorPage[SessionListItem]

sessions.delete(
    session_id: str,
    *,
    delete_branch: bool = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> ConversationDeleted

sessions.fork(
    source_session_id: str,
    *,
    title: str | None = None,
    agent_id: str | None = None,
    up_to_response_id: str | None = None,
    model_override: str | None | NotGiven = NOT_GIVEN,
    reasoning_effort: str | None | NotGiven = NOT_GIVEN,
    terminal_launch_args: Sequence[str] | None | NotGiven = NOT_GIVEN,
    codex_bypass_sandbox: bool = False,
    host_type: Literal["external", "managed"] = "external",
    sandbox_provider: str | None = None,
    workspace: str | None | NotGiven = NOT_GIVEN,
    side_chat: bool = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResponse

events.stream(
    session_id: str,
    *,
    idle: bool = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionEventStream

items.list(
    session_id: str,
    *,
    limit: int = 100,
    after: str | None = None,
    before: str | None = None,
    order: Literal["asc", "desc"] = "asc",
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SyncCursorPage[SessionItem]

subagents.list(
    session_id: str,
    *,
    limit: int = 20,
    after: str | None = None,
    before: str | None = None,
    order: Literal["asc", "desc"] = "desc",
    tool: str | None = None,
    session_name: str | None = None,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SyncCursorPage[ChildSessionSummary]

files.list(
    session_id: str,
    *,
    limit: int = 20,
    after: str | None = None,
    before: str | None = None,
    order: Literal["asc", "desc"] = "desc",
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SyncCursorPage[SessionResourceObject]
```

The asynchronous resource exposes the same parameters and protocol return
models. Calls are awaited; list methods return `AsyncCursorPage[T]`, and
`events.stream` returns `AsyncSessionEventStream`. No selected method accepts
an arbitrary `extra_body`.

`NotGiven` and its singleton `NOT_GIVEN` are public SDK support values. They
are required here because this route inspects Pydantic `model_fields_set`.
For `model_override`, `reasoning_effort`, `terminal_launch_args`, and
`workspace`, `NOT_GIVEN` omits the JSON key and preserves the server's
inheritance behavior; Python `None` sends JSON `null` and requests the
route's explicit clear behavior. The serializer must not globally drop
`None`, and it must never serialize the sentinel. One parameterized transport
contract covers omit, null, and concrete values for these four fields.

For signatures in this section, `Timeout` means
`float | httpx.Timeout | None`, `Headers` means
`Mapping[str, str] | None`, and `Query` means
`Mapping[str, str | int | float | bool | None] | None`. These are notation in
this ledger, not new wrapper objects.

The same sentinel preserves PATCH field presence. The target update signature
is:

```text
sessions.update(
    session_id: str,
    *,
    runner_id: str | None | NotGiven = NOT_GIVEN,
    title: str | None | NotGiven = NOT_GIVEN,
    labels: Mapping[str, str] | None | NotGiven = NOT_GIVEN,
    reasoning_effort: str | None | NotGiven = NOT_GIVEN,
    model_override: str | None | NotGiven = NOT_GIVEN,
    collaboration_mode: str | None | NotGiven = NOT_GIVEN,
    permission_mode: str | None | NotGiven = NOT_GIVEN,
    approval_mode: str | None | NotGiven = NOT_GIVEN,
    cost_control_mode_override: Literal["on", "off"] | None | NotGiven = NOT_GIVEN,
    subagent_routing_override: Literal["on", "off"] | None | NotGiven = NOT_GIVEN,
    share_workspace_files: bool | None | NotGiven = NOT_GIVEN,
    external_session_id: str | None | NotGiven = NOT_GIVEN,
    terminal_launch_args: Sequence[str] | None | NotGiven = NOT_GIVEN,
    archived: bool | None | NotGiven = NOT_GIVEN,
    project_id: str | None | NotGiven = NOT_GIVEN,
    silent: bool = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResponse
```

This preserves the existing presence-sensitive `cost_control_mode_override`,
`subagent_routing_override`, `share_workspace_files`, and `project_id`
behavior and does not reinterpret `None`: for example, explicit null clears
the two routing overrides but is rejected for `project_id`, just as the route
does today. Other fields retain their documented server handling of explicit
null. `silent=False` needs no sentinel because omission and explicit false are
equivalent.

### Remaining selected operation signatures

The following are the exact synchronous target signatures; the asynchronous
resource has the same parameters and return models, with the result awaited.

```text
events.create(
    session_id: str,
    *,
    events: PublicSessionEventInput | Sequence[PublicSessionEventInput],
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> EventAcknowledgement | list[EventAcknowledgement]

events.cancel(
    session_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> EventAcknowledgement

sessions.compact(
    session_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> EventAcknowledgement

files.upload(
    session_id: str,
    path: str | os.PathLike[str],
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResourceObject

files.retrieve(
    session_id: str,
    file_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResourceObject

files.content(
    session_id: str,
    file_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> bytes

files.download(
    session_id: str,
    file_id: str,
    to_path: str | os.PathLike[str],
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> pathlib.Path

files.delete(
    session_id: str,
    file_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResourceDeleted

files.copy(
    session_id: str,
    *,
    source_session_id: str,
    file_ids: Sequence[str],
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> CopyFilesResponse

agent.retrieve(
    session_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> AgentObject

agent.contents(
    session_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> bytes

agent.update(
    session_id: str,
    bundle: bytes,
    *,
    filename: str = "agent.tar.gz",
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> AgentObject

elicitations.retrieve(
    session_id: str,
    elicitation_id: str,
    *,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> ElicitationState

elicitations.resolve(
    session_id: str,
    elicitation_id: str,
    *,
    action: Literal["accept", "decline", "cancel"],
    content: Mapping[str, str | int | float | bool | list[str] | None] | None = None,
    meta: Mapping[str, object] | None = None,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> ElicitationResolutionAcknowledgement
```

`PublicSessionEventInput` is the typed `SessionMessage |
FunctionCallOutput | Interrupt` union or a raw mapping that validates through
the same allowlist. A sequence must contain 1--100 entries. `files.copy`
validates `file_ids` as non-empty, unique, non-empty strings before network
I/O and sends both `source_session_id` and `file_ids`; it does not infer a
source from the destination's lineage. `files.download` is the transparent
one-GET `files.content` composition plus a local write. `meta` serializes as
the existing `_meta` wire alias. Elicitation methods remain preview.

The mature method signatures also accept only the shared per-request options
documented for that method (`timeout`, `extra_headers`, and `extra_query`).
They do not accept arbitrary `extra_body` fields.

Published session routes for projects, comments, goals, permissions, policies,
read state, MCP servers, environments, terminals, and GitHub remain deferred
until a real SDK use case takes them through the extension workflow below.
Deferral does not mean deprecation. Empty namespaces are not created in
advance.

## Session creation contract

### Exact create overloads

`CreateSessionInput` means `str | SessionMessage |
Sequence[SessionMessage]`. The two request shapes are separate overload
families so a registered-only argument cannot be silently discarded during a
bundle upload. These are the exact synchronous target overloads:

```text
sessions.create(
    *,
    agent_id: str | None = None,
    project_id: str | None = None,
    initial_items: Sequence[PublicSessionEventInput] | None = None,
    title: str | None = None,
    labels: Mapping[str, str] | None = None,
    parent_session_id: str | None = None,
    sub_agent_name: str | None = None,
    host_type: Literal["external", "managed"] = "external",
    host_id: str | None = None,
    sandbox_provider: str | None = None,
    workspace: str | None = None,
    workspaces: Sequence[str] | None = None,
    git: SessionGitOptions | Mapping[str, object] | None = None,
    terminal_launch_args: Sequence[str] | None = None,
    model_override: str | None = None,
    reasoning_effort: str | None = None,
    cost_control_mode_override: Literal["on", "off"] | None = None,
    subagent_routing_override: Literal["on", "off"] | None = None,
    harness_override: str | None = None,
    smart_routing_message: str | None = None,
    input: CreateSessionInput | None = None,
    stream: Literal[False] = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResponse

sessions.create(
    *,
    agent_id: str | None = None,
    project_id: str | None = None,
    initial_items: Sequence[PublicSessionEventInput] | None = None,
    title: str | None = None,
    labels: Mapping[str, str] | None = None,
    parent_session_id: str | None = None,
    sub_agent_name: str | None = None,
    host_type: Literal["external", "managed"] = "external",
    host_id: str | None = None,
    sandbox_provider: str | None = None,
    workspace: str | None = None,
    workspaces: Sequence[str] | None = None,
    git: SessionGitOptions | Mapping[str, object] | None = None,
    terminal_launch_args: Sequence[str] | None = None,
    model_override: str | None = None,
    reasoning_effort: str | None = None,
    cost_control_mode_override: Literal["on", "off"] | None = None,
    subagent_routing_override: Literal["on", "off"] | None = None,
    harness_override: str | None = None,
    smart_routing_message: str | None = None,
    input: CreateSessionInput | None = None,
    stream: Literal[True],
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionEventStream

sessions.create(
    bundle: bytes,
    *,
    filename: str = "agent.tar.gz",
    title: str | None = None,
    project_id: str | None = None,
    labels: Mapping[str, str] | None = None,
    reasoning_effort: str | None = None,
    host_id: str | None = None,
    workspace: str | None = None,
    terminal_launch_args: Sequence[str] | None = None,
    parent_session_id: str | None = None,
    host_type: Literal["external", "managed"] = "external",
    sandbox_provider: str | None = None,
    input: CreateSessionInput | None = None,
    stream: Literal[False] = False,
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionResponse

sessions.create(
    bundle: bytes,
    *,
    filename: str = "agent.tar.gz",
    title: str | None = None,
    project_id: str | None = None,
    labels: Mapping[str, str] | None = None,
    reasoning_effort: str | None = None,
    host_id: str | None = None,
    workspace: str | None = None,
    terminal_launch_args: Sequence[str] | None = None,
    parent_session_id: str | None = None,
    host_type: Literal["external", "managed"] = "external",
    sandbox_provider: str | None = None,
    input: CreateSessionInput | None = None,
    stream: Literal[True],
    timeout: Timeout = None,
    extra_headers: Headers = None,
    extra_query: Query = None,
) -> SessionEventStream
```

The async resource has the same four overload parameter lists. Each call is
awaited; `stream=False` returns `SessionResponse`, while `stream=True` returns
`AsyncSessionEventStream`.

The caller chooses bundle creation by supplying `bundle`; otherwise the SDK
sends JSON. Bundle bytes remain the first optional positional argument for
compatibility with existing `sessions.create(bundle)` calls; every JSON-create
field is keyword-only, so this is unambiguous. JSON creation requires either a
non-null `agent_id` or a non-null `project_id` whose project configuration
resolves an agent. `agent_id` and `project_id` may coexist where the route
supports it. A null or omitted `project_id` does not relax the legacy
`agent_id` requirement. `input` and `initial_items` are mutually exclusive.

### Registered-agent JSON fields

The server request has these 20 fields. The SDK must not silently omit any:

| Field | Meaning or constraint |
|---|---|
| `agent_id` | Required unless a non-null `project_id` selects the server's project default. |
| `project_id` | Optional first-class project. |
| `initial_items` | History seed only; not the implementation of `input=`. |
| `title` | Optional session title. |
| `labels` | Initial labels. |
| `parent_session_id` | Optional parent session. |
| `sub_agent_name` | Optional subagent name. |
| `host_type` | `external` or `managed`. |
| `host_id` | External host binding; invalid with managed host type. |
| `sandbox_provider` | Managed-host provider only. Availability is deployment-specific. |
| `workspace` | External path or managed repository URL according to `host_type`. |
| `workspaces` | Managed multi-repository URLs; mutually exclusive with `workspace`. |
| `git` | Existing worktree options; requires a host unless project behavior supplies one. |
| `terminal_launch_args` | Existing native-terminal arguments. |
| `model_override` | Session model override. |
| `reasoning_effort` | Session reasoning-effort hint. |
| `cost_control_mode_override` | Existing `on`/`off` override. |
| `subagent_routing_override` | Existing `on`/`off` override. |
| `harness_override` | Existing create-time harness selection. |
| `smart_routing_message` | Routing-only first-message text; not persisted or dispatched. |

`input`, `stream`, `timeout`, `extra_headers`, and `extra_query` are SDK
options, not fields in this JSON schema. `input` composes the existing event
route after creation.

### Uploaded-bundle multipart fields

The multipart parts are `bundle` plus JSON `metadata`. Metadata has exactly:

`title`, `project_id`, `labels`, `reasoning_effort`, `host_id`, `workspace`,
`terminal_launch_args`, `parent_session_id`, `host_type`, and
`sandbox_provider`.

The SDK must not accept registered-only fields and then discard them. Bundle
create returns `CreatedSessionResponse`; the SDK performs the existing follow-up
retrieve only when a full `SessionResponse` is promised.

### Update fields

`PATCH /v1/sessions/{id}` accepts exactly these 16 fields:

`runner_id`, `title`, `labels`, `reasoning_effort`, `model_override`,
`collaboration_mode`, `permission_mode`, `approval_mode`,
`cost_control_mode_override`, `subagent_routing_override`,
`share_workspace_files`, `external_session_id`, `terminal_launch_args`,
`archived`, `project_id`, and `silent`.

The request model is strict. Omitted fields remain unchanged. Field-specific
clear behavior remains the server's behavior; the SDK does not normalize null,
empty string, and clear aliases into one value.

## Session response inventory

`SessionResponse` has **47 fields** at the pinned baseline. Classification says
where a value comes from; it does not remove the field from the public model.

| # | Field | Durability | Contract note |
|---:|---|---|---|
| 1 | `id` | Persisted | Conversation/session identity. |
| 2 | `agent_id` | Persisted | Bound agent identity. |
| 3 | `agent_name` | Derived | Agent-row lookup at snapshot time. |
| 4 | `status` | Derived | Store/runtime lifecycle projection; not success output. |
| 5 | `background_task_count` | Volatile | Live cache overlay. |
| 6 | `background_tasks` | Volatile | Live per-task cache overlay. |
| 7 | `created_at` | Persisted | Creation timestamp. |
| 8 | `updated_at` | Persisted | Last persisted session activity/metadata change. |
| 9 | `title` | Derived | Projection of persisted conversation metadata. |
| 10 | `labels` | Derived | Viewer-aware projection of persisted labels. |
| 11 | `runner_id` | Persisted | Current runner binding. |
| 12 | `host_id` | Persisted | Host binding. |
| 13 | `runner_online` | Volatile | Point-in-time liveness lookup. |
| 14 | `host_online` | Volatile | Point-in-time host liveness lookup. |
| 15 | `host_resumable` | Derived | Managed-provider capability projection. |
| 16 | `reasoning_effort` | Persisted | Session metadata. |
| 17 | `items` | Persisted | Optional embedded durable history. |
| 18 | `permission_level` | Derived | Requesting viewer's effective access. |
| 19 | `sub_agent_name` | Persisted | Child-session metadata. |
| 20 | `kind` | Persisted | Session kind. |
| 21 | `parent_session_id` | Persisted | Parent relationship. |
| 22 | `root_conversation_id` | Persisted | Spawn-tree root. |
| 23 | `llm_model` | Derived | Runtime report or bound-agent fallback. |
| 24 | `harness` | Derived | Bound-agent lookup. |
| 25 | `model_override` | Persisted | Session override. |
| 26 | `cost_control_mode_override` | Persisted | Session override. |
| 27 | `subagent_routing_override` | Persisted | Session override. |
| 28 | `share_workspace_files` | Persisted | Sharing preference. |
| 29 | `context_window` | Derived | Model-registry/environment lookup. |
| 30 | `last_total_tokens` | Derived | Latest stored task usage projection. |
| 31 | `total_cost_usd` | Derived | Cumulative usage/pricing projection. |
| 32 | `usage_by_model` | Derived | Per-model usage projection. |
| 33 | `last_task_error` | Derived | Latest failed-task projection. |
| 34 | `external_session_id` | Persisted | Runtime-native session identity. |
| 35 | `terminal_launch_args` | Persisted | Native-terminal launch metadata. |
| 36 | `pending_elicitations` | Volatile | Process-memory registry; not durable required actions. |
| 37 | `pending_inputs` | Volatile | Process-memory pending-input registry. |
| 38 | `workspace` | Persisted | Validated host workspace/worktree path. |
| 39 | `git_branch` | Persisted | Session worktree branch. |
| 40 | `archived` | Persisted | Archive state. |
| 41 | `todos` | Persisted | Harness-reported plan metadata. |
| 42 | `model_options` | Volatile | Runner-owned live options. |
| 43 | `terminal_pending` | Volatile | Process-memory startup cache. |
| 44 | `sandbox_status` | Volatile | Process-memory managed-launch cache. |
| 45 | `mcp_startup` | Volatile | Process-memory MCP startup cache. |
| 46 | `active_response_id` | Volatile | In-flight cache, not a durable turn ID. |
| 47 | `project_id` | Persisted | First-class project relationship. |

Known fields and unknown additive response fields must round-trip. One complete
fixture covers this inventory; there is no field-by-field test explosion.

## Item contract

The current REST API has two item wire shapes, and PR 0 preserves that fact:

- `SessionResponse.items` contains nested `ConversationItem` values with common
  fields `id`, `response_id`, `type`, `status`, `created_at`, optional
  `created_by`, and a type-specific `data` object.
- `GET /v1/sessions/{id}/items` returns the same common fields but spreads the
  contents of `data` onto each item. It does not include the `data` key.

The SDK deliberately models both truthful wire shapes rather than silently
rewriting one. `Session.items` uses `ConversationItem`; `items.list()` returns
a page of a separate typed flat `SessionItem` union. Durable-history examples
prefer `items.list()`. These are not competing definitions of one wire shape:
the server emits both today.

The nested `data` models and corresponding flat item variants have these
type-specific fields:

| `type` | Type-specific API fields |
|---|---|
| `message` | `role`, `content`, optional `model`, `is_meta`, `interrupted`, `stream_message_id` |
| `function_call` | `model`, `name`, `arguments`, `call_id` |
| `function_call_output` | `call_id`, `output` |
| `error` | `source`, `code`, `message`, optional `level` |
| `reasoning` | `model`, `summary`, optional `content`, `encrypted_content` |
| `compaction` | `summary`, `last_item_id`, `token_count`, optional `model`, `compacted_messages`, `window_id` |
| `native_tool` | `item` |
| `resource_event` | `event_type`, `resource_id`, `resource_type`, optional `resource` |
| `routing_decision` | `model`, `applied`, `rationale`, optional `agent`, `harness`, `scope`, `decision_id`, `raw_model`, `attempted_override`, `router_source`, `task_description` |
| `slash_command` | `model`, `kind`, `name`, `arguments`, optional `output` |
| `terminal_command` | `kind`, optional `input`, `stdout`, `stderr` |

Known item discriminators are strict; additive output fields are retained in
both shapes.

## Public event-input boundary

The stable public-safe allowlist is exactly:

| `type` | Public data |
|---|---|
| `message` | `role="user"`, `content`; may carry existing public `model_override` and `tools` envelope options. |
| `function_call_output` | `call_id`, `output`. |
| `interrupt` | Empty data object. |

Raw dictionaries are accepted only through the same allowlist and strict
per-type validation. `created_by` is internal attribution and is not exposed by
the mature public constructors.

The server may keep a broader private ingress envelope for runner traffic. The
SDK's narrow union and the server's private controls are intentionally distinct
trust boundaries; moving portable types must not accidentally restrict current
runner behavior or publish those controls.

Typed approval and elicitation resolution remain preview methods. The mature
event API excludes `approval`, `mcp_elicitation`, `compact`, `stop_session`,
`retry_session`, `slash_command`, every `external_*` bridge event, and
runner-specific collaboration, permission, approval, model, and routing
controls. Server acceptance is intentionally broader than the public SDK.

### Acknowledgements

The event route returns one acknowledgement for one event and an ordered list
for a batch. Public branches currently use:

- message: `queued: true`, normally an `item_id`, and sometimes a `pending_id`;
- function-call output: `queued: true`, with the existing call/item ID; and
- interrupt: `queued: false`.

Other server branches add `denied`, `reason`, `elicitation_id`,
`child_session_id`, recovery fields, or omit IDs. The output model therefore
preserves additive fields and does not flatten all branches into an invented
fixed `{queued, item_id}` response. A failed batch keeps earlier side effects,
does not execute later entries, and does not return earlier acknowledgements.

## Output event discriminator inventory

The canonical `ServerStreamEvent` union has these 55 discriminators:

| Family | Discriminators |
|---|---|
| Session state | `session.status`, `session.usage`, `session.model`, `session.title`, `session.reasoning_effort`, `session.collaboration_mode`, `session.permission_mode`, `session.codex_approval_mode`, `session.agent_changed`, `session.todos`, `session.terminal_pending`, `session.sandbox_status`, `session.mcp_startup`, `session.model_options`, `session.input.consumed`, `session.interrupted`, `session.created`, `session.superseded`, `session.btw_sidechat`, `session.presence` |
| Session resources | `session.resource.created`, `session.resource.deleted`, `session.child_session.updated`, `session.changed_files.invalidated`, `session.terminal.activity` |
| Deltas and completed output | `response.output_text.delta`, `response.function_call_output.delta`, `response.reasoning.started`, `response.reasoning_text.delta`, `response.reasoning_summary_text.delta`, `response.output_item.done`, `response.output_file.done` |
| Keepalive | `response.heartbeat`, `session.heartbeat` |
| Decisions and browser | `response.elicitation_request`, `response.elicitation_resolved`, `browser.action_request`, `response.policy_denied` |
| Response lifecycle | `response.created`, `response.queued`, `response.in_progress`, `response.completed`, `response.failed`, `response.cancelled`, `response.incomplete` |
| Operational | `response.retry`, `response.error`, `response.compaction.in_progress`, `response.compaction.completed`, `response.compaction.failed`, `response.client_task.cancel` |
| Turn lifecycle | `turn.started`, `turn.completed`, `turn.failed`, `turn.cancelled` |

One output-only `UnknownEvent` retains the unknown discriminator and all raw
fields. It is not added to request validation. Known event models also retain
additive fields. Nullable `sequence_number` is preserved when present, but the
current session stream does not populate it and makes no cursor or replay
guarantee.

## Locked interaction behavior

These are behavioral fixtures, not promises to copy OpenAI wire types.

1. `Omnigent` and `AsyncOmnigent` expose the same resources, parameters,
   models, errors, and semantics; only I/O syntax differs.
2. `client.agents.sessions` is the primary resource hierarchy;
   `client.sessions` is a compatibility alias to the same object.
3. Create with no input performs one create and returns.
4. Registered create with non-streaming input performs create, event submit,
   then retrieve. It does not wait for a final answer.
5. Registered `create(stream=True)` performs create, opens the stream through
   the initial `session.heartbeat`, submits input, and then returns an already-
   open stream object. Entering that object's context only establishes local
   ownership. The bundle branch also performs the existing full-session
   retrieve where required.
6. A missing readiness heartbeat makes `create(stream=True)` raise
   `SessionCompositionError` with phase `stream_open`, retain `session_id`,
   close local transport, and prove no input POST occurred.
7. Input-submit or final-retrieve composition failures never resend input and
   never delete or interrupt the created session.
8. Entering or leaving a stream/client context changes local transport only.
   Examples perform explicit cleanup with `sessions.delete(session_id)`.
9. For a standalone `events.stream(session_id)` manager, context entry opens
   the stream and consumes readiness before a following `events.create` call.
   Iteration stops only on explicit OmniGent terminal event types; there is no
   fabricated `is_root_terminal` because current turn events carry no durable
   turn or root/subagent identity.
10. Durable recovery opens a replacement stream, retrieves session/items, and
    merges by item `id` and `response_id`. The SDK does not automate replay or
    resend writes.
11. Primary quickstarts use only portable required values. Managed providers,
    model IDs, and harness overrides are capability-aware advanced examples.
12. Files retain the OmniGent `files` name and document the deployment's file
    store requirement; they are not renamed to OpenAI artifacts.
13. `with_raw_response` shares the same serializer, transport, and error path
    and exposes status and request ID without making a second request.

## Compatibility inventory

### Existing root exports

At the baseline, `omnigent_client.__all__` had exactly 52 names. The following
imports remain available until an explicit migration release says otherwise;
PR 1 adds the separately documented `NotGiven` and `NOT_GIVEN` values:

`MCP_ELICITATION_METHOD`, `TERMINAL_TASK_STATUSES`, `AnyBlock`, `BlockContext`,
`BlockStream`, `CompactionBlock`, `ElicitationRequest`,
`ElicitationRequestCtx`, `ErrorBlock`, `File`, `FileBlock`, `LocalServer`,
`NativeToolBlock`, `OmnigentClient`, `OmnigentError`, `QueryResult`,
`QueryStream`, `RateLimitedError`, `ReasoningBlock`, `ReasoningChunk`,
`ReasoningStartBlock`, `RegisteredAgent`, `ResponseEndBlock`,
`ResponseStartBlock`, `RetryBlock`, `Session`, `SessionToolCallInfo`,
`SessionsChat`, `SessionsNamespace`, `StaleCursorError`, `StreamBlock`,
`StreamHooks`, `TextChunk`, `TextDone`, `ToolCallDenied`, `ToolCallInfo`,
`ToolCallable`, `ToolExecution`, `ToolGroup`, `ToolHandler`, `ToolMetadata`,
`ToolResultBlock`, `ToolState`, `child_session_busy`, `child_summary_busy`,
`format_tool_args_brief`, `merge_text_across_iterations`, `only_agent`, `pipe`,
`skip_blocks`, `skip_intermediate_ends`, and `tool`.

The existing root `Session` is the legacy Responses helper. The durable session
model must not silently replace it.

All 52 names above use the documented import path
`from omnigent_client import <name>`. This is the mechanical constructor/value
ledger at the baseline (`<factory>` means the existing dataclass factory):

```text
MCP_ELICITATION_METHOD @ builtins = "elicitation/create"
TERMINAL_TASK_STATUSES @ builtins = ("completed", "failed", "cancelled")
AnyBlock @ types = union of the 14 exported block variants
BlockContext @ _blocks (agent=None, depth=0, turn=0, timestamp=<factory>)
BlockStream @ _stream (text_flush_threshold=30)
CompactionBlock @ _blocks (*, ctx=<factory>)
ElicitationRequest @ _events (elicitation_id, message, requested_schema, mode, phase,
  policy_name, content_preview, url=None, target_session_id=None)
ElicitationRequestCtx @ _tool_handler (elicitation_id, message, requested_schema,
  mode, phase, policy_name, content_preview, response_id, url=None,
  target_session_id=None)
ErrorBlock @ _blocks (*, ctx=<factory>, message, source, code="")
File @ _types (id, filename, bytes, created_at)
FileBlock @ _blocks (*, ctx=<factory>, file_id, filename=None)
LocalServer @ _server (agent_path, *, host="127.0.0.1", port=0)
NativeToolBlock @ _blocks (*, ctx=<factory>, tool_type, label, data=<factory>)
OmnigentClient @ _client (base_url, *, headers=None, auth=None, timeout=30.0)
OmnigentError @ _errors (message, status_code=None, code=None)
QueryResult @ _query (text, files=<factory>)
QueryStream @ _query (chunks, files)
RateLimitedError @ _errors (message, status_code=None, code=None)
ReasoningBlock @ _blocks (*, ctx=<factory>, reasoning_text, summary_text)
ReasoningChunk @ _blocks (*, ctx=<factory>, text)
ReasoningStartBlock @ _blocks (*, ctx=<factory>)
RegisteredAgent @ _sessions (id, harness=None)
ResponseEndBlock @ _blocks (*, ctx=<factory>, status, response=None)
ResponseStartBlock @ _blocks (*, ctx=<factory>, model, response_id)
RetryBlock @ _blocks (*, ctx=<factory>, source, attempt, max_attempts, delay_seconds)
Session @ _session (client, model, tool_handler=None, hooks=None)
SessionToolCallInfo @ _sessions_chat (name, arguments, call_id, item_id)
SessionsChat @ _sessions_chat (namespace, files_uploader, files_getter, session,
  tool_callables=None, agent_tools_getter=None, hooks=None)
SessionsNamespace @ _sessions (http, base_url)
StaleCursorError @ _errors (message, status_code=None, code=None)
StreamBlock @ _blocks (*, ctx=<factory>)
StreamHooks @ _tool_handler (on_tool_call_start=None, on_tool_call_end=None,
  on_native_tool_call=None, on_tool_results_ready=None, on_reasoning_start=None,
  on_reasoning_end=None, on_compaction_start=None, on_compaction_end=None,
  on_message_start=None, on_message_end=None, on_file_output=None, on_retry=None,
  on_server_error=None, on_transport_error=None, on_sub_agent_spawned=None,
  on_sub_agent_completed=None, on_response_start=None, on_response_end=None,
  on_elicitation_request=None)
TextChunk @ _blocks (*, ctx=<factory>, text)
TextDone @ _blocks (*, ctx=<factory>, full_text, has_code_blocks=False)
ToolCallDenied @ _errors (standard Exception constructor)
ToolCallInfo @ _tool_handler (name, arguments, call_id, agent_name, response_id,
  iteration)
ToolCallable @ _sessions_chat = Callable[[SessionToolCallInfo], Awaitable[str] | str]
ToolExecution @ _blocks (*, name, arguments=<factory>, args_summary, call_id,
  agent_name, executed_by="server", output=None)
ToolGroup @ _blocks (*, ctx=<factory>, executions=<factory>, iteration=0)
ToolHandler @ _tool_handler (schemas, execute)
ToolMetadata @ tools._decorator (name, description, json_schema, strict,
  return_annotation, uses_tool_state=False)
ToolResultBlock @ _blocks (*, ctx=<factory>, name, call_id, agent_name, output,
  arguments=<factory>, args_summary="")
ToolState @ tools._state (root)
child_session_busy @ _child_status (*, busy, current_task_status,
  pending_elicitations_count=0) -> bool
child_summary_busy @ _child_status (summary) -> bool
format_tool_args_brief @ _stream (name, arguments) -> str
merge_text_across_iterations @ _transforms () -> StreamTransform
only_agent @ _transforms (agent_name) -> StreamTransform
pipe @ _transforms (stream, *transforms) -> AsyncIterator[AnyBlock]
skip_blocks @ _transforms (*types) -> StreamTransform
skip_intermediate_ends @ _transforms () -> StreamTransform
tool @ tools._decorator (fn=None, *, strict=True) -> callable or decorator
```

### Existing client and namespace access

The existing client constructs these public resource attributes today:

```text
OmnigentClient.sessions -> SessionsNamespace
OmnigentClient.files -> FilesNamespace
OmnigentClient.responses -> ResponsesNamespace
```

`responses` is retained compatibility surface over the removed server path; it
is inventoried here but is not a foundation for new code. These are the exact
current signatures. Types use their definitions from the owning modules; all
methods are async unless marked property.

```text
OmnigentClient.session(model, *, tool_handler=None, hooks=None) -> legacy Session
OmnigentClient.query(*, model, input, tools=None, tool_handler=None, files=None,
  reasoning=None, model_override=None, stream=False) -> QueryResult | QueryStream
OmnigentClient.sessions_chat(bundle, *, filename="agent.tar.gz",
  tool_callables=None, hooks=None) -> SessionsChat
OmnigentClient.close() -> None
OmnigentClient.__aenter__() -> OmnigentClient
OmnigentClient.__aexit__(*exc: object) -> None

SessionsNamespace.create(bundle, *, filename="agent.tar.gz", title=None,
  labels=None, reasoning_effort=None, workspace=None, host_type="external",
  sandbox_provider=None) -> durable Session
SessionsNamespace.create_from_agent_id(agent_id, *, title=None, labels=None,
  reasoning_effort=None, workspace=None) -> durable Session
SessionsNamespace.resolve_agent(agent_name) -> RegisteredAgent
SessionsNamespace.resolve_online_runner(*, harness=None, canonicalize=None) -> str | None
SessionsNamespace.list(*, limit=20, after=None, before=None, agent_id=None,
  agent_name=None, order="desc", sort_by="created_at", include_archived=False)
  -> list[SessionListItem]
SessionsNamespace.bind_runner(session_id, *, runner_id) -> durable Session
SessionsNamespace.unbind_runner(session_id) -> durable Session
SessionsNamespace.set_reasoning_effort(session_id, *, reasoning_effort) -> durable Session
SessionsNamespace.set_model_override(session_id, *, model_override, silent=False)
  -> durable Session
SessionsNamespace.set_archived(session_id, *, archived) -> durable Session
SessionsNamespace.set_external_session_id(session_id, *, external_session_id)
  -> durable Session
SessionsNamespace.list_items(session_id, *, limit=100, after=None, order="asc")
  -> list[dict]
SessionsNamespace.child_sessions(session_id, *, limit=100) -> list[dict]
SessionsNamespace.child_sessions_tree(session_id, *, max_depth=3, limit=100)
  -> list[dict]
SessionsNamespace.subtree_busy(session_id, *, max_depth=3, limit=100) -> bool
SessionsNamespace.get(session_id) -> durable Session
SessionsNamespace.post_event(session_id, event) -> dict
SessionsNamespace.resolve_elicitation(session_id, elicitation_id, result) -> dict
SessionsNamespace.fork(source_session_id, *, title=None, up_to_response_id=None) -> dict
SessionsNamespace.compact(session_id) -> None
SessionsNamespace.interrupt(session_id) -> None
SessionsNamespace.stream(session_id) -> AsyncIterator[ServerStreamEvent]

SessionsChat.create(namespace, bundle, *, filename="agent.tar.gz",
  files_uploader=None, files_getter=None, files_namespace=None,
  tool_callables=None, agent_tools_getter=None, hooks=None) -> SessionsChat
SessionsChat.session_id / agent_id / status -> properties
SessionsChat.refresh() -> durable Session
SessionsChat.tree_busy(*, max_depth=3) -> bool
SessionsChat.send(input, *, files=None) -> AsyncIterator[ServerStreamEvent]
SessionsChat.cancel() -> None
SessionsChat.post_event(event) -> None
SessionsChat.stream() -> AsyncIterator[ServerStreamEvent]
SessionsChat.query(input, *, files=None, stream=False) -> QueryResult | QueryStream
SessionsChat.await_turn(*, timeout=1200.0) -> QueryResult

FilesNamespace.for_session(session_id) -> SessionFilesNamespace
FilesNamespace.upload(path: str) -> File [raises RuntimeError compatibility stub]
FilesNamespace.list(*args: object, **kwargs: object) -> list[File]
  [raises RuntimeError compatibility stub]
FilesNamespace.get(file_id: str) -> File [raises RuntimeError compatibility stub]
FilesNamespace.get_content(file_id: str) -> bytes
  [raises RuntimeError compatibility stub]
FilesNamespace.download(file_id: str, to_path: str | pathlib.Path) -> pathlib.Path
  [raises RuntimeError compatibility stub]
FilesNamespace.delete(file_id: str) -> None [raises RuntimeError compatibility stub]
SessionFilesNamespace.session_id -> property
SessionFilesNamespace.upload(path: str) -> File
SessionFilesNamespace.list(*, limit=20, after=None, order="desc") -> list[File]
SessionFilesNamespace.get(file_id: str) -> File
SessionFilesNamespace.get_content(file_id: str) -> bytes
SessionFilesNamespace.download(file_id: str, to_path: str | pathlib.Path)
  -> pathlib.Path
SessionFilesNamespace.delete(file_id: str) -> None

ResponsesNamespace.create(*, model, input, background=False, instructions=None,
  previous_response_id=None, tools=None, reasoning=None, model_override=None) -> Response
ResponsesNamespace.stream(*, model, input, background=False, instructions=None,
  previous_response_id=None, tool_handler=None, hooks=None, reasoning=None,
  model_override=None) -> AsyncIterator[StreamEvent]
ResponsesNamespace.get(response_id) -> Response
ResponsesNamespace.poll(response_id, *, interval=0.5, tool_handler=None) -> Response
ResponsesNamespace.steer(response_id, input, *, model, reasoning=None,
  model_override=None) -> Response
ResponsesNamespace.cancel(response_id) -> Response
ResponsesNamespace.delete(response_id) -> None

legacy Session.model / current_response_id / is_streaming / reasoning_effort /
  model_override -> properties
legacy Session.set_reasoning_effort(effort) -> None
legacy Session.set_model_override(model) -> None
legacy Session.send(input, *, files=None, instructions=None) -> AsyncIterator[StreamEvent]
legacy Session.query(input, *, files=None, tools=None, stream=False)
  -> QueryResult | QueryStream
legacy Session.cancel() -> Response | None
legacy Session.reset() -> None
legacy Session.resume_from_response(response_id) -> None

BlockStream.stream(session, input: str | list[dict[str, object]], *,
  files: list[str] | None=None)
  -> AsyncIterator[AnyBlock]
QueryStream.__aiter__() -> AsyncIterator[str]
QueryStream.files -> list[File] property returning a shallow copy
File.from_dict(data: dict[str, object]) -> File

ToolState.get(key: str, *, default: Any=None) -> Any
ToolState.set(key: str, value: Any) -> None
ToolState.delete(key: str) -> None
ToolState.keys() -> list[str]
ToolState.__contains__(key: object) -> bool
ToolState.transaction(key: str, *, default: Any=None) -> Iterator[Any]

LocalServer.base_url -> str property
LocalServer.client -> OmnigentClient property
LocalServer.__aenter__() -> LocalServer
LocalServer.__aexit__(*exc: object) -> None
```

The `omnigent_client.tools` submodule additionally keeps this exact public
surface:

```text
TOOL_MARKER_ATTR = "_omnigent_tool_metadata"
ToolMetadata(name, description, json_schema, strict, return_annotation,
  uses_tool_state=False)
ToolState(root: pathlib.Path)
build_tool_handler(functions: list[Callable[..., Any]]) -> ToolHandler
get_tool_metadata(obj: Any) -> ToolMetadata | None
tool(fn=None, *, strict=True) -> callable or decorator
```

SessionsChat and the block/tool/query APIs remain opt-in conveniences over
existing behavior; they are not called by the new low-level resource API.

## Deprecation and unsupported inventory

Do not build new code on:

- removed server `/v1/responses` routes;
- deprecated `client.responses`, legacy root `Session`, `client.session()`, and
  `client.query()` implementations that depend on those routes;
- response-specific event dataclasses, response SSE parser, block stream, or
  query transforms as foundations for session transport; or
- removed global `/v1/files` methods. Only session-scoped files are active.

No source marker, accepted design, or open removal proposal was found for
`SessionsNamespace`, `SessionsChat`, `/v1/sessions`, its stream broker, or
session-scoped files at this baseline. This is a source snapshot, not knowledge
of private future plans. Every implementation PR must compare against current
main and repeat the deprecation search before deepening a dependency.

Unsupported until matching server contracts exist: top-level agent create,
retrieve, update, or delete; durable turn list/retrieve and turn IDs; durable
required-actions lists; event replay; webhooks; and trace export.

## Focused test-reuse matrix

| Distinct risk | Reuse first | Minimal addition |
|---|---|---|
| JSON and bundle create | `test_sessions_namespace.py` create cases | One strict full-field create fixture and composed partial-failure cases. |
| Shape separation | Same create cases | One parameterized registered-versus-bundle validation table. |
| Session parsing | Existing session snapshot tests | One complete 47-field fixture with one unknown additive field. |
| Event body and cancellation | Existing post-event/interrupt cases | One allowlist table and one heterogeneous ordered-batch case. |
| Stream parsing | Existing typed stream case | One unknown discriminator case producing `UnknownEvent`. |
| Stream-before-send | Existing `test_sessions_chat.py` readiness case | Extend it: missing heartbeat fails and proves no input POST. |
| Stream lifecycle | Existing typed stream and termination cases | One table for context close, `[DONE]`, and premature EOF/overflow distinction. |
| Reconnect | Existing durable reconnect journey | Update public imports/signatures; no automatic recovery engine. |
| Pagination | Existing server item/child/file cursor cases and SDK list cases | Add one reusable SDK page-next contract that also preserves list truthiness, iteration, `len`, indexing, and `isinstance(..., list)`; direct item-page and real session-file cursor contracts do not exist in the SDK today. |
| HTTP/error behavior | `test_http.py`, `test_errors.py` | Add only new error subclasses and composition phases. |
| Redirect safety | `test_stream_redirects.py` | Reuse for sync transport; add only a failing behavior not covered by parameterization. |
| Public examples | Existing README examples | One executable sync/async import-and-signature fixture covering locked flows. |
| Resource cleanup | Same executable fixture | Assert context close sends neither interrupt nor delete, then one explicit delete. |
| Session file paths | Existing SessionsChat file fakes | One representative MockTransport contract for the real nested file route. |
| Packaging | Existing lockstep release smoke | One wheel-pair install/public-import smoke; do not clone it per platform. |

PR 0 itself is documentation. Runtime tests land with the vertical slice whose
behavior they protect.

## Future REST extension workflow

For each newly stable REST feature:

1. Classify the route stable, preview, or internal and make OpenAPI accurate
   when it is intended for external use.
2. Add or extend only the portable canonical type family in
   `omnigent.protocol`.
3. Add the explicit method to the matching handwritten resource namespace.
   Create a module or nested namespace only for the first real route that needs
   it.
4. Share models, serializers, errors, and pagination between native sync and
   async I/O; do not share network execution through an async bridge.
5. Add the smallest MockTransport contract test for the route plus only its
   distinct streaming, error, or pagination risks.
6. Deliberately export the public name, add one executable example, update this
   ledger, and run the lockstep package build/install gate.

This workflow keeps the SDK easy to extend without pre-building unused
abstractions or generating an endpoint framework around an incomplete public
OpenAPI contract.
