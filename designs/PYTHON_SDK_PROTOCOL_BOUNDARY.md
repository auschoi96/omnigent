# Python SDK protocol boundary

Status: accepted for the Python SDK implementation sequence
Decision baseline: `b3308f9f852617e6dd5be1c823f440785c40f26a`
Date: 2026-09-17

## Decision

Portable REST request, response, item, event, and acknowledgement types are
owned by:

```text
omnigent/protocol/
    __init__.py
    sessions.py
```

The existing `omnigent-client` package imports those types. Server code imports
them from the same location, while `omnigent.server.schemas` compatibility-
reexports moved names so existing imports do not break in the migration PR.

`omnigent.protocol` may depend only on the Python standard library and Pydantic.
It may not import FastAPI, HTTPX, stores, runtime services, routes, runner code,
or SDK transport. Protocol models contain validation and serialization rules,
not network calls or business orchestration.

The first boundary is intentionally only `sessions.py`. New files such as
`agents.py` or `resources.py` appear only when real public routes make a second
cohesive family useful. We will not create one file per endpoint or an empty
future namespace.

## PR 1 migration manifest

PR 1 is a mechanical ownership move plus missing wire-output models. It does
not move route services or change JSON.

### Move from `omnigent.entities.conversation`

Move the item value layer: `MessageData`, `FunctionCallData`,
`FunctionCallOutputData`, `ErrorData`, `ReasoningData`, `CompactionData`,
`NativeToolData`, `ResourceEventData`, `RoutingDecisionData`,
`SlashCommandData`, `TerminalCommandData`, `ItemData`,
`ITEM_TYPE_TO_DATA_CLS`, `NewConversationItem`, `ConversationItem`, and their
type/data validation and API-flattening helpers. The current pure binary-
redaction dependency used by `CompactionData` moves with its private stdlib
helper closure so the protocol module does not import the LLM adapter package;
`omnigent.llms.adapters._content` keeps a compatibility import for existing
callers.

`Conversation`, title synthesis, non-content filtering policy, and store/domain
logic remain under `omnigent.entities`. `omnigent.entities.conversation` and
`omnigent.entities` re-export the moved item names so current imports retain
identity.

The existing API really emits two item representations. `ConversationItem`
continues to model nested items in `SessionResponse.items`. PR 1 adds one
canonical typed flat `SessionItem` union for the output of
`GET /v1/sessions/{id}/items`, based on `ConversationItem.to_api_dict()`.
These models describe different wire shapes; the SDK does not normalize one
into the other.

### Move or base from `omnigent.server.schemas`

Move the portable response/page layer: `PaginatedList`,
`ConversationDeleted`, `CreatedSessionResponse`, `SandboxStatus`, `ModelUsage`,
`BackgroundTaskInfo`, `SessionResponse`, `UpdateSessionRequest`,
`SessionListItem`, `SessionList`, `ChildSessionSummary`, and
`ChildSessionList`.

Move each portable dependency required by those types rather than leaving an
import back to `omnigent.server.schemas`. In particular,
`SandboxLaunchStage` moves with `SandboxStatus`.

Move the portable request field definitions for `SessionEventInput`,
`ElicitationResult`, `SessionGitOptions`, registered session create, bundle
session create, and `SessionForkRequest`. Pure field and cross-field validation
moves with them. Repository/host/provider validation that currently imports
`omnigent.server.managed_hosts` remains on thin server subclasses; those
subclasses inherit the portable field definitions rather than repeat them.
Server-only attribution and control-event variants remain private.

At this baseline `SessionEventInput` also imports the reserved framework-notice
validator from `omnigent.inner.native_attachments`. Move only
`FRAMEWORK_NOTICE_BLOCK_TYPE` and the pure recursive
`reject_authored_framework_notices` helper into the protocol module (with a
compatibility import from `native_attachments`); do not import the inner
attachment/runtime module from `omnigent.protocol`. Preserve the new
`SessionForkRequest.side_chat` field along with the rest of that request.

Move `_SSEEventBase`, the 55 classes in the `ServerStreamEvent` union, the
union, and its discriminator set. Also move the Pydantic support values directly
used by those models: `NativeReasoningEffortOption`, `NativeModelOption`,
`McpServerStartup`, `SessionInputConsumedPayload`,
`SessionInterruptedPayload`, `PresenceViewer`, `ElicitationRequestParams`, and
`RetryErrorDetail`. The session stream's response lifecycle events also require
`ErrorDetail`, `ResponseObject`, `FailedResponseObject`, `Usage`,
`UsageDetails`, `ConversationRef`, and `IncompleteDetails`; those portable
value models move with the events. This does not revive the removed Responses
route or make its runtime a dependency--the current session SSE payloads
already contain these values. `InjectionConsumedEvent` and the
policy/subagent runner-stream models outside `ServerStreamEvent` remain
server-owned until a public route requires them.

Move the portable models used by the selected session subresources:
`MCPServerSummary`, `PolicySummary`, `SkillSummary`, and `AgentObject` for the
session-bound agent routes; and `CopyFilesRequest`, `CopiedFile`,
`CopyFilesResponse`, and `SessionResourceObject` for session files. These are
wire values, not agent loading or file-store services.

Add the output-only `UnknownEvent`, event acknowledgement models, typed flat
`SessionItem` variants, a typed session-resource deletion acknowledgement, and
a typed elicitation-state response because those wire outputs currently have
only `dict` annotations. These additions model bytes already emitted by the
server; they do not add routes or behavior.

### Compatibility imports and ownership checks

`omnigent.server.schemas` imports and re-exports every moved server-schema name.
Existing routes keep their current annotations or inherit a portable field
base where server-only validation is required. The SDK imports only
`omnigent.protocol`; it does not import the compatibility facade.

PR 1 must prove object identity for compatibility re-exports, identical model
dump/validation behavior for existing fixtures, unchanged OpenAPI/wire output
except for deliberately published schemas, and the absence of FastAPI, HTTPX,
store, runtime, route, runner, or LLM-adapter imports below
`omnigent.protocol`.

## Why the canonical types remain in `omnigent`

The server already owns the wire contract and the SDK is developed and released
in the same repository. Importing one canonical portable model set avoids the
drift of copying server schemas into an SDK-only tree. It also lets route,
protocol, and SDK changes land as one vertical slice.

This is not a requirement that the SDK be operationally independent of the
repository. Standalone packaging is a separate distribution concern; type
ownership should not be distorted to solve it prematurely.

## Current package evidence

At the earlier `94907703f` installation-measurement snapshot:

- `omnigent-client` directly depends on `omnigent`, `httpx`, and `pydantic`;
- the root `omnigent` distribution directly depends on `omnigent-client` and
  `omnigent-ui-sdk`, while the UI SDK also depends on `omnigent-client`;
- current direct SDK imports reach `omnigent.server.schemas`,
  `omnigent.runner.identity`, and server schemas from `SessionsChat`;
- tracked client source is 372,273 bytes, versus 20,051,663 bytes for the root
  package and 270,523 bytes for the UI SDK; and
- a fresh `omnigent-client==0.15.0.dev0` installation resolved 93
  distributions and occupied 419,448,349 logical bytes in `site-packages`.

The installation measurement used Python 3.13.0 on arm64 macOS 26.6.2 on
2026-09-17. No Python package manifest changed between that measurement and
this decision baseline. The locally built wheels were 110,960 bytes for
`omnigent-client`, 13,158,330 bytes for `omnigent`, and 84,492 bytes for
`omnigent-ui-sdk`. The
three project wheels came from the pinned checkout; build isolation and third-
party installation used the configured package proxies, so these numbers are a
dated platform/index snapshot rather than a locked offline benchmark.

This proves an existing distribution cycle and a meaningful import boundary
problem. It does not prove that copying types or splitting a new distribution
is the right first fix.

## Standalone packaging is deferred

The first implementation phase keeps the lockstep `omnigent` /
`omnigent-client` release model and removes SDK imports from private server and
runner modules by moving only portable contract types/constants to
`omnigent.protocol`.

A standalone wheel decision requires more than the clean-environment wheel and
dependency-closure measurement above: it also needs import-time data, a target
installation budget, release ownership, and compatibility analysis. The
roughly 400 MiB result establishes cost, but it does not by itself choose a
package boundary. We therefore do not create a third protocol distribution
speculatively.

If later measurements justify a package split, `omnigent.protocol` is already
an import-safe seam that can be packaged deliberately without changing its
public names.

## Model evolution rules

Request models are strict: unknown fields and unsupported discriminators fail
before network I/O. This prevents the SDK from accepting an option the server
will ignore or interpreting internal runner controls as public API.

The server event-ingestion route is deliberately broader than the public SDK.
The protocol module may own one shared envelope plus the narrow public
`SessionMessage`, `FunctionCallOutput`, and `Interrupt` variants. Server-only
control and `external_*` validation stays private to the server and may compose
that envelope; it is not re-exported by the SDK. These are different trust
boundaries, not two copies of the same public model tree.

Response models retain unknown additive fields. Known stream events use the
canonical discriminated union. One output-only `UnknownEvent` retains the raw
unknown discriminator and payload so a newer server does not become silent
data loss for an older SDK. `UnknownEvent` is never a valid request event and
does not duplicate the known event hierarchy.

Moving models must preserve wire aliases, validation, serialization,
discriminators, and existing `omnigent.server.schemas` import behavior. A move
is incomplete until compatibility imports and route serialization tests pass.

## Client architecture consequences

- `Omnigent` and `AsyncOmnigent` use native sync and async HTTPX clients over
  shared paths, serializers, protocol models, errors, pagination metadata, and
  redirect policy.
- `client.agents.sessions` and compatibility `client.sessions` point to the
  same resource implementation.
- Bound resource views remove repeated parent IDs only; they do not own remote
  state or invent lifecycle behavior.
- The low-level resource API reuses side-effect-free transport behavior from
  `SessionsNamespace` and `SessionsChat` where needed, but never calls the chat
  helper to perform low-level operations. Client-tool execution, elicitation
  callbacks, and hooks remain opt-in convenience behavior.
- Context managers close local connections. Remote cancellation and deletion
  remain explicit REST actions.

The detailed route, model, event, compatibility, and test contracts live in
`sdks/python-client/SDK_CONTRACT.md`.

## Compatibility and deprecation boundary

The migration preserves existing public imports and entry points. In
particular, the root `Session` name continues to mean the legacy Responses
helper until a separately planned breaking release; the durable sessions model
is not silently substituted.

New architecture must not depend on the removed `/v1/responses` server path,
deprecated `ResponsesNamespace`, legacy response `Session`, response-specific
SSE/block stack, or removed global `/v1/files` methods. The supported
foundation is the `/v1/sessions` route family, its stream, session-scoped files,
and existing `SessionsNamespace`/`SessionsChat` behavior that maps to those
routes.

Before every implementation PR, compare the branch with current main and
repeat the repository deprecation/removal search. If a foundation is newly
deprecated, stop and update this decision rather than wrapping it more deeply.

## Future vertical slice

When the REST API adds a feature, maintainers extend the SDK in one small
vertical slice:

1. publish or classify the route contract;
2. add portable canonical protocol types only when the route needs them;
3. add the explicit method to the existing handwritten resource namespace;
4. provide native sync and async I/O with identical semantics;
5. add one focused route contract test and only distinct failure-mode tests;
6. export deliberately, add one executable example, update the contract
   ledger, and run the lockstep build/install gate.

Code generation, automatic replay/reconciliation, new orchestration helpers,
and pre-created resource families are not part of this decision. They require
their own demonstrated maintenance or product need.

## Rejected alternatives

### Copy server schemas into `omnigent_client`

Rejected because it creates two known-model trees that can drift and turns each
server field addition into manual synchronization work.

### Make `omnigent_client` completely standalone first

Rejected for the initial phase even though the clean wheel install measured a
93-distribution, 419,448,349-byte environment. The measurement establishes
cost, but there is no agreed installation budget, import-time result, release
owner, compatibility plan, or standalone product requirement. The protocol
seam addresses private-import coupling without forcing a premature package
split.

### Keep importing `omnigent.server.schemas`

Rejected because that 5,000-line server module imports runtime/domain code and
keeps SDK transport coupled to a private server implementation location.

### Generate the SDK from current OpenAPI

Rejected for now because important event/elicitation operations are hidden and
several useful semantics are compositions rather than generated single-route
calls. The repository's established extension style is explicit handwritten
methods. Revisit generation only when the public schema is accurate enough to
reduce maintenance rather than conceal it.
