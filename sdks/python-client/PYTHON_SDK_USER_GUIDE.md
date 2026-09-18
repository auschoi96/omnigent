# OmniGent Python SDK user guide

This is the practical manual for the mature `omnigent-client` SDK. It covers
the supported Python interface to OmniGent's existing REST and SSE APIs.

The SDK does not create a second runtime or protocol. Requests, responses,
items, events, and acknowledgements use the canonical public models in
`omnigent.protocol`. OpenAI Agents influenced some Python ergonomics, but
OmniGent retains its own resources, event names, and lifecycle semantics.

For exact route contracts and complete signatures, see
[`SDK_CONTRACT.md`](SDK_CONTRACT.md). For a runnable Databricks walkthrough,
see
[`examples/databricks_sessions_quickstart.py`](examples/databricks_sessions_quickstart.py).

## What changed in the mature SDK

This work adds Python ergonomics around APIs that already existed; it does not
add server capabilities. The main changes are:

- matching native sync and async clients;
- the `client.agents.sessions` resource hierarchy, with `client.sessions` as
  the same-object compatibility alias;
- canonical request, response, item, and event types from `omnigent.protocol`;
- typed pagination, streams, errors, per-request options, and raw responses;
- complete session subresources for events, items, subagents, files, the bound
  agent, and elicitations; and
- the existing async `SessionsChat` helper extended to registered agents,
  typed approvals, client tools, and conservative task-tree following.

The rest of this guide shows how to use each part.

## Requirements and installation

Python 3.12 or newer is required.

Install the published SDK:

```bash
python -m pip install --upgrade omnigent-client
```

The `omnigent`, `omnigent-client`, and `omnigent-ui-sdk` packages are released
in lockstep. Do not mix their versions.

To install the current SDK directly from the `auschoi96` fork:

```bash
python -m pip install --upgrade \
  "omnigent @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base" \
  "omnigent-client @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base#subdirectory=sdks/python-client" \
  "omnigent-ui-sdk @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base#subdirectory=sdks/ui"
```

In a Databricks notebook, use `%pip` and restart Python afterward:

```python
%pip install --upgrade \
  "omnigent @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base" \
  "omnigent-client @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base#subdirectory=sdks/python-client" \
  "omnigent-ui-sdk @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base#subdirectory=sdks/ui" \
  "databricks-sdk>=0.56,<1"
```

```python
from databricks.sdk.runtime import dbutils

dbutils.library.restartPython()
```

## Choose the sync or async client

The two clients share the same mature `agents` and `sessions` resource layout,
parameters, protocol models, pagination, and error behavior. The async client
also retains older compatibility APIs described near the end of this guide.

| Client | Use it when | I/O style |
|---|---|---|
| `Omnigent` | Scripts, synchronous services, and simple blocking code | Native synchronous HTTPX |
| `AsyncOmnigent` | Async applications and `SessionsChat` | Native asynchronous HTTPX |
| `OmnigentClient` | Compatibility spelling for `AsyncOmnigent` | Native asynchronous HTTPX |

Synchronous client:

```python
import os

from omnigent_client import Omnigent

with Omnigent(base_url=os.environ["OMNIGENT_BASE_URL"]) as client:
    agents = client.agents.list()
```

Asynchronous client:

```python
import asyncio
import os

from omnigent_client import AsyncOmnigent


async def main() -> None:
    async with AsyncOmnigent(base_url=os.environ["OMNIGENT_BASE_URL"]) as client:
        agents = await client.agents.list()


asyncio.run(main())
```

Databricks Python notebooks support top-level `await`, so they do not require
`asyncio.run()`:

```python
from omnigent_client import AsyncOmnigent

client = AsyncOmnigent(base_url="https://omnigent.example.com")
agents = await client.agents.list()
await client.close()
```

## Connect to an OmniGent server

Pass the OmniGent REST base URL explicitly. The SDK works with an OmniGent
server hosted anywhere, provided the client can reach it and has valid
authentication.

### Static headers

```python
from omnigent_client import Omnigent

client = Omnigent(
    base_url="https://omnigent.example.com",
    headers={"Authorization": "Bearer <token>"},
    timeout=300.0,
)
```

The client also accepts `httpx.Auth`, cookies, or a caller-configured HTTPX
client. When injecting an HTTPX client, configure its authentication, headers,
timeouts, and retries on that client. The caller retains ownership of it.

### Databricks-managed OmniGent server

Inside a Databricks notebook, the current workspace can provide the server URL
and authentication:

```python
from collections.abc import Generator
from urllib.parse import urlsplit

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from omnigent_client import AsyncOmnigent


class DatabricksAuth(httpx.Auth):
    """Refresh Databricks credentials for every OmniGent request."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers.update(self.config.authenticate())
        yield request


workspace = WorkspaceClient()
workspace_url = urlsplit(workspace.config.host)
if workspace_url.scheme != "https" or not workspace_url.netloc:
    raise RuntimeError("The current Databricks workspace has no secure URL.")

base_url = f"{workspace_url.scheme}://{workspace_url.netloc}/api/2.0/omnigent"
client = AsyncOmnigent(
    base_url=base_url,
    auth=DatabricksAuth(workspace.config),
    timeout=300.0,
)
```

This connects directly to the managed OmniGent REST endpoint. It does not look
up a Databricks App. If you use `workspace.apps.get(...)` for another purpose,
its `name` argument must be an app resource name, not a URL.

Outside a notebook, select any Databricks profile explicitly:

```python
workspace = WorkspaceClient(profile="<chosen-profile>")
```

The SDK never chooses a Databricks CLI profile for you.

## Resource model

The primary resource tree is:

```text
client
└── agents
    ├── list(...)
    └── sessions
        ├── create / retrieve / list / update / delete / fork / compact
        ├── events.create / events.cancel / events.stream
        ├── items.list
        ├── subagents.list
        ├── files.list / upload / retrieve / content / download / delete / copy
        ├── agent.retrieve / agent.contents / agent.update
        └── elicitations.retrieve / elicitations.resolve
```

`client.sessions` is a compatibility alias for
`client.agents.sessions`. They are the same object and implementation:

```python
assert client.sessions is client.agents.sessions
```

There is no top-level agent create, update, or delete API. Agent operations in
this SDK are the existing read-only catalog and session-bound agent resources.

## List registered agents

```python
agents = client.agents.list(limit=20, order="asc")
for agent in agents:
    print(agent.id, agent.name, agent.harness)
```

Async:

```python
agents = await client.agents.list(limit=20, order="asc")
```

The return value is a typed cursor page, not a generator. The current page is
already loaded and behaves like a normal list.

## Create a session

Sessions can use an agent already registered on the server or an uploaded agent
bundle.

### Registered agent

```python
session = client.agents.sessions.create(
    agent_id="ag_123",
    title="Repository review",
    host_type="managed",
)

print(session.id, session.status)
```

`host_type="managed"` asks the OmniGent server to provision its configured
managed sandbox. The SDK caller does not create or attach to the sandbox
directly. If the server exposes multiple providers and requires an explicit
selection, pass a configured provider such as `sandbox_provider="lakebox"`.

`host_type="external"` uses the server's existing external-host workflow.

### Uploaded agent bundle

```python
from pathlib import Path

bundle = Path("agent.tar.gz").read_bytes()
session = client.agents.sessions.create(
    bundle,
    filename="agent.tar.gz",
)
```

Bundle and registered-agent creation are distinct request forms. Do not pass
registered-only fields to bundle creation.

### Create and submit input

```python
session = client.agents.sessions.create(
    agent_id="ag_123",
    input="Summarize this repository.",
    host_type="managed",
)
```

This creates the session, submits the input through the existing events route,
and returns a current snapshot. It does not wait for the agent turn to finish.

`initial_items` seeds session history; it does not start agent work. Use
`input` when the agent should act.

## Create and stream the first turn

The easiest synchronous streaming flow is:

```python
with client.agents.sessions.create(
    agent_id="ag_123",
    input="Summarize this repository.",
    stream=True,
    host_type="managed",
    timeout=300.0,
) as events:
    session_id = events.session_id
    for event in events:
        print(event.type)

outcome = events.terminal_event
if outcome is None:
    raise RuntimeError("The stream closed without a terminal event.")
if outcome.type != "response.completed":
    raise RuntimeError(outcome.to_json(indent=None))
```

Async:

```python
events = await client.agents.sessions.create(
    agent_id="ag_123",
    input="Summarize this repository.",
    stream=True,
    host_type="managed",
    timeout=300.0,
)

async with events:
    session_id = events.session_id
    async for event in events:
        print(event.type)

outcome = events.terminal_event
```

The composition opens the stream and consumes its readiness heartbeat before
submitting input. That prevents early output from being lost.

The stream yields canonical `ServerStreamEvent` models from
`omnigent.protocol`. A newer unknown event discriminator is preserved as an
output-only `UnknownEvent`; it is never valid input.

`response.completed` is a successful response boundary. `response.failed`,
`response.cancelled`, and `response.incomplete` are terminal outcomes but are
not success.

## Send follow-up input

A stream is a live tail, so open it before submitting a follow-up:

```python
from omnigent_client import SessionMessage

with client.agents.sessions.events.stream(session_id) as events:
    acknowledgement = client.agents.sessions.events.create(
        session_id,
        events=SessionMessage.text("Now include file sizes."),
    )
    for event in events:
        print(event.type)
```

Async:

```python
async with client.agents.sessions.events.stream(session_id) as events:
    acknowledgement = await client.agents.sessions.events.create(
        session_id,
        events=SessionMessage.text("Now include file sizes."),
    )
    async for event in events:
        print(event.type)
```

The public event-input allowlist is exactly:

| Type | Purpose |
|---|---|
| `SessionMessage` | Submit user content |
| `FunctionCallOutput` | Return a client-executed tool result |
| `Interrupt` | Interrupt current work |

One call can submit one event or an ordered batch of 1–100 events. Batches are
not atomic: earlier events can take effect before a later event fails.

Raw mappings are accepted only if they validate through the same strict public
union. Internal controls and `external_*` events are not public SDK input.

## Typed messages and content blocks

Build a text message:

```python
from omnigent_client import SessionMessage

message = SessionMessage.text("Explain the current changes.")
```

Build a message from supported content blocks:

```python
message = SessionMessage.from_content(
    [
        {"type": "input_text", "text": "Review this file."},
        {"type": "input_file", "file_id": "file_123", "filename": "report.csv"},
    ]
)
```

Typed request envelopes reject unknown top-level fields before network I/O.
Content blocks remain mappings because the server owns their supported shapes.
Response models retain additive fields so an older client can preserve data
from a newer server.

## Retrieve, list, and update sessions

Retrieve a full session snapshot:

```python
session = client.agents.sessions.retrieve(
    session_id,
    include_items=True,
    include_liveness=True,
    refresh_state=False,
)
```

List sessions:

```python
sessions = client.agents.sessions.list(
    agent_id="ag_123",
    order="desc",
    sort_by="updated_at",
    visibility="all",
    include_archived=False,
)
```

Useful list filters include `agent_id`, `agent_name`, `search_query`, `kind`,
`project`, `pinned`, `visibility`, and `include_archived`.

Update a session:

```python
session = client.agents.sessions.update(
    session_id,
    title="Final repository review",
    labels={"team": "sdk"},
    reasoning_effort="high",
)
```

Supported update fields include runner binding, title, labels, reasoning and
model overrides, collaboration/permission/approval modes, cost and subagent
routing overrides, workspace sharing, external session ID, terminal launch
arguments, archive state, and project ID.

For presence-sensitive fields, omission and explicit `None` can mean different
things. The public `NOT_GIVEN` sentinel omits a field; `None` sends JSON `null`:

```python
from omnigent_client import NOT_GIVEN

session = client.agents.sessions.update(
    session_id,
    model_override=NOT_GIVEN,  # preserve inheritance
)
```

## Cursor pagination

List operations return `SyncCursorPage[T]` or `AsyncCursorPage[T]`. Each page:

- is a `list[T]` containing only the loaded page;
- exposes `data`, `first_id`, `last_id`, and `has_more`;
- never fetches another page during iteration or indexing;
- fetches another page only through `get_next_page()`.

Sync:

```python
page = client.agents.sessions.items.list(session_id, limit=100)
all_items = list(page)

while page.has_next_page():
    page = page.get_next_page()
    all_items.extend(page)
```

Async:

```python
page = await client.agents.sessions.items.list(session_id, limit=100)
all_items = list(page)

while page.has_next_page():
    page = await page.get_next_page()
    all_items.extend(page)
```

If a cursor references an item that was deleted, the SDK raises
`StaleCursorError`. Restart the walk from the first page without that cursor.

## Durable session items

```python
items = client.agents.sessions.items.list(
    session_id,
    limit=100,
    order="asc",
)

for item in items:
    print(item.type, item.to_json(indent=2))
```

`SessionItem` is the canonical discriminated union for durable flat items. It
includes message, function call, function-call output, error, reasoning,
compaction, native-tool, resource-event, routing-decision, slash-command, and
terminal-command items.

SSE is not durable history and does not support replay. After disconnecting,
retrieve the session snapshot and item pages, open a replacement stream, and
deduplicate by item ID and response ID. Do not automatically resubmit input.

## Subagents

List direct child sessions:

```python
children = client.agents.sessions.subagents.list(
    session_id,
    limit=20,
    order="desc",
)

for child in children:
    print(child.id, child.tool, child.current_task_status, child.busy)
```

Children are ordinary sessions and can be retrieved through the same session
resource. `child_summary_busy(...)` applies the canonical busy predicate used
by the SDK's task follower:

```python
from omnigent_client import child_summary_busy

busy = any(
    child_summary_busy(child.model_dump(mode="python"))
    for child in children
)
```

For a high-level recursive follower, use async `SessionsChat.run()` rather
than writing a polling loop in application code.

## Session files

Files are always session-scoped. There are no supported global `/v1/files`
operations.

```python
files = client.agents.sessions.files

uploaded = files.upload(session_id, "report.csv")
metadata = files.retrieve(session_id, uploaded.id)
content = files.content(session_id, uploaded.id)
local_path = files.download(session_id, uploaded.id, "downloads/report.csv")
deleted = files.delete(session_id, uploaded.id)
```

Copy files from a strict ancestor session into a descendant:

```python
copied = files.copy(
    child_session_id,
    source_session_id=parent_session_id,
    file_ids=["file_123"],
)
```

The file IDs must be non-empty and unique. The source must be a strict ancestor
of the destination. A deployment without a session file store returns HTTP
501, surfaced as `OmnigentError`.

Async file operations have the same names and parameters and must be awaited.

## Session-bound agent

Retrieve the agent snapshot bound to a session:

```python
agent = client.agents.sessions.agent.retrieve(session_id)
bundle_bytes = client.agents.sessions.agent.contents(session_id)
```

Update a session-scoped agent bundle:

```python
agent = client.agents.sessions.agent.update(
    session_id,
    bundle_bytes,
    filename="agent.tar.gz",
)
```

These operations do not create a top-level agent CRUD surface.

## Elicitations and approvals

Elicitation APIs are preview. Resolve only real server-published elicitation
IDs; ordinary assistant text is not an approval request.

Inspect and resolve directly:

```python
state = client.agents.sessions.elicitations.retrieve(
    session_id,
    elicitation_id,
)

acknowledgement = client.agents.sessions.elicitations.resolve(
    session_id,
    elicitation_id,
    action="accept",  # "accept", "decline", or "cancel"
    content=None,
)
```

There is no implicit approval. If a callback is absent or fails inside
`SessionsChat`, it declines fail-closed.

## Cancel, compact, fork, and delete

Interrupt current work without deleting the session:

```python
client.agents.sessions.events.cancel(session_id)
```

Request the existing compaction control:

```python
client.agents.sessions.compact(session_id)
```

Fork a session:

```python
forked = client.agents.sessions.fork(
    session_id,
    title="Alternative approach",
    up_to_response_id="resp_123",
    host_type="managed",
)
```

Forking supports the existing truncation, run-configuration, managed-host,
side-chat, and referenced-file carry-forward behavior.

Delete explicitly:

```python
deleted = client.agents.sessions.delete(session_id, delete_branch=False)
```

Closing a stream or client never cancels or deletes remote state.

## High-level async `SessionsChat`

`SessionsChat` is an async convenience over the same existing resources. It
adds no server route or durable task object.

### Registered-agent task

```python
from omnigent_client import AsyncOmnigent

async with AsyncOmnigent(
    base_url="https://omnigent.example.com",
    headers={"Authorization": "Bearer <token>"},
    timeout=300.0,
) as client:
    chat = await client.sessions_chat(
        agent_id="ag_123",
        host_type="managed",
    )
    session = await chat.run("Create tree.py, run it, and show its output.")
    print(chat.session_id, session.status)
```

### Uploaded-bundle chat

```python
chat = await client.sessions_chat(
    bundle_bytes,
    filename="agent.tar.gz",
)
```

### Display typed activity and request approval

```python
from omnigent.protocol import ServerStreamEvent, SessionItem, UnknownEvent
from omnigent_client import ElicitationRequestCtx, StreamHooks


def show_event(event: ServerStreamEvent | UnknownEvent) -> None:
    print(f"\n[live · {event.type}]")
    print(event.to_json(indent=2))


def show_item(session_id: str, item: SessionItem) -> None:
    print(f"\n[{session_id} · {item.type}]")
    print(item.to_json(indent=2))


def approve_or_decline(ctx: ElicitationRequestCtx) -> bool:
    print(f"Approval requested: {ctx.message}")
    return input("Approve? [y/N]: ").strip().lower() in {"y", "yes"}


chat = await client.sessions_chat(
    agent_id="ag_123",
    host_type="managed",
    hooks=StreamHooks(on_elicitation_request=approve_or_decline),
)

session = await chat.run(
    "Before changing files, request approval. Then create tree.py and run it.",
    on_event=show_event,
    on_item=show_item,
)
```

This exposes reasoning and tool activity that the server publishes. It cannot
expose a model provider's private chain of thought.

`run()` performs this transparent composition:

1. Opens the root session stream through its readiness heartbeat.
2. Submits the user input.
3. Delivers public live events to `on_event`.
4. Resolves typed elicitations through `StreamHooks`.
5. Follows typed durable items and known child sessions.
6. Returns after the known tree remains quiet for 60 seconds by default.

The default overall stream-and-follow deadline is 1,200 seconds. Configure it
when necessary:

```python
session = await chat.run(
    "Complete the repository task.",
    timeout=1800.0,
    quiet_period=90.0,
    poll_interval=1.0,
    max_depth=3,
)
```

The quiet window is a conservative client composition, not an atomic server
task-completion guarantee. A response terminal event ends one response; child
work or a later parent wake can still follow.

Repeated `run()` calls reuse the same durable session and do not redisplay
items already observed by that `SessionsChat` instance:

```python
await chat.run("Create the first version.", on_item=show_item)
await chat.run("Now improve it.", on_item=show_item)
```

### Other `SessionsChat` methods

| Method | Purpose |
|---|---|
| `send(input, *, files=None)` | Stream one submitted response |
| `query(input, *, files=None, stream=False)` | Collect or stream assistant text and produced files |
| `await_turn(*, timeout=1200.0)` | Collect the next auto-triggered response without posting input |
| `run(input, *, ...)` | Submit input and follow the known task tree |
| `wait_until_quiet(*, ...)` | Follow an already-running known task tree |
| `stream()` | Subscribe without submitting input |
| `post_event(event)` | Submit a validated public event |
| `refresh()` | Refresh the cached root session snapshot |
| `tree_busy(*, max_depth=3)` | Read the point-in-time descendant busy rollup |
| `cancel()` | Submit the existing interrupt event |

### Client-executed tools

Client tools must already be declared by the bound agent spec with
`runtime: "client"`. The callable map must match those names exactly and every
callable must return a string.

```python
from omnigent_client import SessionToolCallInfo


def open_in_editor(call: SessionToolCallInfo) -> str:
    return f"Opened {call.arguments['path']}"


chat = await client.sessions_chat(
    agent_id="ag_123",
    tool_callables={"open_in_editor": open_in_editor},
)
```

The SDK validates the mapping when streaming starts. Missing or extra names
raise before a tool call is silently ignored.

## Stream hooks

Session-native `SessionsChat` uses `StreamHooks` for response, reasoning,
message, tool, file, subagent, and elicitation lifecycles. Common callbacks
include:

- `on_response_start` and `on_response_end`;
- `on_reasoning_start` and `on_reasoning_end`;
- `on_message_start` and `on_message_end`;
- `on_tool_call_start` and `on_tool_call_end`;
- `on_file_output`;
- `on_sub_agent_spawned` and `on_sub_agent_completed`;
- `on_elicitation_request`.

Hooks accept synchronous or asynchronous callables. They are presentation and
interaction callbacks over existing events, not another protocol layer.

## Errors, retries, and timeouts

Import SDK errors from `omnigent_client`:

```python
from omnigent_client import (
    OmnigentError,
    RateLimitedError,
    SessionCompositionError,
    StaleCursorError,
    StreamProtocolError,
)
```

| Error | Meaning |
|---|---|
| `OmnigentError` | Base for translated HTTP status failures |
| `RateLimitedError` | Server rate limit response |
| `SessionCompositionError` | A multi-request create failed after a session may already exist |
| `StaleCursorError` | A cursor no longer identifies a valid pagination position |
| `StreamProtocolError` | A live stream reached EOF before its normal `[DONE]` marker |

`SessionCompositionError` includes `session_id`, `phase`, and the original
exception. Inspect or clean up that session instead of blindly resubmitting the
input:

```python
from omnigent_client import SessionCompositionError

try:
    events = client.agents.sessions.create(
        agent_id="ag_123",
        input="Run the task.",
        stream=True,
        timeout=300.0,
    )
except SessionCompositionError as exc:
    print(exc.session_id, exc.phase, exc.original_exception)
```

HTTPX network exceptions remain HTTPX exceptions. SDK-owned transports retry
safe GET/HEAD connection failures according to `max_retries`; they do not
automatically replay unsafe writes. Caller-injected HTTPX clients own their
retry policy.

Timeouts have distinct scopes:

| Setting | Scope |
|---|---|
| Client `timeout=` | Default ordinary HTTP request timeout |
| Per-method `timeout=` | That REST or stream request |
| `SessionsChat.run(timeout=...)` | Entire initial stream plus task-tree following |
| `quiet_period=` | Sustained inactivity required before `run()` returns |

Use a longer client or per-request timeout, such as 300 seconds, when managed
sandbox cold starts are expected.

The SDK follows redirects on the configured origin and permits a same-host
HTTP-to-HTTPS upgrade. It refuses other cross-origin redirects so credentials
and request bodies are not sent to another host.

## Raw response metadata

Session retrieval can expose status and headers without a second request:

```python
raw = client.agents.sessions.with_raw_response.retrieve(session_id)
print(raw.status_code, raw.request_id, raw.headers)
session = raw.parse()
```

Async:

```python
raw = await client.agents.sessions.with_raw_response.retrieve(session_id)
session = raw.parse()
```

## Per-request options

Supported resource methods accept the explicit options appropriate for that
route:

```python
session = client.agents.sessions.retrieve(
    session_id,
    timeout=60.0,
    extra_headers={"X-Request-Source": "my-service"},
    extra_query={"debug": False},
)
```

The mature surface does not accept arbitrary `extra_body` fields. Add new REST
features through explicit typed parameters instead of sending undocumented
JSON.

## Cleanup and ownership

Prefer client context managers:

```python
with Omnigent(base_url=base_url) as client:
    ...
```

```python
async with AsyncOmnigent(base_url=base_url) as client:
    ...
```

Context exit closes SDK-owned local HTTP resources. Injected HTTPX clients
remain caller-owned. No context manager implicitly interrupts, deletes, or
otherwise mutates a remote session.

## Compatibility APIs to avoid in new code

The following remain only for compatibility or depend on the removed Responses
API and should not be foundations for new work:

- `/v1/responses`;
- `client.responses`;
- `client.session()`;
- top-level `client.query()`;
- the root `Session` helper;
- response-specific stream/block machinery as session transport;
- global file methods.

Use `client.agents.sessions`, session-scoped files, canonical
`omnigent.protocol` types, and async `SessionsChat` instead. `SessionsChat.query`
is session-native and is not the deprecated top-level `client.query()`.

## Current boundaries

The SDK intentionally does not invent resources that the server does not have.
There is currently no public SDK promise for:

- top-level agent CRUD;
- durable turn resources or turn IDs;
- event history replay or `Last-Event-ID`;
- durable required-action lists;
- webhooks;
- trace export;
- client-side durable state.

Typed event submission, elicitation resolution, and compaction are preview.
Stable and preview classifications are recorded in
[`SDK_CONTRACT.md`](SDK_CONTRACT.md).

## Adding future REST features

Add each new server capability as a small typed sync/async resource change; do
not pre-create unused resources or duplicate `omnigent.protocol`. Maintainers
should follow [`SDK_CONTRACT.md`](SDK_CONTRACT.md) and [`AGENTS.md`](AGENTS.md)
for the route, compatibility, and test checklist.

## Quick troubleshooting

| Symptom | Check |
|---|---|
| `GET /apps/https:/...` returns `NotFound` | An app URL was passed where a Databricks app name was expected. Connect directly to the OmniGent REST base URL instead. |
| Input submit times out during managed creation | Increase the client or per-request timeout to cover the sandbox cold start. Inspect `SessionCompositionError.session_id` before retrying. |
| A notebook cell ends after `response.completed` while children continue | Use `await chat.run(...)`; one response completion is not whole-tree completion. |
| No approval prompt appears | Confirm the server emitted a typed elicitation. Prompt text alone is not an approval event. |
| The task follower waits after work looks complete | Its default 60-second quiet window protects against delayed child completion and parent wake-ups. |
| File methods return 501 | The target OmniGent deployment has no configured session file store. |
| Pagination stops after one page | Call `get_next_page()` while `has_next_page()` is true. |
| A follow-up misses early output | Open the session stream before submitting the follow-up event. |
