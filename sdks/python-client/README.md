# omnigent-client

Typed sync and async Python clients for the existing
[OmniGent](https://github.com/omnigent-ai/omnigent) HTTP and SSE APIs.
Session requests and streamed events use the canonical public models in
`omnigent.protocol`.

The resource layout and context-managed streaming take directional inspiration
from the [OpenAI Agents API quickstart](https://developers.openai.com/api/docs/guides/agents-api/quickstart).
OmniGent keeps its own routes, event names, capabilities, and lifecycle
semantics; this is not an API-parity or wire-compatibility claim.

## Installation and compatibility

Python 3.12 or newer is required.

```bash
pip install omnigent-client
```

The repository publishes its Python packages in lockstep. Use the same release
number for every package in one environment:

| `omnigent` | `omnigent-client` | `omnigent-ui-sdk` | Supported |
|---|---|---|---|
| `X` | `X` | `X` | Yes |
| Mixed versions | Mixed versions | Mixed versions | No |

Exact dependency pins normally enforce this rule. It is a package compatibility
contract, not a promise that any SDK version works with every remote server
version. The verified server pairing is the server code shipped in the same
release; no broader cross-version server range is currently published.

Pass the server URL explicitly. The clients also accept caller-supplied HTTPX
authentication, headers, or an HTTP client; they do not discover credentials or
select CLI profiles.

For a Databricks notebook walkthrough—from installing the matching fork wheels
through a streamed turn, follow-up input, durable history, and explicit
cleanup—import
[`examples/databricks_sessions_quickstart.py`](examples/databricks_sessions_quickstart.py)
as a Databricks source notebook.

## Synchronous quickstart

<!-- sdk-example: executable -->

```python
import os

from omnigent_client import Omnigent

server_url = os.environ["OMNIGENT_BASE_URL"]
agent_id = os.environ["OMNIGENT_AGENT_ID"]

with Omnigent(base_url=server_url) as client:
    with client.agents.sessions.create(
        agent_id=agent_id,
        input="Summarize this repository.",
        stream=True,
    ) as events:
        session_id = events.session_id
        for event in events:
            print(event.to_json(indent=None), flush=True)

    outcome = events.terminal_event
    if outcome is None:
        raise RuntimeError("The stream closed without a terminal outcome.")
    print("Outcome:", outcome.type)

    client.agents.sessions.delete(session_id)
```

`create(input=..., stream=True)` transparently composes existing REST calls: it
creates the session, opens its live stream through the readiness heartbeat, and
then submits the input. It returns that already-open stream. Entering its context
establishes local ownership; it does not start another request.

`response.completed` is the successful terminal event. Failed, cancelled, and
incomplete terminal events are outcomes, but not success. Closing the stream or
client only releases local resources. It never interrupts or deletes the remote
session.

## Async equivalent

The async session resource tree has the same operations and parameters. Use
`AsyncOmnigent`; `OmnigentClient` remains its supported compatibility name.

<!-- sdk-example: executable -->

```python
import asyncio
import os

from omnigent_client import AsyncOmnigent


async def main() -> None:
    async with AsyncOmnigent(base_url=os.environ["OMNIGENT_BASE_URL"]) as client:
        events = await client.agents.sessions.create(
            agent_id=os.environ["OMNIGENT_AGENT_ID"],
            input="Summarize this repository.",
            stream=True,
        )
        session_id = events.session_id
        async with events:
            async for event in events:
                print(event.to_json(indent=None), flush=True)

        outcome = events.terminal_event
        if outcome is None:
            raise RuntimeError("The stream closed without a terminal outcome.")
        print("Outcome:", outcome.type)

        await client.agents.sessions.delete(session_id)


asyncio.run(main())
```

## Follow-up input and cleanup

Open the live stream before submitting later input so the readiness heartbeat is
confirmed first:

```text
from omnigent_client import SessionMessage

with client.agents.sessions.events.stream(session_id) as events:
    client.agents.sessions.events.create(
        session_id,
        events=SessionMessage.text("Now include file sizes."),
    )
    for event in events:
        print(event.type)
```

Cancellation and deletion are deliberately separate remote actions:

```text
client.agents.sessions.events.cancel(session_id)
client.agents.sessions.delete(session_id)
```

Event submission and typed elicitation resolution are preview APIs. The public
event-input allowlist is `SessionMessage`, `FunctionCallOutput`, and `Interrupt`.
Elicitation decisions are always explicit:

```text
client.agents.sessions.elicitations.resolve(
    session_id,
    elicitation_id,
    action="decline",
)
```

## Durable state and recovery

The SSE endpoint is a live tail, not replayable history. If it disconnects:

1. Open a replacement stream first and buffer new events.
2. Retrieve the current snapshot and durable items.
3. Merge the snapshot, items, and buffered events by item `id` and
   `response_id`.
4. Do not automatically repost the input.

Use `sessions.retrieve()` for a snapshot and `sessions.items.list()` for durable
history. List operations return an already-loaded list-compatible cursor page;
call `get_next_page()` only when `has_next_page()` is true.

```text
session = client.agents.sessions.retrieve(session_id)
items = client.agents.sessions.items.list(session_id, limit=100)
if items.has_next_page():
    more_items = items.get_next_page()
```

Session files remain under the existing session-scoped routes and require a
deployment with a file store:

```text
uploaded = client.agents.sessions.files.upload(session_id, "report.csv")
client.agents.sessions.files.download(session_id, uploaded.id, "copy.csv")
```

Other current resources include `client.agents.list()`,
`sessions.subagents.list()`, and the session-bound `sessions.agent` operations.
There is no top-level agent CRUD surface.

## Errors and raw responses

All HTTP status failures raised by the SDK derive from `OmnigentError`. Network
and other HTTPX transport failures propagate as HTTPX exceptions after any
safe-read retry is exhausted. A multi-request create that fails after the remote
session exists raises `SessionCompositionError`; its `session_id` and `phase`
let the caller inspect or clean up that session without resending input.
SDK-owned transports retry connection failures for safe GET/HEAD requests;
caller-injected HTTPX clients control their own retry policy.

Raw response metadata is currently available for session retrieval without a
second request:

```text
raw = client.agents.sessions.with_raw_response.retrieve(session_id)
print(raw.status_code, raw.request_id)
session = raw.parse()
```

## `SessionsChat`

`SessionsChat` remains an async-only, opt-in convenience for uploaded agent
bundles. It can dispatch client tools and handle elicitation hooks, so the
low-level resource API never calls it implicitly:

```text
chat = await client.sessions_chat(bundle_bytes)
async for event in chat.send("Summarize this repository."):
    print(event.type)
```

Client tools must already be declared by the uploaded agent spec. Supply a
matching name-to-callable map; it is validated when streaming starts:

```text
from omnigent_client import SessionToolCallInfo


def open_in_editor(call: SessionToolCallInfo) -> str:
    return f"Opened {call.arguments}"


chat = await client.sessions_chat(
    bundle_bytes,
    tool_callables={"open_in_editor": open_in_editor},
)
```

## Migrating existing code

| Existing spelling | Mature resource spelling |
|---|---|
| `OmnigentClient(...)` | `AsyncOmnigent(...)` (the old name remains an alias) |
| `client.sessions` | `client.agents.sessions` (both are the same object) |
| `sessions.get(id)` | `sessions.retrieve(id)` |
| `sessions.create_from_agent_id(id)` | `sessions.create(agent_id=id)` |
| `sessions.list_items(id)` | `sessions.items.list(id)` |
| `sessions.child_sessions(id)` | `sessions.subagents.list(id)` |
| `sessions.stream(id)` | `sessions.events.stream(id)` |
| `sessions.post_event(id, event)` | `sessions.events.create(id, events=event)` |
| `sessions.interrupt(id)` | `sessions.events.cancel(id)` |
| Direct `SessionsChat` construction | `await client.sessions_chat(bundle)` |
| `client.files.for_session(id)` | `sessions.files` methods with `id` |

Compatibility imports remain available, but new session-first code should not
build on `/v1/responses`, `client.responses`, `client.session()`,
`client.query()`, the root `Session` type, response-specific SSE helpers, or
global file methods. `BlockStream` and its transforms remain optional
presentation conveniences; the low-level session transport does not depend on
them. Only `ResponsesNamespace` currently emits a deprecation warning; this
documentation does not introduce new deprecations or removal dates.
