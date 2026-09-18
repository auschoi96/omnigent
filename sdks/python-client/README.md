# omnigent-client

Python client SDK for the [omnigent](https://github.com/omnigent-ai/omnigent)
server API.

`omnigent-client` is a typed client for driving omnigent sessions over the
server's HTTP + SSE API — creating sessions, sending turns, and streaming
responses. It shares the `StreamEvent` / `SessionStreamEventType` types that the
server emits, so streamed envelopes are validated against a single source of
truth.

It is released in lockstep with the core `omnigent` package at a matching
version:

```bash
pip install omnigent-client
```

See the [omnigent repository](https://github.com/omnigent-ai/omnigent) for full
documentation.

## Async sessions

Create a session, subscribe before its first input is submitted, and consume
the existing typed OmniGent events:

```python
from omnigent_client import OmnigentClient

async with OmnigentClient(base_url="http://localhost:8080") as client:
    events = await client.agents.sessions.create(
        agent_id="ag_123",
        input="Summarize this repository.",
        stream=True,
    )
    async with events:
        async for event in events:
            print(event.to_json())
```

The stream is a live tail, not replayable history. If it disconnects, open a
replacement stream first, then rebuild the durable view with
`sessions.retrieve()` and `sessions.items.list()`, keyed by item ID and
`response_id`. Do not resend the input automatically. Closing a stream or the
client releases local HTTP resources only; cancellation and deletion are
explicit API calls.

For later input, the same readiness rule is available directly:

```python
from omnigent_client import SessionMessage

async with client.agents.sessions.events.stream(session_id) as events:
    await client.agents.sessions.events.create(
        session_id,
        events=SessionMessage.text("Now include file sizes."),
    )
    async for event in events:
        print(event.type)
```
