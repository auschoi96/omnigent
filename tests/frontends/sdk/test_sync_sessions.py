"""Focused native-sync contracts not already covered by the async suite."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterable
from typing import Any

import httpx
import omnigent_client._client as async_client_module
import omnigent_client._sync_client as sync_client_module
import pytest
from omnigent_client import (
    AsyncOmnigent,
    Omnigent,
    OmnigentClient,
    OutputTextDeltaEvent,
    Session,
    SessionCompositionError,
    SessionEventStream,
    SessionMessage,
    StreamProtocolError,
    SyncCursorPage,
)
from omnigent_client._errors import OmnigentError
from omnigent_client._session import Session as LegacySession
from omnigent_client._sessions import _parse_sse_lines
from omnigent_client._sessions_shared import SessionSSEDecoder

from omnigent.server.schemas import OutputTextDeltaEvent as UpstreamOutputTextDeltaEvent


def _session(session_id: str = "conv_1", status: str = "idle") -> dict[str, Any]:
    return {
        "id": session_id,
        "agent_id": "ag_1",
        "status": status,
        "created_at": 1,
        "updated_at": 2,
        "items": [],
    }


def _completed(response_id: str = "resp_1") -> dict[str, Any]:
    return {
        "type": "response.completed",
        "response": {
            "id": response_id,
            "status": "completed",
            "model": "test-model",
            "created_at": 1,
        },
    }


def _sse(events: Iterable[tuple[str | None, dict[str, Any] | str]]) -> bytes:
    parts: list[str] = []
    for event_type, payload in events:
        if event_type is not None:
            parts.append(f"event: {event_type}\n")
        wire = payload if isinstance(payload, str) else json.dumps(payload)
        parts.append(f"data: {wire}\n\n")
    return "".join(parts).encode()


class _TrackedStream(httpx.SyncByteStream):
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.closed = False

    def __iter__(self):  # type: ignore[no-untyped-def]
        yield self.body

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_public_sync_exports_alias_identity_and_shared_client_ownership() -> None:
    """Injected policy persists until the last sync or async SDK wrapper closes."""

    def page_response() -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": [],
                "first_id": None,
                "last_id": None,
                "has_more": False,
            },
        )

    sync_requests: list[httpx.Request] = []

    def sync_list_response(request: httpx.Request) -> httpx.Response:
        sync_requests.append(request)
        return page_response()

    injected = httpx.Client(transport=httpx.MockTransport(sync_list_response))
    sync_state = (
        dict(injected.headers),
        injected.follow_redirects,
        {name: list(hooks) for name, hooks in injected.event_hooks.items()},
    )
    client = Omnigent("http://srv", http_client=injected)
    second_client = Omnigent("http://srv", http_client=injected)
    assert AsyncOmnigent is OmnigentClient
    assert Session is LegacySession
    assert client.sessions is client.agents.sessions
    client.close()
    assert injected.is_closed is False
    assert injected.headers["Origin"]
    assert injected.follow_redirects is True
    assert len(injected.event_hooks["response"]) == 1
    with pytest.raises(RuntimeError, match="Omnigent client is closed"):
        client.sessions.list()
    assert sync_requests == []
    assert second_client.sessions.list() == []
    second_client.close()
    assert (
        dict(injected.headers),
        injected.follow_redirects,
        {name: list(hooks) for name, hooks in injected.event_hooks.items()},
    ) == sync_state
    assert injected.get("http://srv/v1/sessions").status_code == 200
    injected.close()

    owned = Omnigent("http://127.0.0.1:1")
    owned.close()
    assert owned._http.is_closed is True

    async_requests: list[httpx.Request] = []

    def async_list_response(request: httpx.Request) -> httpx.Response:
        async_requests.append(request)
        return page_response()

    async_injected = httpx.AsyncClient(transport=httpx.MockTransport(async_list_response))
    async_state = (
        dict(async_injected.headers),
        async_injected.follow_redirects,
        {name: list(hooks) for name, hooks in async_injected.event_hooks.items()},
    )
    async_client = AsyncOmnigent("http://srv", http_client=async_injected)
    second_async_client = AsyncOmnigent("http://srv", http_client=async_injected)
    await async_client.close()
    assert async_injected.is_closed is False
    assert async_injected.headers["Origin"]
    assert async_injected.follow_redirects is True
    assert len(async_injected.event_hooks["response"]) == 1
    with pytest.raises(RuntimeError, match="Omnigent client is closed"):
        await async_client.sessions.list()
    assert async_requests == []
    assert await second_async_client.sessions.list() == []
    await second_async_client.close()
    assert (
        dict(async_injected.headers),
        async_injected.follow_redirects,
        {name: list(hooks) for name, hooks in async_injected.event_hooks.items()},
    ) == async_state
    assert (await async_injected.get("http://srv/v1/sessions")).status_code == 200
    await async_injected.aclose()


@pytest.mark.asyncio
async def test_injected_policy_rolls_back_when_resource_construction_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed wrapper must release either a fresh or shared policy lease."""

    def fail_resource(*args: object, **kwargs: object) -> None:
        raise RuntimeError("resource construction failed")

    sync_http = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(500)))
    sync_state = (
        dict(sync_http.headers),
        sync_http.follow_redirects,
        {name: list(hooks) for name, hooks in sync_http.event_hooks.items()},
    )
    monkeypatch.setattr(sync_client_module, "SyncSessionsResource", fail_resource)
    with pytest.raises(RuntimeError, match="resource construction failed"):
        Omnigent("http://srv", http_client=sync_http)
    assert (
        dict(sync_http.headers),
        sync_http.follow_redirects,
        {name: list(hooks) for name, hooks in sync_http.event_hooks.items()},
    ) == sync_state
    sync_http.close()

    async_http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    async_state = (
        dict(async_http.headers),
        async_http.follow_redirects,
        {name: list(hooks) for name, hooks in async_http.event_hooks.items()},
    )
    first = AsyncOmnigent("http://srv", http_client=async_http)
    monkeypatch.setattr(async_client_module, "SessionsNamespace", fail_resource)
    with pytest.raises(RuntimeError, match="resource construction failed"):
        AsyncOmnigent("http://srv", http_client=async_http)
    assert async_http.headers["Origin"]
    assert async_http.follow_redirects is True
    assert len(async_http.event_hooks["response"]) == 1
    await first.close()
    assert (
        dict(async_http.headers),
        async_http.follow_redirects,
        {name: list(hooks) for name, hooks in async_http.event_hooks.items()},
    ) == async_state
    await async_http.aclose()


def test_public_sdk_reexports_upstream_event_model() -> None:
    """The public convenience import keeps upstream schemas single-source."""
    assert OutputTextDeltaEvent is UpstreamOutputTextDeltaEvent


def test_sync_typed_event_request_and_raw_response_share_one_execution() -> None:
    """Nearest async tests cover each half; this pins native-sync composition."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/events"):
            assert json.loads(request.content) == SessionMessage.text("hello").model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
            return httpx.Response(202, json={"queued": True, "item_id": "msg_1"})
        return httpx.Response(200, headers={"x-request-id": "req_1"}, json=_session())

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = Omnigent("http://srv", http_client=http)
        assert (
            client.sessions.events.create("conv_1", events=SessionMessage.text("hello")).item_id
            == "msg_1"
        )
        raw = client.sessions.with_raw_response.retrieve("conv_1")
        assert raw.request_id == "req_1"
        assert raw.parse().id == "conv_1"
        assert raw.parse().id == "conv_1"
        assert len(requests) == 2


def test_sync_create_stream_orders_readiness_write_and_local_close() -> None:
    """Extends the async create-stream test for blocking iterator ownership."""
    requests: list[str] = []
    body = _TrackedStream(
        _sse(
            [
                ("session.heartbeat", {"type": "session.heartbeat"}),
                (
                    "response.output_text.delta",
                    {"type": "response.output_text.delta", "delta": "ok"},
                ),
                ("response.completed", _completed()),
                (None, "[DONE]"),
            ]
        )
    )

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(f"{request.method} {request.url.path}")
        if request.url.path == "/v1/sessions":
            return httpx.Response(201, json=_session())
        if request.url.path.endswith("/stream"):
            return httpx.Response(200, stream=body)
        return httpx.Response(202, json={"queued": True})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = Omnigent("http://srv", http_client=http)
        stream = client.sessions.create(agent_id="ag_1", input="start", stream=True)
        assert isinstance(stream, SessionEventStream)
        assert requests == [
            "POST /v1/sessions",
            "GET /v1/sessions/conv_1/stream",
            "POST /v1/sessions/conv_1/events",
        ]
        with stream:
            observed = list(stream)

    assert [event.type for event in observed] == [
        "response.output_text.delta",
        "response.completed",
    ]
    assert stream.last_response_id == "resp_1"
    assert stream.terminal_event is observed[-1]
    assert body.closed is True
    assert not any(request.startswith("DELETE") for request in requests)


@pytest.mark.parametrize(
    ("phase", "stream", "input_value"),
    [
        ("stream_open", True, "start"),
        ("input_submit", True, "start"),
        ("snapshot_retrieve", False, "start"),
        ("snapshot_retrieve", False, None),
    ],
)
def test_sync_create_composition_failure_retains_remote_session(
    phase: str, stream: bool, input_value: str | None
) -> None:
    """Native counterpart to the one existing async partial-failure table."""
    requests: list[httpx.Request] = []
    stream_events = (
        [] if phase == "stream_open" else [("session.heartbeat", {"type": "session.heartbeat"})]
    )
    body = _TrackedStream(_sse(stream_events))

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/sessions":
            return httpx.Response(
                201,
                json={"session_id": "conv_1"} if phase == "snapshot_retrieve" else _session(),
            )
        if request.url.path.endswith("/stream"):
            return httpx.Response(200, stream=body)
        if request.url.path.endswith("/events"):
            if phase == "input_submit":
                return httpx.Response(500, json={"error": {"message": "submit failed"}})
            return httpx.Response(202, json={"queued": True})
        return httpx.Response(500, json={"error": {"message": "retrieve failed"}})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = Omnigent("http://srv", http_client=http)
        with pytest.raises(SessionCompositionError) as raised:
            if phase == "snapshot_retrieve":
                client.sessions.create(b"bundle", input=input_value)
            else:
                client.sessions.create(agent_id="ag_1", input=input_value, stream=stream)

    assert raised.value.phase == phase
    assert raised.value.session_id == "conv_1"
    assert raised.value.original_exception is raised.value.__cause__
    assert sum(request.url.path.endswith("/events") for request in requests) == (
        0 if phase == "stream_open" or input_value is None else 1
    )
    assert all(request.method != "DELETE" for request in requests)
    if stream:
        assert body.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lines",
    [
        [
            "event: response.output_text.delta",
            'data: {"type":"response.output_text.delta","delta":"x"}',
            "",
            "data: [DONE]",
        ],
        ["event: future.event", 'data: {"type":"future.event","added":1}', "", "data: [DONE]"],
        ["event: malformed", "data: not-json", "", "data: [DONE]"],
    ],
)
async def test_shared_sse_decoder_matches_async_iterator(lines: list[str]) -> None:
    """The old async parser is now an I/O driver over the same pure decoder."""
    decoder = SessionSSEDecoder()
    sync_events = []
    for line in lines:
        event, done = decoder.feed(line)
        if event is not None:
            sync_events.append(event)
        if done:
            break

    async def source() -> AsyncIterator[str]:
        for line in lines:
            yield line

    async_events = [event async for event in _parse_sse_lines(source())]
    assert [type(event) for event in sync_events] == [type(event) for event in async_events]
    assert [event.model_dump() for event in sync_events] == [
        event.model_dump() for event in async_events
    ]


@pytest.mark.asyncio
async def test_sync_and_async_streams_reject_eof_without_done() -> None:
    """Subscriber overflow is distinguishable from normal ``[DONE]`` closure."""
    content = _sse(
        [
            ("session.heartbeat", {"type": "session.heartbeat"}),
            ("response.output_text.delta", {"type": "response.output_text.delta", "delta": "x"}),
        ]
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=content)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = Omnigent("http://srv", http_client=http)
        with client.sessions.events.stream("conv_1") as stream:
            assert next(stream).type == "response.output_text.delta"
            with pytest.raises(StreamProtocolError):
                next(stream)
        client.close()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AsyncOmnigent("http://srv", http_client=http)
        stream = client.sessions.events.stream("conv_1")
        async with stream:
            assert (await stream.__anext__()).type == "response.output_text.delta"
            with pytest.raises(StreamProtocolError):
                await stream.__anext__()
        await client.close()


def test_sync_cursor_page_is_loaded_list_and_fetches_next_cursor() -> None:
    """Nearest async page test cannot detect blocking next-page wiring errors."""
    cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("after")
        cursors.append(cursor)
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": [],
                "first_id": None,
                "last_id": "cursor_2" if cursor is None else None,
                "has_more": cursor is None,
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        page = Omnigent("http://srv", http_client=http).sessions.list()
        assert isinstance(page, SyncCursorPage)
        assert isinstance(page, list) and page.data is page and len(page) == 0
        next_page = page.get_next_page()
        assert next_page.has_more is False
    assert cursors == [None, "cursor_2"]


def test_sync_nested_file_retrieve_uses_session_scoped_route() -> None:
    """Complements the async file matrix with native path quoting and parsing."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.raw_path == b"/v1/sessions/session%2Fa/resources/files/file%2F1"
        return httpx.Response(
            200,
            json={
                "id": "file/1",
                "object": "session.resource",
                "type": "file",
                "session_id": "session/a",
                "name": "report.txt",
                "metadata": {},
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        value = Omnigent("http://srv", http_client=http).sessions.files.retrieve(
            "session/a", "file/1"
        )
    assert value.id == "file/1"


@pytest.mark.asyncio
async def test_sync_and_async_nested_elicitations_share_the_preview_contract() -> None:
    """The legacy async POST test cannot catch the missing nested or sync resource."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "status": "pending",
                    "message": "Choose",
                    "phase": "approval",
                    "policy_name": "shell",
                    "content_preview": "rm file",
                },
            )
        return httpx.Response(202, json={"queued": False})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = AsyncOmnigent("http://srv", http_client=http)
        state = await client.sessions.elicitations.retrieve(
            "session/a",
            "elicit/1",
            timeout=7.0,
            extra_headers={"x-test-client": "async"},
            extra_query={"view": "full"},
        )
        ack = await client.sessions.elicitations.resolve(
            "session/a",
            "elicit/1",
            action="accept",
            content={"choice": "a"},
            meta={"persist": "session"},
            timeout=7.0,
            extra_headers={"x-test-client": "async"},
            extra_query={"view": "full"},
        )
        await client.close()
    assert state.status == "pending" and state.message == "Choose"
    assert ack.queued is False

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = Omnigent("http://srv", http_client=http)
        state = client.sessions.elicitations.retrieve(
            "session/a",
            "elicit/1",
            timeout=7.0,
            extra_headers={"x-test-client": "sync"},
            extra_query={"view": "full"},
        )
        ack = client.sessions.elicitations.resolve(
            "session/a",
            "elicit/1",
            action="accept",
            content={"choice": "a"},
            meta={"persist": "session"},
            timeout=7.0,
            extra_headers={"x-test-client": "sync"},
            extra_query={"view": "full"},
        )
        client.close()
    assert state.status == "pending" and state.message == "Choose"
    assert ack.queued is False

    assert [request.method for request in requests] == ["GET", "POST", "GET", "POST"]
    assert [request.url.raw_path.split(b"?")[0] for request in requests] == [
        b"/v1/sessions/session%2Fa/elicitations/elicit%2F1",
        b"/v1/sessions/session%2Fa/elicitations/elicit%2F1/resolve",
        b"/v1/sessions/session%2Fa/elicitations/elicit%2F1",
        b"/v1/sessions/session%2Fa/elicitations/elicit%2F1/resolve",
    ]
    assert all(dict(request.url.params) == {"view": "full"} for request in requests)
    assert [request.headers["x-test-client"] for request in requests] == [
        "async",
        "async",
        "sync",
        "sync",
    ]
    assert [json.loads(request.content) for request in requests if request.method == "POST"] == [
        {"action": "accept", "content": {"choice": "a"}, "_meta": {"persist": "session"}},
        {"action": "accept", "content": {"choice": "a"}, "_meta": {"persist": "session"}},
    ]


def test_sync_client_refuses_cross_origin_redirect() -> None:
    """Existing redirect tests cover async hooks, not httpx.Client hooks."""
    seen_hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_hosts.append(request.url.host)
        return httpx.Response(307, headers={"location": "https://elsewhere.invalid/steal"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        client = Omnigent("http://srv", http_client=http)
        with pytest.raises(OmnigentError):
            client.sessions.events.create("conv_1", events=SessionMessage.text("secret"))
    assert seen_hosts == ["srv"]


def test_sync_sse_uses_explicit_timeout() -> None:
    """Async timeout coverage cannot catch missing sync stream options."""
    observed: list[dict[str, float]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(request.extensions["timeout"])
        return httpx.Response(
            200,
            content=_sse(
                [
                    ("session.heartbeat", {"type": "session.heartbeat"}),
                    (None, "[DONE]"),
                ]
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        with Omnigent("http://srv", http_client=http).sessions.events.stream(
            "conv_1", timeout=17.0
        ) as stream:
            assert list(stream) == []
    assert observed[0]["read"] == 17.0
