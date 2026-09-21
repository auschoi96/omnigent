"""Distinct invariants for shared SDK response and pagination primitives."""

from __future__ import annotations

import httpx
import omnigent_client
import pytest
from omnigent_client import APIResponse, AsyncCursorPage, StreamProtocolError
from omnigent_client._sessions_shared import SessionSSEDecoder, parse_session_response


@pytest.mark.asyncio
async def test_async_cursor_page_is_a_loaded_list_and_uses_server_cursor() -> None:
    requested: list[str] = []

    async def fetch(cursor: str) -> AsyncCursorPage[dict[str, str]]:
        requested.append(cursor)
        return AsyncCursorPage([{"id": "next-item"}], last_id="next", has_more=False)

    page = AsyncCursorPage[dict[str, str]](
        [], first_id=None, last_id="server-cursor", has_more=True, fetch_next_page=fetch
    )

    assert isinstance(page, list)
    assert page.data is page
    assert not page
    assert list(page) == []
    assert page[:] == []
    assert requested == []
    assert page.has_next_page()

    next_page = await page.get_next_page()

    assert requested == ["server-cursor"]
    assert next_page.data == [{"id": "next-item"}]


@pytest.mark.asyncio
async def test_async_cursor_page_without_next_page_fails_without_fetching() -> None:
    page = AsyncCursorPage(["loaded"], last_id="last", has_more=False)

    assert not page.has_next_page()
    with pytest.raises(RuntimeError, match="No next page"):
        await page.get_next_page()


def test_api_response_parses_one_existing_response_once() -> None:
    assert omnigent_client.APIResponse is APIResponse
    assert omnigent_client.AsyncCursorPage is AsyncCursorPage

    response = httpx.Response(
        200,
        headers={"x-request-id": "request_123"},
        json={"id": "session_123"},
    )
    parse_count = 0

    def parse(existing: httpx.Response) -> dict[str, str]:
        nonlocal parse_count
        parse_count += 1
        return existing.json()

    wrapped = APIResponse(response, parse)

    assert wrapped.status_code == 200
    assert wrapped.request_id == "request_123"
    assert wrapped.headers is response.headers
    assert wrapped.content == response.content
    assert wrapped.text == response.text
    assert wrapped.parse() == {"id": "session_123"}
    assert wrapped.parse() == {"id": "session_123"}
    assert parse_count == 1


def test_session_sse_decoder_rejects_malformed_known_event() -> None:
    decoder = SessionSSEDecoder()
    assert decoder.feed("event: response.output_text.delta") == (None, False)

    with pytest.raises(StreamProtocolError, match="does not match the upstream schema"):
        decoder.feed('data: {"type":"response.output_text.delta"}')


@pytest.mark.parametrize(
    ("item_type", "data"),
    [
        (
            "function_call",
            {"model": "agent", "name": "tool", "arguments": "{}", "call_id": "call_1"},
        ),
        (
            "reasoning",
            {"model": "agent", "summary": [{"type": "summary_text", "text": "thinking"}]},
        ),
        (
            "slash_command",
            {"model": "agent", "kind": "skill", "name": "review", "arguments": ""},
        ),
    ],
)
def test_session_snapshot_accepts_upstream_serialized_agent_aliases(
    item_type: str, data: dict[str, object]
) -> None:
    session = parse_session_response(
        {
            "id": "session_1",
            "agent_id": "agent_1",
            "status": "idle",
            "created_at": 1,
            "items": [
                {
                    "id": "item_1",
                    "type": item_type,
                    "status": "completed",
                    "response_id": "response_1",
                    "created_at": 1,
                    "data": data,
                }
            ],
        }
    )

    assert session.items[0].data.model_dump()["agent"] == "agent"
