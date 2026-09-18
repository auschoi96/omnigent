"""Focused route contract for session-scoped SDK file resources."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from omnigent_client._files import AsyncSessionFilesResource, FilesNamespace
from pydantic import ValidationError


def _file(file_id: str, session_id: str = "session/a") -> dict[str, object]:
    return {
        "id": file_id,
        "object": "session.resource",
        "type": "file",
        "session_id": session_id,
        "name": "report.txt",
        "metadata": {"filename": "report.txt", "bytes": 3, "created_at": 7},
    }


@pytest.mark.asyncio
async def test_session_files_share_routes_types_pagination_and_legacy_adapter(
    tmp_path: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        assert request.url.raw_path.startswith(b"/v1/sessions/session%2Fa/resources/files")
        if request.method == "GET" and path.endswith("/files"):
            cursor = request.url.params.get("after")
            return httpx.Response(
                200,
                json={
                    "object": "list",
                    "data": [],
                    "first_id": None,
                    "last_id": "server-next" if cursor is None else None,
                    "has_more": cursor is None,
                },
            )
        if request.method == "POST" and path.endswith("/files"):
            return httpx.Response(201, json=_file("file-upload"))
        if request.method == "POST" and path.endswith("/files:copy"):
            return httpx.Response(
                200,
                json={
                    "object": "session.files.copied",
                    "session_id": "session/a",
                    "mapping": {
                        "source": {
                            "new_id": "copy",
                            "filename": "report.txt",
                            "content_type": "text/plain",
                        }
                    },
                },
            )
        if request.method == "DELETE":
            return httpx.Response(
                200, json={"id": "file-1", "object": "session.resource.deleted", "deleted": True}
            )
        if path.endswith("/content"):
            return httpx.Response(304)
        return httpx.Response(200, json=_file("file-1"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=17.0) as http:
        resource = AsyncSessionFilesResource(http, "http://srv")
        page = await resource.list(
            "session/a",
            before="older",
            extra_query={"trace": "1", "after": "stale-after", "before": "stale-before"},
        )
        assert "after" not in requests[-1].url.params
        assert requests[-1].url.params["before"] == "older"
        assert page == [] and page.last_id == "server-next"
        await page.get_next_page()
        assert requests[-1].url.params["after"] == "server-next"
        assert requests[-1].url.params["before"] == "older"
        assert requests[-1].url.params["trace"] == "1"

        source = tmp_path / "report.txt"
        source.write_bytes(b"abc")
        assert (await resource.upload("session/a", source)).id == "file-upload"
        assert (await resource.retrieve("session/a", "file-1")).id == "file-1"
        assert await resource.content("session/a", "file-1") == b""
        assert (await resource.delete("session/a", "file-1")).deleted
        assert (
            await resource.copy("session/a", source_session_id="parent", file_ids=["source"])
        ).mapping["source"].new_id == "copy"

        legacy = FilesNamespace(http, "http://srv").for_session("session/a")
        assert (await legacy.get("file-1")).filename == "report.txt"

        inherited_requests = list(requests)
        await resource.retrieve(
            "session/a",
            "file-1",
            timeout=3.0,
            extra_headers={"x-extra": "present"},
            extra_query={"trace": "override"},
        )
        assert all(request.extensions["timeout"]["read"] == 17.0 for request in inherited_requests)
        assert requests[-1].extensions["timeout"]["read"] == 3.0
        assert requests[-1].headers["x-extra"] == "present"
        assert requests[-1].url.params["trace"] == "override"


@pytest.mark.asyncio
async def test_copy_rejects_invalid_file_ids_before_network() -> None:
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        resource = AsyncSessionFilesResource(http, "http://srv")
        with pytest.raises(ValidationError):
            await resource.copy("destination", source_session_id="parent", file_ids=["", ""])
    assert not called
