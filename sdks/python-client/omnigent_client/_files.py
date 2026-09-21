"""Session-scoped file resources and compatibility adapters."""

from __future__ import annotations

import mimetypes
import pathlib
from collections.abc import Mapping, Sequence
from os import PathLike
from typing import Any, Literal
from urllib.parse import quote

import httpx

from omnigent.server.schemas import (
    CopyFilesRequest,
    CopyFilesResponse,
    SessionResourceObject,
    SessionResourcePaginatedList,
)

from ._errors import raise_for_status, require_json_object, response_body
from ._models import SessionResourceDeleted
from ._pagination import AsyncCursorPage
from ._sessions_shared import session_files_url
from ._types import File

Timeout = float | httpx.Timeout | None
Headers = Mapping[str, str] | None
Query = Mapping[str, str | int | float | bool | None] | None


def _timeout_option(timeout: Timeout) -> dict[str, Any]:
    """Preserve the shared client's timeout unless this request overrides it."""
    return {} if timeout is None else {"timeout": timeout}


class AsyncSessionFilesResource:
    """Async access to session-file REST routes."""

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self._http, self._base = http, base_url

    def _path(self, session_id: str) -> str:
        return session_files_url(self._base, session_id)

    async def list(
        self,
        session_id: str,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        order: Literal["asc", "desc"] = "desc",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> AsyncCursorPage[SessionResourceObject]:
        params: dict[str, str | int | float | bool | None] = dict(extra_query or {})
        params.update(limit=limit, order=order)
        if after is not None:
            params["after"] = after
        else:
            params.pop("after", None)
        if before is not None:
            params["before"] = before
        else:
            params.pop("before", None)
        resp = await self._http.get(
            self._path(session_id),
            params=params,
            headers=extra_headers,
            **_timeout_option(timeout),
        )
        raise_for_status(resp.status_code, response_body(resp))
        wire = SessionResourcePaginatedList.model_validate(
            require_json_object(resp, "GET /v1/sessions/{session_id}/resources/files")
        )

        async def fetch_next(cursor: str) -> AsyncCursorPage[SessionResourceObject]:
            return await self.list(
                session_id,
                limit=limit,
                after=cursor,
                before=before,
                order=order,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            )

        return AsyncCursorPage(
            wire.data,
            first_id=wire.first_id,
            last_id=wire.last_id,
            has_more=wire.has_more,
            fetch_next_page=fetch_next,
        )

    async def upload(
        self,
        session_id: str,
        path: str | PathLike[str],
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResourceObject:
        local = pathlib.Path(path)
        with local.open("rb") as stream:
            resp = await self._http.post(
                self._path(session_id),
                params=extra_query,
                headers=extra_headers,
                files={"file": (local.name, stream, mimetypes.guess_type(str(local))[0])},
                **_timeout_option(timeout),
            )
        raise_for_status(resp.status_code, response_body(resp))
        return SessionResourceObject.model_validate(
            require_json_object(resp, "POST /v1/sessions/{session_id}/resources/files")
        )

    async def retrieve(
        self,
        session_id: str,
        file_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResourceObject:
        resp = await self._http.get(
            f"{self._path(session_id)}/{quote(file_id, safe='')}",
            params=extra_query,
            headers=extra_headers,
            **_timeout_option(timeout),
        )
        raise_for_status(resp.status_code, response_body(resp))
        return SessionResourceObject.model_validate(
            require_json_object(resp, "GET /v1/sessions/{session_id}/resources/files/{file_id}")
        )

    async def content(
        self,
        session_id: str,
        file_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> bytes:
        resp = await self._http.get(
            f"{self._path(session_id)}/{quote(file_id, safe='')}/content",
            params=extra_query,
            headers=extra_headers,
            **_timeout_option(timeout),
        )
        raise_for_status(resp.status_code, response_body(resp))
        return resp.content

    async def download(
        self,
        session_id: str,
        file_id: str,
        to_path: str | PathLike[str],
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> pathlib.Path:
        content = await self.content(
            session_id,
            file_id,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        path = pathlib.Path(to_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    async def delete(
        self,
        session_id: str,
        file_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResourceDeleted:
        resp = await self._http.delete(
            f"{self._path(session_id)}/{quote(file_id, safe='')}",
            params=extra_query,
            headers=extra_headers,
            **_timeout_option(timeout),
        )
        raise_for_status(resp.status_code, response_body(resp))
        return SessionResourceDeleted.model_validate(
            require_json_object(resp, "DELETE /v1/sessions/{session_id}/resources/files/{file_id}")
        )

    async def copy(
        self,
        session_id: str,
        *,
        source_session_id: str,
        file_ids: Sequence[str],
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> CopyFilesResponse:
        body = CopyFilesRequest(source_session_id=source_session_id, file_ids=list(file_ids))
        resp = await self._http.post(
            f"{self._path(session_id)}:copy",
            params=extra_query,
            headers=extra_headers,
            json=body.model_dump(mode="json"),
            **_timeout_option(timeout),
        )
        raise_for_status(resp.status_code, response_body(resp))
        return CopyFilesResponse.model_validate(
            require_json_object(resp, "POST /v1/sessions/{session_id}/resources/files:copy")
        )


class FilesNamespace:
    """Compatibility factory for session-bound file namespaces."""

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self._resource = AsyncSessionFilesResource(http, base_url)

    def for_session(self, session_id: str) -> SessionFilesNamespace:
        return SessionFilesNamespace(self._resource, session_id)

    async def upload(self, path: str) -> File:
        raise RuntimeError(
            "/v1/files was removed; use client.files.for_session(session_id).upload(path)"
        )

    async def list(self, *args: object, **kwargs: object) -> list[File]:
        raise RuntimeError(
            "/v1/files was removed; use client.files.for_session(session_id).list()"
        )

    async def get(self, file_id: str) -> File:
        raise RuntimeError(
            "/v1/files was removed; use client.files.for_session(session_id).get(file_id)"
        )

    async def get_content(self, file_id: str) -> bytes:
        raise RuntimeError(
            "/v1/files was removed; use client.files.for_session(session_id).get_content(file_id)"
        )

    async def download(self, file_id: str, to_path: str | pathlib.Path) -> pathlib.Path:
        raise RuntimeError(
            "/v1/files was removed; use "
            "client.files.for_session(session_id).download(file_id, path)"
        )

    async def delete(self, file_id: str) -> None:
        raise RuntimeError(
            "/v1/files was removed; use client.files.for_session(session_id).delete(file_id)"
        )


class SessionFilesNamespace:
    """Legacy session-bound adapter over ``AsyncSessionFilesResource``."""

    def __init__(self, resource: AsyncSessionFilesResource, session_id: str) -> None:
        self._resource, self._session_id = resource, session_id

    @property
    def session_id(self) -> str:
        return self._session_id

    @staticmethod
    def _as_file(resource: SessionResourceObject) -> File:
        metadata = resource.metadata
        return File.from_dict(
            {
                "id": resource.id,
                "filename": metadata.get("filename", resource.name),
                "bytes": metadata.get("bytes", 0),
                "created_at": metadata.get("created_at", 0),
            }
        )

    async def upload(self, path: str) -> File:
        return self._as_file(await self._resource.upload(self._session_id, path, timeout=30.0))

    async def list(
        self, *, limit: int = 20, after: str | None = None, order: str = "desc"
    ) -> list[File]:
        page = await self._resource.list(self._session_id, limit=limit, after=after, order=order)  # type: ignore[arg-type]
        return [self._as_file(item) for item in page]

    async def get(self, file_id: str) -> File:
        return self._as_file(await self._resource.retrieve(self._session_id, file_id))

    async def get_content(self, file_id: str) -> bytes:
        return await self._resource.content(self._session_id, file_id, timeout=30.0)

    async def download(self, file_id: str, to_path: str | pathlib.Path) -> pathlib.Path:
        return await self._resource.download(self._session_id, file_id, to_path, timeout=30.0)

    async def delete(self, file_id: str) -> None:
        await self._resource.delete(self._session_id, file_id)
