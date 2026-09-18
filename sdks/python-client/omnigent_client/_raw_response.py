"""Typed access to an HTTP response that has already been executed."""

from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar, cast

import httpx

_T = TypeVar("_T")
_UNPARSED = object()


class APIResponse(Generic[_T]):
    """Expose response metadata and lazily parse its already-loaded body."""

    def __init__(self, response: httpx.Response, parser: Callable[[httpx.Response], _T]) -> None:
        self._response = response
        self._parser = parser
        self._parsed: object = _UNPARSED

    def parse(self) -> _T:
        if self._parsed is _UNPARSED:
            self._parsed = self._parser(self._response)
        return cast(_T, self._parsed)

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def request_id(self) -> str | None:
        return self._response.headers.get("X-Request-Id")

    @property
    def headers(self) -> httpx.Headers:
        return self._response.headers

    @property
    def content(self) -> bytes:
        return self._response.content

    @property
    def text(self) -> str:
        return self._response.text
