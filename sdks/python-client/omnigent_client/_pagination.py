"""Cursor pages shared by asynchronous resource list methods."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from typing import Generic, TypeAlias, TypeVar

_T = TypeVar("_T")

AsyncPageFetcher: TypeAlias = Callable[[str], Awaitable["AsyncCursorPage[_T]"]]


class AsyncCursorPage(list[_T], Generic[_T]):
    """One loaded cursor page with an explicit asynchronous next-page fetch."""

    def __init__(
        self,
        data: Iterable[_T] = (),
        *,
        first_id: str | None = None,
        last_id: str | None = None,
        has_more: bool = False,
        fetch_next_page: AsyncPageFetcher[_T] | None = None,
    ) -> None:
        super().__init__(data)
        self.first_id = first_id
        self.last_id = last_id
        self.has_more = has_more
        self._fetch_next_page = fetch_next_page

    @property
    def data(self) -> AsyncCursorPage[_T]:
        """The already loaded list; accessing it never fetches."""
        return self

    def has_next_page(self) -> bool:
        return self.has_more and self.last_id is not None and self._fetch_next_page is not None

    async def get_next_page(self) -> AsyncCursorPage[_T]:
        """Fetch the next page from this response's server-provided cursor."""
        if not self.has_next_page():
            raise RuntimeError("No next page is available")
        assert self.last_id is not None
        assert self._fetch_next_page is not None
        return await self._fetch_next_page(self.last_id)
