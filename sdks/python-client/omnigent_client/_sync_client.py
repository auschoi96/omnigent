"""Native synchronous top-level client and agent resource."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, cast

import httpx

from omnigent.protocol import AgentObject, PaginatedList
from omnigent.trusted_origin import OMNIGENT_INTERNAL_WS_ORIGIN

from ._errors import raise_for_status, require_json_object, response_body
from ._http import (
    _InjectedClientLease,
    _InjectedClientPolicy,
    _SyncHTTPClient,
    apply_injected_client_policy,
    is_loopback_url,
    refuse_cross_origin_redirect,
    restore_injected_client_policy,
)
from ._pagination import SyncCursorPage
from ._sessions_chat import ToolCallable
from ._sessions_shared import Headers, Query, Timeout, query_params, request_options
from ._sync_sessions import SyncSessionsResource
from ._sync_sessions_chat import SyncSessionsChat
from ._tool_handler import StreamHooks


class SyncAgentsResource:
    """Read-only registered-agent catalog with nested session resources."""

    def __init__(self, http: httpx.Client, base_url: str, sessions: SyncSessionsResource) -> None:
        self._http, self._base = http, base_url
        self.sessions = sessions

    def list(
        self,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        order: Literal["asc", "desc"] = "desc",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SyncCursorPage[AgentObject]:
        params = query_params(extra_query, limit=limit, after=after, before=before, order=order)
        response = self._http.get(
            f"{self._base}/v1/agents",
            params=params,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        page = PaginatedList.model_validate(require_json_object(response, "GET /v1/agents"))
        data = [AgentObject.model_validate(item) for item in page.data]
        return SyncCursorPage(
            data,
            first_id=page.first_id,
            last_id=page.last_id,
            has_more=page.has_more,
            fetch_next_page=lambda cursor: self.list(
                limit=limit,
                after=cursor,
                before=before,
                order=order,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            ),
        )


class Omnigent:
    """Synchronous OmniGent client backed directly by :class:`httpx.Client`."""

    def __init__(
        self,
        base_url: str,
        *,
        auth: httpx.Auth | tuple[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: Any = None,
        timeout: float | httpx.Timeout = 30.0,
        max_retries: int = 2,
        http_client: httpx.Client | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self._base_url = base_url.rstrip("/")
        self._owns_http = http_client is None
        self._http: httpx.Client
        self._injected_http: httpx.Client | None = None
        self._injected_client_lease: _InjectedClientLease | None = None
        self._injected_client_policy: _InjectedClientPolicy | None = None
        if http_client is None:
            default_headers = {"Origin": OMNIGENT_INTERNAL_WS_ORIGIN}
            if headers:
                default_headers.update(headers)
            self._http = _SyncHTTPClient(
                max_connect_retries=max_retries,
                headers=default_headers,
                auth=auth,
                cookies=cookies,
                timeout=timeout,
                follow_redirects=True,
                event_hooks={"response": [refuse_cross_origin_redirect]},
                trust_env=not is_loopback_url(self._base_url),
            )
        else:
            if headers is not None or auth is not None or cookies is not None:
                raise ValueError(
                    "headers, auth, and cookies must be configured on an injected http_client"
                )
            if timeout != 30.0 or max_retries != 2:
                raise ValueError(
                    "timeout and max_retries must be configured on an injected http_client"
                )
            self._injected_http = http_client
            self._injected_client_lease = _InjectedClientLease(http_client)
            self._http = cast(httpx.Client, self._injected_client_lease)
            self._injected_client_policy = apply_injected_client_policy(
                http_client,
                origin=OMNIGENT_INTERNAL_WS_ORIGIN,
                response_hook=refuse_cross_origin_redirect,
            )

        try:
            self.sessions = SyncSessionsResource(self._http, self._base_url)
            self.agents = SyncAgentsResource(self._http, self._base_url, self.sessions)
        except BaseException:
            if self._owns_http:
                self._http.close()
            elif self._injected_client_policy is not None:
                assert self._injected_http is not None
                restore_injected_client_policy(
                    self._injected_http,
                    self._injected_client_policy,
                    origin=OMNIGENT_INTERNAL_WS_ORIGIN,
                )
                self._injected_client_policy = None
            if self._injected_client_lease is not None:
                self._injected_client_lease.close()
            raise

    def sessions_chat(
        self,
        agent_id: str,
        *,
        tool_callables: dict[str, ToolCallable] | None = None,
        hooks: StreamHooks | None = None,
        **create_kwargs: Any,
    ) -> SyncSessionsChat:
        """Create a sync sessions chat helper bound to a new registered-agent session.

        Counterpart of the async :meth:`OmnigentClient.sessions_chat` but
        synchronous — no event loop required. Wires the sync sessions
        namespace and agent-tools getter automatically. File upload/get
        callables are not wired by default; pass them to the
        :class:`SyncSessionsChat` constructor directly if needed.

        :param agent_id: Durable agent identifier.
        :param tool_callables: Optional name -> callable map for client-side
            tools. Validated against the agent spec on the first send.
        :param hooks: Optional :class:`StreamHooks` fired from stream events.
        :param create_kwargs: Forwarded to :meth:`SyncSessionsResource.create`
            (e.g. ``host_type="managed"``, ``timeout=300.0``).
        :returns: A :class:`SyncSessionsChat` ready for use.
        """
        return SyncSessionsChat.create_registered(
            self.sessions,
            agent_id=agent_id,
            tool_callables=tool_callables,
            agent_tools_getter=self._fetch_agent_tools,
            hooks=hooks,
            **create_kwargs,
        )

    def _fetch_agent_tools(
        self, agent_id: str, session_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Fetch the spec-declared tool entries for a session's agent.

        Sync counterpart of :meth:`OmnigentClient._fetch_agent_tools`.
        Reads the tool list off the raw agent JSON from
        ``GET /v1/sessions/{session_id}/agent``.
        """
        if session_id is None:
            return []
        path = f"{self._base_url}/v1/sessions/{session_id}/agent"
        resp = self._http.get(path)
        if resp.status_code != 200:
            return []
        agent_data = resp.json()
        tools = agent_data.get("tools")
        if isinstance(tools, list):
            return [t for t in tools if isinstance(t, dict)]
        return []

    def close(self) -> None:
        if self._owns_http:
            self._http.close()
        elif self._injected_client_policy is not None:
            assert self._injected_http is not None
            assert self._injected_client_lease is not None
            self._injected_client_lease.close()
            restore_injected_client_policy(
                self._injected_http,
                self._injected_client_policy,
                origin=OMNIGENT_INTERNAL_WS_ORIGIN,
            )
            self._injected_client_policy = None

    def __enter__(self) -> Omnigent:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


__all__ = ["Omnigent", "SyncAgentsResource"]
