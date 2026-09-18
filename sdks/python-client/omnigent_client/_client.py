"""OmnigentClient — the top-level client tying all namespaces together."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Literal, cast, overload

import httpx

from omnigent.protocol import AgentObject, PaginatedList
from omnigent.trusted_origin import OMNIGENT_INTERNAL_WS_ORIGIN

from ._files import FilesNamespace
from ._http import (
    _AsyncHTTPClient,
    _InjectedClientLease,
    _InjectedClientPolicy,
    apply_injected_client_policy,
    is_loopback_url,
    redirect_stays_on_origin,
    refuse_cross_origin_redirect_async,
    restore_injected_client_policy,
)
from ._pagination import AsyncCursorPage
from ._query import QueryResult, QueryStream
from ._responses import ResponsesNamespace
from ._session import Session
from ._sessions import Headers, Query, SessionsNamespace, Timeout, _options, _query
from ._sessions_chat import SessionsChat, ToolCallable
from ._tool_handler import StreamHooks, ToolHandler


def _redirect_stays_on_origin(request_url: httpx.URL, location: str) -> bool:
    """Compatibility wrapper for the neutral shared redirect policy."""
    return redirect_stays_on_origin(request_url, location)


class AsyncAgentsResource:
    """Read-only built-in agent catalog and its session resources."""

    def __init__(
        self, http: httpx.AsyncClient, base_url: str, sessions: SessionsNamespace
    ) -> None:
        self._http = http
        self._base = base_url
        self.sessions = sessions

    async def list(
        self,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        order: Literal["asc", "desc"] = "desc",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> AsyncCursorPage[AgentObject]:
        params = _query(extra_query, limit=limit, order=order, after=after, before=before)
        response = await self._http.get(
            f"{self._base}/v1/agents", params=params, **_options(timeout, extra_headers)
        )
        from ._errors import raise_for_status, require_json_object, response_body

        raise_for_status(response.status_code, response_body(response))
        page = PaginatedList.model_validate(require_json_object(response, "GET /v1/agents"))
        data = [AgentObject.model_validate(item) for item in page.data]

        async def next_page(cursor: str) -> AsyncCursorPage[AgentObject]:
            return await self.list(
                limit=limit,
                after=cursor,
                before=before,
                order=order,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            )

        return AsyncCursorPage(
            data,
            first_id=page.first_id,
            last_id=page.last_id,
            has_more=page.has_more,
            fetch_next_page=next_page,
        )


class OmnigentClient:
    """Typed Python client for the omnigent server API.

    One-shot::

        async with OmnigentClient(base_url="http://localhost:8080") as client:
            result = await client.query(model="archer", input="hello")
            print(result.text)        # the assistant's reply
            print(result.files)       # any files the agent produced

    Streaming::

        stream = await client.query(model="archer", input="hi", stream=True)
        async for chunk in stream:
            print(chunk, end="", flush=True)
        print(stream.files)            # populated after the stream ends

    Multi-turn conversation::

        session = client.session(model="archer")
        await session.query("hello")
        await session.query("what did I just say?")

    For access to raw events or semantic blocks (tool-call display,
    reasoning, lifecycle), drop to :attr:`responses` or
    :class:`BlockStream`.

    :param base_url: Server base URL, e.g. ``"http://localhost:8080"``.
    :param headers: Extra headers sent on every request (e.g. auth).
    :param auth: Optional ``httpx.Auth`` for per-request
        authentication. When set, the auth handler runs on every
        request, allowing transparent token refresh for OAuth
        flows. ``None`` (default) relies on static ``headers``.
    :param timeout: Default timeout for non-streaming HTTP requests in
        seconds. SSE streams use a 600-second read timeout so server-side
        tool execution can pause the stream for several minutes.

    The client follows 3xx redirects on the configured origin (including
    http→https upgrades on the same host) and refuses cross-origin hops
    with :class:`OmnigentError`, so headers and bodies never leave the
    host the caller configured.
    """

    def __init__(
        self,
        base_url: str,
        *,
        auth: httpx.Auth | tuple[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        cookies: Any = None,
        timeout: float | httpx.Timeout = 30.0,
        max_retries: int = 2,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        self._base_url = base_url.rstrip("/")
        # Announce this as a first-party non-browser client via the sentinel
        # Origin. The server's require_trusted_origin CSRF guard on the
        # multipart routes (POST /v1/sessions bundle create, file upload)
        # requires a trusted Origin; the SDK sends none of its own, so the
        # sentinel is what lets it through. Caller-supplied headers win on
        # conflict (so an explicit Origin override is still honored).
        self._owns_http = http_client is None
        self._http: httpx.AsyncClient
        self._injected_http: httpx.AsyncClient | None = None
        self._injected_client_lease: _InjectedClientLease | None = None
        self._injected_client_policy: _InjectedClientPolicy | None = None
        if http_client is None:
            default_headers = {"Origin": OMNIGENT_INTERNAL_WS_ORIGIN}
            if headers:
                default_headers.update(headers)
            self._http = _AsyncHTTPClient(
                max_connect_retries=max_retries,
                headers=default_headers,
                auth=auth,
                cookies=cookies,
                timeout=timeout,
                # Follow proxy/gateway redirects (3xx) transparently, streams
                # included, but only on the configured origin: the response hook
                # refuses cross-origin hops so headers and bodies never leave
                # the host the caller configured. httpx raises TooManyRedirects
                # on loops.
                follow_redirects=True,
                event_hooks={"response": [refuse_cross_origin_redirect_async]},
                # A proxy cannot reach our loopback server, so bypass the
                # environment for local targets. Loopback is plain HTTP with
                # explicit headers, so losing netrc/CA env with it costs nothing.
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
            self._http = cast(httpx.AsyncClient, self._injected_client_lease)
            self._injected_client_policy = apply_injected_client_policy(
                http_client,
                origin=OMNIGENT_INTERNAL_WS_ORIGIN,
                response_hook=refuse_cross_origin_redirect_async,
            )

        try:
            self.sessions = SessionsNamespace(self._http, self._base_url)
            self.agents = AsyncAgentsResource(self._http, self._base_url, self.sessions)
            self.files = FilesNamespace(self._http, self._base_url)
            self.responses = ResponsesNamespace(self._http, self._base_url)
        except BaseException:
            if self._injected_client_policy is not None:
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

    def session(
        self,
        model: str,
        *,
        tool_handler: ToolHandler | None = None,
        hooks: StreamHooks | None = None,
    ) -> Session:
        """Create a conversation session.

        A session tracks ``previous_response_id`` automatically.
        ``send()`` auto-steers if a response is in progress, or
        starts a new turn if the response is terminal.

        :param model: Agent name.
        :param tool_handler: Optional client-side tool execution config.
        :param hooks: Optional lifecycle hooks.
        :returns: A new :class:`Session`.
        """
        return Session(
            client=self,
            model=model,
            tool_handler=tool_handler,
            hooks=hooks,
        )

    @overload
    async def query(
        self,
        *,
        model: str,
        input: str | list[dict[str, object]],
        tools: list[Callable[..., Any]] | None = ...,
        tool_handler: ToolHandler | None = ...,
        files: list[str] | None = ...,
        reasoning: dict[str, str] | None = ...,
        model_override: str | None = ...,
        stream: Literal[False] = ...,
    ) -> QueryResult: ...

    @overload
    async def query(
        self,
        *,
        model: str,
        input: str | list[dict[str, object]],
        tools: list[Callable[..., Any]] | None = ...,
        tool_handler: ToolHandler | None = ...,
        files: list[str] | None = ...,
        reasoning: dict[str, str] | None = ...,
        model_override: str | None = ...,
        stream: Literal[True],
    ) -> QueryStream: ...

    async def query(
        self,
        *,
        model: str,
        input: str | list[dict[str, object]],
        tools: list[Callable[..., Any]] | None = None,
        tool_handler: ToolHandler | None = None,
        files: list[str] | None = None,
        reasoning: dict[str, str] | None = None,
        model_override: str | None = None,
        stream: bool = False,
    ) -> QueryResult | QueryStream:
        """One-shot invocation: send a prompt, get text (plus any files) back.

        Non-streaming (default) returns a :class:`QueryResult`::

            result = await client.query(model="archer", input="hi")
            print(result.text)
            for f in result.files:
                await client.files.for_session("<session-id>").download(
                    f.id, f"./out/{f.filename}"
                )

        Streaming returns a :class:`QueryStream`::

            stream = await client.query(model="archer", input="hi", stream=True)
            async for chunk in stream:
                print(chunk, end="", flush=True)
            # After iteration, stream.files holds the produced files.

        With client-side tools, pass ``@tool``-decorated functions::

            from omnigent_client import tool

            @tool
            def get_time() -> str:
                '''Return the current time.'''
                return datetime.now().isoformat()

            result = await client.query(
                model="archer", input="what time?", tools=[get_time],
            )

        Creates a single-turn session internally. For multi-turn
        conversations, call :meth:`session` and use its ``query()``.

        :param model: Agent name, e.g. ``"archer"``.
        :param input: User text or a list of content-block dicts.
        :param tools: List of ``@tool``-decorated Python functions
            the agent may call. Mutually exclusive with ``tool_handler``.
        :param tool_handler: Low-level escape hatch — a pre-built
            :class:`ToolHandler` with custom schemas/dispatch. Most
            callers should use ``tools=`` instead.
        :param files: Optional list of local file paths to attach.
        :param reasoning: Optional Responses API reasoning config, e.g. {"effort": "high"}.
        :param model_override: Optional per-request LLM model override
            (e.g. ``"openai/gpt-5.4-mini"``). Shadows the spec model
            for this one-shot call; mirrors
            :meth:`Session.set_model_override`.
        :param stream: If True, return a :class:`QueryStream`. If
            False (default), return a :class:`QueryResult`.
        :returns: :class:`QueryResult` (``stream=False``) or
            :class:`QueryStream` (``stream=True``).
        :raises ValueError: If both ``tools`` and ``tool_handler``
            are provided.
        """
        handler = _resolve_tool_handler(tools=tools, tool_handler=tool_handler)
        session = self.session(model=model, tool_handler=handler)
        effort = reasoning.get("effort") if reasoning is not None else None
        if effort is not None:
            session.set_reasoning_effort(effort)
        if model_override is not None:
            session.set_model_override(model_override)
        if stream:
            return await session.query(input, files=files, stream=True)
        return await session.query(input, files=files)

    @overload
    async def sessions_chat(
        self,
        bundle: bytes,
        *,
        filename: str = "agent.tar.gz",
        tool_callables: dict[str, ToolCallable] | None = None,
        hooks: StreamHooks | None = None,
    ) -> SessionsChat: ...

    @overload
    async def sessions_chat(
        self,
        *,
        agent_id: str,
        filename: Literal["agent.tar.gz"] = "agent.tar.gz",
        title: str | None = None,
        labels: dict[str, str] | None = None,
        reasoning_effort: str | None = None,
        workspace: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        sandbox_provider: str | None = None,
        tool_callables: dict[str, ToolCallable] | None = None,
        hooks: StreamHooks | None = None,
    ) -> SessionsChat: ...

    async def sessions_chat(
        self,
        bundle: bytes | None = None,
        *,
        agent_id: str | None = None,
        filename: str = "agent.tar.gz",
        title: str | None = None,
        labels: dict[str, str] | None = None,
        reasoning_effort: str | None = None,
        workspace: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        sandbox_provider: str | None = None,
        tool_callables: dict[str, ToolCallable] | None = None,
        hooks: StreamHooks | None = None,
    ) -> SessionsChat:
        """Create a sessions-API-native chat helper bound to a new session.

        Counterpart to :meth:`session` but built on ``/v1/sessions``
        rather than ``/v1/responses``. Use this for new code; the
        legacy :meth:`session` is preserved for in-flight migrations.

        :param bundle: Gzipped agent tarball bytes uploaded through
            multipart ``POST /v1/sessions``.
        :param agent_id: Durable id of an agent already registered on the
            server. Pass either ``bundle`` or ``agent_id``, never both.
        :param filename: Filename for the multipart upload, e.g.
            ``"agent.tar.gz"``.
        :param tool_callables: Optional mapping from tool name to
            an executable callable (sync or async) for client-side
            tool execution. Validated against the agent's
            spec-declared tools at stream-start time (the first
            ``send()`` / ``query()`` / ``stream()`` call), not at
            construction. See :class:`SessionsChat` for the
            validation rules.
        :param hooks: Optional lifecycle hooks fired from sessions
            stream events.
        :returns: A :class:`SessionsChat` ready for use.
        :raises OmnigentError: If session creation fails.
        """
        if bundle is not None and agent_id is not None:
            raise ValueError("bundle and agent_id are mutually exclusive")
        if bundle is None and agent_id is None:
            raise ValueError("Pass bundle or agent_id")
        if agent_id is not None:
            if filename != "agent.tar.gz":
                raise ValueError("filename is only valid with bundle")
            return await SessionsChat.create_for_agent(
                namespace=self.sessions,
                agent_id=agent_id,
                title=title,
                labels=labels,
                reasoning_effort=reasoning_effort,
                workspace=workspace,
                host_type=host_type,
                sandbox_provider=sandbox_provider,
                files_namespace=self.files,
                tool_callables=tool_callables,
                agent_tools_getter=self._fetch_agent_tools,
                hooks=hooks,
            )

        registered_options = {
            "title": title,
            "labels": labels,
            "reasoning_effort": reasoning_effort,
            "workspace": workspace,
            "sandbox_provider": sandbox_provider,
        }
        invalid = [name for name, value in registered_options.items() if value is not None]
        if host_type != "external":
            invalid.append("host_type")
        if invalid:
            raise ValueError("bundle sessions_chat does not support: " + ", ".join(invalid))
        assert bundle is not None
        return await SessionsChat.create(
            namespace=self.sessions,
            bundle=bundle,
            filename=filename,
            files_namespace=self.files,
            tool_callables=tool_callables,
            agent_tools_getter=self._fetch_agent_tools,
            hooks=hooks,
        )

    async def _fetch_agent_tools(
        self, agent_id: str, session_id: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetch the spec-declared tool entries for an agent.

        Used as the ``agent_tools_getter`` injection for
        :class:`SessionsChat`. Reads the tool list off the
        :class:`Agent` returned by ``GET /api/agents/{agent_id}``.

        The server's :class:`AgentObject`
        carries a ``tools`` list where each entry has a ``name``
        and a ``runtime`` discriminator
        (``"server"`` or ``"client"``). When that field is not yet
        present, the SDK's :class:`Agent` dataclass simply lacks
        the field and this returns ``[]`` — which means
        validation succeeds for any caller that doesn't pass
        ``tool_callables``, and fails loud (with a clear "extra
        callable" message) for any caller that does. That is the
        correct degraded behavior: in F1's absence we cannot
        verify the spec, but we will never silently accept a
        broken setup.

        :param agent_id: The agent's durable identifier, e.g.
            ``"ag_abc123"``.
        :returns: List of tool-entry dicts with at least ``name``
            and (post-F1) ``runtime`` keys. Empty if the agent
            declares no tools or the server response shape
            predates F1.
        :raises OmnigentError: If the agents endpoint returns
            a non-2xx (e.g. 404).
        """
        if session_id is None:
            return []  # No session context — cannot resolve agent tools
        path = f"{self._base_url}/v1/sessions/{session_id}/agent"
        resp = await self._http.get(path)
        if resp.status_code != 200:
            return []
        agent_data = resp.json()
        tools = agent_data.get("tools")
        if isinstance(tools, list):
            return [t for t in tools if isinstance(t, dict)]
        return []

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._owns_http:
            await self._http.aclose()
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

    async def __aenter__(self) -> OmnigentClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


def _resolve_tool_handler(
    *,
    tools: list[Callable[..., Any]] | None,
    tool_handler: ToolHandler | None,
) -> ToolHandler | None:
    """Pick one of ``tools=`` or ``tool_handler=``; reject both.

    :param tools: High-level list of ``@tool``-decorated functions.
    :param tool_handler: Low-level pre-built handler.
    :returns: The handler to use, or ``None`` if neither was given.
    :raises ValueError: If both were provided.
    """
    if tools is not None and tool_handler is not None:
        raise ValueError(
            "Pass either `tools=[...]` or `tool_handler=...`, not both. "
            "`tools=` is the high-level API (auto-builds a handler from "
            "@tool-decorated functions); `tool_handler=` is the low-level "
            "escape hatch."
        )
    if tools is not None:
        # Local import keeps the dep inside the tools subpackage.
        from .tools import build_tool_handler

        return build_tool_handler(tools)
    return tool_handler
