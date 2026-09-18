"""Sessions namespace — create, snapshot, post events, interrupt, stream.

Targets the server's ``/v1/sessions`` route family. This is a thin
client over the snapshot + live-tail SSE contract documented in
``server/API.md``: callers ``create()`` a session from an agent
bundle, optionally
``post_event()`` more inputs, ``stream()`` the live events, and
``get()`` a snapshot to reconcile on reconnect. There is no replay —
the server intentionally does not buffer past events.

The SDK-side ``Session`` name aliases the canonical
:class:`omnigent.protocol.SessionResponse`. Note that the
``Session`` class exported from :mod:`omnigent_client._session` is
an unrelated higher-level ``/v1/responses`` chat helper; the two
concepts share a name because the server route is ``/v1/sessions``
and the chat helper predates the new route. To avoid surfacing the
collision in the public namespace we deliberately do NOT re-export
this module's ``Session`` from :mod:`omnigent_client.__init__` —
callers obtain it via ``client.sessions.create()``.
"""

from __future__ import annotations

import builtins
import json
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import TypeAdapter

from omnigent.protocol import (
    SERVER_STREAM_EVENT_TYPES,
    PublicSessionEventInput,
    ServerStreamEvent,
    SessionList,
    SessionResponse,
    UnknownEvent,
)
from omnigent.protocol import (
    SessionListItem as ProtocolSessionListItem,
)

from ._child_status import child_summary_busy
from ._errors import OmnigentError, raise_for_status, require_json_object, response_body
from ._timeouts import _SSE_TIMEOUT

# Default recursion cap for the sub-agent tree helpers. Mirrors web's
# ``MAX_TREE_DEPTH`` and the REPL's ``_MAX_SUBAGENT_TREE_DEPTH`` so the SDK
# rollup, the CLI ``↓`` tree, and the web Agents rail all walk the same depth.
_DEFAULT_SUBTREE_DEPTH = 3

# Adapter that validates a single SSE ``data:`` payload against the
# typed discriminated union. Built once at module load — TypeAdapter
# caches the validator. ``ServerStreamEvent`` is a Pydantic-discriminated
# union, so the result of ``validate_python`` is one of the concrete
# event subclasses (CreatedEvent, OutputTextDeltaEvent, …) — see
# :mod:`omnigent.protocol`.
_SERVER_STREAM_EVENT_ADAPTER: TypeAdapter[ServerStreamEvent] = TypeAdapter(ServerStreamEvent)
_PUBLIC_SESSION_EVENT_ADAPTER: TypeAdapter[PublicSessionEventInput] = TypeAdapter(
    PublicSessionEventInput
)

# ── Module-level constants (rule 34) ─────────────────────────────────

_log = logging.getLogger("omnigent_client.sessions")

# Wire literal for the interrupt event ``type`` discriminator. Mirrors
# ``_INTERRUPT_TYPE`` in ``omnigent/server/routes/sessions.py``;
# kept as a module-level constant so :meth:`SessionsNamespace.interrupt`
# matches a single named symbol rather than an inline string.
_INTERRUPT_TYPE: str = "interrupt"

# The server emits this event immediately after registering the live-tail
# subscriber. Consuming it is the only stream-readiness acknowledgement.
_STREAM_READY_EVENT_TYPE: str = "session.heartbeat"


# Private compatibility aliases.  The public root `omnigent_client.Session`
# remains the legacy Responses helper exported from `_session.py`.
SessionEventInput = PublicSessionEventInput
Session = SessionResponse
SessionListItem = ProtocolSessionListItem


@dataclass(frozen=True)
class RegisteredAgent:
    """
    A server-registered agent resolved by display name.

    :param id: The agent's durable identifier, e.g. ``"ag_abc123"``.
    :param harness: Harness the agent runs on, e.g.
        ``"openai-agents"``. ``None`` when the server did not report
        one.
    """

    id: str
    harness: str | None = None


class SessionsNamespace:
    """
    Client namespace for ``/v1/sessions`` endpoints.

    Provides the four operations the route module exposes: create a
    session from an uploaded agent bundle, bind it to a runner, fetch
    a snapshot, post an event into the session's input queue (or an
    interrupt that bypasses it), and live-tail the SSE stream. There
    is no replay; see the module docstring for the snapshot +
    live-tail reconnect contract.

    :param http: Pre-built ``httpx.AsyncClient`` shared with the
        parent :class:`OmnigentClient`. Owned by the parent;
        this namespace must NOT close it.
    :param base_url: Server base URL, e.g.
        ``"http://localhost:8000"``. Trailing slash already stripped
        by the parent client.
    """

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        """
        Initialize the namespace.

        :param http: Shared ``httpx.AsyncClient`` from the parent
            client.
        :param base_url: Server base URL, e.g.
            ``"http://localhost:8000"``.
        """
        self._http = http
        self._base = base_url

    async def create(
        self,
        bundle: bytes,
        *,
        filename: str = "agent.tar.gz",
        title: str | None = None,
        labels: dict[str, str] | None = None,
        reasoning_effort: str | None = None,
        workspace: str | None = None,
        host_type: str = "external",
        sandbox_provider: str | None = None,
    ) -> Session:
        """
        Create a new session from an uploaded agent bundle.

        Calls multipart ``POST /v1/sessions`` with a JSON
        ``metadata`` form part and a ``bundle`` file part. The
        endpoint returns only ``{"session_id": "..."}``, so this
        method immediately fetches ``GET /v1/sessions/{id}`` and
        returns the full typed snapshot.

        :param bundle: Gzipped agent tarball bytes.
        :param filename: Filename sent for the multipart file part,
            e.g. ``"agent.tar.gz"``.
        :param title: Optional human-readable title for the session,
            e.g. ``"debugging auth flow"``.
        :param labels: Initial guardrails labels to set. ``None``
            starts with no labels.
        :param reasoning_effort: Optional per-session reasoning
            effort, e.g. ``"high"``. ``None`` uses the agent default.
        :param workspace: Optional starting workspace. For an external
            host (the default), an absolute cwd to record on the session,
            e.g. ``"/Users/corey/projects/myapp"`` — CLI-launched sessions
            populate this with ``os.getcwd()``. For ``host_type="managed"``,
            a git repository URL (optionally ``#<branch>``) the server
            clones into the sandbox. ``None`` records no workspace.
        :param host_type: ``"external"`` (default) uploads the bundle and
            runs it on a caller-managed runner; ``"managed"`` uploads the
            bundle and has the server provision a sandbox host to run it.
        :param sandbox_provider: With ``host_type="managed"``, which
            configured sandbox provider to provision (e.g. ``"lakebox"``);
            ``None`` takes the server's first. Ignored for external hosts.
        :returns: The newly created :class:`Session` snapshot.
        :raises OmnigentError: If the server returns a non-2xx
            status.
        """
        metadata: dict[str, Any] = {}
        if title is not None:
            metadata["title"] = title
        if labels is not None:
            metadata["labels"] = labels
        if reasoning_effort is not None:
            metadata["reasoning_effort"] = reasoning_effort
        if workspace is not None:
            metadata["workspace"] = workspace
        if host_type != "external":
            metadata["host_type"] = host_type
        if sandbox_provider is not None:
            metadata["sandbox_provider"] = sandbox_provider
        resp = await self._http.post(
            f"{self._base}/v1/sessions",
            data={"metadata": json.dumps(metadata)},
            files={"bundle": (filename, bundle, "application/gzip")},
        )
        raise_for_status(resp.status_code, response_body(resp))
        created = require_json_object(resp, "POST /v1/sessions")
        session_id = str(created["session_id"])
        return await self.get(session_id)

    async def create_from_agent_id(
        self,
        agent_id: str,
        *,
        title: str | None = None,
        labels: dict[str, str] | None = None,
        reasoning_effort: str | None = None,
        workspace: str | None = None,
    ) -> Session:
        """
        Create a new session bound to an already-registered agent.

        Calls JSON ``POST /v1/sessions`` with an ``agent_id`` body. This
        is the path for a client with no local bundle to upload — the
        agent is already registered on the server, as when connecting to
        a remote URL. Unlike :meth:`create`, the JSON route returns the
        full session snapshot, so no follow-up ``GET`` is needed.

        :param agent_id: Durable identifier of a registered agent, e.g.
            ``"ag_abc123"``.
        :param title: Optional human-readable title for the session,
            e.g. ``"debugging auth flow"``.
        :param labels: Initial guardrails labels to set. ``None``
            starts with no labels.
        :param reasoning_effort: Optional per-session reasoning
            effort, e.g. ``"high"``. ``None`` uses the agent default.
        :param workspace: Optional absolute starting cwd to record on
            the session, e.g. ``"/Users/corey/projects/myapp"``.
        :returns: The newly created :class:`Session` snapshot.
        :raises OmnigentError: If the server returns a non-2xx
            status.
        """
        body: dict[str, Any] = {"agent_id": agent_id}
        if title is not None:
            body["title"] = title
        if labels is not None:
            body["labels"] = labels
        if reasoning_effort is not None:
            body["reasoning_effort"] = reasoning_effort
        if workspace is not None:
            body["workspace"] = workspace
        resp = await self._http.post(f"{self._base}/v1/sessions", json=body)
        raise_for_status(resp.status_code, response_body(resp))
        created = require_json_object(resp, "POST /v1/sessions")
        return Session.model_validate(created)

    async def resolve_agent(self, agent_name: str) -> RegisteredAgent:
        """
        Resolve a registered agent's id and harness from its display name.

        Used by clients that only know the name the user picked — the
        remote-URL chat picker lists names, but session creation binds
        by id. ``GET /v1/agents`` lists only server-registered
        (``session_id IS NULL``) agents, so a session-scoped agent is
        not resolvable this way and raises ``LookupError``.

        Follows the listing cursor, so an agent past the first page
        still resolves.

        :param agent_name: Agent display name, e.g. ``"hello_world"``.
        :returns: The matching agent's id and advertised harness.
        :raises OmnigentError: If the listing returns a non-2xx status.
        :raises LookupError: If no registered agent has that name.
        """
        # Cap the miss-path name list: a large deployment should not
        # build thousands of names just to render one error message.
        names: list[str] = []
        truncated = False
        after: str | None = None
        while True:
            params: dict[str, str | int] = {"limit": 1000}
            if after is not None:
                params["after"] = after
            resp = await self._http.get(f"{self._base}/v1/agents", params=params)
            raise_for_status(resp.status_code, response_body(resp))
            listing = require_json_object(resp, "GET /v1/agents")
            data = listing.get("data", [])
            for item in data if isinstance(data, list) else []:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                if name == agent_name:
                    harness = item.get("harness")
                    return RegisteredAgent(
                        id=str(item["id"]),
                        harness=str(harness) if isinstance(harness, str) else None,
                    )
                if isinstance(name, str):
                    if len(names) < 50:
                        names.append(name)
                    else:
                        truncated = True
            if not listing.get("has_more"):
                break
            last_id = listing.get("last_id")
            if not last_id:
                break
            after = str(last_id)
        available = ", ".join(names) + (", …" if truncated else "")
        raise LookupError(
            f"No agent named {agent_name!r} is registered on this server. Available: {available}"
        )

    async def resolve_online_runner(
        self,
        *,
        harness: str | None = None,
        canonicalize: Callable[[str], str] | None = None,
    ) -> str | None:
        """
        Find an online runner on the server that can drive *harness*.

        A remote-URL client has no local runner of its own, but the
        session still needs one bound before a turn can dispatch.
        ``GET /v1/runners`` lists the online runners owned by the
        requesting user along with the harnesses each advertises, so
        the client can bind to one the server already has.

        :param harness: Harness the session needs, e.g.
            ``"openai-agents"``. ``None`` accepts any online runner.
        :param canonicalize: Optional harness-name normalizer applied to
            both sides of the comparison, so a spec spelling that is an
            alias (``"claude"``) still matches a runner advertising the
            canonical name (``"claude-sdk"``). Mirrors the server's own
            matching. ``None`` compares the names as given.
        :returns: A matching runner id, or ``None`` when the server has
            no online runner that advertises *harness*.
        :raises OmnigentError: If the listing returns a non-2xx status.
        """
        resp = await self._http.get(f"{self._base}/v1/runners")
        raise_for_status(resp.status_code, response_body(resp))
        listing = require_json_object(resp, "GET /v1/runners")
        data = listing.get("data", [])
        # Accept either spelling on either side, as the server does.
        wanted = {harness} if harness is not None else set()
        if harness is not None and canonicalize is not None:
            wanted.add(canonicalize(harness))
        # Prefer a runner that advertises the harness; fall back to any
        # online runner that didn't report its harness list at all.
        unknown_harness: str | None = None
        for item in data if isinstance(data, list) else []:
            if not isinstance(item, dict) or not item.get("online"):
                continue
            runner_id = item.get("runner_id")
            if not isinstance(runner_id, str) or not runner_id:
                continue
            advertised = item.get("harnesses")
            if not isinstance(advertised, list):
                unknown_harness = unknown_harness or runner_id
                continue
            if not wanted:
                return runner_id
            names = {name for name in advertised if isinstance(name, str)}
            if canonicalize is not None:
                names |= {canonicalize(name) for name in names}
            if wanted & names:
                return runner_id
        return unknown_harness

    async def list(
        self,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        agent_id: str | None = None,
        agent_name: str | None = None,
        order: str = "desc",
        sort_by: str = "created_at",
        include_archived: bool = False,
    ) -> list[SessionListItem]:
        """
        List sessions with cursor-based pagination.

        Calls ``GET /v1/sessions``. Returns only sessions (conversations
        with an agent binding), not legacy conversations.

        :param limit: Maximum number of sessions to return
            (1-1000, default 20).
        :param after: Cursor — return sessions after this session ID.
        :param before: Cursor — return sessions before this session ID.
        :param agent_id: Filter to sessions bound to this agent,
            e.g. ``"ag_abc123"``. ``None`` returns all agents.
        :param agent_name: Filter to sessions whose bound agent row
            has this name, including distinct session-scoped agents
            that share the name. ``None`` returns all names.
        :param order: Sort direction, ``"desc"`` or ``"asc"``.
        :param sort_by: Column to sort on, ``"created_at"`` or
            ``"updated_at"``.
        :param include_archived: When ``False`` (default), archived
            sessions are omitted. When ``True``, archived sessions are
            returned alongside active ones.
        :returns: List of :class:`SessionListItem`.
        :raises StaleCursorError: If ``after``/``before`` names a session
            that has since been deleted. The walk cannot continue from
            that cursor — restart it from the first page with no cursor.
        :raises OmnigentError: On non-2xx status.
        """
        params: dict[str, str | int] = {"limit": limit, "order": order, "sort_by": sort_by}
        if after is not None:
            params["after"] = after
        if before is not None:
            params["before"] = before
        if agent_id is not None:
            params["agent_id"] = agent_id
        if agent_name is not None:
            params["agent_name"] = agent_name
        if include_archived:
            params["include_archived"] = "true"
        resp = await self._http.get(
            f"{self._base}/v1/sessions",
            params=params,
        )
        raise_for_status(resp.status_code, response_body(resp))
        body = require_json_object(resp, "GET /v1/sessions")
        return SessionList.model_validate(body).data

    async def bind_runner(
        self,
        session_id: str,
        *,
        runner_id: str,
    ) -> Session:
        """
        Bind or rebind a session to a registered runner.

        Calls ``PATCH /v1/sessions/{session_id}`` with
        ``{"runner_id": "..."}``. This is last-write-wins and
        replaces any prior binding.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :param runner_id: Registered runner id, e.g.
            ``"runner_abc123"``.
        :returns: The updated :class:`Session` snapshot.
        :raises OmnigentError: On non-2xx status (404 when the
            session does not exist, 400 when the runner is not
            registered).
        """
        resp = await self._http.patch(
            f"{self._base}/v1/sessions/{session_id}",
            json={"runner_id": runner_id},
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(
            require_json_object(resp, "PATCH /v1/sessions/{session_id}"),
        )

    async def unbind_runner(self, session_id: str) -> Session:
        """
        Clear a session's runner binding.

        PATCHes ``{"runner_id": ""}`` (the server's clear sentinel;
        ``None`` means "leave unchanged"). Counterpart to
        :meth:`bind_runner` for the 1:1 session↔runner invariant.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :returns: The updated :class:`Session` snapshot.
        :raises OmnigentError: On non-2xx status (404 when the
            session does not exist).
        """
        resp = await self._http.patch(
            f"{self._base}/v1/sessions/{session_id}",
            json={"runner_id": ""},
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(
            require_json_object(resp, "PATCH /v1/sessions/{session_id}"),
        )

    async def set_reasoning_effort(
        self,
        session_id: str,
        *,
        reasoning_effort: str | None,
    ) -> Session:
        """
        Set or clear a session's reasoning-effort metadata.

        Calls ``PATCH /v1/sessions/{session_id}`` with
        ``{"reasoning_effort": "..."}``. ``None`` is sent as the
        server's explicit clear alias because omitted or JSON-null
        fields leave the current value unchanged.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :param reasoning_effort: New effort, e.g. ``"high"``, or
            ``None`` to clear to the agent default.
        :returns: The updated :class:`Session` snapshot.
        :raises OmnigentError: On non-2xx status.
        """
        wire_effort = reasoning_effort if reasoning_effort is not None else "default"
        resp = await self._http.patch(
            f"{self._base}/v1/sessions/{session_id}",
            json={"reasoning_effort": wire_effort},
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(
            require_json_object(resp, "PATCH /v1/sessions/{session_id}"),
        )

    async def set_model_override(
        self,
        session_id: str,
        *,
        model_override: str | None,
        silent: bool = False,
    ) -> Session:
        """
        Set or clear a session's LLM model override.

        Calls ``PATCH /v1/sessions/{session_id}`` with
        ``{"model_override": "..."}``. ``None`` is sent as the
        server's explicit clear alias because omitted or JSON-null
        fields leave the current value unchanged.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :param model_override: New model identifier, e.g.
            ``"claude-opus-4-7"``, or ``None`` to clear to the
            agent default.
        :param silent: When ``True``, persist without triggering the
            claude-native ``/model`` slash-command forward into the
            tmux pane. Use for bind-time auto-apply (e.g. the REPL's
            pre-create ``/model`` snapshot) where the visible
            slash-command item would look like an unexpected first
            message in the chat. Default ``False`` matches the
            user-driven ``/model`` flow where the live forward is the
            desired feedback.
        :returns: The updated :class:`Session` snapshot.
        :raises OmnigentError: On non-2xx status (400 on invalid
            input, 404 when the session does not exist).
        """
        wire_model = model_override if model_override is not None else "default"
        body: dict[str, object] = {"model_override": wire_model}
        if silent:
            body["silent"] = True
        resp = await self._http.patch(
            f"{self._base}/v1/sessions/{session_id}",
            json=body,
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(
            require_json_object(resp, "PATCH /v1/sessions/{session_id}"),
        )

    async def set_archived(
        self,
        session_id: str,
        *,
        archived: bool,
    ) -> Session:
        """
        Archive or unarchive a session.

        Calls ``PATCH /v1/sessions/{session_id}`` with
        ``{"archived": ...}``. Archived sessions are hidden from the
        default :meth:`list` listing and surfaced only with
        ``include_archived=True``. Owner-only (the web UI stops the
        session on archive, an owner-gated lifecycle action, so archive
        is held to the same gate); note this method only flips the
        archived flag — it does not stop the session.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :param archived: ``True`` to archive, ``False`` to unarchive.
        :returns: The updated :class:`Session` snapshot.
        :raises OmnigentError: On non-2xx status (403 without owner
            access, 404 when the session does not exist).
        """
        resp = await self._http.patch(
            f"{self._base}/v1/sessions/{session_id}",
            json={"archived": archived},
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(
            require_json_object(resp, "PATCH /v1/sessions/{session_id}"),
        )

    async def set_external_session_id(
        self,
        session_id: str,
        *,
        external_session_id: str,
    ) -> Session:
        """
        Record the runtime-native session id this session wraps.

        Calls ``PATCH /v1/sessions/{session_id}`` with
        ``{"external_session_id": "..."}``. Captured by a wrapper
        bridge from the underlying runtime (Claude Code, Codex,
        Pi, ...). Idempotent on same-value writes; the server
        returns ``400 invalid_input`` on attempted overwrite of
        a different existing value.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :param external_session_id: Runtime-native session id,
            e.g. a Claude Code session uuid
            ``"a1b2c3d4-1234-5678-9abc-def012345678"``.
        :returns: The updated :class:`Session` snapshot.
        :raises OmnigentError: On non-2xx status (400 on
            overwrite conflict, 404 when the session does not
            exist).
        """
        resp = await self._http.patch(
            f"{self._base}/v1/sessions/{session_id}",
            json={"external_session_id": external_session_id},
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(
            require_json_object(resp, "PATCH /v1/sessions/{session_id}"),
        )

    async def list_items(
        self,
        session_id: str,
        *,
        limit: int = 100,
        after: str | None = None,
        order: str = "asc",
    ) -> builtins.list[dict[str, Any]]:
        """
        List items in a session with cursor-based pagination.

        Calls ``GET /v1/sessions/{session_id}/items``. Same
        pagination contract as ``GET /v1/conversations/{id}/items``.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :param limit: Maximum number of items to return
            (1-1000, default 100).
        :param after: Cursor — return items after this item ID.
        :param order: Sort order, ``"asc"`` (chronological) or
            ``"desc"``.
        :returns: List of conversation item dicts.
        :raises StaleCursorError: If ``after`` names an item that has since
            been deleted. The walk cannot continue from that cursor —
            restart it from the first page with no cursor.
        :raises OmnigentError: On non-2xx status (404 when the
            session does not exist).
        """
        params: dict[str, str | int] = {"limit": limit, "order": order}
        if after is not None:
            params["after"] = after
        resp = await self._http.get(
            f"{self._base}/v1/sessions/{session_id}/items",
            params=params,
        )
        raise_for_status(resp.status_code, response_body(resp))
        body = require_json_object(resp, "GET /v1/sessions/{session_id}/items")
        data = body.get("data", [])
        return data if isinstance(data, list) else []

    async def child_sessions(
        self,
        session_id: str,
        *,
        limit: int = 100,
    ) -> builtins.list[dict[str, Any]]:
        """
        List sub-agent (child) sessions under a parent session.

        Calls ``GET /v1/sessions/{session_id}/child_sessions`` and
        returns a page of ``ChildSessionSummary`` dicts (``id``,
        ``title``, ``tool``, ``agent_name``, ``busy``,
        ``current_task_status``, ``last_message_preview``,
        ``pending_elicitations_count``, …). The REPL recurses this per
        node to assemble the sub-agent tree shown on the main interface.

        :param session_id: Parent session/conversation identifier,
            e.g. ``"conv_parent123"``.
        :param limit: Maximum number of children to return
            (1-1000, default 100).
        :returns: List of child-session summary dicts (empty when the
            session has no sub-agents).
        :raises OmnigentError: On non-2xx status (404 when the
            session does not exist).
        """
        resp = await self._http.get(
            f"{self._base}/v1/sessions/{session_id}/child_sessions",
            params={"limit": limit},
        )
        raise_for_status(resp.status_code, response_body(resp))
        body = require_json_object(resp, "GET /v1/sessions/{session_id}/child_sessions")
        data = body.get("data", [])
        return data if isinstance(data, list) else []

    async def child_sessions_tree(
        self,
        session_id: str,
        *,
        max_depth: int = _DEFAULT_SUBTREE_DEPTH,
        limit: int = 100,
    ) -> builtins.list[dict[str, Any]]:
        """List the whole sub-agent subtree under *session_id*, flattened.

        :meth:`child_sessions` is one level deep; this recurses it breadth-first
        to *max_depth*, mirroring web's ``useChildSessions`` per-node fetch.
        Each returned row is the raw ``ChildSessionSummary`` dict with an added
        ``parent_id`` recording the session it was queried under, so callers can
        reconstruct the hierarchy. *session_id* itself is not included.

        This is the shared recursion behind both the CLI ``↓`` sub-agent tree
        (the REPL seeds its registry from this) and :meth:`subtree_busy`.

        :param session_id: Root parent session identifier.
        :param max_depth: Levels to descend (1 = direct children only). Capped
            to match the CLI tree and the web Agents rail.
        :param limit: Per-level page size passed to :meth:`child_sessions`.
        :returns: Flattened list of child-session summary dicts, each carrying a
            ``parent_id`` key (empty when the session has no sub-agents).
        :raises OmnigentError: On non-2xx status (404 when the session does not
            exist).
        """
        nodes: list[dict[str, Any]] = []
        seen: set[str] = {session_id}
        frontier: list[str] = [session_id]
        depth = 0
        while frontier and depth < max_depth:
            next_frontier: list[str] = []
            for parent_id in frontier:
                rows = await self.child_sessions(parent_id, limit=limit)
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    sid = row.get("id")
                    if not isinstance(sid, str) or sid in seen:  # cycle / dupe guard
                        continue
                    seen.add(sid)
                    nodes.append({**row, "parent_id": parent_id})
                    next_frontier.append(sid)
            frontier = next_frontier
            depth += 1
        return nodes

    async def subtree_busy(
        self,
        session_id: str,
        *,
        max_depth: int = _DEFAULT_SUBTREE_DEPTH,
        limit: int = 100,
    ) -> bool:
        """Whether any sub-agent anywhere under *session_id* is still working.

        The queryable rollup an SDK driver needs: a parent's own ``status`` is
        per-session and reads ``idle`` once it delegates and returns to its own
        prompt, even while its sub-agents run. This recurses the subtree
        (:meth:`child_sessions_tree`) and applies the canonical
        :func:`omnigent_client.child_summary_busy` predicate — the same "busy"
        definition the CLI badge and the web ``SubagentsPanel`` use — so an
        eval loop can gate "your turn" on real subtree activity.

        Point-in-time (no subscription); re-call for a fresh value.

        :param session_id: Root parent session identifier.
        :param max_depth: Levels to descend (see :meth:`child_sessions_tree`).
        :param limit: Per-level page size.
        :returns: ``True`` while any descendant is busy, else ``False``.
        :raises OmnigentError: On non-2xx status.
        """
        nodes = await self.child_sessions_tree(session_id, max_depth=max_depth, limit=limit)
        return any(child_summary_busy(node) for node in nodes)

    async def get(self, session_id: str) -> Session:
        """
        Fetch the current snapshot of a session.

        Calls ``GET /v1/sessions/{session_id}``. The returned
        :class:`Session` includes committed items and any pending
        queued inputs — clients use this on reconnect to reconcile
        state observed via :meth:`stream`.

        :param session_id: Session/conversation identifier,
            e.g. ``"conv_abc123"``.
        :returns: The current :class:`Session` snapshot.
        :raises OmnigentError: If the server returns a non-2xx
            status (404 when the session does not exist).
        """
        resp = await self._http.get(
            f"{self._base}/v1/sessions/{session_id}",
        )
        raise_for_status(resp.status_code, response_body(resp))
        return Session.model_validate(require_json_object(resp, "GET /v1/sessions/{session_id}"))

    async def post_event(
        self,
        session_id: str,
        event: PublicSessionEventInput | dict[str, Any],
    ) -> dict[str, Any]:
        """
        Post an event/input item to a running session.

        Calls ``POST /v1/sessions/{session_id}/events``. The body
        is a single event dict matching :class:`SessionEventInput`
        on the wire. The server returns 202 with a small ack body
        (``{"queued": true, "item_id": "..."}`` for persisted
        item events; ``{"queued": false}`` for interrupt / approval).

        :param session_id: Session/conversation identifier, e.g.
            ``"conv_abc123"``.
        :param event: A public event model or raw payload, e.g.
            ``{"type": "message", "data": {"role": "user",
            "content": [{"type": "input_text",
            "text": "Hello"}]}}``. Raw dictionaries are validated
            against the public ``message`` / ``function_call_output`` /
            ``interrupt`` allowlist before network I/O.
        :raises OmnigentError: If the server returns a non-2xx
            status (404 when the session does not exist).
        """
        validated = _PUBLIC_SESSION_EVENT_ADAPTER.validate_python(event)
        payload = validated.model_dump(mode="json", by_alias=True, exclude_none=True)
        return await self._post_event_payload(session_id, payload)

    async def _post_event_payload(
        self,
        session_id: str,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """Post an already-validated public or SDK-composed control event."""
        resp = await self._http.post(
            f"{self._base}/v1/sessions/{session_id}/events",
            json=event,
        )
        raise_for_status(resp.status_code, response_body(resp))
        return require_json_object(resp, "POST /v1/sessions/{session_id}/events")

    async def resolve_elicitation(
        self,
        session_id: str,
        elicitation_id: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Resolve an outstanding elicitation via its dedicated URL.

        Calls ``POST /v1/sessions/{session_id}/elicitations/
        {elicitation_id}/resolve`` with the MCP-shape
        ``ElicitationResult`` body — the URL-based counterpart to
        delivering the verdict as a ``{"type": "approval", ...}``
        event through :meth:`post_event`. The elicitation id travels
        in the URL path; the body carries only ``action`` (and
        optional form ``content``). Both paths converge on the same
        server-side resolver, so the effect is identical; routing
        through the URL keeps human approval on a dedicated,
        owner-gated path rather than an in-band session event.

        :param session_id: Session/conversation identifier, e.g.
            ``"conv_abc123"``.
        :param elicitation_id: Correlation id of the elicitation to
            resolve, e.g. ``"elicit_abc123"``.
        :param result: MCP ``ElicitationResult`` body, e.g.
            ``{"action": "accept"}`` or ``{"action": "accept",
            "content": {"choice": "a"}}``. ``action`` is one of
            ``"accept"`` / ``"decline"`` / ``"cancel"``.
        :returns: The server ack dict (``{"queued": false}``).
        :raises OmnigentError: If the server returns a non-2xx
            status (404 when the session does not exist).
        """
        resp = await self._http.post(
            f"{self._base}/v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
            json=result,
        )
        raise_for_status(resp.status_code, response_body(resp))
        return require_json_object(
            resp,
            f"POST /v1/sessions/{session_id}/elicitations/{elicitation_id}/resolve",
        )

    async def fork(
        self,
        source_session_id: str,
        *,
        title: str | None = None,
        up_to_response_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Fork an existing session into a new session.

        Calls ``POST /v1/sessions/{source_session_id}/fork``.
        Deep-copies the source session's items into a new session
        with the same agent binding.

        :param source_session_id: ID of the session to fork, e.g.
            ``"conv_abc123"``.
        :param title: Optional title for the forked session. When
            ``None``, the server derives one from the source.
        :param up_to_response_id: Optional truncation point, e.g.
            ``"resp_abc123"``. When set, the fork copies history only
            up to and including that response; ``None`` copies the
            full history.
        :returns: Raw response dict matching the ``SessionResponse``
            shape: ``id``, ``agent_id``, ``status``, ``created_at``,
            ``title``, ``labels``, ``reasoning_effort``, and
            ``items``.
        :raises OmnigentError: 404 if *source_session_id* does
            not exist; 400 if the source has no agent binding or
            *up_to_response_id* names no response in the source.
        """
        body: dict[str, Any] = {}
        if title is not None:
            body["title"] = title
        if up_to_response_id is not None:
            body["up_to_response_id"] = up_to_response_id
        resp = await self._http.post(
            f"{self._base}/v1/sessions/{source_session_id}/fork",
            json=body,
        )
        raise_for_status(resp.status_code, response_body(resp))
        return require_json_object(
            resp,
            f"POST /v1/sessions/{source_session_id}/fork",
        )

    async def compact(self, session_id: str) -> None:
        """
        Request explicit context compaction for a session.

        Convenience wrapper over :meth:`post_event` that posts a
        ``{"type": "compact", "data": {}}`` control event. The server
        runs compaction without appending a user message or starting a
        normal agent turn.

        :param session_id: Session/conversation identifier, e.g.
            ``"conv_abc123"``.
        :raises OmnigentError: If the server returns a non-2xx status.
        """
        await self._post_event_payload(
            session_id,
            {"type": "compact", "data": {}},
        )

    async def interrupt(self, session_id: str) -> None:
        """
        Interrupt a running session.

        Convenience wrapper over :meth:`post_event` that posts an
        ``{"type": "interrupt", "data": {}}`` event. The server
        bypasses the input queue and cancels the loop directly,
        co-emitting ``response.incomplete`` (``reason=
        "user_interrupt"``) and ``session.interrupted`` on the
        stream.

        :param session_id: Session/conversation identifier, e.g.
            ``"conv_abc123"``.
        :raises OmnigentError: If the server returns a non-2xx
            status (404 when the session does not exist).
        """
        await self.post_event(
            session_id,
            {"type": _INTERRUPT_TYPE, "data": {}},
        )

    async def stream(
        self,
        session_id: str,
    ) -> AsyncIterator[ServerStreamEvent | UnknownEvent]:
        """
        Live-tail the session's SSE event stream.

        Calls ``GET /v1/sessions/{session_id}/stream``. Yields one
        :class:`ServerStreamEvent` per server event in arrival order. The
        server does NOT replay history; on reconnect, callers should
        open a new stream and reconcile via :meth:`get`.

        Iteration ends cleanly when the server closes the stream
        (the ``[DONE]`` sentinel). Network errors propagate to the
        caller — auto-reconnect lives at the application layer
        because the snapshot/dedupe step is application-specific.

        :param session_id: Session/conversation identifier, e.g.
            ``"conv_abc123"``.
        :yields: Known :class:`omnigent.protocol.ServerStreamEvent`
            values or :class:`omnigent.protocol.UnknownEvent` for a
            newer discriminator.
        :raises OmnigentError: If the server returns a non-2xx
            status when opening the stream (404 when the session
            does not exist).
        """
        async for event in _stream_session_events(
            self._http,
            self._base,
            session_id,
        ):
            yield event

    async def _open_stream_ready(
        self,
        session_id: str,
    ) -> AsyncIterator[ServerStreamEvent | UnknownEvent]:
        """Open a session stream and consume its registration heartbeat.

        Advancing :meth:`stream` establishes
        ``GET /v1/sessions/{session_id}/stream``. The server registers the
        subscriber before immediately emitting ``session.heartbeat``; only
        after that event has been consumed is it safe for a caller to submit
        input without racing the live tail. This helper performs no writes,
        tool dispatch, hooks, or orchestration.

        The returned iterator starts after the readiness heartbeat and remains
        caller-owned. A stream that closes or yields another event first is a
        protocol failure and is closed before the error is raised.

        :param session_id: Session/conversation identifier.
        :returns: The already-open event iterator, positioned after readiness.
        :raises OmnigentError: If the stream closes or emits another event
            before its registration heartbeat.
        """
        stream_aiter = self.stream(session_id).__aiter__()
        try:
            ready_event = await stream_aiter.__anext__()
            if ready_event.type != _STREAM_READY_EVENT_TYPE:
                raise OmnigentError(
                    "session stream did not begin with the required "
                    f"{_STREAM_READY_EVENT_TYPE!r} readiness event; "
                    f"received {ready_event.type!r}"
                )
        except StopAsyncIteration as exc:
            await _aclose_stream(stream_aiter)
            raise OmnigentError(
                "session stream closed before the required "
                f"{_STREAM_READY_EVENT_TYPE!r} readiness event"
            ) from exc
        except BaseException:
            await _aclose_stream(stream_aiter)
            raise
        return stream_aiter


# ── Helpers ─────────────────────────────────────────────────────────


async def _stream_session_events(
    http: httpx.AsyncClient,
    base_url: str,
    session_id: str,
) -> AsyncIterator[ServerStreamEvent | UnknownEvent]:
    """
    Open a single SSE connection and yield parsed
    :class:`ServerStreamEvent` instances.

    Does NOT handle reconnection — that is the caller's
    responsibility. Network errors (``httpx.RemoteProtocolError``,
    ``httpx.ReadTimeout``, etc.) propagate.

    :param http: Shared ``httpx.AsyncClient``.
    :param base_url: Server base URL, e.g.
        ``"http://localhost:8000"``.
    :param session_id: Session/conversation identifier whose stream
        to subscribe to, e.g. ``"conv_abc123"``.
    :yields: :class:`ServerStreamEvent` envelopes parsed from the SSE
        ``data:`` payload.
    :raises OmnigentError: If the stream open fails with a non-2xx
        status, including a redirect that was not followed.
    :raises httpx.TooManyRedirects: If on-origin redirects loop past
        httpx's limit.
    """
    async with http.stream(
        "GET",
        f"{base_url}/v1/sessions/{session_id}/stream",
        timeout=_SSE_TIMEOUT,
    ) as resp:
        if resp.status_code >= 400:
            await resp.aread()
            raise_for_status(resp.status_code, response_body(resp))
        elif 300 <= resp.status_code < 400:
            # OmnigentClient follows redirects, so a 3xx here was not
            # followable (no Location header, a 304, or a caller-supplied
            # client with redirects disabled). Parsing its non-SSE body
            # would yield a silent, error-free, empty stream — fail loud
            # instead.
            raise OmnigentError(
                f"stream open returned a 3xx response (status {resp.status_code}) "
                "instead of an event stream",
                resp.status_code,
            )

        async for event in _parse_sse_lines(resp.aiter_lines()):
            yield event


async def _parse_sse_lines(
    line_stream: AsyncIterator[str],
) -> AsyncIterator[ServerStreamEvent | UnknownEvent]:
    """
    Parse raw SSE text lines into :class:`ServerStreamEvent` instances.

    Expects the standard SSE framing emitted by the server's
    ``_format_sse`` helper: ``event: <type>`` followed by
    ``data: <json>``, separated by blank lines. The ``[DONE]``
    sentinel terminates the stream cleanly. Each ``data:`` JSON
    payload is the full envelope dict ``{"type": ..., "data": ...}``
    that the server publishes; we feed it into Pydantic to enforce
    the :class:`ServerStreamEvent` discriminator.

    Malformed payloads (non-JSON, non-dict, or invalid known-event
    shapes) are logged and skipped so a single bad event does not poison
    the stream. Unknown event discriminators are yielded as
    :class:`UnknownEvent` values instead of being dropped.

    :param line_stream: Async iterator of text lines from
        ``httpx.Response.aiter_lines()``.
    :yields: Parsed :class:`ServerStreamEvent` instances.
    """
    current_event: str | None = None

    async for line in line_stream:
        line = line.rstrip("\r\n")

        if line.startswith("event: "):
            current_event = line[7:]
        elif line.startswith("data: ") and current_event is not None:
            data_str = line[6:]
            if data_str.strip() == "[DONE]":
                return
            parsed = _try_parse_envelope(data_str)
            if parsed is not None:
                yield parsed
            current_event = None
        elif line == "":
            current_event = None


def _try_parse_envelope(raw: str) -> ServerStreamEvent | UnknownEvent | None:
    """
    Parse a single SSE ``data:`` payload into a typed
    :data:`ServerStreamEvent`.

    The server emits each event with a flat shape carrying the
    fields documented on the matching subclass in
    :mod:`omnigent.protocol` (e.g. ``{"type":
    "response.output_text.delta", "delta": "Hello",
    "sequence_number": 5}``). The
    :data:`_SERVER_STREAM_EVENT_ADAPTER` dispatches on ``type`` to
    the right concrete model. Unknown event names become an
    :class:`UnknownEvent` that retains the decoded payload.

    :param raw: Raw JSON string from an SSE ``data:`` field, e.g.
        ``'{"type": "response.output_text.delta", "delta": "Hi"}'``.
    :returns: A typed known event, an :class:`UnknownEvent`, or
        ``None`` when the payload is malformed.
    """
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        _log.warning("Failed to parse SSE data: %s", raw[:200])
        return None
    if not isinstance(decoded, dict):
        _log.warning("SSE data is not a JSON object: %s", raw[:200])
        return None
    event_type = decoded.get("type")
    if isinstance(event_type, str) and event_type not in SERVER_STREAM_EVENT_TYPES:
        return UnknownEvent(type=event_type, raw=decoded)
    try:
        return _SERVER_STREAM_EVENT_ADAPTER.validate_python(decoded)
    except ValueError as exc:
        # ValueError covers Pydantic ValidationError (a subclass) for
        # missing discriminators and invalid or missing required fields.
        # Log + skip so a single bad event does not abort the
        # iteration; the caller still observes every well-formed
        # event in arrival order.
        _log.debug("Skipping unparseable session event: %s (%s)", raw[:200], exc)
        return None


async def _aclose_stream(
    iterator: AsyncIterator[ServerStreamEvent | UnknownEvent],
) -> None:
    """Close an iterator returned by :meth:`SessionsNamespace.stream`."""
    aclose = getattr(iterator, "aclose", None)
    assert aclose is not None, (
        "SessionsNamespace.stream() must return an async generator "
        "exposing aclose(); got an iterator without it"
    )
    await aclose()
