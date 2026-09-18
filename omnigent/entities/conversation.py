"""Conversation entities — conversation, items, and item data types."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from omnigent.inner.native_attachments import UNRESOLVED_ATTACHMENT_MARKER_PATTERN
from omnigent.protocol import (  # noqa: F401 - compatibility re-exports
    ITEM_TYPE_TO_DATA_CLS,
    USER_SESSION_TITLE_MAX_CHARS,
    CompactionData,
    ConversationItem,
    ErrorData,
    FunctionCallData,
    FunctionCallOutputData,
    ItemData,
    MessageData,
    NativeToolData,
    NewConversationItem,
    ReasoningData,
    ResourceEventData,
    RoutingDecisionData,
    SlashCommandData,
    TerminalCommandData,
    parse_item_data,
)
from omnigent.protocol.sessions import (  # noqa: F401 - compatibility re-exports
    _binary_payload_omitted,
    _validate_type_matches_data,
)

# Attachment markers the native executors prepend to prompt text
# ("[Attached: /tmp/.../x.png]" from claude-native's _content_to_text,
# "[Attached file: /tmp/...]" from codex-native's _file_block_to_input_item,
# "[Attachment <name> could not be loaded]" from native_attachments'
# unresolved_attachment_marker).
# Those markers round-trip through the vendor transcript as user-message
# text, so without filtering them a session started with an image is
# titled by a temp-file path instead of what the user typed. Matched per
# line by synthesize_conversation_title; keep the Attached variants in
# sync with attachment_reference_line in omnigent/inner/native_attachments.py
# and omnigent/inner/codex_native_executor.py.
_ATTACHMENT_MARKER_RE = re.compile(
    rf"^(?:\[Attached(?: file)?: .+\]|{UNRESOLVED_ATTACHMENT_MARKER_PATTERN})$"
)

# Generated titles stay compact by default, while explicit user formats and
# manually assigned titles have room for structured identifiers.
DEFAULT_GENERATED_TITLE_MAX_CHARS = 100

# ── Conversation ──────────────────────────────────────


@dataclass
class Conversation:
    """
    A conversation grouping related turns.

    :param id: Unique conversation identifier,
        e.g. ``"conv_abc123"``.
    :param created_at: Unix epoch timestamp of creation.
    :param updated_at: Unix epoch timestamp of the last
        update (item append, title change, etc.).
    :param title: Optional user-assigned title. Phase 4 named
        sub-agents store ``"<type>:<name>"`` here so the partial
        unique index on ``(parent_conversation_id, title)`` can
        enforce uniqueness within a parent.
    :param kind: Conversation type. ``"default"`` for
        user-initiated, ``"sub_agent"`` for sub-agent
        execution conversations.
    :param parent_conversation_id: Phase 4 — for child
        sub-agent conversations, points at the owning parent
        conversation. ``None`` for top-level conversations.
    :param root_conversation_id: For child sub-agent
        conversations, the id of the root (top-level) conversation
        in the spawn tree. Equal to ``id`` for top-level
        conversations. Powers O(1) tree-scoped lookups so any
        agent in a tree can peek at any other by
        ``conversation_id`` without walking the parent chain.
    :param agent_id: Foreign key to the agent bound to this
        conversation at creation time. ``None`` only for legacy
        rows or callers that cannot bind a conversation.
    :param runner_id: Runner the conversation is pinned to (hard
        affinity per ``designs/RUNNER.md`` §5). ``None`` until
        the first dispatch claims a runner; thereafter every
        subsequent dispatch routes to this runner while it is
        online (or fails with ``runner_unavailable`` if not).
    :param host_id: Host that launched (or should launch) the
        runner for this session. Set when a session is created
        from the Web UI targeting a specific host. ``None`` for
        sessions started via ``omnigent run`` (the CLI
        orchestrates runner spawning directly). Used for
        retry-on-reconnect: if the server restarts before the
        runner connects, the server re-sends the launch request
        to this host.
    :param labels: Session-scoped guardrails labels persisted
        in ``conversation_labels``. Populated by
        :meth:`ConversationStore.get_conversation` via a JOIN;
        empty dict when no labels have been written yet. Labels
        survive conversation_items compaction by design
        (POLICIES.md §6.3) — the two tables are
        independent.
    :param session_state: Mutable per-conversation key/value
        store used by policy callables to accumulate state
        across turns (e.g. running counters, audit trails).
        Persisted as a JSON column on the ``conversations``
        table and loaded by the policy engine builder at
        workflow start. Empty dict when no state has been
        written yet.
    :param session_usage: Cumulative LLM token usage for the
        session. Shape: ``{"input_tokens": N, "output_tokens": M,
        "total_tokens": T, "total_cost_usd": C}`` plus an optional
        nested ``"by_model"`` sub-dict keyed by the raw harness model
        id, each holding the same per-bucket token keys (and
        ``total_cost_usd`` when that model's turns were priced), e.g.
        ``{"by_model": {"claude-sonnet-4-6": {"input_tokens": N, ...}}}``.
        Typed ``dict[str, Any]`` (not ``dict[str, float]``) to admit the
        nested ``by_model`` object. Persisted as a JSON column and
        loaded by the policy engine builder at workflow start. Empty
        dict when no LLM calls have been recorded yet.
    :param session_todos: Latest native Plan display snapshot, restored after
        Server restart without invoking the harness or replaying task work.
    :param reasoning_effort: Per-session reasoning-effort hint,
        e.g. ``"high"``. ``None`` means use the agent default.
        Set at session creation via ``POST /v1/sessions`` metadata
        and mutable thereafter via ``PATCH /v1/sessions/{id}``
        (alongside the runner-binding primitive of the Alpha
        runner-state design). Both paths validate the value against
        the supported set; invalid values fail with ``invalid_input``.
    :param reported_model: The model the harness last REPORTED the
        session is actually on, verbatim in the harness's own
        spelling, e.g. ``"claude-opus-4-8[1m]"``. Written only by
        harness reports (native ``external_model_change`` events or
        SDK terminal-response usage); never by user picks. The only
        model value UI surfaces display. ``None`` means no report has
        arrived yet.
    :param model_override: Per-session LLM model override — the user's
        REQUEST, e.g. ``"claude-opus-4-7"``. ``None`` means use the
        agent default from the spec's ``llm.model``. Mutable via
        ``PATCH /v1/sessions/{id}`` and the REPL's ``/model``
        command. Mirrors the persistence shape of
        ``reasoning_effort`` so the web UI and the TUI stay
        in sync — both read it from the session snapshot and
        write it through the same PATCH endpoint.
    :param cost_control_mode_override: Per-session cost-control
        switch: ``"on"`` activates the spec's configured cost-control
        mode, ``"off"`` disables cost control for this session, and
        ``None`` (unset) defers to the spec default. Set at session
        creation via ``POST /v1/sessions`` and mutable via
        ``PATCH /v1/sessions/{id}`` (the web "Cost Optimized"
        toggle). Read by the cost-control advisor pipeline at turn
        start; mirrors the persistence shape of ``model_override``.
    :param subagent_routing_override: Per-session subagent-routing
        switch, two-state: ``"on"`` routes native/SDK subagent spawns,
        and ``"off"`` or ``None`` (unset) both leave them on the parent's
        model. A session created on Smart Routing is stamped ``"on"`` by
        the create route, so unset reads as Default and inherits nothing.
        Mutable via ``PATCH /v1/sessions/{id}`` at any time; read per
        spawn by the route-subagent relay, so a change takes effect on
        the next spawn.
    :param harness_override: Per-session harness override for the
        bound agent's brain, e.g. ``"pi"`` or ``"openai-agents"``.
        ``None`` means use the harness declared in the agent spec
        (``executor.config.harness``). Set at session creation via
        ``POST /v1/sessions`` (the new-chat harness picker) and
        immutable thereafter — the runner spawns the harness on the
        first turn, so a later switch would orphan the running
        process. Only valid for ``executor.type: omnigent`` agents;
        the create route validates against ``OMNIGENT_HARNESSES``.
        Sub-agent sessions never *inherit* the parent brain's override,
        so e.g. polly's workers keep their declared harnesses when the
        brain is overridden. A sub-agent session MAY, however, carry its
        own create-time override when ``sys_session_send`` supplied an
        allowlisted ``args.harness`` (gated by the sub-agent spec's
        ``executor.config.allowed_harnesses``); that value is set on the
        child's own row, not inherited.
    :param share_workspace_files: Whether the owner opted into letting
        people with *view* (read-only) access browse the session's
        workspace files — the Files/Changes/GitHub-diff surfaces and the
        file contents behind them. ``False`` (the default) keeps those
        surfaces edit-only, so a plain read grant shares the conversation
        without exposing the workspace (which routinely holds secrets like
        ``.env`` / key files). Set from the share dialog (manage-gated) via
        ``PATCH /v1/sessions/{id}``; never widens absolute-path browsing,
        which stays owner-only. See ``designs/SESSIONS_AUTH.md``.
    :param sub_agent_name: For sub-agent sessions (``kind="sub_agent"``),
        the sub-agent type name within the parent's spec tree,
        e.g. ``"summarizer"``. The runner uses this to resolve the
        sub-agent's :class:`AgentSpec` from the parent's
        ``sub_agents`` list instead of using the parent's spec
        directly. ``None`` for top-level sessions. Replaces
        ``task.agent_name`` from the removed task store
        (RUNNER_SUBAGENT_DISPATCH.md).
    :param external_session_id: Runtime-native session id this
        conversation wraps, e.g. Claude Code's session uuid for
        ``omnigent claude`` sessions. ``None`` for regular
        AP-only conversations. Populated by the wrapper bridge
        from the underlying runtime and used by ``--resume`` to
        recover the external session's prior transcript on a
        fresh runner. Generic across runtimes — at most one
        external session per conversation.
    :param terminal_launch_args: Pass-through CLI args for a native
        terminal wrapper (claude / codex), e.g.
        ``["--dangerously-skip-permissions"]``. ``None`` for
        non-native sessions, or a native session launched with no
        extra args. Set at session create (so the runner has them
        before it boots) and updated on resume via
        ``PATCH /v1/sessions/{id}`` (last-write-wins). The runner
        reconstructs the terminal launch command from these plus the
        harness binary; the command and all bridge / Omnigent / auth wiring
        stay runner-owned and are never stored here. A flat list (not
        a dict) is deliberate — there is no key for a user to smuggle
        internal wiring through. See
        designs/NATIVE_RUNNER_SERVER_LAUNCH.md.
    :param workspace: Absolute path on disk where the runner cd's,
        e.g. ``"/Users/corey/universe/src/foo"``. Required when
        ``host_id`` is set (enforced by a check constraint at the
        DB layer); optional for CLI-launched sessions that record
        their starting cwd for display. Stored as the canonicalized
        realpath returned by ``host.stat`` at session-create time;
        symlinks are pre-resolved so the agent's ``os_env.cwd``
        boundary check cannot be smuggled past. Immutable after
        creation — see designs/SESSION_WORKSPACE_SELECTION.md. When
        a git worktree was created for the session, this is the
        worktree directory path rather than the picked source repo.
    :param git_branch: Git branch checked out in the session's
        worktree, e.g. ``"feature/login"``. Set only when the
        session was created with a server-created git worktree (the
        ``git`` block of ``POST /v1/sessions``); ``None`` otherwise.
        ``git_branch IS NOT NULL`` is the gate for offering worktree
        cleanup on session delete. See
        designs/SESSION_GIT_WORKTREE.md.
    :param archived: Whether the session is archived. Archived
        sessions are hidden from the default ``GET /v1/sessions``
        listing (and the sidebar), surfacing only when the caller
        passes ``include_archived=True``. ``False`` for normal
        sessions; toggled via ``PATCH /v1/sessions/{id}``.
    :param project_id: The first-class project this session is filed
        under, or ``None`` if unfiled. Owner-private membership; see
        ``designs/PROJECTS_PRD.md``.
    :param search_snippet: Transient, list-only excerpt of the chat
        content that matched a ``search_query`` — set by
        ``list_conversations`` whenever the query hit an item's body (even
        if the title also matched), so the search UI can show *where* the
        session matched. Never persisted (not a DB column) and ``None`` on
        every non-search read path and title-only matches.
    """

    id: str
    created_at: int
    updated_at: int
    root_conversation_id: str
    title: str | None = None
    kind: str = "default"
    parent_conversation_id: str | None = None
    agent_id: str | None = None
    runner_id: str | None = None
    host_id: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    session_state: dict[str, Any] = field(default_factory=dict)
    session_usage: dict[str, Any] = field(default_factory=dict)
    session_todos: list[dict[str, Any]] = field(default_factory=list)
    reasoning_effort: str | None = None
    model_override: str | None = None
    reported_model: str | None = None
    cost_control_mode_override: str | None = None
    subagent_routing_override: str | None = None
    harness_override: str | None = None
    share_workspace_files: bool = False
    sub_agent_name: str | None = None
    task_summary: str | None = None
    external_session_id: str | None = None
    terminal_launch_args: list[str] | None = None
    workspace: str | None = None
    git_branch: str | None = None
    archived: bool = False
    # Live-state fields written by the replica holding the runner tunnel
    # so any replica's session list can serve them. ``live_status`` is the
    # last relay-observed turn status ("idle"/"running"/"waiting"/"failed",
    # None = never reported); ``pending_elicitation_count`` is the
    # outstanding approval-prompt count (None = never written);
    # ``runner_last_seen`` is the runner tunnel's last heartbeat (epoch
    # seconds, None = no live stamp) — carried on the row so a session list
    # can judge runner liveness without a second connectivity query.
    live_status: str | None = None
    pending_elicitation_count: int | None = None
    runner_last_seen: int | None = None
    project_id: str | None = None
    # Transient: populated only by list_conversations on a content search;
    # never read from or written to the DB.
    search_snippet: str | None = None


# ── Conversation item data types ───────────────────────


def synthesize_conversation_title(
    content: list[dict[str, Any]],
    *,
    limit: int = 60,
) -> str | None:
    """
    Derive a one-line conversation title from message content blocks.

    Non-text blocks (``input_image``, ``input_file``) are skipped, and
    lines matching the native executors' attachment path markers
    (:data:`_ATTACHMENT_MARKER_RE`) are dropped so attachments never
    leak temp-file paths into the title.

    :param content: Message content blocks, e.g.
        ``[{"type": "input_text", "text": "Hello"}]``.
    :param limit: Max chars before truncating with an ellipsis.
    :returns: Collapsed/truncated title, or ``None`` when no
        usable text is present.
    """
    parts: list[str] = []
    for block in content:
        if block.get("type") == "input_text":
            text = block.get("text")
            if isinstance(text, str):
                kept_lines = [
                    line
                    for line in text.splitlines()
                    if not _ATTACHMENT_MARKER_RE.match(line.strip())
                ]
                parts.append("\n".join(kept_lines))
    collapsed = " ".join(" ".join(parts).split())
    if not collapsed:
        return None
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(0, limit - 1)].rstrip() + "…"


# Item types that are metadata / lifecycle events — not content
# the agent loop should include in the LLM's message context.
# Used by _sync_history and _load_initial_history to filter.
NON_CONTENT_ITEM_TYPES: frozenset[str] = frozenset(
    {
        "compaction",
        "error",
        "resource_event",
        "routing_decision",
        "slash_command",
        "terminal_command",
    }
)
