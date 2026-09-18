"""A cost-budget policy on a parent session must proactively interrupt a
running sub-agent once the budget is blown — and the blown budget also locks
the parent out of its own session (the catch-22 this documents).

Reported journey (Codex-native, macOS): the user set a ``$100`` cost budget
on a parent session and dispatched a Codex sub-agent that looped through many
sequential LLM calls. The sub-agent ran freely past the budget to ``$379.11``
because enforcement is purely reactive/pull-based — the budget check only
fires when a session hits a gate event (``UserPromptSubmit`` → ``PHASE_REQUEST``
or ``PreToolUse`` → ``PHASE_TOOL_CALL``). When the budget was first exhausted
nothing signalled or cancelled the running child. Worse, once tree-wide spend
crossed the cap the parent's *own* next model call was DENYed by the same
policy, so the user could no longer interact with the parent to stop the
runaway child (the catch-22) — the only escape was "Stop session", which kills
the whole tree.

These tests drive the *real* server app (real stores + policy engine + the
exact ``POST /v1/sessions/{id}/policies/evaluate`` a native hook posts) through
that journey. The interrupt is observed via the same
``_forward_session_change_to_runner(..., {"type": "interrupt"})`` mechanism the
human-decline path already uses (``routes_hooks.py``), which the reporter's
suggested fix wires the budget DENY into.

- ``test_budget_deny_at_parent_interrupts_running_child`` — the core gap.
  A ``$100`` ``cost_budget`` on the parent, tree spend seeded to ``$379.11``
  (parent + child), then the parent's next model call is gated. Requires the
  over-budget DENY to dispatch an interrupt to the running child. FAILS on the
  buggy build (the DENY dispatches zero interrupts); passes once the budget
  DENY is wired to the interrupt mechanism.
- ``test_budget_deny_at_web_prompt_interrupts_running_child`` — the same
  guarantee on the web path: a user message POSTed to ``/events`` (the SPA's
  prompt) that the budget DENYs must also interrupt the running child.
- ``test_budget_locks_parent_out_of_its_own_session`` — the catch-22. Once
  over budget, the parent's own ``PHASE_REQUEST`` (a user message turn) is
  hard-DENYed, so the user cannot interact to cancel the child.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from omnigent.runtime.agent_cache import AgentCache
from omnigent.server.app import create_app
from omnigent.stores.agent_store.sqlalchemy_store import SqlAlchemyAgentStore
from omnigent.stores.artifact_store.local import LocalArtifactStore
from omnigent.stores.comment_store.sqlalchemy_store import SqlAlchemyCommentStore
from omnigent.stores.conversation_store.sqlalchemy_store import (
    SqlAlchemyConversationStore,
)
from omnigent.stores.file_store.sqlalchemy_store import SqlAlchemyFileStore
from omnigent.stores.policy_store.sqlalchemy_store import SqlAlchemyPolicyStore
from tests.server.conftest import ControllableMockClient
from tests.server.helpers import create_test_agent

pytestmark = pytest.mark.asyncio


# A hard $100 session cost budget the user set on the parent — the exact
# builtin the reporter configured (``cost_budget`` with only ``max_cost_usd``,
# so it is a block-all hard stop over budget, no downgrade escape hatch).
_COST_BUDGET_PAYLOAD: dict[str, Any] = {
    "name": "session_cost_budget",
    "type": "python",
    "handler": "omnigent.policies.builtins.cost.cost_budget",
    "factory_params": {"max_cost_usd": 100.0},
    "enabled": True,
}

# Reported over-cap spend: the sub-agent looped to $379.11 against $100.
_PARENT_SPEND_USD = 200.0
_CHILD_SPEND_USD = 179.11
_TREE_SPEND_USD = _PARENT_SPEND_USD + _CHILD_SPEND_USD  # $379.11
_BUDGET_USD = 100.0


@pytest.fixture()
def policy_app(runtime_init: None, db_uri: str, tmp_path: Path) -> FastAPI:
    """FastAPI app with a policy store wired in.

    The standard ``app`` fixture omits the policy store, so the session
    policy CRUD routes are not mounted. This adds one so
    ``POST /v1/sessions/{id}/policies`` (the call that attaches the budget)
    and the evaluate endpoint both see session-attached policies.

    :param runtime_init: Fixture that initializes the runtime with a mock LLM.
    :param db_uri: Test database URI.
    :param tmp_path: Pytest temporary directory fixture.
    """
    artifact_store = LocalArtifactStore(str(tmp_path / "artifacts"))
    return create_app(
        agent_store=SqlAlchemyAgentStore(db_uri),
        file_store=SqlAlchemyFileStore(db_uri),
        conversation_store=SqlAlchemyConversationStore(db_uri),
        artifact_store=artifact_store,
        agent_cache=AgentCache(
            artifact_store=artifact_store,
            cache_dir=tmp_path / "cache",
        ),
        comment_store=SqlAlchemyCommentStore(db_uri),
        policy_store=SqlAlchemyPolicyStore(db_uri),
    )


@pytest_asyncio.fixture()
async def client(
    policy_app: FastAPI,
    mock_llm: ControllableMockClient,
    tmp_path: Path,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[httpx.AsyncClient]:
    """Async HTTP client wired to the policy-enabled app.

    Patches the runtime's ``_policy_store`` global so the evaluate endpoint
    picks up session-attached policies, and starts a harness process manager
    so engine builds that touch harness state do not fail.

    :param policy_app: The policy-enabled FastAPI app.
    :param mock_llm: Controllable mock LLM (released on teardown).
    :param tmp_path: Pytest temporary directory fixture.
    :param db_uri: Test database URI.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    from omnigent.runtime import _globals, set_harness_process_manager
    from omnigent.runtime.harnesses.process_manager import HarnessProcessManager

    pm = HarnessProcessManager(tmp_parent=tmp_path / "harness_pm")
    await pm.start()
    set_harness_process_manager(pm)

    monkeypatch.setattr(_globals, "_policy_store", SqlAlchemyPolicyStore(db_uri))

    transport = httpx.ASGITransport(app=policy_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    mock_llm.release_all()
    set_harness_process_manager(None)
    await pm.shutdown()


# ── Helpers ─────────────────────────────────────────────────────────────────


async def _create_session(client: httpx.AsyncClient, agent_id: str) -> str:
    """Create a top-level session bound to an agent and return its id.

    :param client: Test HTTP client.
    :param agent_id: Agent to bind.
    :returns: New session id.
    """
    resp = await client.post("/v1/sessions", json={"agent_id": agent_id})
    assert resp.status_code == 201, f"create failed: {resp.status_code} {resp.text}"
    return resp.json()["id"]


def _request_event() -> dict[str, Any]:
    """A ``PHASE_REQUEST`` evaluation — what a session's next turn gates on.

    :returns: EvaluationRequest JSON dict.
    """
    return {
        "event": {
            "type": "PHASE_REQUEST",
            "target": "",
            "data": {},
            "context": {},
        },
    }


def _tool_call_event(tool_name: str = "Bash") -> dict[str, Any]:
    """A ``PHASE_TOOL_CALL`` evaluation — what a ``PreToolUse`` hook posts.

    :param tool_name: Tool name, e.g. ``"Bash"``.
    :returns: EvaluationRequest JSON dict.
    """
    return {
        "event": {
            "type": "PHASE_TOOL_CALL",
            "target": "",
            "data": {"name": tool_name, "arguments": {"command": "ls"}},
            "context": {},
        },
    }


async def _set_up_over_budget_tree(
    client: httpx.AsyncClient,
    store: SqlAlchemyConversationStore,
    *,
    policy_payload: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Build a parent+child spawn tree over a $100 cost budget.

    Attaches a hard ``$100`` ``cost_budget`` to the parent, spawns a child
    sub-agent, then seeds cumulative spend so the whole tree totals
    ``$379.11`` — the reported runaway. Returns ``(parent_id, child_id)``.

    :param client: Test HTTP client.
    :param store: Conversation store for spawning the child + seeding usage.
    :param policy_payload: Optional policy override for alternate budget modes.
    :returns: ``(parent_id, child_id)``.
    """
    agent = await create_test_agent(client)
    parent_id = await _create_session(client, agent["id"])

    child_row = store.create_conversation(
        agent_id=agent["id"],
        parent_conversation_id=parent_id,
        title="runaway codex sub-agent",
    )
    child_id = child_row.id
    assert child_row.root_conversation_id == parent_id

    attach = await client.post(
        f"/v1/sessions/{parent_id}/policies",
        json=policy_payload if policy_payload is not None else _COST_BUDGET_PAYLOAD,
    )
    assert attach.status_code < 400, f"policy attach failed: {attach.status_code} {attach.text}"

    # The user worked; the sub-agent looped. Cumulative spend now far exceeds
    # the cap: parent $200 + child $179.11 = tree-wide $379.11.
    store.set_session_usage(parent_id, {"total_cost_usd": _PARENT_SPEND_USD})
    store.set_session_usage(child_id, {"total_cost_usd": _CHILD_SPEND_USD})
    return parent_id, child_id


@pytest.fixture()
def interrupted_sessions(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record runner interrupt targets without requiring online runners."""
    from omnigent.server.routes import sessions as sessions_facade

    interrupted: list[str] = []

    async def forward(session_id: str, router: Any, change: Any, *a: Any, **k: Any) -> None:
        if isinstance(change, dict) and change.get("type") == "interrupt":
            interrupted.append(session_id)

    monkeypatch.setattr(sessions_facade, "_forward_session_change_to_runner", forward)
    return interrupted


async def _assert_denied(client: httpx.AsyncClient, session_id: str, gate: str) -> None:
    """Exercise a native policy hook or the web prompt's policy gate."""
    if gate in {"native", "native-tool"}:
        response = await client.post(
            f"/v1/sessions/{session_id}/policies/evaluate",
            json=_tool_call_event() if gate == "native-tool" else _request_event(),
        )
        assert response.status_code == 200, response.text
        assert response.json()["result"] == "POLICY_ACTION_DENY", response.text
    else:
        response = await client.post(
            f"/v1/sessions/{session_id}/events",
            json={
                "type": "message",
                "data": {
                    "role": "user",
                    "content": [{"type": "text", "text": "continue"}],
                },
            },
        )
        assert response.status_code < 300, response.text
        assert response.json().get("denied") is True, response.text


# ── Tests ─────────────────────────────────────────────────────────────────


async def test_budget_deny_at_parent_interrupts_running_child(
    client: httpx.AsyncClient,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A budget DENY must proactively interrupt the running sub-agent.

    Reconstructs the reported journey: a $100 budget on the parent, a child
    sub-agent that looped tree-wide spend to $379.11. The parent's next model
    call trips the gate and DENYs. The budget guard is supposed to protect
    against a runaway sub-agent, so the DENY must push a cancellation to the
    still-running child — the same ``{"type": "interrupt"}`` the human-decline
    path forwards in ``routes_hooks``.

    On the buggy build the DENY is purely reactive: it returns a deny verdict
    and dispatches **no** interrupt, so the child keeps looping (the reporter
    saw $379 against a $100 cap). This asserts an interrupt is dispatched, so
    it FAILS today and passes once the budget DENY is wired to the interrupt
    mechanism.
    """
    store = SqlAlchemyConversationStore(db_uri)
    parent_id, child_id = await _set_up_over_budget_tree(client, store)

    # Spy on the runner interrupt forwarder at its facade patch point (the
    # helpers-layer proxies resolve it at call time), the same mechanism the
    # human-decline path uses. Record every change dispatched and to which
    # session, without touching a runner (best-effort forward).
    from omnigent.server.routes import sessions as sessions_facade

    dispatched: list[tuple[str, dict[str, Any]]] = []
    real_forward = sessions_facade._forward_session_change_to_runner

    async def _spy_forward(session_id: str, router: Any, change: Any, *a: Any, **k: Any) -> None:
        dispatched.append((session_id, dict(change) if isinstance(change, dict) else change))
        # Do not hit a real runner; the child is not online in this harness.
        return

    monkeypatch.setattr(sessions_facade, "_forward_session_change_to_runner", _spy_forward)
    assert real_forward is not None  # sanity: the symbol we shadowed exists

    # The parent's next model call gates — and DENYs, over budget.
    resp = await client.post(f"/v1/sessions/{parent_id}/policies/evaluate", json=_request_event())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["result"] == "POLICY_ACTION_DENY", body
    assert f"${_TREE_SPEND_USD:.2f}" in (body.get("reason") or ""), body

    interrupts = [
        (sid, change)
        for sid, change in dispatched
        if isinstance(change, dict) and change.get("type") == "interrupt"
    ]
    assert interrupts, (
        "no proactive interrupt: the over-budget DENY "
        f"(tree spend ${_TREE_SPEND_USD:.2f} vs ${_BUDGET_USD:.2f} cap) "
        "dispatched no interrupt to the running sub-agent "
        f"(child {child_id!r}), so it keeps looping past the cap. "
        f"Dispatched changes: {dispatched!r}"
    )
    interrupted_ids = {sid for sid, _change in interrupts}
    assert child_id in interrupted_ids, (
        f"the interrupt missed the running child {child_id!r}; "
        f"it went to {sorted(interrupted_ids)!r}"
    )
    # The parent's own gated call is already blocked by the DENY; it must
    # not receive an interrupt (that would poke the user's own pane).
    assert parent_id not in interrupted_ids, (
        f"the denied parent {parent_id!r} must not be interrupted; "
        f"interrupts went to {sorted(interrupted_ids)!r}"
    )


async def test_budget_deny_at_web_prompt_interrupts_running_child(
    client: httpx.AsyncClient,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A budget DENY on the web prompt gate also interrupts the child.

    The SPA posts user messages to ``POST /v1/sessions/{id}/events``, which
    gates them server-side (``_evaluate_input_policy``) before any runner
    forward. When that gate DENYs over budget, the running sub-agent must be
    interrupted just like on the native-hook path — the runaway doesn't care
    which surface the user drives.
    """
    store = SqlAlchemyConversationStore(db_uri)
    parent_id, child_id = await _set_up_over_budget_tree(client, store)

    from omnigent.server.routes import sessions as sessions_facade

    dispatched: list[tuple[str, dict[str, Any]]] = []

    async def _spy_forward(session_id: str, router: Any, change: Any, *a: Any, **k: Any) -> None:
        dispatched.append((session_id, dict(change) if isinstance(change, dict) else change))
        return

    monkeypatch.setattr(sessions_facade, "_forward_session_change_to_runner", _spy_forward)

    resp = await client.post(
        f"/v1/sessions/{parent_id}/events",
        json={
            "type": "message",
            "data": {
                "role": "user",
                "content": [{"type": "text", "text": "stop the sub-agent"}],
            },
        },
    )
    # POST /events answers 202 for handled events; the deny settles inline.
    assert resp.status_code < 300, resp.text
    body = resp.json()
    assert body.get("denied") is True, body

    interrupted_ids = {
        sid
        for sid, change in dispatched
        if isinstance(change, dict) and change.get("type") == "interrupt"
    }
    assert child_id in interrupted_ids, (
        f"web-prompt budget DENY did not interrupt the running child "
        f"{child_id!r}; dispatched: {dispatched!r}"
    )


async def test_budget_locks_parent_out_of_its_own_session(
    client: httpx.AsyncClient,
    db_uri: str,
) -> None:
    """The catch-22: once over budget the parent DENYs its own turns.

    After tree spend crosses the cap, the parent's own ``PHASE_REQUEST`` (a
    plain user message turn — the user trying to interact to stop the child)
    and ``PHASE_TOOL_CALL`` are both hard-DENYed by the same budget. The user
    is locked out of the parent with no in-session way to cancel the runaway
    sub-agent, which is the reported compounding failure.
    """
    store = SqlAlchemyConversationStore(db_uri)
    parent_id, _child_id = await _set_up_over_budget_tree(client, store)

    # A user message turn on the parent gates at PHASE_REQUEST — and is DENYed.
    req = await client.post(f"/v1/sessions/{parent_id}/policies/evaluate", json=_request_event())
    assert req.status_code == 200, req.text
    req_body = req.json()
    assert req_body["result"] == "POLICY_ACTION_DENY", req_body
    assert "blocked" in (req_body.get("reason") or "").lower(), req_body

    # A tool call on the parent is likewise DENYed — no in-session escape.
    tool = await client.post(
        f"/v1/sessions/{parent_id}/policies/evaluate", json=_tool_call_event()
    )
    assert tool.status_code == 200, tool.text
    tool_body = tool.json()
    assert tool_body["result"] == "POLICY_ACTION_DENY", tool_body


@pytest.mark.parametrize("gate", ["native", "native-tool", "web"])
@pytest.mark.parametrize("model_params", [{}, {"expensive_models": []}], ids=["omitted", "empty"])
async def test_inherited_budget_deny_interrupts_full_spawn_tree(
    client: httpx.AsyncClient,
    db_uri: str,
    interrupted_sessions: list[str],
    gate: str,
    model_params: dict[str, Any],
) -> None:
    """A child hitting a renamed hard cap interrupts all active descendants."""
    store = SqlAlchemyConversationStore(db_uri)
    parent_id, child_id = await _set_up_over_budget_tree(
        client,
        store,
        policy_payload={
            **_COST_BUDGET_PAYLOAD,
            "name": "project_spending_limit",
            "factory_params": {"max_cost_usd": _BUDGET_USD, **model_params},
        },
    )
    child = store.get_conversation(child_id)
    assert child is not None
    sibling = store.create_conversation(agent_id=child.agent_id, parent_conversation_id=parent_id)
    grandchild = store.create_conversation(
        agent_id=child.agent_id, parent_conversation_id=child_id
    )
    archived = store.create_conversation(agent_id=child.agent_id, parent_conversation_id=parent_id)
    store.update_conversation(archived.id, archived=True)
    unrelated = store.create_conversation(agent_id=child.agent_id)
    store.create_conversation(agent_id=child.agent_id, parent_conversation_id=unrelated.id)

    await _assert_denied(client, child_id, gate)

    assert sorted(interrupted_sessions) == sorted([child_id, sibling.id, grandchild.id])


@pytest.mark.parametrize("gate", ["native", "web"])
async def test_downgrade_budget_deny_leaves_children_running(
    client: httpx.AsyncClient,
    db_uri: str,
    interrupted_sessions: list[str],
    gate: str,
) -> None:
    """A downgrade gate must preserve children using cheaper models."""
    store = SqlAlchemyConversationStore(db_uri)
    parent_id, child_id = await _set_up_over_budget_tree(
        client,
        store,
        policy_payload={
            **_COST_BUDGET_PAYLOAD,
            "factory_params": {"max_cost_usd": _BUDGET_USD, "expensive_models": ["opus"]},
        },
    )
    store.update_conversation(parent_id, model_override="claude-opus-4-1")
    store.update_conversation(child_id, model_override="gpt-4o-mini")

    await _assert_denied(client, parent_id, gate)

    child_verdict = await client.post(
        f"/v1/sessions/{child_id}/policies/evaluate", json=_request_event()
    )
    assert child_verdict.status_code == 200, child_verdict.text
    assert child_verdict.json()["result"] == "POLICY_ACTION_ALLOW", child_verdict.text
    assert interrupted_sessions == []


@pytest.mark.parametrize("gate", ["native", "web"])
async def test_unrelated_budget_named_deny_leaves_children_running(
    client: httpx.AsyncClient,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_sessions: list[str],
    gate: str,
) -> None:
    """Neither a policy's budget-like name nor its reason triggers interrupts."""
    from omnigent.server.routes import session_policies

    handler = "omnigent.policies.function.make_fixed_action_callable"
    monkeypatch.setattr(session_policies, "is_registered_handler", lambda value: value == handler)
    parent_id, _child_id = await _set_up_over_budget_tree(
        client,
        SqlAlchemyConversationStore(db_uri),
        policy_payload={
            **_COST_BUDGET_PAYLOAD,
            "handler": handler,
            "factory_params": {
                "action": "deny",
                "reason": "Session cost $379.11 exceeded the $100.00 budget.",
            },
        },
    )

    await _assert_denied(client, parent_id, gate)

    assert interrupted_sessions == []


@pytest.mark.parametrize("cost_on_root", [False, True], ids=["budget-deny", "ordinary-deny"])
async def test_same_named_policies_use_actual_denying_spec(
    client: httpx.AsyncClient,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_sessions: list[str],
    cost_on_root: bool,
) -> None:
    """Duplicate policy names cannot misidentify the policy that denied."""
    from omnigent.server.routes import session_policies

    fixed_handler = "omnigent.policies.function.make_fixed_action_callable"
    original_is_registered = session_policies.is_registered_handler
    monkeypatch.setattr(
        session_policies,
        "is_registered_handler",
        lambda value: value == fixed_handler or original_is_registered(value),
    )
    budget_payload = {
        **_COST_BUDGET_PAYLOAD,
        "factory_params": {"max_cost_usd": 1000.0 if cost_on_root else _BUDGET_USD},
    }
    ordinary_payload = {
        **_COST_BUDGET_PAYLOAD,
        "handler": fixed_handler,
        "factory_params": {"action": "deny" if cost_on_root else "allow"},
    }
    parent_id, child_id = await _set_up_over_budget_tree(
        client,
        SqlAlchemyConversationStore(db_uri),
        policy_payload=budget_payload if cost_on_root else ordinary_payload,
    )
    attach = await client.post(
        f"/v1/sessions/{child_id}/policies",
        json=ordinary_payload if cost_on_root else budget_payload,
    )
    assert attach.status_code < 400, attach.text

    await _assert_denied(client, child_id, "native")

    assert interrupted_sessions == ([] if cost_on_root else [child_id])
    assert parent_id not in interrupted_sessions


async def test_read_only_budget_deny_leaves_children_running(
    client: httpx.AsyncClient,
    db_uri: str,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_sessions: list[str],
) -> None:
    """Viewing a budget verdict cannot interrupt the session's descendants."""
    from omnigent.server.auth import LEVEL_READ
    from omnigent.server.routes._auth_helpers import SessionAccess
    from omnigent.server.routes.sessions import routes_hooks

    store = SqlAlchemyConversationStore(db_uri)
    parent_id, _child_id = await _set_up_over_budget_tree(client, store)
    access = AsyncMock(
        return_value=SessionAccess(
            level=LEVEL_READ, conversation=store.get_conversation(parent_id)
        )
    )
    monkeypatch.setattr(routes_hooks, "_require_access_and_level", access)

    await _assert_denied(client, parent_id, "native")

    access.assert_awaited_once()
    assert interrupted_sessions == []
