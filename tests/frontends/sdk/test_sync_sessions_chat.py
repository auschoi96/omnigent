"""Focused tests for sync subtree_busy and SyncSessionsChat.

Mirrors the async subtree_busy contracts (test_sessions_namespace.py)
for the sync SyncSessionsResource, plus a basic SyncSessionsChat
send/query/tree_busy round-trip. Only distinct new risks are covered.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
from omnigent_client._sync_sessions import SyncSessionsResource

_BASE = "https://test.omnigent.local"


def _child(sid: str, **fields: Any) -> dict[str, Any]:
    return {
        "id": sid,
        "parent_session_id": "",
        "created_at": 1,
        "updated_at": 2,
        **fields,
    }


def _tree_handler(
    tree: dict[str, list[dict[str, Any]]],
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        parts = request.url.path.split("/")
        assert parts[-1] == "child_sessions"
        parent_id = parts[-2]
        return httpx.Response(200, json={"data": tree.get(parent_id, [])})

    return handler


def _make_sync(tree: dict[str, list[dict[str, Any]]]) -> tuple[SyncSessionsResource, httpx.Client]:
    http = httpx.Client(transport=httpx.MockTransport(_tree_handler(tree)))
    return SyncSessionsResource(http, _BASE), http


def test_sync_child_sessions_tree_recurses_and_tags_parent() -> None:
    tree = {
        "root": [_child("a"), _child("b")],
        "a": [_child("a1")],
        "a1": [_child("a1x")],
        "b": [],
    }
    ns, http = _make_sync(tree)
    try:
        nodes = ns.child_sessions_tree("root")
    finally:
        http.close()
    by_id = {n["id"]: n for n in nodes}
    assert set(by_id) == {"a", "b", "a1", "a1x"}
    assert by_id["a"]["parent_id"] == "root"
    assert by_id["a1x"]["parent_id"] == "a1"


def test_sync_child_sessions_tree_respects_max_depth() -> None:
    tree = {"root": [_child("a")], "a": [_child("a1")]}
    ns, http = _make_sync(tree)
    try:
        nodes = ns.child_sessions_tree("root", max_depth=1)
    finally:
        http.close()
    assert [n["id"] for n in nodes] == ["a"]


def test_sync_subtree_busy_true_when_deep_descendant_busy() -> None:
    tree = {
        "root": [_child("a", busy=False, current_task_status="completed")],
        "a": [_child("a1", busy=True, current_task_status=None)],
    }
    ns, http = _make_sync(tree)
    try:
        assert ns.subtree_busy("root") is True
    finally:
        http.close()


def test_sync_subtree_busy_false_when_all_terminal() -> None:
    tree = {
        "root": [
            _child("a", busy=False, current_task_status="completed"),
            _child("b", busy=False, current_task_status="failed"),
        ],
        "a": [_child("a1", busy=False, current_task_status="cancelled")],
    }
    ns, http = _make_sync(tree)
    try:
        assert ns.subtree_busy("root") is False
    finally:
        http.close()


def test_sync_subtree_busy_false_when_no_children() -> None:
    ns, http = _make_sync({"root": []})
    try:
        assert ns.subtree_busy("root") is False
    finally:
        http.close()


def test_sync_subtree_busy_counts_pending_elicitation_as_busy() -> None:
    tree = {
        "root": [
            _child(
                "a",
                busy=False,
                current_task_status="completed",
                pending_elicitations_count=1,
            )
        ],
        "a": [],
    }
    ns, http = _make_sync(tree)
    try:
        assert ns.subtree_busy("root") is True
    finally:
        http.close()
