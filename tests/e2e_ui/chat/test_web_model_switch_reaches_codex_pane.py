r"""UI journey (nightly): a web model switch must reach the native Codex pane.

On a native ``codex-native`` ("Codex") session a user can
change the active model from the embedded terminal (in-TUI ``/model``) and the
running pane moves onto it, but the same switch made in the web composer's
model picker never propagates to the pane -- the ask is persisted server-side
(``model_override``) yet the Codex process keeps running the old model.

This drives the reporter's journey on the live SPA against a real ``codex`` CLI
(mock LLM backend):

1. Open a native Codex chat session and attach its embedded Terminal (the live
   TUI) so a real ``codex`` process is running.
2. Control ("TUI switching works"): change the model in the TUI via ``/model``
   and confirm the pane's own source of truth -- the session ``config.toml``
   ``model`` -- moves off its value.
3. Bug ("web UI cannot switch"): open the composer's model picker and pick a
   different catalog model. The server persists it as ``model_override`` (the
   ask reached the server), but the running pane's ``config.toml`` model must
   then become the picked model. On ``main`` it never does, so the final
   assertion times out; after the fix the pane moves onto the web-picked model.

The pane's ``config.toml`` ``model`` is the terminal's own durable model -- what
an in-TUI ``/model`` writes and what the codex-native forwarder re-reads at each
turn boundary -- so a durable model change (from either surface) must land
there. Asserting against it keeps this faithful regardless of the picker's exact
geometry: the bug is precisely that the web pick's ask does not reach that store
while the TUI's does.

Marked ``nightly``: it boots a real codex CLI and drives its TUI, exactly like
``tests/e2e_ui/messages/test_native_codex_render_parity.py``.
"""

from __future__ import annotations

import logging
import time

import httpx
import pytest
from playwright.sync_api import Page, expect

from omnigent.harnesses.codex_native.bridge import (
    bridge_dir_for_bridge_id,
    codex_home_for_bridge_dir,
)
from tests.e2e_ui.messages.test_message_render_parity import _ensure_chat_view
from tests.e2e_ui.messages.test_native_codex_render_parity import (
    _TERMINAL_READY_TIMEOUT_MS,
    _open_terminal_view,
    _type_into_tui,
    _wait_terminal_connected,
)

_log = logging.getLogger(__name__)


def _read_config_model(session_id: str) -> str | None:
    """Read ``model`` from the session's Codex ``config.toml``.

    The runner is in-process in the e2e_ui harness, so the file the codex pane
    reads its durable model from is directly readable.
    """
    config_path = codex_home_for_bridge_dir(bridge_dir_for_bridge_id(session_id)) / "config.toml"
    if not config_path.exists():
        return None
    for line in config_path.read_text(errors="replace").splitlines():
        key, _, rhs = line.strip().partition("=")
        if key.strip() == "model":
            return rhs.strip().strip('"').strip("'") or None
    return None


def _wait_for_config_model(session_id: str, *, timeout_s: float = 60.0) -> str:
    """Wait until the pane has written a concrete model to ``config.toml``."""
    deadline = time.monotonic() + timeout_s
    latest: str | None = None
    while time.monotonic() < deadline:
        latest = _read_config_model(session_id)
        if latest:
            return latest
        time.sleep(0.5)
    raise AssertionError(f"codex pane never wrote a model to config.toml (last={latest!r})")


def _wait_for_config_model_change(
    session_id: str, baseline: str | None, *, timeout_s: float = 45.0
) -> str:
    """Wait until ``config.toml``'s model differs from *baseline* and return it."""
    deadline = time.monotonic() + timeout_s
    latest = baseline
    while time.monotonic() < deadline:
        latest = _read_config_model(session_id)
        if latest and latest != baseline:
            return latest
        time.sleep(0.5)
    raise AssertionError(
        f"model in config.toml never changed (baseline={baseline!r}, still {latest!r})"
    )


def _model_override(base_url: str, session_id: str) -> str | None:
    resp = httpx.get(f"{base_url}/v1/sessions/{session_id}", timeout=10)
    resp.raise_for_status()
    value = resp.json().get("model_override")
    return value if isinstance(value, str) else None


def _wait_for_model_override(
    base_url: str, session_id: str, expected: str, *, timeout_s: float = 20.0
) -> None:
    """Wait until the server has persisted *expected* as ``model_override``."""
    deadline = time.monotonic() + timeout_s
    latest: str | None = None
    while time.monotonic() < deadline:
        latest = _model_override(base_url, session_id)
        if latest == expected:
            return
        time.sleep(0.5)
    raise AssertionError(
        f"server never persisted model_override={expected!r} (last={latest!r}); "
        "the web pick's ask did not reach the server"
    )


def _open_composer_model_submenu(page: Page) -> None:
    gear = page.get_by_test_id("composer-config-gear")
    expect(gear).to_be_visible(timeout=_TERMINAL_READY_TIMEOUT_MS)
    gear.click()
    edit = page.get_by_test_id("composer-agent-edit")
    expect(edit).to_be_visible(timeout=15_000)
    edit.click()


def _choose_web_target(base_url: str, session_id: str, avoid: str | None) -> str:
    """Pick a catalog model id to switch to in the web composer.

    Chooses a NON-default option other than *avoid*: the composer maps picking
    the default row to "clear the override" (``setModel(null)``), which would
    not exercise a switch-to-a-model ask at all.
    """
    resp = httpx.get(f"{base_url}/v1/sessions/{session_id}", timeout=10)
    resp.raise_for_status()
    options = resp.json().get("model_options") or []
    for option in options:
        model_id = option.get("id")
        if not isinstance(model_id, str) or not model_id:
            continue
        if option.get("isDefault"):
            continue
        if model_id == avoid:
            continue
        return model_id
    raise AssertionError(f"no non-default catalog model distinct from {avoid!r} in {options!r}")


def _pick_model_row(page: Page, model_id: str) -> None:
    """Click the composer model row for *model_id*."""
    row = page.get_by_test_id(f"composer-agent-model-{model_id}")
    expect(row).to_be_visible(timeout=15_000)
    row.click()


def _change_model_in_tui(page: Page) -> None:
    """Change the model in the embedded Codex TUI via ``/model``."""
    _type_into_tui(page, "/model")
    page.wait_for_timeout(1_500)
    page.keyboard.press("ArrowDown")
    page.wait_for_timeout(300)
    page.keyboard.press("Enter")
    page.wait_for_timeout(500)
    page.keyboard.press("Enter")
    page.wait_for_timeout(500)


@pytest.mark.nightly
@pytest.mark.timeout(360)
def test_web_model_switch_reaches_native_codex_pane(
    page: Page,
    native_codex_mock_session: tuple[str, str],
) -> None:
    base_url, session_id = native_codex_mock_session
    _log.info("native-codex mock session: base_url=%s session_id=%s", base_url, session_id)

    page.goto(f"{base_url}/c/{session_id}")
    _open_terminal_view(page)
    _wait_terminal_connected(page)
    _wait_for_config_model(session_id)

    # --- Control: the SAME switch from the TUI applies to the pane. ---------
    tui_baseline = _read_config_model(session_id)
    _change_model_in_tui(page)
    tui_model = _wait_for_config_model_change(session_id, tui_baseline)
    _log.info("TUI /model moved the pane: %r -> %r", tui_baseline, tui_model)

    # --- Bug: the web composer pick must also reach the running pane. -------
    _ensure_chat_view(page)
    web_baseline = _wait_for_config_model(session_id)
    picked = _choose_web_target(base_url, session_id, web_baseline)
    _open_composer_model_submenu(page)
    _pick_model_row(page, picked)
    _log.info("web pick: %r (from %r)", picked, web_baseline)

    # The ask reaches the server...
    _wait_for_model_override(base_url, session_id, picked)

    # ...but the running Codex pane must actually move onto it. On main the web
    # switch never propagates to the pane (config.toml stays on web_baseline)
    # even though the TUI control above proved the pane can switch, so this
    # times out; after the fix the pane's model becomes the web-picked model.
    applied = _wait_for_config_model_change(session_id, web_baseline)
    assert applied == picked, (
        f"web-picked model {picked!r} did not reach the codex pane "
        f"(config.toml model is {applied!r})"
    )
