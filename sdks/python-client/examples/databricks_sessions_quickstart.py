#!/usr/bin/env python3
"""Chat interactively with a Databricks-hosted OmniGent server.

Run this from the repository root so ``uv`` installs the SDK from the current
checkout instead of PyPI::

    uv run --frozen --extra all \
      python sdks/python-client/examples/databricks_sessions_quickstart.py \
      --profile <PROFILE>

The script never chooses a Databricks profile for you. If ``--agent-id`` is
omitted, it displays the registered agents and asks which one to use.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shlex
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol, TypeVar
from urllib.parse import urlsplit

import httpx
from databricks.sdk import WorkspaceClient
from omnigent_client import (
    AgentObject,
    AsyncCursorPage,
    AsyncOmnigent,
    ElicitationRequestCtx,
    OmnigentError,
    OutputItemDoneEvent,
    OutputTextDeltaEvent,
    ServerStreamEvent,
    SessionItem,
    StreamHooks,
    UnknownEvent,
)


@dataclass(frozen=True)
class Options:
    profile: str
    agent_id: str | None
    base_url: str | None
    host_type: Literal["external", "managed"]
    sandbox_provider: str | None
    workspace: str | None
    timeout: float
    poll_interval: float
    quiet_period: float
    max_depth: int
    verbose_events: bool


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> Options:
    parser = argparse.ArgumentParser(
        description="Chat with a Databricks-hosted OmniGent server using this checkout."
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Databricks CLI profile to use (never selected automatically).",
    )
    parser.add_argument(
        "--agent-id",
        help="Registered agent id. Omit it to choose interactively.",
    )
    parser.add_argument(
        "--base-url",
        help=("OmniGent API base URL. Defaults to https://<workspace-host>/api/2.0/omnigent."),
    )
    parser.add_argument(
        "--host-type",
        choices=("external", "managed"),
        default="managed",
        help="Session host type (default: managed).",
    )
    parser.add_argument(
        "--sandbox-provider",
        help="Optional managed-sandbox provider configured on the server.",
    )
    parser.add_argument(
        "--workspace",
        help="Optional workspace path or repository URL passed to session creation.",
    )
    parser.add_argument(
        "--timeout",
        type=_non_negative_float,
        default=1200.0,
        help="Maximum seconds for each prompt and follow operation (default: 1200).",
    )
    parser.add_argument(
        "--poll-interval",
        type=_non_negative_float,
        default=1.0,
        help="Seconds between durable session-tree checks (default: 1).",
    )
    parser.add_argument(
        "--quiet-period",
        type=_non_negative_float,
        default=5.0,
        help=(
            "Seconds the known session tree must stay idle before the next prompt "
            "(default: 5; use 60 for conservative delegated-task following)."
        ),
    )
    parser.add_argument(
        "--max-depth",
        type=_non_negative_int,
        default=3,
        help="Maximum descendant-session depth to follow (default: 3).",
    )
    parser.add_argument(
        "--verbose-events",
        action="store_true",
        help="Print every canonical stream event as JSON.",
    )
    values = parser.parse_args(argv)
    return Options(
        profile=values.profile,
        agent_id=values.agent_id,
        base_url=values.base_url,
        host_type=values.host_type,
        sandbox_provider=values.sandbox_provider,
        workspace=values.workspace,
        timeout=values.timeout,
        poll_interval=values.poll_interval,
        quiet_period=values.quiet_period,
        max_depth=values.max_depth,
        verbose_events=values.verbose_events,
    )


def _omnigent_base_url(workspace: WorkspaceClient, override: str | None) -> str:
    if override is not None:
        parsed = urlsplit(override)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("--base-url must be an absolute HTTPS URL")
        return override.rstrip("/")

    workspace_url = urlsplit(workspace.config.host)
    if workspace_url.scheme != "https" or not workspace_url.netloc:
        raise RuntimeError("The selected Databricks profile has no secure workspace URL.")
    return f"{workspace_url.scheme}://{workspace_url.netloc}/api/2.0/omnigent"


async def _all_agents(client: AsyncOmnigent) -> list[AgentObject]:
    agents: list[AgentObject] = []
    page = await client.agents.list(limit=100, order="asc")
    while True:
        agents.extend(page)
        if not page.has_more:
            return agents
        page = await page.get_next_page()


def _print_agents(agents: Sequence[AgentObject]) -> None:
    print("\nRegistered agents:")
    for index, agent in enumerate(agents, start=1):
        print(f"  {index:>2}. {agent.name or '(unnamed)'}  [{agent.id}]")


async def _choose_agent(client: AsyncOmnigent, requested_id: str | None) -> str:
    if requested_id is not None:
        return requested_id

    agents = await _all_agents(client)
    if not agents:
        raise RuntimeError("The selected workspace has no registered agents.")
    _print_agents(agents)

    while True:
        answer = input("Choose an agent by number or id: ").strip()
        if answer.isdigit() and 1 <= int(answer) <= len(agents):
            return agents[int(answer) - 1].id
        for agent in agents:
            if answer == agent.id:
                return agent.id
        print("Enter one of the displayed numbers or agent ids.")


def _block_text(blocks: object) -> str:
    if not isinstance(blocks, list):
        return ""
    text: list[str] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        value = block.get("text")
        if isinstance(value, str) and block.get("type") in {
            "input_text",
            "output_text",
            "text",
        }:
            text.append(value)
    return "".join(text)


class Renderer:
    def __init__(self, *, verbose_events: bool) -> None:
        self._verbose_events = verbose_events
        self._assistant_open = False

    def finish_line(self) -> None:
        if self._assistant_open:
            print()
            self._assistant_open = False

    def _assistant_text(self, text: str) -> None:
        if not text:
            return
        if not self._assistant_open:
            print("assistant> ", end="", flush=True)
            self._assistant_open = True
        print(text, end="", flush=True)

    def _item(self, source: str, item: dict[str, object], *, live: bool) -> None:
        item_type = item.get("type")
        if item_type == "message" and item.get("role") == "assistant":
            if not live or not self._assistant_open:
                self._assistant_text(_block_text(item.get("content")))
            self.finish_line()
            return

        if item_type == "function_call":
            self.finish_line()
            print(f"[{source} · tool] {item.get('name')}({item.get('arguments', '{}')})")
        elif item_type == "function_call_output":
            self.finish_line()
            print(f"[{source} · tool result] {item.get('output', '')}")
        elif item_type == "error":
            self.finish_line()
            print(f"[{source} · error] {item.get('message', '')}")

    def on_event(self, event: ServerStreamEvent | UnknownEvent) -> None:
        if self._verbose_events:
            self.finish_line()
            print(f"\n[event · {event.type}]\n{event.model_dump_json(indent=2)}")
            return

        if isinstance(event, OutputTextDeltaEvent):
            self._assistant_text(event.delta)
        elif isinstance(event, OutputItemDoneEvent):
            self._item("root", event.item, live=True)
        elif event.type == "session.created":
            self.finish_line()
            print("[subagent created]")
        elif event.type in {"response.failed", "response.cancelled", "response.incomplete"}:
            self.finish_line()
            print(f"[{event.type}]")

    def on_item(self, session_id: str, item: SessionItem) -> None:
        if self._verbose_events:
            self.finish_line()
            print(f"\n[item · {session_id} · {item.type}]\n{item.model_dump_json(indent=2)}")
            return
        self._item(session_id, item.model_dump(mode="python"), live=False)


def _approve_or_decline(
    renderer: Renderer,
    ctx: ElicitationRequestCtx,
) -> bool:
    renderer.finish_line()
    print(f"\n[approval requested] {ctx.message}")
    if ctx.content_preview:
        print(f"Preview: {ctx.content_preview}")
    if ctx.url:
        print(f"Open: {ctx.url}")
    if ctx.requested_schema:
        print("Requested schema:")
        print(json.dumps(ctx.requested_schema, indent=2))
    answer = input("Approve? [y/N]: ")
    return answer.strip().lower() in {"y", "yes"}


def _print_help() -> None:
    print(
        """
Commands:
  /help                 Show this help.
  /session              Show the current durable session snapshot.
  /items                List durable items in the root session.
  /subagents            List direct child sessions.
  /files                List uploaded session files.
  /attach <path> [...]  Attach local files to the next prompt.
  /clear                Clear files queued for the next prompt.
  /delete               Delete the remote session after confirmation, then exit.
  /quit                 Exit without deleting the durable remote session.

Any other input, including an unrecognized slash command, is sent to the agent.
""".strip()
    )


class _JsonPrintable(Protocol):
    def to_json(self, **kwargs: Any) -> str: ...


_JsonPrintableT = TypeVar("_JsonPrintableT", bound=_JsonPrintable)


async def _print_page(page: AsyncCursorPage[_JsonPrintableT]) -> None:
    current = page
    while True:
        for item in current:
            print(item.model_dump_json(indent=2))
        if not current.has_more:
            return
        current = await current.get_next_page()


async def _command(
    text: str,
    *,
    client: AsyncOmnigent,
    session_id: str,
    pending_files: list[str],
) -> bool:
    if text == "/help":
        _print_help()
    elif text == "/session":
        snapshot = await client.agents.sessions.retrieve(session_id)
        print(snapshot.model_dump_json(indent=2))
    elif text == "/items":
        page = await client.agents.sessions.items.list(session_id, limit=100, order="asc")
        await _print_page(page)
    elif text == "/subagents":
        page = await client.agents.sessions.subagents.list(session_id, limit=100, order="asc")
        await _print_page(page)
    elif text == "/files":
        page = await client.agents.sessions.files.list(session_id, limit=100, order="asc")
        await _print_page(page)
    elif text == "/clear":
        pending_files.clear()
        print("Cleared pending attachments.")
    elif text == "/delete":
        answer = input(f"Delete remote session {session_id}? [y/N]: ")
        if answer.strip().lower() in {"y", "yes"}:
            await client.agents.sessions.delete(session_id)
            print(f"Deleted {session_id}.")
            return True
    elif text == "/quit":
        print(f"Remote session retained: {session_id}")
        return True
    else:
        return False
    return False


def _queue_attachments(text: str, pending_files: list[str]) -> bool:
    if text != "/attach" and not text.startswith("/attach "):
        return False
    try:
        paths = shlex.split(text)[1:]
    except ValueError as exc:
        print(f"Could not parse paths: {exc}")
        return True
    if not paths:
        print("Usage: /attach <path> [...]")
        return True

    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            print(f"Not a file: {path}")
            continue
        value = str(path)
        if value not in pending_files:
            pending_files.append(value)
            print(f"Queued for the next prompt: {path}")
    return True


async def run(options: Options) -> None:
    workspace = WorkspaceClient(profile=options.profile)
    base_url = _omnigent_base_url(workspace, options.base_url)
    headers = workspace.config.authenticate()

    renderer = Renderer(verbose_events=options.verbose_events)
    hooks = StreamHooks(on_elicitation_request=lambda ctx: _approve_or_decline(renderer, ctx))

    async with AsyncOmnigent(
        base_url=base_url,
        headers=headers,
        timeout=300.0,
    ) as client:
        agent_id = await _choose_agent(client, options.agent_id)
        chat = await client.sessions_chat(
            agent_id=agent_id,
            host_type=options.host_type,
            sandbox_provider=options.sandbox_provider,
            workspace=options.workspace,
            hooks=hooks,
        )
        print(f"\nConnected to {base_url}")
        print(f"Session: {chat.session_id}")
        _print_help()

        pending_files: list[str] = []
        while True:
            try:
                text = input("\nyou> ").strip()
            except EOFError:
                break
            if not text:
                continue
            if _queue_attachments(text, pending_files):
                continue
            if text in {
                "/help",
                "/session",
                "/items",
                "/subagents",
                "/files",
                "/clear",
                "/delete",
                "/quit",
            }:
                if await _command(
                    text,
                    client=client,
                    session_id=chat.session_id,
                    pending_files=pending_files,
                ):
                    return
                continue

            files = pending_files.copy()
            pending_files.clear()
            renderer.finish_line()
            try:
                session = await chat.run(
                    text,
                    files=files or None,
                    on_event=renderer.on_event,
                    on_item=renderer.on_item,
                    timeout=options.timeout,
                    poll_interval=options.poll_interval,
                    quiet_period=options.quiet_period,
                    max_depth=options.max_depth,
                )
            except (OmnigentError, TimeoutError, httpx.HTTPError) as exc:
                renderer.finish_line()
                print(f"[request failed] {exc}")
                continue
            renderer.finish_line()
            print(f"[session tree settled · {session.status}]")

        renderer.finish_line()
        print(f"\nRemote session retained: {chat.session_id}")


def main() -> None:
    options = parse_args()
    try:
        asyncio.run(run(options))
    except KeyboardInterrupt:
        print("\nInterrupted. The remote session was not deleted.")


if __name__ == "__main__":
    main()
