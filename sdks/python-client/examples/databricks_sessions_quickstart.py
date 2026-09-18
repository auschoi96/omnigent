# Databricks notebook source
# ruff: noqa: E402, E501
# MAGIC %md
# MAGIC # Run an OmniGent agent
# MAGIC
# MAGIC Start by connecting directly to an OmniGent server and running one agent
# MAGIC turn. The later sections are optional examples for continuing and
# MAGIC inspecting the same durable session. No notebook widgets are required.
# MAGIC
# MAGIC Requires Databricks compute with Python 3.12 or newer and access to the
# MAGIC target OmniGent server. [Databricks Runtime 11.3 LTS and newer uses the
# MAGIC IPython kernel and supports interactive input from Python notebook
# MAGIC cells](https://docs.databricks.com/aws/en/notebooks/ipython-kernel).

# COMMAND ----------

# MAGIC %pip install --upgrade \
# MAGIC   "omnigent @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base" \
# MAGIC   "omnigent-client @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base#subdirectory=sdks/python-client" \
# MAGIC   "omnigent-ui-sdk @ git+https://github.com/auschoi96/omnigent.git@sdk-v2-base#subdirectory=sdks/ui" \
# MAGIC   "databricks-sdk>=0.56,<1"

# COMMAND ----------

from databricks.sdk.runtime import dbutils

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
from collections.abc import Callable
from typing import Any, Literal
from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient
from omnigent_client import Omnigent, SessionMessage, child_summary_busy

from omnigent.protocol import ElicitationRequestEvent, OutputItemDoneEvent

workspace = WorkspaceClient()
workspace_url = urlsplit(workspace.config.host)
if workspace_url.scheme != "https" or not workspace_url.netloc:
    raise RuntimeError("The current Databricks workspace has no secure URL.")

# The browser UI is <workspace>/omnigent. Its REST API base is this path.
OMNIGENT_BASE_URL = f"{workspace_url.scheme}://{workspace_url.netloc}/api/2.0/omnigent"
client = Omnigent(
    base_url=OMNIGENT_BASE_URL,
    headers=workspace.config.authenticate(),
)

# For another OmniGent server, use that server's own authentication instead:
# client = Omnigent(base_url="https://omnigent.example.com")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show activity, approvals, and delegated work
# MAGIC
# MAGIC A session stream is a live tail. Its `response.completed` event closes
# MAGIC one agent response; a delegated child can still be working afterward.
# MAGIC These small tutorial helpers use the existing SDK resources to:
# MAGIC
# MAGIC - show every public event from each streamed response, including reasoning
# MAGIC   the server publishes, tool calls, tool output, and lifecycle events;
# MAGIC - pause on an elicitation and submit your explicit decision;
# MAGIC - keep following durable activity across the session tree until the root
# MAGIC   session and all known descendants remain quiet for a safety window.
# MAGIC
# MAGIC OmniGent never exposes a model's private hidden chain-of-thought. The
# MAGIC notebook can only display reasoning content the server deliberately
# MAGIC publishes. An agent's server-side policy decides whether a particular
# MAGIC action requires approval; the SDK does not invent an approval gate.
# MAGIC Session and subagent statuses are point-in-time REST state, not one atomic
# MAGIC "whole tree completed" signal, so the final quiet-window check is
# MAGIC deliberately conservative rather than a server completion guarantee.

# COMMAND ----------

ApprovalAction = Literal["accept", "decline"]
ApprovalPrompt = Callable[[str], str]


def prompt_for_elicitation(
    event: ElicitationRequestEvent,
    *,
    prompt: ApprovalPrompt = input,
) -> tuple[ApprovalAction, dict[str, Any] | None]:
    """Collect an explicit decision for one server-published elicitation."""
    params = event.params
    print(f"\n\n[approval requested] {params.message}")
    if params.content_preview:
        print(f"Preview: {params.content_preview}")
    if params.url:
        print(f"Open: {params.url}")

    schema = params.requestedSchema or {}
    properties = schema.get("properties") if isinstance(schema, dict) else None
    if isinstance(properties, dict) and properties:
        print("Requested response schema:")
        print(json.dumps(schema, indent=2))
        while True:
            answer = prompt("Enter a JSON object, or 'decline': ").strip()
            if answer.lower() in {"decline", "deny", "no", "n"}:
                return "decline", None
            try:
                content = json.loads(answer)
            except json.JSONDecodeError as exc:
                print(f"Invalid JSON: {exc}")
                continue
            if isinstance(content, dict):
                return "accept", content
            print("The response must be a JSON object.")

    answer = prompt("Approve? [y/N]: ").strip().lower()
    return ("accept", None) if answer in {"y", "yes"} else ("decline", None)


def resolve_elicitation(
    session_id: str,
    event: ElicitationRequestEvent,
    handled: set[str],
    *,
    prompt: ApprovalPrompt = input,
) -> None:
    """Prompt once, then use the elicitation's existing resolve route."""
    if event.elicitation_id in handled:
        return
    action, content = prompt_for_elicitation(event, prompt=prompt)
    target_session_id = event.params.target_session_id or session_id
    client.agents.sessions.elicitations.resolve(
        target_session_id,
        event.elicitation_id,
        action=action,
        content=content,
        timeout=300.0,
    )
    handled.add(event.elicitation_id)
    print(f"[{action}] {event.elicitation_id}\n")


def show_live_response(
    events,
    *,
    handled_elicitations: set[str],
    seen_item_ids: set[str],
    prompt: ApprovalPrompt = input,
) -> None:
    """Render all public events until this one response reaches a terminal event."""
    for event in events:
        print(f"\n[{event.type}]")
        print(event.to_json(indent=2))

        if isinstance(event, OutputItemDoneEvent):
            item_id = event.item.get("id")
            if isinstance(item_id, str):
                seen_item_ids.add(item_id)
        elif isinstance(event, ElicitationRequestEvent):
            resolve_elicitation(
                events.session_id,
                event,
                handled_elicitations,
                prompt=prompt,
            )


def loaded_pages(page):
    """Yield every item from an SDK cursor page."""
    while True:
        yield from page
        if not page.has_more:
            return
        page = page.get_next_page()


def follow_session_tree_until_quiet(
    root_session_id: str,
    *,
    handled_elicitations: set[str],
    seen_item_ids: set[str],
    prompt: ApprovalPrompt = input,
    timeout: float = 1200.0,
    poll_interval: float = 1.0,
    quiet_period: float = 5.0,
) -> None:
    """Follow durable activity until every known session stays quiet."""
    deadline = time.monotonic() + timeout
    labels = {root_session_id: "root"}
    last_status: dict[str, str] = {}
    quiet_since: float | None = None

    while time.monotonic() < deadline:
        snapshots = {}
        child_summaries = {}
        pending = False
        activity = False
        for current_id, label in list(labels.items()):
            snapshot = client.agents.sessions.retrieve(current_id, timeout=300.0)
            snapshots[current_id] = snapshot
            if last_status.get(current_id) != snapshot.status:
                print(f"\n[{label} · session.status] {snapshot.status}")
                last_status[current_id] = snapshot.status
                activity = True

            items_page = client.agents.sessions.items.list(
                current_id, limit=100, order="asc", timeout=300.0
            )
            for item in loaded_pages(items_page):
                if item.id not in seen_item_ids:
                    seen_item_ids.add(item.id)
                    print(f"\n[{label} · {item.type}]\n{item.to_json(indent=2)}")
                    activity = True

            for raw_event in snapshot.pending_elicitations:
                event = ElicitationRequestEvent.model_validate(raw_event)
                resolve_elicitation(
                    current_id,
                    event,
                    handled_elicitations,
                    prompt=prompt,
                )
            pending = pending or bool(snapshot.pending_elicitations)

            children_page = client.agents.sessions.subagents.list(
                current_id, limit=100, order="asc", timeout=300.0
            )
            for child in loaded_pages(children_page):
                child_summaries[child.id] = child
                if child.id not in labels:
                    labels[child.id] = (
                        child.session_name or child.agent_name or child.tool or "subagent"
                    )
                    activity = True

        root = snapshots[root_session_id]
        root_busy = root.status in {"running", "waiting"}
        descendants_busy = any(
            child_summary_busy(child.model_dump(mode="python"))
            for child in child_summaries.values()
        )
        if root.status == "failed":
            raise RuntimeError(root.last_task_error or "The root session failed.")

        tree_quiet = not root_busy and not descendants_busy and not pending
        now = time.monotonic()
        if tree_quiet and not activity:
            quiet_since = quiet_since or now
            if now - quiet_since >= quiet_period:
                print(
                    "\n[task tree quiet] The root and every known descendant "
                    f"have reported no work for {quiet_period:.0f} seconds."
                )
                for child in child_summaries.values():
                    if child.current_task_status in {"failed", "cancelled"}:
                        detail = child.last_task_error or child.current_task_status
                        print(f"[{labels[child.id]} · {child.current_task_status}] {detail}")
                return
        else:
            quiet_since = None
        time.sleep(poll_interval)

    raise TimeoutError(
        f"Session tree did not become quiet within {timeout:.0f} seconds: {root_session_id}"
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Find an agent
# MAGIC
# MAGIC OmniGent sessions use agents already registered on the server. Run this
# MAGIC cell, then copy an `id` into the first-turn cell.

# COMMAND ----------

agents = client.agents.list(limit=100, order="asc")
[(agent.id, agent.name) for agent in agents]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the first turn
# MAGIC
# MAGIC `sessions.create(...)` is the high-level operation. It creates the durable
# MAGIC session, opens its live stream, submits the input, and asks the server to
# MAGIC provision its default managed sandbox. The 300-second request timeout
# MAGIC covers a managed sandbox cold start; it is not an agent-run deadline. The
# MAGIC live helper displays the public event stream and handles approval
# MAGIC requests. The durable follower then stays active across delegated work and
# MAGIC parent auto-wake responses until the known session tree remains quiet for
# MAGIC the configured safety window.

# COMMAND ----------

AGENT_ID = "<registered-agent-id>"
handled_elicitations: set[str] = set()
seen_item_ids: set[str] = set()

with client.agents.sessions.create(
    agent_id=AGENT_ID,
    input=(
        "Before changing files, request my approval. If I approve, create tree.py, "
        "run it, and show me its output."
    ),
    stream=True,
    host_type="managed",
    timeout=300.0,
) as events:
    session_id = events.session_id
    print(f"Session: {session_id}\n")
    show_live_response(
        events,
        handled_elicitations=handled_elicitations,
        seen_item_ids=seen_item_ids,
    )

outcome = events.terminal_event
if outcome is None:
    raise RuntimeError("The stream closed without a terminal event.")
if outcome.type != "response.completed":
    raise RuntimeError(outcome.to_json(indent=None))
print(f"\n\nResponse outcome: {outcome.type}")

follow_session_tree_until_quiet(
    session_id,
    handled_elicitations=handled_elicitations,
    seen_item_ids=seen_item_ids,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continue the same session
# MAGIC
# MAGIC Open the live stream before sending a follow-up so no early output is
# MAGIC missed. This reuses the sandbox and conversation created above.

# COMMAND ----------

with client.agents.sessions.events.stream(session_id) as events:
    client.agents.sessions.events.create(
        session_id,
        events=SessionMessage.text(
            "Add a --max-depth option, run tree.py with --max-depth 2, and show me the output."
        ),
        timeout=300.0,
    )
    show_live_response(
        events,
        handled_elicitations=handled_elicitations,
        seen_item_ids=seen_item_ids,
    )

outcome = events.terminal_event
if outcome is None:
    raise RuntimeError("The stream closed without a terminal event.")
if outcome.type != "response.completed":
    raise RuntimeError(outcome.to_json(indent=None))

follow_session_tree_until_quiet(
    session_id,
    handled_elicitations=handled_elicitations,
    seen_item_ids=seen_item_ids,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect durable state
# MAGIC
# MAGIC A stream is a live tail, while the session snapshot and items are durable
# MAGIC REST resources that can be retrieved later.

# COMMAND ----------

session = client.agents.sessions.retrieve(session_id)
items = client.agents.sessions.items.list(session_id, limit=100, order="asc")

print(
    {
        "session_id": session.id,
        "status": session.status,
        "agent": session.agent_name,
        "items": len(items),
    }
)
[item.to_dict() for item in items]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore related resources
# MAGIC
# MAGIC These calls use the same session-native resource hierarchy. File methods
# MAGIC require the OmniGent deployment to have a session file store.

# COMMAND ----------

session_agent = client.agents.sessions.agent.retrieve(session_id)
subagents = client.agents.sessions.subagents.list(session_id)

print("Session agent:", session_agent.name)
print("Subagents:", [(child.id, child.agent_name) for child in subagents])

# Other available operations:
# client.agents.sessions.files.list(session_id)
# client.agents.sessions.files.upload(session_id, "/path/to/file")
# client.agents.sessions.events.cancel(session_id)
# client.agents.sessions.fork(session_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finish
# MAGIC
# MAGIC Closing the client releases local HTTP resources but keeps the durable
# MAGIC session. Uncomment the delete call only when you want to remove the remote
# MAGIC session and its managed sandbox.

# COMMAND ----------

# client.agents.sessions.delete(session_id)
client.close()
