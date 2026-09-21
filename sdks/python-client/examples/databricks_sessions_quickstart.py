# Databricks notebook source
# ruff: noqa: E402, E501, F704, F821
# MAGIC %md
# MAGIC # Run an OmniGent agent from a Databricks notebook
# MAGIC
# MAGIC This tutorial connects to the OmniGent server managed by the current
# MAGIC Databricks workspace, creates a durable agent session, displays its
# MAGIC published activity, handles approval requests, and waits for delegated
# MAGIC work to finish.
# MAGIC
# MAGIC Start with the first five sections. The later cells show how to continue
# MAGIC the same session and inspect files, subagents, and durable history. No
# MAGIC notebook widgets or Databricks App lookup are required.
# MAGIC
# MAGIC The SDK exposes reasoning and tool activity that the OmniGent server
# MAGIC deliberately publishes. It cannot expose a model provider's private
# MAGIC hidden chain-of-thought.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install the SDK from the fork
# MAGIC
# MAGIC The packages share one version and are installed together. This notebook
# MAGIC uses the fork's `main` branch; pin a commit SHA instead of `main` when you
# MAGIC need a reproducible environment.

# COMMAND ----------

# MAGIC %pip install --upgrade \
# MAGIC   "omnigent @ git+https://github.com/auschoi96/omnigent.git@main" \
# MAGIC   "omnigent-client @ git+https://github.com/auschoi96/omnigent.git@main#subdirectory=sdks/python-client" \
# MAGIC   "databricks-sdk>=0.56,<1"

# COMMAND ----------

# MAGIC %md
# MAGIC Restart Python once after installation so every later cell imports the
# MAGIC newly installed packages. Continue with section 2 after the restart.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect to the managed OmniGent server
# MAGIC
# MAGIC `WorkspaceClient()` uses the current notebook's Databricks credentials.
# MAGIC OmniGent is served directly at the workspace's managed REST path; it is
# MAGIC not a Databricks App, so there is no `workspace.apps.get(...)` call.
# MAGIC
# MAGIC `DatabricksAuth` asks the Databricks SDK for a current authorization
# MAGIC header on every request. This matters for a notebook that remains open
# MAGIC long enough for an OAuth token to refresh.

# COMMAND ----------

from collections.abc import Generator
from urllib.parse import urlsplit

import httpx
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from omnigent_client import (
    AsyncOmnigent,
    ElicitationRequestCtx,
    OutputTextDeltaEvent,
    ServerStreamEvent,
    SessionItem,
    StreamHooks,
    UnknownEvent,
)


class DatabricksAuth(httpx.Auth):
    """Refresh Databricks credentials for each OmniGent request."""

    def __init__(self, config: Config) -> None:
        self.config = config

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers.update(self.config.authenticate())
        yield request


workspace = WorkspaceClient()
workspace_url = urlsplit(workspace.config.host)
if workspace_url.scheme != "https" or not workspace_url.netloc:
    raise RuntimeError("The current Databricks workspace has no secure URL.")

OMNIGENT_BASE_URL = (
    f"{workspace_url.scheme}://{workspace_url.netloc}/api/2.0/omnigent"
)
client = AsyncOmnigent(
    base_url=OMNIGENT_BASE_URL,
    auth=DatabricksAuth(workspace.config),
    timeout=300.0,
)

print("Connected to:", OMNIGENT_BASE_URL)

# COMMAND ----------

# MAGIC %md
# MAGIC The same SDK works with an OmniGent server hosted anywhere. For another
# MAGIC server, replace the URL and use that server's authentication:
# MAGIC
# MAGIC ```python
# MAGIC client = AsyncOmnigent(
# MAGIC     base_url="https://omnigent.example.com",
# MAGIC     headers={"Authorization": "Bearer <token>"},
# MAGIC     timeout=300.0,
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Choose a registered agent
# MAGIC
# MAGIC OmniGent creates sessions from agents that already exist on the server.
# MAGIC Run this cell, inspect the names, then paste the desired ID into
# MAGIC `AGENT_ID`.

# COMMAND ----------

agents = await client.agents.list(limit=100, order="asc")
display([{"id": agent.id, "name": agent.name} for agent in agents])

# COMMAND ----------

AGENT_ID = "<registered-agent-id>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Decide what the notebook displays
# MAGIC
# MAGIC `SessionsChat.run()` already implements the difficult parts:
# MAGIC
# MAGIC 1. Open the session stream before submitting input.
# MAGIC 2. Deliver typed live events.
# MAGIC 3. Resolve typed approval requests through `StreamHooks`.
# MAGIC 4. Follow durable items and known child sessions.
# MAGIC 5. Return only after the known session tree has stayed quiet.
# MAGIC
# MAGIC The callbacks below are presentation only. Text deltas are printed as a
# MAGIC readable response; every other published event and durable item is shown
# MAGIC as typed JSON so reasoning summaries, tool calls, tool results, retries,
# MAGIC subagent activity, and errors remain visible.

# COMMAND ----------

def show_event(event: ServerStreamEvent | UnknownEvent) -> None:
    """Display one public live event."""
    if isinstance(event, OutputTextDeltaEvent):
        print(event.delta, end="", flush=True)
        return
    print(f"\n\n[live event · {event.type}]")
    print(event.model_dump_json(indent=2))


def show_item(session_id: str, item: SessionItem) -> None:
    """Display one newly observed durable item from the session tree."""
    print(f"\n\n[durable item · {session_id} · {item.type}]")
    print(item.model_dump_json(indent=2))


def approve_or_decline(ctx: ElicitationRequestCtx) -> bool:
    """Ask the notebook user to decide a real server elicitation."""
    print(f"\n\n[approval requested] {ctx.message}")
    if ctx.content_preview:
        print("Preview:", ctx.content_preview)
    if ctx.url:
        print("Open:", ctx.url)
    if ctx.requested_schema:
        print("Requested schema:", ctx.requested_schema)
    return input("Approve? [y/N]: ").strip().lower() in {"y", "yes"}


hooks = StreamHooks(on_elicitation_request=approve_or_decline)

# COMMAND ----------

# MAGIC %md
# MAGIC Only a typed server elicitation triggers `approve_or_decline`. Ordinary
# MAGIC assistant text is never interpreted as an approval request. If no hook is
# MAGIC registered, `SessionsChat` declines elicitations fail-closed rather than
# MAGIC leaving the agent parked forever.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create a session and run the first task
# MAGIC
# MAGIC Creating `chat` makes one durable session and asks the server to provision
# MAGIC a managed sandbox. `run()` then submits the task and follows the root plus
# MAGIC known descendants.
# MAGIC
# MAGIC The 60-second quiet period is deliberate. A `response.completed` event
# MAGIC ends one response, but a delegated child may still be working and may wake
# MAGIC the parent again. The notebook cell therefore remains active until the
# MAGIC known tree has reported no work for a full minute. `timeout=1800` is the
# MAGIC overall task deadline, not an HTTP read timeout.

# COMMAND ----------

chat = await client.sessions_chat(
    agent_id=AGENT_ID,
    host_type="managed",
    hooks=hooks,
)

print("Session:", chat.session_id)

session = await chat.run(
    (
        "Before changing files, request my approval. If I approve, create "
        "tree.py, run it, and show me its output."
    ),
    on_event=show_event,
    on_item=show_item,
    timeout=1800.0,
    poll_interval=1.0,
    quiet_period=60.0,
    max_depth=3,
)

print("\n\nKnown session tree is quiet.")
print({"session_id": chat.session_id, "status": session.status})

# COMMAND ----------

# MAGIC %md
# MAGIC `run()` waiting for the known tree is a conservative SDK composition of
# MAGIC existing REST resources, not a new server-side task object or an atomic
# MAGIC completion guarantee. If the deadline expires, the durable session still
# MAGIC exists and can be inspected by ID.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Continue the same session
# MAGIC
# MAGIC Reuse the same `chat` object for follow-up work. The session keeps its
# MAGIC conversation and managed sandbox, and `SessionsChat` avoids redisplaying
# MAGIC durable items it already observed.

# COMMAND ----------

session = await chat.run(
    "Add a --max-depth option, run tree.py with --max-depth 2, and show the output.",
    on_event=show_event,
    on_item=show_item,
    timeout=1800.0,
    quiet_period=60.0,
)

print("\nFollow-up complete:", session.status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspect durable session state
# MAGIC
# MAGIC The live stream is transient. Session snapshots and items are durable REST
# MAGIC resources that can be retrieved later. The first page below contains up to
# MAGIC 100 chronological items; call `await items.get_next_page()` when
# MAGIC `items.has_more` is true.

# COMMAND ----------

snapshot = await chat.refresh()
items = await client.agents.sessions.items.list(
    chat.session_id,
    limit=100,
    order="asc",
)

print(
    {
        "session_id": snapshot.id,
        "status": snapshot.status,
        "agent": snapshot.agent_name,
        "item_count_on_page": len(items),
        "has_more_items": items.has_more,
    }
)
display([item.model_dump(mode="json") for item in items])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inspect delegated agents
# MAGIC
# MAGIC Child sessions are ordinary durable sessions. This page shows direct
# MAGIC children; `SessionsChat.run()` recursively follows known descendants up
# MAGIC to `max_depth` while waiting.

# COMMAND ----------

subagents = await client.agents.sessions.subagents.list(
    chat.session_id,
    limit=100,
    order="asc",
)
display([child.model_dump(mode="json") for child in subagents])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Work with session files
# MAGIC
# MAGIC Files are scoped to a session. Set `FILE_TO_UPLOAD` to a driver-local path
# MAGIC such as a workspace file under `/Workspace/Users/...`, then run the cell.
# MAGIC Leave it empty to list existing files without uploading anything.

# COMMAND ----------

FILE_TO_UPLOAD = ""  # Example: "/Workspace/Users/you@example.com/notes.txt"

if FILE_TO_UPLOAD:
    uploaded = await client.agents.sessions.files.upload(
        chat.session_id,
        FILE_TO_UPLOAD,
    )
    print("Uploaded:", uploaded.id, uploaded.name)

files = await client.agents.sessions.files.list(chat.session_id, limit=100)
display([file.model_dump(mode="json") for file in files])

# COMMAND ----------

# MAGIC %md
# MAGIC A file can be downloaded through the same resource:
# MAGIC
# MAGIC ```python
# MAGIC await client.agents.sessions.files.download(
# MAGIC     chat.session_id,
# MAGIC     file_id="<file-id>",
# MAGIC     to_path="/tmp/downloaded-file",
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cancel, retain, or delete the session
# MAGIC
# MAGIC - `await chat.cancel()` submits the existing interrupt event when work is
# MAGIC   still running.
# MAGIC - Closing the client releases local HTTP resources but retains the remote
# MAGIC   session and managed sandbox state.
# MAGIC - Deletion is explicit. Change `DELETE_SESSION` only when you intend to
# MAGIC   remove the remote session.

# COMMAND ----------

DELETE_SESSION = False

if DELETE_SESSION:
    deleted = await client.agents.sessions.delete(chat.session_id)
    print("Deleted:", deleted.id)
else:
    print("Remote session retained:", chat.session_id)

await client.close()
