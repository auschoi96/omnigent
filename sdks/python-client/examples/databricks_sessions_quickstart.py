# Databricks notebook source
# ruff: noqa: E402, E501, F704, I001
# pyrefly: ignore-errors
# MAGIC %md
# MAGIC # Run an OmniGent agent from Databricks
# MAGIC
# MAGIC This quickstart connects directly to an OmniGent REST server, starts a
# MAGIC durable session, shows its public events and durable items, prompts for
# MAGIC real server-published approvals, and waits for delegated work to settle.
# MAGIC The first run uses one high-level `SessionsChat.run(...)` call; later
# MAGIC cells show how to continue and inspect the same session.
# MAGIC
# MAGIC The notebook displays reasoning and tool activity included in the
# MAGIC server's public events.

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

# MAGIC %md
# MAGIC ## 1. Connect to the OmniGent server
# MAGIC
# MAGIC This example uses the managed OmniGent endpoint in the current
# MAGIC Databricks workspace. `WorkspaceClient` supplies only the workspace URL
# MAGIC and authentication; all agent operations go directly to OmniGent.

# COMMAND ----------

from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient
from omnigent_client import AsyncOmnigent

workspace = WorkspaceClient()
workspace_url = urlsplit(workspace.config.host)
if workspace_url.scheme != "https" or not workspace_url.netloc:
    raise RuntimeError("The current Databricks workspace has no secure URL.")

OMNIGENT_BASE_URL = f"{workspace_url.scheme}://{workspace_url.netloc}/api/2.0/omnigent"
client = AsyncOmnigent(
    base_url=OMNIGENT_BASE_URL,
    headers=workspace.config.authenticate(),
    timeout=300.0,  # allows managed-sandbox cold starts on ordinary requests
)

# The same SDK works with an OmniGent server hosted anywhere:
# client = AsyncOmnigent(
#     base_url="https://omnigent.example.com",
#     headers={"Authorization": "Bearer <token>"},
#     timeout=300.0,
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Choose an agent
# MAGIC
# MAGIC Sessions use agents already registered on the connected server. Copy an
# MAGIC `id` from this result into `AGENT_ID`.

# COMMAND ----------

agents = await client.agents.list(limit=100, order="asc")
[(agent.id, agent.name) for agent in agents]

# COMMAND ----------

AGENT_ID = "<registered-agent-id>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Choose what the notebook displays
# MAGIC
# MAGIC `show_event` receives canonical typed stream events. `show_item`
# MAGIC receives canonical durable `SessionItem` values from the root or a
# MAGIC descendant session. The approval callback runs only for a real
# MAGIC `ElicitationRequestEvent`; ordinary assistant text never triggers it.

# COMMAND ----------

from omnigent.protocol import ServerStreamEvent, SessionItem, UnknownEvent
from omnigent_client import ElicitationRequestCtx, StreamHooks


def show_event(event: ServerStreamEvent | UnknownEvent) -> None:
    """Print every public event from the initial live response."""
    print(f"\n[live · {event.type}]")
    print(event.to_json(indent=2))


def show_item(session_id: str, item: SessionItem) -> None:
    """Print each newly observed durable item across the known session tree."""
    print(f"\n[{session_id} · {item.type}]")
    print(item.to_json(indent=2))


def approve_or_decline(ctx: ElicitationRequestCtx) -> bool:
    """Require an explicit decision for a server-published elicitation."""
    print(f"\n[approval requested] {ctx.message}")
    if ctx.content_preview:
        print(f"Preview: {ctx.content_preview}")
    if ctx.url:
        print(f"Open: {ctx.url}")
    return input("Approve? [y/N]: ").strip().lower() in {"y", "yes"}


hooks = StreamHooks(on_elicitation_request=approve_or_decline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run the task
# MAGIC
# MAGIC `sessions_chat(...)` creates one durable registered-agent session.
# MAGIC `run(...)` then opens the stream before submitting input, displays the
# MAGIC live response, resolves typed elicitations through the hook, follows
# MAGIC typed items and child sessions, and returns after the known session tree
# MAGIC has remained quiet for 60 seconds. Its overall default deadline is 20
# MAGIC minutes. `host_type="managed"` asks this OmniGent server to provision its
# MAGIC configured default managed sandbox; the notebook does not create or
# MAGIC connect to that sandbox itself.

# COMMAND ----------

chat = await client.sessions_chat(
    agent_id=AGENT_ID,
    host_type="managed",
    hooks=hooks,
)
print(f"Session: {chat.session_id}\n")

session = await chat.run(
    "Before changing files, request my approval. If I approve, create tree.py, "
    "run it, and show me its output.",
    on_event=show_event,
    on_item=show_item,
)
print(f"\nSession tree settled; root status: {session.status}")

# If this server exposes several managed providers and you must select one,
# pass sandbox_provider="lakebox" to client.sessions_chat(...).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Continue the same session
# MAGIC
# MAGIC Reuse `chat` to preserve the conversation and managed sandbox. The same
# MAGIC approval, event, item, timeout, and quiet-window behavior applies.

# COMMAND ----------

session = await chat.run(
    "Add a --max-depth option, run tree.py with --max-depth 2, and show me the output.",
    on_event=show_event,
    on_item=show_item,
)
print(f"\nSession tree settled; root status: {session.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspect durable REST resources
# MAGIC
# MAGIC The stream is a live tail. Sessions, items, child sessions, agents, and
# MAGIC files are ordinary typed REST resources that remain available later.

# COMMAND ----------

snapshot = await client.agents.sessions.retrieve(chat.session_id)
items = await client.agents.sessions.items.list(
    chat.session_id,
    limit=100,
    order="asc",
)
children = await client.agents.sessions.subagents.list(chat.session_id)
session_agent = await client.agents.sessions.agent.retrieve(chat.session_id)

print(
    {
        "session_id": snapshot.id,
        "status": snapshot.status,
        "agent": session_agent.name,
        "loaded_items": len(items),
        "loaded_children": len(children),
    }
)

# Other existing operations include:
# await client.agents.sessions.files.list(chat.session_id)
# await client.agents.sessions.files.upload(chat.session_id, "/path/to/file")
# await client.agents.sessions.events.cancel(chat.session_id)
# await client.agents.sessions.fork(chat.session_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Close local resources
# MAGIC
# MAGIC Closing the client keeps the durable remote session. Delete it only when
# MAGIC you intentionally want to remove the session and its managed sandbox.

# COMMAND ----------

# await client.agents.sessions.delete(chat.session_id)
await client.close()
