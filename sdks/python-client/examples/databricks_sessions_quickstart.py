# Databricks notebook source
# ruff: noqa: E402, E501
# MAGIC %md
# MAGIC # Run an OmniGent agent with full visibility
# MAGIC
# MAGIC Connect to any OmniGent server and run an agent turn using the
# MAGIC high-level `SyncSessionsChat` helper — the same chat machinery the
# MAGIC async SDK provides, but synchronous so it works in a Databricks
# MAGIC notebook without an event loop.
# MAGIC
# MAGIC You will see every public event the session emits: reasoning the
# MAGIC server publishes, tool calls, tool output, sub-agent activity, and
# MAGIC approval requests. The cell ends only after the agent's response
# MAGIC completes **and** every delegated sub-agent in the session tree has
# MAGIC settled.
# MAGIC
# MAGIC Requires Python 3.12+ and access to the target OmniGent server.
# MAGIC [Databricks Runtime 11.3 LTS and newer uses the IPython kernel and
# MAGIC supports interactive input from Python notebook
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

# MAGIC %md
# MAGIC ## Connect to an OmniGent server
# MAGIC
# MAGIC The SDK works with any OmniGent server. In Databricks, derive the
# MAGIC managed-server URL from the workspace host and pass the workspace's
# MAGIC auth headers. For a standalone server, supply your own base URL and
# MAGIC authentication.

# COMMAND ----------

from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient
from omnigent_client import Omnigent

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
# MAGIC ## Define what you want to see
# MAGIC
# MAGIC `StreamHooks` are callbacks fired from the session's live event
# MAGIC stream. Each hook fires for one lifecycle event: reasoning blocks,
# MAGIC tool calls, messages, sub-agent spawns/completions, and approval
# MAGIC requests. OmniGent never exposes a model's private hidden
# MAGIC chain-of-thought — only reasoning the server deliberately publishes.

# COMMAND ----------

from omnigent_client import StreamHooks


def make_hooks():
    """Return StreamHooks that print every public lifecycle event."""

    def on_response_start(ctx):
        print(f"\n[response started] {ctx.response.id} ({ctx.response.model})")

    def on_reasoning_start(ctx):
        print("\n[reasoning]")

    def on_reasoning_end(ctx):
        if ctx.reasoning_text:
            print(ctx.reasoning_text)
        if ctx.summary_text:
            print(f"\n[summary] {ctx.summary_text}")

    def on_message_start(ctx):
        print("\n[assistant]")

    def on_message_end(ctx):
        for block in ctx.content:
            if isinstance(block, dict) and block.get("type") in ("output_text", "text"):
                print(block.get("text", ""), end="")
        print()

    def on_tool_call_start(ctx):
        who = "client" if ctx.executed_by == "client" else "server"
        print(f"\n[tool call · {who}] {ctx.name}({ctx.arguments})")

    def on_tool_call_end(ctx):
        preview = ctx.output[:200] if ctx.output else ""
        print(f"[tool result] {ctx.name}: {preview}")

    def on_sub_agent_spawned(ctx):
        for sub in ctx.sub_agents:
            print(f"\n[sub-agent spawned] {sub.agent_name} ({sub.response_id})")

    def on_sub_agent_completed(ctx):
        print(
            f"\n[sub-agent done] {ctx.agent_name} — {ctx.status}"
            + (f": {ctx.output_summary}" if ctx.output_summary else "")
        )

    def on_elicitation_request(ctx):
        print(f"\n\n[approval requested] {ctx.message}")
        if ctx.content_preview:
            print(f"Preview: {ctx.content_preview}")
        if ctx.url:
            print(f"Open: {ctx.url}")
        answer = input("Approve? [y/N]: ").strip().lower()
        return answer in ("y", "yes")

    return StreamHooks(
        on_response_start=on_response_start,
        on_reasoning_start=on_reasoning_start,
        on_reasoning_end=on_reasoning_end,
        on_message_start=on_message_start,
        on_message_end=on_message_end,
        on_tool_call_start=on_tool_call_start,
        on_tool_call_end=on_tool_call_end,
        on_sub_agent_spawned=on_sub_agent_spawned,
        on_sub_agent_completed=on_sub_agent_completed,
        on_elicitation_request=on_elicitation_request,
    )


hooks = make_hooks()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find an agent
# MAGIC
# MAGIC OmniGent sessions use agents already registered on the server. Run
# MAGIC this cell, then copy an `id` into the next cell.

# COMMAND ----------

agents = client.agents.list(limit=100, order="asc")
[(agent.id, agent.name) for agent in agents]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run a turn and follow delegated work
# MAGIC
# MAGIC `client.sessions_chat(...)` creates the durable session, wires the
# MAGIC hooks and agent-tools getter, and returns a `SyncSessionsChat`. The
# MAGIC `send()` generator yields every public event until the response
# MAGIC completes. After that, `tree_busy()` polls the sub-agent tree so
# MAGIC delegated work is followed to completion. The 300-second timeout
# MAGIC covers a managed sandbox cold start; it is not an agent-run deadline.

# COMMAND ----------

import time

AGENT_ID = "<registered-agent-id>"
QUIET_WINDOW = 30.0  # seconds of subtree inactivity before declaring settled
POLL_INTERVAL = 5.0  # seconds between tree-busy polls

chat = client.sessions_chat(
    agent_id=AGENT_ID,
    hooks=hooks,
    host_type="managed",
    timeout=300.0,
)

print(f"Session: {chat.session_id}\n")
for event in chat.send(
    "Before changing files, request my approval. If I approve, create tree.py, "
    "run it, and show me its output."
):
    print(f"[{event.type}]")

print("\nResponse completed. Following delegated work...")

# After the response completes, poll the sub-agent tree until it settles.
quiet_since = None
while True:
    busy = chat.tree_busy()
    if busy:
        quiet_since = None
        time.sleep(POLL_INTERVAL)
        continue
    if quiet_since is None:
        quiet_since = time.monotonic()
    elif time.monotonic() - quiet_since >= QUIET_WINDOW:
        break
    time.sleep(POLL_INTERVAL)

print("\n\nSession tree settled. All delegated work is complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continue the same session
# MAGIC
# MAGIC Send a follow-up to continue the conversation. This reuses the
# MAGIC sandbox and conversation created above.

# COMMAND ----------

for event in chat.send(
    "Now add a --max-depth option, run tree.py with --max-depth 2, and show me the output."
):
    print(f"[{event.type}]")

quiet_since = None
while True:
    busy = chat.tree_busy()
    if busy:
        quiet_since = None
        time.sleep(POLL_INTERVAL)
        continue
    if quiet_since is None:
        quiet_since = time.monotonic()
    elif time.monotonic() - quiet_since >= QUIET_WINDOW:
        break
    time.sleep(POLL_INTERVAL)

print("\n\nSession tree settled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect durable state
# MAGIC
# MAGIC A stream is a live tail; the session snapshot and items are durable
# MAGIC REST resources that persist after the stream closes.

# COMMAND ----------

chat.refresh()
items = client.agents.sessions.items.list(chat.session_id, limit=100, order="asc")

print({"session_id": chat.session_id, "status": chat.status, "items": len(items)})
[item.to_dict() for item in items]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finish
# MAGIC
# MAGIC Closing the client releases local HTTP resources but keeps the
# MAGIC durable session. Uncomment the delete call only when you want to
# MAGIC remove the remote session and its managed sandbox.

# COMMAND ----------

# client.agents.sessions.delete(chat.session_id)
client.close()
