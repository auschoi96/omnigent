# Databricks notebook source
# ruff: noqa: E402, E501
# MAGIC %md
# MAGIC # Run an OmniGent agent
# MAGIC
# MAGIC Start by connecting to one OmniGent Databricks App and running one agent
# MAGIC turn. The later sections are optional examples for continuing and
# MAGIC inspecting the same durable session. No notebook widgets are required.
# MAGIC
# MAGIC Requires Databricks compute with Python 3.12 or newer and access to the
# MAGIC OmniGent Databricks App.

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

from databricks.sdk import WorkspaceClient
from omnigent_client import Omnigent, SessionMessage

from omnigent.protocol import OutputTextDeltaEvent

APP_NAME = "<omnigent-app-name>"

workspace = WorkspaceClient()
app = workspace.apps.get(name=APP_NAME)
if not app.url or not app.url.startswith("https://"):
    raise RuntimeError(f"Databricks App {APP_NAME!r} has no secure URL.")

client = Omnigent(
    base_url=app.url,
    headers=workspace.config.authenticate(),
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
# MAGIC provision its default managed sandbox. The loop only displays streamed
# MAGIC text as it arrives.

# COMMAND ----------

AGENT_ID = "<registered-agent-id>"

with client.agents.sessions.create(
    agent_id=AGENT_ID,
    input="Create tree.py, run it, and show me its output.",
    stream=True,
    host_type="managed",
) as events:
    session_id = events.session_id
    print(f"Session: {session_id}\n")
    for event in events:
        if isinstance(event, OutputTextDeltaEvent):
            print(event.delta, end="", flush=True)

outcome = events.terminal_event
if outcome is None:
    raise RuntimeError("The stream closed without a terminal event.")
if outcome.type != "response.completed":
    raise RuntimeError(outcome.to_json(indent=None))
print(f"\n\nOutcome: {outcome.type}")

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
    )
    for event in events:
        if isinstance(event, OutputTextDeltaEvent):
            print(event.delta, end="", flush=True)

outcome = events.terminal_event
if outcome is None:
    raise RuntimeError("The stream closed without a terminal event.")
if outcome.type != "response.completed":
    raise RuntimeError(outcome.to_json(indent=None))

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
