# Databricks notebook source
# MAGIC %md
# MAGIC # Run an OmniGent agent from a Databricks notebook
# MAGIC
# MAGIC This notebook is the shortest path from a Databricks notebook to one
# MAGIC streamed OmniGent session, a follow-up turn, durable history, and explicit
# MAGIC cleanup.
# MAGIC
# MAGIC Its interaction shape follows the
# MAGIC [OpenAI Agents API quickstart](https://developers.openai.com/api/docs/guides/agents-api/quickstart):
# MAGIC create a session with input, stream progress, continue the same session,
# MAGIC and delete it when finished. OmniGent still uses its own registered agents,
# MAGIC REST routes, event names, runners, and lifecycle rules. This is not an API
# MAGIC parity or wire-compatibility claim.
# MAGIC
# MAGIC ## Before you run it
# MAGIC
# MAGIC You need:
# MAGIC
# MAGIC - Databricks compute running Python 3.12 or newer.
# MAGIC - An OmniGent Databricks App in this workspace, or an intentionally
# MAGIC   unauthenticated OmniGent server URL reachable from the notebook.
# MAGIC - A registered OmniGent agent and a configured managed sandbox provider.
# MAGIC - Matching `omnigent`, `omnigent-client`, and `omnigent-ui-sdk` wheels from
# MAGIC   the same fork commit or release. The three packages are released in
# MAGIC   lockstep.
# MAGIC
# MAGIC Upload the three wheels to a Unity Catalog volume or Workspace Files, edit
# MAGIC the paths in the next cell, and run it once. Do not mix package versions.
# MAGIC Build the wheel triple from the fork root with the same commands used by its
# MAGIC release workflow:
# MAGIC
# MAGIC ```bash
# MAGIC uv build --out-dir dist
# MAGIC uv run --no-project --with build python scripts/build_subpackages.py \
# MAGIC   --out-dir dist omnigent-client omnigent-ui-sdk
# MAGIC ```

# COMMAND ----------

# MAGIC %pip install --upgrade \
# MAGIC   /Volumes/<catalog>/<schema>/<volume>/omnigent-0.15.0.dev0-py3-none-any.whl \
# MAGIC   /Volumes/<catalog>/<schema>/<volume>/omnigent_client-0.15.0.dev0-py3-none-any.whl \
# MAGIC   /Volumes/<catalog>/<schema>/<volume>/omnigent_ui_sdk-0.15.0.dev0-py3-none-any.whl \
# MAGIC   "databricks-sdk>=0.56,<1"

# COMMAND ----------

# ruff: noqa: E402
from databricks.sdk.runtime import dbutils

# Restart after the notebook-scoped installation and before importing the packages.
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Connect to OmniGent
# MAGIC
# MAGIC For a Databricks App, provide its registered app name. The notebook resolves
# MAGIC the URL through the current workspace before attaching workspace credentials,
# MAGIC so a bearer token is never sent to an arbitrary user-entered host. No token is
# MAGIC placed in a widget or printed. Choose `none` only when the target server
# MAGIC intentionally requires no authentication.

# COMMAND ----------

from importlib.metadata import version

from databricks.sdk import WorkspaceClient
from databricks.sdk.runtime import dbutils, display
from omnigent_client import Omnigent, SessionMessage

dbutils.widgets.text(
    "omnigent_base_url",
    "https://<your-app>.databricksapps.com",
    "Unauthenticated OmniGent URL",
)
dbutils.widgets.dropdown(
    "auth_mode",
    "databricks",
    ["databricks", "none"],
    "Authentication",
)
dbutils.widgets.text("databricks_app_name", "", "Databricks App name")
dbutils.widgets.text("agent_id", "", "Registered agent ID")
dbutils.widgets.text(
    "sandbox_provider",
    "",
    "Managed sandbox provider (optional)",
)

auth_mode = dbutils.widgets.get("auth_mode")

headers = None
if auth_mode == "databricks":
    app_name = dbutils.widgets.get("databricks_app_name").strip()
    if not app_name:
        raise ValueError("Set databricks_app_name to the OmniGent Databricks App name.")
    workspace_client = WorkspaceClient()
    app = workspace_client.apps.get(name=app_name)
    if not app.url or not app.url.startswith("https://"):
        raise RuntimeError(f"Databricks App {app_name!r} has no secure URL.")
    base_url = app.url.rstrip("/")
    headers = workspace_client.config.authenticate()
else:
    base_url = dbutils.widgets.get("omnigent_base_url").strip().rstrip("/")
    if "<" in base_url or not base_url.startswith(("https://", "http://")):
        raise ValueError("Set omnigent_base_url to the unauthenticated server URL.")

client = Omnigent(base_url=base_url, headers=headers)

print(
    "Package versions:",
    {name: version(name) for name in ("omnigent", "omnigent-client", "omnigent-ui-sdk")},
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Choose an existing registered agent
# MAGIC
# MAGIC OpenAI's quickstart defines an agent inline. OmniGent's current public REST
# MAGIC API instead starts sessions from agents that are already registered on the
# MAGIC server. List them, copy the desired `id` into the `agent_id` widget, and
# MAGIC rerun the next cell. The notebook never silently chooses one for you.

# COMMAND ----------

agents = client.agents.list(limit=100, order="asc")
display([agent.to_dict() for agent in agents])

# COMMAND ----------

agent_id = dbutils.widgets.get("agent_id").strip()
if not agent_id:
    raise ValueError("Choose an id above, set the agent_id widget, and rerun this cell.")

selected = next((agent for agent in agents if agent.id == agent_id), None)
if selected is None:
    raise ValueError(f"Agent {agent_id!r} was not returned by client.agents.list().")

print(f"Selected agent: {selected.name} ({selected.id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create a session and stream the first turn
# MAGIC
# MAGIC `create(input=..., stream=True)` transparently uses existing OmniGent REST
# MAGIC operations: create the durable session, open its event stream through the
# MAGIC readiness heartbeat, and submit the input. The context manager owns only
# MAGIC the local stream; leaving it does not cancel or delete the remote session.
# MAGIC
# MAGIC This notebook deliberately uses an existing managed sandbox provider. An
# MAGIC external session requires an explicit host or runner binding and is outside
# MAGIC this short quickstart.

# COMMAND ----------


def consume_turn(events, *, print_raw_events: bool = False) -> None:
    """Print streamed text and require a truthful terminal outcome."""
    for event in events:
        if print_raw_events:
            print(event.to_json(indent=None), flush=True)
        elif event.type == "response.output_text.delta":
            print(event.delta, end="", flush=True)

    outcome = events.terminal_event
    if outcome is None:
        raise RuntimeError("The stream closed without a terminal outcome.")

    print(f"\n\nTerminal event: {outcome.type}")
    if outcome.type != "response.completed":
        raise RuntimeError(outcome.to_json(indent=None))


sandbox_provider = dbutils.widgets.get("sandbox_provider").strip()

prompt = (
    "Create tree.py, a Python script that prints a readable tree of the files "
    "in the current working directory. Run it and show me the actual output."
)

with client.agents.sessions.create(
    agent_id=agent_id,
    input=prompt,
    stream=True,
    host_type="managed",
    sandbox_provider=sandbox_provider or None,
) as events:
    session_id = events.session_id
    print(f"Session: {session_id}\n")
    consume_turn(events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Continue the same durable session
# MAGIC
# MAGIC For follow-up input, open the stream first. Entering the stream context
# MAGIC consumes OmniGent's readiness heartbeat before the input is submitted, so
# MAGIC early events are not missed.

# COMMAND ----------

with client.agents.sessions.events.stream(session_id) as events:
    client.agents.sessions.events.create(
        session_id,
        events=SessionMessage.text(
            "Add a --max-depth option to tree.py, run it with --max-depth 2, "
            "and show me the output."
        ),
    )
    consume_turn(events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspect durable state
# MAGIC
# MAGIC The live SSE stream is not a replay log. Retrieve the session snapshot and
# MAGIC list durable items when reconnecting or auditing prior work. Do not resend an
# MAGIC input merely because a stream disconnected.

# COMMAND ----------

snapshot = client.agents.sessions.retrieve(session_id)
items = client.agents.sessions.items.list(session_id, limit=100, order="asc")

print(
    {
        "session_id": snapshot.id,
        "status": snapshot.status,
        "agent_name": snapshot.agent_name,
        "durable_item_count": len(items),
    }
)
display([item.to_dict() for item in items])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Clean up explicitly
# MAGIC
# MAGIC Run this cell only when you no longer need the session. Closing the stream or
# MAGIC client releases local HTTP resources; only `sessions.delete` deletes remote
# MAGIC state.

# COMMAND ----------

try:
    deleted = client.agents.sessions.delete(session_id)
    print(deleted.to_json(indent=2))
finally:
    client.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to try next
# MAGIC
# MAGIC - Set `print_raw_events=True` in `consume_turn` to inspect every typed event.
# MAGIC - Use `client.agents.sessions.files` for session-scoped files when the server
# MAGIC   has a file store.
# MAGIC - Use `client.agents.sessions.subagents.list(session_id)` to inspect child
# MAGIC   sessions created by an agent.
# MAGIC - For async notebook code, follow the complete `AsyncOmnigent` example in
# MAGIC   `sdks/python-client/README.md#async-equivalent`. Async construction and
# MAGIC   cleanup use `async with`; resource calls use `await`; streams use
# MAGIC   `async with` and `async for`. It is not a one-name substitution.
# MAGIC
# MAGIC Keep using the server's existing agent configuration for models, tools,
# MAGIC instructions, and sandbox behavior. The Python SDK is the convenient REST
# MAGIC client—not a second agent runtime.
