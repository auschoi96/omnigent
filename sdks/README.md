# SDKs

Python packages for integrating with OmniGent's existing REST APIs.

```text
sdks/
  python-client/              # HTTP/SSE client; import omnigent_client
  ui/                         # Terminal UI; import omnigent_ui_sdk
```

All packages require Python 3.12 or newer and are released with the same version
number. Mixed versions of `omnigent`, `omnigent-client`, and
`omnigent-ui-sdk` are unsupported; see the
[client compatibility table](python-client/README.md#installation-and-compatibility).

## `omnigent-client`

Use the client package for scripts, services, bots, tests, and custom
frontends. Its primary API is the typed `client.agents.sessions` resource tree.

The async equivalent is `AsyncOmnigent`. Both use native HTTPX I/O and share
the same session operations, canonical `omnigent.protocol` models, pagination,
errors, and stream semantics. See the
[complete client guide](python-client/README.md) for follow-up input, durable
recovery, session files, subagents, preview elicitations, migration guidance,
and executable sync and async examples.

`client.responses` and the root `Session` remain compatibility surfaces over
the removed `/v1/responses` route. Global file methods remain runtime-error
stubs because `/v1/files` was removed. `BlockStream` and its transforms are
optional presentation conveniences, but are not the foundation of the
session-native HTTP/SSE transport.

## `omnigent-ui-sdk`

The UI package contains the Rich and prompt_toolkit components used by the
OmniGent terminal frontend, including `RichBlockFormatter` and `TerminalHost`.
It layers on `omnigent-client` and retains the existing block presentation
types required by that UI; it does not define a second REST client.

Install the package matching the rest of the environment:

```bash
pip install omnigent-ui-sdk
```

The built-in REPL under `omnigent/repl/` remains the reference implementation
for terminal rendering, input handling, cancellation, and history.
