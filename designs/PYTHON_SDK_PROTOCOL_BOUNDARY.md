# Python SDK schema boundary

Status: accepted replacement for the earlier `omnigent.protocol` proposal
Source of truth: current `omnigent-ai/omnigent` main branch
Date: 2026-09-21

## Decision

The mature Python SDK follows the schema ownership already used by upstream:

- `omnigent.server.schemas` owns server request, response, pagination, and SSE
  event models.
- `omnigent.entities.conversation` owns persisted conversation item models.
- `omnigent_client` imports those definitions directly and re-exports selected
  public names so normal SDK examples do not require server-internal imports.
- `omnigent_client._models` contains only client-facing input conveniences and
  response shapes that upstream routes currently annotate as dictionaries.

The fork does not introduce an `omnigent.protocol` package or move schema
ownership out of the server. This keeps the SDK aligned with upstream and
avoids turning an SDK maturation project into a server architecture migration.

## SDK-only models

The client-local models are deliberately narrow:

- `SessionMessage`, `FunctionCallOutput`, and `Interrupt` expose the public-safe
  subset of the existing session event route.
- `EventAcknowledgement`, `ElicitationState`,
  `ElicitationResolutionAcknowledgement`, and `SessionResourceDeleted` model
  existing route outputs that upstream currently returns as dictionaries.
- `SessionItem` models the flat output from
  `GET /v1/sessions/{id}/items`; upstream's persisted `ConversationItem` models
  the different nested shape embedded in session snapshots.
- `UnknownEvent` preserves a newer SSE discriminator and its raw payload for
  forward-compatible output handling.

These models add no routes or server behavior. Known server models are never
copied into the SDK.

## Public imports

User documentation imports common models from `omnigent_client`, for example:

```python
from omnigent_client import Omnigent, OutputTextDeltaEvent, SessionItem
```

The re-export is a user-experience boundary, not a second model definition.
The exported event classes retain identity with `omnigent.server.schemas`.

## Evolution rule

When the REST API grows:

1. Treat the current upstream route and schema as authoritative.
2. Reuse its model directly when one exists.
3. Add the smallest client-local output model only when the route has no typed
   upstream response.
4. Add matching native sync and async resource methods.
5. Add one focused transport contract test for the distinct behavior.
6. Update one executable example and the SDK contract ledger.

If upstream later publishes an official protocol facade, the SDK may switch its
internal imports while preserving the `omnigent_client` public re-exports.

## Rejected approach

The earlier fork moved roughly 4,200 lines of schema definitions into a new
`omnigent.protocol` package and changed server modules to re-export them. That
was internally single-source, but it was a broad upstream server refactor. It
conflicted with this project's narrower goal: mature the Python client around
capabilities and ownership that already exist upstream.
