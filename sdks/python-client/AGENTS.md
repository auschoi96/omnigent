# Python SDK agent guidance

These instructions apply to every file under `sdks/python-client/`. Read the
repository-root `AGENTS.md`, this file, `SDK_CONTRACT.md`, and
`../../designs/PYTHON_SDK_PROTOCOL_BOUNDARY.md` before changing SDK code.

## Mission

Make the existing OmniGent REST APIs easy and reliable to use from Python.
This is maturation of the existing `omnigent_client`, not a new runtime,
orchestrator, persistence layer, or product capability.

The OpenAI Agents API is directional interaction guidance only. Do not claim
global parity, wire compatibility, or matching resources. A narrowly verified
flow may be described as offering a comparable experience when its OmniGent
differences are explicit.

## Ground truth

Resolve disagreements in this order:

1. Current OmniGent server routes and schemas.
2. Current SDK behavior and tests.
3. Checked-in OpenAPI where it matches implementation.
4. Official OpenAI documentation for selected Python conventions.
5. The SDK contract and general SDK conventions.

Search results are navigation aids. Read the complete owning module and its
direct call path before changing behavior.

## Scope rules

- Every public method maps to an existing REST route or a documented,
  transparent composition of existing routes.
- Reuse `SessionsNamespace`, `SessionsChat`, `FilesNamespace`, existing error
  conversion, SSE handling, redirect protection, and tests.
- Keep the low-level session resource side-effect-free. Do not implement it by
  calling `SessionsChat.send`, which also executes tools and resolves prompts.
- Stream readiness is the consumed initial `session.heartbeat`, not accepted
  headers, iterator construction, a sleep, or a best-effort timeout fallback.
- Do not build on the removed `/v1/responses` path, deprecated
  `ResponsesNamespace`, legacy root `Session`, response-specific SSE/block
  stack, `client.query()`, or removed global `/v1/files` operations.
- Keep session files under their existing `/resources/files` routes.
- Do not invent turn IDs/resources, durable required actions, replay,
  idempotency, webhooks, traces, agent CRUD, artifacts, or client-side durable
  state.
- The public event-input allowlist is exactly `message`,
  `function_call_output`, and `interrupt`. Typed elicitation/approval flows are
  preview. Internal controls and every `external_*` input stay private.
- Preserve OmniGent wire names. Do not rename events into OpenAI strings.
- `client.sessions` and `client.agents.sessions` must share one implementation
  and object identity.
- Context exit closes local HTTP resources only. Cancellation and deletion are
  explicit calls.
- Never select a Databricks CLI profile automatically.
- Add no dependency, abstraction, file, or test without a concrete current
  requirement.

Before each implementation PR, compare with current main and repeat the
deprecation/removal search for each reused route or module. Stop and revise the
design if a foundation is newly deprecated.

## Schema ownership

The current upstream server routes and `omnigent.server.schemas` are the source
of truth for server-owned request, response, and stream-event models. Reuse
those definitions directly and re-export selected user-facing names from
`omnigent_client`; do not create a parallel `omnigent.protocol` schema tree.

SDK-local models are limited to client-only conveniences or route outputs that
upstream currently exposes as untyped dictionaries. They must remain small,
preserve additive output fields, and validate against the upstream schema when
one exists. `UnknownEvent` is output-only and is never valid input.

## Extension workflow

For a new REST feature:

1. Classify and document the real route contract.
2. Reuse the upstream schema and add a minimal SDK-only model only when the
   route has no typed upstream output.
3. Add an explicit method to the matching handwritten namespace.
4. Share paths, serializers, models, pagination, and errors between native
   sync and async I/O; do not run sync calls through an event-loop bridge.
5. Add the smallest MockTransport contract test for the distinct behavior.
6. Export deliberately, add one executable example, update
   `SDK_CONTRACT.md`, and run the lockstep build/install gate.

Create a module or resource namespace only when its first real route is being
exposed.

## Test discipline

Before adding a test, identify its failure mode, the closest existing test, and
why that test cannot be extended or parameterized.

Prefer one complete model fixture, representative event tables, shared
sync/async contracts, deterministic MockTransport tests, and a few end-to-end
journeys. Do not make one test per field/status/parameter or Cartesian products
of models, harnesses, hosts, sandboxes, and options.

## Delegated review protocol

An antagonist does not edit production code. It challenges missed reuse,
invented semantics, duplicate abstractions, false parity claims, redundant
tests, version skew, and disconnect/partial-write/unknown-event behavior. It
may request at most five evidence-backed revision rounds and stops earlier when
there is no new actionable objection.

After antagonist review, a separate verifier independently checks routes,
schemas, examples, compatibility, test reuse, focused test results, and scope.
Only the coordinating agent accepts the work.

For delegated implementation, preferred model families are Kimi K3 or newer,
DeepSeek V4 or newer, and GLM 5.3 or newer. Never guess a model identifier or
downgrade below those versions. `system.ai.gpt-5-6-sol` is the authorized
fallback when a preferred model is unavailable or rate-limited. A worker must
not verify its own work.
