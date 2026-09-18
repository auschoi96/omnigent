"""Pure request, path, and stream helpers shared by native session clients."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypeAlias
from urllib.parse import quote

import httpx
from pydantic import TypeAdapter

from omnigent.protocol import (
    SERVER_STREAM_EVENT_TYPES,
    CancelledEvent,
    CompletedEvent,
    ElicitationResult,
    FailedEvent,
    IncompleteEvent,
    ProjectSessionCreateRequest,
    PublicSessionEventInput,
    ServerStreamEvent,
    SessionCreateMetadata,
    SessionCreateRequest,
    SessionForkRequest,
    SessionMessage,
    SessionStatusEvent,
    UnknownEvent,
    UpdateSessionRequest,
)

from ._not_given import NotGiven

Timeout = float | httpx.Timeout | None
Headers = Mapping[str, str] | None
Query = Mapping[str, str | int | float | bool | None] | None
SessionStreamEvent: TypeAlias = ServerStreamEvent | UnknownEvent
CreateSessionInput: TypeAlias = str | SessionMessage | Sequence[SessionMessage]

STREAM_READY_EVENT_TYPE = "session.heartbeat"
RESPONSE_TERMINAL_EVENT_TYPES = (
    CompletedEvent,
    FailedEvent,
    IncompleteEvent,
    CancelledEvent,
)

_EVENT_ADAPTER: TypeAdapter[ServerStreamEvent] = TypeAdapter(ServerStreamEvent)
_INPUT_ADAPTER: TypeAdapter[PublicSessionEventInput] = TypeAdapter(PublicSessionEventInput)
_log = logging.getLogger("omnigent_client.sessions")


def sessions_url(base_url: str, session_id: str | None = None, suffix: str = "") -> str:
    """Build one session-family URL with path-segment quoting."""
    root = f"{base_url}/v1/sessions"
    if session_id is None:
        return root + suffix
    return f"{root}/{quote(session_id, safe='')}{suffix}"


def session_files_url(base_url: str, session_id: str, suffix: str = "") -> str:
    return sessions_url(base_url, session_id, "/resources/files" + suffix)


def elicitation_url(base_url: str, session_id: str, elicitation_id: str, suffix: str = "") -> str:
    return sessions_url(
        base_url,
        session_id,
        f"/elicitations/{quote(elicitation_id, safe='')}{suffix}",
    )


def present(**values: Any) -> dict[str, Any]:
    return {key: value for key, value in values.items() if not isinstance(value, NotGiven)}


def request_options(timeout: Timeout, extra_headers: Headers) -> dict[str, Any]:
    options: dict[str, Any] = {"headers": extra_headers}
    if timeout is not None:
        options["timeout"] = timeout
    return options


def query_params(extra_query: Query, **method: Any) -> dict[str, Any]:
    params = dict(extra_query or {})
    for key, value in method.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = value
    return params


def normalize_create_input(
    value: CreateSessionInput | None,
) -> SessionMessage | list[SessionMessage] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return SessionMessage.text(value)
    if isinstance(value, SessionMessage):
        return value
    messages = list(value)
    if not 1 <= len(messages) <= 100:
        raise ValueError("input must contain between 1 and 100 messages")
    if not all(isinstance(message, SessionMessage) for message in messages):
        raise TypeError("input sequences may contain only SessionMessage values")
    return messages


def serialize_registered_create(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and serialize the full registered/project create shape."""
    body = {key: value for key, value in fields.items() if not isinstance(value, NotGiven)}
    request_type = (
        ProjectSessionCreateRequest if body.get("project_id") is not None else SessionCreateRequest
    )
    validated = request_type.model_validate(body)
    wire = validated.model_dump(mode="json", by_alias=True, exclude_unset=True)
    if "initial_items" in validated.model_fields_set:
        wire["initial_items"] = [
            event.model_dump(mode="json", by_alias=True, exclude_none=True)
            for event in validated.initial_items
        ]
    return wire


def serialize_bundle_metadata(fields: Mapping[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in fields.items() if value is not None}
    return SessionCreateMetadata.model_validate(body).model_dump(
        mode="json", by_alias=True, exclude_unset=True
    )


def serialize_update(fields: Mapping[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in fields.items() if not isinstance(value, NotGiven)}
    return UpdateSessionRequest.model_validate(body).model_dump(
        mode="json", by_alias=True, exclude_unset=True
    )


def serialize_fork(fields: Mapping[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in fields.items() if not isinstance(value, NotGiven)}
    return SessionForkRequest.model_validate(body).model_dump(
        mode="json", by_alias=True, exclude_unset=True
    )


def serialize_public_events(
    events: PublicSessionEventInput
    | Mapping[str, Any]
    | Sequence[PublicSessionEventInput | Mapping[str, Any]],
) -> tuple[dict[str, Any] | list[dict[str, Any]], bool]:
    batch = (
        list(events)
        if isinstance(events, Sequence) and not isinstance(events, (str, bytes, bytearray))
        else None
    )
    if batch is not None and not 1 <= len(batch) <= 100:
        raise ValueError("events must contain between 1 and 100 entries")
    source = batch if batch is not None else [events]
    payloads = [
        _INPUT_ADAPTER.validate_python(event).model_dump(
            mode="json", by_alias=True, exclude_none=True
        )
        for event in source
    ]
    return (payloads if batch is not None else payloads[0]), batch is not None


def serialize_elicitation_result(
    *,
    action: Literal["accept", "decline", "cancel"],
    content: Mapping[str, str | int | float | bool | list[str] | None] | None,
    meta: Mapping[str, object] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"action": action}
    if content is not None:
        payload["content"] = dict(content)
    if meta is not None:
        payload["_meta"] = dict(meta)
    return ElicitationResult.model_validate(payload).model_dump(
        mode="json", by_alias=True, exclude_none=True
    )


def stream_observation(
    event: SessionStreamEvent,
) -> tuple[str | None, bool]:
    """Return observed response id and whether the event terminates iteration."""
    response_id = getattr(event, "response_id", None)
    response = getattr(event, "response", None)
    nested_response_id = getattr(response, "id", None)
    if isinstance(nested_response_id, str):
        response_id = nested_response_id
    terminal = isinstance(event, RESPONSE_TERMINAL_EVENT_TYPES) or (
        isinstance(event, SessionStatusEvent) and event.status == "failed"
    )
    return response_id if isinstance(response_id, str) else None, terminal


def try_parse_envelope(raw: str) -> SessionStreamEvent | None:
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        _log.warning("Failed to parse SSE data: %s", raw[:200])
        return None
    if not isinstance(decoded, dict):
        _log.warning("SSE data is not a JSON object: %s", raw[:200])
        return None
    event_type = decoded.get("type")
    if isinstance(event_type, str) and event_type not in SERVER_STREAM_EVENT_TYPES:
        return UnknownEvent(type=event_type, raw=decoded)
    try:
        return _EVENT_ADAPTER.validate_python(decoded)
    except ValueError as exc:
        _log.debug("Skipping unparseable session event: %s (%s)", raw[:200], exc)
        return None


class SessionSSEDecoder:
    """Stateful, I/O-neutral decoder for one sessions-native SSE stream."""

    def __init__(self) -> None:
        self._event_type: str | None = None

    def feed(self, line: str) -> tuple[SessionStreamEvent | None, bool]:
        line = line.rstrip("\r\n")
        if line.startswith("event: "):
            self._event_type = line[7:]
            return None, False
        if line.startswith("data: "):
            raw = line[6:]
            if raw.strip() == "[DONE]":
                self._event_type = None
                return None, True
            if self._event_type is not None:
                self._event_type = None
                return try_parse_envelope(raw), False
        if line == "":
            self._event_type = None
        return None, False
