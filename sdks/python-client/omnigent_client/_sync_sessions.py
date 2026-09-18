"""Native synchronous resources for the OmniGent sessions route family."""

from __future__ import annotations

import builtins
import json
import mimetypes
import pathlib
from collections.abc import Generator, Iterator, Mapping, Sequence
from os import PathLike
from typing import Any, Literal, overload
from urllib.parse import quote

import httpx
from pydantic import TypeAdapter

from omnigent.protocol import (
    AgentObject,
    ChildSessionList,
    ChildSessionSummary,
    ConversationDeleted,
    CopyFilesRequest,
    CopyFilesResponse,
    ElicitationResolutionAcknowledgement,
    ElicitationState,
    EventAcknowledgement,
    PaginatedList,
    PublicSessionEventInput,
    SessionGitOptions,
    SessionItem,
    SessionList,
    SessionListItem,
    SessionMessage,
    SessionResourceDeleted,
    SessionResourceObject,
    SessionResourcePaginatedList,
    SessionResponse,
)

from ._errors import (
    OmnigentError,
    SessionCompositionError,
    StreamProtocolError,
    raise_for_status,
    require_json_object,
    response_body,
)
from ._not_given import NOT_GIVEN, NotGiven
from ._pagination import SyncCursorPage
from ._raw_response import APIResponse
from ._sessions_shared import (
    STREAM_READY_EVENT_TYPE,
    CreateSessionInput,
    Headers,
    Query,
    SessionSSEDecoder,
    SessionStreamEvent,
    Timeout,
    elicitation_url,
    normalize_create_input,
    present,
    query_params,
    request_options,
    serialize_bundle_metadata,
    serialize_elicitation_result,
    serialize_fork,
    serialize_public_events,
    serialize_registered_create,
    serialize_update,
    session_files_url,
    sessions_url,
    stream_observation,
)
from ._timeouts import _SSE_TIMEOUT

_ELICITATION_STATE_ADAPTER: TypeAdapter[ElicitationState] = TypeAdapter(ElicitationState)


class SessionEventStream:
    """Context-managed synchronous stream over one session live tail."""

    def __init__(
        self,
        sessions: SyncSessionsResource,
        session_id: str,
        *,
        idle: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
        iterator: Iterator[SessionStreamEvent] | None = None,
    ) -> None:
        self.session_id = session_id
        self.last_response_id: str | None = None
        self.terminal_event: SessionStreamEvent | None = None
        self._sessions = sessions
        self._idle = idle
        self._timeout = timeout
        self._extra_headers = extra_headers
        self._extra_query = extra_query
        self._iterator = iterator
        self._closed = False

    def _ensure_open(self) -> Iterator[SessionStreamEvent]:
        if self._closed:
            raise RuntimeError("session event stream is closed")
        if self._iterator is None:
            try:
                self._iterator = self._sessions._open_stream_ready(
                    self.session_id,
                    idle=self._idle,
                    timeout=self._timeout,
                    extra_headers=self._extra_headers,
                    extra_query=self._extra_query,
                )
            except BaseException:
                self._closed = True
                raise
        return self._iterator

    def __enter__(self) -> SessionEventStream:
        self._ensure_open()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __iter__(self) -> SessionEventStream:
        return self

    def __next__(self) -> SessionStreamEvent:
        if self._closed:
            raise StopIteration
        try:
            event = next(self._ensure_open())
        except StopIteration:
            self.close()
            raise
        response_id, terminal = stream_observation(event)
        if response_id is not None:
            self.last_response_id = response_id
        if terminal:
            self.terminal_event = event
            self.close()
        return event

    def close(self) -> None:
        """Release only the local response stream."""
        if self._closed:
            return
        self._closed = True
        iterator, self._iterator = self._iterator, None
        if iterator is not None:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


class SyncEventsResource:
    def __init__(self, sessions: SyncSessionsResource) -> None:
        self._sessions = sessions

    def create(
        self,
        session_id: str,
        *,
        events: PublicSessionEventInput
        | Mapping[str, Any]
        | Sequence[PublicSessionEventInput | Mapping[str, Any]],
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> EventAcknowledgement | list[EventAcknowledgement]:
        payload, is_batch = serialize_public_events(events)
        value = self._sessions._post_event_payload(
            session_id,
            payload,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        if is_batch:
            if not isinstance(value, list):
                raise OmnigentError("POST session events returned a non-list batch")
            return [EventAcknowledgement.model_validate(item) for item in value]
        return EventAcknowledgement.model_validate(value)

    def cancel(
        self,
        session_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> EventAcknowledgement:
        result = self.create(
            session_id,
            events={"type": "interrupt", "data": {}},
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        assert isinstance(result, EventAcknowledgement)
        return result

    def stream(
        self,
        session_id: str,
        *,
        idle: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionEventStream:
        return SessionEventStream(
            self._sessions,
            session_id,
            idle=idle,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )


class SyncItemsResource:
    def __init__(self, sessions: SyncSessionsResource) -> None:
        self._sessions = sessions

    def list(
        self,
        session_id: str,
        *,
        limit: int = 100,
        after: str | None = None,
        before: str | None = None,
        order: Literal["asc", "desc"] = "asc",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SyncCursorPage[SessionItem]:
        params = query_params(extra_query, limit=limit, after=after, before=before, order=order)
        response = self._sessions._http.get(
            sessions_url(self._sessions._base, session_id, "/items"),
            params=params,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        page = PaginatedList.model_validate(require_json_object(response, "GET session items"))
        adapter: TypeAdapter[SessionItem] = TypeAdapter(SessionItem)
        data = [adapter.validate_python(item) for item in page.data]
        return SyncCursorPage(
            data,
            first_id=page.first_id,
            last_id=page.last_id,
            has_more=page.has_more,
            fetch_next_page=lambda cursor: self.list(
                session_id,
                limit=limit,
                after=cursor,
                before=before,
                order=order,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            ),
        )


class SyncSubagentsResource:
    def __init__(self, sessions: SyncSessionsResource) -> None:
        self._sessions = sessions

    def list(
        self,
        session_id: str,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        order: Literal["asc", "desc"] = "desc",
        tool: str | None = None,
        session_name: str | None = None,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SyncCursorPage[ChildSessionSummary]:
        params = query_params(
            extra_query,
            limit=limit,
            after=after,
            before=before,
            order=order,
            tool=tool,
            session_name=session_name,
        )
        response = self._sessions._http.get(
            sessions_url(self._sessions._base, session_id, "/child_sessions"),
            params=params,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        page = ChildSessionList.model_validate(require_json_object(response, "GET child sessions"))
        return SyncCursorPage(
            page.data,
            first_id=page.first_id,
            last_id=page.last_id,
            has_more=page.has_more,
            fetch_next_page=lambda cursor: self.list(
                session_id,
                limit=limit,
                after=cursor,
                before=before,
                order=order,
                tool=tool,
                session_name=session_name,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            ),
        )


class SyncSessionAgentResource:
    def __init__(self, sessions: SyncSessionsResource) -> None:
        self._sessions = sessions

    def retrieve(
        self,
        session_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> AgentObject:
        response = self._sessions._http.get(
            sessions_url(self._sessions._base, session_id, "/agent"),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return AgentObject.model_validate(require_json_object(response, "GET session agent"))

    def contents(
        self,
        session_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> bytes:
        response = self._sessions._http.get(
            sessions_url(self._sessions._base, session_id, "/agent/contents"),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return response.content

    def update(
        self,
        session_id: str,
        bundle: bytes,
        *,
        filename: str = "agent.tar.gz",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> AgentObject:
        response = self._sessions._http.put(
            sessions_url(self._sessions._base, session_id, "/agent"),
            files={"bundle": (filename, bundle, "application/gzip")},
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return AgentObject.model_validate(require_json_object(response, "PUT session agent"))


class SyncElicitationsResource:
    """Preview access to the server's process-memory elicitation state."""

    def __init__(self, sessions: SyncSessionsResource) -> None:
        self._sessions = sessions

    def retrieve(
        self,
        session_id: str,
        elicitation_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> ElicitationState:
        response = self._sessions._http.get(
            elicitation_url(self._sessions._base, session_id, elicitation_id),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return _ELICITATION_STATE_ADAPTER.validate_python(
            require_json_object(response, "GET session elicitation")
        )

    def resolve(
        self,
        session_id: str,
        elicitation_id: str,
        *,
        action: Literal["accept", "decline", "cancel"],
        content: Mapping[str, str | int | float | bool | list[str] | None] | None = None,
        meta: Mapping[str, object] | None = None,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> ElicitationResolutionAcknowledgement:
        response = self._sessions._http.post(
            elicitation_url(self._sessions._base, session_id, elicitation_id, "/resolve"),
            json=serialize_elicitation_result(action=action, content=content, meta=meta),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return ElicitationResolutionAcknowledgement.model_validate(
            require_json_object(response, "POST resolve session elicitation")
        )


class SyncSessionFilesResource:
    def __init__(self, http: httpx.Client, base_url: str) -> None:
        self._http, self._base = http, base_url

    def list(
        self,
        session_id: str,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        order: Literal["asc", "desc"] = "desc",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SyncCursorPage[SessionResourceObject]:
        params = query_params(extra_query, limit=limit, after=after, before=before, order=order)
        response = self._http.get(
            session_files_url(self._base, session_id),
            params=params,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        page = SessionResourcePaginatedList.model_validate(
            require_json_object(response, "GET session files")
        )
        return SyncCursorPage(
            page.data,
            first_id=page.first_id,
            last_id=page.last_id,
            has_more=page.has_more,
            fetch_next_page=lambda cursor: self.list(
                session_id,
                limit=limit,
                after=cursor,
                before=before,
                order=order,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            ),
        )

    def upload(
        self,
        session_id: str,
        path: str | PathLike[str],
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResourceObject:
        local = pathlib.Path(path)
        with local.open("rb") as stream:
            response = self._http.post(
                session_files_url(self._base, session_id),
                params=extra_query,
                files={"file": (local.name, stream, mimetypes.guess_type(str(local))[0])},
                **request_options(timeout, extra_headers),
            )
        raise_for_status(response.status_code, response_body(response))
        return SessionResourceObject.model_validate(
            require_json_object(response, "POST session file")
        )

    def retrieve(
        self,
        session_id: str,
        file_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResourceObject:
        response = self._http.get(
            session_files_url(self._base, session_id, f"/{quote(file_id, safe='')}"),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return SessionResourceObject.model_validate(
            require_json_object(response, "GET session file")
        )

    def content(
        self,
        session_id: str,
        file_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> bytes:
        response = self._http.get(
            session_files_url(self._base, session_id, f"/{quote(file_id, safe='')}/content"),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return response.content

    def download(
        self,
        session_id: str,
        file_id: str,
        to_path: str | PathLike[str],
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> pathlib.Path:
        content = self.content(
            session_id,
            file_id,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        path = pathlib.Path(to_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def delete(
        self,
        session_id: str,
        file_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResourceDeleted:
        response = self._http.delete(
            session_files_url(self._base, session_id, f"/{quote(file_id, safe='')}"),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return SessionResourceDeleted.model_validate(
            require_json_object(response, "DELETE session file")
        )

    def copy(
        self,
        session_id: str,
        *,
        source_session_id: str,
        file_ids: Sequence[str],
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> CopyFilesResponse:
        body = CopyFilesRequest(source_session_id=source_session_id, file_ids=list(file_ids))
        response = self._http.post(
            session_files_url(self._base, session_id) + ":copy",
            params=extra_query,
            json=body.model_dump(mode="json"),
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return CopyFilesResponse.model_validate(
            require_json_object(response, "POST copy session files")
        )


class _RawSyncSessionsResource:
    def __init__(self, sessions: SyncSessionsResource) -> None:
        self._sessions = sessions

    def retrieve(
        self,
        session_id: str,
        *,
        include_items: bool = True,
        include_liveness: bool = True,
        refresh_state: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> APIResponse[SessionResponse]:
        response = self._sessions._retrieve_response(
            session_id,
            include_items=include_items,
            include_liveness=include_liveness,
            refresh_state=refresh_state,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        return APIResponse(response, self._sessions._parse_retrieve)


class SyncSessionsResource:
    """Handwritten native-sync client for existing session routes."""

    def __init__(self, http: httpx.Client, base_url: str) -> None:
        self._http, self._base = http, base_url
        self.events = SyncEventsResource(self)
        self.items = SyncItemsResource(self)
        self.subagents = SyncSubagentsResource(self)
        self.agent = SyncSessionAgentResource(self)
        self.elicitations = SyncElicitationsResource(self)
        self.files = SyncSessionFilesResource(http, base_url)
        self.with_raw_response = _RawSyncSessionsResource(self)

    @overload
    def create(
        self,
        bundle: bytes,
        *,
        filename: str = "agent.tar.gz",
        title: str | None = None,
        project_id: str | None = None,
        labels: Mapping[str, str] | None = None,
        reasoning_effort: str | None = None,
        host_id: str | None = None,
        workspace: str | None = None,
        terminal_launch_args: Sequence[str] | None = None,
        parent_session_id: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        sandbox_provider: str | None = None,
        input: CreateSessionInput | None = None,
        stream: Literal[False] = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResponse: ...

    @overload
    def create(
        self,
        bundle: bytes,
        *,
        filename: str = "agent.tar.gz",
        title: str | None = None,
        project_id: str | None = None,
        labels: Mapping[str, str] | None = None,
        reasoning_effort: str | None = None,
        host_id: str | None = None,
        workspace: str | None = None,
        terminal_launch_args: Sequence[str] | None = None,
        parent_session_id: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        sandbox_provider: str | None = None,
        input: CreateSessionInput | None = None,
        stream: Literal[True],
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionEventStream: ...

    @overload
    def create(
        self,
        *,
        agent_id: str | None = None,
        project_id: str | None = None,
        initial_items: Sequence[PublicSessionEventInput] | None = None,
        title: str | None = None,
        labels: Mapping[str, str] | None = None,
        parent_session_id: str | None = None,
        sub_agent_name: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        host_id: str | None = None,
        sandbox_provider: str | None = None,
        workspace: str | None = None,
        workspaces: Sequence[str] | None = None,
        git: SessionGitOptions | Mapping[str, object] | None = None,
        terminal_launch_args: Sequence[str] | None = None,
        model_override: str | None = None,
        reasoning_effort: str | None = None,
        cost_control_mode_override: Literal["on", "off"] | None = None,
        subagent_routing_override: Literal["on", "off"] | None = None,
        harness_override: str | None = None,
        smart_routing_message: str | None = None,
        input: CreateSessionInput | None = None,
        stream: Literal[False] = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResponse: ...

    @overload
    def create(
        self,
        *,
        agent_id: str | None = None,
        project_id: str | None = None,
        initial_items: Sequence[PublicSessionEventInput] | None = None,
        title: str | None = None,
        labels: Mapping[str, str] | None = None,
        parent_session_id: str | None = None,
        sub_agent_name: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        host_id: str | None = None,
        sandbox_provider: str | None = None,
        workspace: str | None = None,
        workspaces: Sequence[str] | None = None,
        git: SessionGitOptions | Mapping[str, object] | None = None,
        terminal_launch_args: Sequence[str] | None = None,
        model_override: str | None = None,
        reasoning_effort: str | None = None,
        cost_control_mode_override: Literal["on", "off"] | None = None,
        subagent_routing_override: Literal["on", "off"] | None = None,
        harness_override: str | None = None,
        smart_routing_message: str | None = None,
        input: CreateSessionInput | None = None,
        stream: Literal[True],
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionEventStream: ...

    def create(
        self,
        bundle: bytes | None = None,
        *,
        agent_id: str | None = None,
        filename: str | NotGiven = NOT_GIVEN,
        title: str | None = None,
        labels: Mapping[str, str] | None = None,
        reasoning_effort: str | None = None,
        workspace: str | None = None,
        host_type: Literal["external", "managed"] = "external",
        sandbox_provider: str | None = None,
        project_id: str | None = None,
        parent_session_id: str | None = None,
        sub_agent_name: str | None = None,
        host_id: str | None = None,
        workspaces: Sequence[str] | None = None,
        git: SessionGitOptions | Mapping[str, object] | None = None,
        terminal_launch_args: Sequence[str] | None = None,
        model_override: str | None = None,
        cost_control_mode_override: Literal["on", "off"] | None = None,
        subagent_routing_override: Literal["on", "off"] | None = None,
        harness_override: str | None = None,
        smart_routing_message: str | None = None,
        initial_items: Sequence[PublicSessionEventInput | Mapping[str, Any]] | None = None,
        input: CreateSessionInput | None = None,
        stream: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResponse | SessionEventStream:
        if input is not None and initial_items is not None:
            raise ValueError("input and initial_items are mutually exclusive")
        normalized_input = normalize_create_input(input)
        if bundle is None and agent_id is None and project_id is None:
            raise ValueError("Pass bundle, agent_id, or project_id")
        if bundle is None and not isinstance(filename, NotGiven):
            raise ValueError("filename is only valid with bundle create")
        if bundle is not None and agent_id is not None:
            raise ValueError("bundle and agent_id are mutually exclusive")
        if bundle is None:
            fields = present(
                agent_id=agent_id if agent_id is not None else NOT_GIVEN,
                project_id=project_id if project_id is not None else NOT_GIVEN,
                title=title if title is not None else NOT_GIVEN,
                labels=labels if labels is not None else NOT_GIVEN,
                parent_session_id=parent_session_id
                if parent_session_id is not None
                else NOT_GIVEN,
                sub_agent_name=sub_agent_name if sub_agent_name is not None else NOT_GIVEN,
                host_type=host_type,
                host_id=host_id if host_id is not None else NOT_GIVEN,
                sandbox_provider=sandbox_provider if sandbox_provider is not None else NOT_GIVEN,
                workspace=workspace if workspace is not None else NOT_GIVEN,
                workspaces=workspaces if workspaces is not None else NOT_GIVEN,
                git=git if git is not None else NOT_GIVEN,
                terminal_launch_args=terminal_launch_args
                if terminal_launch_args is not None
                else NOT_GIVEN,
                model_override=model_override if model_override is not None else NOT_GIVEN,
                reasoning_effort=reasoning_effort if reasoning_effort is not None else NOT_GIVEN,
                cost_control_mode_override=cost_control_mode_override
                if cost_control_mode_override is not None
                else NOT_GIVEN,
                subagent_routing_override=subagent_routing_override
                if subagent_routing_override is not None
                else NOT_GIVEN,
                harness_override=harness_override if harness_override is not None else NOT_GIVEN,
                smart_routing_message=smart_routing_message
                if smart_routing_message is not None
                else NOT_GIVEN,
                initial_items=initial_items if initial_items is not None else NOT_GIVEN,
            )
            response = self._http.post(
                sessions_url(self._base),
                json=serialize_registered_create(fields),
                params=extra_query,
                **request_options(timeout, extra_headers),
            )
            raise_for_status(response.status_code, response_body(response))
            created = SessionResponse.model_validate(
                require_json_object(response, "POST /v1/sessions")
            )
            return self._complete_create(
                created.id,
                created_session=created,
                input=normalized_input,
                stream=stream,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            )

        registered_only = {
            "sub_agent_name": sub_agent_name,
            "workspaces": workspaces,
            "git": git,
            "model_override": model_override,
            "cost_control_mode_override": cost_control_mode_override,
            "subagent_routing_override": subagent_routing_override,
            "harness_override": harness_override,
            "smart_routing_message": smart_routing_message,
            "initial_items": initial_items,
        }
        invalid = [name for name, value in registered_only.items() if value is not None]
        if invalid:
            raise ValueError(f"bundle create does not support: {', '.join(invalid)}")
        metadata = serialize_bundle_metadata(
            {
                "title": title,
                "project_id": project_id,
                "labels": labels,
                "reasoning_effort": reasoning_effort,
                "host_id": host_id,
                "workspace": workspace,
                "terminal_launch_args": terminal_launch_args,
                "parent_session_id": parent_session_id,
                "host_type": host_type if host_type != "external" else None,
                "sandbox_provider": sandbox_provider,
            }
        )
        wire_filename = "agent.tar.gz" if isinstance(filename, NotGiven) else filename
        response = self._http.post(
            sessions_url(self._base),
            data={"metadata": json.dumps(metadata)},
            files={"bundle": (wire_filename, bundle, "application/gzip")},
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        session_id = str(require_json_object(response, "POST /v1/sessions")["session_id"])
        return self._complete_create(
            session_id,
            created_session=None,
            input=normalized_input,
            stream=stream,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )

    def _complete_create(
        self,
        session_id: str,
        *,
        created_session: SessionResponse | None,
        input: SessionMessage | list[SessionMessage] | None,
        stream: bool,
        timeout: Timeout,
        extra_headers: Headers,
        extra_query: Query,
    ) -> SessionResponse | SessionEventStream:
        if stream:
            try:
                iterator = self._open_stream_ready(
                    session_id,
                    timeout=timeout,
                    extra_headers=extra_headers,
                    extra_query=extra_query,
                )
            except Exception as exc:
                raise SessionCompositionError(
                    phase="stream_open", session_id=session_id, original_exception=exc
                ) from exc
            events = SessionEventStream(self, session_id, iterator=iterator)
            if input is None:
                return events
            try:
                self.events.create(
                    session_id,
                    events=input,
                    timeout=timeout,
                    extra_headers=extra_headers,
                    extra_query=extra_query,
                )
            except BaseException as exc:
                events.close()
                if isinstance(exc, Exception):
                    raise SessionCompositionError(
                        phase="input_submit", session_id=session_id, original_exception=exc
                    ) from exc
                raise
            return events
        if input is None and created_session is not None:
            return created_session
        if input is not None:
            try:
                self.events.create(
                    session_id,
                    events=input,
                    timeout=timeout,
                    extra_headers=extra_headers,
                    extra_query=extra_query,
                )
            except Exception as exc:
                raise SessionCompositionError(
                    phase="input_submit", session_id=session_id, original_exception=exc
                ) from exc
        try:
            return self.retrieve(
                session_id, timeout=timeout, extra_headers=extra_headers, extra_query=extra_query
            )
        except Exception as exc:
            raise SessionCompositionError(
                phase="snapshot_retrieve", session_id=session_id, original_exception=exc
            ) from exc

    def list(
        self,
        *,
        limit: int = 20,
        after: str | None = None,
        before: str | None = None,
        agent_id: str | None = None,
        agent_name: str | None = None,
        order: Literal["asc", "desc"] = "desc",
        sort_by: Literal["created_at", "updated_at"] = "created_at",
        include_archived: bool = False,
        search_query: str | None = None,
        kind: Literal["default", "sub_agent", "any"] = "default",
        project: str | None = None,
        pinned: bool = False,
        visibility: Literal["all", "mine", "shared", "archived"] = "all",
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SyncCursorPage[SessionListItem]:
        params = query_params(
            extra_query,
            limit=limit,
            after=after,
            before=before,
            agent_id=agent_id,
            agent_name=agent_name,
            order=order,
            sort_by=sort_by,
            search_query=search_query,
            project=project,
            include_archived="true" if include_archived else None,
            kind=kind if kind != "default" else None,
            pinned="true" if pinned else None,
            visibility=visibility if visibility != "all" else None,
        )
        response = self._http.get(
            sessions_url(self._base), params=params, **request_options(timeout, extra_headers)
        )
        raise_for_status(response.status_code, response_body(response))
        page = SessionList.model_validate(require_json_object(response, "GET /v1/sessions"))
        return SyncCursorPage(
            page.data,
            first_id=page.first_id,
            last_id=page.last_id,
            has_more=page.has_more,
            fetch_next_page=lambda cursor: self.list(
                limit=limit,
                after=cursor,
                before=before,
                agent_id=agent_id,
                agent_name=agent_name,
                order=order,
                sort_by=sort_by,
                search_query=search_query,
                include_archived=include_archived,
                kind=kind,
                project=project,
                pinned=pinned,
                visibility=visibility,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            ),
        )

    def _retrieve_response(
        self,
        session_id: str,
        *,
        include_items: bool = True,
        include_liveness: bool = True,
        refresh_state: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> httpx.Response:
        params = query_params(
            extra_query,
            include_items="false" if not include_items else None,
            include_liveness="false" if not include_liveness else None,
            refresh_state="true" if refresh_state else None,
        )
        return self._http.get(
            sessions_url(self._base, session_id),
            params=params or None,
            **request_options(timeout, extra_headers),
        )

    @staticmethod
    def _parse_retrieve(response: httpx.Response) -> SessionResponse:
        raise_for_status(response.status_code, response_body(response))
        return SessionResponse.model_validate(
            require_json_object(response, "GET /v1/sessions/{id}")
        )

    def retrieve(
        self,
        session_id: str,
        *,
        include_items: bool = True,
        include_liveness: bool = True,
        refresh_state: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResponse:
        return self._parse_retrieve(
            self._retrieve_response(
                session_id,
                include_items=include_items,
                include_liveness=include_liveness,
                refresh_state=refresh_state,
                timeout=timeout,
                extra_headers=extra_headers,
                extra_query=extra_query,
            )
        )

    def get(self, session_id: str) -> SessionResponse:
        return self.retrieve(session_id)

    def update(
        self,
        session_id: str,
        *,
        runner_id: str | None | NotGiven = NOT_GIVEN,
        title: str | None | NotGiven = NOT_GIVEN,
        labels: Mapping[str, str] | None | NotGiven = NOT_GIVEN,
        reasoning_effort: str | None | NotGiven = NOT_GIVEN,
        model_override: str | None | NotGiven = NOT_GIVEN,
        collaboration_mode: str | None | NotGiven = NOT_GIVEN,
        permission_mode: str | None | NotGiven = NOT_GIVEN,
        approval_mode: str | None | NotGiven = NOT_GIVEN,
        cost_control_mode_override: Literal["on", "off"] | None | NotGiven = NOT_GIVEN,
        subagent_routing_override: Literal["on", "off"] | None | NotGiven = NOT_GIVEN,
        share_workspace_files: bool | None | NotGiven = NOT_GIVEN,
        external_session_id: str | None | NotGiven = NOT_GIVEN,
        terminal_launch_args: Sequence[str] | None | NotGiven = NOT_GIVEN,
        archived: bool | None | NotGiven = NOT_GIVEN,
        project_id: str | None | NotGiven = NOT_GIVEN,
        silent: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResponse:
        fields = dict(locals())
        fields.pop("self")
        fields.pop("session_id")
        fields.pop("timeout")
        fields.pop("extra_headers")
        fields.pop("extra_query")
        response = self._http.patch(
            sessions_url(self._base, session_id),
            json=serialize_update(fields),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        return self._parse_retrieve(response)

    def delete(
        self,
        session_id: str,
        *,
        delete_branch: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> ConversationDeleted:
        response = self._http.delete(
            sessions_url(self._base, session_id),
            params=query_params(extra_query, delete_branch=str(delete_branch).lower()),
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        return ConversationDeleted.model_validate(require_json_object(response, "DELETE session"))

    def fork(
        self,
        source_session_id: str,
        *,
        title: str | None = None,
        agent_id: str | None = None,
        up_to_response_id: str | None = None,
        model_override: str | None | NotGiven = NOT_GIVEN,
        reasoning_effort: str | None | NotGiven = NOT_GIVEN,
        terminal_launch_args: Sequence[str] | None | NotGiven = NOT_GIVEN,
        codex_bypass_sandbox: bool = False,
        host_type: Literal["external", "managed"] = "external",
        sandbox_provider: str | None = None,
        workspace: str | None | NotGiven = NOT_GIVEN,
        side_chat: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> SessionResponse:
        fields = present(
            title=title if title is not None else NOT_GIVEN,
            agent_id=agent_id if agent_id is not None else NOT_GIVEN,
            up_to_response_id=up_to_response_id if up_to_response_id is not None else NOT_GIVEN,
            model_override=model_override,
            reasoning_effort=reasoning_effort,
            terminal_launch_args=terminal_launch_args,
            codex_bypass_sandbox=codex_bypass_sandbox if codex_bypass_sandbox else NOT_GIVEN,
            host_type=host_type if host_type != "external" else NOT_GIVEN,
            sandbox_provider=sandbox_provider if sandbox_provider is not None else NOT_GIVEN,
            workspace=workspace,
            side_chat=side_chat if side_chat else NOT_GIVEN,
        )
        response = self._http.post(
            sessions_url(self._base, source_session_id, "/fork"),
            json=serialize_fork(fields),
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        return self._parse_retrieve(response)

    def compact(
        self,
        session_id: str,
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> EventAcknowledgement:
        value = self._post_event_payload(
            session_id,
            {"type": "compact", "data": {}},
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        return EventAcknowledgement.model_validate(value)

    def _post_event_payload(
        self,
        session_id: str,
        payload: dict[str, Any] | builtins.list[dict[str, Any]],
        *,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> dict[str, Any] | builtins.list[dict[str, Any]]:
        response = self._http.post(
            sessions_url(self._base, session_id, "/events"),
            json=payload,
            params=extra_query,
            **request_options(timeout, extra_headers),
        )
        raise_for_status(response.status_code, response_body(response))
        value = response.json()
        if not isinstance(value, (dict, list)):
            raise OmnigentError("POST session events returned invalid JSON")
        return value

    def _stream(
        self,
        session_id: str,
        *,
        idle: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> Generator[SessionStreamEvent, None, None]:
        params = query_params(extra_query, idle="true" if idle else None)
        with self._http.stream(
            "GET",
            sessions_url(self._base, session_id, "/stream"),
            params=params or None,
            headers=extra_headers,
            timeout=_SSE_TIMEOUT if timeout is None else timeout,
        ) as response:
            if response.status_code >= 400:
                response.read()
                raise_for_status(response.status_code, response_body(response))
            if 300 <= response.status_code < 400:
                raise OmnigentError(
                    "stream open returned a 3xx response "
                    f"(status {response.status_code}) instead of an event stream",
                    response.status_code,
                )
            decoder = SessionSSEDecoder()
            for line in response.iter_lines():
                event, done = decoder.feed(line)
                if done:
                    return
                if event is not None:
                    yield event
            raise StreamProtocolError("session stream ended before the [DONE] marker")

    def _open_stream_ready(
        self,
        session_id: str,
        *,
        idle: bool = False,
        timeout: Timeout = None,
        extra_headers: Headers = None,
        extra_query: Query = None,
    ) -> Generator[SessionStreamEvent, None, None]:
        iterator = self._stream(
            session_id,
            idle=idle,
            timeout=timeout,
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        try:
            event = next(iterator)
            if event.type != STREAM_READY_EVENT_TYPE:
                raise OmnigentError(
                    "session stream did not begin with required "
                    f"{STREAM_READY_EVENT_TYPE!r}; received {event.type!r}"
                )
        except StopIteration as exc:
            iterator.close()
            raise OmnigentError(
                f"session stream closed before required {STREAM_READY_EVENT_TYPE!r}"
            ) from exc
        except BaseException:
            iterator.close()
            raise
        return iterator


__all__ = ["SessionEventStream", "SyncSessionsResource"]
