"""Sync counterpart of :class:`omnigent_client._sessions_chat.SessionsChat`.

Mirrors the async :class:`SessionsChat` public surface — ``send``,
``query``, ``stream``, ``tree_busy``, ``refresh``, ``cancel`` — but
delegates to :class:`omnigent_client._sync_sessions.SyncSessionsResource`
so callers in synchronous contexts (Databricks notebooks, scripts,
REPLs) get the same hooks, elicitation handling, client-side tool
dispatch, and sub-agent ``tree_busy`` rollup without standing up an
event loop.

Every protocol type, hook context, and helper is reused from the
existing OSS modules — nothing is copied or redefined:
- :class:`StreamHooks` and all ``*Ctx`` dataclasses from
  :mod:`omnigent_client._tool_handler`;
- :func:`child_summary_busy` and :data:`TERMINAL_TASK_STATUSES` from
  :mod:`omnigent_client._child_status`;
- :class:`QueryResult` from :mod:`omnigent_client._query`;
- :func:`_response_from_server_object`, :func:`_build_tool_call_info`,
  :class:`_StreamHookState`, :func:`_parse_hook_arguments`, and all
  wire-literal constants from :mod:`omnigent_client._sessions_chat`;
- typed events from :mod:`omnigent.protocol`.

Only the I/O boundary differs: ``send`` is a sync generator (not an
async iterator), hooks are called synchronously, and file upload /
elicitation resolution / tool-call dispatch go through the sync
namespace.
"""

from __future__ import annotations

from collections.abc import Generator, Iterator
from typing import Any, Literal, Protocol, overload

from omnigent.protocol import (
    CompletedEvent,
    ElicitationRequestEvent,
    OutputFileDoneEvent,
    OutputItemDoneEvent,
    OutputTextDeltaEvent,
    ReasoningStartedEvent,
    ReasoningSummaryTextDeltaEvent,
    ReasoningTextDeltaEvent,
    SessionChildSessionUpdatedEvent,
    SessionCreatedEvent,
    SessionStatusEvent,
)

from ._child_status import TERMINAL_TASK_STATUSES, child_summary_busy
from ._errors import OmnigentError
from ._query import QueryResult
from ._sessions_chat import (
    _ACTION_REQUIRED_STATUS,
    _FUNCTION_CALL_ITEM_TYPE,
    _FUNCTION_CALL_OUTPUT_TYPE,
    _MESSAGE_INPUT_TYPE,
    _OUTPUT_TEXT_BLOCK_TYPES,
    _RESPONSE_START_EVENT_TYPES,
    _RESPONSE_TERMINAL_EVENT_TYPES,
    _RUNTIME_CLIENT,
    SessionStreamEvent,
    SessionToolCallInfo,
    ToolCallable,
    _build_tool_call_info,
    _parse_hook_arguments,
    _response_from_server_object,
    _StreamHookState,
)
from ._sync_sessions import SyncSessionsResource
from ._tool_handler import (
    ElicitationRequestCtx,
    FileOutputCtx,
    MessageEndCtx,
    MessageStartCtx,
    ReasoningEndCtx,
    ReasoningStartCtx,
    ResponseEndCtx,
    ResponseStartCtx,
    StreamHooks,
    SubAgentCompletedCtx,
    SubAgentInfo,
    SubAgentSpawnedCtx,
    ToolCallEndCtx,
    ToolCallStartCtx,
)
from ._types import File, Response

# Wire ``type`` literal for function_call output items surfaced via
# OutputItemDoneEvent (re-exported so a single grep finds the match site).
_FUNCTION_CALL_OUTPUT_ITEM_TYPE: str = "function_call_output"


def _call_hook_sync(hook: Any, ctx: Any) -> Any:
    """Call a sync hook and return its result (no await)."""
    if hook is None:
        return None
    return hook(ctx)


def _invoke_elicitation_hook_sync(hooks: StreamHooks, ctx: ElicitationRequestCtx) -> bool:
    """Invoke an elicitation hook synchronously, declining fail-closed on error."""
    if hooks.on_elicitation_request is None:
        return False
    try:
        return bool(_call_hook_sync(hooks.on_elicitation_request, ctx))
    except Exception:
        return False


def _invoke_callable_sync(callable_for_tool: ToolCallable, info: SessionToolCallInfo) -> str:
    """Invoke a sync tool callable and validate its string return."""
    output = callable_for_tool(info)
    if not isinstance(output, str):
        raise TypeError(
            f"tool_callable for {info.name!r} must return a str; got {type(output).__name__}"
        )
    return output


def _assistant_text_from_output_item(item: dict[str, Any]) -> list[str]:
    """Extract assistant text from a streamed output_item.done item."""
    if item.get("type") != "message" or item.get("role") != "assistant":
        return []
    return _assistant_text_from_content(item.get("content"))


def _assistant_text_from_response(output: list[dict[str, Any]]) -> list[str]:
    """Extract assistant text from a terminal response snapshot."""
    parts: list[str] = []
    for item in output:
        parts.extend(_assistant_text_from_output_item(item))
    return parts


def _assistant_text_from_content(raw_content: object) -> list[str]:
    """Extract text from an assistant message content list."""
    if not isinstance(raw_content, list):
        return []
    parts: list[str] = []
    for block in raw_content:
        if not isinstance(block, dict):
            continue
        if block.get("type") not in _OUTPUT_TEXT_BLOCK_TYPES:
            continue
        text = block.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    return parts


class SyncQueryStream:
    """Sync-iterable of text chunks from ``query(stream=True)``.

    Iterating yields ``str`` chunks in order. After the iterator is
    exhausted, :attr:`files` holds the file artifacts produced this turn.
    Single-use: iterating a second time raises ``RuntimeError``.
    """

    def __init__(self, chunks: Iterator[str], files: list[File]) -> None:
        self._chunks = chunks
        self._files = files
        self._consumed = False

    def __iter__(self) -> Iterator[str]:
        if self._consumed:
            raise RuntimeError(
                "SyncQueryStream has already been iterated. Each stream is single-use."
            )
        self._consumed = True
        return self._chunks

    @property
    def files(self) -> list[File]:
        return list(self._files)


class _SyncFilesUploader(Protocol):
    def __call__(self, path: str) -> File: ...


class _SyncFilesGetter(Protocol):
    def __call__(self, file_id: str) -> File: ...


class _SyncAgentToolsGetter(Protocol):
    def __call__(self, agent_id: str, session_id: str | None = None) -> list[dict[str, Any]]: ...


class SyncSessionsChat:
    """Synchronous sessions-API chat helper bound to a single durable session.

    Mirrors :class:`omnigent_client.SessionsChat` but uses the sync
    :class:`SyncSessionsResource` so no event loop is required::

        chat = client.sessions_chat(agent_id="ag_abc123", hooks=hooks)
        for event in chat.send("hello"):
            ...
        result = chat.query("summarize")
        print(result.text)

    Hooks are fired synchronously. If a hook is a coroutine function it
    will not be awaited — use sync callbacks with this helper.

    :param namespace: The :class:`SyncSessionsResource` this helper
        delegates to.
    :param session: The :class:`SessionResponse` snapshot returned by
        :meth:`SyncSessionsResource.create`.
    :param files_uploader: Optional sync callable that uploads a local
        path and returns a :class:`File`.
    :param files_getter: Optional sync callable that fetches a
        :class:`File` by id.
    :param tool_callables: Optional mapping from tool name to a sync
        callable. Required iff the agent spec declares
        ``runtime: "client"`` tools.
    :param agent_tools_getter: Optional sync callable returning the
        agent's tool entries.
    :param hooks: Optional :class:`StreamHooks` fired from stream events.
    """

    def __init__(
        self,
        namespace: SyncSessionsResource,
        session: Any,
        *,
        files_uploader: _SyncFilesUploader | None = None,
        files_getter: _SyncFilesGetter | None = None,
        tool_callables: dict[str, ToolCallable] | None = None,
        agent_tools_getter: _SyncAgentToolsGetter | None = None,
        hooks: StreamHooks | None = None,
    ) -> None:
        self._namespace = namespace
        self._session = session
        self._files_uploader = files_uploader
        self._files_getter = files_getter
        self._tool_callables: dict[str, ToolCallable] = (
            dict(tool_callables) if tool_callables else {}
        )
        self._agent_tools_getter = agent_tools_getter
        self._hooks = hooks or StreamHooks()
        self._tool_callables_validated = False

    # ── Factory classmethods ────────────────────────────────────────

    @classmethod
    def create_registered(
        cls,
        namespace: SyncSessionsResource,
        *,
        agent_id: str,
        files_uploader: _SyncFilesUploader | None = None,
        files_getter: _SyncFilesGetter | None = None,
        tool_callables: dict[str, ToolCallable] | None = None,
        agent_tools_getter: _SyncAgentToolsGetter | None = None,
        hooks: StreamHooks | None = None,
        **create_kwargs: Any,
    ) -> SyncSessionsChat:
        """Create a chat helper bound to a new registered-agent session.

        Calls :meth:`SyncSessionsResource.create` with the given
        ``agent_id`` and wraps the resulting session. Pass-through
        kwargs (``host_type``, ``timeout``, etc.) are forwarded to
        ``create``.

        :param namespace: The sync sessions resource.
        :param agent_id: Durable agent identifier, e.g. ``"ag_abc123"``.
        :returns: A :class:`SyncSessionsChat` ready for use.
        """
        session = namespace.create(agent_id=agent_id, stream=False, **create_kwargs)
        return cls(
            namespace=namespace,
            session=session,
            files_uploader=files_uploader,
            files_getter=files_getter,
            tool_callables=tool_callables,
            agent_tools_getter=agent_tools_getter,
            hooks=hooks,
        )

    @classmethod
    def create(
        cls,
        namespace: SyncSessionsResource,
        bundle: bytes,
        *,
        filename: str = "agent.tar.gz",
        files_uploader: _SyncFilesUploader | None = None,
        files_getter: _SyncFilesGetter | None = None,
        tool_callables: dict[str, ToolCallable] | None = None,
        agent_tools_getter: _SyncAgentToolsGetter | None = None,
        hooks: StreamHooks | None = None,
        **create_kwargs: Any,
    ) -> SyncSessionsChat:
        """Create a chat helper bound to a new uploaded-bundle session.

        :param namespace: The sync sessions resource.
        :param bundle: Gzipped agent tarball bytes.
        :param filename: Filename for the multipart upload.
        :returns: A :class:`SyncSessionsChat` ready for use.
        """
        session = namespace.create(bundle, filename=filename, stream=False, **create_kwargs)
        return cls(
            namespace=namespace,
            session=session,
            files_uploader=files_uploader,
            files_getter=files_getter,
            tool_callables=tool_callables,
            agent_tools_getter=agent_tools_getter,
            hooks=hooks,
        )

    # ── Properties ────────────────────────────────────────────────────

    @property
    def session_id(self) -> str:
        return self._session.id

    @property
    def agent_id(self) -> str:
        return self._session.agent_id

    @property
    def status(self) -> str:
        return self._session.status

    # ── Lifecycle ────────────────────────────────────────────────────

    def refresh(self) -> Any:
        """Fetch a fresh session snapshot and update internal state."""
        self._session = self._namespace.get(self._session.id)
        return self._session

    def tree_busy(self, *, max_depth: int = 3) -> bool:
        """Whether any sub-agent anywhere in this session subtree is working.

        Delegates to :meth:`SyncSessionsResource.subtree_busy`.
        """
        return self._namespace.subtree_busy(self._session.id, max_depth=max_depth)

    def cancel(self) -> None:
        """Interrupt the running turn (if any)."""
        self._namespace.events.cancel(self._session.id)

    # ── send ─────────────────────────────────────────────────────────

    def send(
        self,
        input: str | list[dict[str, Any]],
        *,
        files: list[str] | None = None,
    ) -> Generator[SessionStreamEvent, None, None]:
        """Post a user message and yield typed events for the turn.

        Opens a fresh SSE subscription, posts the message event, and
        yields each typed event until a turn-terminal event arrives.
        Hooks are fired synchronously; client-side tool calls are
        dispatched inline.

        :param input: User text or content-block list.
        :param files: Optional local paths to upload and attach.
        :yields: Typed :data:`SessionStreamEvent` instances.
        """
        self._validate_tool_callables()

        content = self._build_content(input, files)
        event_payload = {
            "type": _MESSAGE_INPUT_TYPE,
            "data": {"role": "user", "content": content},
        }

        stream_iter = self._namespace._open_stream_ready(self._session.id)
        hook_state = _StreamHookState()
        try:
            self._namespace.events.create(self._session.id, events=event_payload)
            while True:
                try:
                    event = next(stream_iter)
                except StopIteration:
                    return
                self._fire_stream_hooks(event, hook_state)
                yield event
                if isinstance(event, OutputItemDoneEvent):
                    self._maybe_dispatch_tool_call(event, hook_state)
                if isinstance(event, _RESPONSE_TERMINAL_EVENT_TYPES):
                    return
                if isinstance(event, SessionStatusEvent) and event.status == "failed":
                    message = (
                        event.error.message
                        if event.error is not None and event.error.message
                        else "turn failed"
                    )
                    code = event.error.code if event.error is not None else None
                    raise OmnigentError(message, code=code)
        finally:
            stream_iter.close()

    def stream(self) -> Generator[SessionStreamEvent, None, None]:
        """Subscribe to the live SSE stream without posting a message.

        Validates tool callables, opens the stream, fires hooks, and
        dispatches client-side tool calls. Iteration ends when the
        server closes the stream.
        """
        self._validate_tool_callables()
        stream_iter = self._namespace._open_stream_ready(self._session.id)
        hook_state = _StreamHookState()
        try:
            for event in stream_iter:
                self._fire_stream_hooks(event, hook_state)
                yield event
                if isinstance(event, OutputItemDoneEvent):
                    self._maybe_dispatch_tool_call(event, hook_state)
        finally:
            stream_iter.close()

    # ── query ────────────────────────────────────────────────────────

    @overload
    def query(
        self,
        input: str | list[dict[str, Any]],
        *,
        files: list[str] | None = ...,
        stream: Literal[False] = ...,
    ) -> QueryResult: ...

    @overload
    def query(
        self,
        input: str | list[dict[str, Any]],
        *,
        files: list[str] | None = ...,
        stream: Literal[True],
    ) -> SyncQueryStream: ...

    def query(
        self,
        input: str | list[dict[str, Any]],
        *,
        files: list[str] | None = None,
        stream: bool = False,
    ) -> QueryResult | SyncQueryStream:
        """Send a turn and collect (or stream) the assistant text output.

        Non-streaming (default) returns a :class:`QueryResult`::

            result = chat.query("make me a chart")
            print(result.text)

        Streaming returns a :class:`SyncQueryStream`::

            for chunk in chat.query("hello", stream=True):
                print(chunk, end="", flush=True)
        """
        if stream:
            return self._stream_query(input, files=files)
        return self._collect_query(input, files=files)

    def _collect_query(
        self, input: str | list[dict[str, Any]], *, files: list[str] | None
    ) -> QueryResult:
        text_parts: list[str] = []
        produced: list[File] = []
        for event in self.send(input, files=files):
            if isinstance(event, OutputTextDeltaEvent):
                if event.delta:
                    text_parts.append(event.delta)
            elif isinstance(event, OutputItemDoneEvent) and not text_parts:
                text_parts.extend(_assistant_text_from_output_item(event.item))
            elif isinstance(event, OutputFileDoneEvent):
                produced.append(self._fetch_file(event))
            elif isinstance(event, CompletedEvent):
                if not text_parts:
                    text_parts.extend(_assistant_text_from_response(event.response.output))
        return QueryResult(text="".join(text_parts), files=produced)

    def _stream_query(
        self, input: str | list[dict[str, Any]], *, files: list[str] | None
    ) -> SyncQueryStream:
        produced: list[File] = []

        def _gen() -> Iterator[str]:
            text_started = False
            for event in self.send(input, files=files):
                if isinstance(event, OutputTextDeltaEvent):
                    if event.delta:
                        text_started = True
                        yield event.delta
                elif isinstance(event, OutputItemDoneEvent) and not text_started:
                    for t in _assistant_text_from_output_item(event.item):
                        text_started = True
                        yield t
                elif isinstance(event, OutputFileDoneEvent):
                    produced.append(self._fetch_file(event))

        return SyncQueryStream(_gen(), produced)

    # ── Tool-callable validation ─────────────────────────────────────

    def _validate_tool_callables(self) -> None:
        if self._tool_callables_validated:
            return
        if not self._tool_callables and self._agent_tools_getter is None:
            self._tool_callables_validated = True
            return
        if self._tool_callables and self._agent_tools_getter is None:
            raise RuntimeError(
                "SyncSessionsChat received tool_callables but no agent_tools_getter was wired."
            )
        tools = self._agent_tools_getter(self._session.agent_id, self._session.id)
        client_tool_names = {
            entry.get("name")
            for entry in tools
            if entry.get("runtime") == _RUNTIME_CLIENT
            and isinstance(entry.get("name"), str)
            and entry.get("name")
        }
        missing = client_tool_names - set(self._tool_callables.keys())
        extra = set(self._tool_callables.keys()) - client_tool_names
        if missing or extra:
            parts: list[str] = []
            if missing:
                parts.append("missing: " + ", ".join(sorted(missing)))
            if extra:
                parts.append("extra: " + ", ".join(sorted(extra)))
            raise ValueError("SyncSessionsChat tool_callables mismatch: " + "; ".join(parts))
        self._tool_callables_validated = True

    # ── Content building ─────────────────────────────────────────────

    def _build_content(
        self, input: str | list[dict[str, Any]], files: list[str] | None
    ) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        if isinstance(input, str):
            blocks.append({"type": "input_text", "text": input})
        elif isinstance(input, list):
            blocks.extend(input)
        if files:
            if self._files_uploader is None:
                raise RuntimeError("files= provided but no files_uploader wired")
            for path in files:
                uploaded = self._files_uploader(path)
                blocks.append(
                    {
                        "type": "input_file",
                        "file_id": uploaded.id,
                        "filename": uploaded.filename,
                    }
                )
        return blocks

    def _fetch_file(self, event: OutputFileDoneEvent) -> File:
        if self._files_getter is None:
            raise RuntimeError("file artifact observed but no files_getter wired")
        return self._files_getter(event.file_id)

    # ── Tool-call dispatch ────────────────────────────────────────────

    def _maybe_dispatch_tool_call(
        self, event: OutputItemDoneEvent, hook_state: _StreamHookState
    ) -> None:
        item = event.item
        if item.get("type") != _FUNCTION_CALL_ITEM_TYPE:
            return
        if item.get("status") != _ACTION_REQUIRED_STATUS:
            return
        info = _build_tool_call_info(item)
        callable_for_tool = self._tool_callables.get(info.name)
        if callable_for_tool is None:
            raise ValueError(
                f"SyncSessionsChat received an action_required "
                f"function_call for tool {info.name!r} but no "
                f"callable is registered."
            )
        output_str = _invoke_callable_sync(callable_for_tool, info)
        hook_state.completed_tool_call_ids.add(info.call_id)
        _call_hook_sync(
            self._hooks.on_tool_call_end,
            ToolCallEndCtx(
                name=info.name,
                call_id=info.call_id,
                agent_name="",
                output=output_str,
            ),
        )
        self._namespace.events.create(
            self._session.id,
            events={
                "type": _FUNCTION_CALL_OUTPUT_TYPE,
                "data": {"call_id": info.call_id, "output": output_str},
            },
        )

    # ── Hook firing ──────────────────────────────────────────────────

    def _fire_stream_hooks(self, event: SessionStreamEvent, state: _StreamHookState) -> None:
        """Translate sessions SSE events into StreamHooks callbacks (sync)."""
        if isinstance(event, _RESPONSE_START_EVENT_TYPES):
            response = _response_from_server_object(event.response)
            self._ensure_response_started(response, state)
            return

        if isinstance(event, ReasoningStartedEvent):
            state.in_reasoning = True
            state.reasoning_text = ""
            state.reasoning_summary_text = ""
            _call_hook_sync(self._hooks.on_reasoning_start, ReasoningStartCtx())
            return

        if isinstance(event, ReasoningTextDeltaEvent):
            state.reasoning_text += event.delta
            return

        if isinstance(event, ReasoningSummaryTextDeltaEvent):
            state.reasoning_summary_text += event.delta
            return

        if isinstance(event, OutputTextDeltaEvent):
            self._end_reasoning_if_open(state)
            if not state.message_started:
                state.message_started = True
                _call_hook_sync(
                    self._hooks.on_message_start,
                    MessageStartCtx(response_id=state.current_response_id),
                )
            return

        if isinstance(event, OutputItemDoneEvent):
            self._fire_output_item_hooks(event.item, state)
            return

        if isinstance(event, OutputFileDoneEvent):
            _call_hook_sync(
                self._hooks.on_file_output,
                FileOutputCtx(
                    file_id=event.file_id,
                    filename=event.filename,
                    content_type=event.content_type,
                ),
            )
            return

        if isinstance(event, ElicitationRequestEvent):
            self._handle_elicitation_request(event, state)
            return

        if isinstance(event, SessionCreatedEvent):
            self._fire_sub_agent_spawned(event, state)
            return

        if isinstance(event, SessionChildSessionUpdatedEvent):
            self._fire_sub_agent_completed_if_terminal(event, state)
            return

        if isinstance(event, _RESPONSE_TERMINAL_EVENT_TYPES):
            self._end_reasoning_if_open(state)
            response = _response_from_server_object(event.response)
            self._ensure_response_started(response, state)
            _call_hook_sync(
                self._hooks.on_response_end,
                ResponseEndCtx(response=response, status=response.status),
            )

    def _fire_sub_agent_spawned(self, event: SessionCreatedEvent, state: _StreamHookState) -> None:
        child_id = event.child_session_id
        if not child_id or child_id in state.spawned_child_ids:
            return
        parent_id = event.parent_session_id
        if parent_id and parent_id != self._session.id:
            return
        state.spawned_child_ids.add(child_id)
        agent_name = state.child_agent_names.get(child_id) or (event.agent_id or "")
        _call_hook_sync(
            self._hooks.on_sub_agent_spawned,
            SubAgentSpawnedCtx(
                parent_response_id=state.current_response_id,
                sub_agents=[SubAgentInfo(response_id=child_id, agent_name=agent_name)],
            ),
        )

    def _fire_sub_agent_completed_if_terminal(
        self, event: SessionChildSessionUpdatedEvent, state: _StreamHookState
    ) -> None:
        child_id = event.child_session_id
        if not child_id:
            return
        child = event.child if isinstance(event.child, dict) else {}
        raw_name = child.get("tool") or child.get("agent_name")
        if isinstance(raw_name, str) and raw_name:
            state.child_agent_names[child_id] = raw_name
        raw_preview = child.get("last_message_preview")
        if isinstance(raw_preview, str) and raw_preview:
            state.child_previews[child_id] = raw_preview
        if child_id not in state.spawned_child_ids or child_id in state.completed_child_ids:
            return
        status = child.get("current_task_status")
        if not isinstance(status, str) or status not in TERMINAL_TASK_STATUSES:
            return
        if child_summary_busy(child):
            return
        state.completed_child_ids.add(child_id)
        _call_hook_sync(
            self._hooks.on_sub_agent_completed,
            SubAgentCompletedCtx(
                response_id=child_id,
                agent_name=state.child_agent_names.get(child_id, ""),
                status=status,
                output_summary=state.child_previews.get(child_id),
            ),
        )

    def _ensure_response_started(self, response: Response, state: _StreamHookState) -> None:
        if response.id in state.started_response_ids:
            state.current_response_id = response.id
            return
        state.started_response_ids.add(response.id)
        state.current_response_id = response.id
        _call_hook_sync(self._hooks.on_response_start, ResponseStartCtx(response=response))

    def _end_reasoning_if_open(self, state: _StreamHookState) -> None:
        if not state.in_reasoning:
            return
        state.in_reasoning = False
        _call_hook_sync(
            self._hooks.on_reasoning_end,
            ReasoningEndCtx(
                reasoning_text=state.reasoning_text,
                summary_text=state.reasoning_summary_text,
            ),
        )

    def _fire_output_item_hooks(self, item: dict[str, Any], state: _StreamHookState) -> None:
        item_type = item.get("type")
        if item_type == "message":
            self._end_reasoning_if_open(state)
            if not state.message_started:
                state.message_started = True
                _call_hook_sync(
                    self._hooks.on_message_start,
                    MessageStartCtx(response_id=state.current_response_id),
                )
            raw_content = item.get("content")
            content = raw_content if isinstance(raw_content, list) else []
            _call_hook_sync(self._hooks.on_message_end, MessageEndCtx(content=content))
            state.message_started = False
            return

        if item_type == _FUNCTION_CALL_ITEM_TYPE:
            _call_hook_sync(
                self._hooks.on_tool_call_start,
                ToolCallStartCtx(
                    name=str(item.get("name", "")),
                    arguments=_parse_hook_arguments(item.get("arguments", "{}")),
                    call_id=str(item.get("call_id", "")),
                    agent_name=str(item.get("agent_name", "")),
                    executed_by=(
                        "client" if item.get("status") == _ACTION_REQUIRED_STATUS else "server"
                    ),
                ),
            )
            return

        if item_type == _FUNCTION_CALL_OUTPUT_ITEM_TYPE:
            call_id = str(item.get("call_id", ""))
            if call_id in state.completed_tool_call_ids:
                return
            _call_hook_sync(
                self._hooks.on_tool_call_end,
                ToolCallEndCtx(
                    name=str(item.get("name", "")),
                    call_id=call_id,
                    agent_name=str(item.get("agent_name", "")),
                    output=str(item.get("output", "")),
                ),
            )

    def _handle_elicitation_request(
        self, event: ElicitationRequestEvent, state: _StreamHookState
    ) -> None:
        params = event.params
        accepted = _invoke_elicitation_hook_sync(
            self._hooks,
            ElicitationRequestCtx(
                elicitation_id=event.elicitation_id,
                message=params.message,
                requested_schema=params.requestedSchema or {},
                mode=params.mode,
                phase=params.phase or "",
                policy_name=params.policy_name or "",
                content_preview=params.content_preview or "",
                response_id=state.current_response_id,
                url=params.url,
            ),
        )
        target_session_id = params.target_session_id or self._session.id
        self._namespace.elicitations.resolve(
            target_session_id,
            event.elicitation_id,
            action="accept" if accepted else "decline",
        )


__all__ = ["SessionToolCallInfo", "SyncQueryStream", "SyncSessionsChat", "ToolCallable"]
