"""Typed sync and async clients for the OmniGent HTTP and SSE APIs.

The primary interface is the ``client.agents.sessions`` resource tree.
``AsyncOmnigent`` provides the same session resources with native async I/O.
Legacy Responses and root ``Session`` exports remain available for
compatibility. Block-stream transforms remain optional presentation helpers,
not foundations for the session-native transport.
"""

from omnigent.protocol import FunctionCallOutput, Interrupt, SessionMessage

from ._blocks import (
    AnyBlock,
    BlockContext,
    CompactionBlock,
    ErrorBlock,
    FileBlock,
    NativeToolBlock,
    ReasoningBlock,
    ReasoningChunk,
    ReasoningStartBlock,
    ResponseEndBlock,
    ResponseStartBlock,
    RetryBlock,
    StreamBlock,
    TextChunk,
    TextDone,
    ToolExecution,
    ToolGroup,
    ToolResultBlock,
)
from ._child_status import (
    TERMINAL_TASK_STATUSES,
    child_session_busy,
    child_summary_busy,
)
from ._client import OmnigentClient
from ._errors import (
    OmnigentError,
    RateLimitedError,
    SessionCompositionError,
    StaleCursorError,
    StreamProtocolError,
    ToolCallDenied,
)
from ._events import MCP_ELICITATION_METHOD, ElicitationRequest
from ._not_given import NOT_GIVEN, NotGiven
from ._pagination import AsyncCursorPage, SyncCursorPage
from ._query import QueryResult, QueryStream
from ._raw_response import APIResponse
from ._server import LocalServer
from ._session import Session
from ._sessions import (
    AsyncSessionEventStream,
    CreateSessionInput,
    RegisteredAgent,
    SessionsNamespace,
)
from ._sessions_chat import SessionsChat, SessionToolCallInfo, ToolCallable
from ._stream import BlockStream, format_tool_args_brief
from ._sync_client import Omnigent
from ._sync_sessions import SessionEventStream
from ._tool_handler import (
    ElicitationRequestCtx,
    StreamHooks,
    ToolCallInfo,
    ToolHandler,
)
from ._transforms import (
    merge_text_across_iterations,
    only_agent,
    pipe,
    skip_blocks,
    skip_intermediate_ends,
)
from ._types import File
from .tools import ToolMetadata, ToolState, tool

AsyncOmnigent = OmnigentClient

__all__ = [
    "MCP_ELICITATION_METHOD",
    "NOT_GIVEN",
    "TERMINAL_TASK_STATUSES",
    "APIResponse",
    "AnyBlock",
    "AsyncCursorPage",
    "AsyncOmnigent",
    "AsyncSessionEventStream",
    "BlockContext",
    "BlockStream",
    "CompactionBlock",
    "CreateSessionInput",
    "ElicitationRequest",
    "ElicitationRequestCtx",
    "ErrorBlock",
    "File",
    "FileBlock",
    "FunctionCallOutput",
    "Interrupt",
    "LocalServer",
    "NativeToolBlock",
    "NotGiven",
    "Omnigent",
    "OmnigentClient",
    "OmnigentError",
    "QueryResult",
    "QueryStream",
    "RateLimitedError",
    "ReasoningBlock",
    "ReasoningChunk",
    "ReasoningStartBlock",
    "RegisteredAgent",
    "ResponseEndBlock",
    "ResponseStartBlock",
    "RetryBlock",
    "Session",
    "SessionCompositionError",
    "SessionEventStream",
    "SessionMessage",
    "SessionToolCallInfo",
    "SessionsChat",
    "SessionsNamespace",
    "StaleCursorError",
    "StreamBlock",
    "StreamHooks",
    "StreamProtocolError",
    "SyncCursorPage",
    "TextChunk",
    "TextDone",
    "ToolCallDenied",
    "ToolCallInfo",
    "ToolCallable",
    "ToolExecution",
    "ToolGroup",
    "ToolHandler",
    "ToolMetadata",
    "ToolResultBlock",
    "ToolState",
    "child_session_busy",
    "child_summary_busy",
    "format_tool_args_brief",
    "merge_text_across_iterations",
    "only_agent",
    "pipe",
    "skip_blocks",
    "skip_intermediate_ends",
    "tool",
]
