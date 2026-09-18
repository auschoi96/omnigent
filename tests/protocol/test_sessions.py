"""Focused contracts for the public sessions protocol boundary."""

from __future__ import annotations

import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, get_args

import pytest
from pydantic import TypeAdapter, ValidationError

import omnigent.protocol as protocol

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROTOCOL_ROOT = _REPO_ROOT / "omnigent" / "protocol"


def test_compatibility_reexports_keep_canonical_object_identity() -> None:
    """The ownership move must not leave a second live model definition."""
    from omnigent import entities
    from omnigent.inner import native_attachments
    from omnigent.llms.adapters import _content
    from omnigent.server import schemas

    entity_names = (
        "ITEM_TYPE_TO_DATA_CLS",
        "CompactionData",
        "ConversationItem",
        "ErrorData",
        "FunctionCallData",
        "FunctionCallOutputData",
        "ItemData",
        "MessageData",
        "NativeToolData",
        "NewConversationItem",
        "ReasoningData",
        "ResourceEventData",
        "RoutingDecisionData",
        "SlashCommandData",
        "TerminalCommandData",
        "parse_item_data",
    )
    server_names = (
        "AgentObject",
        "BackgroundTaskInfo",
        "ChildSessionList",
        "ChildSessionSummary",
        "ConversationDeleted",
        "ConversationRef",
        "CopiedFile",
        "CopyFilesRequest",
        "CopyFilesResponse",
        "CreatedSessionResponse",
        "ElicitationRequestParams",
        "ElicitationResult",
        "ErrorDetail",
        "FailedResponseObject",
        "IncompleteDetails",
        "MCPServerSummary",
        "McpServerStartup",
        "ModelUsage",
        "NativeModelOption",
        "NativeReasoningEffortOption",
        "PaginatedList",
        "PolicySummary",
        "PresenceViewer",
        "ResponseObject",
        "RetryErrorDetail",
        "SandboxLaunchStage",
        "SandboxStatus",
        "ServerStreamEvent",
        "SessionGitOptions",
        "SessionInputConsumedPayload",
        "SessionInterruptedPayload",
        "SessionList",
        "SessionListItem",
        "SessionResourceObject",
        "SessionResourcePaginatedList",
        "SessionResponse",
        "SkillSummary",
        "UpdateSessionRequest",
        "Usage",
        "UsageDetails",
    )

    for name in entity_names:
        assert getattr(entities, name) is getattr(protocol, name), name
    for name in server_names:
        assert getattr(schemas, name) is getattr(protocol, name), name

    protocol_events = get_args(get_args(protocol.ServerStreamEvent)[0])
    for event_type in protocol_events:
        assert getattr(schemas, event_type.__name__) is event_type

    assert (
        native_attachments.reject_authored_framework_notices
        is protocol.reject_authored_framework_notices
    )
    assert native_attachments.FRAMEWORK_NOTICE_BLOCK_TYPE == protocol.FRAMEWORK_NOTICE_BLOCK_TYPE
    assert _content.redact_binary_payloads is protocol.redact_binary_payloads
    assert _content.redact_inline_data_uris is protocol.redact_inline_data_uris


def test_protocol_source_imports_only_stdlib_pydantic_or_itself() -> None:
    """Portable models must not regain a server, runtime, or transport edge."""
    invalid: list[tuple[Path, int, str]] = []
    for path in sorted(_PROTOCOL_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports = [(node.lineno, alias.name) for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                imports = [(node.lineno, node.module)]
            else:
                continue
            for line, module in imports:
                root = module.partition(".")[0]
                if (
                    root not in sys.stdlib_module_names
                    and root != "pydantic"
                    and not module.startswith("omnigent.protocol")
                ):
                    invalid.append((path.relative_to(_REPO_ROOT), line, module))

    assert invalid == []


def test_protocol_import_is_clean_in_a_fresh_python_process() -> None:
    """Importing the facade alone must not initialize heavyweight layers."""
    code = """
import json
import sys
import omnigent.protocol
import omnigent.protocol.sessions

forbidden = (
    "fastapi",
    "httpx",
    "omnigent.llms",
    "omnigent.runner",
    "omnigent.runtime",
    "omnigent.server",
    "omnigent.stores",
)
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
)
print(json.dumps(loaded))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=_REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == []


def test_session_response_complete_fixture_retains_additive_fields() -> None:
    """One fixture guards all 47 snapshot fields and version-skew output."""
    fixture: dict[str, Any] = {
        "id": "conv_123",
        "agent_id": "ag_123",
        "agent_name": "code-agent",
        "status": "running",
        "background_task_count": 1,
        "background_tasks": [
            {
                "id": "bg_1",
                "type": "shell",
                "status": "running",
                "description": "Run checks",
                "command": "pytest",
            }
        ],
        "created_at": 1_700_000_000,
        "updated_at": 1_700_000_001,
        "title": "Protocol fixture",
        "labels": {"team": "sdk"},
        "runner_id": "runner_123",
        "host_id": "host_123",
        "runner_online": True,
        "host_online": True,
        "host_resumable": False,
        "reasoning_effort": "high",
        "items": [
            {
                "id": "item_1",
                "response_id": "resp_1",
                "type": "message",
                "status": "completed",
                "created_at": 1_700_000_000,
                "created_by": "user@example.com",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hello"}],
                    "future_message_field": "retained",
                },
            }
        ],
        "permission_level": 3,
        "sub_agent_name": None,
        "kind": "default",
        "parent_session_id": None,
        "root_conversation_id": "conv_123",
        "llm_model": "system.ai.gpt-5-6-sol",
        "harness": "codex",
        "model_override": None,
        "cost_control_mode_override": "on",
        "subagent_routing_override": "off",
        "share_workspace_files": True,
        "context_window": 200_000,
        "last_total_tokens": 1_234,
        "total_cost_usd": 0.42,
        "usage_by_model": {
            "system.ai.gpt-5-6-sol": {
                "input_tokens": 1_000,
                "output_tokens": 234,
                "total_tokens": 1_234,
                "cache_read_input_tokens": 100,
                "cache_creation_input_tokens": 10,
                "total_cost_usd": 0.42,
            }
        },
        "last_task_error": None,
        "external_session_id": "thread_123",
        "terminal_launch_args": ["--model", "gpt-5.6-sol"],
        "pending_elicitations": [{"elicitation_id": "elicit_1"}],
        "pending_inputs": [{"pending_id": "pending_1", "content": "hello"}],
        "workspace": "/workspace/project",
        "git_branch": "feature/sdk",
        "archived": False,
        "todos": [{"content": "Test", "status": "in_progress"}],
        "model_options": [
            {
                "id": "gpt-5.6-sol",
                "model": "system.ai.gpt-5-6-sol",
                "displayName": "GPT-5.6 Sol",
                "defaultReasoningEffort": "medium",
                "supportedReasoningEfforts": [
                    {"reasoningEffort": "high", "description": "More reasoning"}
                ],
                "isDefault": True,
            }
        ],
        "terminal_pending": True,
        "sandbox_status": {"stage": "connecting", "error": None},
        "mcp_startup": {"github": {"status": "ready", "future_mcp_field": "retained"}},
        "active_response_id": "resp_1",
        "project_id": "project_123",
        "future_session_field": {"nested": "retained"},
    }

    session = protocol.SessionResponse.model_validate(fixture)
    dumped = session.model_dump()

    assert len(protocol.SessionResponse.model_fields) == 47
    assert set(protocol.SessionResponse.model_fields) == set(fixture) - {"future_session_field"}
    assert dumped["future_session_field"] == {"nested": "retained"}
    assert dumped["items"][0]["data"]["future_message_field"] == "retained"
    assert dumped["mcp_startup"]["github"]["future_mcp_field"] == "retained"


_FLAT_ITEM_CASES: tuple[tuple[str, dict[str, Any]], ...] = (
    (
        "message",
        {
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}],
            "model": None,
            "is_meta": False,
            "interrupted": False,
            "stream_message_id": "stream_1",
        },
    ),
    (
        "function_call",
        {"model": "agent", "name": "search", "arguments": "{}", "call_id": "call_1"},
    ),
    ("function_call_output", {"call_id": "call_1", "output": "done"}),
    (
        "error",
        {"source": "execution", "code": "failed", "message": "failed", "level": "error"},
    ),
    (
        "reasoning",
        {
            "model": "agent",
            "summary": [{"type": "summary_text", "text": "reasoned"}],
            "content": [{"type": "reasoning_text", "text": "detail"}],
            "encrypted_content": "ciphertext",
        },
    ),
    (
        "compaction",
        {
            "summary": "summary",
            "last_item_id": "item_0",
            "token_count": 42,
            "model": "agent",
            "compacted_messages": [{"role": "user", "content": "hello"}],
            "window_id": "window_1",
        },
    ),
    ("native_tool", {"item": {"type": "web_search_call", "id": "tool_1"}}),
    (
        "resource_event",
        {
            "event_type": "session.resource.created",
            "resource_id": "file_1",
            "resource_type": "file",
            "resource": {"id": "file_1"},
        },
    ),
    (
        "routing_decision",
        {
            "model": "system.ai.gpt-5-6-sol",
            "applied": True,
            "rationale": "best fit",
            "agent": "worker",
            "harness": "codex",
            "scope": "turn",
            "decision_id": "decision_1",
            "raw_model": "gpt-5-6-sol",
            "attempted_override": "gpt-5-5",
            "router_source": "oss-llm",
            "task_description": "Review auth flows",
        },
    ),
    (
        "slash_command",
        {
            "model": "claude-native-ui",
            "kind": "command",
            "name": "compact",
            "arguments": "",
            "output": "ok",
        },
    ),
    ("terminal_command", {"kind": "output", "input": None, "stdout": "ok", "stderr": ""}),
)


@pytest.mark.parametrize(("item_type", "specific"), _FLAT_ITEM_CASES)
def test_flat_session_item_union_covers_current_wire_variants(
    item_type: str, specific: dict[str, Any]
) -> None:
    """The items endpoint's flat shape dispatches all 11 current variants."""
    raw = {
        "id": f"item_{item_type}",
        "response_id": "resp_1",
        "type": item_type,
        "status": "completed",
        "created_at": 1_700_000_000,
        "created_by": "user@example.com",
        "future_item_field": "retained",
        **specific,
    }

    item = TypeAdapter(protocol.SessionItem).validate_python(raw)
    dumped = item.model_dump()

    assert item.type == item_type
    assert dumped["future_item_field"] == "retained"
    assert "data" not in dumped
    for key, value in specific.items():
        assert dumped[key] == value


@pytest.mark.parametrize(
    ("raw", "expected_type"),
    [
        (
            {
                "type": "message",
                "data": {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "hello"}],
                },
                "model_override": "system.ai.gpt-5-6-sol",
                "tools": [],
            },
            protocol.SessionMessage,
        ),
        (
            {
                "type": "function_call_output",
                "data": {"call_id": "call_1", "output": "done"},
            },
            protocol.FunctionCallOutput,
        ),
        ({"type": "interrupt", "data": {}}, protocol.Interrupt),
    ],
)
def test_public_session_event_inputs_accept_only_supported_shapes(
    raw: dict[str, Any], expected_type: type
) -> None:
    parsed = TypeAdapter(protocol.PublicSessionEventInput).validate_python(raw)

    assert type(parsed) is expected_type
    assert parsed.model_dump(exclude_none=True) == raw


@pytest.mark.parametrize(
    "raw",
    [
        {"type": "compact", "data": {}},
        {"type": "external_session_status", "data": {"status": "idle"}},
        {"type": "interrupt", "data": {"reason": "invented"}},
        {
            "type": "message",
            "data": {"role": "assistant", "content": []},
        },
        {
            "type": "message",
            "data": {"role": "user", "content": [], "created_by": "user@example.com"},
        },
        {
            "type": "function_call_output",
            "data": {"output": "missing call id"},
        },
        {"type": "interrupt", "data": {}, "created_by": "user@example.com"},
        {"type": "interrupt", "data": {}, "future_request_field": True},
    ],
)
def test_public_session_event_inputs_reject_internal_or_additive_fields(
    raw: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        TypeAdapter(protocol.PublicSessionEventInput).validate_python(raw)


def test_acknowledgement_deletion_and_elicitation_output_shapes() -> None:
    """Previously untyped route outputs retain their current bytes and extras."""
    event_ack = protocol.EventAcknowledgement.model_validate(
        {
            "queued": False,
            "item_id": "item_1",
            "pending_id": "pending_1",
            "denied": True,
            "reason": "Denied by policy",
            "elicitation_id": "elicit_1",
            "child_session_id": "child_1",
            "recovered": True,
            "recovery": "runner_relaunched",
            "future_ack_field": "retained",
        }
    )
    resolution_ack = protocol.ElicitationResolutionAcknowledgement.model_validate(
        {"queued": False, "future_ack_field": "retained"}
    )
    deletion = protocol.SessionResourceDeleted.model_validate(
        {"id": "file_1", "object": "session.resource.deleted", "deleted": True}
    )
    elicitation_adapter = TypeAdapter(protocol.ElicitationState)
    pending = elicitation_adapter.validate_python(
        {
            "status": "pending",
            "message": "Approve?",
            "phase": "tool_call",
            "policy_name": "approval",
            "content_preview": "run command",
            "future_state_field": "retained",
        }
    )
    resolved = elicitation_adapter.validate_python({"status": "resolved"})

    assert event_ack.model_dump()["future_ack_field"] == "retained"
    assert event_ack.reason == "Denied by policy"
    assert event_ack.elicitation_id == "elicit_1"
    assert event_ack.child_session_id == "child_1"
    assert event_ack.recovery == "runner_relaunched"
    assert resolution_ack.model_dump()["future_ack_field"] == "retained"
    assert deletion.model_dump() == {
        "id": "file_1",
        "object": "session.resource.deleted",
        "deleted": True,
    }
    assert pending.status == "pending"
    assert pending.model_dump()["future_state_field"] == "retained"
    assert resolved.status == "resolved"


def test_unknown_event_is_output_only_and_retains_the_raw_event() -> None:
    """The catch-all preserves version-skew bytes without weakening known input."""
    raw = {
        "type": "response.future_event",
        "sequence_number": 42,
        "payload": {"new": "value"},
    }

    event = protocol.UnknownEvent(type=raw["type"], raw=raw)

    assert event.type == raw["type"]
    assert event.raw == raw
    with pytest.raises(ValidationError):
        TypeAdapter(protocol.ServerStreamEvent).validate_python(raw)
    with pytest.raises(ValidationError):
        TypeAdapter(protocol.PublicSessionEventInput).validate_python(raw)


def test_output_models_offer_thin_dict_and_json_aliases() -> None:
    """SDK-style aliases retain Pydantic options and additive fields."""
    model = protocol.EventAcknowledgement.model_validate(
        {"queued": True, "item_id": None, "future_field": "retained"}
    )

    assert model.to_dict(exclude_none=True) == {
        "queued": True,
        "future_field": "retained",
    }
    assert json.loads(model.to_json(exclude_none=True)) == model.to_dict(exclude_none=True)
