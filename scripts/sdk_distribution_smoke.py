"""Execute the documented SDK quickstarts against a local fake server."""

from __future__ import annotations

import json
import os
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import omnigent_ui_sdk
from omnigent_client import (
    AsyncOmnigent,
    Omnigent,
    OmnigentClient,
    SessionMessage,
    SessionResponse,
)

_EXECUTABLE_FENCE = re.compile(
    r"<!-- sdk-example: executable -->\s*```python\n(.*?)\n```", re.DOTALL
)
_REQUESTS: list[tuple[str, str]] = []
_REQUESTS_LOCK = threading.Lock()
_NEXT_SESSION = 0


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def _session(session_id: str) -> dict[str, object]:
    return {
        "id": session_id,
        "agent_id": "ag_smoke",
        "status": "idle",
        "created_at": 1,
        "updated_at": 1,
        "items": [],
    }


def _stream_body(session_id: str) -> bytes:
    completed = {
        "type": "response.completed",
        "response": {
            "id": f"resp_{session_id}",
            "status": "completed",
            "model": "smoke-agent",
            "created_at": 1,
        },
    }
    events = (
        ("session.heartbeat", {"type": "session.heartbeat"}),
        ("response.completed", completed),
    )
    body = "".join(
        f"event: {event_type}\ndata: {json.dumps(payload)}\n\n" for event_type, payload in events
    )
    return f"{body}data: [DONE]\n\n".encode()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _record(self) -> None:
        with _REQUESTS_LOCK:
            _REQUESTS.append((self.command, self.path))

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def do_POST(self) -> None:
        global _NEXT_SESSION

        self._record()
        self._read_body()
        if self.path == "/v1/sessions":
            with _REQUESTS_LOCK:
                _NEXT_SESSION += 1
                session_id = f"conv_smoke_{_NEXT_SESSION}"
            self._send(201, _json_bytes(_session(session_id)), "application/json")
            return
        if self.path.endswith("/events"):
            self._send(202, _json_bytes({"queued": True}), "application/json")
            return
        self._send(404, _json_bytes({"error": {"message": "not found"}}), "application/json")

    def do_GET(self) -> None:
        self._record()
        if self.path.endswith("/stream"):
            session_id = self.path.removesuffix("/stream").rsplit("/", 1)[-1]
            self._send(200, _stream_body(session_id), "text/event-stream")
            return
        self._send(404, _json_bytes({"error": {"message": "not found"}}), "application/json")

    def do_DELETE(self) -> None:
        self._record()
        session_id = self.path.rsplit("/", 1)[-1]
        body = {"id": session_id, "object": "conversation.deleted", "deleted": True}
        self._send(200, _json_bytes(body), "application/json")

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def _fake_server() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _read_examples() -> list[str]:
    readme = Path(__file__).resolve().parents[1] / "sdks" / "python-client" / "README.md"
    examples = _EXECUTABLE_FENCE.findall(readme.read_text())
    if len(examples) != 2:
        raise AssertionError(f"expected two executable README examples, found {len(examples)}")
    for index, example in enumerate(examples, start=1):
        compile(example, f"{readme} executable block {index}", "exec")
    return examples


def _check_public_imports() -> None:
    _ = omnigent_ui_sdk, SessionResponse, SessionMessage

    if AsyncOmnigent is not OmnigentClient:
        raise AssertionError("AsyncOmnigent compatibility alias changed")
    if Omnigent is OmnigentClient:
        raise AssertionError("Omnigent must remain the native synchronous client")


def main() -> None:
    global _NEXT_SESSION

    _check_public_imports()
    examples = _read_examples()
    with _REQUESTS_LOCK:
        _REQUESTS.clear()
        _NEXT_SESSION = 0

    previous_url = os.environ.get("OMNIGENT_BASE_URL")
    previous_agent = os.environ.get("OMNIGENT_AGENT_ID")
    try:
        with _fake_server() as base_url:
            os.environ["OMNIGENT_BASE_URL"] = base_url
            os.environ["OMNIGENT_AGENT_ID"] = "ag_smoke"
            for index, example in enumerate(examples, start=1):
                namespace = {"__name__": f"__omnigent_readme_example_{index}__"}
                exec(compile(example, f"README executable block {index}", "exec"), namespace)
    finally:
        if previous_url is None:
            os.environ.pop("OMNIGENT_BASE_URL", None)
        else:
            os.environ["OMNIGENT_BASE_URL"] = previous_url
        if previous_agent is None:
            os.environ.pop("OMNIGENT_AGENT_ID", None)
        else:
            os.environ["OMNIGENT_AGENT_ID"] = previous_agent

    with _REQUESTS_LOCK:
        requests = list(_REQUESTS)
    if sum(method == "POST" and path == "/v1/sessions" for method, path in requests) != 2:
        raise AssertionError(f"both quickstarts must create a session: {requests}")
    if sum(method == "DELETE" for method, _ in requests) != 2:
        raise AssertionError(f"both quickstarts must explicitly delete their session: {requests}")

    print("SDK distribution smoke passed")


if __name__ == "__main__":
    main()
