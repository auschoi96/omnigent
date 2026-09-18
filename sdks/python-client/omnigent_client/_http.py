"""HTTP transport helpers shared by the client and its callers."""

from __future__ import annotations

import ipaddress
import threading
import weakref
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from ._errors import OmnigentError

_RETRYABLE_METHODS = frozenset({"GET", "HEAD"})
_CONNECT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)


@dataclass
class _InjectedClientPolicyState:
    origin: str
    origin_added: bool
    previous_follow_redirects: bool
    response_hook: Any
    response_hook_added: bool
    users: int = 1


@dataclass
class _InjectedClientPolicy:
    state: _InjectedClientPolicyState
    released: bool = False


_injected_client_policies: weakref.WeakKeyDictionary[
    httpx.Client | httpx.AsyncClient, _InjectedClientPolicyState
] = weakref.WeakKeyDictionary()
_injected_client_policies_lock = threading.Lock()


class _InjectedClientLease:
    """Per-SDK-wrapper access to a caller-owned HTTPX client."""

    def __init__(self, client: httpx.Client | httpx.AsyncClient) -> None:
        self._client = client
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def __getattr__(self, name: str) -> Any:
        if self._closed:
            raise RuntimeError("Omnigent client is closed")
        return getattr(self._client, name)


def apply_injected_client_policy(
    client: httpx.Client | httpx.AsyncClient,
    *,
    origin: str,
    response_hook: Any,
) -> _InjectedClientPolicy:
    """Apply required request safety while recording caller-owned state."""
    with _injected_client_policies_lock:
        state = _injected_client_policies.get(client)
        if state is not None:
            if state.origin != origin or state.response_hook is not response_hook:
                raise RuntimeError("injected HTTP client already has a different SDK policy")
            state.users += 1
            return _InjectedClientPolicy(state=state)

        origin_added = "Origin" not in client.headers
        if origin_added:
            client.headers["Origin"] = origin
        previous_follow_redirects = client.follow_redirects
        client.follow_redirects = True
        response_hooks = client.event_hooks.setdefault("response", [])
        response_hook_added = response_hook not in response_hooks
        if response_hook_added:
            response_hooks.append(response_hook)
        state = _InjectedClientPolicyState(
            origin=origin,
            origin_added=origin_added,
            previous_follow_redirects=previous_follow_redirects,
            response_hook=response_hook,
            response_hook_added=response_hook_added,
        )
        _injected_client_policies[client] = state
        return _InjectedClientPolicy(state=state)


def restore_injected_client_policy(
    client: httpx.Client | httpx.AsyncClient,
    policy: _InjectedClientPolicy,
    *,
    origin: str,
) -> None:
    """Restore only the caller-owned settings changed by the SDK."""
    with _injected_client_policies_lock:
        if policy.released:
            return
        policy.released = True
        state = policy.state
        current = _injected_client_policies.get(client)
        if current is not state:
            return
        state.users -= 1
        if state.users:
            return
        del _injected_client_policies[client]
        if state.origin_added and client.headers.get("Origin") == origin:
            del client.headers["Origin"]
        client.follow_redirects = state.previous_follow_redirects
        if state.response_hook_added:
            response_hooks = client.event_hooks.setdefault("response", [])
            if state.response_hook in response_hooks:
                response_hooks.remove(state.response_hook)


class _SyncHTTPClient(httpx.Client):
    """HTTPX client with bounded connection retries for safe reads only."""

    def __init__(self, *, max_connect_retries: int, **kwargs: Any) -> None:
        self._max_connect_retries = max_connect_retries
        super().__init__(**kwargs)

    def send(self, request: httpx.Request, **kwargs: Any) -> httpx.Response:
        retries = self._max_connect_retries if request.method in _RETRYABLE_METHODS else 0
        for attempt in range(retries + 1):
            try:
                return super().send(request, **kwargs)
            except _CONNECT_ERRORS:
                if attempt == retries:
                    raise
        raise AssertionError("unreachable")


class _AsyncHTTPClient(httpx.AsyncClient):
    """Async counterpart to :class:`_SyncHTTPClient`."""

    def __init__(self, *, max_connect_retries: int, **kwargs: Any) -> None:
        self._max_connect_retries = max_connect_retries
        super().__init__(**kwargs)

    async def send(self, request: httpx.Request, **kwargs: Any) -> httpx.Response:
        retries = self._max_connect_retries if request.method in _RETRYABLE_METHODS else 0
        for attempt in range(retries + 1):
            try:
                return await super().send(request, **kwargs)
            except _CONNECT_ERRORS:
                if attempt == retries:
                    raise
        raise AssertionError("unreachable")


def _port_or_default(url: httpx.URL) -> int | None:
    if url.port is not None:
        return url.port
    return {"http": 80, "https": 443}.get(url.scheme)


def redirect_stays_on_origin(request_url: httpx.URL, location: str) -> bool:
    """Return whether a redirect may retain credentials and request bodies."""
    try:
        target = request_url.join(location)
    except httpx.InvalidURL:
        return False
    if target.host != request_url.host:
        return False
    if target.scheme == request_url.scheme and _port_or_default(target) == _port_or_default(
        request_url
    ):
        return True
    return (
        request_url.scheme == "http"
        and target.scheme == "https"
        and _port_or_default(request_url) == 80
        and _port_or_default(target) == 443
    )


def refuse_cross_origin_redirect(response: httpx.Response) -> None:
    """Refuse redirects that could forward credentials to another origin."""
    if not response.has_redirect_location:
        return
    location = response.headers["location"]
    if redirect_stays_on_origin(response.request.url, location):
        return
    raise OmnigentError(
        f"refusing to follow a cross-origin redirect (status {response.status_code}) "
        f"to {location}",
        response.status_code,
    )


async def refuse_cross_origin_redirect_async(response: httpx.Response) -> None:
    """Async-client response hook for the shared redirect policy."""
    refuse_cross_origin_redirect(response)


def is_loopback_url(url: str) -> bool:
    """
    Report whether *url* addresses this machine's loopback interface.

    Loopback traffic must never go through an HTTP proxy: the proxy
    resolves ``127.0.0.1`` against itself, so the request reaches the
    wrong host or is refused outright. httpx reads proxy settings from
    the environment by default, and on Windows that also covers the
    system registry — so a machine with any proxy configured cannot
    reach its own local server unless the caller passes
    ``trust_env=False``.

    :param url: Absolute URL to classify, e.g.
        ``"http://127.0.0.1:6767"``.
    :returns: ``True`` for loopback targets — ``localhost``, any
        ``.localhost`` name, ``127.0.0.0/8``, or ``::1``. ``False``
        otherwise, including for a URL with no host.
    """
    host = urlsplit(url).hostname
    if host is None:
        return False
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False
