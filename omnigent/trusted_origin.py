"""Shared first-party client origin used by HTTP and WebSocket transports."""

# A browser derives Origin from its page URL and cannot emit this non-HTTP
# value, so server origin checks can distinguish OmniGent's own clients.
OMNIGENT_INTERNAL_WS_ORIGIN = "omnigent://internal"

__all__ = ["OMNIGENT_INTERNAL_WS_ORIGIN"]
