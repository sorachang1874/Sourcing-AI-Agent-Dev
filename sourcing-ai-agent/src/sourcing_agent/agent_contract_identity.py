"""Canonical immutable identities shared by Agent contract layers."""

from __future__ import annotations

import re

AGENT_TOOL_NAME_MAX_BYTES = 128
AGENT_TOOL_NAME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,127}")


def is_valid_agent_tool_name(value: object) -> bool:
    """Return whether ``value`` is the exact cross-layer Agent tool identity."""

    if type(value) is not str or not value or value != value.strip():
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        return False
    return len(encoded) <= AGENT_TOOL_NAME_MAX_BYTES and AGENT_TOOL_NAME_PATTERN.fullmatch(value) is not None


__all__ = ["AGENT_TOOL_NAME_MAX_BYTES", "AGENT_TOOL_NAME_PATTERN", "is_valid_agent_tool_name"]
