from __future__ import annotations

import os
from typing import Any, Callable

REMOTE_PROVIDER_EVENT_RECOVERY_TOTAL_LIMIT_ENV = "WORKFLOW_REMOTE_EVENT_RECOVERY_TOTAL_LIMIT"
DEFAULT_REMOTE_PROVIDER_EVENT_RECOVERY_TOTAL_LIMIT = 4


def coerce_positive_int(value: Any, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return int(default)
    return parsed if parsed > 0 else int(default)


def remote_provider_event_recovery_total_limit(
    *values: Any,
    default: int = DEFAULT_REMOTE_PROVIDER_EVENT_RECOVERY_TOTAL_LIMIT,
    getenv: Callable[[str], str | None] = os.getenv,
) -> int:
    """Resolve the bounded worker count for one remote-event recovery tick.

    Remote provider events may arrive in bursts. The callback only records and
    wakes recovery; this limit controls how many already-terminal workers the
    daemon may ingest in one tick before yielding back to the state machine.
    """

    for value in values:
        parsed = coerce_positive_int(value)
        if parsed > 0:
            return parsed
    env_value = getenv(REMOTE_PROVIDER_EVENT_RECOVERY_TOTAL_LIMIT_ENV)
    parsed_env = coerce_positive_int(env_value)
    if parsed_env > 0:
        return parsed_env
    return max(1, int(default))
