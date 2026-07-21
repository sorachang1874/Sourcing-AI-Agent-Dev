from __future__ import annotations

import re
from dataclasses import dataclass

HEALTH_SCHEMA_VERSION = "x.grok_cli.transport_health.v1"

STATUS_READY = "ready_for_native_x_canary"
STATUS_DEGRADED = "degraded_requires_single_call_canary"
STATUS_BLOCKED_UNKNOWN_MODEL = "blocked_unknown_model"
STATUS_BLOCKED_NO_MODEL = "blocked_no_available_model"
STATUS_BLOCKED_BACKEND = "blocked_backend_unstable"

_DEFAULT_MODEL_RE = re.compile(r"^Default model:\s*(?P<model>[A-Za-z0-9._-]+)\s*$", re.MULTILINE)
_AVAILABLE_MODEL_RE = re.compile(
    r"^\s*\*?\s*(?P<model>[A-Za-z0-9][A-Za-z0-9._-]{0,127})(?:\s+\(default\))?\s*$",
    re.MULTILINE,
)
_UNKNOWN_MODEL_RE = re.compile(r"unknown model id", re.IGNORECASE)
_NETWORK_FAILURE_RE = re.compile(
    r"(Failed to fetch models|Settings fetch failed|Settings fetch network error|bundle sync failed|TimedOut)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class GrokModelListing:
    default_model: str | None
    available_models: tuple[str, ...]
    model_fetch_network_failed: bool
    settings_fetch_network_failed: bool
    bundle_sync_network_failed: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "default_model": self.default_model,
            "available_models": list(self.available_models),
            "model_fetch_network_failed": self.model_fetch_network_failed,
            "settings_fetch_network_failed": self.settings_fetch_network_failed,
            "bundle_sync_network_failed": self.bundle_sync_network_failed,
        }


@dataclass(frozen=True)
class GrokTransportHealth:
    schema_version: str
    status: str
    requested_model: str
    selected_model: str | None
    available_models: tuple[str, ...]
    stale_requested_model: bool
    backend_unstable: bool
    native_x_canary_required: bool
    large_wave_allowed: bool
    diagnostics: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "requested_model": self.requested_model,
            "selected_model": self.selected_model,
            "available_models": list(self.available_models),
            "stale_requested_model": self.stale_requested_model,
            "backend_unstable": self.backend_unstable,
            "native_x_canary_required": self.native_x_canary_required,
            "large_wave_allowed": self.large_wave_allowed,
            "diagnostics": list(self.diagnostics),
        }


def parse_grok_models_output(output: str) -> GrokModelListing:
    default_match = _DEFAULT_MODEL_RE.search(output)
    default_model = default_match.group("model") if default_match else None
    available = tuple(dict.fromkeys(match.group("model") for match in _AVAILABLE_MODEL_RE.finditer(output)))
    return GrokModelListing(
        default_model=default_model,
        available_models=available,
        model_fetch_network_failed="Failed to fetch models" in output,
        settings_fetch_network_failed=(
            "Settings fetch failed" in output or "Settings fetch network error" in output
        ),
        bundle_sync_network_failed="bundle sync failed" in output,
    )


def evaluate_grok_transport_health(
    *,
    requested_model: str,
    models_output: str,
    canary_exit_code: int | None = None,
    canary_output: str = "",
) -> GrokTransportHealth:
    listing = parse_grok_models_output(models_output)
    combined = f"{models_output}\n{canary_output}"
    backend_unstable = bool(_NETWORK_FAILURE_RE.search(combined))
    unknown_model = bool(_UNKNOWN_MODEL_RE.search(combined))
    available_set = set(listing.available_models)
    selected_model = requested_model if requested_model in available_set else listing.default_model
    stale_requested = bool(available_set) and requested_model not in available_set
    diagnostics: list[str] = []

    if unknown_model:
        diagnostics.append("requested_model_rejected_by_cli")
    if stale_requested:
        diagnostics.append("requested_model_absent_from_available_models")
    if backend_unstable:
        diagnostics.append("grok_backend_models_or_settings_unstable")
    if canary_exit_code is not None and canary_exit_code != 0:
        diagnostics.append("native_x_canary_process_failed")

    if unknown_model:
        status = STATUS_BLOCKED_UNKNOWN_MODEL
    elif not selected_model:
        status = STATUS_BLOCKED_NO_MODEL
        diagnostics.append("no_selectable_model_observed")
    elif backend_unstable and canary_exit_code is None:
        status = STATUS_BLOCKED_BACKEND
    elif backend_unstable or stale_requested or canary_exit_code != 0:
        status = STATUS_DEGRADED
    else:
        status = STATUS_READY

    return GrokTransportHealth(
        schema_version=HEALTH_SCHEMA_VERSION,
        status=status,
        requested_model=requested_model,
        selected_model=selected_model,
        available_models=listing.available_models,
        stale_requested_model=stale_requested,
        backend_unstable=backend_unstable,
        native_x_canary_required=status != STATUS_READY,
        large_wave_allowed=status == STATUS_READY and canary_exit_code == 0,
        diagnostics=tuple(dict.fromkeys(diagnostics)),
    )


def sealed_predecessor_model_is_adoptable(
    *,
    predecessor_model_id: str,
    local_model_id: str,
    predecessor_replay_sealed: bool,
) -> bool:
    """A sealed replay predecessor must not force stale model ids onto new live slices."""

    return bool(predecessor_model_id and local_model_id and predecessor_replay_sealed)
