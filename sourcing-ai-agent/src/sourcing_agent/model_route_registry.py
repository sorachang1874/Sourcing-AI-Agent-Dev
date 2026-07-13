"""Product-owned model route declarations for the Track D D0a substrate.

This module is intentionally configuration-only.  It does not resolve credentials,
construct clients, or perform transport.  D0a permits deterministic ``simulate`` and
``scripted`` exercise only; every live attempt is rejected here before any future
transport adapter can be reached.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Mapping

MODEL_ROUTE_REGISTRY_SCHEMA_VERSION = "model_route_registry_v1"
MODEL_ROUTE_ROLLOUT_DRAFT = "draft"
MODEL_ROUTE_FALLBACK_FAIL_CLOSED = "fail_closed"

MODEL_ROUTE_SPEC_RECORD_KEYS = frozenset(
    {
        "route_id",
        "use_case",
        "provider",
        "model",
        "api_style",
        "capabilities",
        "budget_class",
        "simulate_mapping",
        "fallback_policy",
        "circuit_key",
        "rollout_state",
    }
)
MODEL_ROUTE_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "live_enabled",
        "allowed_provider_modes",
        "routes",
    }
)
MODEL_ROUTE_MANIFEST_ROUTE_KEYS = MODEL_ROUTE_SPEC_RECORD_KEYS | {"route_revision"}

D0A_ALLOWED_PROVIDER_MODES = frozenset({"simulate", "scripted"})
_KNOWN_ROLLOUT_STATES = frozenset({"draft", "canary", "active", "retired"})
_KNOWN_CAPABILITIES = frozenset({"stream", "tools", "usage", "identity_check"})


class ModelRouteRegistryError(RuntimeError):
    """Raised when a checked-in route declaration is internally inconsistent."""


class ModelRouteExecutionRejected(RuntimeError):
    """Raised when the D0a non-live route predicate rejects an execution."""


@dataclass(frozen=True, slots=True)
class ModelRouteSpec:
    """Immutable, non-secret product model route metadata.

    Route revisions are content digests, so callers cannot claim a revision that
    does not match the complete checked-in declaration.
    """

    route_id: str
    use_case: str
    provider: str
    model: str
    api_style: str
    capabilities: frozenset[str]
    budget_class: str
    simulate_mapping: str
    fallback_policy: str = MODEL_ROUTE_FALLBACK_FAIL_CLOSED
    circuit_key: str = ""
    rollout_state: str = MODEL_ROUTE_ROLLOUT_DRAFT

    def __post_init__(self) -> None:
        required = {
            "route_id": self.route_id,
            "use_case": self.use_case,
            "provider": self.provider,
            "model": self.model,
            "api_style": self.api_style,
            "budget_class": self.budget_class,
            "simulate_mapping": self.simulate_mapping,
            "circuit_key": self.circuit_key,
        }
        for field_name, value in required.items():
            if not isinstance(value, str) or not value or value != value.strip():
                raise ModelRouteRegistryError(f"model_route_invalid_{field_name}:{self.route_id or '<missing>'}")
        if not isinstance(self.capabilities, frozenset):
            try:
                object.__setattr__(self, "capabilities", frozenset(self.capabilities))
            except TypeError as exc:
                raise ModelRouteRegistryError(f"model_route_capabilities_invalid:{self.route_id}") from exc
        if self.fallback_policy != MODEL_ROUTE_FALLBACK_FAIL_CLOSED:
            raise ModelRouteRegistryError(f"model_route_fallback_not_fail_closed:{self.route_id}")
        if self.rollout_state not in _KNOWN_ROLLOUT_STATES:
            raise ModelRouteRegistryError(f"model_route_unknown_rollout_state:{self.route_id}:{self.rollout_state}")
        unknown_capabilities = sorted(self.capabilities - _KNOWN_CAPABILITIES)
        if unknown_capabilities:
            raise ModelRouteRegistryError(
                f"model_route_unknown_capabilities:{self.route_id}:{','.join(unknown_capabilities)}"
            )
        if not self.capabilities:
            raise ModelRouteRegistryError(f"model_route_capabilities_required:{self.route_id}")

    def to_record(self) -> dict[str, object]:
        return {
            "route_id": self.route_id,
            "use_case": self.use_case,
            "provider": self.provider,
            "model": self.model,
            "api_style": self.api_style,
            "capabilities": sorted(self.capabilities),
            "budget_class": self.budget_class,
            "simulate_mapping": self.simulate_mapping,
            "fallback_policy": self.fallback_policy,
            "circuit_key": self.circuit_key,
            "rollout_state": self.rollout_state,
        }

    @property
    def revision(self) -> str:
        encoded = json.dumps(
            self.to_record(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


DEFAULT_MODEL_ROUTE_SPECS = (
    ModelRouteSpec(
        route_id="agent.planner.loop",
        use_case="agent planner tool turn",
        provider="openai_compatible_relay",
        model="gpt-5.6-sol",
        api_style="openai_chat_completions",
        capabilities=frozenset({"stream", "tools", "usage", "identity_check"}),
        budget_class="agent_turn_standard",
        simulate_mapping="scripted_tool_turn",
        circuit_key="agent_planner_loop",
    ),
    ModelRouteSpec(
        route_id="company.identity.adjudicate",
        use_case="company identity evidence adjudication",
        provider="openai_compatible_relay",
        model="gpt-5.6-sol",
        api_style="openai_chat_completions",
        capabilities=frozenset({"usage", "identity_check"}),
        budget_class="adjudication_small",
        simulate_mapping="scripted_adjudication",
        circuit_key="company_identity_adjudicate",
    ),
)


def validate_model_route_specs(
    specs: Iterable[ModelRouteSpec],
    *,
    require_draft_only: bool,
) -> Mapping[str, ModelRouteSpec]:
    """Validate uniqueness and the current batch's draft-only rollout fence."""

    routes: dict[str, ModelRouteSpec] = {}
    circuit_owners: dict[str, str] = {}
    for spec in specs:
        if spec.route_id in routes:
            raise ModelRouteRegistryError(f"model_route_duplicate:{spec.route_id}")
        existing_circuit_owner = circuit_owners.get(spec.circuit_key)
        if existing_circuit_owner is not None:
            raise ModelRouteRegistryError(
                f"model_route_circuit_key_duplicate:{spec.circuit_key}:{existing_circuit_owner}:{spec.route_id}"
            )
        if require_draft_only and spec.rollout_state != MODEL_ROUTE_ROLLOUT_DRAFT:
            raise ModelRouteRegistryError(f"model_route_d0a_requires_draft:{spec.route_id}:{spec.rollout_state}")
        routes[spec.route_id] = spec
        circuit_owners[spec.circuit_key] = spec.route_id
    if not routes:
        raise ModelRouteRegistryError("model_route_registry_empty")
    return MappingProxyType(routes)


MODEL_ROUTE_SPECS_BY_ID = validate_model_route_specs(
    DEFAULT_MODEL_ROUTE_SPECS,
    require_draft_only=True,
)


def get_model_route_spec(route_id: str) -> ModelRouteSpec:
    normalized_route_id = str(route_id or "").strip()
    route = MODEL_ROUTE_SPECS_BY_ID.get(normalized_route_id)
    if route is None:
        raise ModelRouteExecutionRejected(f"model_route_unknown:{normalized_route_id or '<missing>'}")
    return route


def assert_d0a_route_execution_allowed(
    *,
    route_id: str,
    provider_mode: str,
    required_capabilities: Iterable[str] = (),
) -> ModelRouteSpec:
    """Return a route only for D0a's explicit simulate/scripted capability lane.

    This is deliberately not an environment-derived gate.  A future live adapter
    must have a typed durable owner, route activation, budget/cost ledger, and the
    repository's low-level provider gate; D0a has none of those and always rejects
    ``live``.
    """

    raw_mode = str(provider_mode or "").strip()
    normalized_mode = raw_mode.lower()
    if normalized_mode == "live":
        raise ModelRouteExecutionRejected("model_tool_live_unavailable_d0a")
    if raw_mode != normalized_mode:
        raise ModelRouteExecutionRejected(f"model_tool_provider_mode_noncanonical_d0a:{raw_mode or '<missing>'}")
    if normalized_mode not in D0A_ALLOWED_PROVIDER_MODES:
        raise ModelRouteExecutionRejected(f"model_tool_provider_mode_unsupported_d0a:{normalized_mode or '<missing>'}")

    route = get_model_route_spec(route_id)
    if route.rollout_state != MODEL_ROUTE_ROLLOUT_DRAFT:
        raise ModelRouteExecutionRejected(f"model_route_not_d0a_draft:{route.route_id}:{route.rollout_state}")
    missing = sorted(set(required_capabilities) - route.capabilities)
    if missing:
        raise ModelRouteExecutionRejected(f"model_route_capability_missing:{route.route_id}:{','.join(missing)}")
    return route


def model_route_registry_manifest() -> dict[str, object]:
    return {
        "schema_version": MODEL_ROUTE_REGISTRY_SCHEMA_VERSION,
        "live_enabled": False,
        "allowed_provider_modes": sorted(D0A_ALLOWED_PROVIDER_MODES),
        "routes": [
            {**spec.to_record(), "route_revision": spec.revision}
            for spec in sorted(MODEL_ROUTE_SPECS_BY_ID.values(), key=lambda item: item.route_id)
        ],
    }


def validate_model_route_registry_manifest(
    manifest: Mapping[str, object],
    *,
    specs: Iterable[ModelRouteSpec] = DEFAULT_MODEL_ROUTE_SPECS,
) -> None:
    """Fail closed on registry/manifest drift in the current draft-only lane."""

    if not isinstance(manifest, Mapping) or set(manifest) != MODEL_ROUTE_MANIFEST_KEYS:
        raise ModelRouteRegistryError("model_route_manifest_keyset_invalid")
    if manifest.get("schema_version") != MODEL_ROUTE_REGISTRY_SCHEMA_VERSION:
        raise ModelRouteRegistryError("model_route_manifest_schema_version_invalid")
    if manifest.get("live_enabled") is not False:
        raise ModelRouteRegistryError("model_route_manifest_live_must_remain_disabled")
    if manifest.get("allowed_provider_modes") != sorted(D0A_ALLOWED_PROVIDER_MODES):
        raise ModelRouteRegistryError("model_route_manifest_provider_modes_invalid")

    validated = validate_model_route_specs(tuple(specs), require_draft_only=True)
    manifest_routes = manifest.get("routes")
    if not isinstance(manifest_routes, list) or len(manifest_routes) != len(validated):
        raise ModelRouteRegistryError("model_route_manifest_routes_invalid")
    expected_routes = {
        spec.route_id: {**spec.to_record(), "route_revision": spec.revision} for spec in validated.values()
    }
    observed_route_ids: set[str] = set()
    observed_circuit_keys: set[str] = set()
    for route_record in manifest_routes:
        if not isinstance(route_record, dict) or set(route_record) != MODEL_ROUTE_MANIFEST_ROUTE_KEYS:
            raise ModelRouteRegistryError("model_route_manifest_route_keyset_invalid")
        route_id = route_record.get("route_id")
        circuit_key = route_record.get("circuit_key")
        if type(route_id) is not str or route_id in observed_route_ids:
            raise ModelRouteRegistryError("model_route_manifest_route_id_invalid")
        if type(circuit_key) is not str or circuit_key in observed_circuit_keys:
            raise ModelRouteRegistryError("model_route_manifest_circuit_key_invalid")
        expected = expected_routes.get(route_id)
        if expected is None or route_record != expected:
            raise ModelRouteRegistryError(f"model_route_manifest_route_mismatch:{route_id}")
        if route_record.get("rollout_state") != MODEL_ROUTE_ROLLOUT_DRAFT:
            raise ModelRouteRegistryError(f"model_route_manifest_route_not_draft:{route_id}")
        observed_route_ids.add(route_id)
        observed_circuit_keys.add(circuit_key)
    if observed_route_ids != set(validated):
        raise ModelRouteRegistryError("model_route_manifest_route_population_mismatch")


validate_model_route_registry_manifest(model_route_registry_manifest())
