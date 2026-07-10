"""Track B ② — per-domain control-plane repositories (the PUBLIC data API).

Each module here owns one domain's ``TableDescriptor``s + a ``Repository`` over the PG adapter
primitives, decomposing the inherited 28k-line ``ControlPlaneStore`` God-class. Repositories are the
public API (owner-ratified 2026-06-21): callers reach them via ``store.repos.<domain>`` and the
corresponding ``ControlPlaneStore`` facade methods are deleted domain-by-domain (②, no dual-track).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .criteria_confidence import CriteriaConfidenceRepository
from .linkedin_profile_registry import LinkedinProfileRegistryRepository
from .manual_review import ManualReviewRepository
from .serving_projection import ServingProjectionRepository


class ControlPlaneRepositories:
    """The per-domain repository namespace hung off ``ControlPlaneStore.repos``.

    One instance per store, sharing the store's live PG adapter (and therefore its pool and
    postgres-only authority mode). Domains join this namespace as their ② migration batch lands.

    ``job_lookup`` is the store's ``get_job`` bound method: the one ratified cross-domain seam (②.1) —
    ``criteria_confidence.record_feedback`` falls back to the job's stored request payload when the
    caller's payload carries none. Repositories must not import ``storage`` (circular), so the store
    injects the callable; a future jobs repository replaces the binding without touching callers.
    """

    def __init__(self, adapter: Any, *, job_lookup: Callable[[str], Any] | None = None) -> None:
        self.linkedin_profile_registry = LinkedinProfileRegistryRepository(adapter)
        self.criteria_confidence = CriteriaConfidenceRepository(adapter, job_lookup=job_lookup)
        self.manual_review = ManualReviewRepository(adapter)
        self.serving_projection = ServingProjectionRepository(adapter)


def linkedin_profile_registry_repo(store: Any) -> Any:
    """Duck-typed accessor for the linkedin_profile_registry repository on a store-like object.

    Returns None when the store (typically a test fake) does not expose the ``repos`` namespace, so
    feature-detection call sites keep their fallback branches exactly as they did when they probed the
    retired ``ControlPlaneStore`` facade methods with ``getattr``.
    """

    return getattr(getattr(store, "repos", None), "linkedin_profile_registry", None)
