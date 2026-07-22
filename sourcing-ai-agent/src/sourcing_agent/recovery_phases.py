"""Recovery-tick phase objects + per-tick context (Phase 4 Step 2, A2).

``SourcingOrchestrator.run_worker_recovery_once`` historically inlined ~33
bespoke recovery phases plus the 14-binding uniform drain block as one ~2000
line method whose cross-phase state was threaded through method-local
``nonlocal`` variables and a long if/elif/else skip ladder per phase. This
module is the one-level-up analogue of ``recovery_drain_registry.py``: it turns
the *self-contained* recovery phases into ``RecoveryPhase`` objects driven by an
ordered registry, and gives the tick a ``TickContext`` that names every
cross-phase threaded local so a phase body can read/write it as ``ctx.<field>``
instead of as a closure ``nonlocal``.

DESIGN ANCHOR: ``docs/PHASE4_ENTANGLED_CORE_DESIGN.md`` §2(a) option A2 + §3
Step 2. The HARD ORACLE is ``tests/test_recovery_tick_characterization.py``
(Step 1): the ordered phase sequence, per-phase owner / status / skip-reason /
``max_sync_work`` observables, the summary-key mapping, and the
budget-exhaustion epilogue are pinned there and MUST stay byte-identical.

PARTIAL ADOPTION (deliberate, mirrors the 2 CRM pinned drains the drain
registry left as named calls): only phases whose gating is a pure predicate
over already-settled context state, whose owner/``max_sync_work`` are constant
(or a small fixed skip-variant set), and which do NOT need a non-phase
threading statement to run between them and the next phase, are modelled here.
The deeply-entangled cascade clusters (the profile_refill / event-level
materialization 级联 with its 5-7-way skip ladders and per-branch owner drift,
the same-tick durable-handoff reason/max_sync computation, the projection
"one-durable-unit-per-tick" cascade, the explicit/remote-event followup 尾环
with its 4-tuple ``summary``/``workflow_resume``/``post_completion_reconcile``
rebind, and ``workflow_resume.extend`` accumulation) remain INLINE in the tick;
extracting them would require expressing the trailing threaded-local writes
that the next phase consumes, which cannot be done through ``ctx`` without
changing the observable phase records. See ``run_worker_recovery_once`` for the
inline clusters and the orchestrator-side commentary keyed to this module.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable


# ---------------------------------------------------------------------------
# Skip decision: a phase guard either runs the phase (True) or returns a fully
# computed skip record. The drain-registry precedent fixed owner/reason/
# max_sync per binding; here a phase may carry a per-branch owner (e.g.
# local_apply_backlog's worker-handoff branch reports owner
# "local_apply_closure_queue" while every other branch reports
# "profile_local_apply_command_owner"), so the guard returns the full triple.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SkipDecision:
    """A guard's decision to skip a phase with a fully computed skip record."""

    reason: str
    max_sync_work: str
    owner: str | None = None  # None => use the phase's default_owner


# ``wants_to_run`` returns ``True`` to run, or a ``SkipDecision`` to skip.
WantsToRun = bool | SkipDecision


@runtime_checkable
class RecoveryPhase(Protocol):
    """One ordered recovery-tick phase.

    ``name`` doubles as the ``recovery_phase_metrics`` key. ``default_owner``
    and ``default_max_sync_work`` are the owner / contract string used when the
    phase runs (and the skip default when a guard does not override them).
    ``wants_to_run(ctx)`` is the gating/skip-ladder guard; ``run(ctx)`` is the
    phase body, executed verbatim from the historical inline callback with
    ``nonlocal`` reads/writes rewritten to ``ctx.<field>``.
    """

    name: str
    default_owner: str
    default_max_sync_work: str

    def wants_to_run(self, ctx: "TickContext") -> WantsToRun: ...

    def run(self, ctx: "TickContext") -> Any: ...


@dataclass
class CallbackRecoveryPhase:
    """Concrete ``RecoveryPhase`` built from inline guard + body callables.

    Each migrated phase supplies a ``guard(ctx) -> WantsToRun`` and a
    ``body(ctx) -> result``. The body still routes through
    ``ctx.run_phase(...)`` so the uniform ``_run_recovery_phase`` metric
    recording / budget accounting that the characterization pins is reused
    unchanged.
    """

    name: str
    default_owner: str
    default_max_sync_work: str
    guard: Callable[["TickContext"], WantsToRun]
    body: Callable[["TickContext"], Any]

    def wants_to_run(self, ctx: "TickContext") -> WantsToRun:
        return self.guard(ctx)

    def run(self, ctx: "TickContext") -> Any:
        return self.body(ctx)


@dataclass
class TickContext:
    """Mutable per-tick context for ``run_worker_recovery_once``.

    Holds the orchestrator + payload, the uniform phase-runner callables (so a
    phase reuses the exact metric-recording / budget seam the oracle pins), and
    a NAMED FIELD for every cross-phase threaded local that was a ``nonlocal``
    in the historical method body. The nonlocal -> ctx-field rewrite is the
    ONLY non-verbatim transform applied to a migrated phase body.
    """

    # --- environment / shared services (closure captures) ---
    orchestrator: Any
    payload: dict[str, Any]

    # --- uniform phase runner seam (bound to the method-local helpers) ---
    run_phase: Callable[..., Any]
    skipped_phase: Callable[..., dict[str, Any]]
    request_durable_work_handoff_yield: Callable[[], None]

    # --- budget clock (read-only views onto the method-local helpers) ---
    tick_budget_exhausted: Callable[[], bool]
    recovery_tick_elapsed_ms: Callable[[], int]

    # --- scope / enable flags settled early in the tick ---
    explicit_job_id: str = ""
    profile_prefetch_refill_enabled: bool = True
    post_completion_reconcile_enabled: bool = True

    # --- Step 2b lift (B2, 2026-07-22): profile-refill cascade threading ---
    # First named fields lifted from the inline cascade clusters' method
    # locals (not oracle-observed; the addendum in
    # docs/PHASE4_ENTANGLED_CORE_DESIGN.md records the migration method).
    profile_refill_submit_observed_this_tick: bool = False
    profile_refill_command_planned_this_tick: bool = False

    # NOTE (Step 2 scope): only the phases currently migrated to the registry
    # read this context (orchestrator/payload/run_phase/skipped_phase/scope
    # flags). The densely-threaded cascade clusters (profile_refill /
    # event-level / projection / workflow_resume / remote_event_followup) remain
    # inline in run_worker_recovery_once with their raw method-locals — see the
    # Step 2 report's "left inline" list. When those migrate, add their threaded
    # nonlocals here as named fields; they are deliberately NOT pre-declared so
    # this context never over-promises the migration's actual reach.


def build_recovery_phase_registry(
    phases: tuple[RecoveryPhase, ...],
) -> tuple[RecoveryPhase, ...]:
    """Validate and freeze the ordered recovery-phase registry.

    Registration fails loudly on empty names and duplicate phase keys, exactly
    like ``build_recovery_drain_registry``: a dropped or shadowed phase would
    silently change the pinned phase sequence.
    """

    registry: list[RecoveryPhase] = []
    seen: set[str] = set()
    for phase in phases:
        if not isinstance(phase, RecoveryPhase):
            raise TypeError(
                f"recovery phase registry entries must be RecoveryPhase, got {type(phase).__name__}"
            )
        for field_name in ("name", "default_owner", "default_max_sync_work"):
            if not str(getattr(phase, field_name) or "").strip():
                raise ValueError(f"recovery phase {phase.name!r} has an empty {field_name}")
        if phase.name in seen:
            raise ValueError(f"duplicate recovery phase {phase.name!r}")
        seen.add(phase.name)
        registry.append(phase)
    if not registry:
        raise ValueError("recovery phase registry must not be empty")
    return tuple(registry)


@dataclass
class _DrainBindingPhase:
    """A ``RecoveryPhase`` adapter over one ``RecoveryDrainBinding``.

    The 14 uniform drains were already a clean data-driven seam
    (``DEFAULT_RECOVERY_DRAIN_BINDINGS`` / the inline ``for drain_binding in
    self._recovery_drain_registry`` loop). Wrapping each binding as a phase
    object lets the drain block flow through the SAME registry loop as the
    bespoke phases while preserving binding order, the payload-flag gate, the
    ``getattr(self, drain_method)(payload)`` call (so instance/class patches and
    facade wrappers keep intercepting), and the per-binding skip record. The
    crm_writer exclusion (metered, summary-invisible) is a property of the
    binding (``include_in_result`` / ``include_in_phase_budget_scan``), not of
    this adapter, so it is preserved by the caller's summary projection. The 2
    CRM pinned drains (crm_public_web_*) are NOT bindings; they stay named
    inline calls in the tick (guardrail-pinned literal source).
    """

    binding: Any  # RecoveryDrainBinding (avoid import cycle in the protocol layer)

    @property
    def name(self) -> str:
        return self.binding.phase

    @property
    def default_owner(self) -> str:
        return self.binding.owner

    @property
    def default_max_sync_work(self) -> str:
        return self.binding.max_sync_work

    def wants_to_run(self, ctx: "TickContext") -> WantsToRun:
        from .orchestrator import _coerce_bool  # local import: avoid cycle

        if _coerce_bool(ctx.payload.get(self.binding.payload_flag), True):
            return True
        return SkipDecision(
            reason=self.binding.disabled_reason,
            max_sync_work=self.binding.skipped_max_sync_work,
        )

    def run(self, ctx: "TickContext") -> Any:
        drain_method = self.binding.drain_method
        return ctx.run_phase(
            self.binding.phase,
            owner=self.binding.owner,
            max_sync_work=self.binding.max_sync_work,
            callback=lambda _drain_method=drain_method: getattr(ctx.orchestrator, _drain_method)(
                ctx.payload
            ),
        )


def build_drain_group_phases(drain_registry: tuple[Any, ...]) -> tuple[RecoveryPhase, ...]:
    """Wrap each drain binding into a ``RecoveryPhase`` preserving order."""

    return tuple(_DrainBindingPhase(binding) for binding in drain_registry)


def run_registry_phase(phase: RecoveryPhase, ctx: TickContext) -> Any:
    """Run one registry phase through the uniform guard + skip seam.

    The tick body's registry loop calls this for each migrated phase. Budget
    exhaustion is handled by ``ctx.run_phase`` itself (it records the
    ``recovery_tick_budget_exhausted`` skip row when the budget is spent), so a
    runnable guard still routes through ``run_phase`` and a skip guard routes
    through ``skipped_phase`` — preserving the exact metric rows the oracle
    pins.
    """

    decision = phase.wants_to_run(ctx)
    if decision is True:
        return phase.run(ctx)
    if isinstance(decision, SkipDecision):
        return ctx.skipped_phase(
            phase.name,
            owner=decision.owner or phase.default_owner,
            reason=decision.reason,
            max_sync_work=decision.max_sync_work,
        )
    raise TypeError(
        f"recovery phase {phase.name!r} wants_to_run returned {type(decision).__name__}, "
        "expected True or SkipDecision"
    )
