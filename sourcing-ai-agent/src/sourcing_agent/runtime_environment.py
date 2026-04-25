from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

LIVE_PROVIDER_MODE = "live"
NON_LIVE_PROVIDER_MODES = frozenset({"simulate", "replay", "scripted"})
PRODUCTION_RUNTIME_ENVIRONMENTS = frozenset({"production"})
ISOLATED_RUNTIME_ENVIRONMENTS = frozenset({"test", "simulate", "scripted", "replay", "ci"})

_PROVIDER_MODE_ALIASES = {
    "": LIVE_PROVIDER_MODE,
    "prod": LIVE_PROVIDER_MODE,
    "production": LIVE_PROVIDER_MODE,
    "offline": "replay",
    "mock": "simulate",
    "fixture": "scripted",
}

_RUNTIME_ENV_ALIASES = {
    "": "",
    "dev": "local_dev",
    "local": "local_dev",
    "local-dev": "local_dev",
    "local_dev": "local_dev",
    "test": "test",
    "tests": "test",
    "ci": "ci",
    "simulate": "simulate",
    "simulation": "simulate",
    "scripted": "scripted",
    "scripted_test": "scripted",
    "replay": "replay",
    "hosted": "production",
    "prod": "production",
    "production": "production",
    "ecs": "production",
}


@dataclass(frozen=True, slots=True)
class RuntimeEnvironment:
    name: str
    provider_mode: str
    runtime_dir: Path | None

    @property
    def is_production(self) -> bool:
        return self.name in PRODUCTION_RUNTIME_ENVIRONMENTS

    @property
    def is_non_live_provider(self) -> bool:
        return self.provider_mode in NON_LIVE_PROVIDER_MODES

    @property
    def requires_isolated_state(self) -> bool:
        return self.name in ISOLATED_RUNTIME_ENVIRONMENTS or self.is_non_live_provider

    @property
    def provider_cache_namespace(self) -> tuple[str, str] | None:
        if self.provider_mode != LIVE_PROVIDER_MODE:
            return None
        return (self.name, self.provider_mode)


def normalize_provider_mode(raw_value: str | None = None) -> str:
    raw = str(raw_value if raw_value is not None else os.getenv("SOURCING_EXTERNAL_PROVIDER_MODE") or "").strip().lower()
    raw = raw.replace("-", "_")
    normalized = _PROVIDER_MODE_ALIASES.get(raw, raw)
    if normalized in {LIVE_PROVIDER_MODE, *NON_LIVE_PROVIDER_MODES}:
        return normalized
    return normalized or LIVE_PROVIDER_MODE


def _runtime_dir_path(runtime_dir: str | Path | None = None) -> Path | None:
    raw = str(runtime_dir if runtime_dir is not None else os.getenv("SOURCING_RUNTIME_DIR") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def _runtime_dir_tokens(runtime_dir: Path | None) -> set[str]:
    if runtime_dir is None:
        return set()
    return {part.strip().lower().replace("-", "_") for part in runtime_dir.parts if part.strip()}


def normalize_runtime_environment(
    raw_value: str | None = None,
    *,
    runtime_dir: str | Path | None = None,
    provider_mode: str | None = None,
) -> str:
    raw = str(raw_value if raw_value is not None else os.getenv("SOURCING_RUNTIME_ENVIRONMENT") or "").strip().lower()
    raw = raw.replace("-", "_")
    explicit = _RUNTIME_ENV_ALIASES.get(raw, raw)
    if explicit:
        return explicit

    mode = normalize_provider_mode(provider_mode)
    if mode in NON_LIVE_PROVIDER_MODES:
        return mode

    path = _runtime_dir_path(runtime_dir)
    tokens = _runtime_dir_tokens(path)
    if tokens & {"test_env", "test_env_live", "test", "tests", "ci", "simulate_smoke", "scripted_smoke"}:
        return "test"
    if tokens & {"runtime_hosted", "hosted_runtime", "production_runtime"}:
        return "production"
    return "local_dev"


def current_runtime_environment(
    *,
    runtime_dir: str | Path | None = None,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
) -> RuntimeEnvironment:
    mode = normalize_provider_mode(provider_mode)
    path = _runtime_dir_path(runtime_dir)
    name = normalize_runtime_environment(runtime_environment, runtime_dir=path, provider_mode=mode)
    return RuntimeEnvironment(name=name, provider_mode=mode, runtime_dir=path)


def external_provider_mode() -> str:
    return normalize_provider_mode()


def runtime_environment_name(*, runtime_dir: str | Path | None = None, provider_mode: str | None = None) -> str:
    return current_runtime_environment(runtime_dir=runtime_dir, provider_mode=provider_mode).name


def runtime_requires_isolated_state(
    *,
    runtime_dir: str | Path | None = None,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
) -> bool:
    return current_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=provider_mode,
        runtime_environment=runtime_environment,
    ).requires_isolated_state


def shared_provider_cache_dir(
    runtime_dir: str | Path,
    logical_name: str,
    *,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
) -> Path | None:
    env = current_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=provider_mode,
        runtime_environment=runtime_environment,
    )
    namespace = env.provider_cache_namespace
    if namespace is None:
        return None
    logical = str(logical_name or "").strip()
    if not logical:
        return None
    return Path(runtime_dir).expanduser() / "provider_cache" / namespace[0] / namespace[1] / logical


def shared_provider_cache_context(
    *,
    runtime_dir: str | Path | None = None,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
) -> dict[str, str]:
    env = current_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=provider_mode,
        runtime_environment=runtime_environment,
    )
    namespace = env.provider_cache_namespace
    return {
        "runtime_environment": env.name,
        "provider_mode": env.provider_mode,
        "provider_cache_namespace": "/".join(namespace or ()),
        "runtime_dir": str(env.runtime_dir or ""),
    }


def validate_runtime_environment(
    *,
    runtime_dir: str | Path | None = None,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
) -> None:
    env = current_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=provider_mode,
        runtime_environment=runtime_environment,
    )
    if env.is_production and env.is_non_live_provider:
        if os.getenv("SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER") == "1":
            return
        if env.provider_mode == "replay" and os.getenv("SOURCING_ALLOW_PRODUCTION_REPLAY") == "1":
            return
        raise RuntimeError(
            "Production runtime requires SOURCING_EXTERNAL_PROVIDER_MODE=live. "
            "Use an isolated test/scripted/replay runtime for non-live provider modes, "
            "or set SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER=1 only for an explicit smoke override."
        )
