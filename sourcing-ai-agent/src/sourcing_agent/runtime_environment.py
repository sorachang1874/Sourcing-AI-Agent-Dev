from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

LIVE_PROVIDER_MODE = "live"
NON_LIVE_PROVIDER_MODES = frozenset({"simulate", "replay", "scripted"})
PRODUCTION_RUNTIME_ENVIRONMENTS = frozenset({"production"})
ISOLATED_RUNTIME_ENVIRONMENTS = frozenset({"test", "simulate", "scripted", "replay", "ci"})
LIVE_PROVIDER_ACCESS_DISABLED_ENV = "SOURCING_LIVE_PROVIDER_ACCESS_DISABLED"
LIVE_PROVIDER_CONFIRM_ENV = "SOURCING_LIVE_PROVIDER_CONFIRM"
ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV = "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS"
LIVE_PROVIDER_SECRET_ENV_KEYS = (
    "APIFY_API_TOKEN",
    "APIFY_TOKEN",
    "APIFY_WEBHOOK_TOKEN",
    "HARVEST_API_TOKEN",
    "HARVEST_PROFILE_API_TOKEN",
    "HARVEST_PROFILE_SEARCH_API_TOKEN",
    "HARVEST_COMPANY_EMPLOYEES_API_TOKEN",
    "DATAFORSEO_LOGIN",
    "DATAFORSEO_PASSWORD",
    "SERPER_API_KEY",
)
RUNTIME_SCOPED_ENV_FILE_NAMES = (
    ".scripted-local-postgres.env",
)

_PROVIDER_INPUT_KEY_HINTS = frozenset(
    {
        "url",
        "urls",
        "profileurl",
        "profile_url",
        "linkedinurl",
        "linkedin_url",
        "publicidentifier",
        "public_identifier",
        "publicidentifiers",
        "public_identifiers",
        "profileid",
        "profile_id",
        "profileids",
        "profile_ids",
        "slug",
        "query",
        "keyword",
    }
)

_SYNTHETIC_PROVIDER_INPUT_RE = re.compile(
    r"(?:linkedin\.com/in/)?(?:"
    r"openai-agent-(?:current|former|cached|final|baseline)[a-z0-9-]*|"
    r"openai-chatgpt-(?:current|former)[a-z0-9-]*|"
    r"openai-whisper-[a-z0-9-]*|"
    r"lovable-roster-[a-z0-9-]*|"
    r"scripted-[a-z0-9-]+|"
    r"xai-(?:compute|research|safety)[a-z0-9-]*|"
    r"google-(?:veo|gemini)[a-z0-9-]*"
    r")",
    re.IGNORECASE,
)


class LiveProviderAccessError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ProviderIsolationContract:
    runtime_environment: str
    provider_mode: str
    live_provider_access_allowed: bool
    live_provider_access_disabled: bool
    live_provider_secret_env_keys: tuple[str, ...]

    def env_overrides(self) -> dict[str, str]:
        if self.live_provider_access_allowed:
            return {}
        return {
            LIVE_PROVIDER_ACCESS_DISABLED_ENV: "1",
            **{key: "" for key in self.live_provider_secret_env_keys},
        }


@dataclass(frozen=True, slots=True)
class RuntimeNamespaceOwnership:
    matches: bool
    owner_runtime_dir: str
    inferred_runtime_dir: str
    path: str
    reason: str

    def to_record(self) -> dict[str, Any]:
        return {
            "matches": self.matches,
            "owner_runtime_dir": self.owner_runtime_dir,
            "inferred_runtime_dir": self.inferred_runtime_dir,
            "path": self.path,
            "reason": self.reason,
        }


RUNTIME_NAMESPACE_PATH_KEYS = frozenset(
    {
        "artifact_path",
        "candidate_doc_path",
        "candidate_documents_path",
        "dataset_items_path",
        "discovery_dir",
        "last_snapshot_dir",
        "overlay_path",
        "raw_path",
        "root_snapshot_dir",
        "run_get_path",
        "run_post_path",
        "serving_projection_path",
        "snapshot_dir",
        "snapshot_path",
        "source_path",
        "summary_path",
    }
)


def _runtime_namespace_key_is_path(key: str, *, parent_key: str = "") -> bool:
    normalized = key.strip().lower().replace("-", "_")
    normalized_parent = parent_key.strip().lower().replace("-", "_")
    if normalized in RUNTIME_NAMESPACE_PATH_KEYS:
        return True
    if normalized_parent in {"artifact_paths", "paths", "runtime_paths"} and "url" not in normalized:
        return True
    return normalized.endswith(("_path", "_dir")) and "url" not in normalized and "uri" not in normalized


def iter_runtime_namespace_path_values(payload: Any) -> Iterator[tuple[str, str]]:
    def _walk(value: Any, *, parent_key: str = "") -> Iterator[tuple[str, str]]:
        if isinstance(value, Mapping):
            for key, item in value.items():
                normalized_key = str(key or "").strip()
                if isinstance(item, str) and _runtime_namespace_key_is_path(
                    normalized_key,
                    parent_key=parent_key,
                ):
                    path_value = item.strip()
                    if path_value:
                        yield normalized_key, path_value
                    continue
                yield from _walk(item, parent_key=normalized_key)
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                if isinstance(item, (Mapping, list, tuple, set)):
                    yield from _walk(item, parent_key=parent_key)

    yield from _walk(payload)

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

_ISOLATED_RUNTIME_CONTAINER_NAMES = frozenset(
    {
        "test_env",
        "scripted_smoke_current",
        "simulate_smoke_current",
        "replay_smoke_current",
        "scripted_runtime",
        "simulate_runtime",
        "replay_runtime",
    }
)
_ISOLATED_RUNTIME_ROOT_NAMES = frozenset(
    {
        "test_env",
        "test_env_live",
        "scripted_smoke",
        "simulate_smoke",
        "replay_smoke",
        "scripted_matrix",
    }
)


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


def _runtime_scoped_env_file_assignments(runtime_dir: Path | None) -> dict[str, str]:
    if runtime_dir is None:
        return {}
    for file_name in RUNTIME_SCOPED_ENV_FILE_NAMES:
        env_file = runtime_dir / file_name
        if not env_file.exists() or not env_file.is_file():
            continue
        assignments: dict[str, str] = {}
        try:
            lines = env_file.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            normalized_key = key.strip()
            if not normalized_key:
                continue
            normalized_value = value.strip()
            if (
                len(normalized_value) >= 2
                and normalized_value[0] == normalized_value[-1]
                and normalized_value[0] in {"'", '"'}
            ):
                normalized_value = normalized_value[1:-1]
            assignments[normalized_key] = normalized_value
        if assignments:
            return assignments
    return {}


def _infer_provider_mode_from_runtime_dir(runtime_dir: Path | None) -> str:
    if runtime_dir is None:
        return ""
    assignments = _runtime_scoped_env_file_assignments(runtime_dir)
    env_file_mode = str(assignments.get("SOURCING_EXTERNAL_PROVIDER_MODE") or "").strip()
    if env_file_mode:
        return normalize_provider_mode(env_file_mode)
    tokens = _runtime_dir_tokens(runtime_dir)
    for provider_mode in ("scripted", "simulate", "replay"):
        if any(token == provider_mode or token.startswith(f"{provider_mode}_") or f"_{provider_mode}_" in token for token in tokens):
            return provider_mode
    return ""


def _env_truthy(name: str, *, environ: Mapping[str, str] | None = None) -> bool:
    value = str((environ or os.environ).get(name) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def isolated_live_provider_access_confirmed(*, environ: Mapping[str, str] | None = None) -> bool:
    env = environ or os.environ
    return _env_truthy(LIVE_PROVIDER_CONFIRM_ENV, environ=env) and _env_truthy(
        ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV,
        environ=env,
    )


def infer_runtime_dir_from_path(
    base_path: str | Path | None,
    *,
    configured_runtime_dir: str | Path | None = None,
) -> Path | None:
    if base_path is None or not str(base_path).strip():
        return None
    raw_current = Path(base_path).expanduser()
    try:
        current = raw_current.resolve()
    except OSError:
        current = raw_current

    configured_resolved: Path | None = None
    configured_raw = str(
        configured_runtime_dir if configured_runtime_dir is not None else os.getenv("SOURCING_RUNTIME_DIR") or ""
    ).strip()
    if configured_raw:
        try:
            configured_resolved = Path(configured_raw).expanduser().resolve()
        except OSError:
            configured_resolved = Path(configured_raw).expanduser()

    for candidate in [raw_current, *raw_current.parents]:
        try:
            candidate_resolved = candidate.expanduser().resolve()
        except OSError:
            candidate_resolved = candidate.expanduser()
        if configured_resolved is not None and candidate_resolved == configured_resolved:
            return candidate
        name = candidate.name.strip().lower().replace("-", "_")
        parent_name = candidate.parent.name.strip().lower().replace("-", "_")
        if name in _ISOLATED_RUNTIME_ROOT_NAMES:
            return candidate
        if parent_name in _ISOLATED_RUNTIME_CONTAINER_NAMES:
            return candidate
        if candidate.parent.name == "runtime" and name.startswith(("test_env_", "scripted_", "simulate_", "replay_")):
            return candidate
        if candidate.name == "runtime":
            return candidate

    if configured_resolved is not None:
        try:
            current.relative_to(configured_resolved)
            return configured_resolved
        except (OSError, ValueError):
            pass

    for candidate in [current, *current.parents]:
        if candidate.name == "runtime":
            return candidate
    return None


def runtime_namespace_ownership_for_path(
    value: Any,
    *,
    configured_runtime_dir: str | Path | None = None,
) -> RuntimeNamespaceOwnership:
    path_value = str(value or "").strip()
    owner_raw = str(configured_runtime_dir or os.getenv("SOURCING_RUNTIME_DIR") or "").strip()
    if not path_value:
        return RuntimeNamespaceOwnership(True, owner_raw, "", "", "path_empty")
    if not owner_raw:
        return RuntimeNamespaceOwnership(True, "", "", path_value, "owner_runtime_dir_unconfigured")
    inferred = infer_runtime_dir_from_path(path_value, configured_runtime_dir=owner_raw)
    if inferred is None:
        return RuntimeNamespaceOwnership(True, owner_raw, "", path_value, "runtime_namespace_unknown")
    try:
        owner = Path(owner_raw).expanduser().resolve()
        candidate = inferred.expanduser().resolve()
    except OSError:
        owner = Path(owner_raw).expanduser()
        candidate = inferred.expanduser()
    matches = candidate == owner
    return RuntimeNamespaceOwnership(
        matches=matches,
        owner_runtime_dir=str(owner),
        inferred_runtime_dir=str(candidate),
        path=path_value,
        reason="runtime_namespace_match" if matches else "runtime_namespace_mismatch",
    )


def runtime_namespace_matches_path(
    value: Any,
    *,
    configured_runtime_dir: str | Path | None = None,
) -> bool:
    return runtime_namespace_ownership_for_path(
        value,
        configured_runtime_dir=configured_runtime_dir,
    ).matches


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
    path = _runtime_dir_path(runtime_dir)
    explicit_mode = str(provider_mode or "").strip()
    inferred_mode = "" if explicit_mode else _infer_provider_mode_from_runtime_dir(path)
    mode = normalize_provider_mode(explicit_mode or inferred_mode or None)
    runtime_file_assignments = _runtime_scoped_env_file_assignments(path) if path is not None else {}
    explicit_environment = str(runtime_environment or "").strip()
    inferred_environment = str(runtime_file_assignments.get("SOURCING_RUNTIME_ENVIRONMENT") or "").strip()
    environment_source: str | None
    if explicit_environment:
        environment_source = explicit_environment
    elif inferred_environment:
        environment_source = inferred_environment
    elif inferred_mode:
        environment_source = ""
    else:
        environment_source = None
    name = normalize_runtime_environment(environment_source, runtime_dir=path, provider_mode=mode)
    return RuntimeEnvironment(name=name, provider_mode=mode, runtime_dir=path)


def external_provider_mode() -> str:
    return normalize_provider_mode()


def provider_isolation_contract(
    *,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
    runtime_dir: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> ProviderIsolationContract:
    env = current_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=provider_mode,
        runtime_environment=runtime_environment,
    )
    isolated_live_allowed = bool(
        env.provider_mode == LIVE_PROVIDER_MODE
        and env.requires_isolated_state
        and isolated_live_provider_access_confirmed(environ=environ)
    )
    live_allowed = bool(
        env.provider_mode == LIVE_PROVIDER_MODE
        and (not env.requires_isolated_state or isolated_live_allowed)
    )
    return ProviderIsolationContract(
        runtime_environment=env.name,
        provider_mode=env.provider_mode,
        live_provider_access_allowed=live_allowed,
        live_provider_access_disabled=not live_allowed,
        live_provider_secret_env_keys=LIVE_PROVIDER_SECRET_ENV_KEYS,
    )


def provider_isolation_env_overrides(
    *,
    provider_mode: str | None = None,
    runtime_environment: str | None = None,
    runtime_dir: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    return provider_isolation_contract(
        provider_mode=provider_mode,
        runtime_environment=runtime_environment,
        runtime_dir=runtime_dir,
        environ=environ,
    ).env_overrides()


def synthetic_provider_input_markers(payload: Any) -> list[str]:
    markers: list[str] = []

    def _walk(value: Any, *, parent_key: str = "") -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                _walk(item, parent_key=str(key or ""))
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _walk(item, parent_key=parent_key)
            return
        if not isinstance(value, str):
            return
        text = " ".join(value.strip().split())
        if not text:
            return
        normalized_key = parent_key.strip().lower().replace("-", "_")
        provider_input_key = normalized_key in _PROVIDER_INPUT_KEY_HINTS or any(
            hint in normalized_key for hint in ("url", "identifier", "profile", "linkedin", "query", "keyword")
        )
        provider_input_value = "linkedin.com/in/" in text.lower()
        if not provider_input_key and not provider_input_value:
            return
        match = _SYNTHETIC_PROVIDER_INPUT_RE.search(text)
        if match is not None:
            markers.append(match.group(0))

    _walk(payload)
    return sorted(set(markers))


def assert_live_provider_access_allowed(
    *,
    provider_name: str,
    operation: str,
    provider_mode: str | None = None,
    runtime_dir: str | Path | None = None,
    runtime_environment: str | None = None,
    payload: Any | None = None,
) -> None:
    mode = normalize_provider_mode(provider_mode)
    if os.getenv(LIVE_PROVIDER_ACCESS_DISABLED_ENV) == "1":
        raise LiveProviderAccessError(
            f"Live provider access is disabled by {LIVE_PROVIDER_ACCESS_DISABLED_ENV}; "
            f"blocked {provider_name}.{operation}."
        )
    if mode != LIVE_PROVIDER_MODE:
        raise LiveProviderAccessError(
            f"Live provider access requires SOURCING_EXTERNAL_PROVIDER_MODE=live; "
            f"blocked {provider_name}.{operation} while provider_mode={mode}."
        )
    env = current_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=mode,
        runtime_environment=runtime_environment,
    )
    if env.requires_isolated_state and not isolated_live_provider_access_confirmed():
        raise LiveProviderAccessError(
            f"Live provider access in isolated runtime requires both {LIVE_PROVIDER_CONFIRM_ENV}=1 "
            f"and {ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV}=1; blocked {provider_name}.{operation} "
            f"for runtime_environment={env.name}, runtime_dir={env.runtime_dir or ''}."
        )
    markers = synthetic_provider_input_markers(payload)
    if markers:
        joined = ", ".join(markers[:5])
        raise LiveProviderAccessError(
            f"Live provider access blocked for synthetic/scripted fixture input in "
            f"{provider_name}.{operation}: {joined}."
        )
    validate_runtime_environment(
        runtime_dir=runtime_dir,
        provider_mode=mode,
        runtime_environment=runtime_environment,
    )


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
    isolation = provider_isolation_contract(
        provider_mode=env.provider_mode,
        runtime_environment=env.name,
        runtime_dir=env.runtime_dir,
    )
    return {
        "runtime_environment": env.name,
        "provider_mode": env.provider_mode,
        "provider_cache_namespace": "/".join(namespace or ()),
        "runtime_dir": str(env.runtime_dir or ""),
        "live_provider_access_allowed": str(isolation.live_provider_access_allowed).lower(),
        "live_provider_access_disabled": str(
            os.getenv(LIVE_PROVIDER_ACCESS_DISABLED_ENV) == "1" or isolation.live_provider_access_disabled
        ).lower(),
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
    if env.provider_mode == LIVE_PROVIDER_MODE and env.requires_isolated_state and not isolated_live_provider_access_confirmed():
        raise RuntimeError(
            f"Isolated runtime live provider access requires both {LIVE_PROVIDER_CONFIRM_ENV}=1 "
            f"and {ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS_ENV}=1. "
            f"runtime_environment={env.name}, runtime_dir={env.runtime_dir or ''}."
        )
