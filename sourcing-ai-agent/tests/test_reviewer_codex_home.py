from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = REPO_ROOT / "scripts" / "bootstrap_reviewer_codex_home.sh"
POLICY = REPO_ROOT / "configs" / "reviewer-codex" / "reviewer.toml"
POLICY_KEYS = ("model", "model_reasoning_effort", "service_tier")
DEFAULT_REVIEWER_HOME = REPO_ROOT / "runtime" / "reviewer_codex_home_v2"
LEGACY_REVIEWER_HOME = REPO_ROOT / "runtime" / "reviewer_codex_home"


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def _source_home(root: Path) -> Path:
    source = root / "source-codex-home"
    source.mkdir(mode=0o700)
    (source / "config.toml").write_text(
        "\n".join(
            (
                'model = "desktop-model"',
                'model_reasoning_effort = "medium"',
                'service_tier = "default"',
                "feature_flag = true",
                "",
                "[profiles.keep_nested]",
                'model = "nested-model"',
                "",
            )
        ),
        encoding="utf-8",
    )
    (source / "auth.json").write_text('{"token":"test-only"}\n', encoding="utf-8")
    (source / "sessions").mkdir()
    (source / "sessions" / "sentinel").write_text("rollout\n", encoding="utf-8")
    (source / "model_catalog.json").write_text('{"catalog":"shared"}\n', encoding="utf-8")
    (source / "models_cache.json").write_text('{"schema":"stale-source"}\n', encoding="utf-8")
    return source


def _run(source: Path, target: Path, *, check: bool = True) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "REVIEWER_SOURCE_CODEX_HOME": str(source),
            "REVIEWER_CODEX_HOME": str(target),
        }
    )
    return subprocess.run(
        ["bash", str(BOOTSTRAP)],
        cwd=REPO_ROOT,
        env=env,
        check=check,
        capture_output=True,
        text=True,
    )


def test_bootstrap_uses_isolated_policy_and_preserves_source_home(tmp_path: Path) -> None:
    source = _source_home(tmp_path)
    target = tmp_path / "reviewer-codex-home"
    source_config_digest = _digest(source / "config.toml")
    source_models_cache_digest = _digest(source / "models_cache.json")

    _run(source, target)
    first_config = (target / "config.toml").read_bytes()
    shared_names = ("auth.json", "sessions", "model_catalog.json")
    first_links = {name: os.readlink(target / name) for name in shared_names}
    _run(source, target)

    assert _digest(source / "config.toml") == source_config_digest
    assert _digest(source / "models_cache.json") == source_models_cache_digest
    assert (target / "config.toml").read_bytes() == first_config
    assert {name: os.readlink(target / name) for name in shared_names} == first_links
    assert first_links == {
        "auth.json": str(source / "auth.json"),
        "sessions": str(source / "sessions"),
        "model_catalog.json": str(source / "model_catalog.json"),
    }
    assert (target / "auth.json").is_symlink()
    assert (target / "sessions").is_symlink()
    assert (target / "model_catalog.json").is_symlink()
    assert not (target / "models_cache.json").exists()
    assert not (target / "models_cache.json").is_symlink()

    with (target / "config.toml").open("rb") as fh:
        generated = tomllib.load(fh)
    with POLICY.open("rb") as fh:
        policy = tomllib.load(fh)
    assert {key: generated[key] for key in POLICY_KEYS} == {key: policy[key] for key in POLICY_KEYS}
    assert generated["feature_flag"] is True
    assert generated["profiles"]["keep_nested"]["model"] == "nested-model"
    assert _mode(target) == 0o700
    assert _mode(target / "config.toml") == 0o600
    assert "umask 077" in BOOTSTRAP.read_text(encoding="utf-8")


def test_bootstrap_removes_legacy_models_cache_link_without_touching_source(tmp_path: Path) -> None:
    source = _source_home(tmp_path)
    target = tmp_path / "reviewer-codex-home"
    target.mkdir()
    target_cache = target / "models_cache.json"
    target_cache.symlink_to(source / "models_cache.json")
    source_models_cache = (source / "models_cache.json").read_bytes()

    _run(source, target)

    assert not target_cache.exists()
    assert not target_cache.is_symlink()
    assert (source / "models_cache.json").read_bytes() == source_models_cache
    assert (target / "auth.json").is_symlink()
    assert (target / "sessions").is_symlink()
    assert (target / "model_catalog.json").is_symlink()


def test_bootstrap_preserves_private_models_cache_across_refreshes(tmp_path: Path) -> None:
    source = _source_home(tmp_path)
    target = tmp_path / "reviewer-codex-home"
    source_models_cache = (source / "models_cache.json").read_bytes()

    _run(source, target)
    private_cache = target / "models_cache.json"
    private_cache.write_text('{"schema":"reviewer-runtime"}\n', encoding="utf-8")
    private_cache_bytes = private_cache.read_bytes()
    _run(source, target)

    assert private_cache.is_file()
    assert not private_cache.is_symlink()
    assert private_cache.read_bytes() == private_cache_bytes
    assert (source / "models_cache.json").read_bytes() == source_models_cache
    assert os.readlink(target / "auth.json") == str(source / "auth.json")
    assert os.readlink(target / "sessions") == str(source / "sessions")
    assert os.readlink(target / "model_catalog.json") == str(source / "model_catalog.json")


def test_bootstrap_rejects_non_file_private_models_cache_without_partial_refresh(tmp_path: Path) -> None:
    source = _source_home(tmp_path)
    target = tmp_path / "reviewer-codex-home"
    target.mkdir()
    private_cache_dir = target / "models_cache.json"
    private_cache_dir.mkdir()
    sentinel = private_cache_dir / "keep-me"
    sentinel.write_text("operator-state\n", encoding="utf-8")
    source_models_cache = (source / "models_cache.json").read_bytes()

    result = _run(source, target, check=False)

    assert result.returncode != 0
    assert f"refusing non-file reviewer-private entry: {private_cache_dir}" in result.stderr
    assert sentinel.read_text(encoding="utf-8") == "operator-state\n"
    assert (source / "models_cache.json").read_bytes() == source_models_cache
    assert not (target / "auth.json").exists()
    assert not (target / "config.toml").exists()


def test_bootstrap_fails_without_touching_same_name_real_directory(tmp_path: Path) -> None:
    source = _source_home(tmp_path)
    target = tmp_path / "reviewer-codex-home"
    target.mkdir()
    target_sessions = target / "sessions"
    target_sessions.mkdir()
    sentinel = target_sessions / "keep-me"
    sentinel.write_text("operator-state\n", encoding="utf-8")
    source_config_digest = _digest(source / "config.toml")

    result = _run(source, target, check=False)

    assert result.returncode != 0
    assert f"refusing to replace non-symlink reviewer entry: {target_sessions}" in result.stderr
    assert sentinel.read_text(encoding="utf-8") == "operator-state\n"
    assert not (target_sessions / "sessions").exists()
    assert not (target / "auth.json").exists()
    assert not (target / "config.toml").exists()
    assert _digest(source / "config.toml") == source_config_digest


def test_make_review_cuts_over_to_v2_isolated_home_and_1800_second_default() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "independent-review-gate",
            "REVIEW_EXECUTE=0",
            "REVIEW_BASE=HEAD",
            "REVIEW_FILES=scripts/bootstrap_reviewer_codex_home.sh docs/INDEPENDENT_REVIEW_GATE.md",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert f'REVIEWER_CODEX_HOME="{DEFAULT_REVIEWER_HOME}"' in result.stdout
    assert f'CODEX_HOME="{DEFAULT_REVIEWER_HOME}"' in result.stdout
    assert f'REVIEWER_CODEX_HOME="{LEGACY_REVIEWER_HOME}"' not in result.stdout
    assert f'CODEX_HOME="{LEGACY_REVIEWER_HOME}"' not in result.stdout
    assert '--timeout-seconds "1800"' in result.stdout
    assert '--file "scripts/bootstrap_reviewer_codex_home.sh"' in result.stdout
    assert '--file "docs/INDEPENDENT_REVIEW_GATE.md"' in result.stdout
    assert "--files" not in result.stdout
    assert 'TARGET_HOME="${REVIEWER_CODEX_HOME:-$REPO_ROOT/runtime/reviewer_codex_home_v2}"' in BOOTSTRAP.read_text(
        encoding="utf-8"
    )
