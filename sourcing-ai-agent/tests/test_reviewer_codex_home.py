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

    _run(source, target)
    first_config = (target / "config.toml").read_bytes()
    first_links = {name: os.readlink(target / name) for name in ("auth.json", "sessions")}
    _run(source, target)

    assert _digest(source / "config.toml") == source_config_digest
    assert (target / "config.toml").read_bytes() == first_config
    assert {name: os.readlink(target / name) for name in ("auth.json", "sessions")} == first_links
    assert first_links == {
        "auth.json": str(source / "auth.json"),
        "sessions": str(source / "sessions"),
    }
    assert (target / "auth.json").is_symlink()
    assert (target / "sessions").is_symlink()

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
            "REVIEW_FILES=scripts/bootstrap_reviewer_codex_home.sh",
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
    assert 'TARGET_HOME="${REVIEWER_CODEX_HOME:-$REPO_ROOT/runtime/reviewer_codex_home_v2}"' in BOOTSTRAP.read_text(
        encoding="utf-8"
    )
