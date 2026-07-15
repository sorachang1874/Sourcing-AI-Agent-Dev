#!/usr/bin/env bash
# Build (or refresh) the dedicated reviewer CODEX_HOME.
#
# Rationale (docs/INDEPENDENT_REVIEWER_HOME_PROPOSAL.md): the shared
# ~/.codex/config.toml is rewritten by ChatGPT Desktop, which downgrades the
# top-level reviewer policy values and makes the canonical review runner
# fail closed. This script materializes an isolated CODEX_HOME whose
# config.toml is derived from the CURRENT shared config with the three
# reviewer policy values (configs/reviewer-codex/reviewer.toml) forced on
# top. Everything else (auth.json, sessions/, caches, ...) is symlinked back
# to the real ~/.codex so login state and rollout persistence keep working.
#
# GUARANTEE: this script only READS the real ~/.codex. It never writes,
# touches, or re-permissions anything under it.
set -euo pipefail
umask 077

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_HOME="${REVIEWER_SOURCE_CODEX_HOME:-$HOME/.codex}"
# `reviewer_codex_home` is a preserved pre-hardening legacy directory. It may
# contain real entries created by the old `ln -sfn` behavior, so never reuse or
# mutate it implicitly. The v2 path is the clean one-time canonical cutover.
TARGET_HOME="${REVIEWER_CODEX_HOME:-$REPO_ROOT/runtime/reviewer_codex_home_v2}"
POLICY_FILE="$REPO_ROOT/configs/reviewer-codex/reviewer.toml"

[ -d "$SOURCE_HOME" ] || { echo "source codex home not found: $SOURCE_HOME" >&2; exit 1; }
[ -f "$SOURCE_HOME/config.toml" ] || { echo "source config.toml not found" >&2; exit 1; }
[ -f "$POLICY_FILE" ] || { echo "reviewer policy file not found: $POLICY_FILE" >&2; exit 1; }

[ ! -L "$TARGET_HOME" ] || {
  echo "refusing symlinked reviewer CODEX_HOME: $TARGET_HOME" >&2
  exit 1
}
[ ! -e "$TARGET_HOME" ] || [ -d "$TARGET_HOME" ] || {
  echo "reviewer CODEX_HOME is not a directory: $TARGET_HOME" >&2
  exit 1
}

mkdir -p "$TARGET_HOME"
chmod 0700 "$TARGET_HOME"

# Fail before linking anything if an earlier/manual target contains a real
# entry with the same name. `ln -sfn SOURCE REAL_DIR` creates a nested link
# inside REAL_DIR instead of replacing it, so silently continuing would leave
# a mixed reviewer home. Existing real entries remain untouched for the
# operator to reconcile or preserve under a different target path.
for entry in "$SOURCE_HOME"/* "$SOURCE_HOME"/.[!.]*; do
  [ -e "$entry" ] || continue
  base="$(basename "$entry")"
  [ "$base" = "config.toml" ] && continue
  target_entry="$TARGET_HOME/$base"
  if { [ -e "$target_entry" ] || [ -L "$target_entry" ]; } && [ ! -L "$target_entry" ]; then
    echo "refusing to replace non-symlink reviewer entry: $target_entry" >&2
    exit 1
  fi
done

[ ! -d "$TARGET_HOME/config.toml" ] || [ -L "$TARGET_HOME/config.toml" ] || {
  echo "refusing to replace reviewer config directory: $TARGET_HOME/config.toml" >&2
  exit 1
}

# Symlink every top-level entry except config.toml (refresh on every run so
# newly created entries in the real home are picked up).
for entry in "$SOURCE_HOME"/* "$SOURCE_HOME"/.[!.]*; do
  [ -e "$entry" ] || continue
  base="$(basename "$entry")"
  [ "$base" = "config.toml" ] && continue
  ln -sfn "$entry" "$TARGET_HOME/$base"
done

# Derive reviewer config: current shared config + forced top-level policy trio.
python3 - "$SOURCE_HOME/config.toml" "$POLICY_FILE" "$TARGET_HOME/config.toml" <<'PY'
import os
import sys
import tempfile
import tomllib

src_path, policy_path, out_path = sys.argv[1:4]
with open(policy_path, "rb") as fh:
    policy = tomllib.load(fh)
forced = {k: policy[k] for k in ("model", "model_reasoning_effort", "service_tier")}

lines = open(src_path, encoding="utf-8").read().splitlines()
out, seen, in_section = [], set(), False
for line in lines:
    stripped = line.strip()
    if stripped.startswith("["):
        in_section = True
    if not in_section:
        key = stripped.split("=", 1)[0].strip() if "=" in stripped else None
        if key in forced:
            out.append(f'{key} = "{forced[key]}"')
            seen.add(key)
            continue
    out.append(line)

# Insert any forced key missing from the top level, before the first section.
missing = [k for k in forced if k not in seen]
if missing:
    insert_at = next((i for i, l in enumerate(out) if l.strip().startswith("[")), len(out))
    out[insert_at:insert_at] = [f'{k} = "{forced[k]}"' for k in missing]

text = "\n".join(out) + "\n"
tomllib.loads(text)  # fail closed on invalid TOML

# Replace the generated config atomically without following a pre-existing
# config symlink. The private mode is set before the file becomes visible.
fd, tmp_path = tempfile.mkstemp(prefix=".config.toml.", dir=os.path.dirname(out_path), text=True)
try:
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, out_path)
    tmp_path = ""
finally:
    if tmp_path:
        os.unlink(tmp_path)
PY

echo "reviewer CODEX_HOME ready: $TARGET_HOME"
python3 - "$TARGET_HOME/config.toml" <<'PY'
import json
import sys
import tomllib

with open(sys.argv[1], "rb") as fh:
    config = tomllib.load(fh)
keys = ("model", "model_reasoning_effort", "service_tier")
print("forced policy: " + " ".join(f"{key} = {json.dumps(config[key])}" for key in keys))
PY
