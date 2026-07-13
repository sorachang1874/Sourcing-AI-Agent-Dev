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

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_HOME="${REVIEWER_SOURCE_CODEX_HOME:-$HOME/.codex}"
TARGET_HOME="${REVIEWER_CODEX_HOME:-$REPO_ROOT/runtime/reviewer_codex_home}"
POLICY_FILE="$REPO_ROOT/configs/reviewer-codex/reviewer.toml"

[ -d "$SOURCE_HOME" ] || { echo "source codex home not found: $SOURCE_HOME" >&2; exit 1; }
[ -f "$SOURCE_HOME/config.toml" ] || { echo "source config.toml not found" >&2; exit 1; }
[ -f "$POLICY_FILE" ] || { echo "reviewer policy file not found: $POLICY_FILE" >&2; exit 1; }

mkdir -p "$TARGET_HOME"

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
import sys, tomllib

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
with open(out_path, "w", encoding="utf-8") as fh:
    fh.write(text)
PY

echo "reviewer CODEX_HOME ready: $TARGET_HOME"
echo "forced policy: $(grep -E '^(model|model_reasoning_effort|service_tier) ' "$TARGET_HOME/config.toml" | tr '\n' ' ')"
