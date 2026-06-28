"""Session-wide test isolation guards.

Background: on 2026-06-27 a unit test run (test_pipeline against a shared Postgres,
provider mode defaulting to live, with runtime/secrets/providers.local.json present)
caused real, billed Apify calls — a detached workflow-runner subprocess resolved
AppSettings.from_env() to live mode and read the production Apify token from the
secrets file.

This conftest closes the test/prod *secrets* boundary: it guarantees that no test
(or any subprocess a test spawns, which inherits os.environ) can read the real
production secrets file. Tests that need specific credentials build settings
explicitly or set SOURCING_SECRETS_FILE themselves; setdefault() below respects any
such explicit override (e.g. the Makefile live-test lanes), so it only supplies a
safe empty default when nothing else has.

Combined with the fail-closed provider-mode default (runtime_environment.
SAFE_DEFAULT_PROVIDER_MODE) this means a bare test process is doubly safe: non-live
by default (which blanks resolved tokens) AND pointed at an empty secrets file.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

# A stable, empty secrets file outside the repo. Stable path so subprocesses spawned
# during a test (which inherit this env) resolve to the same empty payload.
_ISOLATED_TEST_SECRETS = Path(tempfile.gettempdir()) / "sourcing_agent_test_empty_secrets.json"
try:
    if not _ISOLATED_TEST_SECRETS.exists():
        _ISOLATED_TEST_SECRETS.write_text("{}\n", encoding="utf-8")
    # Only supply the safe default when the caller has not pinned a secrets file.
    os.environ.setdefault("SOURCING_SECRETS_FILE", str(_ISOLATED_TEST_SECRETS))
except OSError:
    # If the temp file cannot be created, fall back to a definitely-nonexistent path
    # rather than leaving the real production secrets file reachable.
    os.environ.setdefault("SOURCING_SECRETS_FILE", str(_ISOLATED_TEST_SECRETS))
