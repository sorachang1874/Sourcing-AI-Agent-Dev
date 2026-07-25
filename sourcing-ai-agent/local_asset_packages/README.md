# Local Asset Packages

> Status: Component/package-specific doc. Useful within its local scope, but not a global runtime contract unless `docs/INDEX.md` points to it explicitly.


This directory is the project-owned home for large local bootstrap packages that should no longer be read directly from sibling projects.

Current rule:
- application code prefers `local_asset_packages/<package>/` first
- legacy sibling-project discovery is kept only as a temporary import source
- synced package payloads are intentionally ignored by git

Current package path:
- `local_asset_packages/anthropic/`

CI / synthetic fixture (2026-07-24):
- the real package is gitignored PERSONAL DATA and must never leave this machine
  (no commits, no fixtures with real values), so CI lanes cannot and must not use it
- curated CI gates instead build `local_asset_packages/synthetic_ci_fixture/`
  (`make synthetic-asset-package`, generator `tests/synthetic_asset_package.py`)
  and point `AssetCatalog.discover()` at it via `SOURCING_ASSET_PACKAGE_ROOT`
- the env override is fail-closed (invalid path errors out) and does not change
  production resolution order when unset; `tests/test_asset_catalog.py` guards both

Recommended workflow:
1. keep the external legacy package available only until the first import
2. run the Anthropic package sync/import flow
3. let supplement / Excel intake merge members into snapshot-authoritative assets
4. treat the resulting snapshot + artifacts + registry as the serve path
