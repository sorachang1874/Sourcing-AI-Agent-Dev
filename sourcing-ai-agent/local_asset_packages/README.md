# Local Asset Packages

> Status: Component/package-specific doc. Useful within its local scope, but not a global runtime contract unless `docs/INDEX.md` points to it explicitly.


This directory is the project-owned home for large local bootstrap packages that should no longer be read directly from sibling projects.

Current rule:
- application code prefers `local_asset_packages/<package>/` first
- legacy sibling-project discovery is kept only as a temporary import source
- synced package payloads are intentionally ignored by git

Current package path:
- `local_asset_packages/anthropic/`

Recommended workflow:
1. keep the external legacy package available only until the first import
2. run the Anthropic package sync/import flow
3. let supplement / Excel intake merge members into snapshot-authoritative assets
4. treat the resulting snapshot + artifacts + registry as the serve path
