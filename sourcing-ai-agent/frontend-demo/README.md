# Frontend Demo

This directory contains a React + TypeScript demo frontend for `Sourcing AI Agent`.

## Scope

Current demo pages:

- `Search`
- `Plan Review`
- `Run Status`
- `Results Dashboard`
- `Manual Review`
- `Candidate One-Pager`

The app is designed to work in two modes:

- mock-first UI development
- API-ready mode for the local backend

## Run

```bash
cd sourcing-ai-agent/frontend-demo
npm install
npm run dev
```

## Environment

By default the app uses mock data.

To connect to the local backend API instead:

```bash
VITE_USE_MOCK=false
VITE_API_BASE_URL=http://127.0.0.1:8765
```

The production build is now also pinned to `http://127.0.0.1:8765`, so the deployed `pages.dev` frontend can talk directly to a backend running on the same machine without relying on a hard-coded `localtunnel` URL.

The results page will also try to load imported local Thinking Machines Lab assets from `public/tml/` before falling back to mock data.

## Import local Thinking Machines Lab assets

After you restore the handoff bundle into `../runtime`, run:

```bash
npm run import:tml-assets
```

This copies the latest restored Thinking Machines Lab normalized artifacts into:

- `public/tml/index.json`
- `public/tml/normalized_candidates.json`
- `public/tml/materialized_candidate_documents.json`
- `public/tml/reusable_candidate_documents.json`
- `public/tml/manual_review_backlog.json`
- `public/tml/profile_completion_backlog.json`
- `public/tml/asset_registry.json`

Then the `Results` page will render from real local JSON assets instead of mock candidates.

## Pull directly from object storage

If you do not have local runtime assets yet, but you already have Cloudflare R2 credentials configured in:

- `../runtime/secrets/providers.local.json`

or the equivalent `OBJECT_STORAGE_*` environment variables, you can pull the frontend JSON assets directly from object storage:

```bash
npm run pull:tml-assets
```

This uses the configured S3-compatible object storage API and downloads the Thinking Machines Lab frontend asset subset into:

- `public/tml/`

Optional overrides:

```bash
python3 ./scripts/pull_tml_assets.py \
  --bundle-kind company_handoff
```

By default, the script now:

- reads remote `indexes/bundle_index.json`
- finds the latest `company_handoff` bundle matching `thinkingmachineslab`
- downloads only the frontend JSON subset into `public/tml/`

You can still pin a specific bundle explicitly:

```bash
python3 ./scripts/pull_tml_assets.py \
  --bundle-kind company_handoff \
  --bundle-id company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z
```

## Backend pairing

Run the backend locally from the sibling project:

```bash
cd ../
PYTHONPATH=src python3 -m sourcing_agent.cli serve --port 8765
```

The backend CORS handler also responds to `Access-Control-Request-Private-Network`, which improves browser compatibility when `https://*.pages.dev` calls `http://127.0.0.1:8765`.

## Next recommended steps

1. Replace mock results with restored Thinking Machines Lab artifacts.
2. Add a workflow status page backed by `/api/jobs/{job_id}` and `/api/jobs/{job_id}/trace`.
3. Replace the current manual review demo actions with real `/api/manual-review/review` mutations.
4. Formalize the frontend/backend API contract in `docs/`.
