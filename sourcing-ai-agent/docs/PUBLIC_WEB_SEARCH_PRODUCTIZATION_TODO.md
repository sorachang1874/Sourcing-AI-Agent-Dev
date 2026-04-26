# Public Web Search Productization TODO

> Status: Active productization tracker. This document now combines the accepted product shape, empirical search/fetch notes, implemented target-candidate Public Web backend/frontend slice, and remaining product hardening work. Review it with `NEXT_TODO.md`, `FRONTEND_API_CONTRACT.md`, and `LEAD_DISCOVERY_METHODS.md` before changing code.

## Goal

把旧的 `Public Web Stage 2` 从 workflow 内部阶段改造成目标候选人页上的独立批量能力：

- 用户先在候选人看板中把人加入 `target_candidates`。
- 用户进入目标候选人页面，选择一批人，触发 `Public Web Search`。
- 系统对这些目标候选人做可恢复、可审计的公开网络信息补全。
- 结果写回长期候选人资产，支持再次使用、二次补全、排序、筛选和导出。

这不是把 `enrich_public_web_signals` 重新塞回默认 workflow。默认 workflow 仍应优先交付候选人看板，Public Web Search 是用户显式触发的后续 enrich action。

## Current Progress Snapshot

Status as of `2026-04-26`:

- Completed storage cleanup prerequisite:
  - Public Web does not add a new SQLite-first product surface.
  - LinkedIn URL key normalization is storage-neutral via `linkedin_url_normalization.normalize_linkedin_profile_url_key(...)`.
  - New Public Web control-plane tables are registered through the PG-authoritative schema/live table path; any SQLite use is limited to the existing ephemeral shadow/bootstrap contract.
- Completed candidate-level experiment foundation:
  - `src/sourcing_agent/public_web_search.py` contains source-family query planning, entry-link classification/ranking, DataForSEO batch/queue execution, source-aware fetch slicing, deterministic email extraction, sanitized AI adjudication payloads, and artifact-only experiment output.
  - Live/replay calibration has validated high-value sources including personal homepages, Google Scholar pages, GitHub, paper/publication pages, and selected X/Substack entry links, with known noisy cases tracked below.
- Completed quality evaluation layer before richer UI/export/promotion rollout:
  - `src/sourcing_agent/public_web_quality.py` evaluates `signals.json` artifacts for email/source/evidence safety and media/profile identity quality.
  - CLI `evaluate-public-web-quality` writes `public_web_quality_report.json`, `public_web_quality_signals.csv`, and `public_web_quality_report.md`, with `--summary-only` for large runs and `--fail-on-high-risk` for CI-style gates.
  - Reports count trusted vs needs-review X/Substack/GitHub/Google Scholar links by type and flag common noise such as search-only identity, GitHub repo/deep links, X utility/post links, Substack non-profile pages, same-name Scholar pages, and promotion-recommended emails without source/evidence/trusted identity.
  - Public Web experiment options now include `max_ai_evidence_documents`; product API options normalize it to a bounded 1..20 range so larger pre-rollout LLM evidence budgets are explicit and auditable.
- Completed first backend productization slice:
  - `src/sourcing_agent/target_candidate_public_web.py` is the service boundary for target-candidate selected batch actions.
  - PG tables now exist for `target_candidate_public_web_batches`, `target_candidate_public_web_runs`, and `person_public_web_assets`.
  - `POST /api/target-candidates/public-web-search` creates an idempotent batch plus per-candidate runs and queues recoverable `exploration_specialist` workers.
  - `GET /api/target-candidates/public-web-search` lists batch/run state for polling.
  - Per-candidate runs persist query manifests and DataForSEO-style search checkpoints so recovery can poll/fetch existing remote tasks instead of resubmitting.
  - Completed runs with a normalized LinkedIn URL key upsert reusable `person_public_web_assets`.
  - Completed runs now materialize first-class `person_public_web_signals` rows for email candidates and profile/public links.
  - `GET /api/target-candidates/{record_id}/public-web-search` returns record detail with latest run/person asset summaries, grouped signals, typed email candidates, profile links, and evidence links.
  - The detail endpoint returns model-safe summaries/signals only; raw HTML/PDF/search payloads and raw document paths remain internal.
- Completed first frontend productization slice:
  - `frontend-demo/src/components/TargetCandidatesPanel.tsx` now has checkbox selection, a user-triggered `Public Web Search` action, backend state polling, and compact per-candidate run summaries.
  - The target-candidate card can open a Public Web detail section backed by `GET /api/target-candidates/{record_id}/public-web-search`.
  - Detail UI displays email candidates, profile/evidence links, identity labels, publishability, suppression reasons, and URL-shape warning chips.
  - The target-candidate page consumes `GET /api/target-candidates/public-web-search`; browser state is selection/cache only, not the Public Web truth source.
  - The UI now has Public Web promotion controls and a dedicated Public Web export button; raw HTML/PDF remain excluded from default export.
- Completed URL-shape and runtime-efficiency hardening:
  - Shared URL-shape helper marks GitHub repo/deep links, X status/search/utility URLs, Substack post/home-feed/deep links, and non-profile Scholar URLs.
  - `summary.primary_links` and `person_public_web_signals.publishable` require both trusted identity and clean URL shape.
  - Detail API/frontend surfaces `link_shape_warnings` and `clean_profile_link`; X/Substack deep links stay as evidence/review signals instead of clean profile links.
  - Candidate analysis supports bounded concurrent URL fetches and experiment CLI supports bounded concurrent candidate finalization/LLM adjudication.
- Completed manual promotion/export slice:
  - PG-authoritative `target_candidate_public_web_promotions` records manual email/link promotion or rejection with signal lineage, source URL/domain/family, identity/confidence, URL-shape metadata, operator, timestamp, previous value, and new value.
  - `GET/POST /api/target-candidates/{record_id}/public-web-promotions` exposes promotion state and writes promotion records. Email promotion writes the record before mutating `target_candidates.primary_email`.
  - `POST /api/target-candidates/public-web-export` creates a dedicated zip containing model-safe summaries, exported signals, evidence links, promotions, per-candidate JSON, and manifest. Default mode is `promoted_only`; `promoted_and_publishable` is explicit.
  - Target-candidate frontend detail rows can promote/reject email candidates and clean profile links. Public Web export defaults to selected candidates, or all target candidates when none are selected.
  - Browser E2E now covers target-candidate selection -> Public Web Search trigger -> refresh/polling -> compact status card, plus completed-run detail promotion -> promoted-only export download.
- Not completed yet:
  - Company-level Public Web refresh API/CLI lane.
  - Promotion override/reason workflow for non-publishable signals.
  - Product-level export mode controls beyond the default promoted-only package.

## Cross-Session Review Notes

Status as of `2026-04-26`: another session continued from the backend slice into the first frontend slice. This has been reviewed and is aligned with the intended product boundary.

Conforms to expectation:

- Public Web remains a user-triggered target-candidate action.
- The default workflow still stays independent; no new default `enrich_public_web_signals` path was introduced.
- The target-candidate page calls `POST /api/target-candidates/public-web-search` and polls `GET /api/target-candidates/public-web-search`.
- Frontend state is selection/cache only; it does not become the Public Web truth source.
- The UI displays compact run summaries and can open the record-level detail section for grouped signals, evidence links, URL-shape warnings, and promotion controls.
- Public Web email candidates are not written into `target_candidates.primary_email` until an explicit promotion API call writes a promotion record first.
- Raw HTML/PDF exclusion is kept visible in the UI copy and remains the default export policy.

Suggested follow-up improvements:

- Keep frontend browser E2E for selecting target candidates, triggering Public Web Search, polling run status, rendering compact status cards, and completed-run promotion/export download.
- Replace global `limit=500` frontend polling with scoped polling when target-candidate volume grows.
- Keep the detail section on top of `GET /api/target-candidates/{record_id}/public-web-search`; any future override promotion must still go through promotion persistence/API before mutating target-candidate fields.
- Keep company-level Public Web as a later API/CLI-only lane; do not surface it in the target-candidate page until candidate-level ROI is stable.

For the full resume record, read `SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md`.

## Product Decisions From Review

- Email extraction is enabled by default for Public Web Search, but not by turning on HarvestAPI email lookup or trusting low-confidence provider emails.
- Contact signals should come from public-web evidence such as personal homepages, CV/resume files, Google Scholar domain/profile evidence, paper PDFs, university profile pages, company pages, and other searchable public sources.
- AI adjudication is required for ownership, validity, freshness, and publishability of emails because rule-based extraction alone cannot reliably distinguish candidate-owned email from boilerplate or same-name collisions.
- Raw HTML/PDF assets are analysis inputs. They are stored for audit/reuse, but are not included in target-candidate export bundles by default.
- Reuse should behave like LinkedIn profile reuse: if the same person is later added to target candidates by another workflow/user, the previously collected public-web materials and analysis should be reusable after identity matching. In v1, automatic reuse is primarily gated by a normalized/sanity LinkedIn URL key, not by raw URL text or name-only matching.
- Public Web needs two levels:
  - company-level public web assets for official company publications, arXiv affiliation search, research/blog/engineering pages, and similar low-ROI broad scans
  - target-candidate public web enrichment for selected people, including email, personal homepage, papers/research direction, X/Substack/GitHub, media, and personal documents
- Public-web emails should be displayed like existing profile email metadata: source, type, confidence, publishability, and verification status should be visible.
- High-confidence email sources include personal homepages, CV/resume files, past papers, and academic/university profile pages when AI identity matching supports the candidate ownership.
- Writing a public-web email into `primary_email` should go through an explicit manual promotion action. The promotion record must preserve who/when/why/source and mark the email as manually promoted.
- X/Substack/GitHub/social links should also carry AI identity confidence. If the model cannot confidently confirm the link belongs to the target candidate, store it as `needs_review` or `ambiguous_identity`, not as a confirmed profile link.
- Company-level public web refresh is v1 API/CLI-only. UI controls can wait until target-candidate Public Web Search proves ROI.
- Run status has two scopes: a lightweight batch aggregate for one multi-select user action, and per-candidate runs for each selected target candidate. The per-candidate run is the v1 source of truth; batch status is only progress aggregation and is not the company-level public web lane.

## Current Review

### Existing pieces to reuse

- `target_candidates` 已经是跨 workflow 的目标候选人 truth source。
  - Backend API: `GET/POST /api/target-candidates`
  - Import API: `POST /api/target-candidates/import-from-job`
  - Export API: `POST /api/target-candidates/export`
  - Storage table: `target_candidates` with `metadata_json`
- 前端目标候选人页已存在：
  - `frontend-demo/src/pages/TargetCandidatesPage.tsx`
  - `frontend-demo/src/components/TargetCandidatesPanel.tsx`
  - 当前能力：列表、状态、质量分、备注、打开 LinkedIn、查看历史、批量导出。
- 当前 public-web 实现可复用但需要拆出 stage 语义：
  - `ExploratoryWebEnricher`
  - `document_extraction`
  - `search_provider`
  - `CompanyPublicationConnector`
  - `AssetLogger`
  - worker lane `exploration_specialist`
- 现有 search provider 已统一：
  - `dataforseo_google_organic`
  - `serper_google`
  - `google_browser`
  - `bing_html`
  - `duckduckgo_html`
  - provider chain fallback

### Historical limitations that drove the product split

- `Public Web Stage 2` 仍以 workflow stage/snapshot task 的形态存在，核心入口是 `enrich_public_web_signals`。
- `ExploratoryWebEnricher` 的结果主要写入 job/company snapshot 下的 `exploration/<candidate_id>/...`，不是可跨 workflow 复用的 person-level public-web asset，也不是 target-candidate overlay。
- `target_candidates` 原本只保存 CRM-like fields，缺少公开网络补全状态、运行记录、evidence summary、search artifact pointer；当前 target-candidate Public Web slice 已补上独立 run/signal/promotion/export 表达。
- 目标候选人导出原本主要打包 CSV + 可用 LinkedIn profile 原始文件；当前已新增 promoted-only Public Web 专用导出包，默认只导出人工确认的 model-safe signals/evidence/promotions/manifest。
- 网页/resume 的 email 原本没有一等 extraction contract；当前 Public Web experiment/service path 已有 deterministic extraction、AI adjudication、signal persistence 和 manual promotion guardrail。
- “Scholar”命名容易误导：当前 production 的 scholar coauthor 第一版实际是 arXiv author feed，而不是 Google Scholar profile connector。
- 现有 Stage 2 把 company-level publication discovery 和 per-person public-web enrichment 混在一个 workflow stage 里；产品化后必须拆成两条可独立触发、独立复用的 lane。

### Backend and frontend product slice now implemented

已完成第一层可执行后端 contract、first-class grouped signal detail API、目标候选人页 compact UI、Public Web detail section、manual promotion 和 promoted-only export：

- 新增 `src/sourcing_agent/target_candidate_public_web.py` 作为 target-candidate selected batch action 的服务边界。
- 新增 PG-authoritative control-plane tables：
  - `target_candidate_public_web_batches`
  - `target_candidate_public_web_runs`
  - `person_public_web_assets`
  - `person_public_web_signals`
  - `target_candidate_public_web_promotions`
- 新表已注册到 `DEFAULT_CONTROL_PLANE_TABLES` / live PG table registry / live writer indexes；磁盘 SQLite 只作为现有 schema bootstrap/shadow 机制，不作为 hosted/live 产品 truth source。
- 新增 API：
  - `POST /api/target-candidates/public-web-search`
  - `GET /api/target-candidates/public-web-search`
  - `GET /api/target-candidates/{record_id}/public-web-search`
  - `GET/POST /api/target-candidates/{record_id}/public-web-promotions`
  - `POST /api/target-candidates/public-web-export`
- POST 语义：
  - 接收 `record_ids` 和 `options`
  - 为一次多选操作创建 batch
  - 为每个目标候选人创建 per-candidate run
  - 同一批候选人和 options 重复提交时通过 idempotency key join 既有 batch/run
  - 创建 recoverable background worker，但不在 HTTP request 内跑 live DataForSEO/fetch/LLM
- Worker ownership：
  - 复用 `agent_worker_runs` 的 `exploration_specialist` lane
  - metadata `recovery_kind=target_candidate_public_web_search`
  - 初始 checkpoint 写 `stage=waiting_remote_search`，与 Stage 1 的远程 provider 等待/恢复模型一致
- DataForSEO checkpoint resume：
  - run 的 `search_checkpoint_json` 保存 query manifest、task key、provider task id、poll count、query results、classified entry links 和 errors
  - worker 第一次执行可提交 batch queue task；未 ready 时保持 run=`searching`、worker=`waiting_remote_search`
  - 下一轮 recovery takeover 复用同一 checkpoint poll/fetch，不重新 submit query
  - 所有 durable summary/signal 写完之后才把 run 更新为 terminal status
- Reuse layer：
  - run 完成后，如果存在 normalized LinkedIn URL key，会 upsert `person_public_web_assets`
  - v1 自动复用锚点是 `person_identity_key=linkedin:<normalized_key>`
  - 没有 LinkedIn key 的结果仍可用于当前 target candidate run，但不会 silent auto-merge 到 person asset
- Signal/detail layer：
  - run 完成后会把 `signals.json` 中的 email candidates 和 profile/public links 物化为 `person_public_web_signals`
  - signal row 保存 source URL/domain/family、identity label、confidence、publishability、suppression reason、artifact refs、model provider/version
  - record detail API 只返回 model-safe summaries/signals/evidence links，不返回 raw HTML/PDF/search payloads
- URL-shape/detail layer：
  - GitHub repo/deep link、X post/search/utility、Substack post/home-feed/deep link、非 profile Scholar URL 会写入 `link_shape_warnings`
  - `primary_links` 和 `person_public_web_signals.publishable` 同时要求 trusted identity 与 clean URL shape
  - detail API/frontend 会同时展示 identity label 和 URL-shape warning，避免把 evidence deep link 误读为 clean profile

尚未完成：

- company-level public web API/CLI-only refresh。
- promotion override/reason workflow。
- export mode controls beyond the default promoted-only package。

## Product Shape

### Two product lanes

#### Company-level Public Web Assets

用途：

- 从目标公司的官方/半官方公共来源补充长期资产。
- 主要服务于后续候选人 evidence matching、publication lead、研究方向补充，而不是立即提升每个候选人的联系信息。

典型来源：

- company homepage
- `/blog`
- `/research`
- `/engineering`
- `/news`
- `/docs`
- RSS / `index.xml`
- arXiv affiliation search
- OpenReview / publication platforms later

产品策略：

- 不默认运行。
- 可保留为 optional `Public Web Stage 2` / company asset completion action。
- v1 先提供 API/CLI-only trigger，不在前端目标候选人页放 UI 控制。
- 运行后写入 company-level asset，不绑死某个 target candidate。
- 后续 target-candidate search 可以读取这些公司级资产做姓名/论文/研究方向匹配。

#### Target-candidate Public Web Search

用途：

- 用户在目标候选人页面手动选择一批人，对这些人做公开网络补全。
- 重点产出可被 recruiter 使用、复用和导出的候选人级信息。

典型来源：

- personal homepage
- resume / CV / Google Docs export
- Google Scholar / paper pages / paper PDFs
- university profile pages
- GitHub
- X / Twitter
- Substack / personal blog
- talks / interviews / podcasts
- company/person-specific public pages
- public email/contact signals

产品策略：

- 用户显式触发。
- Email extraction 默认开启。
- 结果先进入 reusable person-level public-web asset，再通过 target candidate record 暴露给当前页面。
- 不把低置信 email 自动提升为 `primary_email`。

### User flow

1. 候选人看板中选择或单独点击 `加入目标候选人`。
2. 进入 `目标候选人` 页面。
3. 勾选一批目标候选人。
4. 点击 `Public Web Search`。
5. UI 显示每个人的状态：
   - `not_started`
   - `queued`
   - `searching`
   - `analyzing`
   - `completed`
   - `needs_review`
   - `failed`
6. 完成后在候选人卡片展示：
   - personal homepage
   - resume / CV
   - GitHub / X / Substack / Google Scholar discovery / publication links
   - AI-extracted work/education/affiliation signals
   - candidate-level email candidates, email type, confidence, source, publishability, and promotion status
   - social/profile links with `confirmed`, `likely`, `ambiguous`, or `needs_review` identity status
   - evidence count and last run timestamp
7. 导出目标候选人时，zip 应包含：
   - existing `target_candidates_summary.csv`
   - per-candidate public web summary JSON
   - evidence links CSV
   - model-safe summaries
   - raw asset manifest
   - no raw HTML/PDF by default

### Product boundaries

- Public Web Search 是 user-triggered batch enrichment，不是默认 acquisition prerequisite。
- 它应支持重复运行，但默认应复用同一人的 successful public-web assets，除非用户显式 `force_refresh` 或出现身份冲突。
- 自动复用 person-level asset 时，应优先要求 normalized/sanity LinkedIn URL key 匹配。raw LinkedIn URL 需要先规范化；raw value 可以留作 evidence，但不能作为 durable identity key。
- 它应支持 candidate subset，而不是整池统一深挖。
- 它应能在后台运行、恢复、取消或重新排队，不阻塞目标候选人页面打开。
- 它输出的是 evidence-backed enrich signals，不应自动覆盖人工备注或高置信 LinkedIn profile fields。
- Company-level publication/blog/arXiv assets 和 target-candidate personal/contact assets 要分开治理、分开导出、分开复用。
- Public-web email candidates can be high-confidence and visible without being promoted to `primary_email`; `primary_email` mutation requires an explicit promotion record.

## Workflow Orchestration Lessons From LinkedIn Stage 1

LinkedIn Stage 1 的踩坑可以直接变成 Public Web Search 的编排约束。这里的核心不是把 Public Web 重新挂回 workflow stage，而是把同样的状态机、防重复派发、恢复和资产完成语义用到目标候选人的独立批量动作上。

### Orchestration principles

- Keep the default sourcing workflow independent. Target-candidate Public Web Search must be a user-triggered batch action, not a hidden prerequisite for candidate board readiness and not a new default `enrich_public_web_signals` path.
- Treat per-candidate run state as the source of truth. A batch row only aggregates one multi-select action; it must never be the only durable state that says a candidate is completed, failed, or reusable.
- Do not mark a candidate run `completed` until durable model-safe outputs exist. For v1 this means query manifest/search result summary, entry links, fetched document manifest/evidence slices, extracted signals, AI adjudication result when enabled, and compact export summary are written. Later PG tables should commit signals before the status changes to terminal.
- Separate "current action can continue" from "asset is reusable by future users." A candidate can finish with partial evidence and still be useful for the current UI, but person-level auto-reuse should require the normalized/sanity LinkedIn URL key and completed reusable person asset metadata.
- Recovery should resume from checkpoints, not redispatch from scratch. Persist DataForSEO task IDs/checkpoints, fetched URL keys, evidence-slice paths, email candidates, model adjudication attempts, and terminal errors so a takeover can continue at search, fetch, or analyze phase.
- Duplicate provider dispatch is a regression. Use an idempotency signature based on normalized LinkedIn URL key, target candidate record id, source families, query text, provider, force-refresh flag, and run id. A user double-click, page refresh, or recovery takeover should join or resume in-flight work unless `force_refresh=true`.
- Partial failure should be local. One candidate's search timeout, blocked page, fetch error, or LLM failure should produce `completed_with_errors` or `needs_review` for that candidate; it should not block other selected candidates from entering fetch/analyze/exportable states.
- Batch search should not force every candidate to wait for the slowest query when productized. The experiment harness can still fetch a batch after polling, but the service path should emit candidate-level readiness as soon as enough high-value entry links are available, then continue late query results as incremental evidence or a follow-up run.
- Backpressure needs separate budgets. DataForSEO submit/poll/fetch, URL fetch, PDF extraction, and LLM adjudication have different rate limits and failure modes. Product serving should queue/limit them independently and keep persistence writes behind a single clear writer contract, mirroring the materialization writer lessons from Stage 1.
- Fetch and LLM phases should be resumable independently. A fetched document with an evidence slice should not be refetched just because model adjudication failed; an LLM retry should reuse the same deterministic payload unless `force_refresh` changes the evidence set.
- UI progress must show real phases, not optimistic completion. Useful per-candidate phases are `queued`, `search_submitted`, `searching`, `entry_links_ready`, `fetching`, `analyzing`, `completed`, `completed_with_errors`, `needs_review`, `failed`, and `cancelled`.
- Cancel/retry semantics should be explicit. Cancelling a batch should stop unscheduled work, preserve completed candidate assets, mark in-flight candidates cancelled where possible, and leave failed/partial runs retryable without losing evidence.
- Export should read from completed model-safe outputs or PG signal tables, not from raw HTML/PDF scans. Raw assets remain audit inputs and need an explicit future debug/audit export flag.

### Public Web service state machine draft

For each selected target candidate:

1. `queued`
   - create `target_candidate_public_web_runs` row with request options, idempotency key, artifact root, and normalized LinkedIn URL key
2. `search_submitted`
   - write query manifest and provider task checkpoints
3. `searching`
   - poll/fetch provider tasks; persist per-query result/error; allow candidate-level transition once enough entry links exist
4. `entry_links_ready`
   - classify/rank entry links, write entry-link summary, decide diversified fetch queue
5. `fetching`
   - fetch available source types with per-source-type coverage, write raw/internal assets plus model-safe evidence slices, extract deterministic emails/links
6. `analyzing`
   - send one aggregated candidate-level LLM payload when AI is enabled and useful; sanitize output so the model cannot introduce unobserved emails or unknown URLs
7. terminal
   - `completed` when model-safe summary/signals are persisted without run-level errors
   - `completed_with_errors` when useful evidence exists but some source families failed
   - `needs_review` when identity/email/social confidence is too ambiguous for automatic display as confirmed
   - `failed` only when no useful result can be produced

The batch aggregate should derive counts from these per-candidate rows:

- `queued_count`
- `running_count`
- `entry_links_ready_count`
- `fetching_count`
- `analyzing_count`
- `completed_count`
- `completed_with_errors_count`
- `needs_review_count`
- `failed_count`
- `cancelled_count`

### Regression tests needed before frontend/API rollout

- Default workflow tests must continue asserting that single-stage jobs do not include `enrich_public_web_signals` or `public_web_stage_2` summaries.
- Batch trigger idempotency: submitting the same selected record ids and options twice should join or return existing in-flight runs, not duplicate DataForSEO tasks.
- Recovery resume: a run with submitted DataForSEO task checkpoints should poll/fetch existing tasks rather than submit new ones after process restart.
- Candidate isolation: one candidate's query timeout or fetch failure should not prevent other candidates in the same batch from reaching `completed`.
- Completion ordering: terminal status may be written only after signals/model-safe summaries are persisted.
- Fetch diversity: when enough fetch budget and source types exist, the first fetch window should include one slice per available media/source type before duplicate source types.
- AI output safety: model-invented emails and unknown URLs must be dropped; normalized link aliases such as `scholar_profile` must map to canonical Public Web signal types.
- Export safety: default Web Search export includes summaries/signals/evidence links but excludes raw HTML/PDF.
- Manual email promotion: no public-web email can mutate `target_candidates.primary_email` without a promotion record.
- Reuse identity: person-level auto-reuse requires normalized/sanity LinkedIn URL key match in v1; name/company-only matches remain review candidates.

### Reuse semantics

Public Web Search reuse should follow the same mental model as LinkedIn profile reuse:

- A successful person-level public-web asset is not tied to the one target candidate record that triggered it.
- If the same person is later selected from another workflow or added by another user, the existing person-level public-web asset can be reused after identity matching.
- Reuse is based on stable person identity keys, not on a short TTL. In v1, the stable auto-reuse key is the normalized/sanity LinkedIn URL key whenever LinkedIn exists.
- `force_refresh=true` creates a new run while retaining previous runs for audit.
- If identity matching is strong, the existing asset can hydrate the new target candidate overlay automatically.
- If identity matching is weak, same-name only, or conflicting, the UI should show `needs_review` rather than silently merging.
- Google Scholar URL, homepage domain, email evidence, name, company, and affiliation can create review candidates or support AI adjudication. They should not silently auto-reuse a person asset in v1 without a matching normalized LinkedIn key.
- If the stored value is a raw LinkedIn URL, normalize it first and compare the canonical URL key. Raw LinkedIn text should remain an evidence/source field, not the reusable identity key.
- Source-family freshness can still be surfaced as metadata:
  - personal homepage / GitHub / X / Substack may become stale faster
  - papers / CV PDFs / university pages may remain useful but need currentness context
  - company emails require stronger currentness evidence before promotion

## Proposed Module Boundary

### New backend module

建议把实现拆成一个 public-web 专用模块边界，而不是继续扩展 workflow acquisition stage：

- `src/sourcing_agent/public_web_search.py`
  - shared source-family registry
  - shared query planning
  - shared signal schema
  - shared reuse/identity matching helpers
- `src/sourcing_agent/company_public_web_assets.py`
  - company-level official/publication/arXiv asset refresh
  - may wrap/refactor existing `CompanyPublicationConnector`
- `src/sourcing_agent/target_candidate_public_web.py`
  - target-candidate selected batch action
  - person-level public-web asset creation/reuse
  - target-candidate overlay update

职责：

- 接收 company scope or target candidate records。
- 决定每个 scope 需要哪些 source families。
- 调用 search provider、document extraction、AI analysis。
- 产出 company-level or person-level public-web assets。
- 将 person-level result 绑定到 current target candidate record。
- 写入 store + artifact directory。
- 返回 batch/run summary。

不要把这部分继续挂在 `AcquisitionEngine._enrich_profiles(...)` 或 `MultiSourceEnricher.enrich(...)` 的 workflow stage path 里。

### Suggested source families

把旧的 `Public Web Stage 2` 拆成更清晰的 source families，并明确 scope。

#### Company-level source families

- `company_official_publications`
  - company homepage, `/blog`, `/research`, `/engineering`, `/news`, `/docs`, RSS
- `company_arxiv_affiliation`
  - arXiv affiliation search by company name/domain root
- `company_publication_platforms`
  - OpenReview / Semantic Scholar / other publication surfaces later

These are low-ROI broad scans. They can enrich candidate evidence later, but should not run by default in every workflow.

#### Target-candidate source families

- `profile_web_presence`
  - general web, personal homepage, profile pages
- `resume_and_documents`
  - PDF resume, HTML CV, Google Docs export, portfolio PDFs
- `technical_presence`
  - GitHub, package registries, project pages
- `social_presence`
  - X/Twitter, Substack, talks, interviews, podcasts
- `candidate_publication_presence`
  - Google Scholar organic discovery, arXiv/paper pages, paper PDFs, university pages, OpenReview links
- `scholar_profile_discovery`
  - organic discovery/fetch of Google Scholar pages where accessible; structured metrics parser is a future dedicated connector
- `contact_signals`
  - personal emails, academic emails, company emails, contact pages, obfuscated emails, homepage contact forms

Each family should have:

- query templates
- provider policy
- fetch policy
- extraction schema
- confidence and suppression rules
- artifact path convention
- export representation

### V1 default source families

Target-candidate Public Web Search should default to:

- `profile_web_presence`
- `resume_and_documents`
- `technical_presence`
- `social_presence`
- `candidate_publication_presence`
- `scholar_profile_discovery`
- `contact_signals`

Company-level Public Web Assets should default to an explicit opt-in action with:

- `company_official_publications`
- `company_arxiv_affiliation`

`company_publication_platforms` and a first-class Google Scholar connector can be added after v1 stabilizes.

### Existing modules to refactor toward this shape

- `exploratory_enrichment.py`
  - keep page fetch/search mechanics, but move candidate target batch orchestration to `public_web_search.py`
  - make query generation source-family aware instead of one generic list
- `document_extraction.py`
  - add structured contact extraction inputs/outputs
  - keep deterministic link extraction, but let AI adjudication handle harder cases
- `model_provider.py`
  - add/extend page analysis contract for contact signals and source-family labels
- `storage.py`
  - target-candidate batch/run and person asset records are now durable PG-backed records
  - remaining storage work: company assets, email promotions, and target overlay cache
- `orchestrator.py`
  - target-candidate Public Web start/list/detail service methods are exposed
  - remaining orchestration work: company lane, promotion flow, richer export composition
- `api.py`
  - target-candidate Public Web POST/list/detail endpoints are implemented
  - remaining API work: company lane endpoints, promotion endpoint, Web Search export options
- `frontend-demo/src/components/TargetCandidatesPanel.tsx`
  - add selection, batch action, status, and evidence summary UI
- `frontend-demo/src/lib/api.ts`
  - add typed client methods and cache invalidation

## Data Model

### Prefer a dedicated table

Do not store the full result only inside `target_candidates.metadata_json`. That field can carry denormalized status/summary for fast rendering, but the auditable state should be separate.

The durable model should separate four concepts:

- company-level public web assets
- reusable person-level public web assets
- lightweight target-candidate batch aggregate for one user multi-select action
- target-candidate run/overlay that links the selected CRM record to the reusable person asset

Proposed tables:

Implementation status as of `2026-04-26`: v1 backend ships `target_candidate_public_web_batches`, `target_candidate_public_web_runs`, `person_public_web_assets`, `person_public_web_signals`, and `target_candidate_public_web_promotions`. The broader company-level tables and a separate `person_public_web_runs` table remain future work. The current per-person run state is represented by `target_candidate_public_web_runs` plus reusable `person_public_web_assets`.

Actual v1 table behavior:

- `target_candidate_public_web_batches` stores one UI multi-select action, the idempotency key, requested record IDs, source families, options, run IDs, aggregate summary, requester, and `force_refresh`.
- `target_candidate_public_web_runs` stores the durable per-candidate state: target candidate identity fields, normalized LinkedIn key, person identity key, idempotency key, status/phase, query manifest, `search_checkpoint_json`, future fetch/analyze checkpoint slots, summary, artifact root, worker key, lease fields, attempts, and last error.
- `person_public_web_assets` stores the reusable person-level asset keyed by `person_identity_key`, currently `linkedin:<normalized_linkedin_url_key>` when available, plus latest run summary/signals and source run IDs.
- `person_public_web_signals` stores durable model-safe email/link signals by run/person/record, including source family, source URL/domain, identity confidence, publishability, suppression reason, artifact refs, and model provider/version.

#### `company_public_web_asset_runs`

- `run_id`
- `company_key`
- `target_company`
- `source_families_json`
- `status`
- `provider_policy_json`
- `force_refresh`
- `started_at`
- `completed_at`
- `error`
- `artifact_root`
- `summary_json`
- `created_at`
- `updated_at`

#### `company_public_web_assets`

- `asset_id`
- `run_id`
- `company_key`
- `target_company`
- `source_family`
- `asset_type`
- `title`
- `url`
- `source_domain`
- `normalized_key`
- `publication_year`
- `authors_json`
- `topics_json`
- `artifact_path`
- `evidence_json`
- `created_at`
- `updated_at`

Company-level assets should also register through existing asset governance when promoted:

- `scope_kind = company`
- `scope_key = company_key`
- `asset_kind = public_web_publications`

#### `person_public_web_assets`

This is the reusable layer, analogous to LinkedIn profile reuse.

Implemented in v1 as a narrowed reusable asset row keyed by `person_identity_key`, with `linkedin_url_key`, latest run, target candidate lineage, summary/signals JSON, source run IDs, artifact root, and metadata. The richer fields below remain the target shape for later identity review and UI display.

- `person_asset_id`
- `person_identity_key`
- `identity_confidence_label`
- `canonical_name`
- `known_company`
- `linkedin_url_key`
- `linkedin_url_raw_examples_json`
- `scholar_url_key`
- `primary_homepage_domain`
- `matched_target_candidate_record_ids_json`
- `latest_successful_run_id`
- `latest_summary_json`
- `created_at`
- `updated_at`

Identity matching and automatic reuse:

1. Automatic reuse in v1 is allowed when the normalized/sanity LinkedIn URL key matches.
2. Raw LinkedIn URLs must be canonicalized before comparison. Store raw variants only as evidence/audit examples.
3. Verified Google Scholar URL key, personal homepage URL/domain plus matching name, email domain/name evidence, publication evidence, or affiliation evidence can strengthen review confidence.
4. Google Scholar/homepage/email/name/company evidence without a LinkedIn key match should create `needs_review` or `ambiguous_identity` candidates, not silent auto-merge.
5. Normalized name + target company is always a low-confidence candidate match and must never silently auto-merge.

Implementation note: reuse `linkedin_url_normalization.normalize_linkedin_profile_url_key(...)` when deriving `linkedin_url_key`; do not call through the store facade for identity utilities. If profile registry metadata has both `raw_linkedin_url` and `sanity_linkedin_url`, prefer `sanity_linkedin_url` as the source for the key and retain `raw_linkedin_url` only for lineage.

#### `target_candidate_public_web_batches`

This table is a lightweight aggregate for one target-candidate page multi-select action. It is not the company-level public web lane, and it should not be the source of truth for individual candidate state.

Implemented in v1 as a lightweight aggregate with `run_ids_json` and `summary_json` instead of separate count columns. The counts remain an API/UI summary concern derived from per-candidate runs.

- `batch_id`
- `requested_record_ids_json`
- `requested_source_families_json`
- `provider_policy_json`
- `force_refresh`
- `requested_by`
- `status`
- `queued_count`
- `running_count`
- `completed_count`
- `needs_review_count`
- `failed_count`
- `started_at`
- `completed_at`
- `error`
- `summary_json`
- `created_at`
- `updated_at`

#### `person_public_web_runs`

Not implemented in v1. Current per-person run state is represented by `target_candidate_public_web_runs`; a separate person run table should only be added if reuse, retries, or cross-record asset lifecycle need a dedicated run layer.

- `run_id`
- `person_asset_id`
- `person_identity_key`
- `trigger_record_id`
- `trigger_candidate_id`
- `trigger_job_id`
- `trigger_history_id`
- `known_target_company`
- `status`
- `requested_source_families_json`
- `provider_policy_json`
- `force_refresh`
- `requested_by`
- `started_at`
- `completed_at`
- `error`
- `artifact_root`
- `summary_json`
- `created_at`
- `updated_at`

#### `target_candidate_public_web_runs`

This table records the user-triggered action for the current target candidate row.

Implemented in v1 as a narrowed per-candidate run table with target-candidate identity fields, normalized LinkedIn key, person identity key, idempotency key, status/phase, query manifest, checkpoint JSON fields, summary, artifact root, worker key, lease/attempt metadata, and last error. Conceptual fields such as `person_run_id`, job/history linkage, and richer target overlay linkage remain future work.

- `run_id`
- `batch_id`
- `record_id`
- `person_asset_id`
- `person_run_id`
- `candidate_id`
- `job_id`
- `history_id`
- `target_company`
- `status`
- `phase`
- `requested_source_families_json`
- `provider_policy_json`
- `options_json`
- `query_manifest_json`
- `search_checkpoint_json`
- `fetch_checkpoint_json`
- `analysis_checkpoint_json`
- `force_refresh`
- `requested_by`
- `linkedin_url`
- `linkedin_url_key`
- `person_identity_key`
- `worker_key`
- `lease_owner`
- `lease_expires_at`
- `attempt_count`
- `last_error`
- `started_at`
- `completed_at`
- `error`
- `artifact_root`
- `summary_json`
- `created_at`
- `updated_at`

#### `person_public_web_signals`

Not implemented as a dedicated table in v1. The current backend writes compact extracted/adjudicated signals into run/person summaries and artifact outputs. Add this table before treating grouped evidence, email candidates, or social link identity labels as independently queryable frontend/export truth.

- `signal_id`
- `run_id`
- `person_asset_id`
- `record_id`
- `candidate_id`
- `source_family`
- `signal_type`
- `value`
- `normalized_value`
- `confidence_label`
- `confidence_score`
- `publishable`
- `suppression_reason`
- `identity_match_label`
- `identity_match_score`
- `email_type`
- `promotion_status`
- `promotion_record_id`
- `source_url`
- `source_domain`
- `source_title`
- `artifact_path`
- `evidence_json`
- `model_version`
- `created_at`

Signal types should include:

- `personal_homepage`
- `resume_url`
- `github_url`
- `x_url`
- `substack_url`
- `scholar_url`
- `publication_url`
- `email_candidate`
- `affiliation`
- `work_history`
- `education`
- `technical_project`
- `talk_or_interview`

Email signal fields:

- `email_type`
  - `personal`
  - `academic`
  - `company`
  - `generic`
  - `unknown`
- `promotion_status`
  - `not_promoted`
  - `promotion_recommended`
  - `manually_promoted`
  - `rejected`
  - `suppressed`

High-confidence extracted emails can be marked `promotion_recommended`, but should not mutate `target_candidates.primary_email` until a user explicitly promotes them.

#### `target_candidate_public_web_promotions`

Implemented in v1 for target-candidate Public Web Search. Public-web emails and clean profile links can be manually promoted/rejected, and `target_candidates.primary_email` is mutated only after a promotion row has been written.

- `promotion_id`
- `signal_id`
- `run_id`
- `asset_id`
- `person_identity_key`
- `record_id`
- `candidate_id`
- `signal_kind`
- `signal_type`
- `value` / `new_value`
- `email_type`
- `source_url`
- `source_domain`
- `source_family`
- `confidence_label` / `confidence_score`
- `identity_match_label` / `identity_match_score`
- `clean_profile_link` / `link_shape_warnings`
- `action`
- `promotion_status`
- `operator`
- `note`
- `previous_value`
- `created_at`

Promotion effects:

- `manually_promoted` may update `target_candidates.primary_email`.
- Detail API overlays latest promotion status on the original signal; source/evidence fields remain model-safe and do not expose raw HTML/PDF.
- The target-candidate overlay should show both extracted confidence and manual promotion status.

### Denormalized target candidate metadata

`target_candidates.metadata_json` can keep a compact overlay for page rendering:

```json
{
  "public_web_search": {
    "person_asset_id": "...",
    "latest_run_id": "...",
    "latest_person_run_id": "...",
    "status": "completed",
    "completed_at": "...",
    "source_family_counts": {
      "profile_web_presence": 3,
      "resume_and_documents": 1,
      "contact_signals": 2
    },
    "summary": "AI-generated model-safe summary",
    "primary_links": {
      "personal_homepage": "...",
      "resume_url": "...",
      "github_url": "...",
      "scholar_url": "..."
    },
    "email_candidates": [
      {
        "value": "name@example.com",
        "email_type": "academic",
        "confidence_label": "high",
        "publishable": true,
        "promotion_status": "promotion_recommended",
        "source_url": "https://university.edu/~name/",
        "reason": "Found on candidate-owned university profile and matched by AI identity check"
      }
    ],
    "manual_promotions": [
      {
        "promotion_record_id": "...",
        "email": "name@example.com",
        "email_type": "academic",
        "promoted_at": "...",
        "promoted_by": "operator"
      }
    ],
    "identity_review_flags": [
      {
        "signal_type": "x_url",
        "value": "https://x.com/example",
        "status": "ambiguous_identity",
        "reason": "Name matches but profile text does not mention the target company or known research area"
      }
    ]
  }
}
```

This overlay is a cache, not the source of truth.

## Artifact Layout

Use company/person scoped artifacts rather than workflow-stage artifacts.

Company-level:

```text
runtime/public_web/company/<company_key>/<run_id>/
  run_summary.json
  source_family_manifest.json
  official_surfaces/
    home.html
    surface_manifest.json
    blog_index.html
    research_index.html
  arxiv/
    affiliation_search.html
    paper_<id>_abs.html
    paper_<id>_full.html
  company_public_web_assets.json
  model_safe_summary.json
  export_manifest.json
```

Person-level reusable asset:

```text
runtime/public_web/person/<person_asset_id>/<run_id>/
  run_summary.json
  query_manifest.json
  search/
    001_<family>.json
    002_<family>.html
  documents/
    page_001.html
    page_001_analysis_input.json
    page_001_analysis.json
    resume_001.pdf
    resume_001_extracted.txt
  signals.json
  model_safe_summary.json
  export_manifest.json
```

Target-candidate scoped overlay:

```text
runtime/target_candidates/public_web_search/<record_id>/<run_id>/
  run_summary.json
  person_asset_ref.json
  target_candidate_overlay.json
```

For object storage, these should later become scoped assets with:

- `scope_kind = company`, `scope_key = company_key`, `asset_kind = public_web_publications`
- `scope_kind = person`, `scope_key = person_asset_id`, `asset_kind = public_web_profile`
- `scope_kind = target_candidate`, `scope_key = record_id`, `asset_kind = public_web_overlay`
- lifecycle: `draft -> completed/canonical` only after signals are persisted

## API Contract Draft

### Trigger company-level refresh

`POST /api/company-assets/public-web`

Request:

```json
{
  "target_company": "Anthropic",
  "source_families": [
    "company_official_publications",
    "company_arxiv_affiliation"
  ],
  "force_refresh": false
}
```

Response:

```json
{
  "status": "queued",
  "run_id": "...",
  "company_key": "anthropic"
}
```

### List company-level public web assets

`GET /api/company-assets/public-web?target_company=...`

Response should include latest company-level run status, source-family counts, publication/blog/arXiv asset counts, and artifact manifest pointers.

### Trigger batch search

`POST /api/target-candidates/public-web-search`

This endpoint creates one lightweight `batch_id` for the user's selected records and one per-candidate run for each selected `record_id`. The per-candidate run is the source of truth for status, retry, artifact pointers, and final result. The batch status is only an aggregate progress view for the current UI action.

Request:

```json
{
  "record_ids": ["..."],
  "source_families": [
    "profile_web_presence",
    "resume_and_documents",
    "technical_presence",
    "social_presence",
    "candidate_publication_presence",
    "scholar_profile_discovery",
    "contact_signals"
  ],
  "force_refresh": false,
  "max_queries_per_candidate": 10,
  "max_pages_per_query": 10,
  "ai_extraction": true,
  "extract_contact_signals": true
}
```

Response:

```json
{
  "status": "queued",
  "batch_id": "...",
  "run_count": 3,
  "runs": [
    {
      "run_id": "...",
      "record_id": "...",
      "person_asset_id": "...",
      "candidate_id": "...",
      "status": "queued"
    }
  ]
}
```

### List run status

`GET /api/target-candidates/public-web-search?record_id=...`

Response should include latest run and compact signal counts.

`GET /api/target-candidates/public-web-search?batch_id=...`

Response should include aggregate counts from `target_candidate_public_web_batches` plus the compact per-candidate run statuses. Batch-level status means one target-candidate page multi-select operation; it does not mean company-level public web search.

### Get detail

`GET /api/target-candidates/{record_id}/public-web-search`

Returns:

- latest run summary
- linked reusable person asset summary
- model-safe summary
- signals grouped by source family
- evidence links
- email candidates with publishability status
- artifact manifest paths

### Export

Existing `POST /api/target-candidates/export` should be extended with:

```json
{
  "record_ids": ["..."],
  "include_public_web": true,
  "include_public_web_model_safe_summaries": true,
  "include_raw_public_web_assets": false
}
```

Default should include model-safe summaries and link evidence, not raw HTML/PDF.

### Promote public-web email

`POST /api/target-candidates/{record_id}/public-web-email-promotions`

Request:

```json
{
  "signal_id": "...",
  "email_value": "name@example.com",
  "promotion_status": "manually_promoted",
  "reason": "Verified source and selected as outreach email"
}
```

Response:

```json
{
  "status": "promoted",
  "promotion_record_id": "...",
  "target_candidate": {
    "record_id": "...",
    "primary_email": "name@example.com"
  }
}
```

Promotion endpoint rules:

- only `email_candidate` signals can be promoted
- promotion must preserve the original source URL and email type
- promotion should update `target_candidates.primary_email` only after writing the promotion record
- rejected/suppressed promotions should remain visible in public-web detail but not appear as primary contact data

## AI-First Extraction

### Why AI is needed

Rule-based extraction is enough for URL classes, but weak for:

- obfuscated emails such as `first [at] domain dot com`
- deciding whether a found email belongs to the candidate or a coauthor/contact page
- distinguishing personal vs company/general inbox
- deciding whether a resume is current
- judging whether a homepage affiliation is current, historical, academic, or unrelated
- resolving same-name ambiguity

Email extraction is on by default in this feature, but the source policy is public-web evidence, not HarvestAPI email lookup. Expected email sources include:

- personal homepage contact sections
- CV/resume text
- Google Scholar verified domain/profile text where accessible
- paper PDFs and paper appendix/contact sections
- university lab/profile pages
- company profile/team/author pages
- conference/open-review profile pages
- other public pages found by candidate-name/company-name search

### Proposed AI contract

Add a structured `public_web_page_analysis` model contract that receives:

- candidate name
- target company
- candidate known context
- source family
- source URL/domain/title
- visible text excerpt
- extracted deterministic links
- document type
- deterministic email regex candidates
- source evidence snippets around each contact candidate

The model returns:

- `identity_match`
  - `match_label`: `same_person | likely_same_person | ambiguous | different_person`
  - `confidence_score`
  - `reason`
- `affiliation_assessment`
  - `current_target_company`
  - `past_target_company`
  - `academic_only`
  - `unrelated`
- `contact_signals`
  - candidate emails
  - academic emails
  - company emails
  - generic emails
  - obfuscated email parse
  - publishable recommendation
  - promotion recommendation
  - suppression reason
- `recommended_links`
  - homepage, resume, GitHub, X, Scholar, publications
- `evidence_summary`
  - short model-safe explanation
- `review_flags`
  - ambiguous identity
  - stale affiliation
  - likely generic inbox
  - unsupported email

### Email publishability rules

Email extraction should be conservative:

- publishable only when the model and deterministic evidence agree it likely belongs to the candidate.
- personal homepage, CV/resume, paper PDF, and university profile emails can be high-confidence when identity matching is strong.
- high-confidence emails should be shown as `promotion_recommended`, not silently written into `primary_email`.
- HarvestAPI-discovered low-confidence emails are out of scope for this path and must not be used to satisfy Public Web Search email extraction.
- suppress generic inboxes by default:
  - `info@`
  - `contact@`
  - `press@`
  - `support@`
  - `jobs@`
- suppress emails found only in unrelated page boilerplate.
- corporate emails should require stronger evidence than personal-domain academic emails.
- academic emails are useful but must carry source and current/stale context; they should not be treated as current company proof.
- Google Scholar verified email domain is an affiliation signal, not necessarily a full email address or current employment proof.
- every email candidate must retain source URL, evidence snippet reference, email type, confidence, promotion status, and suppression reason.

Do not merge these emails directly into `target_candidates.primary_email` unless a separate manual promotion step approves the candidate email. Store them first as `email_candidate` signals and record manual promotion separately.

### Social/profile identity confidence

Public Web Search should not treat every discovered social/profile link as confirmed.

- `confirmed`
  - strong identity match, e.g. same name plus target company, known homepage cross-link, matching GitHub homepage, or matching publication/profile context
- `likely`
  - enough supporting context for display, but not enough to overwrite existing canonical profile fields
- `ambiguous_identity`
  - same/similar name but weak or conflicting evidence
- `different_person`
  - evidence indicates it is not the target candidate
- `needs_review`
  - AI cannot decide with sufficient confidence

`ambiguous_identity`, `different_person`, and `needs_review` links should be stored for audit/review but not promoted into primary candidate links.

## Query Strategy

Initial source-family query templates after the first live batch review and the `2026-04-25` budget loosening pass:

The first search pass should spend provider budget on high-precision entry discovery, but the experiment default is no longer extremely tight. Because the selected target-candidate batches are small, the default is now `10` queries per candidate and `10` results per query. `resume/CV` and generic `email contact` queries remain later fallbacks because the live 10-candidate batch showed they mostly return contact directories, unrelated PDFs, and third-party pages. Resume and email evidence should primarily come from fetched homepages, CV links discovered on those pages, Scholar/publication pages, university pages, and paper PDFs.

### `profile_web_presence`

- `"{name}" "{target_company}"`
- `"{name}" "{target_company}" (homepage OR "personal website" OR "personal site") -site:rocketreach.co -site:zoominfo.com -site:apollo.io -site:signalhire.com -site:contactout.com -site:idcrawl.com -site:linkedin.com -site:twstalker.com -site:luma.com -site:clay.earth -site:theorg.com -site:facebook.com`
- `"{name}" (homepage OR "personal website" OR "personal site") ...noise-negative-sites`

### `scholar_profile_discovery`

- `"{name}" "{target_company}" site:scholar.google.com/citations`
- `"{name}" site:scholar.google.com/citations`

In v1 this means organic discovery and fetch when accessible. Production should not claim structured Scholar metrics unless a dedicated parser/connector is implemented and anti-bot behavior is handled.

### `technical_presence`

- `"{name}" "{target_company}" site:github.com`
- `"{name}" site:github.com`

### `social_presence`

- `"{name}" "{target_company}" (site:x.com OR site:twitter.com)`
- `"{name}" (site:x.com OR site:twitter.com)`
- `"{name}" site:substack.com`
- `"{name}" "{target_company}" site:substack.com`

### `candidate_publication_presence`

- `"{name}" "{target_company}" (arxiv OR OpenReview OR publication)`
- `"{name}" arXiv`

### `resume_and_documents`

- `"{name}" CV filetype:pdf "{target_company}" ...noise-negative-sites`

This is intentionally later than homepage/Scholar/GitHub. Direct resume/CV search is high-noise and should not consume the first 4-query budget.

### `contact_signals`

- `"{name}" "{target_company}" academic email university ...noise-negative-sites`
- `"{name}" "{target_company}" email contact ...noise-negative-sites`

These queries must be governed by privacy and publishability rules; the product should show evidence and confidence, not silently expose low-confidence contacts.

With the default `--max-queries-per-candidate 10`, the current entry-discovery pass is:

1. general identity web: `"{name}" "{target_company}"`
2. company-constrained homepage/personal website with negative filters
3. name-only homepage/personal website fallback with negative filters
4. company-constrained Google Scholar citations profile discovery
5. company-constrained GitHub discovery
6. name-only GitHub fallback
7. name-only Google Scholar fallback
8. company-constrained X/Twitter discovery
9. name-only X/Twitter fallback
10. name-only Substack discovery

This pairing is needed because company-constrained GitHub, Scholar, X, and Substack queries can miss real profiles when the target page does not mention the employer, while name-only queries require identity adjudication from page context such as bio, education, LinkedIn context, homepage links, and known work history. In entry-link-only mode, X/Substack/GitHub links remain evidence candidates and should not be shown as confirmed profiles until AI or human identity review confirms them.

## Implementation Plan

### Phase -1: Close storage dual-track risk

Public Web Search should not become another SQLite/PG bridge.

Status as of `2026-04-26`: completed for the backend slice. The product-facing SQLite snapshot surface was retired before Public Web persistence work, and the new Public Web tables landed through the PG-authoritative control-plane path. `sqlite_snapshot` export/restore CLI, handoff SQLite options, import fallback, object-storage upload/download, profile-registry disk fallback, and test-env seeding disk fallback are no longer available. Remaining SQLite code must stay migration-only or ephemeral-shadow-only.

- Treat Postgres as the only authoritative control-plane store for new Public Web tables.
- Do not add Public Web tables as SQLite-first tables in `storage.py` and then mirror them later.
- Add new Public Web tables to the PG control-plane schema path first, including live-table registration, primary keys, indexes, and migration/sync behavior.
- Keep any SQLite compatibility strictly ephemeral test/dev scaffolding, matching the current PG-only contract: `postgres_only` live mode plus shared-memory SQLite shadow, never disk-backed live authority.
- Use storage-neutral helpers for shared identity utilities such as LinkedIn URL key normalization, instead of calling methods through the store facade from new product modules.
- New tests for Public Web persistence should run against PG-only behavior and assert that per-candidate runs, batch aggregates, person assets, signals, and email promotions do not depend on disk SQLite fallback.

### Phase 0: Lock the product contract

- Locked decisions:
  - email extraction is enabled by default for Public Web Search
  - HarvestAPI email lookup is not the source for this feature
  - high-confidence public-web emails are displayed as typed candidates and can be `promotion_recommended`
  - writing a public-web email into `primary_email` requires a manual promotion record
  - raw HTML/PDF are retained for audit/reuse but excluded from export by default
  - company-level public web assets and target-candidate public web enrichment are separate lanes
  - company-level public web refresh is API/CLI-only in v1
  - target-candidate enrichment should create/reuse person-level public-web assets
  - v1 target-candidate default source families are `profile_web_presence`, `resume_and_documents`, `technical_presence`, `social_presence`, `candidate_publication_presence`, `scholar_profile_discovery`, and `contact_signals`
  - automatic person-level reuse in v1 requires a matching normalized/sanity LinkedIn URL key when LinkedIn is available
  - raw LinkedIn URLs must be normalized before identity matching; raw strings remain evidence, not identity keys
  - run status uses per-candidate runs as the v1 source of truth, with a lightweight batch aggregate only for one selected-candidate action
  - Public Web persistence must be PG-authoritative from the first implementation pass; do not introduce a new SQLite-first bridge for these tables
- Remaining contract decisions:
  - what source-family freshness warnings should the UI expose for reused person-level assets
  - whether model-safe summaries are visible immediately or gated behind a detail view
  - whether `scholar_profile_connector` should parse structured Scholar metrics in a later phase
- Update `FRONTEND_API_CONTRACT.md` after API shape is accepted.

### Phase 0.5: Candidate-level experiment harness

Status as of `2026-04-25`: an experiment-only module exists at `src/sourcing_agent/public_web_search.py` with a CLI entrypoint:

```bash
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli run-target-candidate-public-web-experiment \
  --limit 10 \
  --external-provider-mode simulate \
  --no-fetch-content
```

This harness was used before adding PG Public Web tables and remains the method-calibration path before frontend rollout. It validates the method in three separable steps:

1. Entry-link discovery
   - plan source-family-aware queries per target candidate
   - call the existing `search_provider` chain
   - classify and rank entry links as personal homepage, resume/CV, GitHub, X/Twitter, Substack, Scholar, publication, academic profile, company page, LinkedIn, or other
   - write per-candidate `query_manifest.json`, raw search responses, and `entry_links.json`
2. Content fetch and analysis
   - optional via `--no-fetch-content`
   - reuse `document_extraction.analyze_remote_document(...)`
   - write raw HTML/PDF as internal analysis assets, plus model-safe extracted text/analysis/evidence-slice artifacts
   - Public Web now runs fetch as deterministic collection plus source-aware slicing; it does not call `analyze_page_asset` once per fetched document
   - source-aware slices keep high-value sections by source type:
     GitHub profile/contact block, Google Scholar profile/verified-domain/homepage/interests, X/Substack profile headers, PDF first-page/front-matter evidence, academic profile snippets, and broader but bounded personal homepage text
   - keep raw HTML/PDF out of export-by-default semantics
3. Signal extraction and AI adjudication
   - deterministic email extraction is on by default unless `--no-contact-extraction`
   - HarvestAPI email lookup is not used
   - email candidates carry type, source URL/domain, confidence, publishability, promotion status, and suppression reason
   - `--ai-extraction on|auto|off` lets configured Qwen/OpenAI-compatible model clients judge identity confidence, publishability, and promotion recommendation from the candidate context, entry links, email candidates, and aggregated `evidence_slices`
   - the intended Public Web product shape is one candidate-level adjudication call per candidate run when AI is needed, not one LLM call per fetched page
   - ambiguous social/profile ownership and weak email identity should remain reviewable signals, not silent `primary_email` updates

Current LLM adjudication implementation:

- Entry point: `public_web_search.run_public_web_candidate_adjudication(...)`
- Model call: `model_provider.QwenResponsesModelClient.analyze_public_web_candidate_signals(...)` when Qwen is enabled, otherwise `model_provider.OpenAICompatibleChatModelClient.analyze_public_web_candidate_signals(...)` when the OpenAI-compatible provider is enabled
- Prompt builder: `model_provider._build_public_web_signal_adjudication_prompt()`
- Transport: Qwen `/responses` for the Qwen client, or OpenAI-compatible `/chat/completions` with `temperature=0` for the OpenAI-compatible client; strict JSON is expected in both paths
- Auto mode behavior: `ai_extraction=auto` skips entry-link-only evidence. It runs when fetched documents or email candidates exist, and `--ai-extraction on` can force link-level review for debugging.
- Payload context: candidate record, normalized LinkedIn URL key, headline, current company, primary email, known education/work history, up to 20 email candidates, source-balanced `entry_links`/`search_evidence` from DataForSEO URL/title/snippet/query/rank/provider context, and a configurable fetched-document / source-aware `evidence_slices` budget (`max_ai_evidence_documents`, default 8; product API bounded to 1..20). Search evidence is intentionally present because X/Substack and other platform pages may not be fetchable.
- Fetch/analyze split: document fetch writes raw/internal assets and model-safe evidence slices; only the candidate-level Public Web adjudication prompt sees those slices. This keeps raw HTML/PDF out of prompts/exports and avoids per-document LLM spend.
- Fetch queue policy: after entry-link ranking, the first fetch window is source/type diversified. When enough fetch budget and evidence exist, the queue gives one slot each to personal homepage, Scholar, GitHub, X/Twitter, Substack, resume/CV, academic profile, and publication before spending duplicate slots on the same source type. Company pages and LinkedIn remain non-default fetch targets.
- Prompt policy: suppress generic inboxes, paper-title fake emails, Scholar verified-domain placeholders, unrelated coauthor emails, same-name collisions, and boilerplate contacts. Grouped paper emails such as `{barryz, lesli}@domain` must be judged per expanded address.
- Output safety policy: model output is sanitized against the deterministic payload before it is applied. The model may adjudicate only email candidates produced by deterministic extraction; it cannot introduce new email addresses from Scholar verified domains, coauthor domains, or inferred company domains. Link assessment signal types are normalized back to the Public Web entry-type registry and unknown URLs are dropped.
- Link policy: X/Twitter, Substack, GitHub, Scholar, homepage, publication, resume, academic profile, company page, and LinkedIn links should be identity-adjudicated; search-result-only social/profile links should not be marked confirmed from name match alone.
- Academic summary policy: when Scholar, publication, academic profile, resume/CV, GitHub research repo, or personal homepage evidence is fetched, candidate-level adjudication should return `academic_summary` with research directions, notable work, academic affiliations, publication signals, outreach angles, confidence, and evidence sources. This summary must only use slices judged to belong to the target candidate; same-name Scholar/coauthor profiles should be excluded or marked low confidence.

Default artifact root:

```text
runtime/public_web/experiments/<run_id>/
  run_manifest.json
  run_summary.json
  candidates/<ordinal>_<record_id>/
    query_manifest.json
    search/*.json
    entry_links.json
    documents/*
    signals.json
    candidate_summary.json
```

Operational examples:

```bash
# Use the current PG target_candidates scope, capped at 10.
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli run-target-candidate-public-web-experiment --limit 10

# Reproducible, no-network smoke of the planning/artifact path.
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli run-target-candidate-public-web-experiment \
  --sample \
  --external-provider-mode simulate \
  --no-fetch-content \
  --run-id sample-entry-discovery

# Live entry-link discovery first; fetch/analyze only after query quality is acceptable.
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli run-target-candidate-public-web-experiment \
  --limit 10 \
  --no-fetch-content \
  --max-queries-per-candidate 10 \
  --max-results-per-query 10

# Full experimental pass with fetch + deterministic contact extraction + AI adjudication when a live model is configured.
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli run-target-candidate-public-web-experiment \
  --limit 10 \
  --max-fetches-per-candidate 5 \
  --ai-extraction auto

# Larger pre-rollout quality pass: broader candidate/query/fetch/LLM evidence budget.
PYTHONPATH=src .venv-tests/bin/python -m sourcing_agent.cli run-target-candidate-public-web-experiment \
  --sample \
  --limit 15 \
  --external-provider-mode live \
  --max-queries-per-candidate 16 \
  --max-results-per-query 20 \
  --max-entry-links-per-candidate 80 \
  --max-fetches-per-candidate 12 \
  --max-ai-evidence-documents 16 \
  --ai-extraction on \
  --batch-ready-poll-interval-seconds 10 \
  --max-batch-ready-polls 24 \
  --run-id live-public-web-quality-15x16-fetch10-ai16

# Evaluate one or more experiment directories without printing every signal row to stdout.
PYTHONPATH=src .venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality \
  --experiment-dir runtime/public_web/experiments/live-public-web-quality-15x16-fetch10-ai16 \
  --output-dir runtime/public_web/quality/live-public-web-quality-15x16-fetch10-ai16 \
  --summary-only
```

Acceptance criteria for continuing method calibration before frontend rollout:

- For a 10-candidate experiment batch, inspect `run_summary.json` and at least three `signals.json` files.
- Entry-link discovery should produce useful non-LinkedIn links for a meaningful share of candidates before fetch is enabled.
- Query duplicates should stay low; equivalent query text is deduped in the planner.
- Content fetch should stay bounded by `max_fetches_per_candidate` and skip low-value/blocked surfaces such as LinkedIn, contact directories, social mirrors, search/utility pages, media assets, and company pages. Google Scholar and X/Twitter may be fetched only when the URL is classified as a real candidate profile rather than a search or utility page.
- Product service runs must persist enough checkpoints that a worker restart resumes the same DataForSEO tasks instead of resubmitting provider work.
- Extracted emails must include evidence and suppression metadata. Generic inboxes and weak identity matches must not become `promotion_recommended`.
- Raw HTML/PDF artifacts remain internal analysis inputs and are not treated as export-default assets.

Empirical notes from the first candidate-level pass:

- `simulate` mode validates CLI/artifact structure only. It returns no search results by design.
- `duckduckgo_html`/free HTML fallback is not reliable enough for this experiment in the current environment; the provider chain inserted `bing_html`, which returned mostly unrelated pages for rare candidate queries. Do not judge product ROI from that path.
- DataForSEO organic search produced usable entry links for a 10-candidate target-candidate sample:
  - 40 queries, 10 candidates, entry-link-only
  - 106 ranked links
  - source types included resume/CV, Google Scholar, GitHub, X, Substack, academic profiles, publication pages, company pages, and LinkedIn
  - one candidate had no useful fetchable non-LinkedIn links in the first 4 queries
  - run time was about 4 minutes in the current synchronous CLI runner
- The synchronous CLI runner is acceptable for method discovery but not for product serving. Phase 1/2 should use provider batch/queue support and persist per-candidate state incrementally.
- Initial classifier tuning was required:
  - company homepages/event pages should not be treated as personal homepages just because the snippet contains the candidate name
  - RocketReach/ZoomInfo/contact aggregators should be retained as evidence links but not fetched by default and not treated as high-confidence personal homepages
- Initial fetch/extraction tuning was required:
  - model output normalization must tolerate non-dict `recommended_links`
  - PDF extraction can create fake emails from paper titles such as `Learning@Scale.Conference`; these must be suppressed before AI adjudication
  - a replayed Chelsea Finn full-chain run fetched the Stanford homepage/CV, extracted `cbfinn@cs.stanford.edu`, and marked it as a high-confidence academic email with `promotion_recommended`
- Current gap:
  - social/profile links and page-internal links still need first-class AI identity adjudication before they are shown as confirmed primary links
  - search quality should improve with source-family-specific negative filters and provider-specific policies

Live fetch calibration notes from `2026-04-26`:

- Run `live-dataforseo-fetch-scholar-academic-summary-3x7`
  - Scope: Michael Zhang, Wei Chen, Xiang Fu
  - 21 DataForSEO queries, 12 fetched documents, 4 email candidates, no search/fetch errors
  - Google Scholar fetch worked and extracted profile-level evidence such as verified domains, citation metrics, affiliations, and research interests
  - Correction after raw artifact inspection: Google Scholar raw HTML did include publication rows. The first parser missed them because it assumed `class` appeared before `href` on `gsc_a_at` anchors; the parser is now attribute-order independent.
  - Candidate-level AI produced `academic_summary` objects with research directions, notable work, and outreach angles
  - Calibration issue found: high-ranked same-name Scholar pages can pollute `academic_summary` if the prompt is not explicit enough about excluding non-matching identities
  - Calibration issue found: model link assessments returned non-canonical labels like `scholar_profile`, `github_profile`, and `personal_website`; these are now normalized back to Public Web entry types
- Direct Qwen adjudication check using saved Xiang Fu Scholar HTML after rebuilding the evidence slice from raw HTML:
  - Parsed 10 Scholar publications from the saved raw Google Scholar artifact
  - Qwen returned a useful high-confidence `academic_summary` covering materials-science ML, generative material design, force-field benchmarking, and outreach angles
  - Qwen returned 0 final email assessments when `email_candidates=[]`; this verifies the sanitizer blocks earlier hallucinated placeholder emails such as `email@periodic.com`, `email@csail.mit.edu`, and `email@meta.com`
  - Qwen confirmed the Scholar link as `scholar_url` after link-type normalization
- Run `live-dataforseo-fetch-diversified-xiang-1x7`
  - Confirmed fetch type diversification normalized entry type counts, but page-internal links from the personal homepage could still jump ahead of Scholar/GitHub and fetch a `.gif` asset
  - Follow-up fix: media/binary URLs are filtered from `personal_urls`; discovered personal-homepage links are prioritized only when they come from Scholar, discovered resume/CV links remain high-priority, and discovered GitHub links are appended instead of front-inserted so they cannot starve Scholar/X/Substack fetch diversity
- Run `live-dataforseo-fetch-diversified-xiang-1x7-v2`
  - Confirmed media filtering removed `.gif` fetches and canonical entry type counts stayed stable
  - New issue found: GitHub repository pages expose login/signup links that were extracted as GitHub URLs and consumed fetch slots
  - Follow-up fix: GitHub utility URLs such as `/login`, `/signup`, `/search`, `/topics`, `/features`, and `/pricing` are filtered out during page-signal extraction
  - Email calibration: repository contact emails belonging to coauthors/maintainers can be extracted; AI `needs_review` with low identity score now makes those email candidates non-publishable by default
- Follow-up fetch-queue fix:
  - The front of the fetch queue now does a one-per-source-type pass before allowing duplicate Scholar/homepage results
  - X/Twitter profile URLs are allowed into the fetch queue when classified as real profile URLs, but X/Twitter search/utility URLs still demote to `other`
  - This increases the chance that the candidate-level LLM receives at least one model-safe slice for each available media/source type, instead of over-sampling the highest-scoring type

Batch/queue optimization notes from `2026-04-25`:

- The experiment runner now builds search plans for all selected target candidates first, then uses provider batch/queue search when available.
- DataForSEO path now submits all candidate/query tasks through `submit_batch_queries(...)`, polls `poll_ready_batch(...)`, fetches ready tasks through `fetch_ready_batch(...)`, and records per-query errors/timeouts on the owning candidate instead of falling back to slow synchronous search.
- The CLI defaults to batch search; `--no-batch-search` is available only for debugging/fallback.
- Per-candidate status artifacts are written as `candidate_status.json` with `queued -> searching -> analyzing -> completed/completed_with_errors` phase transitions in the experiment harness. The product backend now persists the corresponding per-candidate run state in `target_candidate_public_web_runs`, including search checkpoints for recovery.
- `ai_extraction=auto` no longer calls a live model for entry-link-only evidence. Auto mode waits for higher-value fetched documents or email candidates; `--ai-extraction on` still forces link-level AI adjudication for debugging or focused review.
- Entry links with `identity_match_label=unreviewed` are no longer promoted into `primary_links`. Primary links require `confirmed` or `likely_same_person`; unreviewed links remain evidence candidates.
- Classifier fixes after the live batch:
  - low-value directory/contact/social mirror domains such as RocketReach, ZoomInfo, localized LinkedIn directory pages, LinkedIn posts, `idcrawl`, `luma`, `clay.earth`, `theorg`, Facebook, and `*.twstalker.com` are not treated as company pages or personal homepages
  - likely personal domains are checked before company text matches, so a real homepage that mentions the current company is not swallowed as a company page
  - personal homepage classification now requires stronger URL/title name evidence and does not rely only on snippets mentioning the candidate name
  - `company_page` now means company-owned domains, such as `periodic.com` or `academy.openai.com`; third-party articles or event pages that merely mention the target company are retained as `other` unless another more specific type applies
  - candidate-level `company_page` links are no longer fetched by default; they are low-ROI affiliation/company evidence, not a likely source of candidate emails
  - generic `.pdf` links are no longer automatically classified as `resume_url`; paper PDFs stay `publication_url` unless the URL/title/snippet has CV/resume indicators
  - X/Twitter search and utility URLs such as `/search`, `/intent`, `/share`, `/hashtag`, and `/i/...` are demoted to `other` instead of being counted as candidate `x_url` evidence
- Query planning fixes after the live batch:
  - the first 4-query budget no longer includes generic resume/CV or email-contact searches
  - the default experiment budget is now 10 queries x 10 results per candidate because selected target-candidate batches are limited and need better coverage
  - first-pass query order is now general identity, company-constrained homepage, name-only homepage, company-constrained Scholar, company-constrained GitHub
  - name-only GitHub/Scholar and X/Twitter/Substack discovery are included in the default 10-query pass
  - resume/CV and generic email/contact queries are lower-priority fallbacks because they produced mostly contact directories and unrelated PDFs in the live run
  - LLM adjudication payloads now preserve common top-level target-candidate context fields such as `experience_lines`, `education_lines`, `work_history`, and `education`, instead of relying only on `metadata.candidate`
- Exploration fixes after the live batch:
  - fetched homepage/GitHub/Scholar/publication pages now emit second-hop entry links from page-internal signals such as personal homepage, CV/PDF, GitHub, X, and LinkedIn URLs
  - Google Scholar profile pages now extract structured signals: homepage link, verified email domain, profile affiliation, and research interests
  - Google Scholar `Verified email at domain.com` is treated as an affiliation/domain signal, not as a real `email@domain.com` address
  - paper PDFs with multiple emails now conservatively suppress coauthor emails whose local part does not match the candidate name tokens, leaving them for AI review instead of marking them publishable

Live entry-discovery result after batch optimization:

- Run id: `live-dataforseo-batch-entry-discovery-10x4-ai-skip`
- Scope: 10 target candidates, 4 queries per candidate, 5 results per query, fetch disabled
- Provider: DataForSEO Google organic Standard Queue
- Search execution: 40 submitted tasks, 38 fetched task results, 0 task timeouts, 3 ready polls
- Wall time: about 88 seconds, down from about 4 minutes in the earlier synchronous CLI run
- One candidate completed with search errors because two queries returned DataForSEO `40102: No Search Results`; other candidates completed
- Current classifier recheck over the same search payloads produced 166 ranked links:
  - `company_page`: 8 after the company-domain-only fix; the original broader classifier had incorrectly counted 100+ third-party mention pages as company pages
  - `linkedin_url`: 18
  - `resume_url`: 9
  - `github_url`: 4
  - `x_url`: 5
  - `substack_url`: 3
  - `scholar_url`: 9
  - `personal_homepage`: 5
  - `academic_profile`: 6
  - `publication_url`: 8
  - `other`: 91 after low-value directories and third-party company mentions were demoted
- Manual title/URL/snippet review indicates the useful high-precision entry types in this sample are LinkedIn, X, GitHub, Scholar, and obvious personal homepages. Resume/publication categories remain high-recall but noisy and need fetch + AI identity adjudication before they should be shown as confirmed candidate evidence.

Live social/link coverage check after budget loosening:

- Run id: `live-dataforseo-entry-discovery-10x10-social-links`
- Scope: 10 target candidates, 10 queries per candidate, 10 results per query, fetch disabled
- Provider: DataForSEO Google organic Standard Queue
- Search execution: 100 submitted tasks, 10 candidates completed, 3 candidates had isolated `40102: No Search Results` query errors
- Wall time: about 213 seconds
- Reclassified with the current classifier after suppressing plain-PDF resume false positives and X search pages:
  - `github_url`: 93 links across 10/10 candidates
  - `scholar_url`: 109 links across 9/10 candidates
  - `personal_homepage`: 46 links across 9/10 candidates
  - `x_url`: 34 links across 6/10 candidates
  - `substack_url`: 9 links across 5/10 candidates
  - `publication_url`: 9 links across 3/10 candidates
  - `academic_profile`: 16 links across 7/10 candidates
  - `resume_url`: 9 links across 4/10 candidates
- Social coverage improved, but X/Substack links are entry candidates only. Some returned links are posts, mentions, or same-name publications rather than owned profiles. They should stay `unreviewed` until link-level AI or human identity adjudication runs.
- Scholar and GitHub coverage is high-recall but noisy for common names. Company-constrained and name-only query pairs are both needed, but primary profile promotion must use fetched page context, LinkedIn context, education/work history, and cross-links.

Replay-style fetch/extraction check:

- Run id: `replay-fetch-homepage-email-4-candidates`
- Scope: Chelsea Finn, Xiang Fu, Zijian Hu, Hai Lin using replayed entry links, no DataForSEO calls
- Result: 4 candidates, 8 replay searches, 11 entry links, 6 fetched documents, 2 extracted email candidates, 2 promotion recommendations
- Verified extracted emails:
  - `cbfinn@cs.stanford.edu` from Chelsea Finn's Stanford homepage/CV, academic, high confidence, `promotion_recommended`
  - `dainves1@gmail.com` from Xiang Fu's homepage `mailto:` evidence, personal, high confidence, `promotion_recommended`
- Fetched homepage coverage worked for Xiang Fu, Zijian Hu, and Hai Lin; Zijian/Hai did not expose emails in the fetched pages.
- These emails are still email candidates until a user promotes one through `POST /api/target-candidates/{record_id}/public-web-promotions`; only then can `primary_email` be updated.

Known optimization cases:

- Noah Yonack GitHub case: Google can return `https://github.com/noahyonack` for `noah yonack site:github.com`, and the profile has useful identity context such as `Data Scientist. Harvard '17.`. The unauthenticated fetched GitHub HTML used by the current harness did not expose `noah.yonack@gmail.com`, so this case should remain a tracked optimization for later GitHub API/profile-rendered extraction, link-level AI identity adjudication, or a browser/API-backed fetch path. Do not treat this as HarvestAPI email evidence.

### Phase 0.75: Quality evaluation gate

Status as of `2026-04-26`: first pass implemented. This remains the productization gate before export and email promotion.

CLI:

```bash
PYTHONPATH=src .venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality \
  --experiment-dir runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates \
  --output-dir runtime/public_web/quality/replay-fetch-homepage-email-4-candidates \
  --summary-only
```

Outputs:

- `public_web_quality_report.json`: full machine-readable report.
- `public_web_quality_signals.csv`: one row per email/link signal with source URL/domain/family, identity label, confidence, publishability, promotion status, evidence presence, severity, and issue codes.
- `public_web_quality_report.md`: human-readable summary with top issue codes and media link quality counts.

Quality dimensions:

- Email candidates must carry source URL, source family, evidence excerpt, email type, confidence, publishability, identity label, and suppression/promotion status.
- Promotion-recommended emails without trusted identity, source URL, evidence excerpt, publishability, or non-generic type are high-risk issues.
- X/Substack/GitHub/Google Scholar media/profile links are counted separately as trusted vs needs-review.
- GitHub repo/deep links, X search/post/utility URLs, Substack non-profile/non-publication URLs, non-profile Scholar URLs, and unreviewed/ambiguous media links are flagged before UI treats them as confirmed.
- Search-only links should generally remain `unreviewed`; fetch + model-safe evidence + AI adjudication is required before the detail UI can present them as confirmed/likely same person.

Empirical report checks:

- Replay fetch/email sample:
  - 4 candidates, 13 signals, 2 email candidates, 2 promotion-recommended emails, 0 issues.
  - `cbfinn@cs.stanford.edu` and `dainves1@gmail.com` both had source URLs and evidence-bearing homepage/CV context.
- Live 10x10 social discovery sample:
  - 10 candidates, 378 profile link signals, 0 trusted media links, 648 medium issues.
  - Dominant issue families were unreviewed profile/media identity, GitHub repo/deep links, X post/utility links, Substack non-profile links, and one non-profile Scholar result.
  - This validates the current guardrail: high-recall media discovery is useful as a candidate evidence pool, but search-only X/Substack/GitHub/Scholar results must not be shown as confirmed profiles.
- Live fetch + AI Scholar/academic summary sample:
  - 3 candidates, 94 signals, 4 email candidates, 21 trusted media links, 180 medium issues.
  - Trusted media appeared only after fetch + adjudication, while unresolved links still carried `needs_review`, `ambiguous_identity`, or `not_same_person`.
  - The report also surfaced legacy/model-specific link labels such as `scholar_profile`, `github_profile`, and `github_repository`; the quality evaluator now normalizes them to canonical Public Web entry types and flags `non_canonical_profile_link_type` for audit.
- Larger live quality pass:
  - Run id: `live-public-web-quality-11x14-fetch12-ai16`
  - Scope: 11 real local `target_candidates`
  - Budget: 14 queries/candidate, 20 results/query, 80 ranked links/candidate, 12 fetches/candidate, 16 AI evidence documents/candidate
  - Result: 154 queries, 880 entry links, 131 fetched docs, 12 email candidates, 2 promotion-recommended emails, 94 trusted media links
  - Quality report: 892 signals, 1670 medium issues, 0 high issues. Most remaining issues are expected search/discovery noise: unreviewed profile links, GitHub repo/deep links, X non-profile URLs, Substack non-profile URLs, non-profile Scholar URLs, and a small number of non-canonical link types.
  - Important finding: the actual fetched document composition was skewed to GitHub (`118/131` fetched docs), with only 10 homepage and 4 Scholar fetches and no X/Substack fetches. The root cause was discovered GitHub links being front-inserted during fetch, starving the initial diversified queue.
  - Follow-up fix: discovered GitHub links are now appended instead of front-inserted. This means `12` fetches/candidate should be retested before increasing the default to `16`; the first problem was queue fairness, not just budget size.
- Fetch diversity sanity after the fix:
  - Run id: `live-public-web-quality-diversity-fix-3x14-fetch12-ai16`
  - Scope: first 3 real local `target_candidates`
  - Result: 42 queries, 240 entry links, 33 fetched docs, 7 email candidates, 3 promotion-recommended emails
  - Fetched composition after GitHub append fix: homepage 5, resume 10, Scholar 4, GitHub 7, X 2, Substack 4, publication 3, academic profile 1.
  - Quality report: 247 signals, 7 email candidates, 8 trusted media links, 534 medium issues, 0 high issues. Trusted media included GitHub, Scholar, X, and Substack.
  - Conclusion: `12` fetches/candidate is a reasonable product-cap validation budget once queue fairness works. Raise to `16` only for exploratory stress tests or if detail UI validation shows repeated source-family gaps.
- Search-evidence LLM adjudication check:
  - Run id: `live-public-web-quality-search-evidence-3x14-fetch12-ai16`
  - Same 3-candidate budget after adding source-balanced `search_evidence` to the LLM payload.
  - Runtime: about 20 minutes. DataForSEO batch queue took about 5 minutes 15 seconds; the rest was sequential per-candidate fetch and candidate-level LLM analysis. The third candidate dominated tail latency because slow/blocked fetches happen sequentially inside the experiment CLI.
  - X/Substack identity distribution before `search_evidence`: X `confirmed=1, unreviewed=25`; Substack `unreviewed=44`.
  - X/Substack identity distribution after `search_evidence`: X `confirmed=1, likely_same_person=8, unreviewed=16`; Substack `likely_same_person=6, needs_review=1, ambiguous_identity=8, unreviewed=15`.
  - Quality report: 248 signals, 8 email candidates, 3 promotion-recommended emails, 39 trusted media links, 1 high issue. The high issue was a promotion-recommended email without trusted identity, so promotion UI must still gate on quality warnings.
  - Product implication: DataForSEO search evidence materially improves link adjudication coverage for hard-to-fetch platforms, but URL-shape warnings still matter. A post/deep link with `likely_same_person` should not be rendered as a clean profile link without showing the evidence and issue state.
- URL-shape hardening follow-up:
  - Re-evaluated `live-public-web-quality-search-evidence-3x14-fetch12-ai16` after making URL shape a shared contract.
  - Trusted media link count is now 9 under the publishable clean-profile definition; X/Substack deep links remain visible as evidence/review signals with `x_link_not_profile` or `substack_link_not_profile_or_publication`.
- Runtime-efficiency follow-up:
  - Candidate analysis now supports bounded concurrent URL fetches (`max_concurrent_fetches_per_candidate`, default 4).
  - Experiment CLI can finalize candidates concurrently (`max_concurrent_candidate_analyses`, default 2), so candidate-level LLM adjudication no longer has to run strictly one candidate at a time in live quality passes.
  - Remaining long tail is mostly external DataForSEO queue wait plus slow/blocked domains that exceed per-URL timeout; keep concurrency bounded to preserve provider cost/rate-limit behavior.

Next quality pass:

- Run 12-15 candidates with 10-16 queries/candidate, 8-12 fetches/candidate, and 12-16 AI evidence documents/candidate when live DataForSEO/LLM credentials and budget are available.
- Inspect the generated CSV/Markdown before implementing email promotion; promotion UX should only operate on signals that have durable source/evidence metadata.
- Keep raw HTML/PDF out of quality/export outputs unless a future explicit audit/debug export flag is accepted.

### Phase 1: Backend service skeleton

Status as of `2026-04-26`: target-candidate backend service, PG control-plane skeleton, first-class signal rows, and record detail API are implemented. The company lane and email promotion persistence remain future work.

Completed:

- `target_candidate_public_web.py` now wraps the experiment primitives into a product service boundary for selected target-candidate batches.
- Store methods exist for target-candidate Public Web batches, per-candidate runs, and reusable person-level public-web assets.
- Store methods exist for first-class `person_public_web_signals`, including replace-by-run materialization to avoid stale rerun signals.
- Idempotency keys exist at both batch and run level.
- LinkedIn-key based person asset reuse is implemented for completed runs with a normalized LinkedIn URL key.

Remaining:

- Add `company_public_web_assets.py` and company-level public web runs/assets when the lower-ROI company lane is ready.
- Add email promotion tables/writers before any Public Web email can update `target_candidates.primary_email`.
- Add dataclasses:
  - `PublicWebSearchRequest`
  - `PublicWebSearchRun`
  - `PublicWebSignal`
  - `PublicWebSearchResult`
  - `CompanyPublicWebAssetRun`
  - `PersonPublicWebAsset`
- Continue moving/refactoring any remaining query generation from `exploratory_enrichment.py` into the source-family registry.
- Add store methods for company asset and target overlay cache; public-web promotion records are implemented for target-candidate v1.
- Add tests around:
  - record_id selection
  - person identity key construction
  - LinkedIn raw URL normalization into a stable/sanity URL key
  - company lane vs target-candidate lane separation
  - force refresh vs reuse
  - source-family query expansion
  - email suppression defaults
  - email promotion audit trail
  - ambiguous social/profile link handling

### Phase 2: API and worker integration

Status as of `2026-04-26`: partially completed for target-candidate Public Web Search.

Completed:

- `POST /api/target-candidates/public-web-search`
- `GET /api/target-candidates/public-web-search`
- `GET /api/target-candidates/{record_id}/public-web-search`
- Recoverable worker creation through `agent_worker_runs` with lane `exploration_specialist`
- Recovery kind `target_candidate_public_web_search`
- DataForSEO-style submit/poll/fetch checkpoint resume in `search_checkpoint_json`
- Batch aggregate sync from per-candidate runs
- First-class `person_public_web_signals` materialization for detail/export-ready email/link evidence

Remaining endpoints:

- `POST /api/company-assets/public-web`
- `GET /api/company-assets/public-web`
- `POST /api/target-candidates/{record_id}/public-web-email-promotions`
- Add progress/detail payloads for company public web runs.

### Phase 3: AI extraction

Status as of `2026-04-26`: implemented in the experiment/analyzer layer and materialized into first-class PG signal rows for target-candidate runs.

- Model provider method for candidate-level Public Web adjudication exists.
- Deterministic fallback includes conservative email/link extraction and suppression.
- AI output is sanitized against deterministic evidence so the model cannot introduce unobserved emails or unknown URLs.
- AI output is stored in artifacts/run summaries and the model-safe portions are materialized into `person_public_web_signals`.
- Add regression tests for:
  - obfuscated email
  - generic inbox suppression
  - same-name mismatch
  - resume with unrelated email
  - high-confidence academic/personal email marked `promotion_recommended`
  - promoted email updates `primary_email` only after promotion record is persisted

### Phase 4: Frontend target candidate action

Status as of `2026-04-26`: target-candidate page action and Public Web detail section are implemented; export/promotion controls remain.

Completed:

- Checkbox selection in `TargetCandidatesPanel`.
- User-triggered `Public Web Search` batch button.
- Polling through `GET /api/target-candidates/public-web-search` while runs are non-terminal.
- Compact per-candidate status and latest summary display from per-candidate runs.
- Detail section backed by `GET /api/target-candidates/{record_id}/public-web-search`.
- Email candidates display type, identity label, publishability, suppression reason, evidence excerpt, source link, and promotion status.
- Profile/evidence links display identity label plus URL-shape warning chips such as `x_link_not_profile` and `substack_link_not_profile_or_publication`.
- Default export action remains separate and does not imply raw HTML/PDF inclusion.

Remaining:

- Add manual promote/reject controls for email candidates.
- Keep export action separate but aware of public-web results.

### Phase 5: Export and reuse

- Extend target-candidate export archive:
  - summary CSV columns for public web status and primary links
  - `public_web_signals.csv`
  - per-candidate `public_web_summary.json`
  - `public_web_manifest.json`
- Add reuse logic:
  - default reuse successful person-level assets when normalized/sanity LinkedIn URL keys match
  - low-confidence same-name reuse goes to `needs_review`, not silent merge
  - Google Scholar/homepage/email/name/company-only evidence can support review but cannot silently auto-reuse in v1
  - `force_refresh` creates a new run
  - older successful runs remain reusable but can carry source-family freshness warnings
  - failed runs remain visible for audit
- Keep raw HTML/PDF out of default export; include only if an explicit future debug/audit export flag is accepted.

### Phase 6: Optional first-class Scholar connector

- Add `scholar_profile_connector` only after product path is stable.
- Support:
  - author search by domain where permitted
  - profile URL parsing
  - verified email domain
  - affiliation text
  - research interests
  - citation/h-index metrics if accessible
- Do not use Google Scholar as current-employment truth without independent corroboration.

## Verification Plan

Targeted tests:

- `tests/test_public_web_search.py`
- `tests/test_public_web_quality.py`
- `tests/test_document_extraction.py`
- `tests/test_results_api.py`
- `tests/test_target_candidate_public_web.py`
- `tests/test_control_plane_live_postgres.py`
- `tests/test_worker_recovery_daemon.py`
- frontend target-candidate page component/API tests

Latest backend validation reported for the `2026-04-26` slice:

- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py`
- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_target_candidate_public_web.py`
- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_quality.py tests/test_public_web_search.py -k 'public_web_quality or ai_evidence_document_budget or sanitizer'`
- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'`
- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidates or public_web_api'`
- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_worker_recovery_daemon.py -k 'waiting_remote_search or recovery'`
- `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'build_sourcing_plan_omits_public_web_stage_by_default or single_stage_workflow_skips_public_web_stage'`
- `bash ./scripts/run_python_quality.sh typecheck`

Regression gates:

- single-stage workflow must not include `enrich_public_web_signals`.
- target-candidate public web action must not require rerunning the workflow.
- export should include public web summaries when requested.
- low-confidence or generic email must not populate `primary_email`.
- high-confidence public-web emails must not populate `primary_email` until a manual promotion record exists.
- manually promoted emails must retain email type, source URL, promotion timestamp, and operator metadata.
- ambiguous X/Substack/GitHub/social links must be marked as review-needed and not promoted as confirmed profile links.
- repeated runs should preserve old artifacts and create auditable new run records.
- automatic person-level reuse should compare normalized/sanity LinkedIn URL keys, never raw LinkedIn URL strings.
- batch status should aggregate per-candidate runs, while per-candidate runs remain the source of truth.

Manual/live checks:

- one candidate with a personal homepage and PDF CV
- one candidate with GitHub/X only
- one candidate with same-name ambiguity
- one candidate with no useful public-web evidence
- one candidate with an obfuscated email

## Open Decisions

- Should model-safe summaries be visible immediately, while raw source snippets require a detail view?
- What source-family freshness warnings should the UI expose for reused person-level assets?
- Should `scholar_profile_connector` parse structured Scholar metrics in a later phase, or should v1 stay limited to organic-link discovery and accessible page evidence?
