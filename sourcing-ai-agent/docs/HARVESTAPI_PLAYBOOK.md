# HarvestAPI Playbook

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档沉淀 `harvestapi` 在本项目中的实际使用方法、参数约束、真实踩坑和 Thinking Machines Lab 测试结论。目标不是复述官方 README，而是给后续开发提供一个“可以直接照着执行”的调用手册。

这里的 Harvest 集成用于候选人 roster/public-profile 补全和公开资料研究，前提是 provider 支持的 actor/API 与项目内的预算、恢复、审计约束。文档重点是产品化调用、队列治理、结果质量和成本控制。

## Actor 页面

- `linkedin-profile-scraper`
  - `https://console.apify.com/actors/LpVuK3Zozwuipa5bp/information/latest/readme`
- `linkedin-profile-search`
  - `https://console.apify.com/actors/M2FMdjRVeF1HPGFcc/input`
- `linkedin-company-employees`
  - `https://console.apify.com/actors/Vb6LZkh4EqRlR0Ka9/information/latest/readme`

## 当前定位

- `linkedin-company-employees`
  - 适合作为 `current roster` 主入口
  - 目标是先拿“当前成员池”
- `linkedin-profile-scraper`
  - 适合作为 `known URL -> full detail` 主入口
  - 目标是把候选人升级成完整 dossier
- `linkedin-profile-search`
  - 适合作为 `former recall / scoped company-specific recall` 主入口
  - `full_company_roster` 的 former lane 默认应走这里
  - `scoped_search_roster` 的 current/former company-scoped recall 也允许走这里
  - 不应该默认承担 `FullName + Organization` 的 exact-name resolution

## 配置位置

本地开发默认放在：

- `runtime/secrets/providers.local.json`

当前项目已经支持在这个文件里显式配置：

```json
{
  "harvest": {
    "profile_scraper": {
      "enabled": true,
      "actor_id": "LpVuK3Zozwuipa5bp",
      "api_token": "REDACTED",
      "default_mode": "full",
      "collect_email": false
    },
    "profile_search": {
      "enabled": true,
      "actor_id": "M2FMdjRVeF1HPGFcc",
      "api_token": "REDACTED",
      "default_mode": "short",
      "collect_email": false
    },
    "company_employees": {
      "enabled": true,
      "actor_id": "Vb6LZkh4EqRlR0Ka9",
      "api_token": "REDACTED",
      "default_mode": "short",
      "collect_email": false
    }
  }
}
```

默认建议把 `harvest.profile_scraper.collect_email` 保持为 `false`。
如果后台 run summary 里出现 `chargedEventCounts.profile_with_email > 0`，说明运行时配置或 provider 输入已经偏离了 “no email” 模式，需要先修正，再继续 live 调用。

生产环境更推荐 secret manager 或环境变量，不建议依赖旧项目里的 `api_accounts.json` 自动发现。

## 参数规范

### 1. `linkedin-profile-scraper`

当前代码入口：

- `src/sourcing_agent/harvest_connectors.py`

本项目当前正确 payload：

```json
{
  "urls": [
    "https://www.linkedin.com/in/kzl/",
    "https://www.linkedin.com/in/ACwAAA4HOMcBjHQNGyUbyfYCY-sOZshkNFC30Jk"
  ],
  "profileScraperMode": "Profile details + email search ($10 per 1k)"
}
```

关键结论：

- 不要再传错误的 `profileUrls`
- 当前经 live 验证可用的字段是：
  - `urls`
  - `publicIdentifiers`
  - `queries`
  - `profileIds`
- `profileScraperMode` 应使用完整枚举
  - 当前项目默认配置更偏向 `Profile details + email search ($10 per 1k)`
  - `Profile details no email ($4 per 1k)`
  - 如需低成本路径，可显式关闭 `collect_email`
- 适合已知 LinkedIn URL 的 batch enrichment
- `vanity URL` 与 `opaque LinkedIn URL` 当前都可用
- 当前 profile-scraper actor 受两层 limiter 保护：
  - 进程内 `runtime_inflight_slot("harvest_profile_scrape" / "harvest_profile_batch_submit")` 只限制当前 Python 进程
  - DB 级 `runtime_provider_limiter_leases` 使用 limiter key `harvest_profile_scraper_actor` 控制跨进程 active actor，默认 `harvest_profile_actor_global_inflight=4`，fast smoke 为 `2`
  - background `harvest_profile_batch` pending remote actor 的 limiter lease 会写入 worker checkpoint，直到 recovery terminal 后释放；不要在 submit 返回 pending 时提前释放
  - background prefetch 提交前必须先 claim `linkedin_profile_registry_leases`；URL 已 queued 或 claim contended 时只能补 lineage/event，不能重复 submit actor
  - 例外：如果 URL 已 queued 是当前 worker 自己写入的，并且 checkpoint 带有 remote `run_id/dataset_id`，recovery 必须继续 poll/download 这个 actor run，而不是返回 queued
  - background submit window 需要按来源区分：former/search-seed 的 medium `harvest_profile_search` 批次可以默认并行排出多个 profile-scraper actor；large company roster 仍按保守窗口推进，最终 active actor 由 DB limiter 兜底
  - 还需要继续满足一个编排要求：provider actor completed 后，应尽快触发本地 ingest 和下一轮 deferred-tail submit；不要把“等候选人详情物化完成”变成默认的下一批 submit 前置条件

### 2. `linkedin-profile-search`

本项目当前正确 former/company-scoped recall 思路：

```json
{
  "profileScraperMode": "Short",
  "maxItems": 50,
  "startPage": 1,
  "takePages": 2,
  "pastCompanies": [
    "https://www.linkedin.com/company/thinkingmachinesai/"
  ]
}
```

关键结论：

- former workflow 应以 `pastCompanies` 做 recall-first
- company-scoped current/former recall 应优先走 `currentCompanies / pastCompanies` 这类组织约束，而不是先退化成零散姓名检索
- 不要默认加 `excludeCurrentCompanies`
- Thinking Machines Lab 这个 case 上：
  - `pastCompanies` 两页能返回接近 LinkedIn 官网 former 搜索页的结果
  - `excludeCurrentCompanies` 会把结果压成 `0`
- `profile-search` 是 core company search lane 的一部分，不应再受通用成本开关影响
- `profile-search` 适合 former/company-scoped recall，不适合默认承担 exact-name resolution
- former search 不应再静态写死页数上限
  - 正确做法是先发一轮小 probe
  - 从 provider 返回里读取 `total_elements / total_pages`
  - 再按 provider 总量重发 full request
- 请求扩张时，timeout / `max_paid_items` / charge cap 也应同步放大
- `0 profiles` 是 provider/LinkedIn 已确认的偶发 transient issue：
  - 本项目只对真 0 结果做 bounded retry：`rows=[]` 且 `pagination.total_elements<=0`
  - 默认 retry 次数由 `HARVEST_PROFILE_SEARCH_ZERO_RESULT_RETRY_ATTEMPTS` 或 cost policy `provider_people_search_zero_result_retry_attempts` 控制，当前默认 `2`
  - `HARVEST_PROFILE_SEARCH_ZERO_RESULT_RETRY_BACKOFF_SECONDS` / `provider_people_search_zero_result_retry_backoff_seconds` 可设置 backoff
  - live 模式下，旧 raw/shared cache 若也是 0 结果，会被忽略并重新请求，避免一次 provider transient 变成长期缓存污染
- 不要把 provider total 漂移或缺页直接判定成强 incomplete：
  - 同一 Gemini/currentCompanies=Google input 曾出现 `0`、`1845`、`2338` 这类大幅 total 差异
  - 缺页/空 page range 比真 0 更常见，并且与 probe total 漂移叠加，不能准确证明“本地 workflow 已失败”
  - 当前做法是 page chunk / single-page fallback 后标记 `status=degraded` 与 `provider_search_degraded=true`，用于审计和后续补跑
  - 只有 zero-result retry 耗尽且没有可用 fallback rows 时，才标记 `status=incomplete` / `provider_search_incomplete=true`
- Anthropic 的美国 former case 已验证：
  - probe：`25 items / 1 page`
  - provider total：`562 items / 23 pages`
  - full request：`takePages=23`、`maxItems=562`
  - 实际返回：`556`

### 3. `linkedin-company-employees`

本项目当前正确 payload：

```json
{
  "profileScraperMode": "Short ($4 per 1k)",
  "companies": [
    "https://www.linkedin.com/company/thinkingmachinesai/"
  ],
  "takePages": 1,
  "maxItems": 25
}
```

关键结论：

- `profileScraperMode` 也应使用完整枚举
  - `Short ($4 per 1k)`
  - `Full ($8 per 1k)`
- 适合作为小中型公司的 current roster 主入口
- 默认 `20 pages x 25 = 500` 只是安全初始预算，不代表“全部成员”完成
- full roster path 必须先跑 1-page probe：
  - 如果 log 里出现 `Found N profiles total` 且 `N > requested maxItems`，正式请求应扩到 `min(N, 2500)`，例如 Mistral AI probe 发现 `1055 profiles total` 时，正式请求应为 `takePages=43`、`maxItems=1055`
  - 如果 `N > 2500` 或 provider log 显示 `limited to 2500 items`，结果必须标记 `provider_cap_hit / partial_result`，不能展示成普通 completed
  - summary / queue summary 必须保留 `requested_items_before_probe`、`effective_max_items`、`estimated_total_count`、`provider_cap_hit`、`requested_limit_would_truncate`
- Harvest actor log 中的 `Max items limit reached: N` 是请求 cap 被打满的审计信号；它和 provider 2500 ceiling 不是同一件事
- 当前 company-employees actor 使用 DB 级 limiter key `harvest_company_employees_actor`，预算走 `harvest_company_roster_global_inflight`；direct、segmented shard 和 recoverable worker 都必须走同一 limiter
- Thinking Machines Lab live test 中：
  - `25` 条 current roster
  - `25` visible
  - `0` headless

## 成本策略

默认策略：

- `company-employees Short -> current roster`
- `profile-scraper -> prioritized detail`
- `profile-search -> former / targeted fallback`

原因：

- `roster` 先解决 recall
- `detail` 再解决 precision
- 小公司可以视预算改成 `company-employees Full`
- 但默认仍建议分层，因为：
  - retry 更简单
- raw asset 更清晰
- 可以先积累人口池，再按优先级补 detail

## 大组织的 adaptive shard 策略

对于 Anthropic 这类 large org，当前不再把 shard 写死成固定的两片。

现在的推荐做法是：

1. 先对 root scope 发 `company-employees` probe
2. 从 Harvest actor log 里解析：
   - `Found X profiles total for input ...`
   - 是否触发 `limited to 2500 items`
3. 只有当 root scope 超过 provider cap 时，才继续按预设 partition rules 做下一层 probe
4. 最终只执行 live estimated count 已确认落到 cap 以内的 shard

当前 Anthropic 的默认 root scope 是：

- `locations=["United States"]`

当前默认 partition rules 是按 function 逐步切：

- `Engineering`
- `Research`
- `Product Management`
- `Operations`
- `Business Development`
- `Sales`

注意这里的关键差异：

- 旧逻辑：直接写死 `United States / Engineering` 和 `United States / Exclude Engineering`
- 新逻辑：先 probe 根范围，再决定是否真的需要切，以及切到哪一层为止

这意味着：

- 如果 root scope 本身已经低于 2500，就不会再人为切 shard
- 如果 `Engineering` 已经足够把范围压到 cap 内，就只切这一层
- 如果 root probe 或 branch probe 已经能证明需要继续拆，才会继续往下 probe

当前 live smoke 已验证：

- `Anthropic / United States`
  - `estimated_total_count=2837`
  - `provider_result_limited=true`
- `Anthropic / United States / Engineering`
  - `estimated_total_count=1098`
  - `provider_result_limited=false`
- `Anthropic / United States / Remaining after Engineering`
  - `estimated_total_count=1928`
  - `provider_result_limited=false`

这说明 Anthropic 的第一层 live shard 现在已经稳定收敛为：

- `United States / Engineering`
- `United States / Remaining after Engineering`

并且两片都在 provider cap 以内，不需要继续往 `Research / Product Management / Operations ...` 方向下钻。

补充说明：

- `runtime/company_assets/anthropic/20260409T080131/harvest_company_employees/adaptive_shard_plan.json`
  已经落下当前可执行 plan。
- `live_tests` 里的 Harvest probe summary 现在可被后续真实 snapshot 直接复用，不会因为 summary 缺少 `input_payload`
  就重复发远端 probe。

## 请求级 location / functionID 参数（request-scoped roster shard）

full-company roster（Harvest company-employees）lane 的两个一等请求参数，统一入口是
`company_shard_planning.build_request_scoped_company_employee_query_plan(target_locations=..., function_ids=..., max_pages=..., page_limit=...)`，
对所有公司同一份参数契约，不做任何公司名分支：

- **location**：请求级 `target_locations` 直接流入 lane 的 `locations` filter。
  - 字段缺省（`None`）→ 默认 `["United States"]`；
  - 非空列表 → 原样透传，允许多区域（如 `["United States", "Germany"]`）；
  - 显式空列表 `[]` → 完全不加 location filter（single-writer 规则：请求值永远优先，绝不与默认值合并）。
- **functionID**：请求显式选择 function（结构化 `must_have_primary_role_buckets`；user-explicit
  `cohort_selection.role_bucket_ids` 在请求归一化时会镜像进该字段）时，按 registry 里的 canonical
  映射（`query_signal_knowledge.ROLE_BUCKET_KNOWLEDGE`：research→`"24"`、engineering→`"8"`、
  product_management→`"19"`、founding→`"9"`）把 roster lane 切成**每个 function id 一条独立的
  company-employees 查询**。provider 单次调用最多返回 ~2500 条，按 function 分片是覆盖率的实现机制；
  不接受把多个 functionIds 合并进一条查询。未选择 function 时保持现状：一条不分片的查询（小公司
  TML 行为），只携带 location filter。

执行与审计语义：

- 每个 function shard 都有独立的 worker/queue 行与 receipts（`shards/<shard_id>/harvest_company_employees/`），
  `shard_id` 形如 `function_24`，`strategy_id=request_function_partition`。
- 下游 merge 沿用现有 segmented roster 语义做 union-dedupe（按 LinkedIn URL / member key），merged entry
  会带上 `source_shard_id` / `source_shard_filters`，并回填 entry 级 `function_ids` 归因。
- 汇总 manifest（`harvest_company_employees_summary.json` 的 `shard_summaries`、raw manifest 的 `shards`）
  记录每个 shard 的 `company_filters`、unique/duplicate 计数与 strategy id，可审计“哪个 function lane
  产出了哪些成员”。
- dispatch 保持 slot-free-fill：有空闲 worker slot 就立即派发，不引入 batch/tail 特例。
- `plan_review` 的 task metadata 同步会重建同一组 request-scoped shard，不会在 review 后丢失该接线。
- `large_org_keyword_probe_mode`（Google 类 keyword-union probe lane）保留自己的 keyword 分片契约，
  function ids 在该模式下仍作为合并后的 root filter 轴，不走 request-scoped function 分片。
- planner 产出的 request-scoped shard 会抢占 generic adaptive probe policy（`adaptive_us_technical_partition`）；
  delta rerun 的 `missing_company_employee_shards` 与 planner metadata shard 仍然优先于 request 推导。

相关测试：`tests/test_request_scoped_roster_shards.py`（纯 scripted/offline，无 provider 调用）。

## Profile enrich 的执行策略

对于已经拿到 LinkedIn URL 的 current / former roster，`profile-scraper` 不应再完全串行，也不应再固定按“每批约 100 条”机械切分。

这里真正要避免的是两种退化：

1. 上游太激进：同一时间向 provider 排出过多 actor，触发 queue/backpressure 问题。
2. 下游太串行：provider 明明已经完成一批，但系统仍等本地 poll/download/apply/materialize 全部结束后，才继续排下一批。

正确目标是有界并发的流水线：

- 按 budget/lease 提交若干 profile-scraper actor
- 任一 actor completed 后，立即消费 completed dataset
- 已完成结果进入下游 candidate-detail apply/materialize
- 与此同时，只要 budget/lease 允许，就继续提交下一批 deferred URLs

也就是说，`submit`、`ingest completed run`、`materialize` 是相邻阶段，但不应退化成单线程长链。

当前推荐做法是：

1. 先做 URL 级去重
2. 根据 URL 总量和来源混合（`company_roster / profile_search / targeted / other`）决定 batch 大小
3. live 模式使用更小、更均衡的 batch，并采用 sliding-window 提交
4. in-flight batch 维持有界并发：
   - roster-heavy 大集合最多 `3` 个并发 batch
   - profile-search / targeted 默认 `2` 个
   - non-live 最多 `4` 个
5. 只对失败或未返回 detail 的 unresolved subset 做定向 retry
6. 这套 window contract 不能只放在前台 enrichment：
   - background `company_asset_completion`
   - Excel intake 复用的 profile completion
   也必须共用同一套 live batching 规则，不能再退回“大批次 + 串行瀑布流”

这样做的原因：

- 可以避免 roster-heavy 大集合被切成少数超大 batch，导致单批失败成本过高
- 可以避免“上一批完全结束之后才发下一批”的低利用率串行模式
- retry 只打 unresolved subset，不会把已成功 URL 再跑一遍

Anthropic 当前 snapshot 的 former enrich 已验证：

- 目标 URL：`258`
- 批次数：`3`
- 并行 worker：`3`
- 成功 detail：`258`
- 转成 `non_member`：`2`
- 剩余错误：`2`

对应 summary：

- `runtime/company_assets/anthropic/20260409T080131/asset_completion/profile_enrichment_former_missing_detail.json`

## Thinking Machines Lab 真实经验

### 已验证可用

- `company-employees Short`
  - 适合作为 current roster 主入口
  - `2026-04-07` 新 Apify token 已重新完成 live smoke test，Thinking Machines Lab 返回 `STATUS 201`
- `profile-scraper`
  - 之前的 live batch 已成功返回 `12` 份 prioritized full profile detail
  - `2026-04-07` live smoke test 再次验证通过：
    - `Saurabh Garg`
    - `STATUS 201`
  - 结果写在：
    - `runtime/live_tests/harvest_profile_batch_tml/batch_summary.json`
- `profile-search + pastCompanies`
  - 能召回 former leads
  - 命中过 `Alexis Dunn`

### 已验证不该默认这么做

- `profile-search + excludeCurrentCompanies`
  - 在 TML former case 上会把结果压成 `0`
- `profile-search` 直接做 `FullName + Organization`
  - 成本高
  - 不适合作为 corner-case exact-name 主链

## 当前已知坑

### 1. 认证失败会伪装成“没有结果”

`2026-04-07` 早先使用旧 Harvest token 做最小化 smoke test 时，Apify 返回了 `401`：

- `user-or-token-not-found`

这说明：

- 之前 `fetched_profile_count = 0` 不一定只是参数问题
- 也可能是当前本机配置的 token 已失效或被轮换

当前已切换到新的可用 token，并重新验证通过。因此后续看到 `0 results` 时，必须先区分：

- payload 问题
- actor 过滤过严
- token/auth 问题
- actor 自己的可见性/覆盖问题

### 2. `profile-scraper` 不应再传 `urls`

这是这轮代码里已经修掉的关键问题。

### 3. opaque LinkedIn URL 不保证稳定拿回 detail

即使 profile-scraper actor 正常工作，某些 opaque LinkedIn URL 在某轮调用里也可能拿不到 detail。之前 TML current detail batch 成功过，但后续在另一轮小预算补全里没成功复现，所以：

- 不能把一次失败当成 schema 错误
- 要把 raw payload 和 run context 落盘

## 代码侧当前状态

相关文件：

- `src/sourcing_agent/harvest_connectors.py`
- `tests/test_harvest_connectors.py`

当前已固化的行为：

- `profile-scraper` 使用 `profileUrls`
- `profile-search` former recall 默认不加 `excludeCurrentCompanies`
- `company-employees` / `profile-scraper` 使用完整 `profileScraperMode` 枚举
- runtime 会优先复用：
  - snapshot cache
  - provider cache
  - live-test bridge

## 推荐执行顺序

### Current roster

1. `linkedin-company-employees` 拿 current roster
2. 把 raw payload 和 normalized roster 落盘
3. 给 prioritized current members 调 `linkedin-profile-scraper`

### Former fallback

1. `linkedin-profile-search` with `pastCompanies`
2. 不默认加 `excludeCurrentCompanies`
3. 命中 former lead 后，再尝试：
   - Google/网页 exploration
   - known URL profile detail

### Corner case exact-name

默认不要直接打 `linkedin-profile-search`。

更推荐：

1. Google / public web search
2. homepage / CV / GitHub / X exploration
3. 若拿到 LinkedIn URL，再调用 `profile-scraper`
4. 仍无法验证则保留为 unresolved lead

## 后续待办

- 在 Harvest connector 层补更清晰的 auth/error surfacing，避免把 `401/400` 吞成“空结果”
- 对 `profile-scraper` 再做一次 live re-validation：
  - 当前 known vanity URL
  - 当前 opaque LinkedIn URL
  - former opaque LinkedIn URL
- 补一个 `harvest smoke test` CLI，显式验证：
  - auth
  - actor payload
  - returned row count

## 关键词 shard 去重与剪枝（2026-04-10 更新）

为减少重复调用与成本，当前在 `profile-search` paid fallback 增加了两层防重：

- 查询签名去重：
  - `Vision-language` / `Vision Language` / `vision_language` 会视作同一 query。
  - 统一按去空格/去连字符后的签名去重。
- probe 驱动 overlap 剪枝：
  - 对每个 query 先做 probe，抽取返回的 profile URL 样本集。
  - 用 Jaccard overlap 评估与已保留 query 的重叠。
  - 超过阈值（默认 `0.9`）则直接标记 `skipped_high_overlap`，不再跑 full fetch。

结果：在 Google 这类大组织上，能明显减少“语义近似 query 重复抓取”。
