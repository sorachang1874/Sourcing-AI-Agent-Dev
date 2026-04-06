# Thinking Machines Lab Retrospective

## Goal

这次 live test 的目标不是一次性做“绝对全量”的 Thinking Machines Lab 成员库，而是验证一条可复用、可审计、可渐进扩展的 sourcing backend 主链：

1. current roster acquisition
2. prioritized current detail enrichment
3. former fallback
4. publication supplementation
5. manual review resolution

## Confirmed Execution Strategy

在正式执行前，用户明确确认了这条策略：

```text
company-employees Short
  -> current roster
  -> profile-scraper for prioritized members
  -> profile-search only as former / targeted fallback
```

后续又进一步确认了两条原则：

- former employee workflow 采用 `pastCompanies` recall first，不默认加 `excludeCurrentCompanies`
- publication/corner-case lead 默认先走低成本 web exploration；如果仍无法验证，就保留为 unresolved lead，而不是默认走高成本 Harvest name search

## What Worked

### 1. Current Roster

Harvest `linkedin-company-employees` 已证明可以作为 Thinking Machines Lab 的 current roster 主入口。

关键结果：

- snapshot: `runtime/company_assets/thinkingmachineslab/20260406T160749`
- summary: `harvest_company_employees/harvest_company_employees_summary.json`
- result:
  - `raw_entry_count = 25`
  - `visible_entry_count = 25`
  - `headless_entry_count = 0`
  - `stop_reason = completed`

这批结果质量足够高，当前样本里稳定包含：

- name
- headline
- location
- LinkedIn profile URL

### 2. Prioritized Current Detail

current roster 之后，系统对优先级最高的一批 current members 拉取了 full profile detail。

关键结果：

- snapshot: `runtime/company_assets/thinkingmachineslab/20260406T165131`
- `candidate_documents.json`
- result:
  - `candidate_count = 25`
  - `resolved_profile_count = 12`
  - `evidence_count = 38`

已成功写入高价值 detail 的成员包括：

- `Mira Murati`
- `Lilian Weng`
- `Soumith Chintala`
- `Andy Hwang`
- `Saurabh Garg`

### 3. Former Fallback

former fallback 的关键修正是：

- 不再默认使用 `excludeCurrentCompanies`
- `pastCompanies` 先做 recall
- web search 失败时，才使用 Harvest `profile-search` fallback

关键结果：

- snapshot: `runtime/company_assets/thinkingmachineslab/20260406T175245`
- `search_seed_discovery/summary.json`
- result:
  - web low-cost queries 全部因为 DuckDuckGo TLS 问题失败
  - fallback 到 `harvest_profile_search`
  - `entry_count = 25`
  - `query = "__past_company_only__"`

归一化后的 former candidates 已包含：

- `Alexis Dunn`
- `Andrew Tulloch`
- `Barret Zoph`
- `Joshua Gross`
- `Songlin Yang`

### 4. Publication Supplement

Thinking Machines Lab 的 publication supplementation 已不再只依赖 Anthropic 本地 publication 资产或通用 arXiv affiliation。

关键结果：

- snapshot: `runtime/company_assets/thinkingmachineslab/20260406T172439`
- `publications/official_surfaces`
- `publications/publication_leads.json`

官方 surface 抽出的 publication/blog leads：

- `Kevin Lu`
- `John Schulman`
- `Jeremy Bernstein`
- `Horace He`

## What Did Not Work Reliably

### 1. Targeted Harvest Name Search Is Too Expensive for Default Use

旧流程里，publication lead 默认会进入 targeted Harvest current/past exact-name search。

在 Thinking Machines Lab 这组 case 上，这条链的 live 结果并不好：

- `Jeremy Bernstein` current/past exact-name search: `0`
- `John Schulman` current/past exact-name search: `0`
- `Horace He` current/past exact-name search: `0`

这说明：

- actor 能调用
- 但 direct name + org search 不是可靠的 default corner-case resolver
- 成本又高，不适合作为默认路径

因此现在的系统策略已经改成：

```text
publication lead
  -> slug / low-cost exploration
  -> unresolved lead 保留
  -> targeted Harvest name search only if explicitly approved
```

### 2. DuckDuckGo HTML Search Is Not Reliable in the Current Environment

这次复盘确认了一个重要事实：DuckDuckGo 问题不是单纯的旧代码写法问题。

历史 snapshot 中的报错是：

`SSL: UNEXPECTED_EOF_WHILE_READING`

这次已经用新的 `requests + endpoint fallback` 对 `fetch_search_results_html(...)` 做了 live 复测，结果仍然在当前环境下失败。这意味着：

- 问题不只是 `urllib`
- 当前网络路径对 DuckDuckGo HTML endpoint 本身不稳定
- 不能把 DuckDuckGo HTML 搜索视为 production-grade 默认 search backend

因此当前结论是：

- low-cost web search 仍然保留，但只能作为 `best-effort`
- unresolved lead 必须保留
- 后续需要引入更稳定的 search provider abstraction，而不是把业务逻辑绑定到 DuckDuckGo HTML endpoint

## Manual Review Resolution Outcomes

manual review 现在已经是正式数据资产入口，而不是临时备注。

关键实现：

- 人工提供的链接会进入 `runtime/manual_review_assets/...`
- 会落盘：
  - `resolution_input.json`
  - source manifest
  - fetched html
  - analysis input / output
- candidate 与 evidence 会正式 upsert 到 SQLite

Thinking Machines Lab 这轮人工 review 的实际结果：

### Confirmed Current Employees

- `Kevin Lu`
  - confirmed current employee
  - LinkedIn 已写回：`https://www.linkedin.com/in/kzl/`
- `John Schulman`
  - confirmed current employee
  - 个人主页写回：`http://joschu.net/`
- `Jeremy Bernstein`
  - confirmed current employee
  - 个人主页和 CV 都已写回

### Kept as Unresolved Lead

- `Horace He`
  - 个人主页和 CV 已写回为 evidence
  - 但 homepage analysis 没有直接确认 Thinking Machines Lab affiliation
  - 因此当前保留为 unresolved `lead`

这正符合当前系统原则：

- 能确认就升级
- 不能确认就保留 unresolved lead
- 不因为缺 LinkedIn 或高成本 search 没命中就静默删除

## Harvest Lessons Learned

这轮 live test 里，Harvest 相关最重要的经验如下。

### 1. `company-employees` 适合 current roster

- 质量高
- 成本可控
- 适合作为小公司的 current baseline

### 2. `profile-scraper` 适合高价值 detail

- 已知 URL 时直接拉 full detail
- 更适合沉淀单人高价值资产
- 比“先 name search 再 profile”更稳

### 3. `profile-search` 不适合默认承担 corner-case exact-name resolution

- former recall 可以用
- 但 exact-name + org 作为默认路径，性价比低
- 更适合：
  - former fallback
  - 明确批准后的 targeted probe

### 4. 参数经验

- `company-employees`
  - mode 必须使用 actor 当前真实枚举
  - `maxTotalChargeUsd` 不能低于 actor 最小值
- `profile-search`
  - former 场景优先 `pastCompanies`
  - 不默认加 `excludeCurrentCompanies`
- `profile-scraper`
  - 已知 URL 时优先级最高
  - 默认不抓 email

## Current State

截至这轮复盘，Thinking Machines Lab 的可审计资产已经包含：

- current roster snapshot
- prioritized current profile detail
- former fallback roster
- publication leads
- manual review assets
- upserted candidate/evidence state

这意味着 Thinking Machines Lab 现在已经不是“跑过一次 demo”，而是具备了完整端到端复盘能力的 live test 基线。

补充一点：复盘中提到的稳定 search provider abstraction 现在已经落地为共享模块。

- `search_seed_discovery`
- `slug_resolution`
- `exploratory_enrichment`

都已切到同一套 `search_provider` 抽象；默认链路可按 `serper_google -> duckduckgo_html` 做 provider 级 fallback，并将 raw search payload 按 `html/json` 统一落盘。

## Next Recommendations

1. 引入并验证更稳定的 production search provider  
   当前抽象层已经就位，但仍需要在正式 live test 前把稳定 provider 配置和预算策略固定下来。

2. 保持 low-cost-first  
   publication/corner-case lead 默认先走：
   - official source
   - homepage / CV / social / web exploration
   - unresolved lead retention

3. 把 targeted Harvest name search 保持为 opt-in  
   不要再把它当默认路径。

4. 在下一家公司复用这套策略  
   Thinking Machines Lab 已经证明 current / former / publication / manual review 四条链能形成可审计闭环。
