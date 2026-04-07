# HarvestAPI Playbook

这份文档沉淀 `harvestapi` 在本项目中的实际使用方法、参数约束、真实踩坑和 Thinking Machines Lab 测试结论。目标不是复述官方 README，而是给后续开发提供一个“可以直接照着执行”的调用手册。

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
  - 适合作为 `former fallback / targeted fallback`
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
  "profileScraperMode": "Profile details no email ($4 per 1k)"
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
  - `Profile details no email ($4 per 1k)`
  - 如需邮箱，才切 email 模式
- 适合已知 LinkedIn URL 的 batch enrichment
- `vanity URL` 与 `opaque LinkedIn URL` 当前都可用

### 2. `linkedin-profile-search`

本项目当前正确 former recall 思路：

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
- 不要默认加 `excludeCurrentCompanies`
- Thinking Machines Lab 这个 case 上：
  - `pastCompanies` 两页能返回接近 LinkedIn 官网 former 搜索页的结果
  - `excludeCurrentCompanies` 会把结果压成 `0`
- `profile-search` 适合 former fallback，不适合默认承担 exact-name resolution

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
