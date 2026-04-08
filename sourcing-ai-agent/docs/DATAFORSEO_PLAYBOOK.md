# DataForSEO Google Organic Playbook

这份文档记录本项目当前对 `DataForSEO Google Organic SERP API` 的实际接入方式、默认成本口径和推荐使用场景。

## 适用场景

- 需要 Google organic first-page evidence，但不想继续依赖 browser Google。
- `google_browser` 容易被 CAPTCHA 阻断。
- `bing_html` 虽然免费，但相关性噪声较大，不适合做高置信 affiliation verification。

## 当前结论

- `live/regular` 已在本机真实验证通过。
  - 查询：`"Eric Wallace" "Thinking Machines Lab"`
  - 返回：`10` 条第一页 organic results
  - 质量：能稳定返回和 `Thinking Machines Lab` 相关的公开网页、论文、OpenReview 等结果
- `task_post -> tasks_ready -> task_get/regular/{task_id}` 已在本机真实验证通过。
  - 这条链更适合低成本后台批处理。

## 默认成本策略

- 同步、一条一条的人工 smoke test：
  - 可用 `live/regular`
- evidence-only prospects 的后台验证：
  - 优先用 `Standard Queue`
  - 不追求即时返回
  - 默认只取 `1 SERP / 10 organic results`
- 不把它作为“对全部 `scholar_coauthor_prospects` 自动全量开跑”的默认动作。
  - 先收集 prospects
  - 再由用户显式选择批次或名单
  - 默认运行时应保持 `scholar_coauthor_follow_up_limit = 0`

## 本地密钥位置

- `runtime/secrets/providers.local.json`

当前项目把 DataForSEO 配在 `search_provider` 下：

```json
{
  "search_provider": {
    "provider_order": [
      "dataforseo_google_organic",
      "serper_google",
      "google_browser",
      "bing_html",
      "duckduckgo_html"
    ],
    "dataforseo_login": "REDACTED",
    "dataforseo_password": "REDACTED",
    "dataforseo_base_url": "https://api.dataforseo.com",
    "dataforseo_default_location_name": "United States",
    "dataforseo_default_language_name": "English",
    "dataforseo_default_device": "desktop",
    "dataforseo_default_os": "windows",
    "dataforseo_default_depth": 10
  }
}
```

不要把这个文件提交到 Git。

## 已接入能力

### 1. 同步 search provider

- provider 名称：`dataforseo_google_organic`
- 当前接到 `src/sourcing_agent/search_provider.py`
- 用途：
  - 给同步 low-cost search lane 提供稳定的 Google organic fallback
- 当前实现走：
  - `POST /v3/serp/google/organic/live/regular`

### 2. 异步后台 helper

- helper 脚本：
  - `scripts/dataforseo_google_organic.py`
- 当前支持：
  - `live`
  - `task-post`
  - `tasks-ready`
  - `task-get`

### 3. Worker runtime queue lane

- `search_seed_discovery` 与 `exploratory_enrichment` 的 worker 模式下，当前默认会优先走：
  - `task_post`
  - `tasks_ready`
  - `task_get/regular`
- queue 未 ready 时，worker 会回到 `queued` 状态，而不是伪装成“0 results”或错误完成。
- intermediate API payload 也会落盘：
  - `*_task_post.json`
  - `*_tasks_ready.json`
- 真正完成后，最终 `task_get` payload 会随 `SearchResponse` 一起落盘。

## 推荐参数

- queue 类型：
  - `Standard Queue`
- 返回深度：
  - `depth=10`
- 只做 organic first page 初筛：
  - 不需要 rectangles
  - 不需要 AI Overview
  - 不加额外参数
- 默认地域与语言：
  - `location_name=United States`
  - `language_name=English`
  - `device=desktop`
  - `os=windows`

## 使用示例

### 1. 同步 smoke test

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/dataforseo_google_organic.py live \
  --keyword '"Eric Wallace" "Thinking Machines Lab"'
```

### 2. 提交一个 Standard Queue 任务

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/dataforseo_google_organic.py task-post \
  --keyword '"Eric Wallace" "Thinking Machines Lab"'
```

### 3. 查看 ready tasks

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/dataforseo_google_organic.py tasks-ready
```

### 4. 拉取单个任务结果

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/dataforseo_google_organic.py task-get \
  --task-id '<task_id>'
```

## 针对 evidence-only prospects 的执行规则

- 不要默认对全部 prospects 自动发 search。
- 推荐默认流程：
  1. 先生成并保留 `scholar_coauthor_prospects.json`
  2. 用户选择一个批次，或限定 top-N prospects
  3. 对这一小批人提交 DataForSEO Standard Queue
  4. 只读取第一页前 `10` 条 organic results 做 affiliation 初筛
  5. 只有出现明确目标组织信号时，才允许继续找 LinkedIn URL / profile fetch

## 后续建议

- 下一步更值得做的是把 queue task id、query template、signal summary 汇总成公司级 SERP backlog/index，方便用户按批次挑选 prospects 继续跑。
- 当前 helper + worker queue lane 已足够替代“浏览器 Google 被 block 后只能返回假 `0 results`”的问题。
