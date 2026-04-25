# Service Evolution Strategy

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


## Short Answer

现在不应该在“纯云端 SaaS”和“纯本地 App”之间二选一。

当前最合适的方向是：

- 云端做 control plane + asset plane
- 本地做 acquisition-capable worker
- retrieval / filtering / semantic analysis 在云端和本地都能跑，但优先消费已经同步好的标准化资产

这是一条 hybrid 路线。

## Why This Is The Right Next Step

当前后端已经具备几个关键条件：

- acquisition 和 retrieval 已经明显解耦
- snapshot / artifact / bundle / worker recovery 已经存在
- object storage sync 已经可用
- retrieval 可以直接从标准化资产运行，而不必每次重新 acquire

这意味着现在开始考虑服务形态是合适的，不算太早。

## Recommended Target Shape

### 1. Cloud Control Plane

职责：

- 用户请求入口
- workflow / plan review
- job state
- asset registry
- manual review UI
- retrieval result viewer
- evaluation dashboard

它不必须直接持有所有 provider 凭据，也不必须直接跑所有 acquisition。

### 2. Local Worker Plane

职责：

- live acquisition
- live enrichment
- browser-based / provider-based scrape
- 持有用户自己的 API keys、账号和本地增强依赖

这层最适合：

- Harvest
- browser search
- 本地 Playwright
- 需要用户私有凭据或受风控影响的 connector

### 3. Cloud Asset Plane

职责：

- object storage
- asset registry
- snapshot promotion
- retrieval-ready artifact storage

一旦资产同步上来：

- 多层过滤
- semantic recall
- result diff
- 人工 review

都可以不依赖原始 acquisition 环节重新执行。

## Why Not Full Cloud Right Now

纯云端方案当前有几个明显问题：

- provider 凭据和账号管理复杂
- browser / anti-bot / local dependency 问题更难控
- 新 sourcing 方法试验会受限
- 调试和快速修 connector 的反馈回路变慢

更关键的是：

- acquisition 是当前最脆弱、最 provider-specific 的部分
- 这部分还在快速演进，不适合一开始就完全封进托管云服务

## Why Not Full Local Only

纯本地方案同样不理想：

- 工作流定义难以统一
- 数据资产容易碎片化
- 团队协作和审计困难
- manual review、result compare、asset discoverability 都会变差
- 很难形成稳定的产品入口

## Recommended Product Strategy

### Near Term

先做：

- Web frontend
- Cloud API
- Asset registry
- Local runner / desktop-style worker

用户体验可以是：

1. 用户在 Web 上提请求
2. 系统判断是否已有可复用资产
3. 若已有，直接做 retrieval / analysis
4. 若没有，派发 acquisition job 给本地 runner
5. 本地 runner 执行后把 snapshot/bundle 同步回云端
6. 云端继续做统一分析、交付、人工 review

### Medium Term

再加：

- cloud runner
- hosted retrieval worker
- tenant-scoped queue
- auth / team workspace

这时 retrieval-only 任务基本都能云端完成。

## Query Storage And Parallel Isolation

如果后续部署到云端，不同 query 和并行运行必须按下面方式隔离：

### 1. Request / Plan / Execution 分层持久化

每个用户请求至少拆成 4 个对象：

- `JobRequest`
- `plan_review_session`
- `job`
- `job events / worker runs`

这样才能回答：

- 这个 query 原始请求是什么
- AI 当时如何理解它
- 用户是否修改过 plan
- 最终执行的是哪个 approved plan

### 2. Asset 与 Query Result 分开存

- company / scoped asset 是可复用资产
- retrieval result 是一次 query 的交付物

同一个组织的并行 query 不应互相覆盖，因为它们共享的是 immutable asset，而不是共享可变结果文件。

### 3. Fresh Acquisition 只能写入新 snapshot

如果两个 query 同时要求刷新同一组织：

- 都必须写入新的 `snapshot_id`
- 不允许直接覆盖当前 canonical snapshot
- promotion 发生在验证通过之后

这样并行 fresh run 最多只会产生多个 draft/partial snapshot，不会把同一个资产目录写坏。

### 4. Worker 与 Provider Queue 以 Job 为边界

异步 worker、Harvest run、DataForSEO task、search batch 都必须记录：

- `job_id`
- `lane_id`
- provider run/task id
- checkpoint

恢复 daemon 只能基于这些边界 resume，不能跨 job 复用“还没被消费的远端任务状态”。

### 5. 未来应增加 Workspace / Tenant 作用域

当前本地开发默认是单 workspace。

云端化后建议至少增加：

- `workspace_id`
- `user_id`
- `asset visibility`
- `provider credential scope`

对象存储路径、job 表、asset registry 都应带上这个上层作用域。

### Long Term

最后才考虑：

- managed acquisition runner
- hosted provider vault
- tenant-level connector management

这一步应该在 acquisition 规则、provider 约束和合规边界稳定后再做。

## Workflow Orchestration Improvements

当前已有编排基础，但还可以进一步拆清楚：

### Pipeline A: Asset Build

负责：

- resolve company identity
- acquire roster
- enrich
- normalize
- publish snapshot

产物：

- company/scoped asset

### Pipeline B: Retrieval & Delivery

负责：

- load published asset
- structured filtering
- semantic recall
- confidence banding
- summarization
- manual review routing

产物：

- retrieval artifact

关键规则：

- Pipeline B 默认不触发 Pipeline A
- 只有资产不存在、过旧、或用户明确要求刷新时，才触发 acquisition

这样能明显降低：

- 成本
- 风控压力
- 结果不稳定性

## Does Cloud Deployment Hurt New Criteria / Methods

如果直接做“封闭式云端黑盒”，会 hurt。

但 hybrid 方案不会，只要方法层继续版本化：

- source families
- search bundles
- criteria patterns
- hard filters
- prompt templates
- evaluation cases

推荐做法：

- 实验性方法先在本地 runner / dev 环境验证
- 验证通过后提升为 versioned method pack
- 云端 control plane 只消费已发布的方法版本

这样既保留创新速度，又保证线上行为可控。

## Does The Product Depend Too Much On Codex

当前开发节奏确实强依赖 Codex，这对研发是优势，但不能让产品运行也依赖 Codex。

真正需要产品化的不是“Codex 本身”，而是 Codex 帮我们沉淀出来的规则：

- plan compiler
- structured filters
- asset governance
- recovery logic
- provider fallback
- validation suites
- manual review triggers

如果这些规则不显式落成代码、测试和文档，那么离开 Codex 后能力会下降。

如果这些规则已经产品化：

- 运行时 agent 能力主要来自系统设计
- Codex 只继续充当研发加速器，而不是线上依赖

## What To Build Next

建议的服务化顺序：

1. 先补 asset registry 和 snapshot promotion 机制
2. 再补 Web API 和 Web frontend
3. 引入 local runner registration / heartbeat / job pickup
4. 把 retrieval-only jobs 默认云端化
5. 建立 evaluation suite 和 golden company set
6. 再考虑 managed cloud acquisition

## Recommended Final Direction

推荐的最终形态不是：

- 全本地单机工具
- 也不是一开始就全托管 SaaS

而是：

- 一个有 Web control plane 的 hybrid sourcing system
- acquisition 可以本地跑，也可以未来迁移到托管 runner
- retrieval 和结果交付围绕标准化云端资产运行

这条路径最符合当前项目状态，也最不容易把方法创新和工程规范对立起来。
