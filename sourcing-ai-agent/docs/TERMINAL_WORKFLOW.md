# Terminal Workflow

这份文档说明如何直接在终端里调用 `Sourcing AI Agent`，并尽量贴近产品原生流程：

- `plan`
- `review-plan`
- `start-workflow`
- `show-progress`
- `show-job`

## 1. 最小前提

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
```

如果要跑后台异步 worker，另开一个终端：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

## 2. 终端里的标准交互顺序

### Step 1: 先做 `plan`

`plan` 接收一个 workflow request JSON，其中 `raw_user_request` 就是自然语言请求。

示例：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli plan \
  --file configs/demo_workflow_humansand_coding_researchers.json
```

输出里会包含：

- `request`
- `plan`
- `plan_review_gate`
- `plan_review_session`
- `intent_rewrite`

`intent_rewrite` 的用途是把“系统对用户原话做了什么简称改写”显式暴露出来。

示例：

- 如果请求里没有命中 rewrite 规则：
  - `intent_rewrite.request.matched=false`
- 如果请求里写了 `华人成员` / `泛华人` / `Chinese members`：
  - `intent_rewrite.request.matched=true`
  - `intent_rewrite.request.summary` 会给出一条前端可直接显示的短句
  - `intent_rewrite.request.rewrite` 会给出结构化关键词与 targeting terms

如果要看 Thinking Machines Lab 全量刷新请求，可以直接用：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli plan \
  --file configs/demo_workflow_thinking_machines_lab_full_roster.json
```

### Step 2: 查看待 review 的 plan

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli show-plan-reviews --target-company "Humans&" --brief
PYTHONPATH=src python3 -m sourcing_agent.cli show-plan-reviews --target-company "Thinking Machines Lab" --brief
```

确认对应的 `review_id`。

### Step 3: 用 `review-plan` 修改并批准 plan

有两种方式。

#### 方式 A: 直接给 JSON

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --file configs/plan_review_approve.example.json
```

#### 方式 B: 直接给自然语言 instruction

先 preview：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 12 \
  --reviewer sora \
  --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。" \
  --preview
```

`--preview` 现在会输出两部分：

- `review_payload`
- `instruction_compiler`
- `intent_rewrite`

其中 `instruction_compiler` 会告诉你：

- 这次是否命中了模型
- provider 是谁
- 是否触发了 deterministic fallback
- 哪些字段是 fallback 补上的

而 `intent_rewrite` 会告诉你：

- `request`
  - 原始用户 query 有没有被 rewrite
- `instruction`
  - 当前 operator instruction 有没有命中简称改写

典型场景：

- 用户 query 是 `帮我找 Anthropic 的华人成员`
  - `intent_rewrite.request.matched=true`
- 但 review instruction 只是 `scope 大一些，要整家公司全量成员`
  - `intent_rewrite.instruction.matched=false`

确认输出的结构化 payload 没问题后，再真正 apply：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 12 \
  --reviewer sora \
  --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。"
```

当前自然语言 helper 能稳定覆盖的 review 字段主要有：

- `acquisition_strategy_override`
- `use_company_employees_lane`
- `force_fresh_run`
- `allow_high_cost_sources`
- `precision_recall_bias`
- `extra_source_families`
- `confirmed_company_scope`

当前实现是：

- 模型优先把自然语言指令转成严格 JSON
- 后端再做 schema 校验和字段白名单约束
- 若模型没有给出可用字段，再回退到 deterministic parser
- 对 `整家公司 / 全量成员 / scope 放大 / 重新跑` 这类高层意图，后端会额外推断内部执行偏好
  - 例如自动补出 `use_company_employees_lane=true`
  - 用户不需要记住 `company-employees lane` 这种内部术语

它已经比纯关键词匹配更接近产品化形态，但还不等于完整的会话式 PM Agent。

## 3.5 大组织的低成本推荐路径

对于 Anthropic 这类 large org，不应把“重新跑 full live roster acquisition”当成默认重跑方式。

当前推荐做法是：

- 先把某一次完整 current roster 获取沉淀成 durable snapshot
- 后续大多数 rerun 复用这个 current roster
- 只增量补：
  - former search seed
  - 新增的 search / publication / exploration 方法
  - 新的 retrieval / ranking / filtering 逻辑

如果 `plan_review_gate.execution_mode_hints.incremental_rerun_recommended=true`，说明当前 full roster fresh run 会先走 adaptive company-employees probe，再决定 live shard，成本明显高于增量模式。

典型 operator 指令：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 12 \
  --reviewer sora \
  --instruction "沿用现有 roster，只补 former 增量，不要重抓 current roster。"
```

更口语化一点也可以：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 12 \
  --reviewer sora \
  --instruction "基于之前抓过的 roster 继续做增量，只补 former 和新的方法，不重新拉公司全量。"
```

只有在下面这些情况，才建议 fresh run：

- 当前 roster 资产明显过旧
- 之前抓取方式有系统性缺陷，需要重建 baseline
- 用户明确要求 live roster refresh
- 需要验证新的 shard 方案本身

### Step 4: 启动 workflow

现在 `start-workflow` 默认是非阻塞模式：

- 会立即返回 `job_id`
- 在本地 CLI 调试下，默认会自动启动一个 job-scoped recovery daemon
- 不再像旧版本那样把整条 workflow blocking 在当前终端

补充：

- 这是本地/单次调试的便捷路径
- 云端默认仍应使用 hosted `serve + run-worker-daemon-service`

直接按 `plan_review_id` 启动：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --plan-review-id 12
```

也可以继续沿用 JSON 文件：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow \
  --file configs/demo_workflow_humansand_coding_researchers.json
```

如果你确实想保留旧行为，再显式加：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow \
  --plan-review-id 12 \
  --blocking
```

### Step 5: 看执行进度

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli show-progress --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-workers --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-scheduler --job-id <job_id>
```

`show-progress` 是前端友好的阶段摘要，重点看：

- 顶层 `elapsed_seconds`
- `stage`
- `progress.milestones`
- `progress.timing`
- `progress.worker_summary`
- `progress.counters`
- `workflow_stage_summaries`

当前 `workflow_stage_summaries` 的稳定阶段顺序是：

- `linkedin_stage_1`
- `stage_1_preview`
- `public_web_stage_2`
- `stage_2_final`

当前 `show-progress` 仍主要负责“阶段状态”和“后台 worker 状态”，不重复塞入完整 `intent_rewrite`。

前端推荐做法：

- 在 `plan` 或 `review-plan` 阶段拿到 `intent_rewrite`
- workflow 运行时用 `show-progress` 刷新阶段状态
- 结果页或复盘页再用 `show-job` / `GET /api/jobs/{job_id}/results` 读取同一份 `intent_rewrite`

这样分工更清晰：

- `show-progress`
  - 看任务走到哪一步了
- `workflow_stage_summaries`
  - 看 LinkedIn Stage 1、Stage 1 Preview、Public Web Stage 2、Final Analysis 哪些已经完成
- `intent_rewrite`
  - 看系统把用户原话解释成了什么
- `show-job`
  - 看最终结果、证据和可回放语义解释

### Step 5.5: Excel intake 快速入口

如果当前不是自然语言 query，而是用户上传了一份联系人表，可以直接走：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli intake-excel --file <workbook.xlsx>
```

若结果里有 `manual_review_local` 或 `manual_review_search`，再继续：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli continue-excel-intake --file <review_decisions.json>
```

这条链路会：

- 先做 schema 识别
- 再做本地 exact / near match
- 最后决定直接复用、继续人工复核，还是触发新的 LinkedIn fetch/search

### Step 6: 看最终结果

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli show-job --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-trace --job-id <job_id>
```

`show-job` 当前会包含：

- `job`
- `results`
- `manual_review_items`
- `agent_runtime_session`
- `agent_workers`
- `intent_rewrite`
- `workflow_stage_summaries`

因此如果前端在 progress 页没缓存 `intent_rewrite`，结果页仍然可以从 `show-job` 补拿。
如果前端要做阶段漏斗或阶段完成提示，应优先消费 `workflow_stage_summaries`，而不是直接读取 snapshot 文件。

## 3.1 前端友好的 summary 字段建议

如果你要做一个很薄的前端，建议按下面方式消费：

- Plan 页
  - 读 `plan.intent_brief`
  - 读顶层 `intent_rewrite`
- Review 页
  - 读 `instruction_compiler`
  - 读顶层 `intent_rewrite`
- Progress 页
  - 读 `show-progress`
  - 同时保留上一步已经拿到的 `intent_rewrite`
- Result 页
  - 读 `show-job`
  - 若需要回放 request normalization，直接使用 `show-job.intent_rewrite`

## 4. 一组实际终端调用示例

下面这组示例尽量贴近你当前会手动操作的方式。

### 示例 A: 看 `plan` 里的 `intent_rewrite`

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli plan \
  --file configs/demo_workflow_humansand_coding_researchers.json
```

如果你想快速只看 `intent_rewrite`，可以把输出重定向到临时文件再读：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli plan \
  --file configs/demo_workflow_humansand_coding_researchers.json \
  > /tmp/saa_plan.json

python3 -c 'import json; d=json.load(open("/tmp/saa_plan.json")); print(json.dumps(d["intent_rewrite"], ensure_ascii=False, indent=2))'
```

### 示例 B: 对 shorthand query 看 rewrite 命中

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

cat > /tmp/anthropic_chinese_members.json <<'"'"'JSON'"'"'
{
  "raw_user_request": "帮我找 Anthropic 的华人成员",
  "target_company": "Anthropic",
  "planning_mode": "model_assisted"
}
JSON

PYTHONPATH=src python3 -m sourcing_agent.cli plan \
  --file /tmp/anthropic_chinese_members.json \
  > /tmp/anthropic_chinese_members_plan.json

python3 -c 'import json; d=json.load(open("/tmp/anthropic_chinese_members_plan.json")); print(json.dumps(d["intent_rewrite"], ensure_ascii=False, indent=2))'
```

预期会看到：

- `intent_rewrite.request.matched=true`
- `rewrite_id=greater_china_outreach`

### 示例 C: 看 `review-plan --preview` 里的 request / instruction rewrite

先列出 review：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli show-plan-reviews --target-company "Humans&" --brief
```

假设拿到 `review_id=43`，再 preview：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 43 \
  --reviewer sora \
  --instruction "scope大一些，要整家公司全量成员，重新跑，不沿用旧 roster 或历史 profile，不要开高成本来源。" \
  --preview
```

如果只想抽出 `intent_rewrite`：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 43 \
  --reviewer sora \
  --instruction "scope大一些，要整家公司全量成员，重新跑，不沿用旧 roster 或历史 profile，不要开高成本来源。" \
  --preview \
  > /tmp/saa_review_preview.json

python3 -c 'import json; d=json.load(open("/tmp/saa_review_preview.json")); print(json.dumps(d["intent_rewrite"], ensure_ascii=False, indent=2))'
```

### 示例 D: 启动 workflow 后分开看 progress 和 results

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --plan-review-id 43
```

拿到 `job_id` 后：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli show-progress --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-job --job-id <job_id>
```

这里的推荐理解是：

- `show-progress`
  - 看 `status / stage / blocked_task / worker_summary`
- `show-job`
  - 看 `matches / manual_review_items / intent_rewrite`

## 5. HTTP API 入口

如果你不想直接用 CLI，也可以先起 HTTP API：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli serve --port 8765
```

核心接口：

- `POST /api/plan`
- `POST /api/plan/review`
- `POST /api/plan/review/compile-instruction`
- `POST /api/workflows`
- `GET /api/jobs/{job_id}/progress`
- `GET /api/jobs/{job_id}/results`
- `GET /api/jobs/{job_id}/workers`

这已经足够挂一个很薄的本地前端。

## 6. Thinking Machines Lab 重新跑端到端

如果你想重跑 TML 的完整 workflow，建议按下面顺序：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli plan \
  --file configs/demo_workflow_thinking_machines_lab_full_roster.json
```

拿到 `review_id` 后：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id <review_id> \
  --reviewer sora \
  --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，保持 balanced。"
```

然后直接运行：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --plan-review-id <review_id>
```

如果你是通过 HTTP API 触发 workflow，生产默认路径仍然应是 hosted `serve + run-worker-daemon-service`。

如果你想单独跑一个全局 recovery daemon，仍然可以另开一个终端：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

但日常线上运行，不要把 job-scoped recovery 当成主要托管方式；它更适合本地单条调试。

## 7. 当前限制

- `plan` 仍然主要通过 JSON request 触发；虽然 `raw_user_request` 是自然语言，但 CLI 本身还不是对话式 shell。
- `review-plan` 的自然语言 helper 目前是 operator-oriented parser，不是完整 LLM PM。
- 复杂的多轮澄清仍更适合走前端或 API 层的会话 UI。
- `show-progress` 当前不重复返回完整 `intent_rewrite`；前端应从 `plan / review-plan / show-job` 这些静态语义响应里复用它。
