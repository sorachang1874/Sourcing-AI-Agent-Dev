# Backend MVP

## 1. MVP 输入

请求体示例：

```json
{
  "query": "找 Anthropic 当前偏基础设施方向的华人技术成员",
  "target_company": "Anthropic",
  "categories": ["employee"],
  "employment_statuses": ["current"],
  "keywords": ["基础设施", "GPU", "预训练", "推理"],
  "top_k": 5
}
```

也支持更上游的 workflow 请求：

```json
{
  "raw_user_request": "帮我找 xAI 当前偏基础设施和训练系统的核心技术成员，先做全量资产获取再检索。",
  "target_company": "xAI"
}
```

## 2. MVP 输出

- `job_id`
- `status`
- `stage`
- `plan`
- `summary`
- `matches`
- `artifact_path`

每个 match 包含：

- 候选人基础字段
- 匹配分数
- 匹配到的字段和关键词
- 相关 evidence

## 3. 运行方式

### Bootstrap

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli bootstrap
```

### 生成 plan

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_xai.json
```

### 启动 async workflow

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_xai.json
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_anthropic.json
```

### 运行 demo job

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli run-job --file configs/demo_current_infra.json
```

### 启动 API

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli serve --port 8765
```

## 4. 当前限制

- 当前 scoring 是规则引擎，不是模型 reranker
- 当前 live acquisition 只完成了 LinkedIn `company/people` baseline connector
- xAI 这类公司的 snapshot 已可真实执行，但 profile detail / publication / co-author enrichment 还没补齐
- LinkedIn `company/people` 不返回 slug，导致 profile 级 enrichment 仍需第二类 connector
- 为避免额度失控，roster acquisition 有安全页数上限，因此当前“全量”是 baseline，不是理论覆盖上限
- 当前没有 reviewer UI

## 5. 下一阶段后端演进

1. LinkedIn profile detail / publication-author / co-author adapters
2. Source adapter 异步化
3. Job 状态机
4. 人工 review queue
5. LLM-driven criteria compiler
6. 云端资产存储与 provider secrets / config management
