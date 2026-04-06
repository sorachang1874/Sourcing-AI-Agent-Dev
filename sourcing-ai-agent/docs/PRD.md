# Sourcing AI Agent PRD

## 1. 背景

`Anthropic华人专项` 和相关 Skill 已经把一个真实 sourcing 项目沉淀成了可复用的方法论，但当前仍存在三个问题：

1. 状态主要散落在 Excel、JSON、Markdown 和对话上下文中，难以形成产品闭环。
2. 工作流具有明显的 Agent 特征，却还没有抽象成统一的 job、tool、state、artifact。
3. 外部数据源很多，但没有统一的 source adapter 和结果归档层。

本项目的目标是把这些经验产品化为一个适合 Agent 使用的通用 Sourcing 后端，而不只服务于 Anthropic 单个项目。

## 2. 目标用户

- 内部研究/战略团队
- 需要按 criteria 定向找人的招聘或合作团队
- 后续会接入 Claude Code / 其他 Agent runtime 的调用方

## 3. 核心任务

用户输入一个自然语言目标和一组 sourcing criteria，例如：

```json
{
  "target_company": "Anthropic",
  "categories": ["employee"],
  "employment_statuses": ["current"],
  "keywords": ["基础设施", "GPU", "预训练", "推理"],
  "top_k": 5
}
```

系统需要先完成 planning，再进入 acquisition 和 retrieval：

- 解读用户意图
- 明确 sourcing criteria
- 设计 acquisition 方法
- 在执行前与用户确认 plan、source family、成本与停止条件
- 异步获取全量数据资产
- 最后检索和呈现结果

系统最终需要返回：

- 匹配候选人列表
- 每个人的匹配原因
- 引用到的证据链
- 本次 job 的审计记录和 artifact

对于新增 sourcing 方法，例如“公开访谈 / Podcast / YouTube surface”，系统还需要支持一条 source onboarding 流程：

- 先识别这是“新增 source family”请求，而不是普通 retrieval
- 先与用户确认：
  - 目标关系定义
  - 覆盖范围
  - 只做 surface capture 还是继续深挖
  - 成本容忍度和人工 review 阶段门
- 再进入规划、开发、测试
- 新链路上线前必须满足：
  - raw asset 默认落盘
  - 全程可审计
  - schema 和执行策略可扩展

## 4. MVP 范围

### In Scope

- 读取本地 Anthropic 调研资产
- 统一候选人 schema
- workflow planning
- acquisition stage 抽象
- company asset snapshot 落盘
- SQLite 持久化
- 同步 retrieval job + 异步 workflow job 编排
- CLI 和 HTTP API
- 结果 artifact 落盘

### Out of Scope

- 大规模实时联网批量扫描
- 真正的生产级向量索引
- 前端界面
- 权限系统

## 5. 成功标准

- 能在本地一条命令完成 bootstrap
- 能用一条 job 请求拿到稳定结果
- 输出结果可追溯到原始资产
- 外部 provider 能通过 adapter 方式接入，而不是重写核心流程

## 6. 路线图

### Phase 1

- 本地资产驱动 MVP
- 后端链路跑通

### Phase 2

- 接入 LinkedIn / The Org / Hunter / Scholar / Search
- 引入异步任务、人工 review 阶段门和 company asset snapshots
- 设计云端资产存储，把高价值 LinkedIn Profile 和最终 artifact 从本地 runtime 中抽离

### Phase 3

- 接入 Claude Code API
- 提供 Demo 前端和结果审核台
- 在正式服务器环境安装和托管 worker daemon systemd service
