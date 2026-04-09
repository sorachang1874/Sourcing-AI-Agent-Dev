# Next TODO

## Goal

这份清单记录当前工作 backlog。它不是默认 onboarding 文档；接手项目时应先读 `docs/INDEX.md` 和 `../PROGRESS.md`，再回到这里看待办。

## Highest Priority

### 1. Harden `start-workflow` runner-spawn and background recovery

- `start-workflow` 当前虽然已经默认 non-blocking，但 runner-spawn / recovery daemon 仍需要更稳定的 ownership 和重试语义。
- 目标是不依赖当前终端存活，也能让 job 从 `plan_review -> background acquisition -> results` 自己收口。
- 需要把 job-scoped runner / daemon / worker 状态边界继续收敛，减少“主 job 已启动，但后台实际没接上”的不透明状态。

### 2. Run a fresh end-to-end quality pass on top of the stabilized Anthropic/Humans& workflow

- Anthropic 当前已经有可复用的 durable snapshot `20260409T080131`，不应再把“重新抓大组织全量 roster”当成默认验证方式。
- 更合理的下一轮验证应该聚焦：
  - query interpretation
  - retrieval quality
  - ranking / filtering
  - manual review routing
  - result explanation
- `Humans&` 仍然适合作为中等规模真实组织继续做一次完整质量测试。
- Anthropic 更适合作为“大组织增量复用、former fallback、adaptive shard”的长期 regression fixture。

### 3. Productize asset governance and snapshot promotion

- 把 `draft / partial / canonical / superseded / archived` 状态落成可执行规则，而不是只停留在文档。
- 为 company asset / scoped asset / retrieval artifact 建立更稳定的 registry 和 default pointer。
- 让上传新资产、替换旧 canonical、保留历史版本的流程标准化。
- 继续扩充“可用于排序和分析的长期 roster 资产”，优先考虑：
  - roster 在 Google Scholar / publication surface 上的论文标题、方向、摘要
  - roster 在 X / Substack 上的公开内容资产
  - roster 的公司邮箱、个人主页邮箱、论文中的学术邮箱
  - 从个人主页和论文附录中抽取 email 时的 lineage / evidence 落盘规则

## Lower Priority

### 4. Continue service evolution work

- 把当前 hybrid 方向继续推进为：
  - cloud control plane
  - cloud asset plane
  - local acquisition-capable runner
- 先服务化 retrieval-only / asset-backed 请求，再考虑托管 acquisition。

### 5. Tighten planning_mode and model-assisted planning

- 当前 `heuristic / model_assisted / llm_brief / product_brief_model_assisted` 的职责边界还不够清晰，需要收敛。
- `model_assisted` 至少要稳定产出可验证的结构化 `intent_brief` / planning JSON，而不是只做 request normalize。
- company identity resolve 不应继续过度依赖 heuristic；需要为 model-assisted path 定义更稳的组织解析 contract。
- 需要补一轮针对 team / org / facet / role / keyword 组合 query 的 planning regression cases。
- 需要把 plan review 的自然语言 operator instruction 继续产品化，从当前 CLI helper 演进到真正的会话式 plan editing。

### 6. Batch the DataForSEO async SERP lane

- 当前 DataForSEO Standard Queue 实现仍是单 query -> 单 `task_post`，没有利用一个 POST 最多携带多 task 的能力。
- 需要把 exploration / search seed discovery 的 Google organic lane 改成 batch submit、batch-ready check、incremental task_get。
- 需要把 batch job 的 request payload、task ids、provider attempts、resume checkpoint 统一落盘，避免再靠 payload hash 反推。
- 前台 retrieval 不应被全量 exploration batch 阻塞；默认先交付 baseline 结果，batch SERP 作为后台 enrich lane。

### 7. Extend adaptive Harvest sharding beyond the first Anthropic policy

- 当前 Anthropic 已从静态 `US + Engineering / exclude Engineering` 升级为 live probe-driven adaptive sharding。
- 第一层 live probe 已验证：
  - `US root = 2837`，超过 provider cap
  - `US + Engineering = 1098`
  - `US + exclude Engineering = 1928`
  - 当前可执行 shard set 已收敛为 `Engineering` + `Remaining after Engineering`
- 下一步要继续补：
  - 当某个 branch 本身仍高于 cap 时的二级 splitter
  - location-based overflow policy
  - plan/review 阶段对 shard policy 的更细粒度可编辑能力
  - 不同大组织的 live validation 与耗时统计沉淀

### 8. Continue query guardrail and shorthand UX refinement

- 当前 `intent_rewrite` 已能把 `华人成员 / 泛华人` 解释成 `greater_china_outreach`。
- 下一步不再是“能不能 rewrite”，而是：
  - 让更多 query shorthand 有稳定、可解释、前后端一致的 rewrite contract
  - 让 plan/review/result 页面都能更自然地展示 rewrite 前后语义
  - 让 operator 更容易在 review 阶段调整 rewrite 后的 targeting 意图

## Resume Checklist

切换账号之后，先做：

1. 阅读 `docs/INDEX.md`
2. 阅读 `../PROGRESS.md`
3. 阅读最近的 canonical asset / validation 文档
4. 检查 provider 和 object storage 配置
5. 再回到这份 `docs/NEXT_TODO.md`

## Useful Commands

```bash
cd "sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli build-company-candidate-artifacts --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli complete-company-assets --company thinkingmachineslab --profile-detail-limit 12 --exploration-limit 2
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-snapshot-bundle --company anthropic --snapshot-id 20260409T080131
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company anthropic
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json
```
