# Outreach Layering

> Status: Design/reference doc. Useful for product or architecture context, but not the source of truth for current runtime behavior.


这个模块用于在本地 JSON 资产（`runtime/company_assets/.../normalized_artifacts/materialized_candidate_documents.json`）上做分层筛选，支持“自然语言简称 -> 可审计层级结果”的分析流程。

## Layer 定义

- `Layer 0 (roster)`  
  组织 snapshot 中的全量成员。

- `Layer 1 (name_signal)`  
  弱信号层：主要由姓名线索支撑（例如中文字符或常见拼音姓氏），但缺少更强地区/语言信号。

- `Layer 2 (greater_china_region_experience)`  
  泛华人地区公开经历层：教育/工作/地点命中中国大陆、港澳台、新加坡等信号。

- `Layer 3 (mainland_china_experience_or_chinese_language)`  
  更强信号层：命中中国大陆经历，或命中中文/普通话/粤语等语言信号（含 Mandarin Chinese / Putonghua / Guoyu / Hokkien / Shanghainese 等常见变体）。

## 设计原则

- 只使用公开职业相关信号（教育、工作经历、地点、语言、公开简介）。
- 输出“outreach 适配层级”，不输出族裔、国籍等身份结论。
- 两阶段判定：
  - 第一阶段：规则引擎先给出 `deterministic_proposed_layer`
  - 第二阶段：AI 基于姓名、教育、工作、语言等综合信息输出 `final_layer`
- 每个候选会保留 deterministic 命中词与 AI 校验结果，便于复盘。

## AI Prompt 模板（可复用）

当前 OpenAI / Qwen 共用同一套系统指令模板（`outreach_layering_v3_explicit_greater_china_scope`）：

```text
You are assigning a candidate to outreach layer 0/1/2/3 using only explicit public profile signals.
Please comprehensively evaluate name, education history, work history, and language signals from LinkedIn profile detail text.
请综合候选人的姓名、教育经历、工作经历、语言能力等公开信息，综合判断该候选人属于哪一层。
Return strict JSON with keys final_layer,confidence_label,evidence_clues,rationale.
final_layer must be an integer 0..3.
Layer definitions:
0 = no sufficient outreach signal,
1 = weak name-only signal,
2 = broader Greater China region experience signal (Mainland China, Hong Kong, Macau, Taiwan, or Singapore),
3 = strongest signal with Mainland China experience and/or explicit Chinese language signal (Mandarin/Cantonese/Chinese language variants).
Important boundary: Layer 2 is broader than Mainland China and must NOT be interpreted as Mainland-only.
边界要求：Layer 2 是“广义 Greater China（中国大陆、香港、澳门、台湾、新加坡）经历”，不是狭义中国大陆经历。
confidence_label must be one of high, medium, low.
evidence_clues must be an array of concise strings.
Do not infer or output ethnicity, nationality, religion, or other protected-attribute conclusions.
```

AI 输入 payload 中会包含：

- `query`：用户原始语义上下文（例如“帮我找出Gemini所有的华人”）；不会改变本地资产读取范围，仅用于 AI 判层语境。
- `candidate`：候选人基础信息（姓名、职位、组织、LinkedIn URL 等）。
- `candidate_profile`：教育经历、工作经历、备注文本。
- `deterministic_signals`：规则引擎的命中证据（姓名、地区、语言）。
- `profile_text_excerpt`：从 LinkedIn 公开信息拼接的摘要文本。

## CLI

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli segment-company-outreach-layers \
  --company thinkingmachineslab \
  --snapshot-id 20260407T181912 \
  --asset-view canonical_merged \
  --query "帮我找出Gemini所有的华人" \
  --max-ai-verifications 80 \
  --ai-workers 8 \
  --ai-max-retries 2 \
  --ai-retry-backoff-seconds 0.8 \
  --provider openai \
  --summary-only
```

其中 `--provider` 支持：

- `openai`：默认值，优先使用 openai-compatible provider
- `auto`：按系统默认 provider 选择
- `qwen`：强制使用 qwen provider

输出落盘路径默认在：

- `runtime/company_assets/<company>/<snapshot_id>/layered_segmentation/greater_china_outreach_<timestamp>/analysis_summary.json`
- `runtime/company_assets/<company>/<snapshot_id>/layered_segmentation/greater_china_outreach_<timestamp>/layered_analysis.json`

其中 `analysis_summary.json` 会显式返回 `ai_prompt_template_version`，便于前端/后端按同一版本校验行为一致性。

补充：为兼容已有消费方，结果 JSON 里会同时保留旧字段别名：
- `layer_2_greater_china_experience` -> `layer_2_greater_china_region_experience`
- `layer_3_mainland_or_chinese_language` -> `layer_3_mainland_china_experience_or_chinese_language`

## 工作流默认行为

- 在 `start-workflow` / live acquisition 主链路中，完成 Profile Fetch 后会默认执行分层分析（不依赖 query 中是否包含“华人”等词）。
- Retrieval 阶段会读取分层结果并附加层级元数据；过滤策略默认按 Greater China 意图触发（`min_layer=2`，空结果时回退 `min_layer=1`）。
- 如需“所有 query 都强制按分层过滤”，可设置 `OUTREACH_LAYERING_FILTER_ALL_QUERIES=true`。
- 关键环境变量：
  - `OUTREACH_LAYERING_ENABLED`（默认 `true`）
  - `OUTREACH_LAYERING_ENABLE_AI`（默认 `true`）
  - `OUTREACH_LAYERING_MAX_AI_VERIFICATIONS`（默认 `0`，表示按 `Layer 1+` 全量动态决定；>0 表示设置上限）
  - `OUTREACH_LAYERING_AI_WORKERS`（默认 `6`）
  - `OUTREACH_LAYERING_FILTER_ENABLED`（默认 `true`）
  - `OUTREACH_LAYERING_FILTER_ALL_QUERIES`（默认 `false`）
  - `OUTREACH_LAYERING_DEFAULT_MIN_LAYER`（默认 `2`）
  - `OUTREACH_LAYERING_FALLBACK_MIN_LAYER`（默认 `1`）
