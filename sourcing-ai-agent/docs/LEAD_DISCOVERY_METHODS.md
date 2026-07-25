# Lead Discovery Methods

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


## 1. Publication Lead Public-Web Verification

用途：

- 处理 official blog / docs / arXiv / acknowledgement 中发现、但当前 roster 尚未命中的作者或被致谢人名。

触发条件：

- publication author / acknowledgement 被抽出
- name match 未命中现有 candidate
- name 看起来像真实人名

标准流程：

1. 先创建 `publication lead`，保留 publication provenance。
2. 再做低成本 public-web verification，而不是直接走 paid people search。
3. exploration query 至少覆盖：
   - `Name + Target Company`
   - `Name + Target Company + LinkedIn`
   - `Name + Publication Title`
   - `Name + Publication Title + Target Company`
4. 若 public web 已确认其有目标组织经历，且找到了 LinkedIn URL：
   - 直接走 profile detail / profile scraper by URL。
5. 若 public web 已确认其有目标组织经历，但还没有 LinkedIn URL：
   - 保留为 unresolved lead。
   - 不自动走 Harvest name search。
   - 需要逐人明确确认后，才允许 paid targeted search。
6. 若只找到了 LinkedIn URL，但 public web 仍未确认其属于目标组织：
   - 保留为 unresolved lead。
   - 不自动抓 profile，不自动走 Harvest name search。
   - 等待人工确认或第二独立证据源。
7. 若 public web 也没有确认 affiliation：
   - 保留为 `publication provenance only` lead。
   - 等待第二独立公开证据，或人工 review。

执行原则：

- `publication lead` 默认是 evidence-first，不是 roster-first。
- 单一 publication provenance 不足以自动确认成员身份。
- paid search 不是默认 resolver，只能作为逐人批准后的补充动作。

## 2. Roster-Anchored Scholar Coauthor Expansion

正式名称：

- `Roster-Anchored Scholar Coauthor Expansion`

用途：

- 从已确认 roster 成员出发，在 Google Scholar / Semantic Scholar / 论文平台中扩展 coauthor 网络，寻找尚未进入 roster 的潜在同组织成员。

严格定义：

1. 起点必须是已确认的 roster member，而不是模糊 lead。
2. 先遍历这些 confirmed members 的 publication graph。
3. 抽取每篇 publication 的完整 coauthor 列表。
4. 对 coauthor 做 affiliation verification：
   - 需要有目标组织的公开 affiliation 证据，或后续 profile-level 证据。
   - 不能仅因为与 roster member 合著，就直接升为成员。
5. 在 affiliation 未确认前，coauthor 只能作为 evidence 或候选线索存在。

默认安全规则：

- coauthor 本身不等于 membership。
- 模糊组织 alias 不得直接把不同组织视为同一组织。
- coauthor lead 不自动升 roster。
- 只有通过第二条独立证据链确认 affiliation，才允许进入正式 candidate pool。

默认执行口径：

- `Roster-Anchored Scholar Coauthor Expansion` 默认只负责产出 prospect asset，不负责把 prospects 全量送入 search。
- 代码层默认值也是关闭状态：
  - `scholar_coauthor_follow_up_limit = 0`
- 默认先保存：
  - `scholar_coauthor_prospects.json`
  - `scholar_coauthor_graph.json`
  - seed roster / seed publication 资产
- 后续 public-web verification 必须是显式触发的批量动作：
  - 由用户选定名单
  - 或限定 top-N prioritized prospects
- 不允许把全部 evidence-only prospects 直接作为默认 search backlog 自动开跑。
- 若走 Google organic verification，默认只取第一页前 `10` 条 organic results 做 affiliation 初筛。
- 批量低成本运行时，优先使用可恢复、可轮询的异步队列，而不是浏览器直搜。

当前实现状态：

- 当前已接入第一版 production backend：
  - 用 confirmed roster member 作为 author seed。
  - 通过 arXiv author search 拉取近期论文 author list。
  - 对已在 roster 中命中的 coauthor，写入 `scholar_coauthor` evidence。
  - 对未命中的 coauthor，只生成 prospect artifact，不直接升为 lead。
  - prospect 必须先经过 public-web affiliation verification。
  - 只有在 public web 已确认 affiliation 且已拿到 LinkedIn URL 时，才允许继续走 profile fetch。
- 当前仍未接入：
  - Google Scholar / Semantic Scholar 专用 connector
  - 仅凭 coauthor 关系自动升 lead
  - 未经第二证据链确认的自动成员化
