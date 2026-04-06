# Anthropic 华人专项调研 — 项目进度看板

> **用途：** 每次执行前必读，每次执行后必更新。对话压缩/重启后，从这里恢复状态。
> **原则：** 记录"已完成+结果"，不记录计划。计划写在 TODO 节。
> **⚠️ 重要：** 进度同步必须先查数据库实际状态（`python3` 查询），不能只看 summary。

---

## 一、文件结构

```
Anthropic华人专项/
├── Anthropic华人专项调研_v6zz18.xlsx         ← 当前最新 Excel
├── PROGRESS.md                                ← 本文件
├── api_accounts.json                          ← LinkedIn API 账号配置
│
├── data/
│   ├── theorg/
│   │   ├── theorg_accounts.json               ← The Org API 账号 + 月度额度追踪
│   │   ├── orgchart_anthropic_2026-04-05.json ← Anthropic org-chart（空，已废弃）
│   │   └── orgchart_raw_2026-04-05.json       ← Anthropic org-chart（⚠️ 数据不完整，仅81节点，完整数据需2026-05-01重新调用）
│   ├── hunter/
│   │   ├── hunter_accounts.json               ← Hunter.io 账号 + 月度用量追踪
│   │   ├── domain_search_anthropic_2026-04-05.json ← Free tier 10条原始数据
│   │   └── call_hunter.py                     ← 调用脚本（原子落盘，含额度守卫）
│   ├── chinese_filter/                        ← 四层分类数据库
│   │   ├── all_members.json                   ← 全体已知成员（100人）
│   │   ├── suspected.json                     ← Layer 1: 疑似华人（1人）
│   │   ├── confirmed_cjk.json                 ← Layer 2: 确认泛华人（49人）
│   │   ├── confirmed_mainland.json            ← Layer 3: 确认大陆华人（48人）
│   │   └── excluded.json                      ← 排除（4人）
│   ├── profiles/
│   │   └── anthropic_employees/               ← LinkedIn Profile 存档（130+ JSON）
│   ├── arxiv_scan/
│   │   ├── arxiv_scan_db.json                 ← arXiv 扫描数据库（Schema V2，129篇）
│   │   ├── arxiv_scan_db_v1_backup.json       ← V1 备份（2026-04-05）
│   │   ├── SCHEMA_V2.md                       ← Schema 文档
│   │   ├── ack_summary.json                   ← ACK 扫描结果（12篇）
│   │   ├── html_raw/                          ← 下载的论文 HTML（12篇）
│   │   └── ack_extracted/                     ← 提取的 ACK 文本
│   ├── engineering_scan/
│   │   └── engineering_scan_db.json           ← 工程博客扫描（21篇，100%完成）
│   ├── claude_blog_scan/
│   │   └── claude_blog_scan_db.json           ← Claude博客扫描（98篇，100%完成）
│   ├── scholar_coauthor/
│   │   ├── SCHEMA.md                          ← 图谱数据库 Schema 文档
│   │   ├── coauthor_graph_db.json             ← 主数据库（52 coauthor 节点）
│   │   ├── seed_authors.json                  ← 种子节点（11人，全部 scan_status=done）
│   │   └── candidate_queue.json               ← 断点续传队列（seeds 已清空）
│   ├── publications_unified.json              ← ⭐ 统一视图（248 publications）
│   ├── scholar_scan_results.json              ← Google Scholar 扫描结果
│   └── anthropic_scholar_ids.json             ← Scholar ID 索引（11人）
```

---

## 二、Excel 当前状态

**当前版本：v6zz18**（2026-04-05）

| 表 | 人数 |
|----|------|
| 在职华人员工 | **101人**（最大行 Row 102） |
| 已离职华人员工 | **8人** |

**路径：** `/home/node/.openclaw/workspace/Anthropic华人专项/Anthropic华人专项调研_v6zz18.xlsx`

**v6zz8 → v6zz18 主要变更（详见版本历史）：**
- v6zz8-v6zz14：新增 Whitney Chen / Tony Liu / Thomas Liao / Xunjie Yu / Alvin Tsuei / Qian Liang / Ziqi Wang / Lifu Huang / Martina Long / Jiasen Yang / Ming Zhong（Row92-102）
- v6zz15-v6zz18：数据质量修正（中文名来源核实、教育格式统一、LinkedIn 数据补全）

---

## 三、版本历史（倒序）

| 版本 | 在职人数 | 变更说明 |
|------|---------|---------|
| **v6zz18** | **101人** | 全表教育格式统一（「学位在前」，97行）；Row101 Jiasen Yang 从 account_008 重抓补全 USTC 少年班本科 |
| v6zz17 | 101人 | 教育格式全表转换（过渡版，随即被 v6zz18 覆盖）|
| v6zz16 | 101人 | Row85-102 全面 LinkedIn 数据补全（5个profiles）；Zhao Meng/Shi Feng 无来源中文名删除；Row98-101 重大内容修正 |
| v6zz15 | 101人 | Ming Zhong 钟明→钟鸣；删除推测性复旦大学本科；Row98/101 教育格式修正 |
| v6zz14 | 101人 | Ming Zhong 钟鸣(Row102) 新增（Anthropic Research Fellow，UIUC CS PhD，Hunter.io Signals）|
| v6zz13 | 100人 | 格式修正：Row91-96 列顺序/格式修正；Hunter.io Discover 数据落盘 |
| **v6zz12** | **100人** | Jiasen Yang(Row101) — The Org 发现，USTC 统计学本科 2009-2013，LinkedIn /jiaseny/ |
| **v6zz11** | **99人** | Ziqi Wang 王子奇(Row98) / Lifu Huang(Row99) / Martina Long(Row100) 新增；Ruiqi Zhong 离职表 Row8 补全 |
| v6zz10 | 97人 | Alvin Tsuei(Row93) / Qian Liang(Row94) 写入（字段不完整）；确认 Mu(Kevin)Lin 已存在 |
| v6zz9 | 97人 | Tony CW Liu(Row95) / Thomas I. Liao(Row96) / Xunjie Yu(Row97) 新增 |
| v6zz8 | 91人 | Whitney Chen(Row92) 新增，LinkedIn 确认在职 MTS @ Anthropic |
| v6zz7 | 90人 | Saffron Huang(Row88) / Alwin Peng(Row89) / Shi Feng(Row90) / Rowan Wang(Row91) 新增 |
| v6zz6 | 89人 | — |
| v6zu | 83人 | Sam Jiang(Row83) + Bin Wu(Row84) 新增 |
| v6zt | 81人 | Theodora Chu(Row81) + Daniel Jiang(Row82) |
| v6zs | 79人 | 教育格式全表修正（78行） |
| v6zr | 79人 | Jeffrey Wu 完整更新；Miranda Zhang profile 存档 |
| v6zq | 79人 | Jeffrey Wu 新增(Row80) |
| v6zp | 78人 | 清除 13 行无来源中文名；Christine Ye 新增(Row79) |
| v6zo | 77人 | Danny Lin(Row77) + Ashley Wang(Row78) |
| v6zn | 75人 | Jerry Hong 新增(Row76) |
| v6zk | 74人 | Runjin/Chao/Michael Ran/Qile Paul Chen + Judy Shen 新增 |
| v6zi | 67人 | Yanda Chen 新增(Row69) |

---

## 四、数据库状态

### 4.1 arxiv_scan_db.json（Schema V2，2026-04-05 迁移）

| 指标 | 数值 |
|------|------|
| 总论文 | 129 篇 |
| Anthropic 自有（is_anthropic_paper=true） | **52 篇** |
| 外部/排除 | 77 篇 |
| authors_list 已补全（Anthropic 自有） | **52/52 = 100%** |
| ACK 字段存在（Anthropic 自有） | **52/52**（`scanned=True`）|
| 其中有实质人名致谢文本 | **23/52**（非 Simons Foundation 模板）|
| 华人名字提取完成 | **9/52**（已处理：2401.05566/2412.14093/2505.05410/2503.10965/2501.18837/2503.04761/2507.21509/2411.07494/2601.04603）|
| **华人名字提取待完成** | **23 篇有实质 ACK 文本**，其中约 14 篇尚未提取（高优先级）|
| HTML 已下载 | 12 篇 |

### 4.2 publications_unified.json（2026-04-05 新建）

| 来源 | 数量 | authors_list 完整 |
|------|------|------------------|
| arXiv | 129 | 52（Anthropic 自有 100%）|
| Engineering Blog | 21 | 21（100%）|
| Claude Blog | 98 | 0（无个人署名）|
| **合计** | **248** | **74** |

### 4.3 chinese_filter/

| 文件 | 人数 |
|------|------|
| all_members.json | 100 |
| confirmed_cjk.json | 49 |
| confirmed_mainland.json | 48 |
| suspected.json | 1 |
| excluded.json | 4 |

### 4.4 scholar_coauthor/（2026-04-05 建立 + 完成）

| 指标 | 数值 |
|------|------|
| 种子节点 | **11人** |
| 已扫描种子 | **11/11 ✅ 全部完成** |
| 发现 coauthor 总节点 | **52人** |
| 华人 Layer 2+ | **28人** |
| 确认 Anthropic（含离职） | **7人**（Ruiqi Zhong 离职 Row8；Thomas I. Liao 在职 Row96；Lampinen/Perez/Bowman/Leike/Burns 非华裔）|
| 新写入 Excel 的华人 | **Ruiqi Zhong**（离职 Row8 补全）、**Thomas I. Liao**（在职 Row96 新增）|
| 最后更新 | 2026-04-05 |

**方法说明（见 Skill §27）：**
- 本次执行的是「方法 B：Scholar Coauthor 图谱 — 一阶扩展」
- 即：以 11 位已知 Anthropic 华人员工为种子，提取每人 Scholar 主页侧边栏 Co-authors 列表（一跳关系）
- 尚未执行「方法 C：二阶扩展」（对一阶发现的 28 位华人 Layer 2+ 再扩展一跳），当前暂停，性价比低

### 4.5 The Org API（2026-04-05 首次调用）

| 指标 | 数值 |
|------|------|
| 配置文件 | `data/theorg/theorg_accounts.json` |
| API Key（账号 theorg_001） | `6b4ad86403c9413da382c27902f7b5d1` |
| Free tier 月限额 | **10 credits/月 = 最多 1 次 org-chart 调用** |
| 2026-04 状态 | **已耗尽**（2026-04-05 使用，下次可用：2026-05-01）|
| Anthropic 扫描总节点 | **81 个节点**（⚠️ 数据不完整：首次调用用 curl 直接输出到终端被截断，仅保留 81 节点；实际返回估计 ~450 节点；完整数据需 2026-05-01 重新调用，届时用 §29.3 Python 脚本原子落盘）|
| 华人候选初筛 | **63 人** |
| 新发现写入 Excel | **Jiasen Yang**（Row101，The Org 显示"Jiasen Y."，LinkedIn /jiaseny/ 确认）|
| 原始数据文件 | `data/theorg/orgchart_raw_2026-04-05.json` |

### 4.6 Hunter.io Domain Search（2026-04-05 首次调用）

| 指标 | 数值 |
|------|------|
| 账号 | `hunter_001`（Free tier）|
| API Key | `a688f5eebd426dc15dff2753b68ada189cf9a064` |
| 月度 search 限额 | 60 次/月，重置日期 2026-05-04 |
| 2026-04-05 已用 | **4 次**（剩 56 次）|
| **Free tier 核心限制** | **每次最多返回 10 条，offset > 0 报 400，无法翻页** |
| Anthropic.com 总记录 | **565 条** |
| Free tier 实际获取 | **10 条**（1.8% 覆盖率）|
| 华人候选 | **0 人**（前 10 条全为 C-suite/Director，无华人）|
| 全量获取方案 | 升级 Starter（$34/月），6 次翻页即可获取全部 565 条 |
| 存档文件 | `data/hunter/domain_search_anthropic_2026-04-05.json` |
| 调用脚本 | `data/hunter/call_hunter.py`（含原子落盘 + 额度守卫 + 幂等检查）|

---

## 五、扫描渠道状态

| 渠道 | 归属层级 | 状态 | 新发现人员 |
|------|---------|------|-----------|
| LinkedIn company/people API | Layer 1A | ✅ 主体完成（余42次）| 约78人 |
| **The Org Org Chart API** | Layer 1A | ✅ **2026-04 已完成**（本月额度耗尽，下次 2026-05-01）| Jiasen Yang(Row101) |
| **Hunter.io Domain Search** | Layer 1A | ⚡ **Free tier 已调用**（只能获取前10条，无翻页）；全量需升级 Starter($34/月) | 0（前10条无华人，均为C-suite）|
| LinkedIn Search API（/api/search/people） | Layer 1A | ❌ **currentCompanies/passCompanies 参数实测无效**（keywords 关键词过滤生效，公司过滤不生效）| — |
| **Google Scholar Bio 批量扫描** | Layer 1B | ⚡ 部分执行（方法A，见 Skill §27.2）| Kaidi Cao / Zhao Meng / Yanda Chen / Jiaxin Wen / Rowan Wang |
| **X (Twitter) Bio 批量搜索** | Layer 1B | ⬜ 未执行 | — |
| **GitHub Bio 批量扫描** | Layer 1B | ⬜ 未执行 | — |
| arXiv 论文署名扫描 | Layer 1C | ✅ **129/129完成** | Runjin/Chao/Michael/Qile/Judy/Jerry Hong 等 |
| arXiv ACK 致谢扫描 | Layer 2 | ⚡ **52/52 ack字段存在**；其中23篇有实质人名致谢文本，**华人名字提取尚未完成**（高优先级）| Rowan Wang / Whitney Chen 等 |
| Scholar Coauthor 图谱（方法B，一阶）| Layer 2 | ✅ **11/11 全部完成** | Ruiqi Zhong（离职Row8）/ Thomas I. Liao（在职Row96）|
| Scholar Coauthor 图谱（方法C，二阶）| Layer 2 | ⬜ 暂停（性价比低）| — |
| Anthropic Engineering Blog | Layer 1C/其他 | ✅ **21/21完成** | Danny Lin / Ashley Wang / Sam Jiang / Bin Wu / Daniel Jiang |
| Claude Blog | Layer 1C/其他 | ✅ **98/98完成** | 无新发现 |

---

## 六、ACK 扫描详情

> **状态说明（2026-04-05 Review 修正）：**
> - 52/52 篇 Anthropic 论文全部有 `ack` 字段（`scanned=True`）
> - 其中 23 篇有实质人名致谢文本（非 Simons Foundation 模板），已于 2026-04-05 完成华人名字提取
> - 结论：**全部已发现华人均为已知人员，无新增**（见 §6.1 完整扫描结果）

### 6.1 23 篇实质 ACK 华人提取结果（2026-04-05 完成）

| 论文 ID | 标题（简） | 华人候选 | 处理状态 |
|---------|----------|---------|---------|
| 2401.05566 | Sleeper Agents | Jeff Wu / Miranda Zhang | ✅ 均已在 Excel |
| 2412.14093 | Alignment Faking | Andi Peng / Jeff Wu / Leo Gao / Whitney Chen | Andi Peng 离职；Jeff Wu/Whitney Chen 在职；Leo Gao 外部 |
| 2505.05410 | Reasoning Don't Say | Zihan Wang / Jerry Wei / Ruiqi Zhong | ✅ 均已在 Excel |
| 2503.10965 | Auditing LLMs | Xuchan Bao / Rowan Wang | Xuchan Bao 外部；Rowan Wang 已写入 Row91 |
| 2601.19062 | Who's in Charge | Saffron Huang | ✅ 已在 Excel Row88 |
| 2502.16797 | Forecasting Rare Behaviors | Yanda Chen | ✅ 已在 Excel Row69 |
| 2410.13787 | Looking Inward | Jenny Bao | ✅ **确认排除**：Jenny Bao = Xuchan Bao，UToronto PhD + Vector Institute，X账号 @XuchanB，ABC华裔，无Anthropic工作经历 |
| 2406.00877 | Chess Look-Ahead | Lawrence Chan | 外部（GovAI），非 Anthropic 员工 |
| 2312.06942 | AI Control | Lawrence Chan | 外部（GovAI），非 Anthropic 员工 |
| 2412.03556 | Best-of-N Jailbreaking | Edwin Chen | 外部（Surge AI CEO），已排除 |
| 2601.04603 | Constitutional Classifiers | 无 | — |
| 2601.10387 | Situating Default Persona | 无 | — |
| 2411.14257 | Knowledge Awareness | 无 | — |
| 2310.13548 | Sycophancy Understanding | 无 | — |
| 2411.00986 | AI Welfare | 无 | — |
| 2510.07192 | Poisoning Attacks | 无 | — |
| 2411.07494 | Rapid Response | 无 | — |
| 2406.12775 | Multi-Hop Queries | 无 | — |
| 2503.04761 | Economic Tasks | 无（名字提取失效）| — |
| 2406.10162 | Sycophancy to Subterfuge | 无 | — |
| 2310.13798 | Constitutional AI Principles | 无 | — |
| 2412.13678 | Clio | 无 | — |
| 2306.16388 | Subjective Opinions | 无（HTML 结构问题）| — |

**全部华人候选汇总（去重）：**
- ✅ 已在 Excel：Andi Peng / Jeff Wu / Miranda Zhang / Zihan Wang / Jerry Wei / Ruiqi Zhong / Rowan Wang / Saffron Huang / Yanda Chen / Whitney Chen
- ❌ 外部排除（全部用户确认）：Leo Gao（EleutherAI）/ Xuchan Bao=Jenny Bao（UToronto PhD，X:@XuchanB）/ Lawrence Chan（GovAI）/ Edwin Chen（Surge AI）

**结论：23 篇实质 ACK 扫描无新增在职华人员工。**

### 6.2 模板噪音论文（29 篇，含 Simons Foundation 类）

这些论文 ack 文本主要为 Simons Foundation / Schmidt Sciences 等机构致谢模板，无实质人名，已跳过。

---

## 七、人员状态

### 7.1 待处理候选人

| 人员 | 状态 | 问题 | 优先级 |
|------|------|------|--------|
| ~~Jenny Bao~~ | ~~未写入~~ | ✅ **已排除**：= Xuchan Bao，UToronto PhD，X @XuchanB，ABC，无 Anthropic 工作经历 | — |
| Xunjie Yu | Row97，在职 | Layer 2 待核实，无公开信息 | 中 |
| Newton Cheng | Row23，在职 | Layer 2，置信度"中"，进一步华裔身份核实 | 中 |
| Peilin Zhong | coauthor 节点 | Meta Superintelligence Lab，needs_check | 低 |
| Shannon Yang | 未写入 | 已知去向 UK AI Security Institute，需写入离职表 | 中 |
| Anna Chen | Row 待确认 | 在职状态待核实 | 中 |
| Daisong Yan | Row 待确认 | 在职状态待核实 | 中 |

### 7.2 字段不完整的已有人员

| 人员 | Excel Row | 缺失字段 |
|------|-----------|---------|
| Alvin Tsuei | Row93 | Col2（职位）、Col3（加入时间）为 None |
| Qian Liang | Row94 | Col2（职位）、Col3（加入时间）为 None |

### 7.3 关键人员确认记录

| 人员 | Excel Row | 华裔状态 | 备注 |
|------|-----------|---------|------|
| Newton Cheng | Row23 | Layer 2（姓 Cheng，待进一步核实）| Research Scientist, Frontier Red Team |
| Jack Chen | Row12 | Layer 2 | MTS，Stanford CS BS+MS，2024-06加入 |
| Rowan Wang | Row91 | Layer 2 | Harvard PhD on leave，@anthropic.com Scholar 验证 |
| Jiasen Yang | Row101 | **Layer 3（大陆华人）** | USTC统计学2009-2013，LinkedIn Chinese母语，The Org发现 |

---

## 八、API 账号状态

### LinkedIn API（2026-04-05 更新）

| 账号 | API 类型 | 状态 | 余量 | 用途 |
|------|---------|------|------|------|
| account_008 | enrichgraph Profile详情 | ✅ **首选** | ~56次 | 个人Profile全量（含隐藏教育记录） |
| account_015 | enrichgraph Profile详情（013 key） | ✅ 可用 | ~100次 | account_008 额度耗尽时备用 |
| account_014 | z-scraper 公司成员（013 key） | ✅ 可用 | ~99次 | 批量扫描公司成员 |
| account_009 | z-scraper 公司成员 | ✅ 可用 | ~46次 | 批量扫描公司成员 |
| account_013 | linkedin-scraper-fast Profile详情 | ✅ 可用（降为备用） | ~44次 | 仅主页教育字段，无隐藏记录 |
| account_005 | z-scraper 公司成员 | ❌ 已耗尽 | 0 | — |
| account_003/012/010 | 各异 | ❌ 已耗尽 | 0 | 5月重置 |

**关键发现（2026-04-05）：**
- account_008（enrichgraph）能抓到 `/details/education/` 隐藏教育记录（如 Jiasen Yang 的 USTC 少年班本科）
- account_013（ugoBoy）只返回主页可见的教育记录，无法获取隐藏页数据
- 从现在起：Profile 详情优先用 account_008，account_013 降为备用

### Apify（生产级，待正式上线使用）

| 账号 | Actor ID | 定价 | 状态 |
|------|---------|------|------|
| apify_001 | LpVuK3Zozwuipa5bp | $4/1k profiles | reserved（正式上线前不使用）|

- 按量计费，无月度限额，适合 Sourcing AI Agent 生产环境
- 实测可返回 USTC 少年班本科等隐藏教育记录（与 enrichgraph 能力一致）
- 字段差异：`insights`（非 `activities`）存社团信息；`startDate/endDate`（非 `date_range`）
- REDoc 字段结构参考：shortcutId=`cb3212d851f29adaf53859ad08fe4788`（hash=`400f58b8`）

### The Org API

| 账号 | API Key | 月限额 | 2026-04 状态 | 下次可用 |
|------|---------|--------|-------------|---------|
| theorg_001 | `6b4ad86403c9413da382c27902f7b5d1` | 10 credits = 1次调用/月 | **已耗尽** | 2026-05-01 |

---

## 九、已排除候选人

| 人名 | 排除原因 |
|------|---------|
| Meg Tong | 确认非华裔（用户确认，2026-04-05） |
| Amy Deng | METR/Martian 员工 |
| Tao Lin | Meta 工程师 |
| Richard Yuanzhe Pang | Meta FAIR |
| Alan Chan | Mila PhD → GovAI |
| Edwin Chen | Surge AI CEO，非 Anthropic 员工 |
| Xuchan Bao（Jenny Bao）| UToronto PhD + Vector Institute，X @XuchanB，ABC华裔。出现在 2410.13787 ACK 中，用户确认非员工（2026-04-05）|
| Yoon Kim | 韩裔 |
| Dawn Song | UC Berkeley 教授，外部合作者 |
| Leo Gao | EleutherAI，外部合作者，用户确认排除（2026-04-05）|
| Lawrence Chan | GovAI，外部 AI 安全研究者，用户确认排除（2026-04-05）|
| Ruiqi Zhong | Thinking Machines Lab（已移至离职表，非在职）|

---

## 十、关键决策记录

- **中文名强制规范**：无确认来源绝对不填
- **LinkedIn 优先**：在职状态以 LinkedIn 为准
- **Sourcing Agent 产品化方向**：本 Skill 是面向通用 Sourcing AI Agent 的工作流探索（见 Skill §0）
- **数据库 Schema V2**（2026-04-05）：arXiv 数据库升级，ACK 从分离文件合并到论文记录
- **统一视图 publications_unified.json**（2026-04-05）：三渠道合并
- **Scholar Coauthor 方法 C 暂停**：一阶发现的 28 位华人 Layer 2+ 均在其他学术机构，性价比低
- **The Org Free tier 限制**：每月仅 1 次 org-chart 调用（10 credits），2026-04 已用尽
- **方法分层体系（v5.5）**：Layer 1A（组织数据库）/ 1B（Bio批量过滤）/ 1C（论文作者）/ Layer 2（关系扩展）/ Layer 3（逐人核实）；判据：批量产出名单→L1，关系链扩展→L2，逐人确认→L3

---

## 十一、Open TODOs

### 🔴 高优先级
- [x] ~~**arXiv ACK 华人提取**：23 篇有实质人名 ACK 的论文~~ ✅ **已完成（2026-04-05）**：无新增在职华人，Jenny Bao 待核实
- [ ] 补全 Row93（Alvin Tsuei）、Row94（Qian Liang）缺失字段（职位、加入时间）

### 🟡 中优先级
- [ ] **Hunter.io 升级决策**：Free tier 仅获取 10 条（1.8%）；升级 Starter（$34/月）可全量 565 条，6 次翻页即可，脚本已就绪（`data/hunter/call_hunter.py`）
- [ ] Shannon Yang 写入离职表（去向：UK AI Security Institute）
- [ ] Newton Cheng（Row23）华裔身份进一步核实（Layer 2，置信度"中"）
- [ ] Xunjie Yu（Row97）华裔身份核实（无公开信息）

### 🟢 未执行但已定义的方法（按执行价值排序）

| 方法 | 层级 | 执行方式 | 预期价值 |
|------|------|---------|---------|
| **Hunter.io 全量（升级后）** | Layer 1A | 升级 Starter 后运行 `call_hunter.py`，翻页拉取 565 条 | **高**（与 LinkedIn 互补，含职级/部门）|
| **X (Twitter) Bio 批量搜索** | Layer 1B | X 高级搜索 / 搜索引擎辅助 `site:x.com "Anthropic"` | 中（Bio 及时性强，覆盖新入职/刚离职）|
| **GitHub Bio 批量扫描** | Layer 1B | `curl "https://api.github.com/search/users?q=company:anthropic"` | 中（覆盖工程型员工，与学术渠道互补）|
| **arXiv/Semantic Scholar 论文作者批量扫描** | Layer 1C | Semantic Scholar API `affiliation:Anthropic`，提取全部作者 | 中（系统化补全，但与已有 arxiv_scan 重叠）|
| **LinkedIn Search API** | Layer 1A | RapidAPI `company:"Anthropic"` 搜索 | 低（与 company/people API 高度重叠）|
| **Scholar Coauthor 图谱（方法 C，二阶）** | Layer 2 | 对 28 位华人 Layer 2+ 再扩一跳 | 低（性价比低，暂停）|
| ~~**arXiv 23篇有实质 ACK 华人提取**~~ | Layer 2 | ✅ **已完成（2026-04-05）**，无新增在职华人 | — |

### ⚙️ 工作流可优化点

1. **整合去重流程未实现**：The Org 数据（63人初筛）与 LinkedIn 数据尚未做 normalized_name 合并去重，目前靠人工对比。建议下次新渠道扫描时使用 §30.3 的合并去重模板代码先执行整合。

2. **候选人列表未系统展示给用户确认（阶段门 1 未执行）**：目前发现→直接核实→写入，缺少"一次性呈现所有未核实候选人→用户批量决策"的步骤。应在累积一定候选人后统一执行阶段门。

3. **The Org 姓名缩写问题（如"Jiasen Y."）**：The Org 部分用户使用 Last Name 缩写，导致初筛可能漏掉。已在 Skill §30.4.3 定义专项处理流程（extract_abbreviated_nodes + Google 搜索补全）。2026-05-01 重新调用时需对全部缩写节点批量处理。

4. **arXiv 早期论文 ACK 价值有限**：2021-2022 年论文（2112.xxx/2202.xxx 等）的被致谢人大多已通过其他渠道发现或非在职员工，建议优先扫描 2024 年以后的新论文 ACK。

5. **Xunjie Yu（Row97）来源不明**：当前备注为"Layer 2 待核实"，但未记录来源渠道。建议优先核实并补充来源字段，若无可靠证据应从 Excel 移出。

---

## 十二、Skill 版本记录

| 版本 | 日期 | 关键变更 |
|------|------|---------|
| **v5.9** | 2026-04-05 | account_008 恢复首选地位；account_008 vs account_013 对比表；教育格式模板恢复「学位在前」规范 |
| **v5.8** | 2026-04-05 | §15.5 新增 Excel 写入前强制检查清单（Checklist Gate）；§16 教育格式模板更新 |
| **v5.7** | 2026-04-05 | — |
| **v5.6** | 2026-04-05 | §31 Bio批量过滤方法体系；§30 方法分层架构优化 |
| **v5.5** | 2026-04-05 | §30/§31 方法分层体系重构：Layer 1 拆分 1A/1B/1C；Google Scholar/X/GitHub Bio 批量扫描明确为 Layer 1B；方法F/G/H详细定义；Layer 1B vs Layer 3 操作对照表 |
| **v5.4** | 2026-04-05 | §30 重构（三层定义）+ §31 新增（arXiv/ACK/Scholar Bio 归属） |
| **v5.3** | 2026-04-05 | §29 The Org API 方法 + §30 成员列表获取统一工作流（初版）|
| **v5.2** | 2026-04-05 | §28 投资机构成员扫描方法（6大渠道/机构规模分级/七步工作流）|
| **v5.1** | 2026-04-05 | §27 Scholar扫描方法完整定义（方法A/B/C）|
| **v4.0** | 2026-04-05 | §0 战略定位（Sourcing Agent 产品化）；§22 Publication 数据库架构；§23 进度规范；§24 产品化设计原则 |
| v3.8 | 2026-04-05 | §22 Publication 数据库架构；arxiv_scan_db V1→V2；unified 视图 |
| v3.7 | 2026-04-03 | §21 新增 claude.com/blog 渠道；claude_blog_scan_db 建立 |
| v3.5 | — | 教育经历学校前置全表转换 |
| v3.4 | — | §20 PROGRESS.md 强制规范 |
| v3.3 | — | §19 中文名防幻觉规范 |

## 十三、REDoc 文档

- **shortcutId**: `2454d39e5975288af6cc89291d551e06`
- **标题**: 「Anthropic 华人网络调研：Agent 工作流全记录」
- **最后更新**: 2026-04-05（本 session）
- **本次更新内容**:
  - 工作流总览补充 Step A1.2（方法分层框架）和 A1.5（Publication 扫描）
  - Step A1 候选人来源表格新增 The Org / Google Scholar Bio / X Bio / GitHub Bio 四行，并标注 Layer 归属
  - 新增 Step A1.2 节：三层方法分层框架完整表格（Layer 1A/1B/1C + Layer 2 + Layer 3）+ 标准工作流 7 步顺序

---

*最后更新：2026-04-05 17:35（Asia/Shanghai）*
