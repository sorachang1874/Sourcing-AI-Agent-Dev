# Anthropic 华人专项调研

> **项目性质：** 技术战略研究 · 内部参考  
> **负责人：** 小红书技术战略组  
> **最后更新：** 2026-04-05  
> **当前状态：** 🟡 持续进行中（在职 101 人已确认，持续扫描新渠道）

---

## 一、项目目的

系统性梳理 **Anthropic 公司的华人/华裔员工网络**，覆盖：

- **在职员工**：职位、团队、加入时间、教育背景、工作经历
- **离职员工**：离职去向、时间节点
- **关联网络**：投资机构华人成员、学术合作者

**战略价值：** 理解 Anthropic 的人才结构与来源，追踪华人 AI 研究者的流向，支持技术战略判断与人才洞察。

**方法论价值：** 本项目同时是 **Sourcing AI Agent 工作流**的实验场——沉淀可复用的多渠道扫描方法论（见 `skills/anthropic-employee-scan/SKILL.md`）。

---

## 二、当前进度

### Excel 数据（核心交付物）

| 表格 | 人数 | 最新版本 |
|------|------|---------|
| **在职华人员工** | **101 人** | `Anthropic华人专项调研_v6zz18.xlsx` |
| **已离职华人员工** | **8 人** | 同上（Sheet 2）|

**列结构（10列）：**
`姓名（英文+中文名）` | `现任职位/团队` | `加入时间` | `当前状态` | `LinkedIn` | `媒体链接` | `教育经历` | `工作经历` | `研究方向` | `备注`

### 扫描渠道完成情况

| 渠道 | 层级 | 状态 | 发现人数 |
|------|------|------|---------|
| LinkedIn 公司成员 API | Layer 1A | ✅ 主体完成 | ~78人 |
| The Org Org-Chart API | Layer 1A | ✅ 2026-04 完成（额度已用）| Jiasen Yang 等 |
| Hunter.io Domain Search | Layer 1A | ⚡ Free tier（仅10条）| Ming Zhong（Signals）|
| Google Scholar @anthropic.com | Layer 1B | ✅ 部分完成（方法A）| 5人 |
| arXiv 论文署名扫描（129篇） | Layer 1C | ✅ 100% 完成 | 约10人 |
| arXiv ACK 致谢扫描（52篇） | Layer 2 | ✅ 100% 完成（无新增）| — |
| Scholar Coauthor 图谱（一阶） | Layer 2 | ✅ 11/11 种子完成 | Thomas Liao / Ruiqi Zhong |
| Anthropic Engineering Blog | 其他 | ✅ 21篇 100% | 5人 |
| Claude Blog | 其他 | ✅ 98篇 100% | — |
| X Bio 批量搜索 | Layer 1B | ⬜ 未执行 | — |
| GitHub Bio 批量扫描 | Layer 1B | ⬜ 未执行 | — |

### 数据质量规范

- **中文名零容忍**：必须有 LinkedIn 主页 / 个人主页 / 媒体报道等可靠来源，禁止音译/推测
- **教育格式**：`学位：学校（简称），专业，YYYY-YYYY`（学位在前，倒序）
- **在职状态**：以 LinkedIn 当前 headline 为准
- **写入门槛**：Layer 2（已确认 Anthropic 工作经历）方可写入 Excel

---

## 三、文件结构

```
Anthropic华人专项/
│
├── README.md                          ← 本文件（项目入口）
├── PROGRESS.md                        ← 操作日志（每次执行前必读，执行后必更新）
├── api_accounts.json                  ← 所有 API 账号配置（LinkedIn/Apify/Hunter 等）
├── company_ids.json                   ← LinkedIn 公司 numeric ID 速查（Anthropic=74126343）
│
├── Anthropic华人专项调研_v6zz18.xlsx  ← 当前最新（在职101人，离职8人）
├── Anthropic华人专项调研_v6z*.xlsx    ← 历史版本（保留，可按版本号追溯）
│
├── onepagers/                         ← 早期个人一页纸报告（参考用）
├── onepager_fixup/                    ← 格式修正版
│
└── data/
    ├── profiles/
    │   └── anthropic_employees/       ← LinkedIn Profile 原始 JSON（155个文件）
    │                                     命名规则：{linkedin_slug}_{account_id}.json
    │
    ├── chinese_filter/                ← 华人身份四层分类数据库
    │   ├── all_members.json           ← 全体已知成员（103人）
    │   ├── confirmed_cjk.json         ← Layer 2：确认泛华人（51人）
    │   ├── confirmed_mainland.json    ← Layer 3：确认大陆华人
    │   ├── suspected.json             ← Layer 1：疑似华人（待核实）
    │   ├── excluded.json              ← 已排除候选人（附排除原因）
    │   └── README.md                  ← 数据库说明
    │
    ├── arxiv_scan/                    ← arXiv 论文扫描
    │   ├── arxiv_scan_db.json         ← 主数据库（129篇，Schema V2）
    │   ├── ack_summary.json           ← ACK 致谢扫描结果（52篇）
    │   ├── arxiv_scan_index.csv       ← 论文索引
    │   ├── fetch_ack.py               ← ACK 抓取脚本
    │   ├── SCHEMA_V2.md               ← 数据库字段说明
    │   └── README.md
    │
    ├── scholar_coauthor/              ← Google Scholar Coauthor 图谱
    │   ├── coauthor_graph_db.json     ← 图谱数据库（52个 coauthor 节点）
    │   ├── seed_authors.json          ← 种子节点（11人，全部完成）
    │   ├── candidate_queue.json       ← 断点续传队列
    │   └── SCHEMA.md
    │
    ├── hunter/                        ← Hunter.io 邮箱扫描
    │   ├── call_hunter.py             ← 调用脚本（含额度守卫+原子落盘）
    │   ├── hunter_accounts.json       ← 账号配置
    │   └── domain_search_anthropic_2026-04-05.json
    │
    ├── theorg/                        ← The Org 组织架构 API
    │   ├── call_theorg.py             ← 调用脚本（原子落盘）
    │   ├── orgchart_raw_2026-04-05.json  ← ⚠️ 不完整（81节点，2026-05-01重新调用）
    │   └── theorg_accounts.json
    │
    ├── engineering_scan/              ← Anthropic Engineering Blog 扫描
    │   └── engineering_scan_db.json   ← 21篇，100%完成
    │
    ├── claude_blog_scan/              ← claude.com/blog 扫描
    │   └── claude_blog_scan_db.json   ← 98篇，100%完成
    │
    ├── publications_unified.json      ← 统一视图（arXiv+Engineering+Claude Blog，248篇）
    ├── anthropic_scholar_ids.json     ← Scholar ID 索引（11人种子）
    └── scholar_scan_results.json      ← Google Scholar 扫描汇总
```

---

## 四、API 账号总览

> 详细配置见 `api_accounts.json`

| 账号 | 供应商 | 用途 | 月额度 | 当前余量 |
|------|--------|------|--------|---------|
| **account_008** | enrichgraph（独立key） | ⭐ Profile 详情首选（含隐藏教育记录）| 100次 | ~57次 |
| **account_015** | enrichgraph（013 key） | Profile 详情备用 | 100次 | ~100次 |
| **account_014** | z-real-time-scraper（013 key） | 公司成员批量扫描 | 100次 | ~99次 |
| **account_009** | z-real-time-scraper | 公司成员批量扫描 | 100次 | ~46次 |
| **account_013** | linkedin-scraper-fast | Profile 详情降级备用 | 50次 | ~44次 |
| **apify_001** | Apify | 🏭 生产级（reserved，正式上线后用）| 按量 $4/1k | — |
| **hunter_001** | Hunter.io | 邮箱域名搜索（Free tier） | 60次 | 56次 |
| **theorg_001** | The Org | Org-chart（10 credits/月）| 1次/月 | 0（2026-05-01重置）|

**关键参数：**
- Anthropic LinkedIn numeric ID：`74126343`
- Anthropic LinkedIn slug：`anthropic`
- account_008 端点：`GET https://real-time-linkedin-data-scraper-api.p.rapidapi.com/people/profile?profile_id={slug}`

---

## 五、快速上手（新会话/新成员）

### 第一步：了解项目状态
```bash
# 1. 读本文件（已在读）
# 2. 读 PROGRESS.md 了解最新操作进度和 TODO
cat PROGRESS.md | head -100

# 3. 确认当前 Excel 版本
ls -t *.xlsx | head -1
```

### 第二步：继续扫描（标准操作流程）

1. **打开 PROGRESS.md** → 找「Open TODOs」
2. **选择一个未完成渠道** → 参照 `skills/anthropic-employee-scan/SKILL.md` 对应章节
3. **执行前检查 API 余量**：`python3 -c "import json; d=json.load(open('api_accounts.json')); [print(a['id'], a.get('total_used','?'), '/', a.get('monthly_limit','?')) for a in d['accounts']]"`
4. **发现候选人** → 先查 Excel 确认未重复 → 通过 LinkedIn/个人主页核实 → 写入 Excel 新版本
5. **写入前自检**：教育格式（`学位：学校，专业，YYYY-YYYY`）、中文名来源、加入时间格式
6. **每次执行后**：git commit + 更新 PROGRESS.md

### 第三步：写入 Excel 规范
```python
# 写入前必查：确认不重复
python3 -c "
import openpyxl
wb = openpyxl.load_workbook('Anthropic华人专项调研_v6zz18.xlsx')
ws = wb['在职华人员工']
names = [ws.cell(r,1).value for r in range(2, ws.max_row+1) if ws.cell(r,1).value]
print(f'当前 {len(names)} 人')
print([n for n in names if 'Yang' in str(n)])  # 示例：搜索 Yang
"
```

---

## 六、待完成事项（截至 2026-04-05）

### 🔴 高优先级
- [ ] **The Org 完整数据**：2026-05-01 重新调用 `data/theorg/call_theorg.py`（当前81节点不完整）
- [ ] **Alvin Tsuei（Row93）、Qian Liang（Row94）**：职位和加入时间字段缺失，需补全

### 🟡 中优先级
- [ ] **X Bio 批量搜索**（方法G）：`site:x.com "Anthropic" bio` 批量过滤华人
- [ ] **GitHub Bio 批量扫描**（方法H）：`https://api.github.com/search/users?q=company:anthropic`
- [ ] **Newton Cheng（Row23）** 华裔身份进一步核实
- [ ] **Xunjie Yu（Row97）** 华裔身份核实（无公开信息）
- [ ] **Hunter.io 升级决策**：Starter $34/月 可获取完整 565 条，脚本 `data/hunter/call_hunter.py` 已就绪

### 🟢 长期规划
- [ ] 2026-05-01：The Org 重新调用（完整数据约 ~450 节点）
- [ ] Sourcing AI Agent 正式上线后：切换至 Apify 生产方案（apify_001，$4/1k）

---

## 七、关键规则（必读）

1. **中文名零容忍**：无可靠来源（LinkedIn 主页/个人主页/媒体）绝对不填，禁止音译、推断、占位
2. **Excel 版本递增**：每次修改另存新版本号，不覆盖原文件
3. **不重复写入**：每次写入前必须 Python 查重
4. **API 额度守卫**：每个账号有 stop_at 阈值，超出停止调用
5. **原子落盘**：脚本先写 `.tmp` 再 `os.rename()`，防止截断
6. **每次执行后必更新 PROGRESS.md**：保持进度同步

---

## 八、相关文档

| 文档 | 位置 | 用途 |
|------|------|------|
| **Skill 文档**（方法论）| `skills/anthropic-employee-scan/SKILL.md` | 所有扫描方法的完整定义（v5.9）|
| **操作进度日志** | `PROGRESS.md` | 每次执行的详细记录和 TODO |
| **Agent 工作流全记录** | REDoc shortcutId: `2454d39e5975288af6cc89291d551e06` | 完整操作历史（云端）|
| **Apify 字段结构参考** | REDoc shortcutId: `cb3212d851f29adaf53859ad08fe4788` | Jiasen Yang 实测返回字段 |

---

*README 生成于 2026-04-05 · 小红书技术战略组 AI 实习生*
