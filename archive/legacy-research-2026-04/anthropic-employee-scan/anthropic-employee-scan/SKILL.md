# Skill: anthropic-employee-scan

> **适用场景：** 系统性扫描目标 AI 公司（以 Anthropic 为例）的全量员工名单，识别其中华人/华裔成员，提取标准化信息，产出「在职华人员工」和「已离职华人员工」两张 Excel 表格。目标是最大化覆盖率，同时保证信息真实准确可溯源。

---

## §0 战略定位：面向 AI Agent 产品的工作流探索

> ⚠️ **本 Skill 不仅是一个执行手册，更是一次对可复用 Sourcing AI Agent 的工作流探索。**

### 0.1 最终目标

本项目的所有实践——数据库设计、多渠道扫描方法、多层过滤机制、任务编排方式——将沉淀为一个通用 **Sourcing AI Agent** 的基础能力，用于：

- Anthropic 华人员工全量 Sourcing（当前项目）
- Google Gemini Team Researcher Sourcing
- 任意 AI 公司特定条件成员 Sourcing（sourcing criteria 可迭代）

### 0.2 三大执行意识

在执行项目的每一步时，必须同时保持三个意识：

| 意识 | 含义 | 具体体现 |
|------|------|----------|
| **前瞻性** | 以合适的方法编排项目架构 | 数据库 Schema 版本化、多层过滤分离、统一 Publication 视图、可迭代 sourcing criteria |
| **即时性** | 不让经验和进度停留在对话上下文中 | 每次发现新方法 → 立刻写入 Skill；每次执行 → 立刻更新 PROGRESS.md；所有 API 返回 → 立刻持久化 |
| **可审计性** | 所有操作留有历史记录 | Excel 版本递增命名（v6zz7）、数据库备份（v1_backup）、`scan_date` 字段、ack_summary 留存原文 |

### 0.3 Sourcing Agent 产品化路径

```
当前阶段（Skill）                    目标阶段（Agent 产品）
─────────────────────────────────    ─────────────────────────────────
手动触发扫描任务                  →  定时/事件驱动自动扫描
人工确认 华人候选人               →  多维度置信度评分自动过滤
Excel 手动维护                   →  结构化数据库 + 前端查询界面
单公司 Anthropic                 →  多公司并行扫描（criteria 配置化）
对话 context 传递状态            →  PROGRESS.md + DB 持久化状态机
```

### 0.4 Sourcing Criteria 可迭代设计

每次 Sourcing 任务的核心参数应设计为可配置：

```json
{
  "target_company": "Anthropic",
  "company_slug": "anthropicresearch",
  "criteria": {
    "ethnicity": ["CJK"],
    "employment_status": ["current", "former"],
    "confidence_layer": 2
  },
  "scan_channels": ["linkedin_api", "arxiv", "engineering_blog", "scholar"],
  "output_format": "xlsx"
}
```

未来切换到 Google Gemini Team 时，只需修改 `target_company`、`company_slug`、`criteria`，所有扫描方法和数据管道保持不变。

---

## 一、整体工作流（七步法）

```
Step 1: API 批量拉取在职员工完整列表（company/people）
        ↓
Step 2: 多渠道补充已离职员工候选人
        （论文 / 官网 / GitHub / 搜索引擎 / 中文媒体）
        ↓
Step 3: 合并候选人名单，初步华人识别
        （姓名特征 + 教育背景，不调用 Profile API）
        ↓
Step 4: ⚠️ 阶段门：用户确认候选人名单
        ↓
Step 5: 通过搜索引擎批量确认 LinkedIn slug
        ↓
Step 6: 调用 Profile API 精细化信息提取（用户确认后）
        ↓
Step 7: 写入 Excel，产出在职 / 离职两张表
```

**核心原则：**
- **数据资产意识**：所有 API 返回立即存档，目录结构见第九节
- **信息真实第一**：宁留空标「待核实」，不凭推断填写
- **Profile API 须用户确认后执行**：消耗额度，须审慎

---

## 二、Step 1：API 批量拉取在职员工

### 2.1 API 选型

**主力：Z Real-Time LinkedIn Scraper（account_009 / account_005）**

```
Host:     z-real-time-linkedin-scraper-api1.p.rapidapi.com
Endpoint: GET /api/company/people
Params:   username={company_slug}&page={n}&limit=50
月额度:   100次/账号
```

```bash
curl -s --request GET \
  --url 'https://z-real-time-linkedin-scraper-api1.p.rapidapi.com/api/company/people?username=anthropicresearch&page=1&limit=50' \
  --header 'x-rapidapi-host: z-real-time-linkedin-scraper-api1.p.rapidapi.com' \
  --header 'x-rapidapi-key: {API_KEY_009}'
```

### 2.2 关键参数说明与踩坑记录

| 参数 | 正确值 | 踩坑记录 |
|------|--------|---------|
| `username` | `anthropicresearch`（LinkedIn 公司主页 slug）| ⚠️ 不是 `anthropic`（那是另一家 PE 基金），也不是 numeric company_id |
| `limit` | `50`（每页最大条数） | ⚠️ **曾误用 `limit=10`，导致每次只拿 1/5 的数据，浪费大量额度** |
| `page` | 从 1 开始递增 | `hasMore` 字段不完全可靠，需强制翻页直到返回空 |

### 2.3 翻页策略

```python
page = 1
while True:
    resp = call_api(username="anthropicresearch", page=page, limit=50)
    people = resp.get("data", {}).get("data", [])
    if not people:          # 返回空列表 → 真正结束
        break
    save_page(page, resp)
    if not resp.get("data", {}).get("hasMore", True):
        break               # hasMore=False 也停止（但不能只靠它）
    page += 1
    time.sleep(0.8)         # 速率控制
```

> **⚠️ `hasMore` 字段在 company/people 接口中基本可信（与 search/people 不同）**，但仍以返回空列表为最终判断标准。

### 2.4 返回结构与 headless 过滤

```json
{
  "data": {
    "data": [
      {
        "fullName": "Da Yan",
        "headline": "Member of Technical Staff at Anthropic",
        "urn": "urn:li:fsd_profile:ACoAAB...",
        "id": "412509275"
      },
      {
        "fullName": "LinkedIn Member",   ← headless，隐私模式，直接丢弃
        "headline": "...",
        "urn": "..."
      }
    ],
    "hasMore": true,
    "total": 1050
  }
}
```

过滤条件：`fullName != "LinkedIn Member"`（约占 30~40%）

### 2.5 数据存档

```
linkedin_profiles/raw/anthropic_employees_page001.json   ← 每页原始响应
linkedin_profiles/raw/anthropic_employees_page002.json
...
linkedin_profiles/raw/anthropic_employees_all.json       ← 全量合并去重
```

---

## 三、Step 2：多渠道补充已离职员工

company/people 只返回**当前在职**成员，离职员工须多渠道补充。

### 3.1 渠道优先级

```
arXiv 论文署名 > Anthropic 官网 research/news > GitHub 组织成员 > 搜索引擎 > 中文媒体
```

### 3.2 各渠道操作方法

#### ① arXiv 论文署名（最高优先级，覆盖离职员工）

核心论文的作者在署名时标注了 Anthropic，即使已离职也留有记录——这是覆盖率最高的离职员工来源。

```bash
# 检索 Anthropic 机构的论文
web_fetch "https://arxiv.org/search/?query=anthropic&searchtype=affiliation&start=0" maxChars=50000

# 直接检索核心论文（抓作者列表）
web_search 'arxiv Anthropic "Constitutional AI" OR "RLHF" OR "Claude" OR "Alignment Faking" OR "Sleeper Agents" authors site:arxiv.org 2022 2023 2024 2025'

# 已知核心论文（直接 fetch 抓作者）
web_fetch "https://arxiv.org/abs/2212.08073"   # Constitutional AI
web_fetch "https://arxiv.org/abs/2204.05862"   # RLHF (Anthropic)
web_fetch "https://arxiv.org/abs/2401.05566"   # Alignment Faking
web_fetch "https://arxiv.org/abs/2401.05561"   # Sleeper Agents
```

提取作者名，对照华人姓氏词典筛选，重点关注已署名但不在 company/people 返回列表中的人（即离职员工）。

#### ② Anthropic 官网 Research & News

```bash
web_fetch "https://www.anthropic.com/research" maxChars=50000
web_fetch "https://www.anthropic.com/news"     maxChars=50000
web_fetch "https://www.anthropic.com/blog"     maxChars=50000
```

提取 `by` / `author` 字段人名，与 company/people 结果交叉比对。

#### ③ GitHub 组织成员

```bash
web_fetch "https://github.com/orgs/anthropics/people" maxChars=30000
```

#### ④ 搜索引擎（离职员工专项）

```bash
# 已知离职人员用作验证基准：Yuntao Bai（→OpenAI）、Shunyu Yao（→Google DeepMind）、Xin Zhou（→Meta）
web_search '"left Anthropic" OR "ex-Anthropic" OR "formerly Anthropic" Chinese researcher 2024 2025 2026'
web_search 'Anthropic 华人研究员 离职 去向 site:36kr.com OR site:qbitai.com OR site:jiqizhixin.com'
web_search 'site:linkedin.com "formerly at Anthropic" OR "ex-Anthropic" Chinese'
```

#### ⑤ 中文科技媒体

```bash
web_search '"Anthropic" "华人" OR "前Anthropic" OR "离开Anthropic" 2024 2025 2026'
web_search 'site:scmp.com Anthropic Chinese researcher engineer'
```

### 3.3 候选人存档

```json
// linkedin_profiles/raw/anthropic_alumni_candidates.json
{
  "fetched_at": "2026-04-02T14:00:00+08:00",
  "candidates": [
    {
      "name": "Yuntao Bai",
      "source": "arXiv论文署名",
      "linkedin_slug": "yuntao-bai-3039bb138",
      "current": "OpenAI MTS",
      "confidence": "confirmed"
    }
  ]
}
```

---

## 四、Step 3：合并候选人名单，初步华人识别

### 4.0 多层过滤机制（四层分类系统）

华人身份判定采用**渐进式四层过滤**，从全体成员开始逐层收窄，避免"姓名推断"误判。数据资产完整保存在 `data/chinese_filter/` 目录。

#### 架构总览

```
Layer 0: all_members.json
    ━━ 全体已知 Anthropic 成员（含非华人）
       来源：LinkedIn API / arXiv论文署名 / 官网 / 媒体
       每次发现新成员，先在此登记
         ↓ 姓名符合华人特征
Layer 1: suspected.json
    ━━ 疑似华人（仅姓名特征，缺乏背景证据）
       ⚠️ 不写入Excel
         ↓ 找到语言/教育/工作华人证据
Layer 2: confirmed_cjk.json
    ━━ 确认泛华人（CJK华裔）
       包含：大陆/香港/台湾/新加坡/ABC/英裔/加裔等广义华裔
       ✅ 可写入Excel，备注标注证据
         ↓ 确认大陆教育或工作经历
Layer 3: confirmed_mainland.json
    ━━ 确认大陆华人
       ✅ 可写入Excel
```

#### 分层定义

| 层级 | 文件 | 触发条件 | 写Excel |
|------|------|---------|---------|
| ⬜ Layer 0 | `all_members.json` | 任何来源发现的 Anthropic 成员 | - |
| 🟡 Layer 1 | `suspected.json` | 仅姓名特征符合华人模式 | ❌ |
| 🟠 Layer 2 | `confirmed_cjk.json` | 有直接华人背景证据 | ✅ |
| 🔴 Layer 3 | `confirmed_mainland.json` | 大陆教育/工作经历 | ✅ |
| ❌ 排除 | `excluded.json` | 明确非华裔或非目标人员 | ❌ |

#### 新候选人处理流程（强制）
```
1. 检索 all_members.json → 是否已记录？
   ├── 是：查当前 layer，决定是否晋级
   └── 否：添加到 all_members.json，再判断 layer

2. 依据证据分配 layer（见下方晋级条件）

3. Layer 2+ 且 in_excel=false → 写入 Excel

4. 更新 last_updated 字段
```

**⚠️ 写入 Excel 之前，必须先检查 all_members.json。**

#### 晋级证据

**Layer 1 → 2（满足任一）：**
- LinkedIn `languages` 含 Chinese（Simplified 或 Traditional）
- 教育/实习/工作含大陆/港澳台/新加坡华文机构
- 本人公开自述华裔身份
- 有可确认来源的中文名（非推断，须标注来源）

**Layer 2 → 3（满足任一）：**
- 本科/硕士/博士在中国大陆高校
- 明确的大陆工作或实习经历
- 本人自述来自大陆

**降级 → excluded：**
- 明确确认非华裔
- 非 Anthropic 在职/离职员工（如外部合作者，写表格时标注）

#### 写 Excel 规则
- Layer 0/1 → **不写Excel**（仅本地存档）
- Layer 2/3 → **可写Excel**，备注注明证据来源
- 若身份仍有存疑 → 备注标注 `【疑似华人，待确认】`（仍需 Layer 2+）

#### 存档格式（见 data/chinese_filter/README.md）
```json
// all_members.json（Layer 0）
{
  "name_en": "英文名",
  "anthropic_status": "在职 | 已离职 | 外部合作 | 未确认",
  "source": ["excel_active", "arxiv_paper", "linkedin_api", "media"],
  "position": "职位",
  "excel_sheet": "在职华人员工",
  "excel_row": 85,
  "last_updated": "YYYY-MM-DD"
}

// suspected / confirmed_cjk / confirmed_mainland（Layer 1-3）
{
  "name_en": "英文名",
  "name_zh": "中文名（须有来源，不推断）",
  "layer": 2,
  "layer_label": "确认泛华人/CJK华裔",
  "anthropic_status": "在职",
  "evidence": {
    "name_pattern": "Huang是华人常见姓",
    "linkedin_languages": "Chinese (Simplified)",
    "education_cn": "清华大学 本科 2014-2018",
    "work_cn": null,
    "self_stated": null,
    "media_source": "量子位报道，中文名'赵孟'"
  },
  "confidence": "高 | 中 | 低",
  "in_excel": true,
  "excel_row": 85,
  "excel_sheet": "在职华人员工",
  "notes": "备注",
  "last_updated": "YYYY-MM-DD"
}
```

---

### 4.1 华人识别维度

| 维度 | 判断依据 | 置信度 |
|------|---------|--------|
| 姓名特征 | 常见华人姓氏拼音（Li/Zhang/Wang/Chen/Liu/Yang/Huang/Lin/Wu/Zhou/Xu/Sun/He/Gao/Ma/Ye/Luo；粤闽变体：Chan/Leung/Ng/Tan/Lim/Yip/Kwok/Cheung） | 中（→Layer 1） |
| LinkedIn languages | 含 Chinese (Simplified/Traditional) | 高（→Layer 2） |
| 教育背景 | 含大陆/港台/新加坡华文机构 | 高（→Layer 2/3） |
| 论文 / 博客 | 有中文姓名或自述华人背景 | 高（→Layer 2/3） |
| 中文媒体报道 | 有明确报道+中文名 | 高（→Layer 2/3） |
| Location 仅作参考 | 当前在 Bay Area/NYC，不单独作为华人判据 | 低 |

**写入Excel前必须达到 Layer 2（confirmed_cjk）或以上。**

### 4.2 候选人汇总格式

| 序号 | 姓名 | 来源 | LinkedIn slug | 置信度 | Layer | 备注 |
|------|------|------|--------------|--------|-------|------|
| 1 | Da Yan | API | da-yan-1b8211216 | confirmed | L3 | 大陆背景 |
| 2 | (新发现) | arXiv | 待查 | possible | L1 | 仅姓名，待核实 |

---

## 五、Step 4：阶段门——用户确认

展示候选人清单，明确：
1. 名单中有无需要移除的（非华人 / 信息错误）？
2. 有无遗漏的已知华人成员？
3. 哪些人需要调用 Profile API？（已有存档的跳过）

---

## 六、Step 5：批量确认 LinkedIn slug

**正确方法：直接搜索 `{姓名} Anthropic LinkedIn`，不加任何职能词修饰**

```bash
# ✅ 正确
web_search "Da Yan Anthropic LinkedIn"
web_search "Sihan Li Anthropic LinkedIn"

# ❌ 错误（修饰词会过滤掉结果）
web_search 'site:linkedin.com/in "Sihan Li" Anthropic "member of technical staff"'
web_search "Victor Wen Anthropic LinkedIn finance strategy"
```

> **踩坑记录：**
> - `site:linkedin.com/in "姓名" + 多修饰词` → 搜索引擎过度过滤，大量人搜不到
> - 职能词（如 "finance strategy"）匹配不上实际职位（如 "Revenue Accounting"）→ 漏掉目标
> - 正确策略：最简关键词，让搜索引擎自然排序，第一条 99% 就是目标人

**从搜索结果 URL 提取 slug：**
```
https://www.linkedin.com/in/sihan-li-30170639/  →  slug = sihan-li-30170639
https://www.linkedin.com/in/victorswen/          →  slug = victorswen
```

---

## 七、Step 6：调用 Profile API

### 7.1 优先读取本地存档

```python
for slug in slug_list:
    archive = f"linkedin_profiles/raw/profiles/{slug}_003.json"
    if os.path.exists(archive):
        data = json.load(open(archive))['raw']
        continue   # 已有存档，不调用 API
    # 否则调用 API
```

### 7.2 API 选型与调用

**主力：account_003（linkedin-scraper，字段最完整且稳定）**

```bash
curl -s --request GET \
  --url 'https://linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com/profile/detail?username={slug}' \
  --header 'x-rapidapi-host: linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com' \
  --header 'x-rapidapi-key: {API_KEY_003}'
```

返回结构（`data.basic_info` / `data.experience` / `data.education`）：

```json
{
  "success": true,
  "data": {
    "basic_info": {
      "fullname": "Sihan Li",
      "headline": "Member of Technical Staff at Anthropic",
      "profile_url": "https://linkedin.com/in/sihan-li-30170639",
      "location": { "full": "California, United States" }
    },
    "experience": [
      {
        "title": "Member of Technical Staff",
        "company": "Anthropic",
        "start_date": {"year": 2023, "month": "Jun"},
        "end_date": null
      }
    ],
    "education": [...]
  }
}
```

**主力推荐：account_008（enrichgraph）** — 数据最完整，优先使用

```bash
curl -s --request GET \
  --url 'https://real-time-linkedin-data-scraper-api.p.rapidapi.com/people/profile?profile_id={slug}&bypass_cache=false&include_contact_info=false&include_network_info=false' \
  --header 'Content-Type: application/json' \
  --header 'x-rapidapi-host: real-time-linkedin-data-scraper-api.p.rapidapi.com' \
  --header 'x-rapidapi-key: {API_KEY_008}'
```

> ✅ **2026-04-05 重新验证可用**，2026-04-02 的不可达问题为临时故障，现已恢复正常。

**account_008 vs account_013 核心差异（2026-04-05 实测）：**

| 能力 | account_013（ugoBoy） | account_008（enrichgraph） |
|------|----------------------|---------------------------|
| 教育经历 | 仅主页可见（`/details/education/`隐藏记录抓不到） | ✅ **全量**（含需点击「显示全部」才能看到的隐藏记录） |
| 字段结构 | `education[].degree_name / field_of_study / start_date.year` | `education[].degree / field_of_study / date_range.start.year` |
| 工作经历精确月份 | ✅ 有 | ✅ 有（含 `positions[]` 嵌套多职位） |
| 实测案例 | Jiasen Yang：仅返回 Purdue 2条 | Jiasen Yang：返回 Purdue 2条 + **USTC 少年班本科**（隐藏页才有） |
| 月额度 | 50次 | 100次 |
| 剩余额度（2026-04）| ~44次 | ~57次 |

> **结论：优先使用 account_008；account_013 仅在 account_008 不可达时作为备用**

**速率控制：** 每次请求间隔 `≥ 0.8s`

### 7.3 存档规范

```
linkedin_profiles/raw/profiles/{slug}_003.json   ← account_003 结果
linkedin_profiles/raw/profiles/{slug}_008.json   ← account_008 结果（备用）
```

---

## 八、Step 7：写入 Excel

### 8.1 字段规范（在职华人员工）

| 字段 | 提取来源 | 格式规范 |
|------|---------|---------|
| **姓名** | `basic_info.fullname` + 搜索核实中文名 | 英文在前，确认中文名才加换行，如：`Da Yan\n闫达` |
| **现任职位/团队** | `experience[0].title` | 英文职位 + 括号内注方向，如：`Member of Technical Staff\n（预训练基础设施）` |
| **加入时间** | Anthropic experience 的最早 `start_date` | 格式 `YYYY-MM`，无月份写 `YYYY` |
| **当前状态** | `experience[0].company` 是否为 Anthropic 且 `end_date` 为空 | `✅在职` / `❓待核实` |
| **LinkedIn链接** | `basic_info.profile_url` | 完整 URL |
| **媒体链接** | 搜索引擎补充 | 论文/X/Twitter/个人网站，无则留空 |
| **教育经历** | `education[]` 全量 | 见下方"教育经历格式规范"，多条换行，倒序（最高学历在前） |
| **主要工作经历** | `experience[]` 历任职位 | `YYYY/MM-今，公司，职位`，倒序，多条换行 |
| **主要研究方向** | headline + experience 描述 + 论文 | 精简关键词，如：`GPU计算 / DNN编译器 / 预训练基础设施` |
| **备注** | 综合 | 族裔背景、院校年级、关键亮点，≤2句，客观事实 |

#### 教育经历格式规范（2026-04-03 更新）

**模板：** `学校，学位，专业（方向），YYYY-YYYY`（学校在前，学位在后）

> ⚠️ **2026-04-05 格式统一**：全表已统一为「学校在前」格式，旧的「博士：学校」前置写法已全部废弃。

| 情形 | 格式 | 示例 |
|------|------|------|
| 正常完成（有完整年份） | `学校，学位，专业，YYYY-YYYY` | `斯坦福大学，博士，计算机科学，2018-2023` |
| 只知毕业年 | `学校，学位，专业，YYYY毕业` | `斯坦福大学，硕士，计算机科学，2014毕业` |
| 在读（知道入学年） | `学校，学位，专业，YYYY入学，在读` | `德克萨斯大学奥斯汀分校，博士，计算机科学，2023入学，在读` |
| 在读（不知入学年） | `学校，学位，专业，在读` | `加州大学伯克利分校，博士，计算机科学，在读` |
| 休学/暂停 | `学校，学位，专业，休学中` 或 `学校，硕士，专业，暂停` | `哈佛大学，博士，计算机科学，休学中` |
| 肄业/退学 | `学校，学位，专业，YYYY入学，肄业` | `达特茅斯学院，博士，2008入学，肄业` |
| 无年份信息 | `学校，学位，专业` | `斯坦福大学，本科，计算机科学` |
| 联合培养/转学（两段本科） | 各写一行，均写`本科`，按时间倒序 | `伊利诺伊大学香槟分校，本科，计算机科学，2009-2011\n武汉大学，本科，软件工程，2007-2009` |
| 联合学位（本硕连读） | 拆成两行分别写 | `哈佛大学，硕士，统计学（AB/SM联合项目），2019-2023\n哈佛大学，本科，统计学，2019-2023` |
| 法学博士（JD） | `学校，法学博士，法学，YYYY-YYYY` | `加州大学旧金山法学院，法学博士，知识产权法，2010-2013` |
| MBA | `学校，MBA，YYYY-YYYY` | `宾夕法尼亚大学沃顿商学院，MBA，2014-2016` |
| 访问研究员 | `机构，访问研究员（导师/组名），YYYY-YYYY` | `MIT LIDS，访问研究员（Suvrit Sra组），2022-2023` |

**禁止的格式：**
- ❌ `博士：学校，专业，YYYY-YYYY` → 改为 `学校，博士，专业，YYYY-YYYY`
- ❌ `本科：UC Berkeley，EECS` → 改为 `加州大学伯克利分校，本科，EECS`
- ❌ `硕士（MBA）：沃顿商学院` → 改为 `宾夕法尼亚大学沃顿商学院，MBA`

**专有名词翻译规则：**
- ETH Zürich → 苏黎世联邦理工学院（ETH Zürich）（括号保留，无通用中译）
- RMIT University → RMIT理工大学（澳大利亚）
- 麻省理工学院（MIT）括号内英文缩写可保留
- ❌ `硕士（MSE）+本科（BS）：学校，专业，YYYY-YYYY` → 拆成两行
- ❌ `本科/硕士（联合学位）：学校，专业，YYYY-YYYY` → 拆成两行
- ❌ `本科（转学前）：学校，专业，YYYY-YYYY` → 直接写`本科：学校，专业，YYYY-YYYY`
- ❌ `法学博士（JD）：学校，YYYY-YYYY` → 改为 `博士：学校，法学，YYYY-YYYY`
- ❌ `博士：学校，专业，YYYY`（单年份含义不明） → 加 `毕业` 或 `入学` 说明

**字段禁忌：不写主观评价；不推断动机；不确定信息写「待核实」**

### 8.2 已离职华人员工（额外字段）

| 字段 | 格式 |
|------|------|
| **原职位/团队** | 在 Anthropic 的最后职位 |
| **离职时间** | `YYYY-MM`，仅有年份写 `YYYY（约）` |
| **当前去向** | `公司，职位`，如：`OpenAI，Member of Technical Staff` |

### 8.3 Python 写入代码模板

```python
import openpyxl
from openpyxl.styles import Alignment
from copy import copy
import shutil

# 备份后在新版本上操作
shutil.copy2("v6y.xlsx", "v6z.xlsx")
wb = openpyxl.load_workbook("v6z.xlsx")
ws = wb["在职华人员工"]

headers = [ws.cell(1,c).value for c in range(1, ws.max_column+1)]
col = {h: i+1 for i,h in enumerate(headers) if h}

# 检查已有姓名避免重复
existing = {str(ws.cell(r,1).value or '').split('\n')[0].strip()
            for r in range(2, ws.max_row+1)}

def append_row(ws, last_row, col, p):
    new_row = ws.max_row + 1
    # 复制格式
    for c in range(1, len(col)+1):
        src = ws.cell(last_row, c)
        nc = ws.cell(new_row, c)
        if src.has_style:
            nc.font = copy(src.font)
            nc.fill = copy(src.fill)
            nc.border = copy(src.border)
        nc.alignment = Alignment(wrap_text=True, vertical='top')
    ws.cell(new_row, col['姓名']).value = p['fullName']
    ws.cell(new_row, col['现任职位/团队']).value = p['current_title']
    ws.cell(new_row, col['当前状态']).value = '✅在职' if p['in_anthropic'] else '❓待核实'
    ws.cell(new_row, col['LinkedIn链接']).value = p['linkedin_url']
    ws.cell(new_row, col['教育经历']).value = p['edu_str']
    ws.cell(new_row, col['主要工作经历']).value = p['exp_str']
    ws.cell(new_row, col['备注']).value = p.get('location','')

wb.save("v6z.xlsx")
```

---

## 九、API 账号快速参考（2026-04-02 状态）

| 账号 | Host | 用途 | 月额度 | 已用 | 可用 | 状态 |
|------|------|------|------|------|------|------|
| account_009 | z-real-time-linkedin-scraper-api1 | 批量拉公司在职员工（**首选**） | 100 | ~56 | ~42 | ✅ 可用 |
| account_005 | z-real-time-linkedin-scraper-api1 | 批量拉公司在职员工（次力） | 100 | 84 | 14 | ⚠️ 余量少 |
| account_013 | linkedin-scraper-api-real-time-fast-affordable | 个人 Profile 详情（备用） | 50 | ~6 | ~44 | ✅ 可用（但仅主页教育字段，隐藏记录抓不到） |
| account_008 | real-time-linkedin-data-scraper-api | 个人 Profile 详情（**首选**，全量教育字段） | 100 | ~43 | ~57 | ✅ 2026-04-05 验证恢复可用 |
| account_010 | z-real-time-linkedin-scraper-api1 | 批量公司员工 | 100 | 100 | 0 | ❌ 本月已耗尽 |
| account_001/002/004/006/007 | 各异 | 特定场景备用 | — | — | — | ⚠️ 勿主用 |

**账号状态文件：** `Anthropic华人专项/api_accounts.json`  
**API Key：** 从 `api_accounts.json` 读取，不硬编码在脚本中

---

## 十、数据目录结构

```
Anthropic华人专项/
  Anthropic华人专项调研_v6y.xlsx          ← 当前最新版本（2026-04-02）
  api_accounts.json                        ← API 账号状态存档
  linkedin_profiles/
    raw/
      anthropic_employees_page001.json     ← Step 1 每页原始响应
      anthropic_employees_all.json         ← Step 1 全量合并
      search_past_anthropic_v2_page001-003.json  ← 已离职搜索结果
      anthropic_alumni_candidates.json     ← Step 2 多渠道候选人
      profiles/
        {slug}_003.json                    ← Step 6 个人 Profile（account_003）
        {slug}_008.json                    ← Step 6 个人 Profile（account_008）
    archived_wrong/                        ← 抓错人时移入此目录
  linkedin_profiles/{slug}.json            ← 早期旧格式存档（可读取）
```

---

## 十一、数据资产管理（强制规范）

> ⚠️ **极强的数据资产意识是本 Skill 的核心纪律。** 每一次 API 调用都消耗额度，结果必须立即持久化，不得只在内存或对话上下文中使用后丢弃。

### 11.1 统一存储目录

```
Anthropic华人专项/
  data/
    profiles/
      anthropic_employees/    ← Anthropic 员工 LinkedIn profile（当前项目）
      investor_firms/         ← 投资机构华人员工 profile（扩展项目）
    profiles/README.md        ← 每个子目录的 API 来源说明和字段结构说明
```

**每次爬取 profile 后，必须立即存档：**
```bash
# 爬取后存档（命名规范：{slug}_{account}.json）
curl ... | tee data/profiles/anthropic_employees/{slug}_{account}.json | python3 -c "解析打印"
```

### 11.2 API 调用必须原子化（防 SIGTERM 丢失）

> **历史教训（2026-04-02）：** 批量爬取脚本被 SIGTERM 中断，崩溃前已发出的 API 请求被计费但结果未存档，既浪费额度又无法追溯。

**错误模式：先收集到内存/变量，再统一写文件**
```bash
# ❌ SIGTERM 在循环中发生 → 已调用的 API 结果全部丢失
for slug in $slugs; do
    response=$(curl ...)      # 请求已发出并计费
    results+=($response)      # 存内存
done
echo $results > output.json   # SIGTERM 后这行永远不执行
```

**正确模式：curl 直接 tee 存文件（调用即落盘）**
```bash
# ✅ 每次 curl 完成后立即落盘，SIGTERM 只影响当前这一条
for slug in $slugs; do
    outfile="data/profiles/anthropic_employees/${slug}_012.json"
    # 幂等跳过：已有文件则不重复调用 API
    [ -f "$outfile" ] && echo "跳过 $slug（已存在）" && continue
    curl -s --max-time 15 \
      "https://{host}/profile/detail?username=$slug" \
      -H "x-rapidapi-key: {KEY}" \
      | tee "$outfile" \                  # ← 立即落盘
      | python3 -c "import json,sys; ..."  # 打印摘要
    sleep 0.8
done
```

**两个关键点：**
1. `tee "$outfile"`：curl 输出同时写文件 + 传给 python 打印，不经过变量
2. `[ -f "$outfile" ] && continue`：幂等检查，重跑脚本自动从断点续传，不浪费额度

### 11.3 同一人只保留一份 Profile（去重原则）

**同一 slug 不重复爬取，不同 API 账号爬同一人会产生多份文件——这是浪费。**

正确规则：
- **同 slug + 同 account**：直接读已有文件，跳过 API 调用
- **同 slug + 不同 account**：只有在原文件已失效/数据不完整时才重爬，且替换不新增
- **不同 slug + 同一人**：仅在确认同一人后，保留最新最完整的一份，另一份移入 `data/profiles/archived_wrong/`

**执行前必查：**
```python
import os
slug = "guangfeng_he"
existing = [f for f in os.listdir("data/profiles/anthropic_employees/") if f.startswith(slug)]
if existing:
    print(f"⚠️ 已存在：{existing}，跳过 API 调用，直接读取")
    # → 读取已有文件，不再调用 API
```

### 11.4 爬取 slug 的优先顺序（重要！）

**在爬取任何人的 LinkedIn profile 之前，必须先查 Excel 中已有的链接：**

```
优先级 1：读 Excel（G列/LinkedIn链接列）→ 提取 slug（最可信，经用户核实）
优先级 2：查 data/profiles/anthropic_employees/ 目录 → 已有文件则直接读
优先级 3：Google 搜索 `{姓名} Anthropic LinkedIn` → 找 slug
优先级 4：OpenReview profile 页面 → 有时含 LinkedIn 链接
```

**原因：Excel 中的 LinkedIn 链接是经过用户人工核实的，可信度最高。** 直接从搜索引擎找 slug 容易抓到同名不同人的错误账号（历史教训：`frankzheng` vs `zheng-frank`，`frankzheng` 对应微软SDE而非 Anthropic 的 Frank Zheng）。

```python
# 读取 Excel 中已有的 LinkedIn 链接作为 slug 来源
import openpyxl, re
wb = openpyxl.load_workbook("Anthropic华人专项调研_v6zh.xlsx")
ws = wb["在职华人员工"]
slug_map = {}
for row in ws.iter_rows(min_row=2, values_only=True):
    name = str(row[0] or '').split('\n')[0].strip()
    linkedin_url = str(row[4] or '')  # E列：LinkedIn链接
    m = re.search(r'linkedin\.com/in/([^/?]+)', linkedin_url)
    if m:
        slug_map[name] = m.group(1)
# slug_map 优先于搜索引擎结果
```

### 11.5 投资机构扫描的数据持久化（历史教训）

**2026-04-02 教训：投资机构华人成员扫描时，API 返回结果未做持久化，数据已永久丢失。**

正确做法：扫描任何目标机构时，所有 Profile API 返回必须存入 `data/profiles/investor_firms/{机构名}/` 下，不得只写 Excel 而丢弃原始数据。

---

## 十二、已知踩坑汇总

| 问题 | 原因 | 正确做法 |
|------|------|---------|
| 搜索引擎找不到 slug | 用了 `site:限定 + 多修饰词` | 直接搜 `{姓名} Anthropic LinkedIn`，第一条就是目标 |
| **LinkedIn 链接存在时仍用 Scholar 判断在职状态** | Google Scholar 显示的是论文发表时的学术挂靠，不代表当前工作单位 | **LinkedIn 链接存在 → 以 LinkedIn 为准，不用 Scholar 覆盖判断；Scholar 只用于辅助发现候选人** |
| **LinkedIn 链接与姓名未核对就填写** | 批量写入时复制粘贴失误，row58/row60 的链接对调 | 每次写入 LinkedIn 链接后，必须用「slug提取→profile姓名」核对一遍；有 profile 文件时，核对 `basic_info.fullname` 与 Excel 姓名列是否匹配 |
| limit 用了 10 | 脚本默认值写错 | **必须用 limit=50**，每次只拿 1/5 是严重浪费 |
| company slug 用 `anthropic` | 误用 | 正确是 `anthropicresearch`（那家是 PE 基金） |
| `hasMore=False` 但实际还有数据 | z-real-time company/people 的 hasMore 偶有误报 | 以"返回空列表"为最终判断，不只看 hasMore |
| search/people 关键词用职能词 | `keywords=engineer` 会让 LinkedIn 按语义排序 | search/people 的 keywords 只用 `"Anthropic"`，让 passCompanies 做主要过滤 |
| search/people limit 也用了 10 | 同上 | **同样必须 limit=50** |
| account_008 API 不可达 | 供应商问题 | 立即切 account_003，不重试 |
| urn:li:member:xxx 不被接受 | /api/profile 只接受 slug / url / fsd_profile URN | company/people 返回的是 member URN，不可直接用于 profile 查询 |
| Victor Wen 搜到职能不匹配的同名人 | 搜索关键词带了 "finance strategy"，漏了 "Revenue Accounting" | 不加职能词修饰，让搜索引擎排序 |
| account_010 月内耗尽 | search/people 测试时跑了多余的页数 | search/people 限评估性使用，正式扫描用 company/people |
| **Semantic Scholar 返回的作者名不可信** | SS 的 `name` 字段会截断或误拼真实姓名：`Yi Hua`→实为`Yu Hua Cheng`；`Xunjie Yu`→`Xinpei Yu`；`Christopher Liu`→`Chengyin Liu` | SS 名字只作「线索」，**必须通过 arXiv abs 页面或搜索引擎交叉核实原始拼写，再搜 LinkedIn** |
| **需要核实某人是否为 Anthropic 成员时，先自行 Google** | 反复询问用户会浪费交互资源 | **先用 Google 搜索 `姓名 "Anthropic"` 或 `姓名 "Anthropic" LinkedIn`；90% 的情况第一条结果就能确认；无法确定时再与用户交互** |
| **Yanda Chen 被错误排除** | 从 Google Scholar 判断她是「Columbia 学术合作者」，但 Scholar 显示的是论文发表时的机构，不代表当前工作单位 | **在职/离职状态必须以 LinkedIn 为最终依据**：LinkedIn 显示 Anthropic → 在职；不能用 Scholar 邮箱域名覆盖 LinkedIn 的判断 |
| **经验教训没有及时写入持久化存储** | 有价值的调研结论只存在于对话 summary 中，每次 context compaction 后需要重新从 summary 提取，反复浪费 token | **每次调研结论（人员状态、踩坑、新方法）必须立刻写入 Skill 踩坑表或 MEMORY.md；不要依赖对话 summary 传递历史信息** |
| **新发现候选人前未先核对 Excel 是否已存在** | context compaction 后不知道之前已记录的人员，导致重复添加（Runjin Chen 被添加两次） | **每次发现新候选人，必须先在当前最新 Excel 在职/离职两张表中用姓名关键词检索，确认不存在后再添加** |
| **教育字段格式退化（v6zr 全表73行）** | LLM 批量格式化时没有参照正确样例，将格式从 `学位：学校，专业，年份` 退化为 `学校，专业，年份，学位`；且顺序退化（本科排在硕士/博士前面）| **格式化教育字段必须参照参考表（如 v6y 样本），严格遵循 `学位标签：学校，专业，年份` + 时间倒序（最高/最新学历在前）；LLM 输出后须人工抽查3-5行** |
| **离职表字段对齐错乱（Shannon Yang）** | 填写时混淆了列定义：Twitter链接填入了「当前去向」列（应为LinkedIn）、「当前去向」内容（UK AISI）填入了「离职时间」列 | **填写离职表时，每行写入前先确认列定义（表头行）；col4=离职时间，col5=当前去向，col6=LinkedIn链接，col7=媒体链接** |
| **无来源中文名（Shannon Yang「杨珊珊」）** | Shannon 是英文名，Yang 是华人姓，LLM 推断出「杨珊珊」并填入，无任何来源 | **见 §19 强制规范：中文名字段不确定绝对不填，Yang 姓无法推断出具体中文名** |
| **arXiv 论文 HTML 过大，直接 web_fetch 占满上下文** | arXiv 论文 HTML 全文可达数 MB（如 Constitutional Classifiers 2501.18837），`web_fetch` 返回结果直接进入对话上下文，导致 token 溢出或上下文被无关 HTML 充满 | **必须先用 `curl` 将 HTML 下载到本地 `data/arxiv_scan/html_raw/{id}.html`，然后用 Python/grep 从本地文件提取 ACK 段落。ACK 通常在文件末尾（Appendix/References 之后），可只读最后 200KB。绝对不要把大型论文 HTML 全文通过 `web_fetch` 灌入对话上下文。** 若 `arxiv.org/html/{id}` 返回极短内容（< 5KB），说明该论文无 HTML 版本，此时用 `curl` 下载 PDF 到本地，再用 `pdftotext`（或 `python3 -c "import fitz"`）提取文字后搜索 ACK 段落。 |
| **arXiv HTML 版本不可用的论文** | 部分论文（如 2501.18837）在 `arxiv.org/html/` 端点返回极短 HTML（< 5KB），无法提取任何有用信息 | **检测方法：`curl -s arxiv.org/html/{id} \| wc -c` < 5000 则视为无 HTML 版本。回退方案：(1) 下载 PDF 到本地用 `pdftotext` 提取；(2) 若 `pdftotext` 失败（Syntax Error / trailer dictionary），尝试安装 `pymupdf`；(3) 最后兜底：从 Anthropic 官网对应 research 页面获取论文全文，或用浏览器打开 PDF 页面截图提取。** |

---

## 十三、华人判定注意事项

1. **「华人」定义**：广义华人（大陆/台湾/香港/新加坡/马来西亚华裔均纳入），分析时区分背景
2. **疑似华人**：建立「待核实」分组，不急于纳入或排除；Profile 拿到后再判定
3. **同名同姓陷阱**：常见姓名（如 Li Ming、Wang Fang）须额外确认——Location 在 Bay Area/NYC + headline 含 Anthropic 才初步确认
4. **API 抓错人**：若返回人物与目标不符，立即将错误文件移入 `archived_wrong/`，重新找正确 slug
5. **LinkedIn 公开性**：部分人为纯 headless 模式，无法通过任何 API 获取信息，这是覆盖率的天花板

---

## 十四、当前进度快照（2026-04-02）

- **在职华人员工表**：53 行（含表头），覆盖已确认华人 52 人
- **已离职华人员工表**：7 行（含表头），覆盖 6 人
- **当前最新 Excel 版本**：`Anthropic华人专项调研_v6y.xlsx`
- **下一步工作方向**：通过 arXiv 论文 + Anthropic 官网 + GitHub 补充更多已离职成员

---

## 十五、全覆盖 arXiv ID 清单建立方法

### 14.1 核心原则

- **区分「Anthropic 署名论文」vs 「引用/相关论文」**：只有前者才算入员工识别
- **来源优先级**：Anthropic 官网 sitemap > arXiv 作者搜索 > Semantic Scholar 回溯
- **持续更新**：每月检查官网新文章，补充新 arXiv ID

### 14.2 Step-by-Step 全覆盖流程

#### Step A：从 Anthropic 官网抓取全部 Research 文章

```bash
# 获取官网 sitemap 中所有 /research/ 路径
web_fetch "https://www.anthropic.com/sitemap.xml" maxChars=50000

# 提取 research URL 列表
# 正则：<loc>https://www.anthropic.com/research/([^<]+)</loc>
# 结果：约 107 篇文章（2026-04-02）
```

#### Step B：从每篇文章提取 arXiv ID

```python
# 并行抓取每篇文章，提取 arxiv.org/abs/xxxx.xxxxx 链接
import re, subprocess, json
from concurrent.futures import ThreadPoolExecutor

def fetch_arxiv_ids(url):
    r = subprocess.run(['curl','-s','--max-time','8',url], capture_output=True, text=True)
    return re.findall(r'arxiv\.org/abs/([0-9]{4}\.[0-9]{4,5})', r.stdout)

# 107 篇文章 → 124 个独立 arXiv ID（2026-04-02 实测）
```

**存档格式：**
```json
// anthropic_arxiv_full_coverage.json
{
  "updated_at": "2026-04-02T17:25:00+08:00",
  "source": "anthropic.com sitemap",
  "total_research_pages": 107,
  "unique_arxiv_ids": 124,
  "ids": {
    "2501.18837": {
      "source_slugs": ["constitutional-classifiers"],
      "title": "Constitutional Classifiers: Defending against Universal Jailbreaks",
      "year": 2025,
      "verified_anthropic": true
    },
    "2601.04603": {
      "source_slugs": ["next-generation-constitutional-classifiers"],
      "title": "Constitutional Classifiers++: Efficient Production-Grade Defenses...",
      "year": 2026,
      "verified_anthropic": true
    }
  }
}
```

#### Step C：验证「Anthropic 署名」vs 「外引」

**关键区分方法：**

| 方法 | 操作 | 判断标准 |
|------|------|---------|
| **Semantic Scholar API** | `GET /paper/arXiv:{id}?fields=authors` | 作者中有 Anthropic 核心成员（Dario/Amanda/Jared 等） |
| **arXiv abs 页面** | `curl arxiv.org/abs/{id}` | 作者列表中 ≥3 个已知 Anthropic 成员 |
| **论文内文 affiliation** | fetch PDF 或 abs page | 明确标注 "Anthropic" 机构 |

**注意：引用 Anthropic 技术 ≠ Anthropic 署名**（如 DeepSeek-R1 论文引用 Constitutional AI）

```python
# 批量验证是否为 Anthropic 自有论文
ANTHROPIC_CORE_AUTHORS = {
    'Dario Amodei', 'Daniela Amodei', 'Amanda Askell', 'Jared Kaplan',
    'Sam McCandlish', 'Chris Olah', 'Jack Clark', 'Deep Ganguli',
    'Chris Olsson', 'Nelson Elhage', 'Tom Brown', 'Jan Leike'
}

def is_anthropic_paper(authors_list):
    author_names = set(a.get('name','') for a in authors_list)
    return bool(author_names & ANTHROPIC_CORE_AUTHORS)
```

#### Step D：提取所有作者并筛华人

```python
# 从验证为 Anthropic 的论文中提取全部作者
all_authors = defaultdict(set)      # name -> {paper_ids}
chinese_candidates = defaultdict(list)  # name -> [(paper_id, author_id)]

for arxiv_id, info in verified_anthropic_papers.items():
    authors = fetch_ss_authors(arxiv_id)  # Semantic Scholar API
    for a in authors:
        name = a.get('name','')
        aid = a.get('authorId','')
        all_authors[name].add(arxiv_id)
        if is_likely_chinese(name):  # 用姓氏词典
            chinese_candidates[name].append((arxiv_id, aid))
```

**从 27 篇验证论文中提取：255 位独立作者，33 位疑似华人（2026-04-02）**

#### Step E：人工核实新候选人

对每个疑似华人，确认其与 Anthropic 的关系：

```bash
# 优先级 1：直接搜索 LinkedIn
web_search "{姓名} Anthropic LinkedIn"

# 优先级 2：查 Google Scholar 邮箱域名
web_fetch "https://scholar.google.com/citations?user={author_id}" maxChars=5000
# 看是否显示 "Verified email at anthropic.com"

# 优先级 3：查个人主页 / GitHub
web_search "{姓名} Anthropic site:github.com OR site:{name}.github.io"
```

### 14.3 全覆盖 vs 核心论文的区别

| 维度 | 核心论文（~15篇） | 全覆盖（~27篇已验证，持续增加） |
|------|------------------|------------------------------|
| 目的 | 快速识别核心团队 | 最大化覆盖率，发现边缘/新入职成员 |
| 来源 | Constitutional AI / Sleeper Agents / RLHF 等标志性论文 | 官网 sitemap 全部文章 |
| 作者数 | ~50-80 人 | ~255 人（2026-04-02） |
| 华人发现 | 已知核心成员 | 新发现 Bobby Chen、Zihan Wang 等 |
| 维护成本 | 低（固定清单） | 中高（需定期更新） |

### 14.4 持续更新机制

**每月检查清单：**
1. 重新抓取 `anthropic.com/sitemap.xml`，对比现有清单
2. 新文章 → 提取 arXiv ID → 验证是否为 Anthropic 署名
3. 新论文 → 提取作者 → 筛华人 → 核实 LinkedIn
4. 更新 `anthropic_arxiv_full_coverage.json`

**文件位置：**
- 全覆盖清单：`Anthropic华人专项/arxiv_full_coverage.json`
- 华人候选：`Anthropic华人专项/arxiv_chinese_candidates.json`

---

## 十五·五、Excel 写入前强制检查清单

> ⚠️ **每次生成写入代码前，必须逐项核对以下清单，全部通过才能执行写入。**
> 这是方案一（Checklist Gate）：在成本允许前，用显式自查替代自动校验脚本。
> 背景决策：2026-04-05 用户确认，后续将迁移至前后端分离 Sourcing Agent 架构，届时由后端格式层统一保证，届时此清单可废弃。

### A. 写入前：逐字段自查

**Col1（姓名）**
- [ ] 英文名已填写
- [ ] 中文名**仅在有以下来源时才填**：LinkedIn 个人主页自述 / 中文媒体报道原文 / 个人主页显式写明
- [ ] **不确定 → 绝对不填，不加「待确认」占位**（§19 零容忍）
- [ ] 历史错误示例：赵孟（Zhao Meng）、冯石（Shi Feng）、杨珊珊（Shannon Yang）均因无来源被删除

**Col2（职位/团队）**
- [ ] 格式：`职位全名`（换行）`（括号内：具体团队/方向）`
- [ ] 职位来自 LinkedIn experience.title，不自行翻译

**Col3（加入时间）**
- [ ] 格式：`YYYY-MM`（精确到月）或 `YYYY`（仅知年）或 `（待确认）`
- [ ] 来源：LinkedIn experience[0].start_date（Anthropic 那条）
- [ ] **禁止**：`2022/07`（斜杠）、`2022年7月`（中文）

**Col7（教育经历）**
- [ ] 格式：**`学校，学位，专业，年份`（学校在最前，学位在后）**
- [ ] **❌ 绝对禁止学位前置**：不写 `博士：学校`，不写 `本科：学校`
- [ ] 学校名翻译成中文（参考 §15.2 学校对照表）
- [ ] 有入学+毕业年份的写 `YYYY-YYYY`；仅知毕业写 `YYYY毕业`；在读写 `在读` 或 `在读（预计YYYY）`
- [ ] 最高/最新学历在最前（时间倒序）

**Col8（工作经历）**
- [ ] 格式：`YYYY/MM-今，公司，职位`（倒序，最新在前）
- [ ] 有精确月份写 `YYYY/MM`，仅知年写 `YYYY`，不确定写 `（待核实）`
- [ ] 实习标注：公司名后加 `，Research Intern` 或 `，实习`
- [ ] **禁止**：`前：（待核实）`、`Apple前，Google`（此类含糊描述）

**Col10（备注）**
- [ ] 中文名来源必须在备注中标注（如：「来源：个人主页自述」）
- [ ] 有待核实项目的，明确标注 `【待核实：XXX】`

### B. 写入后：快速抽查

写入 Excel 后，读回 Col7 和 Col1 执行以下 2 行 Python 自检：

```python
import re, openpyxl
wb = openpyxl.load_workbook("xxx.xlsx")
ws = wb["在职华人员工"]
errors = []
for r in range(85, ws.max_row+1):
    edu = str(ws.cell(r,7).value or "")
    name = str(ws.cell(r,1).value or "")
    # 学位前置检测
    if re.match(r'^(博士|硕士|本科|PhD|Master|Bachelor|Doctor)[:：]', edu):
        errors.append(f"Row {r}: 教育字段学位前置 → {edu[:40]}")
    # 英文学校名未翻译
    if re.search(r'\b(University|College|Institute|School)\b', edu):
        errors.append(f"Row {r}: 教育字段含未翻译英文 → {edu[:40]}")
    # 中文名含「待确认」占位
    if "待确认" in name or "待核实" in name:
        errors.append(f"Row {r}: 姓名字段含占位中文名 → {name[:30]}")
for e in errors: print("⚠️", e)
if not errors: print("✅ 格式自检通过")
```

> 此脚本不替代方案二（自动校验脚本），是在架构迁移前的过渡手段。

---

## 十六、核实渠道优先级与 Excel 填写规范（权威标准）

> ⚠️ **格式必须严格对齐前31行基准行**。以下规范来自用户实际审查后的明确要求（2026-04-02），请勿用低智能脚本处理——格式化须用 LLM 逐人翻译整理。

### 15.1 核实渠道优先级（2026-04-02 实战总结）

| 优先级 | 渠道 | 适用场景 | 命中率 |
|--------|------|---------|--------|
| ★★★★★ | **OpenReview** `openreview.net/profile?id=~{名}_{姓}1` | 论文作者核实 @anthropic.com | 极高（直接显示邮箱域名） |
| ★★★★ | **LinkedIn（浏览器 People Also Viewed）** | 主动扩展同公司员工 | 高（基于浏览行为聚类） |
| ★★★★ | **RocketReach** `rocketreach.co/{name}-email_{id}` | 补充工作经历，免费可见 | 高（有时比 LinkedIn 更新更早） |
| ★★★ | **Google Scholar 个人页面** `scholar.google.com/citations?user={id}` | 学术背景 + 邮箱域名验证 | 中高（需找到 Scholar ID） |
| ★★★ | **arXiv abs 页面** `arxiv.org/abs/{id}` | 批量发现 Anthropic 论文作者 | 中（citation_author meta 格式为"姓, 名"） |
| ★★ | **Google 搜索** `{姓名} Anthropic LinkedIn` | 找 LinkedIn slug | 中（搜不到时改用 OpenReview） |

**关键教训：**
- OpenReview 是最强的"无 LinkedIn 时"核实手段，直接返回 @anthropic.com 确认状态
- arXiv citation_author 的格式是 **"Last, First"**（如 `Bai, Yuntao`），脚本用 `parts[-1]` 匹配姓氏会全漏，必须取 `parts[0]`（逗号前）或转为 "First Last" 格式再比对
- Semantic Scholar 作者名不可信（截断/误拼），必须以 arXiv abs 页面原文为准
- LinkedIn「People Also Viewed」只在浏览器可见，API 不返回

### 15.2 Excel 六大字段格式规范（权威版）

以下以 row 2-31 为基准，每字段给出正确 vs 错误示例。

---

#### ① 姓名（A列）
**格式：** `英文名（换行）中文名`，中文名仅在确认时填写
```
✅ Da Yan
   闫达

✅ Calvin Liu-Navarro
   刘长远

✅ Angela Jiang          （无中文名则不加换行）

❌ Calvin Liu-Navarro (刘长远）   （括号格式，错误）
❌ Da Yan / 闫达                  （斜杠格式，错误）
```

---

#### ② 现任职位/团队（B列）
**格式：** `职位全名（换行）（括号内写具体团队/方向）`
```
✅ Member of Technical Staff
   （预训练与推理计算基础设施）

✅ Head of Trust & Safety ML     （无细分方向时，单行即可）

❌ Member of Technical Staff     （缺少团队/方向括号，错误）
```

---

#### ③ 加入时间（C列）
**格式：** `YYYY-MM`（精确到月）；仅知年份写 `YYYY`；不确定标 `（待确认）`
- **来源：** LinkedIn profile 中 Anthropic 工作经历的 `start_date.year` + `start_date.month`
```
✅ 2022-07
✅ 2024      （仅知年份）
✅ （待确认）
❌ 2022/07   （斜杠格式，错误）
❌ 2022年7月  （中文格式，错误）
```

---

#### ④ 教育经历（G列）
**格式：** `学位中文：学校中文，专业，年份`，多段换行

**规则（严格执行）：**
- 学位必须翻译成中文（博士/硕士/本科/博士后/访问研究员/法学博士）
- 学校名必须翻译成中文，不留任何英文
- 入学和毕业年份两者均写（`2019-2024`），仅知毕业年写`XXX毕业`
- 特殊经历（访问研究员、Simons Fellow等）也单独一行

**格式：`学校，学位，专业，年份`（学校在前，学位在后）**

```
✅ 斯坦福大学，博士，计算机科学，2019-2024
   MIT LIDS，访问研究员（Suvrit Sra组）；多伦多大学Vector研究所（Roger Grosse组）
   清华大学，本科，计算机科学（姚班），2015-2019

✅ 哈佛大学，Simons Fellow，自然基本定律中心，2018-2020
   约翰斯·霍普金斯大学，博士后，高能理论
   耶鲁大学，博士，理论物理，2009-2015
   北京大学，本科，物理，2005-2009

✅ 宾夕法尼亚大学沃顿商学院，MBA，2014-2016   （MBA直接写，不加"硕士（MBA）"）
✅ 加州大学旧金山法学院（前UC Hastings），法学博士（JD），知识产权法，2010-2013
✅ 哈佛大学，硕士，公共卫生（生物统计），2017-2019

❌ 博士：斯坦福大学，计算机科学，2019-2024    （学位前置，错误）
❌ 硕士（MBA）：沃顿商学院，2014-2016         （学位前置+冗余括号，错误）
❌ 法学博士（JD）：加州大学旧金山法学院       （学位前置，错误）
❌ Bachelor's Degree, CS：UC Davis            （英文未翻译，错误）
❌ 本科：UC Berkeley，EECS                    （学位前置+学校简称，错误）
❌ 加州大学戴维斯分校，传播学                 （缺学位和年份，错误）
```

**常用学校英→中对照：**
| 英文 | 中文 |
|------|------|
| Stanford University | 斯坦福大学 |
| MIT / Massachusetts Institute of Technology | 麻省理工学院 |
| UC Berkeley / University of California, Berkeley | 加州大学伯克利分校 |
| UCLA / University of California, Los Angeles | 加州大学洛杉矶分校 |
| UC Davis / University of California, Davis | 加州大学戴维斯分校 |
| UC Santa Cruz | 加州大学圣克鲁兹分校 |
| Carnegie Mellon University / CMU | 卡内基梅隆大学 |
| Princeton University | 普林斯顿大学 |
| Harvard University | 哈佛大学 |
| Yale University | 耶鲁大学 |
| Cornell University | 康奈尔大学 |
| University of Michigan | 密歇根大学 |
| University of Illinois Urbana-Champaign / UIUC | 伊利诺伊大学香槟分校 |
| University of Washington / UW | 华盛顿大学 |
| Georgia Institute of Technology / Georgia Tech | 佐治亚理工学院 |
| University of Waterloo | 滑铁卢大学 |
| University of Southern California / USC | 南加州大学 |
| New York University / NYU | 纽约大学 |
| Johns Hopkins University | 约翰斯·霍普金斯大学 |
| University of Texas at Austin / UT Austin | 德克萨斯大学奥斯汀分校 |
| Boston University | 波士顿大学 |
| Nankai University | 南开大学 |
| Peking University | 北京大学 |
| Tsinghua University | 清华大学 |
| Shanghai Jiao Tong University / SJTU | 上海交通大学 |
| Fudan University | 复旦大学 |
| Zhejiang University | 浙江大学 |
| University of Science and Technology of China / USTC | 中国科学技术大学 |
| Sun Yat-sen University / SYSU | 中山大学 |
| Huazhong University of Science and Technology / HUST | 华中科技大学 |
| Renmin University of China | 中国人民大学 |
| Beijing Foreign Studies University | 北京外国语大学 |
| Hong Kong University of Science and Technology / HKUST | 香港科技大学 |
| City University of Hong Kong | 香港城市大学 |
| University of British Columbia / UBC | 英属哥伦比亚大学 |
| Queen's University | 女王大学 |
| Smith School of Business | 史密斯商学院 |

**学位英→中对照：**
| 英文 | 中文 |
|------|------|
| Doctor of Philosophy / PhD / Ph.D. | 博士 |
| Master of Science / MS / M.S. | 硕士 |
| Master of Engineering / MEng | 硕士（工程） |
| Master of Business Administration / MBA | 硕士（MBA） |
| Master of Public Health / MPH | 硕士（公共卫生） |
| Juris Doctor / JD | 法学博士（JD） |
| Bachelor of Science / BS / B.S. | 本科 |
| Bachelor of Arts / BA / B.A. | 本科 |
| Bachelor of Commerce / BCom | 本科（商科） |
| Postdoctoral Researcher | 博士后 |
| Visiting Researcher / Fellow | 访问研究员 |

---

#### ⑤ 主要工作经历（H列）
**格式：** `YYYY/MM-今，公司，职位`，多段换行，前6条

**规则：**
- 时间用 `-`（连字符），**不用 `~`**
- 在职写「今」，结束写 `YYYY/MM`
- 公司名可中英混合或全中文，避免全英文缩写
- 职位可加括号说明具体方向，如 `MTS（预训练/推理计算）`

```
✅ 2022/07-今，Anthropic，MTS（预训练/推理计算）
   2021/04-2022/06，OpenAI，独立承包商（Triton DNN编译器/GPU内核）

✅ 2024/05-今，Anthropic，Head of Revenue Accounting and Operations
   2023/05-2024/05，Plaid，Head of Revenue Accounting and Operations
   2022/07-2023/03，Google，Cloud GTM Finance

❌ 2025/Jul~至今，Anthropic，Member of Technical Staff   （~格式，英文月份，错误）
❌ 2019/Sep~2025/Jun，Google，Senior Software Engineer    （~格式，错误）
```

---

#### ⑥ 主要研究方向（I列）
**格式：** `方向A / 方向B / 方向C`（斜杠分隔，精简2-4项，中文为主）
```
✅ GPU计算 / DNN编译器优化 / 预训练基础设施
✅ 对齐 / 自学推理 / Code RL
✅ AI产品 / 开发者工具
❌ （空白）   ← row 32+ 大量为空，必须填写
```
即使是非研究岗（招聘、法务、财务），也应填写其专业方向（如 `技术招聘 / Recruiting Ops`、`收入会计 / SaaS财务`）。

---

#### ⑦ 备注（J列）
**格式：** 分号分隔的标签串，顺序：身份→学校级年→论文署名→其他亮点

```
✅ 大陆华人；北大2006级物理；宾大CS硕士；多家AI创业公司联合创始人
✅ 美籍华裔（ABC）；中文中级；Sleeper Agents/Constitutional Classifiers论文作者
✅ 台湾裔美国人（ABC）；UCLA本科/硕士→斯坦福博士；Auditing LMs论文作者
✅ 大陆华人；华科大2012级；Anthropic早期华人基础设施工程师
❌ （空白）   ← 必须填写
❌ 在职确认（用户提供LinkedIn）；slug: bing-ju-45119355；待补充profile   ← 过于技术化，不是目标格式
```

**身份标签规范：**
- 大陆出生：`大陆华人`
- 美国土生华裔：`美籍华裔（ABC）` 或 `华裔（美籍，ABC）`
- 台湾裔：`台湾裔美国人（ABC）`
- 香港背景：`香港人` 或 `港人`
- 新加坡/马来西亚：`新加坡华人` 等
- 不确定：`疑似华裔（待核实）`

---

### 15.3 Profile API 状态（2026-04-02 更新）

| 账号 | Provider | Host | 状态 | 响应路径 |
|------|---------|------|------|---------|
| account_012 ★首选 | ugoBoy（新key） | linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com | ✅ 可用 | `d['data']` |
| account_009 | z-real-time | z-real-time-linkedin-scraper-api1.p.rapidapi.com | ✅ ~42次 | company/people 列表 |
| account_013 | ugoBoy/karimgreek（新key）| linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com | ✅ ~49次剩余 | /profile/detail?username={slug} |
| account_008 | enrichgraph | real-time-linkedin-data-scraper-api.p.rapidapi.com | ✅ 可用（2026-04-05验证，首选） | `d['education'][].degree / date_range` |
| account_011 | enrichgraph（新key） | real-time-linkedin-data-scraper-api.p.rapidapi.com | 待测试 | 同 account_008 |
| account_003/005/010 | 各家 | - | ❌ 月额度耗尽 | - |

**account_012 爬取示例：**
```bash
curl -s "https://linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com/profile/detail?username={slug}" \
  -H "x-rapidapi-host: linkedin-scraper-api-real-time-fast-affordable.p.rapidapi.com" \
  -H "x-rapidapi-key: {API_KEY_012}"
```

**响应结构（account_012）：**
```python
d['data']['basic_info']           # fullname, headline, location{full, city, country}
d['data']['experience'][i]        # title, company, start_date{year, month}, end_date, is_current
d['data']['education'][i]         # school, degree, field_of_study, start_date, end_date
```

---

### 15.4 核实渠道优先级

| 优先级 | 渠道 | 适用场景 | 命中率 |
|--------|------|---------|--------|
| ★★★★★ | **OpenReview** `openreview.net/profile?id=~{名}_{姓}1` | 论文作者核实 @anthropic.com | 极高 |
| ★★★★ | **LinkedIn People Also Viewed**（浏览器人工操作） | 主动扩展同公司员工 | 高 |
| ★★★★ | **RocketReach** | 补充工作经历 | 高 |
| ★★★ | **Google Scholar** | 学术背景 + 邮箱域名验证 | 中高 |
| ★★★ | **arXiv abs 页面** | 批量发现 Anthropic 论文作者 | 中 |
| ★★ | **Google 搜索** `{姓名} Anthropic LinkedIn` | 找 LinkedIn slug | 中 |

**关键教训：**
- arXiv `citation_author` 格式是 `"Last, First"`（如 `Bai, Yuntao`），取姓必须用逗号前部分
- Semantic Scholar 作者名不可信（截断/误拼），以 arXiv abs 原文为准
- LinkedIn「People Also Viewed」只在浏览器可见，API 不返回
- **格式化须用 LLM（模型）逐人翻译整理，不要用低智能脚本**——脚本无法翻译学校名/学位名
- ⚠️ **中文名零容忍原则**：中文名只能填有可靠来源的（LinkedIn/个人主页/媒体），**绝对禁止**音译、意译、占位填写。"先填后改"心理是危险陷阱——一旦写入即成"历史数据"，容易被遗忘。历史错误：Bobby Chen→陈中博、Calvin Liu-Navarro→刘长远、Ashley Wang→王、Jo Zhu Kennedy→朱乔·肯尼迪 等均为无来源推断，已批量清除。详见 §19。

---

---

## 十七、BrightData API（备用，按需询问）

> **定位：备用渠道，不纳入常规流程。使用前必须询问用户是否启用。**

### 17.1 能力与局限（2026-04-02 实测）

| 能力 | 评估 |
|------|------|
| 突破私密 Profile（如 Jo Zhu Kennedy）| ✅ 可访问，其他 API 全部 404 |
| 当前公司、城市、姓名 | ✅ 准确 |
| People Also Viewed（10-20条） | ⚠️ **返回结果不含 Anthropic 在职成员，实用价值低** |
| 工作经历详情（职位/时间） | ❌ 完全缺失 |
| 教育详情（学位/专业/年份） | ❌ 仅机构名，无学位/专业/年份 |
| 响应速度 | ⚠️ 异步，约 100 秒 |

**结论：BrightData 主要价值在于突破私密 Profile 的基本信息确认（姓名+公司），不适合补全 Excel 详细字段。**

### 17.2 使用时机（需询问用户）

遇到以下情况时，**主动询问用户是否使用 BrightData**：

```
❓ 候选人 LinkedIn 被其他 API 返回 404（私密 Profile）
   → 询问：「此人 Profile 为私密，是否用 BrightData 尝试获取基本信息（姓名/公司确认）？」

❓ 需要批量验证一批人的当前公司（而非详细字段）
   → 询问：「是否用 BrightData 批量核实这 N 人的在职状态？」
```

**不适合使用的情况（无需询问，直接用其他方案）：**
- 需要补全教育/工作详情 → 等 account_012 月初重置
- 需要 People Also Viewed 扩展新人 → 实测结果不含 Anthropic 成员，价值低

### 17.3 API 使用方法

**账号信息：**
```
Bearer Token: 8f18d2a6-97e2-4c24-9fa8-3afdfca6c148
Dataset ID:   gd_l1viktl72bvl7bjuj0
计费模式:     按记录计费（非按月订阅）
```

**Step 1 — 触发采集（异步）：**
```bash
curl -s -X POST \
  "https://api.brightdata.com/datasets/v3/trigger?dataset_id=gd_l1viktl72bvl7bjuj0&include_errors=true" \
  -H "Authorization: Bearer 8f18d2a6-97e2-4c24-9fa8-3afdfca6c148" \
  -H "Content-Type: application/json" \
  -d '[{"url":"https://www.linkedin.com/in/{slug1}/"},{"url":"https://www.linkedin.com/in/{slug2}/"}]'
# 返回：{"snapshot_id": "sd_xxxxx"}
```

**Step 2 — 轮询进度（约 60-120 秒）：**
```bash
SNAPSHOT_ID="sd_xxxxx"
# 每 10 秒轮询一次，status=ready 时取结果
curl -s "https://api.brightdata.com/datasets/v3/progress/${SNAPSHOT_ID}" \
  -H "Authorization: Bearer 8f18d2a6-97e2-4c24-9fa8-3afdfca6c148"
```

**Step 3 — 取结果并立即存档：**
```bash
curl -s "https://api.brightdata.com/datasets/v3/snapshot/${SNAPSHOT_ID}?format=json" \
  -H "Authorization: Bearer 8f18d2a6-97e2-4c24-9fa8-3afdfca6c148" \
  | tee "data/profiles/anthropic_employees/{slug}_brightdata.json" \   # ← 立即落盘
  | python3 -c "import json,sys; records=json.load(sys.stdin); ..."
```

**返回结构（顶层字段）：**
```python
r['name']                    # 姓名
r['city']                    # 城市
r['country_code']            # 国家
r['current_company_name']    # 当前公司名
r['current_company_company_id']  # 公司 LinkedIn ID（如 "anthropicresearch"）
r['followers'] / r['connections']
r['educations_details']      # 教育机构名（纯文字，如 "Stanford GSB"）
r['education']               # 列表，含 title/start_year/end_year，但无 degree/field
r['people_also_viewed']      # 10-20条，含 name/about/profile_link（实测不含 Anthropic 成员）
r['recommendations']         # 推荐信文字（部分人有）
r['certifications']          # 认证/证书（部分人有）
```

### 17.4 实测存档

测试文件位置：`data/profiles/test/`

| 文件 | 内容 |
|------|------|
| `brightdata_snapshot_sd_mnhj61988tirhymb5.json` | 原始快照（3人合一） |
| `guangfeng-he_brightdata.json` | Guangfeng He（education字段为空，USC仅在educations_details） |
| `jozhukennedy_brightdata.json` | Jo Zhu Kennedy（私密Profile，BrightData可访问，Stanford GSB确认） |
| `shijie-wu-1999b7a0_brightdata.json` | Shijie Wu（JHU确认，无学位/专业详情） |

---

---

## 十八、arXiv 论文扫描规范与可审计性要求

### 18.1 设计原则

所有论文扫描工作必须做到**完全可审计**：每篇论文的处理结果、华人候选人、核查结论均持久化到文件，不依赖对话 context 或记忆。

### 18.2 数据目录结构

```
data/arxiv_scan/
├── README.md              # 使用说明与字段定义
├── arxiv_scan_db.json     # 主数据库（机器可读，逐篇记录）
└── arxiv_scan_index.csv   # 汇总索引（人工可审查）
```

### 18.3 主数据库结构（arxiv_scan_db.json）

```json
{
  "_meta": { "total_papers": 124, "scanned_count": N, "pending_count": M },
  "scanned": {
    "2XXXXXXX": {
      "slug": "论文官网slug",
      "title": "论文完整标题",
      "authors_raw": "原始作者列表",
      "chinese_candidates": ["候选华人姓名"],
      "result": "known|new_added|excluded|no_chinese|pending",
      "note": "处理说明（包含排除理由或已添加的行号）",
      "scan_date": "YYYY-MM-DD"
    }
  },
  "pending": {
    "2XXXXXXX": { "slug": "...", "added_to_db": "YYYY-MM-DD" }
  }
}
```

**result 值含义：**
| result | 含义 |
|--------|------|
| `known` | 华人候选人已在 Excel 表中（无需新增） |
| `new_added` | 新发现并已添加到 Excel（注明行号和版本） |
| `excluded` | 排除：非 Anthropic 成员 / 非华人 / 论文非 Anthropic 自有 |
| `no_chinese` | 已扫描，无华人候选 |
| `pending` | 发现候选人，尚未完成核查 |
| `pending_scan` | 论文尚未扫描 |

### 18.4 工作流程

每次扫描一篇论文后：

1. **立即**在 `arxiv_scan_db.json` 的 `scanned` 下添加记录
2. 从 `pending` 下删除对应记录
3. 更新 `_meta.scanned_count` 和 `_meta.pending_count`
4. 批量扫描结束后重新生成 CSV：
   ```python
   # 重新生成 arxiv_scan_index.csv（见 README.md 中的脚本）
   ```
5. git commit 包含数据库更新

### 18.5 论文来源说明

- **论文列表来源**：`anthropic.com/sitemap.xml` → research 页面（107 篇）→ 提取唯一 arXiv ID
- 注意：sitemap 中含有部分**误纳入的非 Anthropic 论文**（OpenAI、DeepSeek、学术机构等），扫描时标记为 `excluded`
- 对于明确的 Anthropic 自有论文，判断依据是：核心作者含 Dario/Jared/Amanda 等创始人，或作者署名邮箱为 @anthropic.com

### 18.6 华人判断标准

参见第十三节「华人判定注意事项」。对于论文中的华人候选人：
- 必须通过 LinkedIn 或 OpenReview 核查当前工作单位
- 不能仅凭论文署名判断（历史合作作者可能已换工作）
- 核查前先在 Excel 在职/离职两表中搜索，避免重复添加

---

## 十九、中文名字段强制规范（防幻觉）

### 19.1 核心原则：不确定绝对不填

```
❌ 错误做法：根据英文名音译/意译推断中文名（如 Danny → 林丹尼，Ashley → 王）
❌ 错误做法：填"占位中文名"待后续补充
❌ 错误做法：根据"常见对应"猜测（如 Bobby → 陈中博）
✅ 正确做法：只有当中文名来自以下可靠来源时才填写：
   - LinkedIn 个人资料中的显示名称
   - 个人主页/学术主页明确标注
   - 媒体报道/官方简介
   - 论文署名或 OpenReview 账户
   - 用户直接告知
```

### 19.2 中文名可靠性分级

| 可靠性 | 来源 | 操作 |
|--------|------|------|
| ✅ 高 | LinkedIn 本人填写 / 个人主页 / 媒体确认 | 填写 |
| ⚠️ 中 | 推断但逻辑合理（如 Hanwen Shen 对应 沈瀚文，中间名就是中文名拼音） | 待用户确认后填写 |
| ❌ 低 | 仅凭英文名音译（Danny→丹尼）或姓氏推断（Ashley Wang→王） | **不填** |

### 19.3 历史错误案例（已清除，警示留存）

以下中文名曾被错误填入，均属无来源推断，已在 v6zp 中清除：

| 人名 | 错误中文名 | 错误原因 |
|------|-----------|---------|
| Jo Zhu Kennedy | 朱乔·肯尼迪 | 中英混排，非真实中文名 |
| Christina Lu | 陆克里斯蒂娜 | 音译，ABC 无中文名 |
| Calvin Liu-Navarro | 刘长远 | 推断，无来源 |
| Bobby Chen | 陈中博 | 推断，无来源 |
| Yanda Chen | 陈延达 | 推断，搜索无命中 |
| Miranda Zhang | 张明达 | 推断，无来源 |
| Chao Chen | 陈超 | 推断，常见名不等于此人名 |
| Jerry Hong | 洪杰瑞 | 音译 |
| Danny Lin | 林丹尼 | 音译 |
| Ashley Wang | 王 | 占位，不可接受 |

### 19.4 华裔判定范围

**纳入范围（只要具有华裔血统）：**
- 大陆华人（最常见）
- 台湾裔（如台湾本科/高中背景）
- 香港人
- 东南亚华裔
- ABC（American-Born Chinese，美国出生华裔）
- 其他华裔美国人/加拿大人

**不纳入：**
- 仅有华人姓氏但非华裔（需结合背景判断）
- 有疑问时与用户确认，不自行排除

---

---

## 二十、PROGRESS.md 可审计强制规范

> **核心原则：每次执行前必读，每次执行后必更新。路径可审计是项目可持续运行的基础。**

### 20.1 PROGRESS.md 定位

`PROGRESS.md` 是项目的**状态机文件**，记录当前 Excel 版本、人员数量、待办事项、关键 identifiers。它承担两个关键功能：

1. **会话重启恢复**：上下文压缩/对话重启后，从 PROGRESS.md 而非 summary 恢复状态
2. **可审计性**：任何时间点都能回溯"当时做了什么、当前状态是什么"

### 20.2 强制执行规则

```
✅ 每次执行任何操作前：先读 PROGRESS.md，确认当前最新 Excel 版本和人员数量
✅ 每次执行操作后：立即更新 PROGRESS.md（Excel 版本/行数/人数/TODO 变化）
✅ 新增人员：更新 Excel 行数 + 人数
✅ 新 Excel 版本：更新"当前最新 Excel"字段 + 旧版本保留记录
✅ 完成 TODO：移动到"已完成"区域，标注完成时间和结果
❌ 不在 PROGRESS.md 中记录计划——计划写在 TODO 节，完成后移入已完成
❌ 不用 summary 或对话 context 传递状态——所有状态持久化到文件
```

### 20.3 PROGRESS.md 更新时机（完整清单）

| 操作 | 需要更新的字段 |
|------|----------------|
| 新增一人到 Excel | Excel 行数、实际人数、当前最新版本号 |
| 保存新版 Excel（v6zx → v6zy） | 当前最新 Excel 路径、旧版归档记录 |
| 完成一批 arXiv 扫描 | arXiv 扫描进度（已扫/总数/新发现） |
| 爬取新 Profile JSON | 数据资产目录新增记录 |
| API 账号状态变化（额度耗尽/新key） | API 账号表 |
| 发现并排除一个候选人 | 已排除列表（防止重复调查） |
| 完成某个 TODO | 移入已完成，标注时间 |
| git commit | 关键 commit 哈希记录 |

### 20.4 PROGRESS.md 与其他文件的关系

```
PROGRESS.md       → 项目状态机（当前版本/人数/TODO）
memory/YYYY-MM-DD → 当日工作原始记录（细节、决策过程、踩坑）
SKILL.md          → 方法论与操作规范（不含项目状态）
arxiv_scan_db.json → 论文扫描可审计数据库
api_accounts.json  → API 账号配置（不含状态——状态在 PROGRESS.md）
```

---

## 二十一、官方 Publication 渠道完整覆盖规范

> **核心原则：Anthropic 官方发布的所有 Research 和 Engineering 性质文章，均需系统扫描 Authors + Acknowledgements，不得遗漏任何渠道。**

### 21.1 必须覆盖的官方渠道

| 渠道 | URL | 覆盖目标 | 数据库文件 |
|------|-----|---------|-----------|
| **arXiv 论文** | 通过 sitemap 获取 Anthropic 自有论文ID | 论文作者（author列表）+ 致谢 | `data/arxiv_scan/arxiv_scan_db.json` |
| **Research 页面** | `anthropic.com/research` | 论文作者 + 致谢 | 同上 |
| **Engineering 博客** | `anthropic.com/engineering` | 文章作者（Written by）+ 致谢（Acknowledgements）| `data/engineering_scan/engineering_scan_db.json` |
| **Claude 产品博客** | `claude.com/blog` | 文章作者 + 致谢（偏产品/工程方向）| `data/claude_blog_scan/claude_blog_scan_db.json` |
| **News / 公告** | `anthropic.com/news` | 如有署名作者或致谢 | 按需扩展 |

> **泛化提示（用于其他公司）**：将上述渠道替换为目标公司对应的官网博客/技术blog/产品博客/arxiv机构账号即可复用。注意同一公司可能有多个博客站点（如 Anthropic 的 `anthropic.com/engineering` 和 `claude.com/blog` 定位不同但均须覆盖）。不要硬编码 Anthropic 特有路径。

> **关于 claude.com/blog 的特点（已知经验）**：面向企业/开发者用户，内容偏产品实践和 Best Practice；文章多以团队/产品名义发布，较少个人署名，Acknowledgements 少见；97篇全量扫描未发现华人候选人（2026-04-03）。与 engineering 博客互补。

### 21.2 Engineering 博客扫描标准流程

```
Step 1: 抓取 /engineering 页面，提取所有文章URL列表
Step 2: 逐篇抓取 HTML，提取：
          - "Written by {Name}" → authors 字段
          - "Acknowledgements" 段落 → acknowledgements 字段
Step 3: 对所有人名做华人姓氏过滤（见§13华人判定规范）
Step 4: Google 搜索确认候选人 LinkedIn 状态
Step 5: 用 account_013 爬取 profile 精细化
Step 6: 写入 Excel + 更新 engineering_scan_db.json
Step 7: git commit
```

### 21.3 可审计数据库字段（engineering_scan_db.json）

```json
{
  "meta": {
    "source": "anthropic.com/engineering",
    "total": 21,
    "scanned": 21,
    "pending": 0,
    "new_members_found": ["人名列表"],
    "last_updated": "YYYY-MM-DD"
  },
  "articles": [
    {
      "url": "完整URL",
      "title": "文章标题",
      "published": "发布日期",
      "scan_status": "done/pending/error",
      "authors": ["Written by 的人名"],
      "acknowledgements": ["致谢人名"],
      "chinese_candidates": [
        {"name": "姓名", "role": "authors/acknowledgements", "status": "known/new→confirmed/待核实/excluded", "notes": "备注"}
      ],
      "new_members_found": ["实际新增到Excel的人名"],
      "scan_date": "YYYY-MM-DD"
    }
  ]
}
```

### 21.4 致谢提取注意事项

- **⚠️ 大文件禁止直接 web_fetch**：arXiv 论文 HTML 全文可达数 MB，**绝对不要**通过 `web_fetch` 灌入对话上下文。正确做法：
  1. 先用 `curl -s --max-time 30 https://arxiv.org/html/{id} -o data/arxiv_scan/html_raw/{id}.html` 下载到本地
  2. 检查文件大小：`wc -c < html_raw/{id}.html`，若 < 5KB 说明无 HTML 版本，需下载 PDF
  3. 从本地文件用 Python 脚本提取 ACK 段落（正则匹配 `Acknowledg` 到 `References` 之间的文本）
  4. ACK 通常在文件末尾，可只读最后 200KB：`tail -c 200000 html_raw/{id}.html | grep -A50 -i 'acknowledg'`
- **PDF 回退方案**：若无 HTML 版本，`curl -L -o /tmp/{id}.pdf https://arxiv.org/pdf/{id}`，然后 `pdftotext /tmp/{id}.pdf -` 提取全文，再搜索 ACK 段落。若 `pdftotext` 报错（Syntax Error），可能是 PDF 下载不完整，加 `--max-time 60` 重试。
- **非华裔过滤**：Maggie Vo（越南裔）、Prithvi Rajasekaran（印度裔）等不纳入，但要记录在 `chinese_candidates` 的 `status: excluded`。
- **Bin Wu / Sam Jiang 类**：华人常见名，若 LinkedIn slug 无法确认身份，标注 `status: 待核实`，不强行纳入。

### 21.5 定期更新机制

- **每月至少检查一次** `/engineering` 列表页，比对新增文章
- 新增文章按照 21.2 流程扫描
- 更新 `meta.total` + `meta.scanned` + `meta.last_updated`
- git commit 记录变更

### 21.6 20.3 PROGRESS.md 需新增更新时机

| 操作 | 需要更新的字段 |
|------|----------------|
| 完成一批 engineering 博文扫描 | engineering 扫描进度（已扫/总数/新发现） |
| 新增 engineering 博文 | `meta.total` + 新 pending 条目 |

---

## §22 Publication 数据库架构

### 22.1 统一视图 `publications_unified.json`

所有 Anthropic 公开发表内容（arXiv 论文、Engineering 博客、Claude 博客）统一索引在 `data/publications_unified.json`。

**键格式：** `{source}:{id}`
- `arxiv:2501.18837` — arXiv 论文
- `eng:infrastructure-noise` — Engineering 博客
- `claude:1m-context` — Claude 博客

**标准字段（所有源统一）：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 源内唯一 ID（arXiv ID / blog slug） |
| `source` | string | `arxiv` / `engineering_blog` / `claude_blog` |
| `url` | string | 原文链接 |
| `title` | string | 标题 |
| `is_anthropic_paper` | bool | 是否 Anthropic 自有（外部论文被 sitemap 收录的=false） |
| `paper_type` | string | `core` / `collaborative` / `external` / `misclassified` / `blog` |
| `authors_list` | string[] / null | 完整结构化作者列表（null=待补全） |
| `authors_raw` | string | 原始作者字符串（可能截断） |
| `authors_truncated` | bool | `authors_raw` 是否被截断 |
| `ack.scanned` | bool | 是否已扫描致谢 |
| `ack.found` | bool | 是否有致谢段落 |
| `ack.text` | string / null | 致谢原文 |
| `ack.names` | string[] | 致谢中提到的所有人名 |
| `ack.chinese_names` | string[] | 致谢中的华人候选 |
| `chinese_candidates` | string[] | 作者中的华人候选 |
| `result` | string | 扫描结论 |

### 22.2 arXiv 扫描数据库 `arxiv_scan_db.json`（Schema V2）

arXiv 论文的权威数据库。V2 于 2026-04-05 从 V1 迁移。

**V1 → V2 关键变更：**
1. ACK 数据从分离的 `ack_summary.json` **合并到每篇论文的 `ack` 子对象**
2. 新增 `is_anthropic_paper`（布尔）+ `paper_type`（枚举）
3. 新增 `year` / `month`（从 arXiv ID 提取）
4. 新增 `authors_list`（完整作者数组，V2 迁移时全部为 null，需批量补全）
5. 新增 `html_downloaded` / `html_path`（本地 HTML 缓存状态）

**V1 备份：** `arxiv_scan_db_v1_backup.json`
**Schema 文档：** `data/arxiv_scan/SCHEMA_V2.md`

### 22.3 authors_list 批量补全方法

**数据源：** arXiv abs 页面的 `<meta name="citation_author">` 标签

```bash
# 单篇提取示例
curl -s "https://arxiv.org/abs/2501.18837" | grep 'citation_author' | sed 's/.*content="//;s/".*//'
```

**批量流程：**
1. 从 `arxiv_scan_db.json` 提取所有 `authors_list == null` 的论文 ID
2. 逐篇 curl（间隔 2-3 秒避免限流）下载到临时文件
3. 用 grep/Python 提取 `citation_author` 标签
4. 写回 `authors_list` 字段 + 更新 `author_count`
5. 同步更新 `publications_unified.json`

**注意：** arXiv abs 页面较小（~50KB），可直接 curl 不会占满上下文。但 **HTML 全文（/html/{id}）可达数 MB，必须先下载到本地**（见 §21.4）。

### 22.4 数据一致性维护

**写入规则：**
- 新发现论文 → 同时写入 `arxiv_scan_db.json` 和 `publications_unified.json`
- 修改现有记录 → 以 `arxiv_scan_db.json` 为权威源，定期重建 unified 视图
- Engineering / Claude 博客 → 以各自的 `*_scan_db.json` 为权威源

**定期重建 unified 视图：**
```python
# 当源数据库有变更时执行
python3 rebuild_unified.py  # 或内联脚本
```

### 22.5 数据库版本管理规范

所有数据库文件修改必须遵循版本管理规范，确保可审计性：

| 操作类型 | 规范 |
|---------|------|
| Schema 升级（字段增删） | 备份旧文件为 `*_v{N}_backup.json`；更新 `_meta.schema_version` |
| 数据批量修改 | 记录 `_meta.last_updated` 日期；写入修改说明到 `_meta.changelog` |
| 新增论文 | 同时写入 `arxiv_scan_db.json` 和 `publications_unified.json` |
| 权威源冲突 | `arxiv_scan_db.json` > `publications_unified.json`，unified 视图定期从源重建 |

---

## §23 进度追踪规范

### 23.1 PROGRESS.md 更新时机（强制）

PROGRESS.md 是跨会话状态恢复的**唯一可信来源**，必须在以下时机立即更新：

| 事件 | 需更新内容 |
|------|-----------|
| 写入新成员到 Excel | 版本号、人数、新成员姓名 |
| 完成一个扫描渠道 | 渠道进度百分比 |
| 发现新方法/踩坑 | 写入方法论或踩坑表（Skill），PROGRESS.md 更新对应状态 |
| 数据库 Schema 升级 | 数据库状态小节 |
| Session 结束前 | 完整 open TODOs 列表 |

**严禁依赖对话 context 或 summary 传递状态。所有状态必须持久化到文件。**

### 23.2 PROGRESS.md 结构模板

```markdown
# Anthropic 华人专项调研 - PROGRESS

## 快速状态
- Excel 最新版本: v6zz7（在职90人，离职8人）
- 最后更新: 2026-04-05

## 扫描渠道进度
| 渠道 | 进度 | 新发现 |
|------|------|--------|
| LinkedIn API (company/people) | 100% | 78人 |
| arXiv 论文作者 | 129/129 | 12人 |
| arXiv ACK 致谢 | 12/52 | 3人 |
| Scholar @anthropic.com | 进行中 | 8人 |
| Engineering Blog | 21/21 | 4人 |
| Claude Blog | 98/98 | 0人 |

## 数据库状态
- arxiv_scan_db.json: Schema V2，52篇 Anthropic 自有论文 authors_list 100% 补全
- publications_unified.json: 248 publications，74篇有完整 authors_list
- chinese_filter/: all_members(100), confirmed_cjk(49), excluded(4), suspected(1)

## Open TODOs
1. ...

## 已排除（不计入 Excel）
- ...
```

### 23.3 Excel 版本命名规范

- 格式：`Anthropic华人专项调研_v{base}{suffix}.xlsx`
- base 固定为 `6z`，suffix 每次递增（a→z→za→zb→...→zz→zz0→...→zz9→zza→...）
- **每次写入新数据都要版本递增**，绝不覆盖原版本
- 当前最新：`v6zz7`

---

## §24 面向 Agent 产品化的设计原则

### 24.1 Sourcing Criteria 可迭代

每个 Sourcing 任务的核心参数都应该可配置（见 §0.4），不硬编码。扩展新目标时：
1. 修改 `target_company` + `company_slug`
2. 调整 `criteria.ethnicity` / `confidence_layer`
3. 数据管道（扫描 → 过滤 → 验证 → 输出）保持不变

### 24.2 多层过滤分离

Layer 0-3 的分类体系（见 §8）必须与 Excel 输出分离：
- **本地数据库**（`chinese_filter/`）：存储所有候选人的四层分类，是过滤状态的权威记录
- **Excel 输出**：只输出 Layer 1-2（高置信华人），供人类使用
- 两者分离，使得 criteria 调整后可以**从数据库重新生成 Excel**，而不是人工修改

### 24.3 扫描渠道标准化接口

每个扫描渠道（LinkedIn API / arXiv / Scholar / Blog）应封装为标准接口：

```python
class ScanChannel:
    def scan(self, target: SourcingTarget) -> List[Candidate]
    def get_progress(self) -> ScanProgress
    def is_complete(self) -> bool
```

这样未来 Agent 可以并行调度多个渠道，并在 PROGRESS.md / DB 中统一记录进度。

### 24.4 人工审核节点设计

**不是所有步骤都适合完全自动化。** 以下节点必须保留人工确认：
- 华人候选人名单（Layer 1 → Layer 2 的判断）
- 调用 LinkedIn Profile API（消耗额度，不可逆）
- 写入 Excel 新行（最终输出，错误代价高）

Agent 产品设计时，这些节点应触发 `await_human_approval()` 而不是自动执行。

### 24.5 数据资产积累意识

每次执行任务时积累的数据（API 返回、HTML 缓存、论文作者列表）不是一次性消耗品，而是持续积累的资产：
- **LinkedIn profiles** → `data/profiles/anthropic_employees/` 永久存档
- **arXiv HTML** → `data/arxiv_scan/html_raw/` 永久缓存（避免重复 curl）
- **ACK 文本** → `ack_summary.json` + `arxiv_scan_db.json` 双重存储
- **Scholar IDs** → `anthropic_scholar_ids.json` 持续积累

**原则：花过的钱（API 额度）和做过的工（curl 下载）不应该做第二次。**

---

---

## §25 任务进度数据库通用规范

> **核心原则：任何需要多次会话、多轮查询、跨断点执行的任务，都必须有专属的进度数据库文件，而不是依赖对话 context。**

### 25.1 什么情况必须建进度数据库

以下任何一个条件满足，就必须在启动任务前先建 DB：

| 条件 | 典型场景 |
|------|---------|
| 涉及批量查询（≥5 次 API/web_fetch 调用） | Scholar coauthor 扫描、LinkedIn batch profile 爬取 |
| 任务可能跨越多个会话/对话 | 任何需要"下次继续"的任务 |
| 每次查询消耗不可逆资源（API 额度、时间） | LinkedIn/Scholar/BrightData API 调用 |
| 结果需要去重（同一人可能被多次发现） | 多种子 coauthor 图谱、多渠道扫描 |
| 需要断点续传（SIGTERM 风险） | 大批量 curl/爬取脚本 |

**只要满足上述任一条，就建数据库——成本极低（一个 JSON 文件），但能避免大量重复工作和资源浪费。**

### 25.2 进度数据库标准架构

每个进度数据库 JSON 文件必须包含以下四个顶层模块：

```json
{
  "_meta": { ... },        // 元信息：版本、创建时间、统计摘要
  "progress": { ... },     // 进度状态：每个工作单元的状态
  "results": { ... },      // 实际数据：查询结果持久化
  "queue": { ... }         // 待处理队列（断点续传用）
}
```

#### 必须字段（`_meta`）

| 字段 | 类型 | 说明 |
|------|------|------|
| `schema_version` | string | 如 `"1.0"` |
| `created_at` | string | `"YYYY-MM-DD"` |
| `last_updated` | string | `"YYYY-MM-DD"` 每次写入后更新 |
| `task_name` | string | 任务可读名称 |
| `stats` | object | 实时统计（total/done/pending/error） |

#### 必须字段（每个工作单元 / `progress` 条目）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | string | 唯一标识符（Scholar ID / LinkedIn slug / paper ID 等）|
| `status` | enum | `"pending"` / `"done"` / `"error"` / `"skipped"` |
| `scan_date` | string\|null | `"YYYY-MM-DD"`，null=未执行 |
| `error_msg` | string\|null | 出错时记录原因 |

### 25.3 进度数据库文件命名与位置规范

```
Anthropic华人专项/data/
├── arxiv_scan/
│   └── arxiv_scan_db.json              ← arXiv 论文扫描进度
├── engineering_scan/
│   └── engineering_scan_db.json        ← Engineering Blog 扫描进度
├── claude_blog_scan/
│   └── claude_blog_scan_db.json        ← Claude Blog 扫描进度
├── scholar_coauthor/
│   ├── coauthor_graph_db.json          ← Scholar 图谱扩展进度（主 DB）
│   ├── seed_authors.json               ← 种子节点定义
│   ├── candidate_queue.json            ← 断点续传队列
│   └── SCHEMA.md                       ← 字段定义文档
└── {future_task}/
    ├── {task}_db.json                  ← 主进度数据库
    └── SCHEMA.md                       ← 必须有 schema 文档
```

**命名规范：**
- 主数据库：`{task_name}_db.json`
- Schema 文档：`SCHEMA.md`（同目录）
- 队列/辅助文件：语义化命名，如 `candidate_queue.json`、`seed_authors.json`

### 25.4 操作规则（强制执行）

```
① 任务启动前：
   - 检查 DB 是否存在 → 不存在则先建立 DB 和 SCHEMA.md
   - 读取 stats 确认当前进度
   - 从 queue 中取待处理项目

② 每次单元完成后（立即，不攒批次）：
   - 写入 results（原始数据）
   - 更新 progress 中该单元的 status/scan_date
   - 更新 _meta.stats
   - 若涉及资源消耗（API调用），必须先落盘再继续下一个

③ 任务中断/出错时：
   - 已完成单元状态已落盘 → 重启从 pending 继续
   - 出错单元标记 status=error + error_msg → 人工决定是否重试
   
④ 任务结束后：
   - 更新 PROGRESS.md 中对应任务的进度小节
   - git commit DB 文件
```

### 25.5 新任务建 DB 的快速模板

```python
import json, os
from datetime import datetime

def init_task_db(task_name: str, items: list, db_path: str):
    """为新任务初始化进度数据库"""
    db = {
        "_meta": {
            "schema_version": "1.0",
            "created_at": datetime.now().strftime("%Y-%m-%d"),
            "last_updated": datetime.now().strftime("%Y-%m-%d"),
            "task_name": task_name,
            "stats": {
                "total": len(items),
                "pending": len(items),
                "done": 0,
                "error": 0,
                "skipped": 0
            }
        },
        "progress": {
            item["id"]: {
                "id": item["id"],
                "label": item.get("label", item["id"]),
                "status": "pending",
                "scan_date": None,
                "error_msg": None
            }
            for item in items
        },
        "results": {},
        "queue": [item["id"] for item in items]
    }
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, "w") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    print(f"✅ DB 初始化完成：{db_path}（{len(items)} 个待处理项）")

def update_progress(db_path: str, item_id: str, status: str,
                    result: dict = None, error_msg: str = None):
    """原子更新单个工作单元进度"""
    with open(db_path) as f:
        db = json.load(f)
    # 更新 progress
    db["progress"][item_id]["status"] = status
    db["progress"][item_id]["scan_date"] = datetime.now().strftime("%Y-%m-%d")
    db["progress"][item_id]["error_msg"] = error_msg
    # 写入 result
    if result:
        db["results"][item_id] = result
    # 更新 queue（移出已完成）
    if item_id in db["queue"]:
        db["queue"].remove(item_id)
    # 重算 stats
    statuses = [v["status"] for v in db["progress"].values()]
    db["_meta"]["stats"] = {
        "total": len(statuses),
        "pending": statuses.count("pending"),
        "done": statuses.count("done"),
        "error": statuses.count("error"),
        "skipped": statuses.count("skipped")
    }
    db["_meta"]["last_updated"] = datetime.now().strftime("%Y-%m-%d")
    with open(db_path, "w") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
```

### 25.6 PROGRESS.md 中的进度小节模板

每个进度数据库在 PROGRESS.md 中必须有对应小节：

```markdown
### {任务名称}数据库（data/{task}/{task}_db.json）

| 指标 | 数值 |
|------|------|
| 总工作单元 | N |
| 已完成 | X |
| 待处理 | Y |
| 失败/跳过 | Z |
| 最后更新 | YYYY-MM-DD |

**Schema 文档：** `data/{task}/SCHEMA.md`
```

---

## §26 Scholar Coauthor 图谱扩展

### 26.1 任务目标

通过已知 Anthropic 华人员工的 Google Scholar 主页，系统性扩展发现可能的新华人成员。核心逻辑：**Anthropic 华人员工与其他华人的共著概率高于随机**（学术圈华人社区效应）。

### 26.2 数据库位置与结构

```
data/scholar_coauthor/
├── SCHEMA.md                   ← 完整字段定义（权威文档）
├── coauthor_graph_db.json      ← 主数据库（进度 + 图谱数据）
├── seed_authors.json           ← 种子节点（11位已知华人的 Scholar ID）
└── candidate_queue.json        ← 断点续传队列
```

**种子节点（11人，2026-04-05 建立）：**
| Scholar ID | 姓名 | Excel Row |
|-----------|------|-----------|
| Y4sk3aMAAAAJ | Jerry Wei | - |
| ZvMMbt8AAAAJ | Christina Lu | 75 |
| x4JAvwMAAAAJ | Jeffrey Wu | 80 |
| 4Zw1PJ8AAAAJ | Kaidi Cao | 85 |
| hKESpIUAAAAJ | Zhao Meng | 86 |
| jVRL96IAAAAJ | Jiaxin Wen | 87 |
| iY2LhwEAAAAJ | Yanda Chen | 69 |
| 1aL6moAAAAAJ | Saffron Huang | 88 |
| Y4bU0bwAAAAJ | Rowan Wang | 91 |
| S63gb38AAAAJ | Andi Peng | 已离职 |
| c5PO2pgAAAAJ | Alwin Peng | 89 |

### 26.3 执行流程（四阶段）

#### 阶段一：种子扫描（抓 coauthor 列表）

```
对每个 scan_status=pending 的种子：
1. web_fetch https://scholar.google.com/citations?user={id}&sortby=pubdate
   → 提取 coauthor 侧边栏（"co-authors" section）
2. 解析每个 coauthor 的：name / scholar_id（从 URL 的 user= 参数）/ affiliation
3. 写入 coauthor_graph_db.json 的 coauthors 节点
   - 已存在：追加 co_with 列表（去重）
   - 不存在：新建节点，review_status=pending
4. 更新种子 scan_status=done + coauthor_count
5. 立即保存 DB（原子操作）
```

**⚠️ 防重复：每次新建 coauthor 节点前，先检查 scholar_id 是否已在 `coauthors` 中存在。**

#### 阶段二：华人候选判定

```
对 chinese_layer=null 的 coauthor 节点：
1. 检查姓名特征（华人姓氏拼音）→ Layer 1
2. 查 Scholar 主页的 affiliation（机构名）
3. 若机构含中文大学名或已知华人背景信号 → Layer 2
4. 更新 chinese_layer + chinese_evidence
5. Layer 2+ → 加入 candidate_queue 的 coauthors_to_review
```

#### 阶段三：Anthropic 状态核实

```
对 chinese_layer ≥ 2 且 review_status=pending 的 coauthor：
1. 先查 Excel 所有 sheet（防止重复添加）
2. 查 LinkedIn："{姓名} Anthropic LinkedIn"
3. 查 Scholar 主页 verified_email 字段（是否 @anthropic.com）
4. 更新 anthropic_status + review_status + anthropic_evidence
```

#### 阶段四：写入 Excel

```
对 anthropic_status=confirmed_anthropic 且 excel_row=null 的 coauthor：
1. 按 §8.1 格式规范填写所有字段
2. 写入 Excel 在职/离职对应 sheet
3. 更新 coauthor_graph_db.json 的 excel_row + review_date
4. 更新 PROGRESS.md
```

### 26.4 Scholar 主页抓取方法

```bash
# 抓取 Scholar 主页（含 coauthor 列表）
web_fetch "https://scholar.google.com/citations?user={scholar_id}&sortby=pubdate" maxChars=30000

# coauthor 信息通常在侧边栏，HTML 结构示例：
# <div id="gsc_rsb_co">  ← coauthor section
#   <a href="/citations?user={id}">姓名</a>
#   <span>机构名</span>
# </div>
```

**注意：Google Scholar 有反爬，web_fetch 可能返回空或验证页面。**  
回退策略：
1. 尝试不同的 `sortby` 参数（`pubdate` / `title` / `citations`）
2. 若多次 429，等待后重试（加入 `error` 状态，下次 session 继续）
3. **绝不用无头浏览器绕过验证**

### 26.5 防重复查询强制检查

```python
import json

def should_scan_seed(db_path: str, scholar_id: str) -> bool:
    """检查种子是否已扫描，防重复"""
    with open(db_path) as f:
        db = json.load(f)
    seed = db["seed_authors"].get(scholar_id, {})
    if seed.get("scan_status") == "done":
        print(f"⏭️ 跳过 {seed['name']}（已完成，{seed['scan_date']}）")
        return False
    return True

def coauthor_exists(db_path: str, scholar_id: str) -> bool:
    """检查 coauthor 是否已记录"""
    with open(db_path) as f:
        db = json.load(f)
    return scholar_id in db["coauthors"]
```

### 26.6 会话恢复（断点续传）

每次开始任务前：

```python
# 1. 读取队列，确认当前进度
with open("data/scholar_coauthor/candidate_queue.json") as f:
    queue = json.load(f)

with open("data/scholar_coauthor/coauthor_graph_db.json") as f:
    db = json.load(f)

print(f"种子待扫描: {len(queue['seeds_to_scan'])}")
print(f"候选人待 Review: {len(queue['coauthors_to_review'])}")
print(f"待爬 LinkedIn: {len(queue['linkedin_to_scrape'])}")
print(f"Stats: {db['_meta']['stats']}")
```

**永远从队列（queue）出发，绝不重新遍历所有种子。**

---

## §27 Scholar 扫描方法完整定义

> **核心原则：每种方法的边界必须清晰定义，防止混淆"直接扫描"与"图谱扩展"的范围和预期结果。**

### 27.1 方法全景图

本项目已使用或规划的 Google Scholar 相关扫描方法共三种，按发现深度递增排列：

```
方法 A: Scholar @anthropic.com 直接扫描
        ↓ 发现已知员工 Scholar ID
方法 B: Scholar Coauthor 图谱 — 一阶扩展（本项目已执行）
        ↓ 发现一阶共著者（直接合作过的人）
方法 C: Scholar Coauthor 图谱 — 二阶扩展（尚未执行）
        ↓ 发现二阶共著者（与一阶共著者合作过的人）
```

---

### 27.2 方法 A：Google Scholar @anthropic.com 直接扫描

**定义：**  
直接在 Google Scholar 搜索验证邮箱域名为 `@anthropic.com` 的学者，或通过已知员工 Scholar 主页逐一确认其 `Verified email at anthropic.com` 字段。

**数据源：**  
- Google Scholar 个人主页（`scholar.google.com/citations?user={id}`）
- 搜索入口：`site:scholar.google.com "anthropic.com"` 或从论文作者 Scholar ID 列表触发

**扫描对象（输入）：**  
已知 Anthropic 华人员工的 Scholar ID 列表，或通过论文作者提取的 Scholar ID

**输出：**  
- 确认在职（有 `@anthropic.com` 验证邮箱）→ 写入 Excel
- 确认学术背景（论文、研究方向）→ 补充 Excel 字段
- 发现但未验证的新候选人 → 进入 Layer 1 待核实队列

**发现范围：**  
仅限于**有 Scholar 主页且已设置 @anthropic.com 验证邮箱**的员工。未发布学术论文或不公开 Scholar 的员工无法通过此方法发现。

**本项目执行情况：**  
已完成（2026-04-05），从 Scholar 验证发现 Kaidi Cao / Zhao Meng / Yanda Chen / Jiaxin Wen / Rowan Wang 等，存档于 `data/anthropic_scholar_ids.json`。

**关键限制：**  
- Google Scholar 有反爬，频繁访问会触发 CAPTCHA
- 部分员工从未发布学术论文（工程/运营/法务岗），此方法覆盖率上限约为学术型员工
- `@anthropic.com` 邮箱不等于在职（离职员工邮箱有时仍保留显示）

---

### 27.3 方法 B：Scholar Coauthor 图谱 — 一阶扩展

**定义：**  
以已知 Anthropic 华人员工的 Google Scholar 主页为「种子节点」，提取每个种子主页侧边栏中显示的「Co-authors」列表，形成一跳（one-hop）关系图。一阶共著者 = 在 Scholar 主页侧边栏中被 Google 算法推荐显示的最频繁合作者（通常 8-20 人）。

**数据源：**  
`scholar.google.com/citations?user={种子ID}` 主页的右侧 Co-authors 区块

**一阶共著者的含义：**  
Google Scholar 自动计算并展示的共同发表论文最多的合作者。这不是该员工所有论文合著者的完整集合——而是 Scholar **算法筛选**后显示在侧边栏的子集（通常取共著频率最高的几位）。

**输入 → 输出关系：**
```
11 个种子（已知华人员工）
    ↓ 每人 Scholar 主页侧边栏
    ↓ 提取 Co-authors 区块（平均 8-12 人/种子）
52 个 coauthor 节点（2026-04-05 实际结果）
    ↓ 华人身份判定
28 人 华人 Layer 2+
    ↓ Anthropic 在职状态核实
1 人 新发现（Ruiqi Zhong，已离职）
```

**本项目执行情况：**  
11/11 种子全部扫描完成（2026-04-05），数据库存于 `data/scholar_coauthor/coauthor_graph_db.json`。

**关键限制：**  
- 侧边栏 coauthor 列表是**算法生成的子集**，不等于"所有论文合著者"。一个人可能跟某人合著过 5 篇论文但不出现在侧边栏；也可能偶合著 1 篇却出现（取决于 Scholar 算法权重）
- 学术发表少的员工（工程/产品/运营岗）本身无 Scholar 主页，无法作为种子
- 6/11 种子因无 coauthor 侧边栏而扫描结果为空（这是正常情况，不是错误）
- 结果受 Google Scholar 反爬限制，429 时须标记 error 下次继续

---

### 27.4 方法 C：Scholar Coauthor 图谱 — 二阶扩展

**定义：**  
对一阶扩展中发现的华人 Layer 2+ coauthor，将其作为新的「候选种子」，再次提取其 Scholar 主页侧边栏的 Co-authors 列表，形成两跳（two-hop）关系图。

**与一阶的关键区别：**

| 维度 | 一阶扩展 | 二阶扩展 |
|------|---------|---------|
| **种子来源** | 已确认的 Anthropic 华人员工（11人，高置信） | 一阶发现的华人 Layer 2+ coauthor（非 Anthropic 员工为主） |
| **关系距离** | 距 Anthropic 员工 1 跳 | 距 Anthropic 员工 2 跳 |
| **噪音率** | 低（种子质量高） | 中高（种子质量参差，多为学术合作者而非 Anthropic 员工） |
| **期望新发现率** | 较高（每个种子平均 8-12 个新节点） | 较低（大量重复发现 + 与 Anthropic 关联更弱） |
| **成本** | 11 次 Scholar 抓取 | 28 次 Scholar 抓取（一阶发现的华人 Layer 2+ 数量）|

**执行前提条件：**  
1. 一阶扫描已全部完成（11/11 ✅）
2. 一阶发现的 coauthor 华人身份已完成判定（Layer 2+ 已标注）
3. 这些 Layer 2+ coauthor 需有 Scholar 主页（否则无法扫描）
4. 已有足够数据库容量（新节点将追加到 `coauthor_graph_db.json`）

**本项目当前状态：⏸️ 暂停（用户决策，2026-04-05）**

- 一阶扫描发现的 28 位华人 Layer 2+ 全部已在学术机构或其他公司确认非 Anthropic
- 性价比评估：通过这 28 人的共著网络再发现 Anthropic 华人的概率较低（两跳距离衰减效应）
- **用户决定暂不执行二阶扩展，待其他直接方法（LinkedIn API / arXiv ACK）覆盖率接近上限后再评估**
- 如需重新启动，执行对象为 `coauthor_graph_db.json` 中 `chinese_layer >= 2` 且 `review_status != "excluded"` 的节点（28人）

---

### 27.5 方法选择决策树

```
是否已有目标人员的 Scholar ID？
├── 是 → 方法 A（直接确认在职状态 + 补充学术背景）
└── 否 → 是否有足够种子（已知 Anthropic 华人员工有 Scholar 主页）？
           ├── 是，且未做过一阶扫描 → 方法 B（一阶扩展）
           └── 是，且一阶已完成 → 评估二阶性价比
                  ├── 一阶发现大量 Anthropic 华人 → 方法 C（二阶，高价值）
                  └── 一阶发现少量/无 → 方法 C 价值有限，优先其他渠道
```

### 27.6 三种方法的数据库对应

| 方法 | 数据库文件 | 字段标识 |
|------|-----------|---------|
| 方法 A（直接扫描） | `data/anthropic_scholar_ids.json` | `verified_email: "@anthropic.com"` |
| 方法 B（一阶扩展） | `data/scholar_coauthor/coauthor_graph_db.json` | `co_with: [种子ID]`，coauthor 节点 |
| 方法 C（二阶扩展） | 同上（追加） | `layer: 2`（节点来自一阶 Layer 2+ coauthor 作为种子），建议新增 `graph_layer` 字段区分 |

**二阶扩展启动时，建议在 `coauthor_graph_db.json` 的每个节点新增字段：**
```json
{
  "graph_layer": 1,        // 1=一阶发现，2=二阶发现，0=种子
  "discovered_via": ["GskOShAAAAAJ"]  // 发现此节点的上一层节点 Scholar ID
}
```

---

## §28 投资机构华人成员扫描方法体系

> **核心原则：** 投资机构扫描与 Anthropic 员工扫描是两条并行的 Sourcing 线，共享 LinkedIn API 账号池和 Profile 存档规范，但成员名单获取策略不同。

### 28.1 方法全景图

```
渠道 I:  官网 Team 页爬取（大型机构首选）
渠道 II: LinkedIn 公司 company/people API（中小型机构主力）
渠道 III: SPA 渲染页 + browser 工具（JavaScript 密集渲染的机构官网）
渠道 IV: 搜索引擎定向检索（Cloudflare 保护 / 大型机构补充）
渠道 V:  Crunchbase People 页（无 Team 页机构的快速摸底）
渠道 VI: 中文媒体 + 榜单（Forbes Midas List 等华人 VC 专项覆盖）
```

---

### 28.2 机构规模分级策略（强制）

**在调用任何 API 前，先判断机构规模，按规模选方法：**

| 规模 | 判断方式 | 主力扫描方法 | 原因 |
|------|---------|------------|------|
| **大型（>200人）** | LinkedIn 公司页「员工数」字段 | 渠道 I（官网 Team 页）+ 渠道 IV（搜索引擎） | API 覆盖率 <15%，翻页成本高，额度浪费严重 |
| **中小型（≤200人）** | 同上 | 渠道 II（LinkedIn company/people API） | 翻页次数可控，单次获取 50 人，效率高 |

**大型机构参考**（通常用官网+搜索，不调 API）：  
Sequoia、a16z、GV、Lightspeed、Fidelity、BlackRock、Amazon、Salesforce、SoftBank、ICONIQ Capital、Bessemer、GIC

**中小型机构参考**（通常直接调 LinkedIn API）：  
Spark Capital、Menlo Ventures、DFJ Growth、SV Angel、WndrCo、Scale VP、Altimeter、Coatue、Greenoaks、D1 Capital、Founders Fund、Bond Capital

---

### 28.3 渠道 I：官网 Team 页爬取

**适用：** 大型机构（有公开静态 Team 页的 VC / 投资机构）

```bash
# 直接 web_fetch 官网 Team 页
web_fetch "https://lsvp.com/team/" maxChars=50000
web_fetch "https://www.sequoiacap.com/people/" maxChars=50000
web_fetch "https://www.bessemer.com/team" maxChars=50000
```

**提取逻辑：**  
1. 提取所有姓名（含 title/role）
2. 用华人姓氏词典初筛（见 §4.1）
3. 对命中姓名搜索 LinkedIn 确认身份

**⚠️ 注意：部分机构官网采用 SPA（React/Next.js），`web_fetch` 只能拿到空壳 HTML，需改用渠道 III。**

---

### 28.4 渠道 II：LinkedIn company/people API（中小型机构主力）

与 §2（Anthropic 在职员工批量拉取）使用**完全相同的 API 和翻页策略**，仅替换 `username` 参数为目标机构的 LinkedIn slug。

```bash
# 替换 username 为目标机构 slug
curl -s --request GET \
  --url 'https://z-real-time-linkedin-scraper-api1.p.rapidapi.com/api/company/people?username={institution_slug}&page=1&limit=50' \
  --header 'x-rapidapi-host: z-real-time-linkedin-scraper-api1.p.rapidapi.com' \
  --header 'x-rapidapi-key: {API_KEY}'
```

**存档要求（强制）：**  
每家机构的 API 原始返回须立即存档：
```
linkedin_profiles/raw/{institution_slug}_members_page001.json
linkedin_profiles/raw/{institution_slug}_members_all.json   ← 全量合并
```

**⚠️ 历史教训（2026-04-02）：** 投资机构扫描时 Profile API 返回结果未持久化，数据永久丢失。每次调用后必须立刻落盘，不得只写 Excel 而丢弃原始数据。

---

### 28.5 渠道 III：SPA 渲染页 + browser 工具

**适用：** 官网用 React/Next.js 渲染、`web_fetch` 返回空内容（无可读文本）的机构

```
判断条件：web_fetch 返回内容 < 1000 字符 或 不含任何姓名 → 判定为 SPA
```

**操作方法：**
1. 使用 `browser` 工具打开官网 Team 页（`browser action=open url=...`）
2. 等待页面完全渲染（`browser action=snapshot`）
3. 从渲染后的 DOM 中提取姓名列表

**已知 Cloudflare Turnstile 保护的机构（无法自动化，改用渠道 IV）：**
- General Catalyst
- Spark Capital

---

### 28.6 渠道 IV：搜索引擎定向检索

**适用：**  
1. Cloudflare 保护的机构官网（渠道 I/III 均失效）  
2. 大型机构（API 覆盖率低）的华裔定向补充  
3. 榜单覆盖（Forbes Midas List 等）

```bash
# 定向找华裔投资人
web_search 'site:linkedin.com "Sequoia Capital" "partner" "Tsinghua" OR "Peking" OR "Fudan"'
web_search '"ICONIQ Capital" Chinese investor partner analyst 2025'
web_search 'GIC 新加坡政府投资 华人 科技团队 2025'

# 榜单交叉比对
web_search 'Forbes Midas List 2025 Chinese American venture capital'
web_search 'Forbes 30 Under 30 VC 2024 2025 Chinese Asian'

# 媒体报道
web_search '"Anthropic" "Series G" investors 华人 LinkedIn 2026'
```

**⚠️ 搜索引擎结果只作为线索，每个候选人必须通过 LinkedIn 确认当前机构和职位，不能仅凭搜索结果写入。**

---

### 28.7 渠道 V：Crunchbase People 页

**适用：** 无公开 Team 页的机构（如 ICONIQ Capital 等）快速摸底

```bash
web_fetch "https://www.crunchbase.com/organization/{slug}/people" maxChars=30000
```

**限制：**  
- 免费版仅显示部分成员，不保证全量
- 适合判断机构是否有华人成员（快速摸底），不适合作为主力数据源
- 结果须通过 LinkedIn 二次确认

---

### 28.8 渠道 VI：中文媒体 + 华人 VC 榜单

**适用：** 大陆背景的 LP/GP、华人合伙人的特定覆盖

```bash
# 中文媒体
web_search '36氪 创投 华人 Anthropic 投资 合伙人 2024 2025'
web_search '量子位 Anthropic 融资 华人投资人'

# 专项榜单
web_search 'Forbes Asia 30 Under 30 2024 venture capital Chinese'
web_search 'NVCA Venture Forward Asian American VC list 2025'
```

---

### 28.9 成员名单获取的七步工作流

> **本节整合了 Part B 的完整流程，与投资机构成员扫描配套。**

```
Step 1: 获取投资机构名单（多源核实：官方 newsroom > Crunchbase > 媒体）
    ↓
Step 2: 产出「投融资历史与华人网络」初稿
        （标准化格式，华人网络线索列留空）
    ↓
Step 3: 按机构规模分级 → 选择渠道 I/II/III/IV/V 获取成员名单
        （API 原始结果全部存档 → 数据资产）
    ↓
Step 4: 搜索核实 LinkedIn slug + 初步填写华人成员字段
        （不调用个人 Profile API，仅搜索引擎核实）
    ↓
Step 5: ⚠️ 阶段门：用户人工核查 LinkedIn 链接
        （确认无误后方可继续；错误链接会导致 API 额度浪费）
    ↓
Step 6: 根据用户需求调用 Profile API 精细化字段（用户确认后执行）
        （结果立即落盘至 linkedin_profiles/{slug}.json）
    ↓
Step 7: 更新「华人网络线索」列 → 产出 Excel + 更新 PROGRESS.md
```

**Step 5 阶段门是硬性要求，不可跳过。Profile API 消耗不可逆额度，任何一个错误 slug 都是浪费。**

---

### 28.10 华人成员字段规范（投资机构专用）

#### 「主要投资方华人成员」Sheet（Sheet 4）

| 字段 | 数据来源 | 填写规范 |
|------|---------|---------|
| **机构** | 融资历史表 | 机构全称，与 Sheet 3 一致 |
| **姓名** | API fullName + 搜索核实 | 英文名在前，有确认来源的中文名换行在后，不推断 |
| **职位** | LinkedIn headline / 官网 | 写全职位名，如 "Partner" / "Managing Director" |
| **族裔** | 搜索 + 姓名 + 教育推断 | 区分：华裔美国人 / 大陆华人 / 新加坡华裔 / 香港裔 / 台湾裔 |
| **是否涉及Anthropic投资** | 三级标注（见 §28.11） | 括号内必须补充一句理由 |
| **投资方向** | LinkedIn headline + 官网介绍 | 精简到具体赛道，避免套话 |
| **备注** | 教育 + 主要经历 | 客观陈述 ≤3句；禁止主观评价和动机推断 |
| **LinkedIn** | 搜索核实的 slug | 格式：`https://www.linkedin.com/in/{slug}/` |

#### §28.11 「是否涉及 Anthropic 投资」三级标注体系

| 标注 | 含义 | 判定依据 | 示例 |
|------|------|---------|------|
| **⭐ 是（…）** | 确认直接参与投资决策 | LinkedIn 本人写明 / 媒体采访点名 / 机构官方披露 | ⭐ 是（Bessemer deal lead，Bloomberg采访确认主导Series D Anthropic投资） |
| **可能（…）** | 机构参与融资，本人具体参与度未确认 | 机构参与融资 + 本人为投资相关岗位 | 可能（所在机构Lightspeed为Series E4领投方；个人具体参与待确认） |
| **否（…）** | 明确排除 | 人才/法务/财务/行政等非投资岗；或公开说明未参与 | 否（General Catalyst人才合伙人，非投资决策岗位） |

**⚠️ 规则：**
- 括号内**必须补充一句理由**，不允许只写「是」「可能」「否」
- 「可能」参与者**只保留在 Sheet 4**，不写入 Sheet 3「华人网络线索」列
- 只有「⭐ 是」的人才写入 Sheet 3 华人网络线索列

---

### 28.12 Sheet 3「华人网络线索」列更新规则

遍历 Sheet 4 中所有 `⭐ 是` 的人物，按融资轮次写入 Sheet 3 对应行：

**格式（多人用换行分隔，Excel 单元格内 `\n`）：**
```
**Emily Zhao**（Salesforce Ventures）：Salesforce Ventures MD，Anthropic Series A deal负责人；LinkedIn headline直接标注
**Eric Yuan / 袁征**（Zoom Ventures）：Series A和C1均参投；最早参与Anthropic融资的华人创始人/CEO投资者
```

**规则：**
- 该轮次无任何确认华人参与 → 填 `—`
- 仅「可能」参与者 → 不写入此列
- 信息来源须可溯源，不推断

---

## §29 The Org API — Org Chart 完整成员列表获取

> **方法定位：** 属于「早期批量列表获取」类方法，与 LinkedIn company/people API、LinkedIn Search 属于同一层级——**一次性拉取相对完整的成员名单，之后再逐步筛选、补充、核实**。与 Scholar Coauthor 图谱（关系网络扩展）是不同类型的方法。

### 29.1 The Org API 基本参数

| 项目 | 内容 |
|------|------|
| **API Base** | `https://api.theorg.com` |
| **核心端点** | `GET /v1.2/companies/org-chart` |
| **参数** | `domain=anthropic.com` 或 `linkedInUrl={company_linkedin_url}` |
| **返回字段** | `id`（positionId）、`fullName`、`title`、`managerId`、`section`、`nodeType` |
| **不返回的字段** | `linkedinUrl`、`workEmail`（需 Position API 单独丰富，消耗 1 credit/人） |
| **每次调用费用** | **10 credits**（org-chart endpoint） |
| **月限额** | **Free tier: 10 credits/月 = 最多 1 次 org-chart 调用/月** |

**⚠️ Free tier 严重限制：每月只能调用 1 次 org-chart，用尽即止，月初重置。**

**账号配置文件：** `data/theorg/theorg_accounts.json`

### 29.2 额度管理（强制规则）

```
⚠️ 每月最多 10 次 org-chart 调用，违反不可逆！
```

**调用前必须检查：**
```python
with open("data/theorg/theorg_accounts.json") as f:
    config = json.load(f)
acc = config["accounts"][0]
used = acc["total_credits_used"]
limit = acc["monthly_limit_credits"]  # Free tier = 10
if used >= limit:
    print(f"🚫 本月额度已耗尽 ({used}/{limit} credits)，下次可用: {acc.get('reset_date', '月初')}")
    print("   请等待月度重置后再调用，或联系升级付费 plan")
    return  # 强制停止，不执行调用
```

**调用后立即更新（原子操作）：**
```python
config["accounts"][0]["total_credits_used"] += 10
config["accounts"][0]["total_org_chart_calls"] += 1
config["accounts"][0]["usage_log"].append({
    "date": TODAY, "credits": 10,
    "endpoint": "GET /v1.2/companies/org-chart?domain={domain}",
    "result_positions": N, "note": "..."
})
# 并将公司加入 archived_companies 列表
```

**幂等性检查（调用前）：**
```python
# 如果该公司已经存档，直接读取，不重复调用
archived = [c["company"] for c in config.get("archived_companies", [])]
if target_company in archived:
    # 直接读取 data/theorg/orgchart_{slug}_{date}.json
    return  # 不调用 API
```

### 29.3 调用方法（标准 Python + 原子存档，强制使用）

> ⚠️ **严禁直接用 curl 输出到终端再手动保存**：终端输出会被截断（尤其大型 org-chart），导致 JSON 不完整而数据丢失——2026-04-05 首次调用曾因此导致 `orgchart_raw_2026-04-05.json` 仅保留了 81 个节点（实际返回 ~450 节点）。**必须使用下方 Python 脚本，保证数据原子落盘。**

```python
#!/usr/bin/env python3
"""The Org API — Org Chart 安全调用脚本（数据原子落盘）"""
import json, sys, os, requests
from datetime import date

# ── 配置 ──────────────────────────────────────────────────────
ACCOUNTS_FILE = "data/theorg/theorg_accounts.json"
DOMAIN        = "anthropic.com"          # 目标公司域名
SLUG          = "anthropic"              # 用于文件命名
OUT_DIR       = "data/theorg"

# ── 额度检查（强制，不通过则退出）────────────────────────────
with open(ACCOUNTS_FILE) as f:
    config = json.load(f)
acc = config["accounts"][0]
used  = acc["total_credits_used"]
limit = acc["monthly_limit_credits"]
if used >= limit:
    print(f"🚫 本月额度已耗尽 ({used}/{limit} credits)，下次可用: {acc.get('reset_date','月初')}")
    sys.exit(0)

# ── 幂等性检查：该公司本月是否已有存档 ───────────────────────
today = str(date.today())
out_file = f"{OUT_DIR}/orgchart_{SLUG}_{today}.json"
if os.path.exists(out_file):
    print(f"✅ 已有存档 {out_file}，跳过 API 调用")
    sys.exit(0)

# ── 实际调用 ─────────────────────────────────────────────────
print(f"🔄 调用 The Org API (domain={DOMAIN})...")
resp = requests.get(
    "https://api.theorg.com/v1.2/companies/org-chart",
    params={"domain": DOMAIN},
    headers={
        "X-Api-Key": acc["api_key"],
        "Content-Type": "application/json",
    },
    timeout=60,
)
resp.raise_for_status()
data = resp.json()

# ── 立即原子落盘（先写临时文件再 rename，防止写入中途崩溃）──
os.makedirs(OUT_DIR, exist_ok=True)
tmp_file = out_file + ".tmp"
with open(tmp_file, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
os.rename(tmp_file, out_file)   # 原子替换，保证文件完整

# ── 统计节点 ─────────────────────────────────────────────────
nodes = data.get("data", [])
positions = [n for n in nodes if n.get("nodeType") == "position"]
print(f"✅ 存档完成: {out_file}")
print(f"   总节点: {len(nodes)} | position节点: {len(positions)}")

# ── 更新账号使用记录 ──────────────────────────────────────────
acc["total_credits_used"]    += 10
acc["total_org_chart_calls"] += 1
acc["usage_log"].append({
    "date": today, "credits": 10,
    "endpoint": f"GET /v1.2/companies/org-chart?domain={DOMAIN}",
    "result_nodes": len(nodes),
    "result_positions": len(positions),
    "out_file": out_file,
    "note": "Python 原子落盘",
})
if used + 10 >= limit:
    from datetime import date as d
    import calendar
    today_d = d.today()
    last_day = calendar.monthrange(today_d.year, today_d.month)[1]
    next_reset = f"{today_d.year}-{today_d.month+1:02d}-01" if today_d.month < 12 \
                 else f"{today_d.year+1}-01-01"
    acc["status"]     = "exhausted_this_month"
    acc["reset_date"] = next_reset
with open(ACCOUNTS_FILE, "w") as f:
    json.dump(config, f, ensure_ascii=False, indent=2)
print(f"   账号记录已更新: {used+10}/{limit} credits 已用")
```

**exec 调用方式（在项目目录执行）：**
```bash
cd /home/node/.openclaw/workspace/Anthropic华人专项
python3 data/theorg/call_theorg.py
```

**⚠️ 数据安全三原则：**
1. **先写 .tmp，再 rename** — 避免写入中断导致文件损坏
2. **调用后立即更新账号记录** — 确保额度追踪准确
3. **幂等性检查** — 同一公司同一天不重复调用（检测 out_file 是否已存在）

### 29.4 The Org vs 其他列表获取方法对比

| 维度 | The Org Org Chart | LinkedIn company/people | LinkedIn Search |
|------|------------------|------------------------|-----------------|
| **数据来源** | The Org 平台用户自填 + 爬取 | LinkedIn 公司员工页 | LinkedIn 搜索过滤 |
| **覆盖率** | 中等（员工在 The Org 上填写才有）| 中等（LinkedIn 覆盖 30-40%）| 低（仅有公开 profile）|
| **唯一优势** | **有汇报关系（managerId）** + 一次调用全量 | API 稳定，翻页简单 | 可按条件过滤（公司+关键词）|
| **限制** | 月 10 次上限；无 LinkedIn URL；部分人用首字母缩写 | 翻页有上限；大公司覆盖率低 | 搜索结果有限，有噪音 |
| **成本** | 10 credits/次（约￥0，免费额度） | RapidAPI 按调用计费 | RapidAPI 按调用计费 |
| **每次获得** | 全量成员平铺列表（含层级关系）| 最多 50 人/页，需翻页 | 最多 100 条/搜索 |

### 29.5 华人姓名初筛方法（The Org 专用）

The Org 的 `fullName` 字段有时不完整（如 `Jiasen Y.`、`Zhao M.`），需配合多重识别策略：

```python
CHINESE_SURNAMES = {
    'Li','Zhang','Wang','Chen','Liu','Yang','Huang','Lin','Wu','Zhou','Xu','Sun',
    'He','Gao','Ma','Ye','Luo','Zheng','Tang','Song','Han','Feng','Deng','Jiang',
    'Cai','Yan','Xie','Xiao','Shao','Liang','Wei','Pan','Bao','Fang','Zhu',
    'Hu','Cheng','Lu','Cui','Dong','Shen','Meng','Xue','Fu','Lei','Zeng',
    'Peng','Wen','Jia','Kang','Du','Dai','Ding','Jin','Guo','Yu','Tong',
    'Tsai','Hsu','Tung','Ling','Kuo','Liao','Wan','Fung','Rao','Gu','Kong',
    'Shi','Qi','Lai','Mao','Ru','Lou','Ning','Zhong','Tan','Xia','Hong','Qin',
}

CHINESE_PINYIN_PATTERNS = [
    'zhen','zhon','zhan','xiao','xian','yuan','yong','yang','yue',
    'dong','deng','hong','jing','ming','ning','qing','shao','shan',
    'wei','weng','xing','ying','zheng','zhuang','ruoy','jiasen',
    'yunon','xixia','yuana','chih','shijie','ruiyan','dalian',
]

def is_likely_chinese(fullname):
    if not fullname: return False
    # 1. 直接包含中文字符
    if any('\u4e00' <= c <= '\u9fff' for c in fullname): return True
    parts = fullname.strip().split()
    last = parts[-1].strip('.') if parts else ''
    first = parts[0].strip('.') if parts else ''
    # 2. 姓氏词典匹配
    if last in CHINESE_SURNAMES or (first in CHINESE_SURNAMES and len(parts) >= 2): return True
    # 3. 拼音模式匹配
    full_lower = fullname.lower().replace(' ', '').replace('-', '')
    return any(p in full_lower for p in CHINESE_PINYIN_PATTERNS)
```

**注意：** 姓氏缩写（如 `Zhao M.`、`Jiasen Y.`）需要通过 Google 搜索 `"Jiasen" "Anthropic" LinkedIn` 进一步确认。

### 29.6 The Org 数据存档规范

```
data/theorg/
├── theorg_accounts.json          ← 账号 + 月度使用追踪（强制维护）
├── orgchart_{slug}_{date}.json   ← 原始 API 返回（每公司一次，不重复）
│   示例: orgchart_anthropic_2026-04-05.json
└── candidates_{slug}_{date}.json ← 华人候选人筛选结果（可选）
```

### 29.7 Anthropic The Org 扫描结果（2026-04-05）

| 指标 | 结果 |
|------|------|
| **总节点数** | **81 个节点**（⚠️ 数据不完整，见下方说明）|
| **华人候选初筛** | 实际可分析节点基于 81 个 |
| **新发现候选** | **Jiasen Yang**（MTS，The Org显示"Jiasen Y."，LinkedIn /jiaseny/ 确认，USTC统计学本科，大陆华人，已写入Excel Row 101）|
| **已用 credits** | 10/10（**本月额度已耗尽**，Free tier 每月仅 1 次调用）|
| **存档文件** | `data/theorg/orgchart_raw_2026-04-05.json`（不完整）|
| **下次可调用** | **2026-05-01** 月度重置后，届时使用 §29.3 Python 脚本重新获取完整数据 |

**⚠️ 数据不完整原因（教训）：**
- 首次调用使用了 curl 直接输出到终端，终端输出被截断，仅保存了 81 个节点
- API 实际返回估计约 450+ 节点，但截断的 JSON 已无法恢复
- **2026-05-01 重新调用时，务必使用 §29.3 的 Python 原子落盘脚本**

---

## §30 成员列表获取方法统一工作流（优化版）

> **核心设计原则：** 先用所有「批量获取」类方法并行获取原始列表，整合去重后，再统一执行「逐人核实」类操作。避免交替切换方法造成的重复劳动和数据割裂。

### 30.1 方法分层架构

> **判断方法归属的核心标准（唯一判据）：**
>
> | 标准 | 归属 |
> |------|------|
> | **利用平台字段/过滤器，批量产出候选人名单，不依赖已知成员** | Layer 1（批量获取）|
> | **以已知成员为起点，沿关系链扩展发现新人** | Layer 2（关系扩展）|
> | **针对已知候选人，逐一确认身份/补充信息** | Layer 3（逐人核实）|
>
> ⚠️ **同一个数据源可以跨层，取决于使用方式：**
> - Google Scholar：搜索「所有 verified email at anthropic.com」→ **Layer 1**（批量过滤平台字段）
> - Google Scholar：查已知候选人「张三」的研究方向 → **Layer 3**（逐人补全信息）
> - X：搜索「所有 Bio 含 @Anthropic 的账号」→ **Layer 1**（批量过滤平台字段）
> - X：查已知候选人「张三」的 X 账号确认在职 → **Layer 3**（逐人核实）

---

**第一层：批量获取（Early Batch Sourcing）**
> **特征：** 主动利用外部平台字段/过滤器，批量产出候选人名单；不依赖已知成员；成本固定；质量参差，需后续核实。

**子类 1A：组织/成员数据库（职业平台）**
> 直接从职业社交平台或 HR 数据库获取某公司的成员列表

| 方法 | 核心数据源 | 利用的平台字段 | 预期覆盖率 | 引用章节 |
|------|-----------|--------------|-----------|---------|
| LinkedIn company/people API | LinkedIn 公司员工页 | `current_company = Anthropic` | ~40% | §2 |
| LinkedIn Search API | LinkedIn 搜索过滤 | `company:"Anthropic"` + 关键词 | ~20% | §5 |
| The Org Org Chart API | The Org 平台 | `domain=anthropic.com` org-chart | ~30% | §29 |
| **Hunter.io Domain Search** | Hunter.io 邮箱数据库 | `domain=anthropic.com`，返回姓名+职级+LinkedIn | ~30%（与LinkedIn互补）| §32 |
| 官网 Team 页爬取 | 公司官网 | 公司主动公示的成员列表 | 变化大 | §28.3 |
| Crunchbase People 页 | Crunchbase | `organization=Anthropic` | ~20% | §28.7 |

**子类 1B：公开 Bio/自我声明（个人简介字段批量过滤）**
> 利用平台上用户**自主填写的 Bio/简介字段**，批量过滤出声明与目标公司相关的账号。
> **本质**：不从公司角度查「谁在这家公司工作」，而是从个人角度查「谁声称自己在这家公司工作」——两者互补，合计覆盖率更高。

| 方法 | 核心数据源 | 利用的平台字段 | 预期覆盖率 | 实现难度 | 引用章节 |
|------|-----------|--------------|-----------|---------|---------|
| **Google Scholar Bio 批量扫描** | Google Scholar | `Verified email at anthropic.com`（学术机构验证邮箱）| ~20%（学术型员工）| 低（搜索直接可得）| §31 |
| **X (Twitter) Bio 批量搜索** | X 平台 | Bio 字段含 `Anthropic`（用户自填）| ~10-20% | 中（搜索 API 或 `site:x.com "Anthropic" bio:` 限制多）| §31 |
| **GitHub Bio/Profile 批量扫描** | GitHub | Profile 的 Company 字段 = `@anthropic`（用户自填）| ~5-10%（工程型员工）| 中（GitHub API 支持按公司过滤）| §31 |

**子类 1C：学术论文作者扫描**
> 从公开学术论文中，批量提取机构归属为 Anthropic 的作者

| 方法 | 核心数据源 | 利用的平台字段 | 预期覆盖率 | 引用章节 |
|------|-----------|--------------|-----------|---------|
| **arXiv/Semantic Scholar 论文作者扫描** | 学术论文库 | `affiliation: Anthropic`（论文作者机构字段）| ~30%（学术型员工）| §31 |

---

**第二层：关系网络扩展（Network Expansion）**
> **特征：** 以**已知 Anthropic 成员**为起点，沿某种关系链发现新的潜在相关人员；覆盖面比 Layer 1 窄，但精准度高（起点可信时）；发现率随跳数衰减。

| 方法 | 关系类型 | 起点 | 发现距离 | 引用章节 |
|------|---------|------|---------|---------|
| **Scholar Coauthor 图谱（一阶）** | 论文合著关系 | 已知 Anthropic 员工的 Scholar 主页 | 1跳 | §26/§27 |
| **Scholar Coauthor 图谱（二阶）** | 论文合著关系 | 一阶合著者的 Scholar 主页 | 2跳 | §27.4（暂停）|
| **arXiv 论文致谢（ACK）扫描** | 论文致谢关系 | 已知 Anthropic 论文 | 准直接（被致谢≠员工）| §31 |

> ⚠️ **Google Scholar @anthropic.com 邮箱搜索（§27.2 方法 A）** 虽然使用 Scholar 平台，但属于 **Layer 1B**（批量过滤平台字段），不是 Layer 2。

---

**第三层：逐人核实（Individual Verification）**
> **特征：** 针对**已有候选人**，逐一操作；不产生新候选人；目的是确认在职状态、补全 Excel 字段；成本最高，在阶段门确认后执行。

| 操作 | 目的 | 触发条件 | 引用章节 |
|------|------|---------|---------|
| Google 搜索 `"姓名" "Anthropic" LinkedIn` | 找 LinkedIn slug | 对每个华人候选人 | §4 |
| LinkedIn Profile API | 完整职位/教育信息 | 用户确认 slug 后 | §6/§28.4 |
| web_fetch LinkedIn 页面 | 替代 Profile API | 无 API 额度时 | §5 |
| Google Scholar 主页查询（指定人）| 补全研究方向/教育 | 学术型候选人 | §27.2 |
| X 账号查询（指定人） | 核实在职自报状态 | 有 X 账号的候选人 | — |
| 个人主页/GitHub 查询（指定人）| 核实在职状态/项目 | 有公开主页的候选人 | — |

### 30.2 优化后的完整工作流

```
╔═══════════════════════════════════════════════════════════════╗
║  Layer 1: 批量获取（并行，任务开始即执行，顺序无关）           ║
╠═══════════════════════════════════════════════════════════════╣
║  ── 1A. 组织/成员数据库 ──                                    ║
║  1A-a. The Org API           → orgchart_{domain}_{date}.json ║
║  1A-b. LinkedIn company/     → members_all.json              ║
║        people API                                            ║
║  1A-c. LinkedIn Search API   → search_results.json           ║
║  1A-d. 官网 Team 页爬取      → team_page_raw.txt             ║
║                                                              ║
║  ── 1B. 个人 Bio 批量过滤 ──                                  ║
║  1B-a. Google Scholar Bio    → scholar_bio_{date}.json       ║
║        (verified email at anthropic.com)                     ║
║  1B-b. X Bio 批量搜索        → x_bio_{date}.json             ║
║        (Bio 含 "Anthropic"/"@Anthropic")                     ║
║  1B-c. GitHub Bio 扫描       → github_bio_{date}.json        ║
║        (Company 字段 = @anthropic)                           ║
║                                                              ║
║  ── 1C. 学术论文作者 ──                                       ║
║  1C-a. arXiv/Semantic Scholar → arxiv_authors_{date}.json   ║
║        (affiliation: Anthropic，提取全部作者)                 ║
║                                                              ║
║  ⚠️ 全部原始数据立即存档，不丢弃                              ║
╚═══════════════════════════════════════════════════════════════╝
                          ↓
╔═══════════════════════════════════════════════════════════╗
║  整合去重                                                 ║
║  合并所有来源 → normalized_name 去重 → 标记来源渠道       ║
║  输出: candidates_merged_{date}.json                     ║
╚═══════════════════════════════════════════════════════════╝
                          ↓
╔═══════════════════════════════════════════════════════════╗
║  华人初筛                                                 ║
║  姓氏词典 + 拼音特征识别 → chinese_flag: Y/N/uncertain    ║
║  输出: chinese_candidates_{date}.json                    ║
╚═══════════════════════════════════════════════════════════╝
                          ↓
              ⚠️ 阶段门 1：用户目视核查候选列表
              （确认哪些需要继续核实，哪些已知，哪些排除）
              （禁止在此之前调用 LinkedIn Profile API）
                          ↓
╔═══════════════════════════════════════════════════════════╗
║  Layer 2: 关系网络扩展（按需，在初筛后选择性执行）         ║
╠═══════════════════════════════════════════════════════════╣
║  2a. Scholar Coauthor 图谱（对候选人/已知员工的 Scholar   ║
║      主页提取 coauthor，发现新的潜在华人成员）             ║
║  2b. arXiv ACK 扫描（从 Anthropic 论文致谢提取被感谢人）  ║
║  [按需、有目标地执行，非必须步骤]                         ║
╚═══════════════════════════════════════════════════════════╝
                          ↓
              ⚠️ 阶段门 2：用户确认 LinkedIn slug
              （每个候选人的 LinkedIn 链接需用户目视核查）
                          ↓
╔═══════════════════════════════════════════════════════════╗
║  Layer 3: 逐人核实（仅对阶段门 1 确认的候选人）            ║
╠═══════════════════════════════════════════════════════════╣
║  3a. Google 搜索 → 找 LinkedIn slug                      ║
║  3b. LinkedIn Profile API → 职位/教育/详情               ║
║  3c. Google Scholar Bio → 学术背景/研究方向（学术型）     ║
║  3d. X Bio / 个人主页 → 自报在职状态（有公开账号时）      ║
║  3e. 立即存档所有 API 返回 → linkedin_profiles/{slug}.json║
╚═══════════════════════════════════════════════════════════╝
                          ↓
╔═══════════════════════════════════════════════════════════╗
║  写入 Excel + Git commit                                  ║
║  版本号递增；PROGRESS.md 更新                             ║
╚═══════════════════════════════════════════════════════════╝
```

**关键决策点：**

| 情形 | 处理方式 |
|------|---------|
| Layer 1 已有足够候选人 | 直接进 Layer 3，跳过 Layer 2 |
| Layer 1 覆盖率不足（学术型员工多）| 补充执行 Layer 2a（Scholar Coauthor）|
| 发现有论文但无 LinkedIn 的候选人 | Layer 2a → 找 Scholar ID → Layer 3c 补充信息 |
| 候选人姓名缩写（如 "Jiasen Y."）| Layer 3a Google 搜索补全姓名再处理 |

### 30.3 合并去重模板代码

```python
import json
from pathlib import Path

def merge_candidate_sources(theorg_file, linkedin_file, official_file=None):
    """合并多个来源的候选人，以 normalized_name 去重"""
    merged = {}  # normalized_name -> candidate_dict
    
    def normalize(name):
        return name.lower().strip().replace('.', '').replace('  ', ' ')
    
    # The Org 数据
    if theorg_file and Path(theorg_file).exists():
        with open(theorg_file) as f:
            data = json.load(f)
        for node in data.get("data", []):
            if node.get("nodeType") != "position" or not node.get("fullName"):
                continue
            key = normalize(node["fullName"])
            if key not in merged:
                merged[key] = {
                    "fullName": node["fullName"],
                    "title": node.get("title", ""),
                    "sources": ["theorg"],
                    "theorg_id": node.get("id"),
                    "theorg_manager_id": node.get("managerId"),
                }
            else:
                merged[key]["sources"].append("theorg")
    
    # LinkedIn 数据
    if linkedin_file and Path(linkedin_file).exists():
        with open(linkedin_file) as f:
            data = json.load(f)
        for p in data.get("profiles", data if isinstance(data, list) else []):
            name = p.get("fullName") or f"{p.get('firstName','')} {p.get('lastName','')}".strip()
            if not name:
                continue
            key = normalize(name)
            if key not in merged:
                merged[key] = {
                    "fullName": name,
                    "title": p.get("headline", p.get("title", "")),
                    "sources": ["linkedin"],
                    "linkedin_slug": p.get("linkedinUsername", p.get("slug")),
                }
            else:
                merged[key]["sources"].append("linkedin")
                # LinkedIn 有 slug 时补充进来
                if p.get("linkedinUsername"):
                    merged[key]["linkedin_slug"] = p["linkedinUsername"]
    
    return list(merged.values())

# 使用示例
candidates = merge_candidate_sources(
    "data/theorg/orgchart_anthropic_2026-04-05.json",
    "linkedin_profiles/raw/anthropic_members_all.json"
)
print(f"合并后候选人: {len(candidates)} 人")
```

### 30.4 三个关键工作流优化点

> 以下三个优化点来自 Anthropic 华人专项实战中发现的缺陷，已纳入正式工作流规范。

---

#### 30.4.1 ✅ 整合去重：多渠道数据必须先合并，再进入核实流程

**问题根源：** 过去的执行路径是「发现一个候选人 → 立即 LinkedIn 核实 → 写入 Excel」，各渠道（LinkedIn、The Org、Scholar）彼此割裂，导致：
- 同一个人可能被从多个渠道重复发现、重复核实（浪费 API 额度）
- 整体覆盖情况不透明，难以判断何时可以停止扩展

**正确做法：**

```
Layer 1 所有方法并行执行（LinkedIn + The Org + Scholar Bio + X Bio + GitHub Bio）
          ↓
所有原始数据本地存档（立即落盘）
          ↓
统一调用 merge_candidate_sources() 合并去重
          ↓
✅ 阶段门 1：用户目视核查合并后的候选列表（见 §30.4.2）
          ↓
仅对确认候选人调用 Layer 3 核实
```

**合并 The Org + LinkedIn 的标准化规则：**
```python
# The Org 姓名字段有时有缩写（见 §30.4.3），合并前必须先做姓名展开
# 合并 key = normalize(fullName)，normalized = 小写 + 去点 + 去多余空格
# 同一人如果在两个来源都存在，合并 sources 列表，保留 LinkedIn slug（更有价值）
```

**执行时机：** 每当新增一个原始数据来源时，立即重新运行 merge_candidate_sources() 更新合并文件。

---

#### 30.4.2 ✅ 阶段门 1：批量候选决策（禁止直接写入）

**问题根源：** 过去的执行路径「发现 → 写入 Excel」缺少批量决策步骤，导致：
- 质量参差不齐的候选人直接进入 Excel（如 Xunjie Yu，Row 97，至今无公开信息）
- 用户失去对「谁进入 Excel」的整体控制权
- 一旦写入后修正成本高（需要删行、版本递增）

**强制规则：**

```
🚫 禁止：发现候选人 → 直接调用 LinkedIn API 核实 → 直接写入 Excel
✅ 正确：发现候选人 → 加入候选队列 → 积累到一定数量后统一呈现 → 用户批量决策
```

**阶段门触发条件（满足任一即触发）：**
- 新增候选人 ≥ 5 人
- 完成一个完整 Layer 1 渠道（如 The Org 扫描完成）
- 用户主动要求查看当前候选列表

**阶段门输出格式（呈现给用户）：**

```markdown
## 阶段门 1：候选人待决策列表（共 N 人）

| # | 姓名 | 置信度 | 来源渠道 | 职位（已知）| 备注 |
|---|------|--------|---------|------------|------|
| 1 | Jiasen Yang | 高 | The Org | MTS | USTC本科，LinkedIn /jiaseny/ 已核实 |
| 2 | Lifu Huang | 中 | Scholar | MTS | 待LinkedIn核实 |
| 3 | Xunjie Yu | 低 | （来源不明）| 未知 | 无公开信息，建议排除 |
...

**请决策：** 哪些继续核实？哪些置信度足够直接写入？哪些排除？
```

**用户决策选项：**
- `✅ 写入`：置信度高，直接写入 Excel（可能仍有字段需后续补全）
- `🔍 核实`：调用 LinkedIn Profile API 进一步确认后再写入
- `❌ 排除`：加入 excluded 列表，不再处理

---

#### 30.4.3 ✅ 姓名缩写处理：The Org 首字母缩写节点专项流程

**问题根源：** The Org 的 `fullName` 字段部分用户仅填写了 Last Name 首字母缩写，如：
- `Jiasen Y.` → 实际为 Jiasen Yang（已确认）
- `Zhao M.` → 可能为 Zhao Meng（已在 Excel Row 86）
- `Wei Z.` → 无法判断（Wei + Z 姓中有多个可能）

这类节点如果被直接过滤掉（因为 Last Name 单字母不在姓氏词典里），会导致华人漏检。

**处理流程：**

```python
def extract_abbreviated_nodes(theorg_data):
    """提取 The Org 中 Last Name 为首字母缩写的节点"""
    import re
    abbreviated = []
    for node in theorg_data.get("data", []):
        name = node.get("fullName", "")
        parts = name.strip().split()
        if not parts:
            continue
        last = parts[-1]
        # Last Name 为单字母 + 点（如 "Y." 或 "M."）
        if re.match(r'^[A-Z]\.$', last) and len(parts) >= 2:
            abbreviated.append({
                "fullName": name,
                "title": node.get("title", ""),
                "theorg_id": node.get("id"),
                "abbrev_last": last[0],          # 首字母
                "first_name": " ".join(parts[:-1]),  # 名（可能多个词）
            })
    return abbreviated

# 使用：
abbrev_nodes = extract_abbreviated_nodes(theorg_data)
print(f"发现首字母缩写节点: {len(abbrev_nodes)} 个")
for n in abbrev_nodes:
    print(f"  {n['fullName']:25s} | {n['title'][:40]}")
```

**后续处理（对每个缩写节点）：**

```
1. 检查「名」部分是否为华人名（如 Jiasen、Zhao、Wei、Xin、Yue 等）
   → 是：进入华人候选列表，需通过 Google 搜索补全姓名
   → 否：跳过（如 "Alex Y."，名为西方名，低优先级）

2. Google 搜索补全：
   query = f'"{first_name}" Anthropic LinkedIn site:linkedin.com'
   或
   query = f'"{first_name} {abbrev_last}" Anthropic researcher'

3. 找到完整姓名后：
   - 在 Excel 中检查是否已存在（normalized_name 比对）
   - 若不存在：加入阶段门 1 候选列表
   - 若存在：标记 theorg 为额外来源，无需重复处理
```

**Anthropic 2026-04 已知缩写节点处理记录：**
| 缩写名 | 补全结果 | 处理状态 |
|--------|---------|---------|
| `Jiasen Y.` | Jiasen Yang | ✅ 已写入 Excel Row 101 |
| `Zhao M.` | Zhao Meng（Row 86）| ✅ 已在 Excel，确认为同一人 |

---

### 30.5 方法选择决策树（Anthropic 场景）

```
需要获取 Anthropic 成员列表？
├── The Org 已有存档（本月已调用）→ 直接读取，不再消耗 credits
├── The Org 未调用本月 → 使用 §29.3 Python 脚本调用（10 credits），原子落盘
├── LinkedIn 未扫描 → 调用 company/people API（翻页至 empty）
└── 两者都有后 → 合并去重（§30.3）→ 阶段门 1（§30.4.2）→ 逐人核实

覆盖率不足时的补充渠道（按优先级）：
1. arXiv ACK 扫描（§31.2，部分完成）
2. Scholar Coauthor 图谱扩展（§27，已完成一阶）
3. X Bio / GitHub Bio 批量扫描（§31.3，方法 G/H，未执行）
4. 搜索引擎定向华人检索（§28.6）
```

---

## §31 arXiv / 学术论文扫描方法

> **方法定位说明：**
> - **论文作者（Author）扫描** → Layer 1（批量获取）：直接产生候选人名单
> - **论文致谢（Acknowledgement）扫描** → Layer 2（关系扩展）：从已知论文发现周边相关人员

### 31.1 方法 D：arXiv 论文作者批量扫描（Layer 1）

**定义：** 在 arXiv / Semantic Scholar / Papers with Code 等平台搜索机构归属为 Anthropic 的论文，批量提取所有论文的作者列表，形成候选人名单。

**数据源：**
```bash
# arXiv 搜索（作者机构归属）
https://arxiv.org/search/?query=Anthropic&searchtype=all&start=0

# Semantic Scholar（更结构化，支持按机构过滤）
https://api.semanticscholar.org/graph/v1/paper/search?query=Anthropic&fields=authors,year,title

# Papers with Code
https://paperswithcode.com/affiliations/anthropic
```

**提取逻辑：**
1. 批量搜索获取论文列表（最近 2 年为主）
2. 提取每篇论文的全部作者
3. 过滤：保留 affiliation 含 "Anthropic" 的作者
4. 去重合并 → 候选人名单

**与 LinkedIn/The Org 的核心差异：**
- LinkedIn/The Org 反映**当前组织架构**，arXiv 反映**论文发表时的机构归属**（可能有滞后）
- arXiv 仅覆盖**有学术论文产出**的员工（Research/MTS类），不覆盖工程/GTM/运营
- arXiv 提供**研究方向**字段，可直接填入 Excel「主要研究方向」列

**适用场景：** 研究型 AI 公司（Anthropic、OpenAI、DeepMind 等），作为 LinkedIn/The Org 的补充，专门增强学术型员工覆盖率。

**本项目状态：** ⬜ 未执行（Anthropic 华人专项）

---

### 31.2 方法 E：arXiv 论文致谢（ACK）扫描（Layer 2）

**定义：** 从已知 Anthropic 论文的「Acknowledgements」或「Author Contributions」部分，提取被致谢的外部人员姓名，发现与 Anthropic 有实质合作但非作者的人。

**被致谢人的类型：**
- 外部研究顾问（collaborated with us but not an employee）
- 红队/评估参与者（外部 red teamers）
- 数据标注者（contractors）
- 先前在 Anthropic 工作的人（致谢离职员工）

**本质：** 这些人**不是** Anthropic 员工，但与 Anthropic 存在合作关系 → 属于 Layer 2（关系扩展），用于发现外部华人研究者/合作者，而非在职华人员工。

**实际价值评估：**
- 对「寻找在职华人员工」的核心任务价值**有限**
- 对「寻找华人关联研究者/潜在顾问网络」有一定价值
- 噪音较高（致谢范围宽泛，包含大量非华人/与华人项目无关的人）

**建议：** 作为补充手段，在 Layer 1 + Scholar Coauthor 覆盖率接近上限后使用。**本项目当前不优先执行。**

---

### 31.3 Bio 批量过滤方法详解（Layer 1B）

> **Layer 1B 的核心逻辑：** 不问「这家公司有哪些员工」，而是问「哪些人在自己的公开 Bio 里声称在这家公司工作」。平台字段由用户自主填写，未必实时更新，但覆盖面独立于其他数据源，是重要补充。

#### 方法 F：Google Scholar Bio 批量扫描

**定义：** 搜索 Google Scholar 上所有「Verified email at anthropic.com」的用户，批量产出候选人名单。

**原理：** Google Scholar 允许用户绑定机构邮箱（Verified email at xxx.com），相比用户自写 Bio 更可信（需邮箱验证）。这等同于「已验证在 Anthropic 机构邮箱域名下」的学者列表。

**执行方法：**
```bash
# 方法 A（已在 §27.2 定义）：搜索 Google Scholar 的机构邮箱
# URL 格式：
https://scholar.google.com/citations?view_op=search_authors&mauthors=anthropic.com&hl=en

# web_fetch 抓取结果页，解析 .gs_ai_name + .gs_ai_aff
web_fetch "https://scholar.google.com/citations?view_op=search_authors&mauthors=anthropic.com"
```

**输出格式：** `{name, scholar_id, verified_email_domain, affiliation_text, research_interests}`

**与 Layer 3「逐人 Scholar 查询」的本质区别：**
- **Layer 1B（本方法）：** 不知道候选人是谁 → 直接搜索平台 → 拿到名单
- **Layer 3：** 已知候选人姓名 → 去 Scholar 查她的详情/研究方向

**本项目状态：** ✅ 已部分执行（§27.2 方法 A，作为 Scholar Coauthor 种子的补充）

---

#### 方法 G：X (Twitter) Bio 批量搜索（Layer 1B）

**定义：** 在 X 平台搜索 Bio 字段含「Anthropic」关键词的公开账号，批量产出候选人名单。

**原理：** X 用户普遍在 Bio 中自报所在机构（如「Researcher @Anthropic」），且更新比 LinkedIn 更及时（尤其对刚入职或刚离职的员工）。

**执行方法：**
```bash
# 方法一：X 高级搜索（免费，覆盖有限）
# X 搜索栏：Anthropic (filter:verified OR -filter:verified) min_faves:0
# 再手动过滤 Bio 含 Anthropic 的账号

# 方法二：搜索引擎辅助
web_search 'site:x.com "Anthropic" "researcher" OR "engineer" OR "scientist"'
# 注意：搜索引擎对 X 的索引覆盖率有限，结果不完整

# 方法三：X API（付费，最完整）
# GET /2/users/search?query=Anthropic&user.fields=description,name,username
# 过滤条件：description 含 "Anthropic"
```

**局限：**
- Bio 为用户自填，存在滞后（离职后未更新）或夸大（声称在 Anthropic 但实为合作）
- X 免费 API 搜索用户的能力极有限；搜索引擎辅助覆盖率低
- 噪音较高：包含提及 Anthropic 产品的非员工

**实用建议：** 用搜索引擎辅助方式做补充扫描（成本低），不依赖 X API（成本高且限制多）。

**本项目状态：** ⬜ 未执行

---

#### 方法 H：GitHub Bio/Profile 批量扫描（Layer 1B）

**定义：** 通过 GitHub API 或搜索，找到 Company 字段填写为 `@anthropic`/`Anthropic` 的 GitHub 用户。

**原理：** GitHub 的 Company 字段由用户自填，工程师群体习惯维护 GitHub Profile，覆盖 LinkedIn 上不活跃的纯工程型员工。

**执行方法：**
```bash
# GitHub Users Search API（免费，需 token）
curl -H "Authorization: token {GITHUB_TOKEN}" \
  "https://api.github.com/search/users?q=company:anthropic&per_page=100"

# 遍历翻页直到结果为空
# 提取：login, name, company, bio, location, email, blog
```

**输出格式：** `{github_login, name, company, bio, email, blog_url}`

**局限：**
- 仅覆盖有 GitHub 账号且公开 Company 字段的工程师
- Company 字段无验证机制，准确率低于 Google Scholar verified email
- 覆盖率低（~5-10%，主要是工程型员工）

**本项目状态：** ⬜ 未执行

---

#### Layer 1B vs Layer 3 操作对照表

| 使用场景 | 方法 | 归属 |
|---------|------|------|
| 搜索「所有 verified email at anthropic.com」的 Scholar 账号 | Scholar Bio 批量扫描 | **Layer 1B**（方法 F）|
| 查「张三」的 Scholar 主页，补全研究方向 | Scholar 单人查询 | **Layer 3** |
| 搜索「所有 Bio 含 @Anthropic 的 X 账号」 | X Bio 批量搜索 | **Layer 1B**（方法 G）|
| 查「张三」的 X 账号，确认她是否还在 Anthropic | X 单人查询 | **Layer 3** |
| 搜索 GitHub company:anthropic 的全部用户 | GitHub Bio 批量扫描 | **Layer 1B**（方法 H）|
| 查「张三」的 GitHub 个人主页，看项目 | GitHub 单人查询 | **Layer 3** |

---

## §32 Hunter.io Domain Search — 邮箱域名批量成员获取

> **方法定位：** Layer 1A（组织/成员数据库）。通过公司邮箱域名批量拉取所有已被 Hunter.io 收录的员工信息，包含姓名、职位、职级、部门、LinkedIn URL。

### 32.1 Hunter.io API 基本参数

| 项目 | 内容 |
|------|------|
| **API Base** | `https://api.hunter.io/v2/` |
| **核心端点** | `GET /v2/domain-search` |
| **参数** | `domain=anthropic.com`、`limit`、`offset`、`api_key` |
| **返回字段** | `first_name`、`last_name`、`position`、`seniority`（executive/senior/junior）、`department`、`linkedin`、`twitter`、`confidence`（置信度 0-100）、`email` |
| **账号配置文件** | `data/hunter/hunter_accounts.json` |
| **调用脚本** | `data/hunter/call_hunter.py` |

### 32.2 Free tier 严重限制（⚠️ 必读）

| 限制项 | Free tier | Starter（$34/月）|
|--------|-----------|-----------------|
| **每次调用最多返回** | **10 条（不可翻页）** | 100 条（可翻页）|
| 月度 searches | 60 次 | 24000 credits/年 |
| **Domain Search 实际覆盖率** | **1.8%（10/565）** | **100%（565/565，需 6 次翻页）** |

**⚠️ Free tier 核心缺陷：offset > 0 直接返回 400 `pagination_error`，无法翻页，只能获取前 10 条（排序由 Hunter 决定，通常是置信度最高的 executive 层）。**

**Anthropic.com 实测（2026-04-05）：**
- 总记录：**565 条**
- Free tier 可获取：**10 条**（全为 C-suite / Director，无华人）
- 升级 Starter 后可全量获取：565 条，需 6 次翻页（6 credits）

### 32.3 额度管理

```python
# 调用前检查（call_hunter.py 已内置）
with open("data/hunter/hunter_accounts.json") as f:
    config = json.load(f)
acc = config["accounts"][0]
available = acc["monthly_search_limit"] - acc["total_searches_used"]
if available < calls_needed + 2:  # 保留 2 次余量
    print(f"🚫 额度不足，需 {calls_needed}，剩 {available}")
    exit()
```

**Free tier 月度配额（2026-04 实测）：**
- 60 searches/月（2026-04-05 已用 4 次，剩 56 次）
- 但翻页无效，56 次额度对全量拉取无意义

### 32.4 调用方法（标准 Python 原子落盘）

> ⚠️ **使用 `data/hunter/call_hunter.py` 脚本，不要手动 curl**（教训同 The Org：curl 输出可能被截断）

**Free tier 执行（获取前 10 条）：**
```bash
cd /home/node/.openclaw/workspace/Anthropic华人专项
python3 data/hunter/call_hunter.py
```

**Starter 及以上执行（全量 565 条）：**
脚本已支持翻页，升级账号后直接运行即可，无需修改。

**存档文件命名规范：**
```
data/hunter/
├── hunter_accounts.json                    ← 账号 + 月度用量追踪
├── domain_search_{slug}_{date}.json        ← 原始 API 返回（原子落盘）
│   示例: domain_search_anthropic_2026-04-05.json
└── call_hunter.py                          ← 调用脚本（含额度守卫+幂等检查）
```

### 32.5 返回数据结构

```json
{
  "_meta": {
    "domain": "anthropic.com",
    "organization": "Anthropic",
    "email_pattern": "{first}{l}",
    "fetched_at": "2026-04-05",
    "total_reported": 565,
    "total_fetched": 10,
    "per_page": 10
  },
  "emails": [
    {
      "value": "xxx@anthropic.com",
      "first_name": "Daniela",
      "last_name": "Amodei",
      "position": "President",
      "seniority": "executive",
      "department": "executive",
      "confidence": 92,
      "linkedin": "https://www.linkedin.com/in/daniela-amodei-790bb22a",
      "twitter": "danielaamodei"
    }
  ]
}
```

### 32.6 华人初筛方法

```python
def screen_chinese_hunter(domain_search_file, excel_known_names):
    """对 Hunter.io 数据执行华人初筛 + 去重"""
    CHINESE_SURNAMES = {
        'Li','Zhang','Wang','Chen','Liu','Yang','Huang','Lin','Wu','Zhou',
        'Xu','Sun','He','Gao','Ma','Ye','Luo','Zheng','Tang','Song','Han',
        'Feng','Deng','Jiang','Cai','Yan','Xie','Xiao','Liang','Wei','Pan',
        'Bao','Zhu','Hu','Cheng','Lu','Cui','Dong','Shen','Meng','Fu','Lei',
        'Peng','Wen','Jia','Du','Dai','Ding','Jin','Guo','Yu','Tong',
        'Tsai','Hsu','Liao','Fung','Rao','Gu','Shi','Qi','Zhong','Tan',
        'Xia','Hong','Qin','Su','Yao','Cao','Hou','Chang','Zhao',
        'Chan','Ng','Cheung','Kwok','Leung','Yip','Tam',
    }
    with open(domain_search_file) as f:
        data = json.load(f)
    
    candidates = []
    for e in data["emails"]:
        last = (e.get("last_name") or "").strip()
        if last not in CHINESE_SURNAMES:
            continue
        name = f"{e.get('first_name','')} {last}".strip()
        # 与 Excel 已知名字对比（normalized）
        if name.lower() in excel_known_names:
            status = "already_in_excel"
        else:
            status = "new_candidate"
        candidates.append({
            "name": name,
            "position": e.get("position",""),
            "seniority": e.get("seniority",""),
            "department": e.get("department",""),
            "linkedin": e.get("linkedin",""),
            "confidence": e.get("confidence",0),
            "status": status,
        })
    return candidates
```

### 32.7 Anthropic 扫描结果（2026-04-05）

| 指标 | 结果 |
|------|------|
| **总记录（API 报告）** | **565 条** |
| **实际获取（Free tier）** | **10 条**（offset 无法翻页）|
| **华人候选** | **0 人**（前 10 条全为 C-suite/Director，无华人）|
| **已用 searches** | 4/60（本月） |
| **存档文件** | `data/hunter/domain_search_anthropic_2026-04-05.json` |
| **下次可全量拉取** | 升级 Starter（$34/月）后，约需 6 次翻页即可获取全部 565 条 |

### 32.8 升级价值评估

| 方案 | 成本 | 获取记录 | 预估华人候选 | 覆盖率 |
|------|------|---------|------------|--------|
| **当前 Free tier** | $0 | 10条 | 0（已验证）| 1.8% |
| **升级 Starter** | $34/月 | 565条 | 估计 50-80 人华人候选（含已知）| 100% |
| **RapidAPI LinkedIn（已有）** | 按次计费 | ~600+人（已执行）| 约 78 人已发现 | ~40% |

**结论：** Hunter.io 的核心价值在于提供**带职级分类（seniority）、部门（department）和 LinkedIn URL 的邮箱列表**，与 LinkedIn company/people API 有较高重叠，但可互补（Hunter 收录了 LinkedIn 不可见的人）。升级 Starter 后约 $34/月可获全量 565 条，**性价比中等，若本项目覆盖率已接近上限，优先级不高。**

---

*Skill 版本：v5.9 | 更新时间：2026-04-05*
*变更：*
*§29.3 The Org API 调用方法从 curl 重写为 Python 原子落盘脚本（先写.tmp再rename，防截断数据丢失；含幂等性检查和额度守卫）；首次调用数据丢失教训记录*
*§29.7 The Org Anthropic 扫描结果修正：节点数从"~450"更正为"81节点（数据不完整）"；新增数据不完整原因说明*
*§30.4 新增三个工作流优化点：30.4.1 整合去重规范（多渠道先合并再核实）；30.4.2 阶段门1批量决策流程（候选决策表格式/触发条件/禁止直接写入规则）；30.4.3 The Org 首字母缩写节点专项处理流程（extract_abbreviated_nodes + Google 搜索补全）*
*§30.5 方法选择决策树更新（引用新增章节30.4.2/Python脚本/arXiv ACK优先级）*
*§29 修正 The Org Free tier 限额（10 credits/月=1次调用/月）；更新 Jiasen Yang 发现记录（Row 101）*
*§30 重构方法分层架构（三层+子类精确定义；Layer 1 拆分为 1A/1B/1C；核心判据澄清：「批量产出候选名单」vs「逐人确认信息」vs「沿关系链扩展」；明确 Google Scholar/X Bio/GitHub Bio 批量扫描属于 Layer 1B 而非 Layer 3；工作流图加入 1A/1B/1C 子类）*
*§31 更新：arXiv/学术论文扫描 + Bio批量过滤方法体系（方法D=arXiv作者Layer1C；方法E=ACK致谢Layer2；新增方法F=Google Scholar Bio批量Layer1B；方法G=X Bio批量Layer1B；方法H=GitHub Bio批量Layer1B；Layer1B vs Layer3操作对照表）*
