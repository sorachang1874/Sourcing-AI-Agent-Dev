# Skill: investor-chinese-scan

> **适用场景：** 针对特定 AI/科技公司（如 Anthropic）所有参与过投资的机构，定位并整理其内部华人/华裔成员，产出两张结构化表格——「投融资历史与华人网络」和「主要投资方华人成员」，并发布到 Redoc。

---

## 一、整体工作流（七步法）

```
Step 1: 获取投资机构名单（多源核实）
    ↓
Step 2: 产出「投融资历史与华人网络」初稿（标准化格式，华人网络线索留空）
    ↓
Step 3: 批量获取机构成员名单（API 原始结果存档）
    ↓
Step 4: 搜索核实 LinkedIn 链接 + 初步填写华人成员字段
    ↓
Step 5: ⚠️ 用户人工核查 LinkedIn 链接（阶段门，需用户确认后继续）
    ↓
Step 6: 根据用户需求调用 API 爬取个人 Profile，精细化更新华人成员字段
    ↓
Step 7: 根据「是否涉及Anthropic投资」更新华人网络线索列，产出 Excel + Redoc
```

> **关键原则：** Step 6 个人 Profile 爬取属于消耗 API 额度的操作，**必须在用户确认 LinkedIn 链接无误后**方可执行，不可自动触发。

---

## 二、Step 1：获取投资机构名单

**信息来源优先级（依次尝试）：**

1. **目标公司官网 newsroom**（最权威）  
   `web_fetch "https://www.anthropic.com/news"` → 筛选融资公告

2. **Crunchbase 融资历史页**  
   `web_fetch "https://www.crunchbase.com/organization/anthropic/funding_rounds"`  
   免费版仅部分可见，适合核实投资方名单，不保证完整轮次金额

3. **主流媒体报道**（TechCrunch / Bloomberg / Fortune / 36氪）  
   `web_search "Anthropic Series G investors 2026"`  
   中文媒体补充：`web_search "Anthropic G轮 投资方 2026"`

4. **交叉验证**：多源信息不一致时，以官方公告为准；若官方未披露，需在表格中注明「未经官方确认」

---

## 三、Step 2：「投融资历史与华人网络」初稿

### 3.1 表格字段规范

| 字段 | 说明 | 示例 |
|------|------|------|
| **融资轮次** | 标准化写法：天使轮/种子轮、Series A、Series B、Series C1（若同轮有多笔则加序号）、债务融资 | `Series C1` |
| **时间** | 年月，格式 `YYYY年M月` | `2023年5月` |
| **金额** | 含单位，未披露则写「未披露」| `$4.5亿` |
| **投后估值** | 含单位，未披露则写 `—` | `$41亿` |
| **投资方（领投方→跟投方）** | 领投方和跟投方**换行**分列，格式见下 | 见下方示例 |
| **华人网络线索** | Step 7 最后填写，格式：`**姓名**（机构）：一句话说明参与情况` | 见 Step 7 |

**投资方字段格式示例：**
```
领投：Spark Capital
跟投：Google、Salesforce Ventures、Sound Ventures、Zoom Ventures
```
若仅有一方投资：
```
领投：Amazon
```

### 3.2 信息真实性原则

- **严格核实**：每条投资方信息须有可溯源依据（官方公告 / 主流媒体），不凭记忆填写
- **轮次命名**：同一时间段内多笔融资用 C1/C2/C3 等区分；战略投资/债务融资单独成行
- **估值处理**：若媒体报道与官方公告不符，优先采用官方数据并注明；若均为估算，注明「媒体估计」
- **「—」使用**：未披露信息一律用 `—`，不猜测、不留空

---

## 四、Step 3：批量获取机构成员名单

### 4.1 机构规模分级

先通过 LinkedIn 公司主页「员工数」字段判断规模，决定扫描路径：

| 机构规模 | 扫描方法 |
|---------|---------|
| **大型（&gt;200人）** | 官网 Team 页爬取 + 搜索引擎定向查找（API 覆盖率 &lt;15%，不值得调用） |
| **中小型（≤200人）** | RapidAPI LinkedIn 批量扫描（主力） |

**常见分级参考：**
- 大型：Sequoia、a16z、GV、Lightspeed、Fidelity、BlackRock、Amazon、Salesforce、SoftBank、ICONIQ、Bessemer、GIC
- 中小型：Spark Capital、Menlo Ventures、DFJ Growth、SV Angel、WndrCo、Scale VP、Altimeter、Coatue、Greenoaks、D1 Capital、Founders Fund、Bond Capital

### 4.2 大型机构：官网 + 搜索引擎

```bash
# 官网 Team 页（优先）
web_fetch "https://lsvp.com/team/" maxChars=50000
web_fetch "https://www.sequoiacap.com/people/"

# SPA 渲染页（部分机构需用 browser 工具）
# ⚠️ Cloudflare 保护的页面（General Catalyst/Spark Capital）会拦截自动化，改用搜索补充

# 搜索引擎定向查找华裔成员
web_search 'site:linkedin.com "Sequoia Capital" "partner" "Tsinghua" OR "Peking" OR "Fudan"'
web_search '"ICONIQ Capital" Chinese investor partner 2025'
web_search 'GIC 新加坡政府投资 华人 科技团队 2025'

# Forbes Midas List 等华裔 VC 榜单交叉比对
web_search 'Forbes Midas List 2025 Chinese American VC'
```

### 4.3 中小型机构：RapidAPI LinkedIn 批量扫描

**主力 API：Z Real-Time LinkedIn Scraper API（account_005 / account_009）**

```
Host: z-real-time-linkedin-scraper-api1.p.rapidapi.com
Endpoint: GET /api/company/people
Params: username={company_slug}&page={n}&limit=50
月额度: 100次/账号
单次返回: 最多50条，支持翻页（hasMore 字段）
```

```bash
curl --request GET \
  --url 'https://z-real-time-linkedin-scraper-api1.p.rapidapi.com/api/company/people?username=spark-capital&page=1&limit=50' \
  --header 'x-rapidapi-host: z-real-time-linkedin-scraper-api1.p.rapidapi.com' \
  --header 'x-rapidapi-key: {API_KEY}'
```

**翻页策略：** 检查 `hasMore` 字段，为 `true` 则继续 page+1，直到 `hasMore=false` 或整页全为 headless 停止

**过滤规则：** `fullName="LinkedIn Member"` 的 headless 用户（隐私设置严格）直接过滤

**备用 API：** account_006（hamoureliasse，POST 方式，随机抽样，适合 &lt;50人小机构）；account_005/009 耗尽后使用

**额度管理：** 每次调用后即时更新 `api_accounts.json` 中的 `total_used`；达 stop_at 阈值立即停止

**⚠️ 重要：** API 返回原始结果须存档至 `linkedin_profiles/raw/{institution_slug}_members.json`，作为**数据资产**，避免重复消耗额度。

---

## 五、Step 4：搜索核实 LinkedIn 链接 + 初步填写字段

拿到成员名单后，不急于调用 Profile API，先通过搜索引擎初步核实身份：

### 5.1 华人识别

| 判定维度 | 方法 | 说明 |
|---------|------|------|
| 姓名特征 | 匹配常见华人姓氏（Li/Zhang/Wang/Chen/Liu/Yang/Huang/Lin/Wu/Zhou 等，及粤语/闽南语拼音变体） | 初步筛选 |
| LinkedIn headline | 含中国高校关键词（Tsinghua/Peking/Fudan/SJTU/CUHK/NUS 等） | 高置信 |
| 搜索引擎核实 | `"{姓名}" "{机构名}" site:linkedin.com` | 确认 LinkedIn slug |
| 媒体/官网报道 | `"{姓名}" "{机构名}" Chinese background` | 补充背景信息 |

### 5.2 初步填写「主要投资方华人成员」字段

此阶段**不调用 Profile API**，仅基于搜索结果填写可确认字段：

| 字段 | 数据来源 | 填写原则 |
|------|---------|---------|
| 姓名 | API 返回 fullName + 搜索核实 | 见下方格式规范 |
| 职位 | LinkedIn headline / 官网介绍 | 尽量写全职位名 |
| 族裔 | 搜索引擎 + 姓名 + 教育背景推断 | 区分：华裔美国人/大陆华人/新加坡华裔/香港裔/台湾裔 |
| 是否涉及 Anthropic 投资 | 搜索结果 + LinkedIn headline 判断 | 严格标准，见下方 |
| 投资方向 | LinkedIn headline + 机构官网 | 精简到具体赛道 |
| 备注 | 教育背景 + 主要经历 | 客观陈述，不推断动机 |
| LinkedIn | 搜索得到的 slug | 格式：`linkedin.com/in/{slug}` |

---

## 六、字段格式规范详解

### 6.1 「姓名」字段

**格式：英文名在前，中文名换行在后，仅有非常确定的中文名时才写**

```
Alfred Lin
林君叡
```
```
Emily Zhao
```
（无确认中文名则仅写英文名）

**不要分两列**（「姓名（英文）」和「中文名/昵称」），统一为一列「姓名」。

### 6.2 「是否涉及 Anthropic 投资」字段

**严格判断，采用三级标注：**

| 标注 | 含义 | 判定依据 |
|------|------|---------|
| `⭐ 是（…）` | 确认直接参与 Anthropic 投资决策 | LinkedIn 本人写明 / 公开媒体采访点名 / 机构官方披露 |
| `可能（…）` | 所在机构参与了 Anthropic 融资，本人具体参与度未确认 | 机构参与融资 + 本人为投资相关岗位 |
| `否（…）` | 明确排除 | 本人为非投资岗（人才/法务/财务/行政等）/ 公开采访明确说明未参与 |

**括号内补充一句理由，例如：**
- `⭐ 是（Bessemer deal lead，Series D起主导Anthropic投资，Bloomberg采访确认）`
- `可能（所在机构Lightspeed为Series E4 $35亿领投方；个人具体参与待确认）`
- `否（General Catalyst人才合伙人，非投资决策）`

**特别注意：** 「可能」不等于「未知」，要写清楚机构参与了哪轮融资，让读者能快速判断重要性。

### 6.3 「备注」字段

**格式：客观经历总结，不超过3句话，按「教育 → 主要职业轨迹 → 与 Anthropic 相关亮点」顺序**

```
斯坦福统计+计算生物学；在Genentech做转化肿瘤学研究；Sequoia合伙人，专注生物科技/精准医疗/AI健康。
```

**禁止行为：**
- 不写主观评价（「判断力敏锐」「眼光独到」等）
- 不从行为反推动机（「说明她看好 Anthropic 的长期价值」等）
- 不写未经证实的信息

---

## 七、Step 5：阶段门——用户人工核查 LinkedIn 链接

**执行条件：** 完成 Step 4 初稿后，**必须暂停**，将华人成员初稿（含搜索得到的 LinkedIn 链接）呈现给用户确认。

**呈现格式示例：**

> 以下为 {机构名} 初步识别的华裔成员，请确认 LinkedIn 链接无误后，我将调用 Profile API 补全详细信息：
>
> | 姓名 | 职位（初步） | LinkedIn | 是否调用 Profile API |
> |------|------------|---------|---------------------|
> | Alfred Lin | Partner, Sequoia | linkedin.com/in/alfredlin | ☐ 是 / ☐ 否 |

**用户确认 → 继续 Step 6**  
**用户修正链接 → 更新后继续 Step 6**  
**用户标记「不需要调用 API」→ 跳过 Step 6，直接 Step 7**

---

## 八、Step 6：Profile API 爬取（用户确认后执行）

### 8.1 主力 Profile API：enrichgraph（account_008）

**选用理由：** 免费版无字段遮蔽，返回字段最完整（含 experience 精确月份 + education 学位专业 + skills）

```bash
curl --request GET \
  --url 'https://real-time-linkedin-data-scraper-api.p.rapidapi.com/people/profile?profile_id={slug}&bypass_cache=false&include_contact_info=false&include_network_info=false' \
  --header 'x-rapidapi-host: real-time-linkedin-data-scraper-api.p.rapidapi.com' \
  --header 'x-rapidapi-key: {API_KEY}'
```

**关键字段：**
- `experience[]`：历任公司 + 职位 + 精确月份（若 description 中含 Anthropic，置信度极高）
- `education[]`：学校 + 学位 + 专业
- `headline`：当前职位简介

**结果存档路径：** `linkedin_profiles/{slug}.json`（避免重复调用）

### 8.2 备用 Profile API：ugoBoy（account_003）

account_008 额度耗尽时使用，endpoint：`/profile/detail?username={slug}`

### 8.3 Profile 数据如何更新华人成员字段

从 Profile API 结果中补全以下字段：
- **职位**：从 `experience[0].title` + `experience[0].companyName` 更新
- **族裔**：从 `education[]` 中学校名更新（如「清华大学」→ 大陆华人）
- **是否涉及 Anthropic 投资**：若 `experience[].description` 中含 `Anthropic` → 升级为 `⭐ 是`
- **投资方向**：从 `headline` + 历任职位提炼
- **备注**：从 `education[]` + `experience[]` 按格式重新整理

---

## 九、Step 7：更新华人网络线索列，产出输出物

### 9.1 「华人网络线索」字段更新逻辑

遍历「主要投资方华人成员」表中所有「是否涉及 Anthropic 投资」标注为 `⭐ 是` 的人物，按以下格式更新对应融资轮次的「华人网络线索」列：

**格式：**
```
**姓名**（机构）：一句话说明参与情况（来源/依据）
```

**多人时换行分列（使用 <split/> 分隔，适用于 Redoc 表格）：**
```
**Emily Zhao**（Salesforce Ventures）：Salesforce Ventures MD，Anthropic Series A deal负责人；LinkedIn headline直接标注
<split/>
**Eric Yuan / 袁征**（Zoom Ventures）：Series A和C1均参投；最早参与Anthropic融资的华人创始人/CEO投资者
```

**「—」情况：** 该轮次无任何确认华人参与，填 `—`

**「可能」人物处理：** 「可能」参与者**不写入**「华人网络线索」列，只在「主要投资方华人成员」表中保留

### 9.2 Excel 输出规范

Sheet 设计（参考 v6u 版本）：
- **Sheet1：在职华人员工**
- **Sheet2：已离职华人员工**
- **Sheet3：投融资历史与华人网络**
- **Sheet4：主要投资方华人成员**

Sheet3「投融资历史与华人网络」列顺序：融资轮次 / 时间 / 金额 / 投后估值 / 投资方（领投方→跟投方）/ 华人网络线索

Sheet4「主要投资方华人成员」列顺序：机构 / 姓名 / 职位 / 族裔 / 是否涉及Anthropic投资 / 投资方向 / 备注 / LinkedIn

### 9.3 Redoc 输出规范

**参考文档：** `https://docs.xiaohongshu.com/doc/a7718691261c275a32a13ff5a9fa136a`

**格式要点（基于参考文档实际样式）：**

1. **文档头部**：简短说明（数据截止时间、版本号）+ 「小结」卡片（含融资历史和华人网络的 summary 数据）

2. **Section 1：融资历史**（表格）  
   - 使用 Redoc 表格，单元格内多条投资方用 `<split/>` 分隔换行  
   - 华人网络线索列：姓名加粗（`**姓名**`），机构名加括号，多人用 `<split/>` 分隔

3. **Section 2：主要投资方华人成员**  
   - 按机构分组，每组一个小标题（机构名 + 人数 + 已确认参与人数）  
   - 每组一张表，表头：姓名 / 职位 / 族裔 / 参与 Anthropic / 投资方向 / 备注 / LinkedIn  
   - LinkedIn 列用 `[→](url)` 形式呈现  
   - 已确认参与者姓名前加 `⭐`

4. **文档尾注**：数据来源说明、置信度说明（⭐ / 可能 / 否 的含义）

---

## 十、API 账号矩阵（快速参考）

| 账号 | 用途 | 月额度 | 当前状态 | Endpoint |
|------|------|------|---------|---------|
| account_005 | 批量扫描公司成员列表（主力） | 100次 | 可能剩余有限 | `GET /api/company/people?username={slug}&limit=50` |
| account_009 | 批量扫描公司成员列表（备用，同 API 不同 Key） | 100次 | 可能剩余有限 | 同 account_005 |
| account_008 | 个人 Profile 详情（主力，字段最完整）| 100次 | 可用 | `GET /people/profile?profile_id={slug}` |
| account_006 | 小机构补漏（&lt;50人，随机抽样） | 100次 | 可用 | `POST /`，body: `{"companyUrl": "..."}` |
| account_003 | 个人 Profile 详情（备用） | 50次 | 剩余有限 | `GET /profile/detail?username={slug}` |
| account_001 | ~~已用满，停用~~ | — | exhausted | — |
| account_002 | ~~已停服，废弃~~ | — | stopped | — |
| account_004 | 分页获取员工列表（按 numeric company_id）| 50次 | exhausted | `GET /api/v1/company/people?company_id={numeric_id}` |

**账号状态文件：** `Anthropic华人专项/api_accounts.json`（每次调用后实时更新 total_used）

---

## 十一、注意事项

1. **「华人」定义**：广义华人（大陆/台湾/香港/新加坡华裔均纳入），分析时区分背景
2. **隐私保护**：所有数据来自公开 LinkedIn 信息，不涉及非公开数据
3. **时效性**：人员变动频繁，建议每季度更新；存档文件标注数据截止时间
4. **信息真实性优先**：宁可留空注「待核实」，不凭推断填写，尤其是「是否涉及 Anthropic 投资」字段
5. **API 额度保护**：Step 6 之前不调用个人 Profile API；存档原始结果避免重复消耗

---

## 十二、已积累数据快速参考（Anthropic 专项）

**已确认华人关键决策人（⭐）：**

| 机构 | 姓名 | 职位 | 参与轮次 |
|------|------|------|---------|
| GIC | Lim Chow Kiat / 林周凯 | Group CEO | F1→F2→G（连续三轮领投） |
| GIC | Jeffrey Jaensubhakij / 杨健明 | Group CIO | F1/F2/G |
| GIC | Choo Yong Cheen | CIO, Private Equity | F1/F2/G |
| GIC | Bryan Yeo | Group CIO（2025年4月起） | G |
| Sequoia | Alfred Lin / 林君叡 | Partner | F2/G |
| Sequoia | Sonya Huang | Partner | F2/G |
| Coatue | Jade Lai | General Partner | F1/F2/G |
| a16z | Jonathan Lai | General Partner | E/F |
| Lightspeed | Justin Shen | Partner | E4（LinkedIn 写明） |
| GV | Crystal Huang | General Partner | B/C |
| Bessemer | Grace Ma | Investor | D/E4（deal lead，$4亿+） |
| General Catalyst | Paul Kwan | Managing Director | E4/F1 |
| Salesforce Ventures | Emily Zhao | Managing Director | A |
| WndrCo | ChenLi Wang | General Partner | F/G |
| SV Angel | Mike Sho Liu | General Partner | Seed/A |
| Altimeter | Pauline Yang | — | F2（LinkedIn 写明） |
| Baillie Gifford | Lillian Li | Investment Professional | G（LinkedIn 写明） |
| Zoom Ventures | Eric Yuan / 袁征 | 创始人兼CEO | A/C1 |

**相关文档：**
- 华人成员全表：`Anthropic华人专项/vc-directory-v1.md`
- LinkedIn API 教程：`https://docs.xiaohongshu.com/doc/6670ed5441500c55dd389bd5a52c1d89`
- Anthropic 调研流程文档：`https://docs.xiaohongshu.com/doc/2454d39e5975288af6cc89291d551e06`
- Redoc 输出参考：`https://docs.xiaohongshu.com/doc/a7718691261c275a32a13ff5a9fa136a`

---

*Skill 版本：v2.0 | 更新时间：2026-04-02 | 基于 Anthropic 华人专项 v6u 实战经验整理*
