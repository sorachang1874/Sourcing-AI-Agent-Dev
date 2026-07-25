---
name: biz-visit-onepager
description: |
  商务拜访背景调研 one-pager 生成技能。在商务会面/拜访前，对目标人物进行全面背景调研并生成结构化简报文档（one-pager）。
  适用对象：AI研究员、创业公司创始人、投资人、科技公司高管。
  触发场景：用户说"帮我调研一下 XXX"、"生成 XXX 的 one-pager"、"XXX 的背景资料"。
---

# 商务拜访 One-Pager 生成技能（v4）

---

## ⚠️ 四条硬性规范（每篇发布前必须逐项确认）

### 规范0：信息源优先级——本地 JSON 优先，搜索只做补充

**每次生成 one-pager 前，必须先读本地 LinkedIn JSON 文件，以 JSON 中的数据为准：**

```python
# 必须先执行：
# 路径：/home/node/.openclaw/workspace/Anthropic华人专项/linkedin_profiles/{slug}.json
# 读取字段：experience[] / education[] / basic_info.headline / basic_info.current_company
```

**信息优先级：Excel 中文名 > 本地 JSON 职位/雇主 > 搜索结果 > 记忆中的信息。**

#### 中文名权威来源：搜索确认 > 一切

中文名**必须经过搜索工具核实**，以公开来源（个人主页、媒体报道、论文署名）为准：

```
✅ 可信来源：个人主页自述 / 媒体报道明确标注 / Google Scholar 显示
❌ 不可信来源：Excel 用户笔记 / 拼音推断 / 记忆
```

**核实流程（每人必须执行）：**
```
web_search: "{英文名} {机构} "{候选中文名1}" OR "{候选中文名2}"
→ 有明确来源命中 → 使用该中文名
→ 搜索无法区分 / 无结果 → 只写英文名，不标注中文名
```

❌ 真实错误案例（2026-04-01，来自 Excel 与拼音推断）：

| 姓名 | ❌ Excel/推断版本 | 核实结论 |
|------|----------------|--------|
| Shunyu Yao | — | ✅ **姚顺宇**（媒体报道确认） |
| Da Yan | — | ✅ **闫达**（Excel+无反例） |
| Haochen Zhang | 张皓辰 | ❌ 本人官网署名"Jeff Z. HaoChen"，**无中文名** |
| Daliang Li | 李大亮 | ❌ 搜索同时出现"李大亮"和"李大量"，**无法确认** |
| Hongbin Chen | 陈宏彬 | ❌ 搜索只找到无关同名人，**无法确认** |
| Kevin Lin | 林牧/林凯文 | ❌ LinkedIn 仅显示"Mu (Kevin) Lin"，**无法确认** |
| Yijie Zhu | 朱一洁/朱艺杰 | ❌ 无搜索证据，**无法确认** |
| Shuyi Zheng | 郑淑仪/郑书一 | ❌ 无搜索证据，**无法确认** |
| Xiuruo Zhang | 张修若/张秀若 | ❌ 无搜索证据，**无法确认** |
| Shu Wu | 吴舒/吴淑 | ❌ 无搜索证据（"吴书"是另一人），**无法确认** |
| Yizhi Zhao | 赵逸智/赵一至 | ❌ 无搜索证据，**无法确认** |
| Mengyi Xu | 许梦怡/徐梦漪 | ❌ 无搜索证据，**无法确认** |
| Yue Ning | 宁越/宁悦 | ❌ 无搜索证据，**无法确认** |

**原则：宁缺毋滥。写错中文名比不写更糟糕。**

#### 职位/当前雇主权威来源：本地 JSON

❌ 严禁的幻觉场景（来自真实错误案例 2026-04-01）：
- Shunyu Yao 的 LinkedIn JSON 清楚显示 `current_company: Google DeepMind`
- 但之前生成的 one-pager 写成了"OpenAI | Researcher，主导 CUA/Deep Research"
- **根因：写 one-pager 时没有读本地 JSON，而是依赖了记忆中混淆的信息**

✅ 正确流程：
1. 先从 Excel 读取中文名
2. 再 `read` 本地 JSON 文件，提取 `experience[0]`（最新工作）、`basic_info.current_company`
3. 以上述数据为权威，搜索结果只用于补充细节（论文、采访等）

---

### 规范1：姓名格式——英文在前，中文在括号里

```
✅ Yuntao Bai（白云涛）· MTS · Anthropic
✅ Jerry Wei · MTS · Anthropic          ← 无确认中文名，只写英文
❌ 白云涛（Yuntao Bai）· ...            ← 禁止中文在前
❌ 张晓怡（Xiaoyi Zhang）· ...          ← 禁止中文在前
```

### 规范2：工作履历——严格时间逆序（最新在前）

```
✅ 【2025.10–至今】OpenAI → 【2021.07–2025.10】Anthropic → 【2018–2021】Caltech
❌ 【2021.07–2025.10】Anthropic → 【2025.10–至今】OpenAI   ← 禁止
```

### 规范3：照片——必须通过 GitHub 图床

Redoc CSP 白名单（2026-03-31 实测）：
- ✅ `raw.githubusercontent.com` — 可渲染
- ✅ `cs.stanford.edu` 等大学主页 — 可渲染
- ❌ `media.licdn.com` — 被过滤（连破图都不显示）
- ❌ `avatars.githubusercontent.com` — 不渲染

**上传脚本**（固定参数，每次批量生成前执行）：
```python
import urllib.request, base64, json

GITHUB_TOKEN = "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"
GITHUB_USER  = "kasuganosora1874"
GITHUB_REPO  = "one-pager-photos"

def upload_linkedin_photo(photo_url: str, filename: str) -> str:
    """下载 LinkedIn 头像 → 上传 GitHub → 返回 raw URL"""
    img = urllib.request.urlopen(
        urllib.request.Request(photo_url, headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Referer': 'https://www.linkedin.com/',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        }), timeout=12).read()
    
    api = f"https://api.github.com/repos/{GITHUB_USER}/{GITHUB_REPO}/contents/{filename}.jpg"
    gh_h = {'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json'}
    sha = None
    try:
        sha = json.load(urllib.request.urlopen(
            urllib.request.Request(api, headers=gh_h), timeout=8)).get('sha')
    except: pass
    
    payload = {"message": f"Add {filename}", "content": base64.b64encode(img).decode()}
    if sha: payload["sha"] = sha
    result = json.load(urllib.request.urlopen(
        urllib.request.Request(api, data=json.dumps(payload).encode(),
                               headers=gh_h, method='PUT'), timeout=15))
    return result['content']['download_url']

# filename 命名：LinkedIn slug 转连字符小写
# jerryweiai → jerry-wei, xiaoyiz → xiaoyi-zhang
```

若 LinkedIn JSON 无头像 URL：写 `> 📷 照片：未找到公开照片。`

---

## 生成流程

### Step 1：从 LinkedIn JSON 提取结构化数据

路径：`/home/node/.openclaw/workspace/Anthropic华人专项/linkedin_profiles/{slug}.json`

提取字段：`experience[]`（company/title/start_date/end_date/is_current/description）、`education[]`、`about`、`profile_picture_url`

### Step 2：联网多源补充调研（必须执行，不可跳过）

对每个人执行以下搜索，结果直接写入 one-pager：

```
# 通用（所有人）
web_search: "[name] [company] site:x.com"           → X账号 + 近期内容
web_search: "[name] [company] interview OR talk 2024 2025"  → 媒体采访/演讲

# AI研究员（额外必查）
web_search: "[name] google scholar citations"        → 引用数/h-index
web_search: "[name] arxiv [company] 2024 2025"       → 近期论文
web_fetch:  个人主页 / Google Scholar 主页            → 完整论文列表

# 产品/工程/业务类（额外必查）
web_search: "[name] [company] product launch OR feature 2024 2025"  → 主导产品
web_search: "[name] previous company achievement"   → 历史成就
```

**调研结果必须体现在文档中**：若搜索有结果就写，无结果就写"未找到"，不能因为信息少就缩短文档。

### Step 3：撰写 one-pager（中英混排，以中文为主）

---

## 文档模板（必须严格遵守结构和语言风格）

```markdown
# 英文名（中文名） · 职位中文描述 · Anthropic

> 数据来源：LinkedIn / [其他来源]｜更新：YYYY-MM-DD

---

## 人物简介

![英文名](GitHub raw URL 或学术主页直链)

**英文名（中文名）**（推测约 XX 岁），[职位中文]。[3-5句核心介绍：当前职责 + 研究/工作方向 + 背景亮点 + 加入时间]。

---

## 履历介绍

### 工作履历

- **【YYYY.MM–至今/YYYY.MM】[机构]** | [职位]
  - [核心贡献，用中文描述，引用 LinkedIn 描述中的具体内容]
  - [第二条，如有]

（严格时间逆序，最新在前）

### 学业背景

- **【YYYY–YYYY】[学校中文名（英文名）]** | [学位] · [专业]
  - 研究方向：[...] / 导师：[...（如可查）]

### 个人风格

[2-3个加粗关键词，每条2-4句判断性描述。核心要求：**有洞察、有观点、不废话**。

✅ 应该写什么：
- 职业路径的"非显然"之处（为什么他的选择和别人不同）
- 研究方向选择背后的深层逻辑（不只是描述选了什么，而是为什么选）
- 跨领域背景对当前工作的实质性影响（具体说清楚怎么影响的）
- 与同类人物的差异点（"什么让他与其他XX研究员不同"）

❌ 禁止写的废话模式：
- "在Anthropic这种重视产出的环境里，这种风格很匹配" → 无效评价
- "没有活跃的X账号，个人主页简洁" → 描述外在行为，没有分析
- "他是一个低调但高产的研究者" → 空洞形容词堆砌
- "展示了他对研究机会的判断力" → 泛泛结论，不说明判断力体现在哪里
- "这在AI圈属于稀缺品质" → 可以说，但必须先说清楚为什么稀缺、稀缺在哪
- "Residency竞争激烈，转正说明他达到了较高标准" → 没有增量信息，读者自己能推断
- "XX不是常见选择，说明他有清醒的判断" → 用行为反推动机，是空洞写法

**如果提炼不出真正有洞察的内容，直接省略"个人风格"整节。** 一条真正有价值的观察胜过三条废话。]

---

## 社交媒体

- 🌐 个人主页：[URL] 或 未找到
- 🐦 X：[@handle](url) 或 未找到
- 🔗 LinkedIn：[linkedin.com/in/slug](url)
- 💻 GitHub：[url] 或 未找到
- 📚 Google Scholar：[url] 或 未找到

---

## 学术成果（仅研究员）

**研究方向**：[方向1] / [方向2]

**学术指标**（来源 Google Scholar）：引用数：XXX｜h-index：XX｜i10-index：XX
*或*：未找到 Google Scholar 主页

| 论文 | 发表 | 核心贡献（一句话说意义，不复述摘要） |
|---|---|---|
| [中文标题或英文标题](arXiv链接) | YYYY / 会议 | ... |

---

## 近期公开观点（研究员/管理者）

> 来源：X / 媒体采访 / 演讲记录

- **【YYYY-MM】** [内容摘要]（[链接]）

若无：`> 未找到近期公开表态。`

---

*本 one-pager 由 OpenClaw AI 生成，数据来源于 LinkedIn 及公开信息。建议结合最新动态核实。*
```

---

## 语言规范（中英混排原则）

| 内容 | 语言 |
|---|---|
| 正文叙述、分析、个人风格 | **中文为主** |
| 机构名、职位名 | 英文原名（首次出现可加中文解释） |
| 论文标题 | 保留英文（可加中文副标题） |
| 技术术语（RLHF、LLM等） | 英文缩写，首次展开 |
| 人名 | 英文在前，中文在括号 |

**禁止**：大段英文直接粘贴（LinkedIn描述直接复制）；禁止纯英文正文。

---

## 质量差距示例（对照检查）

### ❌ 低质量（禁止）
```
**Xiaoyi Zhang（张晓怡）**，Anthropic Member of Technical Staff, Manager。研究管理，2024.10加入

工作履历：
- 【2024.Oct–至今】Anthropic | Member of Technical Staff (Manager)
  - Research. HCI + AI
  - Making Claude the best knowledge coworker with taste
```

### ✅ 高质量（目标）
```
**Xiaoyi Zhang（张晓怡）**（推测约 35 岁），Anthropic MTS Manager，负责 HCI（人机交互）
与 AI 交叉方向的研究管理工作，目标是让 Claude 成为"有品位的最佳知识型协作伙伴"。
她有深厚的学术背景：华盛顿大学计算机科学博士（HCI 方向），在 Apple 担任 Staff 
Researcher & Manager 长达六年，主导了 Apple Intelligence 模型压缩基础设施
（Talaria 系统），实现 iPhone 上 10 倍压缩后仍保持精度、30 token/sec 推理速度。
2024 年 10 月加入 Anthropic。

个人风格：
- **HCI 出身的 AI 系统建设者**：她的博士研究聚焦于无障碍访问和个人信息学，
  在微软研究院曾开源帮助 ALS 患者通过眼动手势交流的应用（SwipeSpeak），
  并获得专利。这种"以人为本"的工程视角，让她在以模型为中心的 Anthropic 中
  具有独特价值——她关注的不是模型能做什么，而是人怎么用模型。
```

---

## 发布流程

```bash
# 发布
/app/skills/hi-redoc-curd/scripts/hi-redoc-curd.sh -p /path/to/onepager.md

# 更新已有文档
/app/skills/hi-redoc-curd/scripts/hi-redoc-curd.sh -p /path/to/onepager.md -u {docId}

# 更新目录
目录 docId: 17d90029ec4bb462f8ebdceb9262bf51
```

---

## Few-Shot 参考案例

| 案例 | 特点 |
|---|---|
| [Haochen Zhang（张皓辰）](https://docs.xiaohongshu.com/doc/1ef5f91b5aaf87e0a5db80e7674b590c) | 含照片；论文表格；清华姚班背景 |
| [Jerry Wei](https://docs.xiaohongshu.com/doc/cb3374ca12750975ee59ef1cb3b8c856) | 个人主页补充；演讲记录；安全研究 |
| [Da Yan（闫达）](https://docs.xiaohongshu.com/doc/a5d151895e9c005f07c77e8c907a8c4a) | 低公开度人物的处理方式；系统工程师风格描述 |
| [Catherine Wu](https://docs.xiaohongshu.com/doc/127c64f6268cdfa123800feec9547cb8) | 多元职业路径分析；VC→产品转型 |
| [Andrew Dai (Google DeepMind)](https://docs.xiaohongshu.com/doc/9d05830d6da9878112a98641b53b304e) | 外部参考；完整风格示范 |
