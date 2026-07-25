# Few-Shot 案例参考

以下是高质量 one-pager 的参考样例，写作时以此风格为准。

## 案例列表

| # | 人物 | 机构 | 特点 | Redoc 链接 |
|---|---|---|---|---|
| 1 | Andrew Dai（戴明博） | Google DeepMind | 全栈科研+数据驱动；Gemini数据负责人；长职业连贯性 | https://docs.xiaohongshu.com/doc/9d05830d6da9878112a98641b53b304e |
| 2 | Ethan He（何宜晖） | xAI（前Nvidia） | 工程+研究复合型；被FT报道为xAI关键人才；世界模型方向 | https://docs.xiaohongshu.com/doc/b4ec0c87c4748f9440740d09b3a5bd92 |
| 3 | 顾一鸣（Yiming Gu） | Google DeepMind | 跨领域转型（交通→自动驾驶→多模态）；Gemini图像联合负责人 | https://docs.xiaohongshu.com/doc/1264581808498f4b3617d3fb973c72aa |
| 4 | 张皓辰（Haochen Zhang） | Anthropic | Anthropic MTS；预训练/推理基础设施；含照片 | https://docs.xiaohongshu.com/doc/1ef5f91b5aaf87e0a5db80e7674b590c |

## 风格要点提炼

### 结构顺序（重要）
1. 标题：`# 中文名（英文名） · 职位 · 机构`
2. 照片（大图，放在简介前）
3. **履历介绍摘要**：2-3句，说清楚"他是谁+核心贡献"，不是列表
4. **工作履历**：时间倒序，每条有"核心贡献"子弹点
5. **学业背景**：学校、学位、年份、研究方向、导师（如可查）
6. **个人风格**：3个关键词，每条3-4句判断性描述
7. **社交媒体**：所有平台全列
8. **学术成果**：学术指标 + 代表论文表格（最多5篇，每篇有引用数+一句话贡献）
9. **近期观点**：X/采访/博客，时间倒序

### 写作风格
- **全程简体中文**，专有名词保留英文
- **要有判断**：不写"他参与了Gemini"，写"他是Gemini数据架构的核心决策者，这意味着..."
- **具体化**：不写"多项研究"，写具体论文标题+arXiv链接+引用数
- **个人风格要有洞察**：从职业轨迹分析出此人的思维模式/风格，不是百科词条

### 照片处理
- LinkedIn JSON 中的 `profile_picture_url` 字段（`media.licdn.com/dms/image/...` 格式）可直接作为图片直链
- Markdown 语法：`![姓名 头像](URL)`，放在人物简介正文之前
- 若 LinkedIn URL 失效，备选：X 头像 / 个人主页照片 / Google Scholar 照片

### 学术指标来源
- 固定格式：`Citations: XXX（XXX since 2020）｜h-index: XX｜i10-index: XX（来源：Google Scholar，截至YYYY-MM）`
- 访问：`https://scholar.google.com/citations?user=<scholar_id>&hl=en`

### 不要做的事
- ❌ 不要写"他是一位优秀的研究员"这类空话
- ❌ 不要把论文摘要直接粘贴进来，要写"这篇论文的意义是..."
- ❌ 不要遗漏社交媒体（即使找不到也要写"未找到"）
- ❌ 不要省略照片（没有时要明确说明）
- ❌ 不要用英文写作（除专有名词）
