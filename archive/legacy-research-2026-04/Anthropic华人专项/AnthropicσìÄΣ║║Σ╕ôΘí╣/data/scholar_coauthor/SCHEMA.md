# Scholar Coauthor 图谱扩展数据库 — Schema 文档

## 目录

```
data/scholar_coauthor/
├── SCHEMA.md                    ← 本文件：字段定义与使用规范
├── coauthor_graph_db.json       ← 主数据库（进度追踪 + 图谱数据）
├── seed_authors.json            ← 种子节点（已知 Anthropic 华人 Scholar ID）
└── candidate_queue.json         ← 待处理候选人队列
```

---

## 核心设计原则

1. **幂等性**：每个 Scholar ID 只查一次，查过的永远不重复查
2. **断点续传**：任何时间中断，下次从 `candidate_queue.json` 中的 `pending` 记录继续
3. **可审计**：每次扫描记录 `scan_date`，结果与状态永久保留
4. **去重**：同一人出现在多个种子的 coauthor 列表中，只记录一次，但 `co_with` 记录所有关联

---

## coauthor_graph_db.json — 主数据库

### 顶层结构

```json
{
  "_meta": {
    "schema_version": "1.0",
    "created_at": "YYYY-MM-DD",
    "last_updated": "YYYY-MM-DD",
    "description": "Scholar Coauthor 图谱扩展数据库",
    "stats": {
      "seed_authors": 11,
      "seeds_scanned": 0,
      "seeds_pending": 11,
      "total_coauthors_discovered": 0,
      "coauthors_chinese_candidates": 0,
      "coauthors_confirmed_anthropic": 0,
      "coauthors_excluded": 0,
      "coauthors_pending_review": 0
    }
  },
  "seed_authors": { ... },
  "coauthors": { ... }
}
```

---

### `seed_authors` — 种子节点（已知 Anthropic 华人）

**Key：** Google Scholar User ID（如 `Y4sk3aMAAAAJ`）

```json
"Y4sk3aMAAAAJ": {
  "scholar_id": "Y4sk3aMAAAAJ",
  "name": "Jerry Wei",
  "scholar_url": "https://scholar.google.com/citations?user=Y4sk3aMAAAAJ",
  "excel_row": 42,                // 在 Excel 中的行号（null=不在Excel）
  "scan_status": "done|pending|error|skipped",
  "scan_date": "YYYY-MM-DD",      // null=未扫描
  "coauthor_count": 0,            // 抓取到的共著者数量
  "error_msg": null               // 扫描出错时填写原因
}
```

**scan_status 枚举：**
| 值 | 含义 |
|----|------|
| `pending` | 尚未扫描 |
| `done` | 已成功扫描 coauthor 列表 |
| `error` | 扫描失败（网络/限流等），需重试 |
| `skipped` | 主动跳过（如该人 Scholar 主页无合作者列表）|

---

### `coauthors` — 共著者节点

**Key：** Google Scholar User ID（如 `KLFjg9EAAAAJ`）  
如无 Scholar ID，用 `name:{姓名拼音}` 作为临时 Key

```json
"KLFjg9EAAAAJ": {
  "scholar_id": "KLFjg9EAAAAJ",
  "name": "Thomas I. Liao",
  "scholar_url": "https://scholar.google.com/citations?user=KLFjg9EAAAAJ",

  // --- 图谱关系 ---
  "co_with": ["Y4sk3aMAAAAJ", "4Zw1PJ8AAAAJ"],  // 哪些种子作者的 coauthor 列表中出现
  "discovered_date": "YYYY-MM-DD",               // 首次发现日期

  // --- 华人判定 ---
  "chinese_layer": null,     // null=未判定; 0=排除; 1=疑似; 2=确认泛华人; 3=确认大陆
  "chinese_evidence": {
    "name_pattern": "中文名特征描述",
    "education_cn": "发现的华人教育背景",
    "linkedin_lang": null,
    "self_stated": null,
    "media_source": null
  },
  "chinese_confidence": null,  // "高|中|低|排除"

  // --- Anthropic 关联 ---
  "anthropic_status": "unknown|in_excel|confirmed_not|pending_check",
  "anthropic_evidence": null,  // LinkedIn/Scholar/媒体确认证据
  "excel_row": null,           // 已在 Excel 中的行号
  "linkedin_url": null,
  "linkedin_slug": null,

  // --- 处理状态 ---
  "review_status": "pending|confirmed_anthropic|confirmed_not|excluded|needs_check",
  "review_date": null,
  "review_note": null,

  // --- Scholar 个人信息（抓取时填充）---
  "scholar_profile": {
    "affiliation": null,       // Scholar 主页显示的机构
    "verified_email": null,    // Scholar 验证邮箱域名（如 "anthropic.com"）
    "interests": [],           // 研究兴趣标签
    "citations": null,         // 引用数
    "h_index": null
  }
}
```

**review_status 枚举：**
| 值 | 含义 |
|----|------|
| `pending` | 尚未 review |
| `confirmed_anthropic` | 确认为 Anthropic 在职/离职员工 |
| `confirmed_not` | 确认不是 Anthropic 员工 |
| `excluded` | 确认非华人或其他排除理由 |
| `needs_check` | 需要进一步调查 |

---

## candidate_queue.json — 候选人处理队列

用于断点续传和任务调度。

```json
{
  "_meta": {
    "created_at": "YYYY-MM-DD",
    "last_updated": "YYYY-MM-DD"
  },
  "seeds_to_scan": [
    {
      "scholar_id": "Y4sk3aMAAAAJ",
      "name": "Jerry Wei",
      "priority": 1,
      "reason": "种子节点，需扫描其 coauthor 列表"
    }
  ],
  "coauthors_to_review": [
    {
      "scholar_id": "KLFjg9EAAAAJ",
      "name": "Thomas I. Liao",
      "co_with": ["Y4sk3aMAAAAJ"],
      "chinese_layer": 2,
      "priority": 1,
      "reason": "发现于 Jerry Wei coauthor 列表，疑似华人，需核实 Anthropic 状态"
    }
  ],
  "linkedin_to_scrape": [
    {
      "scholar_id": "KLFjg9EAAAAJ",
      "name": "Thomas I. Liao",
      "linkedin_slug": "thomasliao",
      "priority": 2,
      "reason": "华人 Layer 2 确认，需爬取 LinkedIn profile 补全 Excel"
    }
  ]
}
```

---

## 工作流程

### 阶段一：种子扫描
对 `seed_authors` 中每个 `scan_status=pending` 的种子：
1. 抓取其 Scholar 主页的 coauthor 列表
2. 对每个 coauthor 提取 `scholar_id`、`name`、`affiliation`
3. 写入 `coauthors` 节点（若已存在，只追加 `co_with`）
4. 更新种子的 `scan_status=done`、`scan_date`、`coauthor_count`
5. 同步更新 `_meta.stats`

### 阶段二：候选人 Review
对 `chinese_layer=null` 的 coauthor 节点：
1. 判断华人身份（姓名特征 → Layer 1 → 查背景 → Layer 2）
2. 更新 `chinese_layer` + `chinese_evidence`
3. Layer 2+ → 加入 `candidate_queue.json` 的 `coauthors_to_review`

### 阶段三：Anthropic 状态核实
对 `chinese_layer >= 2` 且 `review_status=pending` 的节点：
1. 先查 Excel 是否已在列
2. 查 LinkedIn / Scholar / Google 确认当前机构
3. 更新 `anthropic_status` + `review_status`

### 阶段四：新成员写入 Excel
对 `anthropic_status=confirmed_anthropic` 且 `excel_row=null` 的节点：
1. 按 §8.1 格式规范写入 Excel
2. 更新 `excel_row` + `review_date`

---

## 防重复查询规则（强制）

```
开始任何查询前，必须执行：
  CHECK coauthors[scholar_id] 是否存在
  if 存在 AND scan_status == "done":
    跳过，不重复抓取
  elif 存在 AND scan_status == "pending"/"error":
    继续，但不新建节点
  else:
    新建节点，设 review_status = "pending"
```

**关键：任何中断后重启，从 `candidate_queue.json` 恢复，不重新扫描已完成的种子。**

---

## Stats 更新时机

每次以下操作后，必须立即更新 `_meta.stats`：
- 完成一个种子扫描 → `seeds_scanned+1`, `seeds_pending-1`
- 新增 coauthor 节点 → `total_coauthors_discovered+1`
- 判定华人 Layer 2+ → `coauthors_chinese_candidates+1`
- 确认 Anthropic → `coauthors_confirmed_anthropic+1`
- 排除 → `coauthors_excluded+1`
