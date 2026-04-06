# arXiv 论文扫描数据库

**目标**：通过扫描 Anthropic 官网 sitemap 收录的所有论文，识别华人作者，核查其是否为 Anthropic 在职/离职员工。

## 文件说明

| 文件 | 说明 |
|------|------|
| `arxiv_scan_db.json` | 主数据库（机器可读），包含每篇论文的扫描结果 |
| `arxiv_scan_index.csv` | 汇总索引（人工可审查），每行一篇论文 |
| `README.md` | 本文件 |

## 数据来源

- **论文列表**：`anthropic.com/sitemap.xml` → research 页面 → 提取 arXiv ID
- **提取日期**：2026-04-02
- **总数**：124 个 arXiv ID（含部分误纳入的非 Anthropic 论文）

## 结果状态说明

| result 值 | 含义 |
|-----------|------|
| `known` | 论文华人作者已在 Excel 表中（无需新增） |
| `new_added` | 通过本论文新发现并已添加到 Excel |
| `excluded` | 排除：① 非 Anthropic 成员 ② 非华人确认 ③ 论文非 Anthropic 自有（误纳入） |
| `no_chinese` | 已扫描，无华人候选作者 |
| `pending` | 发现候选人，尚未完成核查 |
| `pending_scan` | 论文尚未扫描 |

## 扫描进度（2026-04-02）

- **已扫描**：49 篇（含排除的非 Anthropic 自有论文）
- **待扫描**：78 篇
- **新增成员**：2 人（Judy Hanwen Shen、Christina Lu）

## 待核查事项

- `2510.05024`：Christine Ye 待核查（是否为华人 Anthropic 成员）
- `2506.15740`：Yuqi Sun 待核查（SHADE-Arena 共同作者）

## 使用方法

扫描新论文后，在 `arxiv_scan_db.json` 的 `scanned` 字段下添加记录：

```json
"2XXXXXXX": {
  "slug": "论文对应的官网slug",
  "title": "论文标题",
  "authors_raw": "原始作者列表字符串",
  "chinese_candidates": ["候选华人姓名列表"],
  "result": "known|new_added|excluded|no_chinese|pending",
  "note": "处理说明",
  "scan_date": "YYYY-MM-DD"
}
```

同时从 `pending` 字段中删除对应记录，并重新生成 CSV：

```bash
python3 -c "
import json, csv
# 重新生成 CSV（见 SKILL.md §18 arXiv扫描规范）
"
```
