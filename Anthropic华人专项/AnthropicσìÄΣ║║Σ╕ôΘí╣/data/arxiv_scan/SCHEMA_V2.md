# arxiv_scan_db.json Schema V2

> **升级时间：** 2026-04-05
> **升级原因：** V1 存在作者列表截断、ACK 数据分离、无 Anthropic 自有标注等问题

## V1 → V2 变更

| 维度 | V1 | V2 |
|------|----|----|
| 作者列表 | `authors_raw`（截断字符串） | 保留 `authors_raw` + 新增 `authors_list`（完整数组） |
| ACK 数据 | 单独在 `ack_summary.json` | 合并到每篇论文的 `ack` 子对象中 |
| Anthropic 归属 | 无 | 新增 `is_anthropic_paper` 布尔字段 |
| 论文分类 | 仅 `result` 字段混合表达 | 新增 `paper_type` 枚举字段 |
| 年份 | 无 | 从 arXiv ID 提取 `year` 和 `month` |
| 华人候选详情 | 仅名字列表 | 新增 `chinese_candidates_detail` 含 role/status |

## 完整字段定义

```json
{
  "_meta": {
    "schema_version": "2.0",
    "description": "Anthropic Publication 扫描数据库",
    "created": "2026-04-02",
    "last_updated": "2026-04-05",
    "total_papers": 129,
    "anthropic_papers": 51,      // is_anthropic_paper=true 的数量
    "external_papers": 78,       // is_anthropic_paper=false 的数量
    "ack_scanned": 12,           // 有 ACK 数据的论文数
    "authors_complete": 129      // authors_list 完整的论文数
  },
  "papers": {
    "{arXiv_ID}": {
      // === 基本信息 ===
      "title": "论文完整标题",
      "slug": "Anthropic 官网 research 页面的 slug（如有）",
      "year": 2025,              // 从 arXiv ID 前2位推断（24→2024, 25→2025, 26→2026）
      "month": 1,                // 从 arXiv ID 第3-4位推断
      
      // === Anthropic 归属 ===
      "is_anthropic_paper": true,  // true=Anthropic 自有论文, false=外部/引用论文
      "paper_type": "core",        // core|collaborative|external|misclassified
      // core: Anthropic 自有（核心作者含 Dario/Jared/Amanda 等）
      // collaborative: 与外部机构合作（有 Anthropic 作者也有大量外部作者）
      // external: 非 Anthropic 论文（被 sitemap 误纳入或仅引用 Anthropic 技术）
      // misclassified: 被 sitemap 误纳入的完全无关论文（物理/波斯语等）
      
      // === 作者信息 ===
      "authors_raw": "原始作者字符串（可能截断）",
      "authors_list": ["Author 1", "Author 2", ...],  // 完整结构化作者列表
      "author_count": 43,          // 作者总数
      
      // === 致谢信息 ===
      "ack": {
        "scanned": true,           // 是否已扫描 ACK
        "found": true,             // 是否找到 ACK 段落
        "text": "完整 ACK 文本",    // ACK 原文（可为 null）
        "names": ["Name 1", ...],  // ACK 中提到的所有人名
        "chinese_names": ["Name"],  // ACK 中的华人候选
        "scan_date": "2026-04-05"
      },
      
      // === 华人扫描结果 ===
      "chinese_candidates": ["Name 1", "Name 2"],  // 华人候选名字列表（向后兼容）
      "chinese_candidates_detail": [
        {
          "name": "Name",
          "source": "author|ack",    // 从作者列表发现 还是 从 ACK 发现
          "status": "known|new_added|excluded|pending",
          "note": "已在 Excel row XX / 外部合作者 / ..."
        }
      ],
      
      // === 扫描元信息 ===
      "result": "known|new_added|excluded|no_chinese|pending",
      "note": "处理说明",
      "scan_date": "2026-04-02",
      "html_downloaded": true,       // 是否已下载 HTML 到 html_raw/
      "html_path": "html_raw/2501.18837.html"  // 本地 HTML 路径（相对）
    }
  }
}
```

## 迁移脚本

迁移脚本将：
1. 从 V1 的 `scanned` 键迁移到 V2 的 `papers` 键
2. 从 `ack_summary.json` 合并 ACK 数据到每篇论文
3. 从 `result` 和 `note` 推断 `is_anthropic_paper` 和 `paper_type`
4. 从 arXiv ID 提取 `year` 和 `month`
5. 标记 `authors_list` 为 null（待后续批量补全）

## 后续补全计划

1. **authors_list 补全**：批量 curl `arxiv.org/abs/{id}` 提取 `citation_author` meta 标签
2. **ACK 补全**：对所有 `is_anthropic_paper=true` 的论文做 ACK 扫描
3. **html_downloaded 补全**：批量下载到 `html_raw/`
