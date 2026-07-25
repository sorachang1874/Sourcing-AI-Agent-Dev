# Engineering Blog 扫描数据库

## 文件说明

- `engineering_scan_db.json`：主数据库（机器可读），记录每篇博文扫描结果
- `engineering_scan_index.csv`：汇总索引（人工可审查）

## engineering_scan_db.json 字段

```json
{
  "url": "完整URL",
  "title": "文章标题",
  "published": "发布日期",
  "authors": ["作者列表（Written by）"],
  "acknowledgements": ["致谢人员列表"],
  "chinese_candidates": [
    {
      "name": "姓名",
      "role": "authors/acknowledgements",
      "status": "known/new/excluded",
      "notes": "备注"
    }
  ],
  "scan_status": "done/pending/error",
  "scan_date": "扫描日期",
  "new_members_found": []
}
```

## 数据来源
- `anthropic.com/engineering` 下所有博文
- 扫描目标：Authors（Written by）+ Acknowledgements 中的华人

## 覆盖状态
- 总博文数：21篇
- 已扫：0篇
- 待扫：21篇
