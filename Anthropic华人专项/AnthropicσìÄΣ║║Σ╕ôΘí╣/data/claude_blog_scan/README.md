# claude.com/blog 扫描数据库

## 文件说明
- `claude_blog_scan_db.json`：主数据库，记录每篇文章扫描结果

## 背景
- claude.com/blog 是面向企业/开发者用户的产品博客，内容偏产品实践和 Best Practice
- 与 anthropic.com/engineering（技术深度）区别：claude.com/blog 更偏产品故事和使用场景
- 两个博客均须纳入覆盖范围

## 数据来源
- 列表页：https://claude.com/blog
- 扫描目标：Authors（Written by / 署名）+ Acknowledgements 中的华人
