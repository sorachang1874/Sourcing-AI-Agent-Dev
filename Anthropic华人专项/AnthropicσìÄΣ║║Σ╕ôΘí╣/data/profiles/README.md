# LinkedIn Profile 数据资产说明

## 目录结构

```
data/profiles/
├── anthropic_employees/   # Anthropic 员工 LinkedIn profile
│   ├── {slug}_012.json    # account_012（ugoBoy，2026-04 新key）✅首选
│   ├── {slug}_003.json    # account_003（ugoBoy旧key，已耗尽）
│   ├── {slug}_008.json    # account_008（enrichgraph）⚠️当前不可达
│   └── {slug}_008early.json  # 早期 account_008 格式（无后缀原文件）
└── investor_firms/        # 投资机构华人员工 profile（待填充）
```

## API 账号

| 账号 | Provider | 状态 | response路径 |
|------|---------|------|-------------|
| account_012 ★首选 | ugoBoy（新key） | ✅ 可用 | `d['data']` |
| account_003 | ugoBoy（旧key） | ❌ 月额度耗尽 | `d['raw']['data']` |
| account_008/011 | enrichgraph | ⚠️ API不可达 | `d['raw']` |

## 注意事项

- 同一人可能有多个不同 API 返回的文件，以 `_012.json` 为准（最新最完整）
- `_003.json` 文件中某些实际上是 API 失败返回（额度耗尽），需检查 `d['raw']['data']` 是否有效
- `frankzheng_012.json` 是**错误的人**（微软SDE），正确文件是 `zheng_frank_012.json`
- Guangfeng He 天津大学未在 LinkedIn 上填写，需从其他渠道补充
