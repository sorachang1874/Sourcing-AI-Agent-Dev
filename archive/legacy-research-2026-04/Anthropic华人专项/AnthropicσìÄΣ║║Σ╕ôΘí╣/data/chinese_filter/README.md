# 华人身份多层过滤机制

> **设计目标：** 最大化覆盖率的同时保证信息真实，避免误判。任何候选人都从 Layer 0 开始，依据证据逐层确认，只有达到 Layer 2+ 才写入 Excel。

---

## 四层架构

```
Layer 0: all_members.json
    全体已知 Anthropic 成员
    （来源：LinkedIn API / arXiv 论文署名 / 官网 / 媒体）
    不区分华人与否，所有来源发现的人名均记录
         ↓ 姓名符合华人特征（中文姓氏 / 拼音名）
Layer 1: suspected.json
    疑似华人
    仅凭姓名推测，缺乏背景证据
    ⚠️ 不写入 Excel（仅本地存档）
         ↓ 找到语言/教育/工作华人证据
Layer 2: confirmed_cjk.json
    确认泛华人（CJK华裔）
    包括：大陆华人、香港人、台湾人、新加坡华人、ABC、
         英裔华人、加拿大裔华人等广义华裔范畴
    ✅ 可写入 Excel，备注注明证据来源
         ↓ 确认有大陆（不含港台新）的教育或工作经历
Layer 3: confirmed_mainland.json
    确认大陆华人
    ✅ 可写入 Excel
```

---

## 晋级证据要求

### Layer 1 → Layer 2（满足任一）
- `linkedin_languages` 字段含 Chinese（Simplified 或 Traditional）
- 教育/实习/工作经历含：
  - 大陆高校（清华/北大/复旦/交大/浙大/中科大/南大等）
  - 香港高校（HKUST/CUHK/HKU 等）
  - 台湾高校（台大/NTU/NCTU 等）
  - 新加坡华文机构（NUS/NTU 等）
- 本人公开自述华裔身份
- 有可确认来源的中文名（非推断，需标注来源）

### Layer 2 → Layer 3（满足任一）
- 本科/硕士/博士在中国大陆高校
- 明确的大陆工作经历（含实习）
- 本人自述来自大陆

### 降级 / 排除（→ excluded.json）
- 找到否定证据（明确确认非华裔）
- 非目标范围人员（如外部研究合作者，写表格时标注）

---

## 文件说明

| 文件 | 说明 | 写入Excel |
|------|------|---------|
| `all_members.json` | Layer 0：全体已知成员（包括非华人） | - |
| `suspected.json` | Layer 1：仅姓名疑似，待核实 | ❌ |
| `confirmed_cjk.json` | Layer 2：确认泛华人 | ✅ |
| `confirmed_mainland.json` | Layer 3：确认大陆华人 | ✅ |
| `excluded.json` | 明确排除的候选人 | ❌ |

---

## 记录格式

### all_members.json（Layer 0）
```json
{
  "name_en": "英文名",
  "anthropic_status": "在职 | 已离职 | 外部合作 | 未确认",
  "source": ["excel_active", "arxiv_paper", "linkedin_api", "media"],
  "position": "职位简介",
  "excel_sheet": "在职华人员工 | 已离职华人员工 | null",
  "excel_row": 85,
  "last_updated": "YYYY-MM-DD"
}
```

### suspected / confirmed_cjk / confirmed_mainland（Layer 1-3）
```json
{
  "name_en": "英文名",
  "name_zh": "中文名（必须有确认来源，不推断）",
  "layer": 1,
  "layer_label": "疑似华人 | 确认泛华人/CJK华裔 | 确认大陆华人",
  "anthropic_status": "在职 | 已离职 | 外部合作 | 未确认",
  "evidence": {
    "name_pattern": "Peng是华人常见姓",
    "linkedin_languages": "Chinese (Simplified)",
    "education_cn": "清华大学 本科 2014-2018",
    "work_cn": "字节跳动 2021-2022",
    "self_stated": "本人主页自述",
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

## 新候选人处理流程

```
发现新候选人
    ↓
1. 检索 all_members.json：是否已记录？
   ├── 是 → 查当前 layer，决定是否晋级
   └── 否 → 添加到 all_members.json

2. 判断 layer：
   ├── 无华人证据 → 仅在 all_members 记录，不加入 suspected
   ├── 仅姓名特征 → suspected.json（Layer 1）
   ├── 有背景证据 → confirmed_cjk.json（Layer 2）
   └── 有大陆经历 → confirmed_mainland.json（Layer 3）

3. 达到 Layer 2+ 且 in_excel=false → 写入 Excel 表格

4. 更新 last_updated 字段
```

---

## 维护规则
- Layer 1 候选人需定期复核（优先通过 LinkedIn languages 字段确认）
- 新增人员写入 Excel 前必须先在本机制中登记（all_members → suspected/confirmed）
- 中文名只写有明确来源（媒体报道/本人主页）的，不推断
- `confidence` 字段只反映**华裔身份判断**的把握度，不是信息完整度
