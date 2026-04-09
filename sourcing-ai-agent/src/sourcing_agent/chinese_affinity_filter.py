"""
chinese_affinity_filter.py
--------------------------
华人亲缘分层筛选模块（Chinese Affinity Layering Filter）

对已有的候选人资产池（JSON 格式，通常存储在 runtime/ 目录）按照华人亲缘程度进行
多层分层标注，用于回答类似「帮我找出 Gemini 所有的华人」这类 query。

分层设计
--------
Layer 0 — Roster（全量）
    组织的全量成员，不做任何过滤，作为漏斗的分母。

Layer 1 — 姓名信号（Name Signal）
    候选人的英文姓名 / 展示名称具有华人姓名特征。
    规则：
    - 中文字符出现在 name_zh / display_name
    - 英文姓名命中华人常见姓氏列表（pinyin 形式）
    - 显示名括号格式含中文（例如 "Wei Chen（陈威）"）

Layer 2 — 泛大中华地区经历（Greater China Region Experience）
    有港澳台、新加坡等泛华人地区的教育或工作经历，但不含中国大陆。
    规则：
    - education / work_history / notes 含 Hong Kong / Macau / Taiwan / Singapore
      及其常见缩写或中文表述
    - LinkedIn 语言技能含粤语（Cantonese）—— 注：粤语仅计入 Layer 2

Layer 3 — 中国大陆强信号（Mainland China Strong Signal）
    有中国大陆教育经历、工作经历、或 LinkedIn 语言技能含中文/普通话/粤语等。
    规则（满足任意一项）：
    - education 含中国大陆高校（清华、北大、复旦、交通大学、浙大、中科大等）
      或地名（Beijing / Shanghai / Shenzhen / Nanjing / Hangzhou / Wuhan...）
    - work_history 含中国大陆公司（阿里 / 腾讯 / 百度 / 字节 / 华为 / 小米等）
      或地名关键词
    - metadata.languages 含 Chinese / Mandarin / Putonghua（粤语只计入 Layer 2）
    - notes / focus_areas / ethnicity_background 有明确中国背景标注

设计原则
--------
- 规则完全 deterministic，不依赖 LLM
- 每个候选人可同时属于多层（较高层一定包含较低层）
- 分层结果写入 candidate 的 metadata["chinese_affinity"] 字段，不修改原始字段
- 输出同时包含：分层后的候选列表、每层数量统计、每个匹配的证据链（hit_reasons）
- 模块可独立运行（CLI），也可作为 library 被 orchestrator / cli 调用

用法示例
--------
# 作为 library
from sourcing_agent.chinese_affinity_filter import run_chinese_affinity_filter

result = run_chinese_affinity_filter(
    candidates=candidate_list,   # list[dict] 或 list[Candidate]
    target_layer=3,              # 只返回 Layer 3 及以上（更严格）
)
print(result["summary"])

# 作为 CLI（见 cli.py 的 chinese-affinity-filter 命令）
PYTHONPATH=src python3 -m sourcing_agent.cli chinese-affinity-filter \\
    --company anthropic \\
    --snapshot-id 20260409T080131 \\
    --target-layer 2 \\
    --output runtime/filter_results/anthropic_chinese_affinity.json
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# 常见华人姓氏（拼音，覆盖最高频的 ~200 个）
# ---------------------------------------------------------------------------

CHINESE_SURNAMES_PINYIN: frozenset[str] = frozenset(
    {
        # 百家姓高频
        "li", "wang", "zhang", "liu", "chen", "yang", "huang", "zhao", "wu",
        "zhou", "xu", "sun", "ma", "zhu", "hu", "guo", "he", "lin", "luo",
        "gao", "zheng", "tang", "xiao", "liang", "han", "cao", "xu", "deng",
        "xie", "feng", "dong", "peng", "wei", "jiang", "shen", "ye", "pan",
        "lu", "fu", "cheng", "yuan", "ding", "song", "gu", "fan", "cui",
        "yin", "qian", "du", "shi", "ye", "ni", "meng", "wen", "mo", "bai",
        "xia", "zeng", "qin", "kong", "yu", "cai", "nie", "liao", "zou",
        "xiong", "dai", "tao", "yan", "yan", "ai", "bei", "bian", "chang",
        "chao", "chi", "chu", "cong", "cui", "dai", "diao", "ding", "dong",
        "dou", "du", "duan", "fang", "fei", "fu", "gang", "ge", "gong",
        "gou", "gui", "guo", "han", "hao", "hao", "he", "hong", "hou", "hu",
        "hua", "huan", "hui", "jia", "jian", "jiang", "jiao", "jin", "jing",
        "ju", "jun", "kang", "kong", "kuang", "lai", "lan", "lei", "li",
        "lian", "liao", "lin", "liu", "long", "lou", "lu", "luo", "mei",
        "miao", "min", "ming", "mu", "nan", "ning", "ou", "pang", "pei",
        "piao", "qi", "qian", "qiang", "qin", "qiu", "qu", "quan", "rao",
        "ren", "rong", "ruan", "shan", "shao", "shen", "sheng", "shi",
        "shu", "si", "su", "tai", "tan", "teng", "tian", "tong", "tu",
        "wan", "wan", "weng", "xian", "xiang", "xiao", "xin", "xing",
        "xiong", "xuan", "xue", "yan", "yao", "yi", "yin", "ying", "yong",
        "you", "yu", "yuan", "yue", "yun", "zeng", "zhan", "zheng", "zhi",
        "zhong", "zhong", "zhu", "zi", "zou", "zuo",
        # 常见台湾/粤语拼写变体
        "chan", "cheung", "chiu", "chong", "chow", "chu", "fong", "fung",
        "ho", "hui", "ip", "kwan", "kwok", "kwong", "lai", "lam", "law",
        "lee", "leung", "lim", "ling", "lo", "lui", "lung", "mak", "mok",
        "ng", "ng", "poon", "siu", "so", "tam", "tang", "tong", "tsang",
        "tsui", "tung", "wan", "wong", "yam", "yan", "yau", "yeung", "yim",
        "yin", "yip", "yuen", "yung",
        # 其他常见
        "bao", "bi", "bian", "cen", "dan", "en", "gao", "gao", "gen",
        "guan", "gun", "hao", "hua", "huo", "jian", "kun", "lao", "le",
        "man", "nian", "pian", "pin", "pu", "que", "run", "sha", "shui",
        "shuai", "sou", "sui", "suo", "tao", "tie", "tou", "xun", "ya",
        "ye", "yi", "zan", "zao", "ze", "zhao", "zhen",
    }
)

# ---------------------------------------------------------------------------
# 泛华人地区关键词（港澳台 + 新加坡）—— Layer 2
# ---------------------------------------------------------------------------

GREATER_CHINA_REGION_PATTERNS: list[str] = [
    # 香港
    r"\bhong\s*kong\b",
    r"\bhk\b",
    r"\b香港\b",
    r"\bhkust\b",
    r"\bthe university of hong kong\b",
    r"\bcuhk\b",
    r"\bcityu\b",
    r"\bpolyu\b",
    # 澳门
    r"\bmacau\b",
    r"\bmacao\b",
    r"\b澳门\b",
    # 台湾
    r"\btaiwan\b",
    r"\bntu\b",       # National Taiwan University
    r"\bnthu\b",      # National Tsing Hua University (Taiwan)
    r"\bnctu\b",      # National Chiao Tung University (Taiwan)
    r"\b台湾\b",
    r"\b台大\b",
    r"\bntust\b",
    r"\bacademia sinica\b",
    # 新加坡
    r"\bsingapore\b",
    r"\bnus\b",        # National University of Singapore
    r"\bntu singapore\b",
    r"\bnanyang\b",
    r"\bnanyang technological university\b",
    r"\bnational university of singapore\b",
    r"\b新加坡\b",
    r"\bsmu\b",        # Singapore Management University
    r"\ba\*star\b",
    r"\bastar\b",
]

# ---------------------------------------------------------------------------
# 粤语信号（计入 Layer 2，因粤语主要属于港澳地区）
# ---------------------------------------------------------------------------

CANTONESE_LANGUAGE_PATTERNS: list[str] = [
    r"\bcantonese\b",
    r"\b粤语\b",
    r"\b广东话\b",
]

# ---------------------------------------------------------------------------
# 中国大陆高校关键词 —— Layer 3
# ---------------------------------------------------------------------------

MAINLAND_UNIVERSITIES: list[str] = [
    # C9 联盟 / 顶尖高校
    r"\btsinghua\b",
    r"\b清华\b",
    r"\bpeking university\b",
    r"\b\bpku\b\b",
    r"\b北京大学\b",
    r"\b北大\b",
    r"\bfudan\b",
    r"\b复旦\b",
    r"\bshanghai jiao tong\b",
    r"\bsjtu\b",
    r"\b上海交通大学\b",
    r"\b上交\b",
    r"\bzhejiang university\b",
    r"\bzju\b",
    r"\b浙江大学\b",
    r"\b浙大\b",
    r"\buniversity of science and technology of china\b",
    r"\bustc\b",
    r"\b中国科学技术大学\b",
    r"\b中科大\b",
    r"\bnanjing university\b",
    r"\b南京大学\b",
    r"\bnju\b",
    r"\bwuhan university\b",
    r"\b武汉大学\b",
    r"\bhuazhong university\b",
    r"\bhust\b",
    r"\b华中科技大学\b",
    r"\bxi'an jiaotong\b",
    r"\bxjtu\b",
    r"\b西安交通大学\b",
    r"\brenmin university\b",
    r"\b中国人民大学\b",
    r"\bbeihang\b",
    r"\b北京航空航天大学\b",
    r"\b北航\b",
    r"\bbupt\b",
    r"\b北京邮电大学\b",
    r"\bbit\b",
    r"\b北京理工大学\b",
    r"\bnankai university\b",
    r"\b南开\b",
    r"\btongji\b",
    r"\b同济\b",
    r"\bsun yat-sen\b",
    r"\bsysu\b",
    r"\b中山大学\b",
    r"\bshandong university\b",
    r"\b山东大学\b",
    r"\bhit\b",
    r"\bharbin institute of technology\b",
    r"\b哈尔滨工业大学\b",
    r"\b哈工大\b",
    r"\bnudt\b",
    r"\b国防科技大学\b",
    r"\bcas\b",   # 注：仅在中国语境下适用，配合其他信号
    r"\bchinese academy of sciences\b",
    r"\b中国科学院\b",
    r"\b中科院\b",
    # 其他知名高校
    r"\bxidian\b",
    r"\b西安电子科技大学\b",
    r"\bupc\b",
    r"\bshanghaitech\b",
    r"\b上海科技大学\b",
    r"\bcuhk shenzhen\b",
    r"\bcuhk-shenzhen\b",
    r"\bsouthern university of science and technology\b",
    r"\bsustech\b",
    r"\b南方科技大学\b",
    r"\bnorth china electric power\b",
    r"\bchongqing university\b",
    r"\b重庆大学\b",
    r"\bxiamen university\b",
    r"\b厦门大学\b",
    r"\bjilin university\b",
    r"\b吉林大学\b",
    r"\blanzhu\b",
    r"\blanzhou university\b",
    r"\b兰州大学\b",
    r"\bcentral south university\b",
    r"\b中南大学\b",
    r"\bhunan university\b",
    r"\b湖南大学\b",
    r"\bsichuan university\b",
    r"\b四川大学\b",
    r"\bbuaa\b",
    r"\bnuaa\b",
    r"\bnanjing university of aeronautics\b",
]

# ---------------------------------------------------------------------------
# 中国大陆城市 / 地名关键词 —— Layer 3
# ---------------------------------------------------------------------------

MAINLAND_LOCATIONS: list[str] = [
    r"\bbeijing\b",
    r"\bshanghai\b",
    r"\bshenzhen\b",
    r"\bguangzhou\b",
    r"\bhangzhou\b",
    r"\bnanjing\b",
    r"\bwuhan\b",
    r"\bchengdu\b",
    r"\bxian\b",
    r"\bxi'an\b",
    r"\btianjin\b",
    r"\bshenyang\b",
    r"\bchongqing\b",
    r"\bqingdao\b",
    r"\bzhengzhou\b",
    r"\bsuzhou\b",
    r"\bwuxi\b",
    r"\bchangsha\b",
    r"\bkunming\b",
    r"\bxiamen\b",
    r"\bhaerbin\b",
    r"\bchangchun\b",
    r"\bshijiazhuang\b",
    r"\bjinan\b",
    r"\bfuzhou\b",
    r"\bguiyang\b",
    r"\bnanchang\b",
    r"\bheifei\b",
    r"\bhefei\b",
    r"\bzhengzhou\b",
    r"\btaiyuan\b",
    r"\bharbin\b",
    r"\b中国\b",
    r"\bchina\b(?!\s+lake)",  # 排除 "China Lake" 等地名
    r"\bmainland\s+china\b",
    r"\bprc\b",
]

# ---------------------------------------------------------------------------
# 中国大陆公司关键词 —— Layer 3
# ---------------------------------------------------------------------------

MAINLAND_COMPANIES: list[str] = [
    r"\bbaidu\b",
    r"\b百度\b",
    r"\btencent\b",
    r"\b腾讯\b",
    r"\balibaba\b",
    r"\b阿里巴巴\b",
    r"\b阿里\b",
    r"\bbytedance\b",
    r"\btiktok\b",
    r"\b字节跳动\b",
    r"\b字节\b",
    r"\bhuawei\b",
    r"\b华为\b",
    r"\bxiaomi\b",
    r"\b小米\b",
    r"\bdidi\b",
    r"\b滴滴\b",
    r"\bmeituan\b",
    r"\b美团\b",
    r"\bpinduoduo\b",
    r"\b拼多多\b",
    r"\bjd\.com\b",
    r"\bjd\b(?=\s+com|\s+\.com|\s+ai|\s*,|\s+group)",
    r"\b京东\b",
    r"\bnetease\b",
    r"\b网易\b",
    r"\bsensetime\b",
    r"\b商汤\b",
    r"\bmegvii\b",
    r"\b旷视\b",
    r"\bhorizon robotics\b",
    r"\b地平线\b",
    r"\bdji\b",
    r"\b大疆\b",
    r"\biflytek\b",
    r"\b科大讯飞\b",
    r"\bzte\b",
    r"\boppo\b",
    r"\bvivo\b",
    r"\boneplus\b",
    r"\blenovo\b",
    r"\b联想\b",
    r"\bsina\b",
    r"\bweibo\b",
    r"\b微博\b",
    r"\bwechat\b",
    r"\b微信\b",
    r"\bdimension\b(?=\s+capital|\s+x)",
    r"\bzhihu\b",
    r"\b知乎\b",
    r"\bsequoia\s+china\b",
    r"\bIDG\s+capital\b",
    r"\bgoogler\b(?=\s+china)",
    r"\bxpeng\b",
    r"\bnio\b",
    r"\bli\s+auto\b",
    r"\bwaymo\s+china\b",
    r"\bdeepseek\b",
    r"\b深度求索\b",
    r"\bmoonshot\b",
    r"\b月之暗面\b",
    r"\bzhipu\b",
    r"\b智谱\b",
    r"\bminimax\b",
    r"\bstep\s+fun\b",
    r"\bstepfun\b",
    r"\bkimi\b(?=\s+ai|\s*,)",
    r"\blingyi\b",
    r"\b零一万物\b",
    r"\bbaichuan\b",
    r"\b百川\b",
    r"\bkunlun\b",
    r"\b混元\b",
    r"\bhunyan\b",
    r"\bglm\b",
    r"\bchat\s*glm\b",
]

# ---------------------------------------------------------------------------
# 语言技能关键词 —— Layer 3（中文 / 普通话），Layer 2（粤语）
# ---------------------------------------------------------------------------

MANDARIN_LANGUAGE_PATTERNS: list[str] = [
    r"\bchinese\b",
    r"\bmandarin\b",
    r"\bputonghua\b",
    r"\b普通话\b",
    r"\b中文\b",
    r"\b汉语\b",
    r"\b简体中文\b",
    r"\b繁体中文\b",
]

# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclass
class ChineseAffinityResult:
    """单个候选人的华人亲缘分析结果。"""

    candidate_id: str
    name_en: str
    name_zh: str
    display_name: str
    layer: int                                     # 所属最高层（0-3）
    layers: list[int] = field(default_factory=list)  # 满足的所有层
    hit_reasons: dict[str, list[str]] = field(default_factory=dict)  # layer -> reasons

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ChineseAffinityFilterOutput:
    """整个筛选运行的输出。"""

    target_company: str
    snapshot_id: str
    run_at: str
    query_description: str
    total_candidates: int
    layer_counts: dict[str, int]      # "layer_0" / "layer_1" / "layer_2" / "layer_3"
    layer_rates: dict[str, str]       # 占 Layer 0 的比例，如 "12.3%"
    results: list[dict[str, Any]]     # 完整结果（含原始 candidate 字段）

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# 内部正则编译（一次性）
# ---------------------------------------------------------------------------


def _compile(patterns: list[str]) -> re.Pattern[str]:
    return re.compile("|".join(patterns), re.IGNORECASE)


_RE_GREATER_CHINA = _compile(GREATER_CHINA_REGION_PATTERNS)
_RE_CANTONESE = _compile(CANTONESE_LANGUAGE_PATTERNS)
_RE_MAINLAND_UNIV = _compile(MAINLAND_UNIVERSITIES)
_RE_MAINLAND_LOC = _compile(MAINLAND_LOCATIONS)
_RE_MAINLAND_CO = _compile(MAINLAND_COMPANIES)
_RE_MANDARIN = _compile(MANDARIN_LANGUAGE_PATTERNS)


# ---------------------------------------------------------------------------
# Layer 检测函数
# ---------------------------------------------------------------------------


def _has_chinese_chars(text: str) -> bool:
    """检测文本中是否含 CJK 汉字。"""
    return bool(re.search(r"[\u4e00-\u9fff\u3400-\u4dbf\uf900-\ufaff]", text))


def _check_layer1_name_signal(candidate: dict[str, Any]) -> list[str]:
    """
    Layer 1 — 姓名信号。
    返回 hit reasons 列表（空表示不符合）。
    """
    reasons: list[str] = []

    name_en = str(candidate.get("name_en") or "").strip()
    name_zh = str(candidate.get("name_zh") or "").strip()
    display_name = str(candidate.get("display_name") or "").strip()

    # 规则 1a：name_zh 或 display_name 含汉字
    if _has_chinese_chars(name_zh):
        reasons.append(f"name_zh 含汉字：「{name_zh}」")
    elif _has_chinese_chars(display_name):
        reasons.append(f"display_name 含汉字：「{display_name}」")

    # 规则 1b：英文姓名中的姓命中华人常见姓氏列表
    if name_en:
        tokens = re.split(r"[\s\-]+", name_en.lower())
        # 通常姓在第一个或最后一个 token
        candidates_surnames = [tokens[0]] if tokens else []
        if len(tokens) > 1:
            candidates_surnames.append(tokens[-1])
        for surname in candidates_surnames:
            # 去掉括号等特殊字符
            clean = re.sub(r"[^\w]", "", surname)
            if clean in CHINESE_SURNAMES_PINYIN:
                reasons.append(f"英文姓名命中华人常见姓氏：「{clean}」（来自 name_en: {name_en}）")
                break

    # 规则 1c：display_name 是"英文（中文）"格式
    if re.search(r"[A-Za-z].+[（(].+[\u4e00-\u9fff].+[）)]", display_name):
        if not any("display_name 含汉字" in r for r in reasons):
            reasons.append(f"display_name 含中英混排格式：「{display_name}」")

    return reasons


def _extract_text_fields(candidate: dict[str, Any]) -> dict[str, str]:
    """提取候选人的主要文本字段，用于关键词匹配。"""
    fields = {
        "education": str(candidate.get("education") or ""),
        "work_history": str(candidate.get("work_history") or ""),
        "notes": str(candidate.get("notes") or ""),
        "focus_areas": str(candidate.get("focus_areas") or ""),
        "role": str(candidate.get("role") or ""),
        "ethnicity_background": str(candidate.get("ethnicity_background") or ""),
    }
    # 也检查 metadata 中的语言技能
    metadata = candidate.get("metadata") or {}
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}
    languages_raw = metadata.get("languages") or metadata.get("profile_languages") or ""
    if isinstance(languages_raw, list):
        languages_raw = " ".join(str(x) for x in languages_raw)
    fields["languages"] = str(languages_raw)

    # harvest profile 里的语言字段可能更深层
    profile = metadata.get("harvest_profile") or metadata.get("profile_detail") or {}
    if isinstance(profile, dict):
        profile_langs = profile.get("languages") or []
        if isinstance(profile_langs, list):
            lang_texts = []
            for item in profile_langs:
                if isinstance(item, dict):
                    lang_texts.append(str(item.get("name") or "") + " " + str(item.get("proficiency") or ""))
                else:
                    lang_texts.append(str(item))
            if lang_texts:
                fields["languages"] += " " + " ".join(lang_texts)

    return fields


def _find_pattern_hits(text: str, pattern: re.Pattern[str]) -> list[str]:
    """返回在 text 中匹配到的所有不重复片段（最多 3 个）。"""
    matches = [m.group(0) for m in pattern.finditer(text)]
    seen: set[str] = set()
    result: list[str] = []
    for m in matches:
        key = m.lower()
        if key not in seen:
            seen.add(key)
            result.append(m)
        if len(result) >= 3:
            break
    return result


def _check_layer2_greater_china(candidate: dict[str, Any]) -> list[str]:
    """
    Layer 2 — 泛大中华地区经历（港澳台 + 新加坡）。
    返回 hit reasons 列表。
    """
    reasons: list[str] = []
    fields = _extract_text_fields(candidate)

    for field_name, text in fields.items():
        if not text:
            continue

        # 地区关键词
        hits = _find_pattern_hits(text, _RE_GREATER_CHINA)
        if hits:
            reasons.append(f"{field_name} 含泛大中华地区关键词：{hits}")

        # 粤语（计入 Layer 2）
        if field_name == "languages":
            canton_hits = _find_pattern_hits(text, _RE_CANTONESE)
            if canton_hits:
                reasons.append(f"语言技能含粤语：{canton_hits}")

    return reasons


def _check_layer3_mainland_china(candidate: dict[str, Any]) -> list[str]:
    """
    Layer 3 — 中国大陆强信号。
    返回 hit reasons 列表。
    """
    reasons: list[str] = []
    fields = _extract_text_fields(candidate)

    for field_name, text in fields.items():
        if not text:
            continue

        # 大陆高校
        univ_hits = _find_pattern_hits(text, _RE_MAINLAND_UNIV)
        if univ_hits:
            reasons.append(f"{field_name} 含中国大陆高校：{univ_hits}")

        # 大陆地名（仅在 education / work_history / notes 里查，避免泛化）
        if field_name in {"education", "work_history", "notes", "ethnicity_background"}:
            loc_hits = _find_pattern_hits(text, _RE_MAINLAND_LOC)
            if loc_hits:
                reasons.append(f"{field_name} 含中国大陆地名：{loc_hits}")

        # 大陆公司
        if field_name in {"work_history", "notes", "ethnicity_background"}:
            co_hits = _find_pattern_hits(text, _RE_MAINLAND_CO)
            if co_hits:
                reasons.append(f"{field_name} 含中国大陆公司：{co_hits}")

        # 语言技能（中文/普通话）
        if field_name == "languages":
            lang_hits = _find_pattern_hits(text, _RE_MANDARIN)
            if lang_hits:
                reasons.append(f"语言技能含中文/普通话：{lang_hits}")

    return reasons


# ---------------------------------------------------------------------------
# 核心分层函数
# ---------------------------------------------------------------------------


def classify_candidate(candidate: dict[str, Any]) -> ChineseAffinityResult:
    """
    对单个候选人进行华人亲缘分层分析。

    参数：
        candidate: 候选人字典（来自 candidate_documents.json 或 SQLite 导出）

    返回：
        ChineseAffinityResult，包含层级和匹配原因
    """
    candidate_id = str(candidate.get("candidate_id") or "")
    name_en = str(candidate.get("name_en") or "")
    name_zh = str(candidate.get("name_zh") or "")
    display_name = str(candidate.get("display_name") or "")

    layers: list[int] = [0]  # Layer 0 = 全量，所有人都在
    hit_reasons: dict[str, list[str]] = {}

    # Layer 1: 姓名信号
    layer1_reasons = _check_layer1_name_signal(candidate)
    if layer1_reasons:
        layers.append(1)
        hit_reasons["layer_1"] = layer1_reasons

    # Layer 2: 泛大中华地区经历（不要求已有 Layer 1）
    layer2_reasons = _check_layer2_greater_china(candidate)
    if layer2_reasons:
        if 1 not in layers:
            layers.append(1)  # Layer 2 包含 Layer 1 的意义：也标记为 Layer 1 候选
        layers.append(2)
        hit_reasons["layer_2"] = layer2_reasons

    # Layer 3: 中国大陆强信号
    layer3_reasons = _check_layer3_mainland_china(candidate)
    if layer3_reasons:
        if 1 not in layers:
            layers.append(1)
        if 2 not in layers:
            layers.append(2)
        layers.append(3)
        hit_reasons["layer_3"] = layer3_reasons

    layers = sorted(set(layers))
    max_layer = max(layers)

    return ChineseAffinityResult(
        candidate_id=candidate_id,
        name_en=name_en,
        name_zh=name_zh,
        display_name=display_name,
        layer=max_layer,
        layers=layers,
        hit_reasons=hit_reasons,
    )


# ---------------------------------------------------------------------------
# 主入口函数
# ---------------------------------------------------------------------------


def run_chinese_affinity_filter(
    candidates: list[dict[str, Any]],
    *,
    target_company: str = "",
    snapshot_id: str = "",
    target_layer: int = 0,
    query_description: str = "华人亲缘分层筛选",
) -> dict[str, Any]:
    """
    对候选人列表执行华人亲缘分层筛选。

    参数：
        candidates:          候选人字典列表（来自 candidate_documents.json 等）
        target_company:      目标公司名（用于报告）
        snapshot_id:         快照 ID（用于报告）
        target_layer:        最低层级过滤（0 = 返回所有，3 = 只返回 Layer 3）
        query_description:   用于报告的查询描述

    返回：
        dict，包含 summary / layer_counts / results（已经包含 chinese_affinity 注解）
    """
    total = len(candidates)
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 对每个候选人进行分层
    all_results: list[ChineseAffinityResult] = []
    for candidate in candidates:
        result = classify_candidate(candidate)
        all_results.append(result)

    # 统计各层数量
    layer_counts = {f"layer_{i}": 0 for i in range(4)}
    layer_counts["layer_0"] = total
    for result in all_results:
        for layer in result.layers:
            if layer >= 1:
                layer_counts[f"layer_{layer}"] = layer_counts.get(f"layer_{layer}", 0) + 1

    def _rate(count: int) -> str:
        if total == 0:
            return "0.0%"
        return f"{count / total * 100:.1f}%"

    layer_rates = {k: _rate(v) for k, v in layer_counts.items()}

    # 过滤目标层
    filtered_results = [r for r in all_results if r.layer >= target_layer]

    # 构建输出（将分析结果注入到原始 candidate 字段的副本中）
    candidate_map = {str(c.get("candidate_id") or ""): c for c in candidates}
    output_records: list[dict[str, Any]] = []
    for result in filtered_results:
        original = dict(candidate_map.get(result.candidate_id) or {})
        # 注入分析结果到 chinese_affinity 字段（不污染原始字段）
        original["chinese_affinity"] = {
            "layer": result.layer,
            "layers": result.layers,
            "hit_reasons": result.hit_reasons,
        }
        output_records.append(original)

    # 按 layer 降序排列，同 layer 内按 name_en 排序
    output_records.sort(
        key=lambda c: (-(c.get("chinese_affinity") or {}).get("layer", 0), str(c.get("name_en") or ""))
    )

    summary_lines = [
        f"目标公司: {target_company or '（未指定）'}",
        f"快照 ID: {snapshot_id or '（未指定）'}",
        f"查询描述: {query_description}",
        f"候选人总数 (Layer 0): {total}",
        f"Layer 1 (姓名信号): {layer_counts['layer_1']} ({layer_rates['layer_1']})",
        f"Layer 2 (泛大中华地区经历: 港澳台/新加坡): {layer_counts['layer_2']} ({layer_rates['layer_2']})",
        f"Layer 3 (中国大陆强信号: 高校/公司/语言): {layer_counts['layer_3']} ({layer_rates['layer_3']})",
        f"输出过滤层 (target_layer={target_layer}): {len(output_records)} 人",
    ]

    return {
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "run_at": run_at,
        "query_description": query_description,
        "total_candidates": total,
        "layer_counts": layer_counts,
        "layer_rates": layer_rates,
        "target_layer": target_layer,
        "filtered_count": len(output_records),
        "summary": "\n".join(summary_lines),
        "results": output_records,
    }


# ---------------------------------------------------------------------------
# 从 runtime 目录加载候选人资产的辅助函数
# ---------------------------------------------------------------------------


def load_candidates_from_snapshot(
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
) -> tuple[list[dict[str, Any]], str, str]:
    """
    从 runtime/company_assets/{company}/{snapshot_id}/normalized_artifacts/ 加载候选人。

    返回：
        (candidates, resolved_snapshot_id, company_key)
    """
    runtime_dir = Path(runtime_dir)
    company_key = re.sub(r"[^a-z0-9]+", "", target_company.lower())
    company_dir = runtime_dir / "company_assets" / company_key

    if not company_dir.exists():
        raise FileNotFoundError(f"Company asset directory not found: {company_dir}")

    # 解析 snapshot_id
    if snapshot_id:
        snapshot_dir = company_dir / snapshot_id
    else:
        # 找最新的 snapshot
        snapshots = sorted(
            [d for d in company_dir.iterdir() if d.is_dir() and re.match(r"\d{8}T\d{6}", d.name)],
            reverse=True,
        )
        if not snapshots:
            raise FileNotFoundError(f"No snapshots found in {company_dir}")
        snapshot_dir = snapshots[0]
        snapshot_id = snapshot_dir.name

    # 根据 asset_view 选择候选文件
    normalized_dir = snapshot_dir / "normalized_artifacts"
    if asset_view == "strict_roster_only":
        strict_dir = normalized_dir / "strict_roster_only"
        candidate_file = strict_dir / "reusable_candidate_documents.json"
        if not candidate_file.exists():
            candidate_file = strict_dir / "normalized_candidates.json"
    else:
        candidate_file = normalized_dir / "reusable_candidate_documents.json"
        if not candidate_file.exists():
            candidate_file = normalized_dir / "materialized_candidate_documents.json"

    if not candidate_file.exists():
        # 回退到直接的 candidate_documents.json
        candidate_file = snapshot_dir / "candidate_documents.json"

    if not candidate_file.exists():
        raise FileNotFoundError(f"Candidate file not found: {candidate_file}")

    raw = json.loads(candidate_file.read_text(encoding="utf-8"))

    # 支持两种格式：list[dict] 或 {"candidates": list[dict]}
    if isinstance(raw, list):
        candidates = raw
    elif isinstance(raw, dict) and "candidates" in raw:
        candidates = raw["candidates"]
    else:
        candidates = list(raw.values()) if isinstance(raw, dict) else []

    return candidates, snapshot_id, company_key


# ---------------------------------------------------------------------------
# CLI 独立入口（可被 cli.py 调用）
# ---------------------------------------------------------------------------


def cli_main(args: list[str] | None = None) -> None:
    """供 cli.py 或直接执行时调用的入口函数。"""
    import argparse

    parser = argparse.ArgumentParser(
        description="华人亲缘分层筛选 (Chinese Affinity Layering Filter)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--company", required=True, help="目标公司名或 key（如 anthropic / thinkingmachineslab）")
    parser.add_argument("--snapshot-id", default="", help="Snapshot ID（不填则取最新）")
    parser.add_argument(
        "--asset-view",
        default="canonical_merged",
        choices=["canonical_merged", "strict_roster_only"],
        help="资产视图（默认 canonical_merged）",
    )
    parser.add_argument(
        "--target-layer",
        type=int,
        default=0,
        choices=[0, 1, 2, 3],
        help="输出过滤层（0=全量, 1=姓名信号, 2=泛华人地区, 3=大陆强信号）",
    )
    parser.add_argument("--runtime-dir", default="runtime", help="runtime 目录路径（默认 ./runtime）")
    parser.add_argument("--output", default="", help="输出 JSON 文件路径（不填则打印到 stdout）")
    parser.add_argument("--query", default="华人亲缘分层筛选", help="查询描述（用于报告）")

    parsed = parser.parse_args(args)

    print(f"[chinese-affinity-filter] 加载候选人资产: company={parsed.company}, snapshot={parsed.snapshot_id or '最新'}")
    candidates, resolved_snapshot_id, company_key = load_candidates_from_snapshot(
        runtime_dir=parsed.runtime_dir,
        target_company=parsed.company,
        snapshot_id=parsed.snapshot_id,
        asset_view=parsed.asset_view,
    )
    print(f"[chinese-affinity-filter] 加载完成: {len(candidates)} 个候选人，snapshot={resolved_snapshot_id}")

    result = run_chinese_affinity_filter(
        candidates=candidates,
        target_company=parsed.company,
        snapshot_id=resolved_snapshot_id,
        target_layer=parsed.target_layer,
        query_description=parsed.query,
    )

    print("\n" + "=" * 60)
    print(result["summary"])
    print("=" * 60)

    if parsed.output:
        out_path = Path(parsed.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n结果已写入: {out_path}")
    else:
        # 打印摘要，不打印完整结果（可能很长）
        print(f"\n过滤后候选人数: {result['filtered_count']}")
        if result["results"]:
            print("\n前 10 个结果示例：")
            for item in result["results"][:10]:
                affinity = item.get("chinese_affinity") or {}
                print(
                    f"  [{affinity.get('layer', 0)}层] {item.get('name_en', '')} "
                    f"({item.get('name_zh', '') or '-'}) "
                    f"| {item.get('role', '')[:50]}"
                )
                for layer_key, reasons in (affinity.get("hit_reasons") or {}).items():
                    for reason in reasons[:2]:
                        print(f"      {layer_key}: {reason[:100]}")


if __name__ == "__main__":
    cli_main()
