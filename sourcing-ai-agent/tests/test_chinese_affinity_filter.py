"""
tests/test_chinese_affinity_filter.py
--------------------------------------
华人亲缘分层筛选模块单元测试
"""

from __future__ import annotations

import unittest
from typing import Any

from sourcing_agent.chinese_affinity_filter import (
    _check_layer1_name_signal,
    _check_layer2_greater_china,
    _check_layer3_mainland_china,
    classify_candidate,
    run_chinese_affinity_filter,
)


def _make_candidate(**kwargs: Any) -> dict[str, Any]:
    """创建测试候选人字典，仅填写测试关注的字段。"""
    base: dict[str, Any] = {
        "candidate_id": "test-id-001",
        "name_en": "",
        "name_zh": "",
        "display_name": "",
        "education": "",
        "work_history": "",
        "notes": "",
        "focus_areas": "",
        "role": "",
        "ethnicity_background": "",
        "metadata": {},
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# Layer 1: 姓名信号
# ---------------------------------------------------------------------------


class TestLayer1NameSignal(unittest.TestCase):
    def test_chinese_chars_in_name_zh(self):
        c = _make_candidate(name_zh="陈伟")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("name_zh" in r for r in reasons))

    def test_chinese_chars_in_display_name(self):
        c = _make_candidate(display_name="Wei Chen（陈威）")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)

    def test_common_pinyin_surname_first(self):
        c = _make_candidate(name_en="Zhang Wei")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("zhang" in r.lower() for r in reasons))

    def test_common_pinyin_surname_last(self):
        c = _make_candidate(name_en="Wei Zhang")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)

    def test_cantonese_surname(self):
        c = _make_candidate(name_en="John Wong")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("wong" in r.lower() for r in reasons))

    def test_non_chinese_name(self):
        c = _make_candidate(name_en="John Smith")
        reasons = _check_layer1_name_signal(c)
        self.assertEqual(reasons, [])

    def test_empty_name(self):
        c = _make_candidate(name_en="", name_zh="", display_name="")
        reasons = _check_layer1_name_signal(c)
        self.assertEqual(reasons, [])

    def test_ng_surname(self):
        # "Ng" 是粤语常见单字姓
        c = _make_candidate(name_en="Ng Chun Fai")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)

    def test_mixed_display_name_format(self):
        c = _make_candidate(display_name="Liang Qing（梁清）")
        reasons = _check_layer1_name_signal(c)
        self.assertTrue(len(reasons) > 0)


# ---------------------------------------------------------------------------
# Layer 2: 泛大中华地区经历
# ---------------------------------------------------------------------------


class TestLayer2GreaterChina(unittest.TestCase):
    def test_hong_kong_education(self):
        c = _make_candidate(education="B.S. in Computer Science, HKUST (Hong Kong University of Science and Technology)")
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("hong kong" in r.lower() or "hkust" in r.lower() for r in reasons))

    def test_singapore_nus(self):
        c = _make_candidate(education="PhD, National University of Singapore (NUS)")
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_taiwan_ntu(self):
        c = _make_candidate(education="B.S., National Taiwan University (NTU)")
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_singapore_work(self):
        c = _make_candidate(work_history="Software Engineer at A*STAR, Singapore")
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_cantonese_language(self):
        c = _make_candidate(metadata={"languages": "Cantonese, English"})
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("粤语" in r or "cantonese" in r.lower() for r in reasons))

    def test_no_greater_china_signal(self):
        c = _make_candidate(
            education="PhD, MIT",
            work_history="Engineer at Google, Mountain View, CA",
        )
        reasons = _check_layer2_greater_china(c)
        self.assertEqual(reasons, [])

    def test_macau_education(self):
        c = _make_candidate(education="Studied at University of Macau")
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_nanyang_university(self):
        c = _make_candidate(education="MS from Nanyang Technological University, Singapore")
        reasons = _check_layer2_greater_china(c)
        self.assertTrue(len(reasons) > 0)


# ---------------------------------------------------------------------------
# Layer 3: 中国大陆强信号
# ---------------------------------------------------------------------------


class TestLayer3MainlandChina(unittest.TestCase):
    def test_tsinghua_education(self):
        c = _make_candidate(education="B.S. from Tsinghua University")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("tsinghua" in r.lower() or "清华" in r for r in reasons))

    def test_peking_university(self):
        c = _make_candidate(education="PhD, Peking University (PKU), Beijing")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_ustc(self):
        c = _make_candidate(education="Undergraduate at USTC (University of Science and Technology of China)")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_beijing_location(self):
        c = _make_candidate(work_history="Researcher at Microsoft Research, Beijing")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("beijing" in r.lower() for r in reasons))

    def test_shanghai_location(self):
        c = _make_candidate(work_history="Software Engineer at Morgan Stanley, Shanghai")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_baidu_company(self):
        c = _make_candidate(work_history="Research Scientist at Baidu Silicon Valley AI Lab")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("baidu" in r.lower() for r in reasons))

    def test_bytedance_company(self):
        c = _make_candidate(work_history="ML Engineer at ByteDance, 2018-2021")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_chinese_language_mandarin(self):
        c = _make_candidate(metadata={"languages": "Mandarin (Native), English (Fluent)"})
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)
        self.assertTrue(any("mandarin" in r.lower() or "中文" in r for r in reasons))

    def test_chinese_language_chinese(self):
        c = _make_candidate(metadata={"languages": "Chinese, English"})
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_deepseek_company(self):
        c = _make_candidate(work_history="Researcher at DeepSeek")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_no_mainland_signal(self):
        c = _make_candidate(
            education="PhD, Stanford University",
            work_history="Research Scientist at OpenAI",
        )
        reasons = _check_layer3_mainland_china(c)
        self.assertEqual(reasons, [])

    def test_harvest_profile_languages(self):
        c = _make_candidate(
            metadata={
                "harvest_profile": {
                    "languages": [
                        {"name": "Mandarin", "proficiency": "Native or Bilingual"},
                        {"name": "English", "proficiency": "Full Professional"},
                    ]
                }
            }
        )
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)

    def test_tencent_company(self):
        c = _make_candidate(work_history="Software Engineer at Tencent AI Lab, Shenzhen")
        reasons = _check_layer3_mainland_china(c)
        self.assertTrue(len(reasons) > 0)


# ---------------------------------------------------------------------------
# classify_candidate: 综合分层
# ---------------------------------------------------------------------------


class TestClassifyCandidate(unittest.TestCase):
    def test_layer0_only(self):
        c = _make_candidate(name_en="John Smith", education="PhD, MIT", work_history="Google Brain")
        result = classify_candidate(c)
        self.assertEqual(result.layer, 0)
        self.assertEqual(result.layers, [0])

    def test_layer1_only(self):
        c = _make_candidate(name_en="Zhang Wei", education="PhD, MIT")
        result = classify_candidate(c)
        self.assertGreaterEqual(result.layer, 1)
        self.assertIn(1, result.layers)

    def test_layer2_implies_layer1_in_layers(self):
        # Layer 2 命中时，即使姓名不是华人特征，layers 里应该也包含 1 和 2
        c = _make_candidate(
            name_en="Bob Miller",  # 非华人姓名
            education="PhD, National University of Singapore (NUS)",
        )
        result = classify_candidate(c)
        self.assertIn(2, result.layers)
        self.assertIn(1, result.layers)  # Layer 2 会自动补充 Layer 1

    def test_layer3_implies_all_lower_layers(self):
        c = _make_candidate(
            name_en="Bob Miller",
            education="B.S. from Tsinghua University, Beijing",
        )
        result = classify_candidate(c)
        self.assertEqual(result.layer, 3)
        self.assertIn(1, result.layers)
        self.assertIn(2, result.layers)
        self.assertIn(3, result.layers)

    def test_full_chinese_candidate(self):
        c = _make_candidate(
            name_en="Wei Chen",
            name_zh="陈威",
            display_name="Wei Chen（陈威）",
            education="B.S. Tsinghua University; PhD Stanford",
            work_history="Research Scientist at Baidu, 2015-2018; Google Brain 2018-2022",
            metadata={"languages": "Mandarin (Native), English (Fluent)"},
        )
        result = classify_candidate(c)
        self.assertEqual(result.layer, 3)
        self.assertIn("layer_1", result.hit_reasons)
        self.assertIn("layer_3", result.hit_reasons)

    def test_greater_china_only(self):
        c = _make_candidate(
            name_en="Alice Lam",
            education="B.S. from HKUST",
            work_history="Engineer at Standard Chartered, Hong Kong",
        )
        result = classify_candidate(c)
        self.assertEqual(result.layer, 2)
        self.assertIn(2, result.layers)
        self.assertNotIn(3, result.layers)

    def test_hit_reasons_populated(self):
        c = _make_candidate(
            name_en="Li Wei",
            education="PhD from Peking University",
            metadata={"languages": "Mandarin"},
        )
        result = classify_candidate(c)
        self.assertIn("layer_1", result.hit_reasons)
        self.assertIn("layer_3", result.hit_reasons)
        self.assertTrue(all(isinstance(r, str) for r in result.hit_reasons.get("layer_1", [])))


# ---------------------------------------------------------------------------
# run_chinese_affinity_filter: 整体 pipeline
# ---------------------------------------------------------------------------


class TestRunChineseAffinityFilter(unittest.TestCase):
    def _sample_candidates(self) -> list[dict[str, Any]]:
        return [
            _make_candidate(
                candidate_id="c001",
                name_en="Zhang Wei",
                name_zh="张威",
                education="B.S. Tsinghua University",
                work_history="Research Scientist at Google",
            ),
            _make_candidate(
                candidate_id="c002",
                name_en="Alice Lam",
                education="B.S. from HKUST",
            ),
            _make_candidate(
                candidate_id="c003",
                name_en="John Smith",
                education="PhD, MIT",
                work_history="Software Engineer at OpenAI",
            ),
            _make_candidate(
                candidate_id="c004",
                name_en="Li Fei",
                name_zh="李飞",
                education="PhD Stanford University",
                work_history="Research at Baidu AI Lab, 2016-2019",
                metadata={"languages": "Mandarin, English"},
            ),
        ]

    def test_layer0_returns_all(self):
        candidates = self._sample_candidates()
        result = run_chinese_affinity_filter(candidates, target_layer=0)
        self.assertEqual(result["filtered_count"], 4)
        self.assertEqual(result["total_candidates"], 4)

    def test_layer1_filters(self):
        candidates = self._sample_candidates()
        result = run_chinese_affinity_filter(candidates, target_layer=1)
        # Zhang Wei (Layer 3), Alice Lam (Layer 2), Li Fei (Layer 3) — John Smith (Layer 0)
        self.assertGreater(result["filtered_count"], 0)
        self.assertLess(result["filtered_count"], 4)  # John Smith 应该被过滤

    def test_layer3_filters_strict(self):
        candidates = self._sample_candidates()
        result = run_chinese_affinity_filter(candidates, target_layer=3)
        names = [r.get("name_en") for r in result["results"]]
        self.assertIn("Zhang Wei", names)  # 清华 -> Layer 3
        self.assertIn("Li Fei", names)     # Baidu + Mandarin -> Layer 3
        self.assertNotIn("John Smith", names)
        # Alice Lam 只有 HKUST -> Layer 2
        self.assertNotIn("Alice Lam", names)

    def test_chinese_affinity_annotation_present(self):
        candidates = self._sample_candidates()
        result = run_chinese_affinity_filter(candidates, target_layer=0)
        for item in result["results"]:
            self.assertIn("chinese_affinity", item)
            affinity = item["chinese_affinity"]
            self.assertIn("layer", affinity)
            self.assertIn("layers", affinity)
            self.assertIn("hit_reasons", affinity)

    def test_results_sorted_by_layer_desc(self):
        candidates = self._sample_candidates()
        result = run_chinese_affinity_filter(candidates, target_layer=0)
        layers = [r.get("chinese_affinity", {}).get("layer", 0) for r in result["results"]]
        self.assertEqual(layers, sorted(layers, reverse=True))

    def test_layer_counts_consistent(self):
        candidates = self._sample_candidates()
        result = run_chinese_affinity_filter(candidates, target_layer=0)
        self.assertEqual(result["layer_counts"]["layer_0"], 4)
        self.assertGreaterEqual(result["layer_counts"]["layer_1"], result["layer_counts"]["layer_2"])
        self.assertGreaterEqual(result["layer_counts"]["layer_2"], result["layer_counts"]["layer_3"])

    def test_summary_string_present(self):
        result = run_chinese_affinity_filter(self._sample_candidates())
        self.assertIn("Layer 0", result["summary"])
        self.assertIn("Layer 1", result["summary"])
        self.assertIn("Layer 3", result["summary"])

    def test_empty_candidates(self):
        result = run_chinese_affinity_filter([])
        self.assertEqual(result["total_candidates"], 0)
        self.assertEqual(result["filtered_count"], 0)
        self.assertEqual(result["layer_counts"]["layer_0"], 0)

    def test_cantonese_only_goes_to_layer2_not_layer3(self):
        c = _make_candidate(
            candidate_id="c-canton",
            name_en="Chan Yiu Fai",
            education="B.S. CUHK",  # 香港中文大学
            metadata={"languages": "Cantonese, English"},
        )
        result = classify_candidate(c)
        self.assertIn(2, result.layers)
        # 粤语不单独触发 Layer 3，且没有大陆高校/地名/公司信号
        self.assertNotIn(3, result.layers)


# ---------------------------------------------------------------------------
# 边界 case 与误报防范
# ---------------------------------------------------------------------------


class TestEdgeCases(unittest.TestCase):
    def test_china_lake_not_triggered(self):
        """'China Lake' 是美国地名，不应触发 Layer 3。"""
        c = _make_candidate(work_history="Worked at Naval Air Weapons Station China Lake, California")
        reasons = _check_layer3_mainland_china(c)
        # 因为我们在正则里排除了 "china lake"
        for r in reasons:
            self.assertNotIn("china lake", r.lower())

    def test_singapore_does_not_imply_mainland(self):
        c = _make_candidate(education="NUS Singapore", work_history="Engineer at DBS Singapore")
        layer3 = _check_layer3_mainland_china(c)
        # Singapore 不应触发 Layer 3
        self.assertEqual(layer3, [])

    def test_non_ascii_chinese_name(self):
        c = _make_candidate(name_zh="李明", name_en="Li Ming")
        result = classify_candidate(c)
        self.assertGreaterEqual(result.layer, 1)

    def test_no_duplicate_layer_entries(self):
        c = _make_candidate(
            name_en="Zhang Wei",
            name_zh="张威",
            education="Tsinghua University",
            metadata={"languages": "Mandarin"},
        )
        result = classify_candidate(c)
        self.assertEqual(len(result.layers), len(set(result.layers)))

    def test_metadata_string_json(self):
        """metadata 可能以 JSON 字符串存储。"""
        import json
        c = _make_candidate(
            name_en="Li Wei",
            metadata=json.dumps({"languages": "Mandarin, English"}),
        )
        result = classify_candidate(c)
        self.assertIn(3, result.layers)


if __name__ == "__main__":
    unittest.main(verbosity=2)
