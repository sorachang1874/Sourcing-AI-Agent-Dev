#!/usr/bin/env python3
"""DeepSeek judge driver for X-First Layer 1-3 batches (live, paid model).

Runs the committed path: `x_first.luna_batch_runner.run_luna_batch_from_files`
with `x_first.deepseek_luna_transport.DeepSeekChatCompletionsLunaTransport`
and `deepseek_judgment_binding()` — the same binding that judged the OpenAI
947 batch, now committed (no /tmp drivers).

OPERATOR DIRECTIVE: the judge input must include the COMPLETE raw LinkedIn
profile (people describe concrete projects in Bio/About/experience/education).
This driver attaches, per seed, a `supporting_context` block (via the runner's
`extra_source_context` hook) containing:
  - `linkedin_profile_field_dictionary`: what each raw field means,
  - `usage_note`: profile is first-class comprehension evidence for both axes;
    v1 citations stay on seed_fact/x_bio/post anchors,
  - `linkedin_profile_raw`: the FULL harvested profile item (no key dropped).

Env: DEEPSEEK_API_KEY.  Smoke first with --limit 2.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from live_xfirst_seed_build import ProfileEnvelopeIndex  # noqa: E402

FIELD_DICTIONARY: dict[str, str] = {
    "about": "用户自述简介 — 研究方向/具体项目自述的最重要来源",
    "headline": "一句话头衔（常含方向关键词）",
    "experience[]": "工作经历: position/companyName/location/startDate/endDate/duration/description — 具体项目常写在 description",
    "education[]": "教育经历: schoolName/degree/fieldOfStudy/description — 研究方向信号",
    "currentPosition": "当前职位摘要",
    "skills[]": "技能列表 (name, endorsements)",
    "topSkills[]": "置顶技能",
    "languages[]": "语言 (name, proficiency)",
    "certifications[]": "证书 (name, authority/issuer)",
    "courses[]": "课程",
    "honorsAndAwards[]": "奖项 (title, issuedBy)",
    "projects[]": "LinkedIn 项目区 (title, description) — 直接的项目描述",
    "patents[]": "专利",
    "publications[]": "发表/论文",
    "organizations[]": "所属组织",
    "volunteering[]": "志愿经历",
    "interests/causes": "兴趣/公益（噪声，通常可忽略）",
    "websites[]": "个人链接（可能指向学术主页/Google Scholar/GitHub）",
    "location": "地区 (linkedinText/parsed)",
    "emails[]": "邮箱（隐私字段，判断无关）",
    "firstName/lastName/multiLocale*/profileLocales": "姓名及多语言形态",
    "id/objectUrn/publicIdentifier/linkedinUrl": "LinkedIn 标识符",
    "photo/profilePicture/coverPicture/featured": "图片/媒体链接（判断无关）",
    "premium/creator/influencer/verified/hiring/openToWork/memorialized": "账号状态标记",
    "connectionsCount/followerCount": "社交规模",
    "receivedRecommendations[]": "收到的推荐信（他评，弱证据）",
    "registeredAt/services/profileActions/moreProfiles/composeOptionType/originalQuery": "平台元数据（判断无关）",
}

USAGE_NOTE = (
    "supporting_context.linkedin_profile_raw is the candidate's COMPLETE raw LinkedIn "
    "profile (same person as the seed; field meanings in linkedin_profile_field_dictionary). "
    "Treat it as first-class evidence for BOTH axes (lab affiliation temporal states and "
    "pretraining experience): work-experience descriptions, About/简介, education, projects, "
    "patents and publications may directly establish or refute pre-train experience. "
    "HARD CITATION RULE (fail-closed): the ONLY legal citation strings are "
    "`seed_fact:<evidence_ref>` exactly as listed in the judged bundle manifest, `x_bio`, and "
    "`post:<stable_post_id>` exactly as listed. When a state rests on raw-profile content, cite "
    "the closest seed_fact anchor(s) covering that content (the profile sections are mirrored "
    "by seed facts). Do NOT invent citation formats such as `linkedin_profile:*`, do NOT cite "
    "raw field names, and do NOT fabricate seed_fact refs — any non-whitelisted citation fails "
    "the entire review as invalid. Judge only the supplied texts and never invent external facts."
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seeds", required=True, type=Path)
    parser.add_argument("--collection", required=True, type=Path)
    parser.add_argument("--snapshot-dir", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--approval-id", required=True)
    parser.add_argument("--workers", type=int, default=24)
    parser.add_argument("--limit", type=int, default=0, help="smoke-first: judge only the first N bundled seeds (0 = all)")
    parser.add_argument("--xfirst-src", default="/Users/changyuyi/projects/Sourcing AI Agent Dev/x-first-researcher-sourcing/src")
    args = parser.parse_args()

    sys.path.insert(0, args.xfirst_src)
    from x_first.deepseek_luna_transport import DeepSeekChatCompletionsLunaTransport, deepseek_judgment_binding  # noqa: E402
    from x_first.luna_batch_runner import run_luna_batch_from_files  # noqa: E402

    snapshot_dir: Path = args.snapshot_dir.resolve()
    envelope_index = ProfileEnvelopeIndex(snapshot_dir)

    def extra_source_context(seed: dict[str, Any]) -> dict[str, Any]:
        url = str(seed.get("source_profile_url") or "").strip()
        item = envelope_index.load(url) if url else {}
        context: dict[str, Any] = {
            "linkedin_profile_field_dictionary": FIELD_DICTIONARY,
            "usage_note": USAGE_NOTE,
        }
        if item:
            context["linkedin_profile_raw"] = item
        return context

    result = run_luna_batch_from_files(
        seeds_path=args.seeds,
        collection_path=args.collection,
        out_dir=args.out_dir,
        transport=DeepSeekChatCompletionsLunaTransport(),
        approval_id=args.approval_id,
        binding=deepseek_judgment_binding(),
        worker_count=args.workers,
        extra_source_context=extra_source_context,
        limit=args.limit if args.limit > 0 else None,
    )
    batch_path = args.out_dir / "luna_batch.json"
    batch = json.loads(batch_path.read_text(encoding="utf-8"))
    from collections import Counter

    dist = Counter(
        str((r.get("review") or {}).get("proposed_pretraining_experience_state") or r.get("status"))
        for r in batch["results"]
    )
    print(json.dumps({"result": {k: result.get(k) for k in ("status", "candidate_count", "completed_count")}, "state_distribution": dict(dist)}, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
