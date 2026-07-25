"""Hosted smoke shared surface — the product-facing slice of workflow_smoke.

Split out 2026-07-22 (REFACTOR_MASTER_PLAN.md WS2 god-file recon: the 10.3k-line
workflow_smoke.py is a productized smoke harness whose PRODUCT-shared surface is
only this case matrix + hosted HTTP client; runner internals stay harness-side
and sink toward tests over later waves). Scripts and product modules import the
smoke surface from here; workflow_smoke re-imports for its internal runners.
"""

from __future__ import annotations

import json
import urllib.request as urllib_request
from pathlib import Path
from typing import Any

from .smoke_expectation_contract import validate_smoke_expectations


def _safe_float(value: Any) -> float:
    # Replicated from workflow_smoke (trivial utility; the harness keeps its
    # own copy for its 50+ internal uses — import direction stays one-way).
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


DEFAULT_SMOKE_CASES: list[dict[str, Any]] = [
    {
        "case": "skild_pretrain",
        "payload": {
            "raw_user_request": "请帮我找Skild AI里做Pre-train方向的人",
            "top_k": 10,
        },
    },
    {
        "case": "humansand_coding",
        "payload": {
            "raw_user_request": "我想了解Humans&里偏Coding agents方向的研究成员",
            "top_k": 10,
        },
    },
    {
        "case": "anthropic_pretraining",
        "payload": {
            "raw_user_request": "帮我找Anthropic里做Pre-training方向的人",
            "top_k": 10,
        },
    },
    {
        "case": "xai_full_roster",
        "payload": {
            "raw_user_request": "给我 xAI 的所有成员",
            "top_k": 10,
        },
    },
    {
        "case": "xai_coding_all_members_scoped",
        "payload": {
            "raw_user_request": "我要 xAI 做 Coding 方向的全部成员",
            "top_k": 10,
        },
    },
    {
        "case": "openai_reasoning",
        "payload": {
            "raw_user_request": "我想要OpenAI里做Reasoning方向的人",
            "top_k": 10,
        },
    },
    {
        "case": "google_multimodal_pretrain",
        "payload": {
            "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人（包括Veo和Nano Banana相关）",
            "top_k": 10,
        },
    },
]




def load_smoke_cases(matrix_file: str = "", selected_cases: set[str] | None = None) -> list[dict[str, Any]]:
    selected = {str(item).strip() for item in list(selected_cases or set()) if str(item).strip()}
    cases = DEFAULT_SMOKE_CASES
    if matrix_file:
        payload = json.loads(Path(matrix_file).read_text(encoding="utf-8"))
        loaded_cases = payload.get("cases")
        if not isinstance(loaded_cases, list):
            raise ValueError("matrix file must contain a top-level `cases` list")
        cases = loaded_cases
    normalized: list[dict[str, Any]] = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        case_name = str(case.get("case") or "").strip()
        if not case_name:
            continue
        if selected and case_name not in selected:
            continue
        payload = case.get("payload")
        if not isinstance(payload, dict):
            raise ValueError(f"case `{case_name}` is missing a dict payload")
        expectations = case.get("expectations")
        normalized_case: dict[str, Any] = {"case": case_name, "payload": payload}
        if isinstance(expectations, dict):
            expectation_errors = validate_smoke_expectations(expectations, context=f"case:{case_name}")
            if expectation_errors:
                raise ValueError("; ".join(expectation_errors))
            normalized_case["expectations"] = dict(expectations)
        max_poll_seconds = _safe_float(case.get("max_poll_seconds"))
        if max_poll_seconds > 0.0:
            normalized_case["max_poll_seconds"] = max_poll_seconds
        if bool(case.get("seed_reference_runtime")):
            normalized_case["seed_reference_runtime"] = True
        runtime_isolation = str(case.get("runtime_isolation") or "").strip()
        if runtime_isolation:
            normalized_case["runtime_isolation"] = runtime_isolation
        coverage_tags = [str(item).strip() for item in list(case.get("coverage_tags") or []) if str(item).strip()]
        if coverage_tags:
            normalized_case["coverage_tags"] = coverage_tags
        review_decision = case.get("review_decision")
        if isinstance(review_decision, dict):
            normalized_case["review_decision"] = dict(review_decision)
        target_public_web_action = case.get("target_public_web_action")
        if isinstance(target_public_web_action, dict):
            normalized_case["target_public_web_action"] = dict(target_public_web_action)
        company_public_web_action = case.get("company_public_web_action")
        if isinstance(company_public_web_action, dict):
            normalized_case["company_public_web_action"] = dict(company_public_web_action)
        scripted_scenario = str(case.get("scripted_scenario") or "").strip()
        if scripted_scenario:
            normalized_case["scripted_scenario"] = scripted_scenario
        runtime_env = {
            str(key or "").strip(): str(value)
            for key, value in dict(case.get("runtime_env") or {}).items()
            if str(key or "").strip()
        }
        if runtime_env:
            normalized_case["runtime_env"] = runtime_env
        normalized.append(normalized_case)
    if selected and not normalized:
        missing = ", ".join(sorted(selected))
        raise ValueError(f"requested cases not found in matrix: {missing}")
    return normalized


class HostedWorkflowSmokeClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))

    def post(
        self,
        path: str,
        payload: dict[str, Any],
        timeout: float = 120.0,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        request = urllib_request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", **dict(headers or {})},
            method="POST",
        )
        with self.opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def get(self, path: str, timeout: float = 120.0) -> dict[str, Any]:
        request = urllib_request.Request(f"{self.base_url}{path}", method="GET")
        with self.opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
