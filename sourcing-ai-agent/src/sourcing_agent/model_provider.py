from __future__ import annotations

import json
from typing import Any, Protocol
from urllib import error, request
import requests

from .document_extraction import infer_structured_signals_from_payload
from .domain import JobRequest
from .settings import ModelProviderSettings, QwenSettings


class ModelClient(Protocol):
    def summarize(self, request: JobRequest, matches: list[dict], total_matches: int) -> str: ...

    def interpret_intent(self, request: JobRequest, draft_plan: dict[str, Any]) -> str: ...

    def plan_search_strategy(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]: ...

    def analyze_page_asset(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def judge_company_equivalence(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def judge_profile_membership(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def provider_name(self) -> str: ...

    def healthcheck(self) -> dict[str, Any]: ...


class DeterministicModelClient:
    def provider_name(self) -> str:
        return "deterministic"

    def summarize(self, request: JobRequest, matches: list[dict], total_matches: int) -> str:
        if not matches:
            return "No candidates matched the current sourcing criteria."
        top = matches[0]
        return (
            f"Found {total_matches} matches for {request.target_company or 'the target company'}. "
            f"Top match is {top['display_name']} with score {top['score']}."
        )

    def interpret_intent(self, request: JobRequest, draft_plan: dict[str, Any]) -> str:
        company = request.target_company or "目标公司"
        categories = ", ".join(request.categories) if request.categories else "unspecified categories"
        strategy = draft_plan.get("retrieval_plan", {}).get("strategy", "hybrid")
        return (
            f"User wants a sourcing workflow for {company}, focused on {categories}, "
            f"with early clarification and a {strategy} retrieval path after full-asset acquisition."
        )

    def plan_search_strategy(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]:
        acquisition = dict(draft_payload.get("acquisition_strategy") or {})
        publication = dict(draft_payload.get("publication_coverage") or {})
        target_company = request.target_company or "target company"
        source_families = [
            str(item.get("family") or "").strip()
            for item in publication.get("source_families") or []
            if isinstance(item, dict)
        ]
        query_bundles = list((draft_payload.get("draft_search_strategy") or {}).get("query_bundles") or [])
        return {
            "planner_mode": "deterministic",
            "objective": (
                f"Search broadly for {target_company} using low-cost sources first, then fall back to paid profile search "
                "only when public evidence cannot resolve membership."
            ),
            "query_bundles": query_bundles,
            "follow_up_rules": [
                "Prefer public web search and official pages before paid profile search.",
                "If a new source family is required, store it as a named family and route it through plan review.",
                f"Current publication-like families: {', '.join(source_families[:4])}" if source_families else "Use research/blog/docs surfaces when available.",
            ],
            "review_triggers": list(acquisition.get("confirmation_points") or []),
        }

    def healthcheck(self) -> dict[str, Any]:
        return {"provider": "deterministic", "status": "ready"}

    def analyze_page_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        target_company = str(payload.get("target_company") or "").strip()
        title = str(payload.get("title") or "").strip()
        description = str(payload.get("description") or "").strip()
        excerpt = str(payload.get("text_excerpt") or "").strip()
        document_type = str(payload.get("document_type") or "").strip() or "unknown"
        combined = " ".join([title, description, excerpt]).lower()
        company_match = bool(target_company and target_company.lower() in combined)
        role_signals = []
        for token in ["research", "engineer", "scientist", "safety", "multimodal", "rl", "reinforcement"]:
            if token in combined:
                role_signals.append(token)
        urls = payload.get("extracted_links") or {}
        structured_signals = infer_structured_signals_from_payload(payload)
        confidence_score = 0.25
        if company_match:
            confidence_score += 0.35
        if role_signals:
            confidence_score += 0.2
        if structured_signals["education_signals"] or structured_signals["work_history_signals"]:
            confidence_score += 0.1
        if (urls.get("linkedin_urls") or []) or (urls.get("personal_urls") or []) or (urls.get("x_urls") or []):
            confidence_score += 0.15
        confidence_label = "high" if confidence_score >= 0.75 else "medium" if confidence_score >= 0.45 else "low"
        return {
            "summary": " | ".join(item for item in [title, description] if item)[:400],
            "target_company_relation": "explicit" if company_match else "unclear",
            "role_signals": role_signals[:6],
            "education_signals": structured_signals["education_signals"][:6],
            "work_history_signals": structured_signals["work_history_signals"][:8],
            "affiliation_signals": structured_signals["affiliation_signals"][:6],
            "confidence_label": confidence_label,
            "confidence_score": round(min(confidence_score, 0.95), 2),
            "document_type": document_type,
            "recommended_links": {
                "linkedin_url": ((urls.get("linkedin_urls") or [""])[0]),
                "personal_url": ((urls.get("personal_urls") or [""])[0]),
                "x_url": ((urls.get("x_urls") or [""])[0]),
                "github_url": ((urls.get("github_urls") or [""])[0]),
                "resume_url": ((urls.get("resume_urls") or [""])[0]),
            },
            "notes": "Deterministic page analysis fallback.",
        }

    def judge_company_equivalence(self, payload: dict[str, Any]) -> dict[str, Any]:
        observed = list(payload.get("observed_companies") or [])
        matched_label = ""
        if len(observed) == 1 and isinstance(observed[0], dict):
            matched_label = str(observed[0].get("label") or "").strip()
        return {
            "decision": "uncertain",
            "matched_label": matched_label,
            "confidence_label": "low",
            "rationale": "Deterministic model does not perform company-equivalence judgment.",
        }

    def judge_profile_membership(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "decision": "uncertain",
            "confidence_label": "low",
            "rationale": "Deterministic model does not perform profile-membership review.",
        }


class QwenResponsesModelClient(DeterministicModelClient):
    def __init__(self, settings: QwenSettings) -> None:
        self.settings = settings

    def summarize(self, request: JobRequest, matches: list[dict], total_matches: int) -> str:
        prompt = {
            "target_company": request.target_company,
            "query": request.query or request.raw_user_request,
            "total_matches": total_matches,
            "top_matches": [
                {
                    "display_name": item["display_name"],
                    "organization": item["organization"],
                    "role": item["role"],
                    "score": item["score"],
                    "explanation": item["explanation"],
                }
                for item in matches[:5]
            ],
        }
        response = self._safe_text_prompt(
            "You are a sourcing assistant. Summarize the sourcing results in 2 short English sentences.",
            json.dumps(prompt, ensure_ascii=False),
        )
        return response or super().summarize(request, matches, total_matches)

    def interpret_intent(self, request: JobRequest, draft_plan: dict[str, Any]) -> str:
        prompt = {
            "user_request": request.raw_user_request or request.query,
            "target_company": request.target_company,
            "draft_plan": draft_plan,
        }
        response = self._safe_text_prompt(
            "Interpret the user's sourcing intent in 2 short English sentences. "
            "Focus on objective, criteria, and why full-asset acquisition matters before retrieval.",
            json.dumps(prompt, ensure_ascii=False),
        )
        return response or super().interpret_intent(request, draft_plan)

    def healthcheck(self) -> dict[str, Any]:
        try:
            response = self._call_responses_api("Reply with exactly: QWEN_OK")
            return {
                "provider": "qwen",
                "status": "ready" if "QWEN_OK" in response else "unexpected_response",
                "model": self.settings.model,
                "preview": response[:80],
            }
        except Exception as exc:
            return {
                "provider": "qwen",
                "status": "degraded",
                "model": self.settings.model,
                "error": str(exc),
            }

    def analyze_page_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are helping with sourcing candidate research. Return strict JSON with keys "
            "summary,target_company_relation,role_signals,education_signals,work_history_signals,"
            "affiliation_signals,confidence_label,confidence_score,document_type,recommended_links,notes. "
            "education_signals must be a list of objects with keys school,degree,field,date_range. "
            "work_history_signals must be a list of objects with keys title,organization,date_range,description. "
            "affiliation_signals must be a list of objects with keys organization,relation,evidence. "
            "Use concise text. confidence_label must be one of high, medium, low.",
            json.dumps(payload, ensure_ascii=False),
        )
        if not response:
            return super().analyze_page_asset(payload)
        parsed = _safe_json_object(response)
        if not parsed:
            return super().analyze_page_asset(payload)
        result = super().analyze_page_asset(payload)
        result.update(
            {
                key: value
                for key, value in parsed.items()
                if key in result
                or key
                in {
                    "summary",
                    "target_company_relation",
                    "role_signals",
                    "education_signals",
                    "work_history_signals",
                    "affiliation_signals",
                    "confidence_label",
                    "confidence_score",
                    "document_type",
                    "recommended_links",
                    "notes",
                }
            }
        )
        return result

    def plan_search_strategy(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are designing a sourcing search plan. Return strict JSON with keys "
            "planner_mode,objective,query_bundles,follow_up_rules,review_triggers. "
            "query_bundles must be a list of objects with keys "
            "bundle_id,source_family,priority,objective,execution_mode,queries,filters. "
            "Use concise strings and do not include markdown.",
            json.dumps(
                {
                    "request": request.to_record(),
                    "draft_payload": draft_payload,
                },
                ensure_ascii=False,
            ),
        )
        parsed = _safe_json_object(response)
        if not parsed:
            return super().plan_search_strategy(request, draft_payload)
        return parsed

    def judge_company_equivalence(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are verifying whether organization labels refer to the same real company. "
            "Return strict JSON with keys decision,matched_label,confidence_label,rationale. "
            "decision must be one of same_company,different_company,uncertain. "
            "matched_label must be one of the observed labels or empty. "
            "Be conservative: only return same_company when the labels clearly describe the same organization or the same LinkedIn company identity.",
            json.dumps(payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        if not parsed:
            return super().judge_company_equivalence(payload)
        return {
            "decision": str(parsed.get("decision") or "uncertain").strip() or "uncertain",
            "matched_label": str(parsed.get("matched_label") or "").strip(),
            "confidence_label": str(parsed.get("confidence_label") or "low").strip() or "low",
            "rationale": str(parsed.get("rationale") or "").strip(),
        }

    def judge_profile_membership(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are verifying whether a LinkedIn profile really supports target-company membership. "
            "Return strict JSON with keys decision,confidence_label,rationale. "
            "decision must be one of confirmed_member,suspicious_member,non_member,uncertain. "
            "suspicious_member means the structured company match exists but the profile content looks implausible, "
            "spam-like, corrupted, or otherwise needs manual review. "
            "Be conservative: only return confirmed_member when the profile is plausibly consistent with a real member, "
            "and only return non_member when the profile clearly does not support target-company membership.",
            json.dumps(payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        if not parsed:
            return super().judge_profile_membership(payload)
        return {
            "decision": str(parsed.get("decision") or "uncertain").strip() or "uncertain",
            "confidence_label": str(parsed.get("confidence_label") or "low").strip() or "low",
            "rationale": str(parsed.get("rationale") or "").strip(),
        }

    def _run_text_prompt(self, system_prompt: str, user_prompt: str) -> str:
        input_text = f"System instruction:\n{system_prompt}\n\nUser input:\n{user_prompt}"
        return self._call_responses_api(input_text)

    def _safe_text_prompt(self, system_prompt: str, user_prompt: str) -> str:
        try:
            return self._run_text_prompt(system_prompt, user_prompt)
        except Exception:
            return ""

    def _call_responses_api(self, input_text: str) -> str:
        endpoint = f"{self.settings.base_url}/responses"
        payload = {
            "model": self.settings.model,
            "input": input_text,
        }
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        http_request = request.Request(
            endpoint,
            data=data,
            headers={
                "Authorization": f"Bearer {self.settings.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=self.settings.timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Qwen HTTP {exc.code}: {detail[:200]}") from exc
        except error.URLError as exc:
            raise RuntimeError(f"Qwen request failed: {exc.reason}") from exc
        return _extract_output_text(body)


class OpenAICompatibleChatModelClient(DeterministicModelClient):
    def __init__(self, settings: ModelProviderSettings) -> None:
        self.settings = settings

    def provider_name(self) -> str:
        return self.settings.provider_name or "openai_compatible"

    def summarize(self, request: JobRequest, matches: list[dict], total_matches: int) -> str:
        prompt = {
            "target_company": request.target_company,
            "query": request.query or request.raw_user_request,
            "total_matches": total_matches,
            "top_matches": [
                {
                    "display_name": item["display_name"],
                    "organization": item["organization"],
                    "role": item["role"],
                    "score": item["score"],
                    "explanation": item["explanation"],
                }
                for item in matches[:5]
            ],
        }
        response = self._safe_text_prompt(
            "You are a sourcing assistant. Summarize the sourcing results in 2 short English sentences.",
            json.dumps(prompt, ensure_ascii=False),
            max_tokens=220,
        )
        return response or super().summarize(request, matches, total_matches)

    def interpret_intent(self, request: JobRequest, draft_plan: dict[str, Any]) -> str:
        prompt = {
            "user_request": request.raw_user_request or request.query,
            "target_company": request.target_company,
            "draft_plan": draft_plan,
        }
        response = self._safe_text_prompt(
            "Interpret the user's sourcing intent in 2 short English sentences. "
            "Focus on objective, criteria, and why full-asset acquisition matters before retrieval.",
            json.dumps(prompt, ensure_ascii=False),
            max_tokens=220,
        )
        return response or super().interpret_intent(request, draft_plan)

    def analyze_page_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are helping with sourcing candidate research. Return strict JSON with keys "
            "summary,target_company_relation,role_signals,education_signals,work_history_signals,"
            "affiliation_signals,confidence_label,confidence_score,document_type,recommended_links,notes. "
            "education_signals must be a list of objects with keys school,degree,field,date_range. "
            "work_history_signals must be a list of objects with keys title,organization,date_range,description. "
            "affiliation_signals must be a list of objects with keys organization,relation,evidence. "
            "Use concise text. confidence_label must be one of high, medium, low.",
            json.dumps(payload, ensure_ascii=False),
            max_tokens=700,
        )
        if not response:
            return super().analyze_page_asset(payload)
        parsed = _safe_json_object(response)
        if not parsed:
            return super().analyze_page_asset(payload)
        result = super().analyze_page_asset(payload)
        result.update(
            {
                key: value
                for key, value in parsed.items()
                if key in result
                or key in {
                    "summary",
                    "target_company_relation",
                    "role_signals",
                    "education_signals",
                    "work_history_signals",
                    "affiliation_signals",
                    "confidence_label",
                    "confidence_score",
                    "document_type",
                    "recommended_links",
                    "notes",
                }
            }
        )
        return result

    def plan_search_strategy(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are designing a sourcing search plan. Return strict JSON with keys "
            "planner_mode,objective,query_bundles,follow_up_rules,review_triggers. "
            "query_bundles must be a list of objects with keys "
            "bundle_id,source_family,priority,objective,execution_mode,queries,filters. "
            "Use concise strings and do not include markdown.",
            json.dumps(
                {
                    "request": request.to_record(),
                    "draft_payload": draft_payload,
                },
                ensure_ascii=False,
            ),
            max_tokens=1200,
        )
        parsed = _safe_json_object(response)
        if not parsed:
            return super().plan_search_strategy(request, draft_payload)
        return parsed

    def judge_company_equivalence(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are verifying whether organization labels refer to the same real company. "
            "Return strict JSON with keys decision,matched_label,confidence_label,rationale. "
            "decision must be one of same_company,different_company,uncertain. "
            "matched_label must be one of the observed labels or empty. "
            "Be conservative: only return same_company when the labels clearly describe the same organization or the same LinkedIn company identity.",
            json.dumps(payload, ensure_ascii=False),
            max_tokens=220,
        )
        parsed = _safe_json_object(response)
        if not parsed:
            return super().judge_company_equivalence(payload)
        return {
            "decision": str(parsed.get("decision") or "uncertain").strip() or "uncertain",
            "matched_label": str(parsed.get("matched_label") or "").strip(),
            "confidence_label": str(parsed.get("confidence_label") or "low").strip() or "low",
            "rationale": str(parsed.get("rationale") or "").strip(),
        }

    def judge_profile_membership(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are verifying whether a LinkedIn profile really supports target-company membership. "
            "Return strict JSON with keys decision,confidence_label,rationale. "
            "decision must be one of confirmed_member,suspicious_member,non_member,uncertain. "
            "suspicious_member means the structured company match exists but the profile content looks implausible, "
            "spam-like, corrupted, or otherwise needs manual review. "
            "Be conservative: only return confirmed_member when the profile is plausibly consistent with a real member, "
            "and only return non_member when the profile clearly does not support target-company membership.",
            json.dumps(payload, ensure_ascii=False),
            max_tokens=260,
        )
        parsed = _safe_json_object(response)
        if not parsed:
            return super().judge_profile_membership(payload)
        return {
            "decision": str(parsed.get("decision") or "uncertain").strip() or "uncertain",
            "confidence_label": str(parsed.get("confidence_label") or "low").strip() or "low",
            "rationale": str(parsed.get("rationale") or "").strip(),
        }

    def healthcheck(self) -> dict[str, Any]:
        try:
            body = self._list_models()
            models = _extract_openai_models(body)
            return {
                "provider": self.provider_name(),
                "status": "ready" if self.settings.model in models else "model_missing",
                "model": self.settings.model,
                "base_url": self.settings.base_url,
                "available_models": models[:8],
            }
        except Exception as exc:
            return {
                "provider": self.provider_name(),
                "status": "degraded",
                "model": self.settings.model,
                "base_url": self.settings.base_url,
                "error": str(exc),
            }

    def _safe_text_prompt(self, system_prompt: str, user_prompt: str, *, max_tokens: int) -> str:
        try:
            return self._run_text_prompt(system_prompt, user_prompt, max_tokens=max_tokens)
        except Exception:
            return ""

    def _run_text_prompt(self, system_prompt: str, user_prompt: str, *, max_tokens: int) -> str:
        return self._call_chat_completions(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
        )

    def _list_models(self) -> dict[str, Any]:
        endpoint = f"{self.settings.base_url}/models"
        try:
            response = requests.get(
                endpoint,
                timeout=self.settings.timeout_seconds,
                headers={
                    "Authorization": f"Bearer {self.settings.api_key}",
                    "User-Agent": "Mozilla/5.0",
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            detail = exc.response.text if exc.response is not None else str(exc)
            status_code = exc.response.status_code if exc.response is not None else "?"
            raise RuntimeError(f"OpenAI-compatible HTTP {status_code}: {detail[:200]}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"OpenAI-compatible request failed: {exc}") from exc

    def _call_chat_completions(self, messages: list[dict[str, str]], *, max_tokens: int) -> str:
        endpoint = f"{self.settings.base_url}/chat/completions"
        payload = {
            "model": self.settings.model,
            "messages": messages,
            "max_tokens": max(32, int(max_tokens or 32)),
            "temperature": 0,
        }
        try:
            response = requests.post(
                endpoint,
                timeout=self.settings.timeout_seconds,
                headers={
                    "Authorization": f"Bearer {self.settings.api_key}",
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
                json=payload,
            )
            response.raise_for_status()
            body = response.json()
        except requests.HTTPError as exc:
            detail = exc.response.text if exc.response is not None else str(exc)
            status_code = exc.response.status_code if exc.response is not None else "?"
            raise RuntimeError(f"OpenAI-compatible HTTP {status_code}: {detail[:200]}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"OpenAI-compatible request failed: {exc}") from exc
        return _extract_openai_chat_text(body)


def _extract_output_text(payload: dict[str, Any]) -> str:
    output_text = str(payload.get("output_text", "")).strip()
    if output_text:
        return output_text
    for item in payload.get("output", []) or []:
        if item.get("type") != "message":
            continue
        for content in item.get("content", []) or []:
            if content.get("type") in {"output_text", "text"}:
                text = str(content.get("text", "")).strip()
                if text:
                    return text
    return ""


def _safe_json_object(text: str) -> dict[str, Any]:
    candidate = text.strip()
    if not candidate:
        return {}
    if "```" in candidate:
        candidate = candidate.split("```", 1)[-1]
        if candidate.startswith("json"):
            candidate = candidate[4:]
        candidate = candidate.rsplit("```", 1)[0]
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        payload = json.loads(candidate[start : end + 1])
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}

def _extract_openai_chat_text(payload: dict[str, Any]) -> str:
    for choice in payload.get("choices", []) or []:
        message = dict(choice.get("message") or {})
        content = message.get("content", "")
        if isinstance(content, str) and content.strip():
            return content.strip()
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                text = str(item.get("text") or item.get("content") or "").strip()
                if text:
                    parts.append(text)
            if parts:
                return "\n".join(parts).strip()
    return ""


def _extract_openai_models(payload: dict[str, Any]) -> list[str]:
    items = payload.get("data") or payload.get("models") or []
    models: list[str] = []
    for item in items:
        if isinstance(item, dict):
            model_id = str(item.get("id") or item.get("name") or "").strip()
            if model_id:
                models.append(model_id)
    return models


def build_model_client(
    model_settings: ModelProviderSettings | None = None,
    qwen_settings: QwenSettings | None = None,
) -> ModelClient:
    if model_settings and model_settings.enabled:
        return OpenAICompatibleChatModelClient(model_settings)
    if qwen_settings and qwen_settings.enabled:
        return QwenResponsesModelClient(qwen_settings)
    return DeterministicModelClient()
