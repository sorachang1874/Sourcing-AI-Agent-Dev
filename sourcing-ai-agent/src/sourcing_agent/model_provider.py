from __future__ import annotations

import json
import re
from typing import Any, Protocol
from urllib import error, request

import requests

from .document_extraction import infer_structured_signals_from_payload
from .domain import JobRequest
from .query_intent_policy import build_supported_rewrite_policy_prompt_context
from .runtime_environment import external_provider_mode
from .settings import ModelProviderSettings, QwenSettings

_OUTREACH_LAYER_PROMPT_TEMPLATE_VERSION = "outreach_layering_v3_explicit_greater_china_scope"


def _external_provider_mode() -> str:
    return external_provider_mode()


def _build_outreach_layer_system_prompt() -> str:
    return (
        "You are assigning a candidate to outreach layer 0/1/2/3 using only explicit public profile signals. "
        "Please comprehensively evaluate name, education history, work history, and language signals from LinkedIn profile detail text. "
        "请综合候选人的姓名、教育经历、工作经历、语言能力等公开信息，综合判断该候选人属于哪一层。 "
        "Return strict JSON with keys final_layer,confidence_label,evidence_clues,rationale. "
        "final_layer must be an integer 0..3. "
        "Layer definitions: "
        "0 = no sufficient outreach signal, "
        "1 = weak name-only signal, "
        "2 = broader Greater China region experience signal (Mainland China, Hong Kong, Macau, Taiwan, or Singapore), "
        "3 = strongest signal with Mainland China experience and/or explicit Chinese language signal (Mandarin/Cantonese/Chinese language variants). "
        "Important boundary: Layer 2 is broader than Mainland China and must NOT be interpreted as Mainland-only. "
        "边界要求：Layer 2 是“广义 Greater China（中国大陆、香港、澳门、台湾、新加坡）经历”，不是狭义中国大陆经历。 "
        "confidence_label must be one of high, medium, low. "
        "evidence_clues must be an array of concise strings. "
        "Do not infer or output ethnicity, nationality, religion, or other protected-attribute conclusions."
    )


def get_outreach_layer_prompt_template() -> dict[str, Any]:
    return {
        "version": _OUTREACH_LAYER_PROMPT_TEMPLATE_VERSION,
        "system_prompt": _build_outreach_layer_system_prompt(),
        "required_output_keys": ["final_layer", "confidence_label", "evidence_clues", "rationale"],
    }


def _with_supported_rewrite_policies(payload: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(payload or {})
    enriched["supported_rewrite_policies"] = build_supported_rewrite_policy_prompt_context()
    return enriched


def _execution_preferences_schema_prompt() -> str:
    return (
        "execution_preferences must be an object and may contain only "
        "acquisition_strategy_override, "
        "use_company_employees_lane, "
        "keyword_priority_only, "
        "former_keyword_queries_only, "
        "provider_people_search_query_strategy, "
        "provider_people_search_max_queries, "
        "large_org_keyword_probe_mode, "
        "force_fresh_run, "
        "precision_recall_bias, "
        "confirmed_company_scope, "
        "extra_source_families, "
        "reuse_existing_roster, "
        "run_former_search_seed. "
        "acquisition_strategy_override is only the base roster strategy axis "
        "(full_company_roster|scoped_search_roster|former_employee_search|investor_firm_roster). "
        "Do not misuse acquisition_strategy_override as a proxy for keyword priority, company-employees lane choice, "
        "former coverage, or people-search union strategy. "
        "provider_people_search_query_strategy must be all_queries_union or first_hit. "
        "provider_people_search_max_queries must be a small positive integer. "
        "Omit uncertain fields from execution_preferences. "
    )


def _explicit_thematic_keyword_boundary_prompt() -> str:
    return (
        "Use the user's explicit topical vocabulary as the boundary for keywords and research_direction_keywords. "
        "Do not expand a single direction into sibling, parent, child, or adjacent directions unless those exact terms also appear in the request. "
        "For example, if the request says Multimodal, keep Multimodal as the direction keyword and do not add Text, Vision, vision-language, or video generation unless they are explicitly present in the user text. "
        "Likewise, preserve the exact explicit topic set instead of broadening it into related families. "
    )


def _build_review_instruction_system_prompt() -> str:
    return (
        "You convert a natural-language sourcing plan review instruction into strict JSON. "
        "Return strict JSON with a single top-level key decision. "
        "Treat the instruction as four orthogonal control axes when possible: "
        "population boundary, scope boundary, acquisition lane policy, and fallback policy. "
        + _execution_preferences_schema_prompt()
        + "Only include fields directly supported by the instruction and editable_fields. "
        "Examples: "
        "'keyword-first' -> keyword_priority_only=true. "
        "'不要 company-employees' -> use_company_employees_lane=false. "
        "'former 也要' -> run_former_search_seed=true when applicable. "
        "'多 query 并集' -> provider_people_search_query_strategy=all_queries_union. "
        "Do not output markdown."
    )


def _build_request_normalization_system_prompt() -> str:
    return (
        "You normalize a sourcing user request into strict JSON. "
        "Return keys target_company,target_scope,categories,employment_statuses,keywords,must_have_keywords,"
        "organization_keywords,must_have_facets,must_have_primary_role_buckets,retrieval_strategy,query,execution_preferences,scope_disambiguation. "
        "You may also return optional list keys: team_keywords,sub_org_keywords,project_keywords,product_keywords,"
        "model_keywords,research_direction_keywords,technology_keywords. "
        "You may also return optional object key intent_axes. "
        "All list fields must be arrays of concise strings. Use empty string or [] when uncertain. "
        "If intent_axes is present, it may only contain the object keys "
        "population_boundary,scope_boundary,acquisition_lane_policy,fallback_policy,thematic_constraints. "
        "Represent the user's real intent with four orthogonal dimensions whenever possible: "
        "population boundary (categories + employment_statuses), "
        "scope boundary (target_company + organization_keywords + scope_disambiguation + confirmed_company_scope), "
        "acquisition lane policy (acquisition_strategy_override + use_company_employees_lane + keyword_priority_only + former_keyword_queries_only + large_org_keyword_probe_mode), "
        "and fallback policy (force_fresh_run + provider_people_search_query_strategy + provider_people_search_max_queries + reuse_existing_roster + run_former_search_seed). "
        + _execution_preferences_schema_prompt()
        + "scope_disambiguation must be an object and may contain only inferred_scope,sub_org_candidates,confidence,rationale. "
        "inferred_scope must be one of parent,sub_org_only,both,uncertain. "
        "sub_org_candidates must be concise org/team/product labels. confidence must be 0..1. "
        "Use scope_disambiguation when parent-company scope is ambiguous (for example Google vs DeepMind/Gemini/Veo). "
        "When disambiguating Google vs Google DeepMind, remember LinkedIn profiles may list employer as Google even for DeepMind members; "
        "treat this as an ambiguity signal and surface sub_org_candidates instead of collapsing too early. "
        "For example, for Gemini-related requests, prefer target_company=Google and preserve Gemini / Google DeepMind as organization or scope clues when relevant. "
        "Use the raw_user_request as the source of truth. fallback_request/current_request are hints only. "
        "Do not invent lab/model/product names that are not in the user text unless they are high-confidence standard aliases. "
        "If a term may be new or ambiguous (for example Avocado, TBD), keep the raw term in keywords and/or organization_keywords instead of dropping it, "
        "and prefer uncertain scope over confident hallucination. "
        "Organization names may legitimately contain generic-looking suffixes such as AI, Lab, Labs, Research, Systems, or Studio; "
        "when such a token is part of the resolved organization or sub-org name, keep it attached to that name instead of surfacing it again as a standalone ambiguous keyword. "
        "Prefer canonical organization names in target_company, team or sub-org names in organization_keywords, "
        "and direction/topic/model/technology terms in keywords. "
        + _explicit_thematic_keyword_boundary_prompt()
        +
        "Common AI direction terms such as Coding, Math, Text, Audio, Vision/Visual, Multimodal, Reasoning, "
        "Pre-train, Post-train, World model, Alignment, and Safety should usually be preserved as explicit keywords "
        "and, when applicable, repeated in research_direction_keywords instead of being dropped as generic language. "
        "If a term is both a team/sub-org clue and an important retrieval/search constraint, it may appear in both organization_keywords and keywords. "
        "Important extraction rule: only return atomic search/retrieval terms. "
        "Never return wrapper phrases or narrative fragments such as '在Veo和Nano Banana团队', '参与Veo和Nano Banana', "
        "'做Reasoning的人', or '负责多模态'; instead split them into the underlying org/product/topic terms such as "
        "'Veo', 'Nano Banana', 'Reasoning', or 'multimodal'. "
        "When the user asks for Product Manager / PM / 产品经理, prefer must_have_primary_role_buckets=['product_management'] "
        "and avoid duplicate keyword noise such as returning both PM and Product Manager unless they add distinct meaning. "
        "Populate product/model/research-direction optional keys whenever possible; if not found, return empty arrays. "
        "categories should prefer employee, former_employee, investor, researcher, engineer. "
        "employment_statuses should use current or former. "
        "If the user asks for people in a technical direction/topic (for example Pre-train, Post-train, Reasoning, "
        "Infra, Multimodal, Eval, RL, Coding, or Math) and does not explicitly say researcher-only or engineer-only, "
        "prefer categories=['researcher','engineer'] instead of narrowing to one side. "
        "retrieval_strategy must be one of empty string, structured, hybrid, semantic. "
        "Unless the user explicitly limits the scope, default employment_statuses to both current and former members. "
        "If the user says 华人, 泛华人, or Chinese members, interpret that as public Greater China study/work experience "
        "and Chinese or bilingual outreach relevance; do not output ethnicity, nationality, or protected-attribute labels. "
        "The payload includes supported_rewrite_policies, a read-only catalog of supported shorthand policies with trigger_sources and request_patch; "
        "use it as canonical rewrite metadata when it matches the user text, but do not force a policy that is unsupported by the request. "
        "Few-shot examples: "
        "Example 1 input: 给我Google做多模态（在Veo和Nano Banana团队）的人. "
        "Example 1 output: "
        "{\"target_company\":\"Google\",\"employment_statuses\":[\"current\",\"former\"],"
        "\"organization_keywords\":[\"Google DeepMind\",\"Veo\",\"Nano Banana\"],"
        "\"keywords\":[\"multimodal\",\"Veo\",\"Nano Banana\"],"
        "\"must_have_facets\":[\"multimodal\"],"
        "\"execution_preferences\":{\"keyword_priority_only\":true,\"provider_people_search_query_strategy\":\"all_queries_union\"},"
        "\"scope_disambiguation\":{\"inferred_scope\":\"both\",\"sub_org_candidates\":[\"Google DeepMind\",\"Veo\",\"Nano Banana\"],\"confidence\":0.8}}. "
        "Example 2 input: 我想找OpenAI在ChatGPT项目，做Reasoning的人. "
        "Example 2 output: "
        "{\"target_company\":\"OpenAI\",\"employment_statuses\":[\"current\",\"former\"],"
        "\"organization_keywords\":[\"ChatGPT\"],"
        "\"keywords\":[\"Reasoning\",\"ChatGPT\"],"
        "\"product_keywords\":[\"ChatGPT\"],"
        "\"research_direction_keywords\":[\"Reasoning\"]}. "
        "Example 3 input: 给我 Meta TBD 的 infra 成员. "
        "Example 3 output: "
        "{\"target_company\":\"Meta\",\"employment_statuses\":[\"current\",\"former\"],"
        "\"organization_keywords\":[\"TBD\"],"
        "\"keywords\":[\"infra\",\"TBD\"],"
        "\"scope_disambiguation\":{\"inferred_scope\":\"uncertain\",\"sub_org_candidates\":[\"TBD\"],\"confidence\":0.4}}. "
        "Example 4 input: 给我Anthropic做Coding、Math和Audio方向的人. "
        "Example 4 output: "
        "{\"target_company\":\"Anthropic\",\"employment_statuses\":[\"current\",\"former\"],"
        "\"keywords\":[\"Coding\",\"Math\",\"Audio\"],"
        "\"research_direction_keywords\":[\"Coding\",\"Math\",\"Audio\"]}."
    )


def _build_spreadsheet_contact_normalization_system_prompt() -> str:
    return (
        "You normalize a spreadsheet contact upload into strict JSON. "
        "Return keys contacts_detected,selected_sheets,ignored_sheets,notes. "
        "contacts_detected must be a boolean. "
        "selected_sheets must be an array of objects with keys sheet_name,column_mapping,confidence_label,notes. "
        "column_mapping must be an object containing only these optional keys: "
        "name,company,title,linkedin_url,email. "
        "Each column_mapping value must be the exact source header text from the sheet, or an empty string when missing. "
        "confidence_label must be one of high, medium, low. "
        "ignored_sheets must be an array of concise sheet names. "
        "Infer the contact sheet and header mapping conservatively from headers and sample rows. "
        "Do not invent columns that are not present. "
        "If the workbook clearly contains people/contact rows, set contacts_detected=true."
    )


_SPREADSHEET_CONTACT_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "name": (
        "name",
        "full name",
        "candidate",
        "person",
        "姓名",
        "名字",
    ),
    "company": (
        "company",
        "organization",
        "org",
        "employer",
        "firm",
        "机构",
        "公司",
        "单位",
    ),
    "title": (
        "title",
        "job title",
        "role",
        "position",
        "headline",
        "职位",
        "岗位",
        "职务",
    ),
    "linkedin_url": (
        "linkedin",
        "linkedin url",
        "linkedin profile",
        "profile url",
        "linkedin链接",
        "linkedin 链接",
        "领英",
    ),
    "email": (
        "email",
        "email address",
        "mail",
        "邮箱",
        "电子邮箱",
    ),
}


def _normalize_spreadsheet_header_token(value: Any) -> str:
    normalized = " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())
    if not normalized:
        return ""
    return normalized


def _infer_spreadsheet_contact_schema(payload: dict[str, Any]) -> dict[str, Any]:
    selected_sheets: list[dict[str, Any]] = []
    ignored_sheets: list[str] = []
    for raw_sheet in list(payload.get("sheets") or []):
        if not isinstance(raw_sheet, dict):
            continue
        sheet_name = str(raw_sheet.get("sheet_name") or raw_sheet.get("name") or "").strip()
        headers = [str(item or "").strip() for item in list(raw_sheet.get("headers") or []) if str(item or "").strip()]
        if not sheet_name:
            continue
        header_lookup = {_normalize_spreadsheet_header_token(header): header for header in headers}
        column_mapping: dict[str, str] = {}
        matched_field_count = 0
        for field_name, aliases in _SPREADSHEET_CONTACT_FIELD_ALIASES.items():
            matched_header = ""
            for alias in aliases:
                normalized_alias = _normalize_spreadsheet_header_token(alias)
                for normalized_header, source_header in header_lookup.items():
                    if normalized_header == normalized_alias or normalized_alias in normalized_header:
                        matched_header = source_header
                        break
                if matched_header:
                    break
            if matched_header:
                matched_field_count += 1
            column_mapping[field_name] = matched_header
        contacts_detected = bool(column_mapping.get("name")) and bool(
            column_mapping.get("company") or column_mapping.get("title") or column_mapping.get("linkedin_url")
        )
        if contacts_detected:
            confidence_label = "high" if matched_field_count >= 4 else "medium"
            selected_sheets.append(
                {
                    "sheet_name": sheet_name,
                    "column_mapping": column_mapping,
                    "confidence_label": confidence_label,
                    "notes": "deterministic_header_match",
                }
            )
        else:
            ignored_sheets.append(sheet_name)
    return {
        "contacts_detected": bool(selected_sheets),
        "selected_sheets": selected_sheets[:3],
        "ignored_sheets": ignored_sheets[:8],
        "notes": "Deterministic spreadsheet schema inference fallback.",
    }


class ModelClient(Protocol):
    def summarize(self, request: JobRequest, matches: list[dict], total_matches: int) -> str: ...

    def normalize_request(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def normalize_spreadsheet_contacts(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def normalize_review_instruction(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def normalize_refinement_instruction(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def interpret_intent(self, request: JobRequest, draft_plan: dict[str, Any]) -> str: ...

    def draft_intent_brief(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]: ...

    def plan_search_strategy(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]: ...

    def analyze_page_asset(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def analyze_public_web_candidate_signals(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def judge_company_equivalence(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def judge_profile_membership(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def synthesize_manual_review(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def evaluate_outreach_profile(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def provider_name(self) -> str: ...

    def supports_outreach_ai_verification(self) -> bool: ...

    def healthcheck(self) -> dict[str, Any]: ...


class DeterministicModelClient:
    def provider_name(self) -> str:
        return "deterministic"

    def supports_outreach_ai_verification(self) -> bool:
        return False

    def summarize(self, request: JobRequest, matches: list[dict], total_matches: int) -> str:
        if not matches:
            return "No candidates matched the current sourcing criteria."
        top = matches[0]
        return (
            f"Found {total_matches} matches for {request.target_company or 'the target company'}. "
            f"Top match is {top['display_name']} with score {top['score']}."
        )

    def normalize_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def normalize_spreadsheet_contacts(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _infer_spreadsheet_contact_schema(payload)

    def normalize_review_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def normalize_refinement_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def interpret_intent(self, request: JobRequest, draft_plan: dict[str, Any]) -> str:
        company = request.target_company or "目标公司"
        categories = ", ".join(request.categories) if request.categories else "unspecified categories"
        strategy = draft_plan.get("retrieval_plan", {}).get("strategy", "hybrid")
        return (
            f"User wants a sourcing workflow for {company}, focused on {categories}, "
            f"with early clarification and a {strategy} retrieval path after full-asset acquisition."
        )

    def draft_intent_brief(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]:
        return {}

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

    def analyze_public_web_candidate_signals(self, payload: dict[str, Any]) -> dict[str, Any]:
        candidate = dict(payload.get("candidate") or {})
        candidate_name = str(candidate.get("candidate_name") or "").strip().lower()
        current_company = str(candidate.get("current_company") or "").strip().lower()
        name_tokens = [token for token in re.findall(r"[a-z0-9]+", candidate_name) if len(token) > 1][:4]
        company_tokens = [token for token in re.findall(r"[a-z0-9]+", current_company) if len(token) > 1][:4]
        assessments: list[dict[str, Any]] = []
        for item in list(payload.get("email_candidates") or []):
            if not isinstance(item, dict):
                continue
            email = str(item.get("normalized_value") or item.get("value") or "").strip().lower()
            local_part = email.split("@", 1)[0]
            evidence = " ".join(
                [
                    str(item.get("source_title") or ""),
                    str(item.get("source_url") or ""),
                    str(item.get("evidence_excerpt") or ""),
                ]
            ).lower()
            identity_score = 0.2
            if name_tokens and any(token in local_part or token in evidence for token in name_tokens):
                identity_score += 0.35
            if company_tokens and any(token in evidence for token in company_tokens):
                identity_score += 0.15
            if str(item.get("source_family") or "") in {
                "profile_web_presence",
                "resume_and_documents",
                "candidate_publication_presence",
            }:
                identity_score += 0.15
            identity_score = min(identity_score, 0.95)
            confidence_score = max(float(item.get("confidence_score") or 0.0), identity_score)
            publishable = bool(item.get("publishable", True))
            suppression_reason = str(item.get("suppression_reason") or "").strip()
            if identity_score < 0.35:
                publishable = False
                suppression_reason = suppression_reason or "weak_identity_match"
            assessments.append(
                {
                    "email": email,
                    "email_type": str(item.get("email_type") or "unknown").strip() or "unknown",
                    "confidence_label": "high" if confidence_score >= 0.72 else "medium" if confidence_score >= 0.45 else "low",
                    "confidence_score": round(min(confidence_score, 0.95), 2),
                    "publishable": publishable,
                    "promotion_status": (
                        "promotion_recommended"
                        if publishable and confidence_score >= 0.72 and not suppression_reason
                        else "suppressed"
                        if suppression_reason
                        else "not_promoted"
                    ),
                    "suppression_reason": suppression_reason,
                    "identity_match_label": (
                        "likely_same_person"
                        if identity_score >= 0.65
                        else "needs_review"
                        if identity_score >= 0.35
                        else "ambiguous_identity"
                    ),
                    "identity_match_score": round(identity_score, 2),
                    "rationale": "Deterministic public-web signal adjudication fallback.",
                }
            )
        return {
            "summary": "Deterministic public-web signal adjudication fallback.",
            "email_assessments": assessments,
            "link_assessments": self._deterministic_public_web_link_assessments(payload),
            "academic_summary": _build_deterministic_academic_summary(payload),
            "notes": [],
        }

    def _deterministic_public_web_link_assessments(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        candidate = dict(payload.get("candidate") or {})
        candidate_name = str(candidate.get("candidate_name") or "").strip().lower()
        current_company = str(candidate.get("current_company") or "").strip().lower()
        name_tokens = [token for token in re.findall(r"[a-z0-9]+", candidate_name) if len(token) > 1][:4]
        company_tokens = [token for token in re.findall(r"[a-z0-9]+", current_company) if len(token) > 1][:4]
        assessments: list[dict[str, Any]] = []
        for item in list(payload.get("entry_links") or []):
            if not isinstance(item, dict):
                continue
            url = str(item.get("normalized_url") or item.get("url") or "").strip()
            text = " ".join(
                [
                    str(item.get("title") or ""),
                    str(item.get("snippet") or ""),
                    url,
                ]
            ).lower()
            identity_score = 0.15
            if name_tokens and all(token in text for token in name_tokens[:2]):
                identity_score += 0.45
            elif name_tokens and any(token in text for token in name_tokens):
                identity_score += 0.2
            if company_tokens and any(token in text for token in company_tokens):
                identity_score += 0.1
            signal_type = str(item.get("entry_type") or "other").strip() or "other"
            if signal_type in {"github_url", "x_url", "substack_url", "scholar_url", "personal_homepage", "academic_profile"}:
                identity_score += 0.1
            if signal_type == "company_page":
                identity_score -= 0.05
            identity_score = max(0.0, min(identity_score, 0.95))
            label = (
                "likely_same_person"
                if identity_score >= 0.65
                else "needs_review"
                if identity_score >= 0.35
                else "ambiguous_identity"
            )
            assessments.append(
                {
                    "url": url,
                    "signal_type": signal_type,
                    "identity_match_label": label,
                    "identity_match_score": round(identity_score, 2),
                    "confidence_label": "high" if identity_score >= 0.75 else "medium" if identity_score >= 0.45 else "low",
                    "rationale": "Deterministic public-web link adjudication fallback.",
                }
            )
        return assessments

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

    def synthesize_manual_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {}

    def evaluate_outreach_profile(self, payload: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG002
        return {}


class OfflineModelClient(DeterministicModelClient):
    def __init__(self, *, mode: str) -> None:
        normalized_mode = str(mode or "simulate").strip().lower() or "simulate"
        self.mode = normalized_mode if normalized_mode in {"simulate", "replay", "scripted"} else "simulate"

    def provider_name(self) -> str:
        return "offline_model"

    def healthcheck(self) -> dict[str, Any]:
        if self.mode == "simulate":
            note = "Simulated model provider; no external model request will be sent."
        elif self.mode == "replay":
            note = "Replay model provider; external model requests are disabled."
        else:
            note = "Scripted model provider; external model requests are disabled and orchestration uses scripted fixtures."
        return {
            "provider": self.provider_name(),
            "status": "ready",
            "provider_mode": self.mode,
            "note": note,
        }


class QwenResponsesModelClient(DeterministicModelClient):
    def __init__(self, settings: QwenSettings) -> None:
        self.settings = settings

    def provider_name(self) -> str:
        return "qwen"

    def supports_outreach_ai_verification(self) -> bool:
        return True

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

    def normalize_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            _build_request_normalization_system_prompt(),
            json.dumps(_with_supported_rewrite_policies(payload), ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

    def normalize_spreadsheet_contacts(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            _build_spreadsheet_contact_normalization_system_prompt(),
            json.dumps(payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else super().normalize_spreadsheet_contacts(payload)

    def normalize_review_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            _build_review_instruction_system_prompt(),
            json.dumps(payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

    def normalize_refinement_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You convert a natural-language post-acquisition sourcing refinement instruction into strict JSON. "
            "Return strict JSON with a single top-level key patch. "
            "patch may contain only these optional keys: "
            "asset_view (canonical_merged|strict_roster_only), "
            "categories (array of strings), "
            "employment_statuses (array using current or former), "
            "keywords (array of strings), "
            "must_have_keywords (array of strings), "
            "exclude_keywords (array of strings), "
            "organization_keywords (array of strings), "
            "must_have_facets (array of strings), "
            "must_have_primary_role_buckets (array of strings), "
            "retrieval_strategy (structured|hybrid|semantic), "
            "top_k (integer), "
            "semantic_rerank_limit (integer). "
            "You may also return optional list keys: team_keywords,sub_org_keywords,project_keywords,product_keywords,"
            "model_keywords,research_direction_keywords,technology_keywords. "
            "Use the raw instruction as the source of truth. base_request is context only. "
            "Do not invent lab/model/product names that are not in the instruction unless they are high-confidence standard aliases. "
            "If a term may be new or ambiguous (for example Avocado, TBD), keep the raw term in keywords and/or organization_keywords instead of dropping it. "
            "Prefer canonical company names in target_company context, team/sub-org/project labels in organization_keywords, "
            "and direction/topic/model/technology terms in keywords or the optional keyword-family fields. "
            + _explicit_thematic_keyword_boundary_prompt()
            +
            "If the instruction narrows scope to a subset, prefer preserving those terms rather than collapsing them away. "
            "Only include fields directly supported by the instruction. "
            "If the instruction uses shorthand like 华人 or Chinese members, rewrite it into public Greater China experience "
            "and Chinese or bilingual outreach keywords instead of identity labels. "
            "The payload includes supported_rewrite_policies, a read-only catalog of supported shorthand policies with trigger_sources and request_patch; "
            "use it as canonical rewrite metadata when the instruction clearly matches one of those policies. "
            "Omit uncertain fields. Do not output markdown.",
            json.dumps(_with_supported_rewrite_policies(payload), ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

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

    def draft_intent_brief(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are preparing the first product-facing planning message for a sourcing workflow. "
            "Return strict JSON with keys identified_request,target_output,default_execution_strategy,review_focus. "
            "Each key must map to an array of concise Chinese bullet strings. "
            "Keep the content practical, specific, and operational. Do not include markdown headings.",
            json.dumps(draft_payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

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

    def analyze_public_web_candidate_signals(self, payload: dict[str, Any]) -> dict[str, Any]:
        fallback = super().analyze_public_web_candidate_signals(payload)
        response = self._safe_text_prompt(
            _build_public_web_signal_adjudication_prompt(),
            json.dumps(payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return _normalize_public_web_signal_adjudication(parsed, fallback=fallback)

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

    def synthesize_manual_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are summarizing a sourcing manual-review case for a human reviewer. "
            "Return strict JSON with keys summary,confidence_takeaways,conflict_points,recommended_checks. "
            "summary must be a concise string. "
            "confidence_takeaways, conflict_points, and recommended_checks must be arrays of concise strings. "
            "Do not decide membership, do not output approve/reject instructions, and do not override the existing queue decision.",
            json.dumps(payload, ensure_ascii=False),
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

    def evaluate_outreach_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._run_text_prompt(
                _build_outreach_layer_system_prompt(),
                json.dumps(payload, ensure_ascii=False),
            )
        except Exception as exc:
            return {"error": str(exc)}
        parsed = _safe_json_object(response)
        if not parsed:
            return {"error": "non_json_response", "raw_preview": response[:240]}
        return _normalize_outreach_profile_response(parsed)

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

    def supports_outreach_ai_verification(self) -> bool:
        return True

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

    def normalize_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            _build_request_normalization_system_prompt(),
            json.dumps(_with_supported_rewrite_policies(payload), ensure_ascii=False),
            max_tokens=700,
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

    def normalize_spreadsheet_contacts(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            _build_spreadsheet_contact_normalization_system_prompt(),
            json.dumps(payload, ensure_ascii=False),
            max_tokens=520,
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else super().normalize_spreadsheet_contacts(payload)

    def normalize_review_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            _build_review_instruction_system_prompt(),
            json.dumps(payload, ensure_ascii=False),
            max_tokens=500,
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

    def normalize_refinement_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You convert a natural-language post-acquisition sourcing refinement instruction into strict JSON. "
            "Return strict JSON with a single top-level key patch. "
            "patch may contain only these optional keys: "
            "asset_view (canonical_merged|strict_roster_only), "
            "categories (array of strings), "
            "employment_statuses (array using current or former), "
            "keywords (array of strings), "
            "must_have_keywords (array of strings), "
            "exclude_keywords (array of strings), "
            "organization_keywords (array of strings), "
            "must_have_facets (array of strings), "
            "must_have_primary_role_buckets (array of strings), "
            "retrieval_strategy (structured|hybrid|semantic), "
            "top_k (integer), "
            "semantic_rerank_limit (integer). "
            "You may also return optional list keys: team_keywords,sub_org_keywords,project_keywords,product_keywords,"
            "model_keywords,research_direction_keywords,technology_keywords. "
            "Use the raw instruction as the source of truth. base_request is context only. "
            "Do not invent lab/model/product names that are not in the instruction unless they are high-confidence standard aliases. "
            "If a term may be new or ambiguous (for example Avocado, TBD), keep the raw term in keywords and/or organization_keywords instead of dropping it. "
            "Prefer canonical company names in target_company context, team/sub-org/project labels in organization_keywords, "
            "and direction/topic/model/technology terms in keywords or the optional keyword-family fields. "
            + _explicit_thematic_keyword_boundary_prompt()
            +
            "If the instruction narrows scope to a subset, prefer preserving those terms rather than collapsing them away. "
            "Only include fields directly supported by the instruction. "
            "If the instruction uses shorthand like 华人 or Chinese members, rewrite it into public Greater China experience "
            "and Chinese or bilingual outreach keywords instead of identity labels. "
            "The payload includes supported_rewrite_policies, a read-only catalog of supported shorthand policies with trigger_sources and request_patch; "
            "use it as canonical rewrite metadata when the instruction clearly matches one of those policies. "
            "Omit uncertain fields. Do not output markdown.",
            json.dumps(_with_supported_rewrite_policies(payload), ensure_ascii=False),
            max_tokens=520,
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

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

    def draft_intent_brief(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are preparing the first product-facing planning message for a sourcing workflow. "
            "Return strict JSON with keys identified_request,target_output,default_execution_strategy,review_focus. "
            "Each key must map to an array of concise Chinese bullet strings. "
            "Keep the content practical, specific, and operational. Do not include markdown headings.",
            json.dumps(draft_payload, ensure_ascii=False),
            max_tokens=900,
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

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

    def analyze_public_web_candidate_signals(self, payload: dict[str, Any]) -> dict[str, Any]:
        fallback = super().analyze_public_web_candidate_signals(payload)
        response = self._safe_text_prompt(
            _build_public_web_signal_adjudication_prompt(),
            json.dumps(payload, ensure_ascii=False),
            max_tokens=900,
        )
        parsed = _safe_json_object(response)
        return _normalize_public_web_signal_adjudication(parsed, fallback=fallback)

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

    def synthesize_manual_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._safe_text_prompt(
            "You are summarizing a sourcing manual-review case for a human reviewer. "
            "Return strict JSON with keys summary,confidence_takeaways,conflict_points,recommended_checks. "
            "summary must be a concise string. "
            "confidence_takeaways, conflict_points, and recommended_checks must be arrays of concise strings. "
            "Do not decide membership, do not output approve/reject instructions, and do not override the existing queue decision.",
            json.dumps(payload, ensure_ascii=False),
            max_tokens=420,
        )
        parsed = _safe_json_object(response)
        return parsed if parsed else {}

    def evaluate_outreach_profile(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self._run_text_prompt(
                _build_outreach_layer_system_prompt(),
                json.dumps(payload, ensure_ascii=False),
                max_tokens=360,
            )
        except Exception as exc:
            return {"error": str(exc)}
        parsed = _safe_json_object(response)
        if not parsed:
            return {"error": "non_json_response", "raw_preview": response[:240]}
        return _normalize_outreach_profile_response(parsed)

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


def _build_public_web_signal_adjudication_prompt() -> str:
    return (
        "You are adjudicating public-web evidence for recruiter-facing candidate research. "
        "Return strict JSON with keys summary,email_assessments,link_assessments,academic_summary,notes. "
        "email_assessments must be a list of objects with keys email,email_type,confidence_label,confidence_score,"
        "publishable,promotion_status,suppression_reason,identity_match_label,identity_match_score,rationale. "
        "link_assessments must be a list of objects with keys url,signal_type,identity_match_label,"
        "identity_match_score,confidence_label,rationale. "
        "academic_summary must be an object with keys research_directions,notable_work,academic_affiliations,"
        "publication_signals,outreach_angles,confidence_label,evidence_sources. "
        "research_directions, academic_affiliations, publication_signals, outreach_angles, and evidence_sources must be arrays "
        "of concise strings. notable_work must be a list of objects with keys title,year,venue,why_it_matters,evidence. "
        "email_type must be one of personal,academic,company,generic,unknown. "
        "confidence_label must be high, medium, or low. promotion_status must be not_promoted,"
        "promotion_recommended,rejected,or suppressed. "
        "identity_match_label must be one of confirmed,likely_same_person,needs_review,ambiguous_identity,not_same_person. "
        "Use high confidence only when the source plausibly belongs to the target candidate, such as a personal homepage, "
        "CV/resume, paper PDF, university profile, or strong company-domain evidence. "
        "Suppress generic inboxes, unrelated coauthor emails, same-name collisions, and boilerplate contacts. "
        "Suppress paper-title artifacts that look like emails, such as Learning@Scale.Conference. "
        "Do not infer a real address from Google Scholar verified-domain text such as Verified email at google.com. "
        "For grouped addresses such as {barryz, lesli}@domain, judge each expanded address separately and identify whether it is "
        "the candidate's own address or a coauthor/lab group address. "
        "For paper PDFs, treat an email as high confidence only when the surrounding author block or local-part evidence ties it "
        "to the target candidate; otherwise mark coauthor or lab addresses as needs_review or suppressed. "
        "For X/Twitter, Substack, GitHub, Scholar, and homepage links discovered only from search results, do not mark confirmed "
        "solely because the name matches. Prefer needs_review or ambiguous_identity unless URL/title/snippet/fetched content "
        "contains strong identity evidence such as matching employer, education, homepage cross-links, or LinkedIn context. "
        "Use the candidate LinkedIn URL/key, headline, known work history, education, current company, source URL, title, "
        "snippet, search_evidence, and evidence_slices to judge identity. search_evidence contains DataForSEO/search-result "
        "URL/title/snippet/query context and is useful for platform pages that cannot be fetched. evidence_slices are source-aware model-safe excerpts from fetched "
        "GitHub, Scholar, X/Twitter, Substack, homepage, academic profile, resume/CV, and publication pages; do not assume "
        "raw HTML/PDF is available in the prompt. "
        "When Scholar, publication, academic profile, or personal homepage evidence is present, summarize the candidate's "
        "research directions, important publications or technical achievements, academic affiliations, and practical outreach "
        "angles. First decide whether each evidence slice belongs to the target candidate; do not use same-name Scholar pages, "
        "coauthor profiles, unrelated university profiles, or conflicting profiles in academic_summary. Keep academic_summary "
        "grounded in evidence_slices that you judge confirmed or likely_same_person, and mark low confidence when identity or "
        "evidence is weak. "
        "Do not promote anything to primary email; only recommend promotion when a human should review it."
    )


def _normalize_public_web_signal_adjudication(
    parsed: dict[str, Any],
    *,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    if not parsed:
        return fallback
    normalized: dict[str, Any] = {
        "summary": str(parsed.get("summary") or fallback.get("summary") or "").strip(),
        "email_assessments": [],
        "link_assessments": [],
        "academic_summary": _normalize_academic_summary(
            parsed.get("academic_summary"),
            fallback=dict(fallback.get("academic_summary") or {}),
        ),
        "notes": [str(item).strip() for item in list(parsed.get("notes") or []) if str(item).strip()][:10],
    }
    for item in list(parsed.get("email_assessments") or []):
        if not isinstance(item, dict):
            continue
        email = str(item.get("email") or item.get("value") or "").strip().lower()
        if not email:
            continue
        confidence_label = str(item.get("confidence_label") or "low").strip().lower()
        if confidence_label not in {"high", "medium", "low"}:
            confidence_label = "low"
        try:
            confidence_score = max(0.0, min(float(item.get("confidence_score") or 0.0), 1.0))
        except (TypeError, ValueError):
            confidence_score = 0.0
        promotion_status = str(item.get("promotion_status") or "not_promoted").strip().lower()
        if promotion_status not in {"not_promoted", "promotion_recommended", "rejected", "suppressed"}:
            promotion_status = "not_promoted"
        try:
            identity_match_score = max(0.0, min(float(item.get("identity_match_score") or 0.0), 1.0))
        except (TypeError, ValueError):
            identity_match_score = 0.0
        normalized["email_assessments"].append(
            {
                "email": email,
                "email_type": str(item.get("email_type") or "unknown").strip().lower() or "unknown",
                "confidence_label": confidence_label,
                "confidence_score": round(confidence_score, 2),
                "publishable": bool(item.get("publishable", False)),
                "promotion_status": promotion_status,
                "suppression_reason": str(item.get("suppression_reason") or "").strip(),
                "identity_match_label": str(item.get("identity_match_label") or "needs_review").strip(),
                "identity_match_score": round(identity_match_score, 2),
                "rationale": str(item.get("rationale") or "").strip(),
            }
        )
    for item in list(parsed.get("link_assessments") or []):
        if isinstance(item, dict):
            try:
                identity_match_score = max(0.0, min(float(item.get("identity_match_score") or 0.0), 1.0))
            except (TypeError, ValueError):
                identity_match_score = 0.0
            normalized["link_assessments"].append(
                {
                    "url": str(item.get("url") or "").strip(),
                    "signal_type": str(item.get("signal_type") or "").strip(),
                    "identity_match_label": str(item.get("identity_match_label") or "needs_review").strip(),
                    "identity_match_score": round(identity_match_score, 2),
                    "confidence_label": str(item.get("confidence_label") or "low").strip(),
                    "rationale": str(item.get("rationale") or "").strip(),
                }
            )
    if not normalized["email_assessments"] and fallback.get("email_assessments"):
        normalized["email_assessments"] = list(fallback.get("email_assessments") or [])
    if not normalized["link_assessments"] and fallback.get("link_assessments"):
        normalized["link_assessments"] = list(fallback.get("link_assessments") or [])
    return normalized


def _build_deterministic_academic_summary(payload: dict[str, Any]) -> dict[str, Any]:
    research_directions: list[str] = []
    affiliations: list[str] = []
    publication_signals: list[str] = []
    notable_work: list[dict[str, str]] = []
    evidence_sources: list[str] = []
    for evidence_slice in list(payload.get("evidence_slices") or []):
        if not isinstance(evidence_slice, dict):
            continue
        source_type = str(evidence_slice.get("source_type") or "").strip()
        if source_type not in {"google_scholar_profile", "publication_pdf", "academic_profile", "personal_homepage", "resume_or_cv"}:
            continue
        source_url = str(evidence_slice.get("final_url") or evidence_slice.get("source_url") or "").strip()
        if source_url:
            evidence_sources.append(source_url)
        structured = dict(evidence_slice.get("structured_signals") or {})
        for item in list(structured.get("research_interests") or []):
            text = str(item or "").strip()
            if text and text not in research_directions:
                research_directions.append(text)
        for item in list(structured.get("affiliation_signals") or []):
            if not isinstance(item, dict):
                continue
            organization = str(item.get("organization") or "").strip()
            if organization and organization not in affiliations:
                affiliations.append(organization)
        for item in list(structured.get("scholar_publications") or []):
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            record = {
                "title": title,
                "year": str(item.get("year") or "").strip(),
                "venue": str(item.get("venue") or "").strip(),
                "why_it_matters": "High-signal Google Scholar publication candidate.",
                "evidence": str(item.get("citations") or item.get("authors") or source_url).strip(),
            }
            if record not in notable_work:
                notable_work.append({key: value for key, value in record.items() if value})
                publication_signals.append(title)
    confidence_label = "high" if notable_work or len(research_directions) >= 2 else "medium" if research_directions or affiliations else "low"
    outreach_angles = []
    for direction in research_directions[:3]:
        outreach_angles.append(f"Reference their work in {direction}.")
    for work in notable_work[:2]:
        title = str(work.get("title") or "").strip()
        if title:
            outreach_angles.append(f"Mention the publication/technical work: {title}.")
    return {
        "research_directions": research_directions[:8],
        "notable_work": notable_work[:8],
        "academic_affiliations": affiliations[:6],
        "publication_signals": publication_signals[:8],
        "outreach_angles": outreach_angles[:6],
        "confidence_label": confidence_label,
        "evidence_sources": _dedupe_strings(evidence_sources)[:8],
    }


def _normalize_academic_summary(value: Any, *, fallback: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    fallback = fallback or {}
    confidence_label = str(raw.get("confidence_label") or fallback.get("confidence_label") or "low").strip().lower()
    if confidence_label not in {"high", "medium", "low"}:
        confidence_label = "low"
    notable_work: list[dict[str, str]] = []
    for item in list(raw.get("notable_work") or fallback.get("notable_work") or []):
        if isinstance(item, dict):
            record = {
                key: str(item.get(key) or "").strip()
                for key in ("title", "year", "venue", "why_it_matters", "evidence")
                if str(item.get(key) or "").strip()
            }
            if record and record not in notable_work:
                notable_work.append(record)
        elif str(item or "").strip():
            notable_work.append({"title": str(item).strip()})
    return {
        "research_directions": _string_list_or_fallback(raw.get("research_directions"), fallback.get("research_directions"))[:10],
        "notable_work": notable_work[:10],
        "academic_affiliations": _string_list_or_fallback(raw.get("academic_affiliations"), fallback.get("academic_affiliations"))[:8],
        "publication_signals": _string_list_or_fallback(raw.get("publication_signals"), fallback.get("publication_signals"))[:10],
        "outreach_angles": _string_list_or_fallback(raw.get("outreach_angles"), fallback.get("outreach_angles"))[:8],
        "confidence_label": confidence_label,
        "evidence_sources": _string_list_or_fallback(raw.get("evidence_sources"), fallback.get("evidence_sources"))[:10],
    }


def _string_list_or_fallback(value: Any, fallback: Any) -> list[str]:
    items = value if isinstance(value, list) else fallback if isinstance(fallback, list) else []
    return _dedupe_strings([str(item or "").strip() for item in items if str(item or "").strip()])


def _dedupe_strings(items: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = str(item or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


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


def _normalize_outreach_profile_response(payload: dict[str, Any]) -> dict[str, Any]:
    final_layer_raw = payload.get("final_layer")
    try:
        final_layer = int(final_layer_raw)
    except (TypeError, ValueError):
        # Backward compatibility for older boolean-based provider outputs
        mainland_or_language = bool(payload.get("mainland_or_language_supported"))
        greater_china = bool(payload.get("greater_china_experience_supported"))
        name_signal = bool(payload.get("name_signal_supported"))
        if mainland_or_language:
            final_layer = 3
        elif greater_china:
            final_layer = 2
        elif name_signal:
            final_layer = 1
        else:
            final_layer = 0
    final_layer = max(0, min(final_layer, 3))
    confidence_label = str(payload.get("confidence_label") or "low").strip().lower()
    if confidence_label not in {"high", "medium", "low"}:
        confidence_label = "low"
    return {
        "final_layer": final_layer,
        "confidence_label": confidence_label,
        "evidence_clues": [str(item).strip() for item in list(payload.get("evidence_clues") or []) if str(item).strip()][:8],
        "rationale": str(payload.get("rationale") or "").strip(),
    }


def build_model_client(
    model_settings: ModelProviderSettings | None = None,
    qwen_settings: QwenSettings | None = None,
) -> ModelClient:
    external_mode = _external_provider_mode()
    if external_mode in {"simulate", "replay", "scripted"}:
        return OfflineModelClient(mode=external_mode)
    if qwen_settings and qwen_settings.enabled:
        return QwenResponsesModelClient(qwen_settings)
    if model_settings and model_settings.enabled:
        return OpenAICompatibleChatModelClient(model_settings)
    return DeterministicModelClient()
