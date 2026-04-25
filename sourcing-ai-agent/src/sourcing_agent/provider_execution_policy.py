from __future__ import annotations

from typing import Any


def task_uses_default_former_member_profile_search(
    *,
    strategy_type: str,
    employment_statuses: list[str] | tuple[str, ...] | None = None,
    search_channel_order: list[str] | tuple[str, ...] | None = None,
) -> bool:
    normalized_strategy = str(strategy_type or "").strip().lower()
    if normalized_strategy == "former_employee_search":
        return True
    normalized_statuses = {
        str(item or "").strip().lower()
        for item in list(employment_statuses or [])
        if str(item or "").strip()
    }
    normalized_channels = {
        str(item or "").strip().lower()
        for item in list(search_channel_order or [])
        if str(item or "").strip()
    }
    return "former" in normalized_statuses and "harvest_profile_search" in normalized_channels


def normalize_former_member_search_contract(
    *,
    strategy_type: str,
    employment_statuses: list[str] | tuple[str, ...] | None = None,
    search_channel_order: list[str] | tuple[str, ...] | None = None,
    cost_policy: dict[str, Any] | None = None,
    min_expected_results: int = 50,
) -> dict[str, Any]:
    normalized_policy = dict(cost_policy or {})
    normalized_channel_order = [
        str(item or "").strip()
        for item in list(search_channel_order or [])
        if str(item or "").strip()
    ]
    if not task_uses_default_former_member_profile_search(
        strategy_type=strategy_type,
        employment_statuses=employment_statuses,
        search_channel_order=normalized_channel_order,
    ):
        return {
            "cost_policy": normalized_policy,
            "search_channel_order": normalized_channel_order,
            "provider_search_only": False,
        }
    try:
        current_min_expected = int(normalized_policy.get("provider_people_search_min_expected_results") or 0)
    except (TypeError, ValueError):
        current_min_expected = 0
    return {
        "cost_policy": {
            **normalized_policy,
            "provider_people_search_mode": "primary_only",
            "provider_people_search_min_expected_results": max(
                current_min_expected,
                max(1, int(min_expected_results or 1)),
            ),
            "former_member_profile_search_contract": "default_on",
        },
        "search_channel_order": ["harvest_profile_search"],
        "provider_search_only": True,
    }
