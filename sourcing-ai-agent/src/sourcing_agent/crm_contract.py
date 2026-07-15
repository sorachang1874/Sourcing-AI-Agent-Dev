"""Shared CRM field semantics used by writers, storage, and Agent schemas."""

from __future__ import annotations

from types import MappingProxyType

CRM_STAGE_CATEGORIES = MappingProxyType(
    {
        "new": "open",
        "researching": "open",
        "outreach_ready": "open",
        "contacted_waiting": "waiting",
        "responded": "open",
        "interview_completed": "terminal_success",
        "accepted": "terminal_success",
        "rejected": "terminal_loss",
        "do_not_contact": "blocked",
        "archived": "archived",
    }
)
CRM_STAGE_VALUES = tuple(CRM_STAGE_CATEGORIES)

TARGET_FOLLOW_UP_TO_CRM_STAGE = MappingProxyType(
    {
        "pending_outreach": "outreach_ready",
        "contacted_waiting": "contacted_waiting",
        "interview_completed": "interview_completed",
        "accepted": "accepted",
        "rejected": "rejected",
    }
)
