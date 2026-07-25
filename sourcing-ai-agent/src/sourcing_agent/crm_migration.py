from __future__ import annotations

from typing import Any

from .crm_writer import CRMWriter
from .legacy_public_web_storage import list_legacy_target_public_web_promotions
from .person_asset_writer import PersonAssetWriter
from .person_identity import build_person_summary_view, resolve_person_identity_key, resolve_profile_url_key
from .storage import ControlPlaneStore

_LEGACY_FOLLOW_UP_STAGE_MAP = {
    "pending_outreach": "outreach_ready",
    "contacted_waiting": "contacted_waiting",
    "interview_completed": "interview_completed",
    "accepted": "accepted",
    "rejected": "rejected",
    "archived": "archived",
}


class CRMTargetCandidateMigrationBackfill:
    """Offline migration from legacy target_candidates into person-first CRM."""

    def __init__(
        self,
        store: ControlPlaneStore,
        *,
        crm_writer: CRMWriter | None = None,
        person_asset_writer: PersonAssetWriter | None = None,
        writer_id: str = "crm_target_candidate_migration_v1",
    ) -> None:
        self.store = store
        self.crm_writer = crm_writer or CRMWriter(store, writer_id=writer_id)
        self.person_asset_writer = person_asset_writer or PersonAssetWriter(store, writer_id=writer_id)
        self.writer_id = writer_id

    def backfill(
        self,
        *,
        workspace_id: str = "default",
        limit: int = 5_000,
        source_projection_id: str = "",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        records = self.store.list_target_candidates(
            source_projection_id=source_projection_id,
            limit=max(1, int(limit or 5_000)),
        )
        migrated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        assertions_created = 0
        for record in records:
            record_id = str(record.get("id") or record.get("record_id") or "").strip()
            person_key = _target_candidate_person_identity_key(record)
            if not person_key:
                skipped.append({"record_id": record_id, "reason": "missing_person_identity"})
                continue
            stage = _LEGACY_FOLLOW_UP_STAGE_MAP.get(
                str(record.get("follow_up_status") or "").strip(),
                "new",
            )
            summary = build_person_summary_view(
                {
                    "candidate_id": record.get("candidate_id"),
                    "display_name": record.get("candidate_name"),
                    "headline": record.get("headline"),
                    "current_company": record.get("current_company"),
                    "linkedin_url": record.get("linkedin_url"),
                    "avatar_url": record.get("avatar_url"),
                },
                person_identity_key=person_key,
                source_projection_id=str(record.get("source_projection_id") or ""),
                source_run_id=str(record.get("source_run_id") or record.get("job_id") or ""),
            )
            if dry_run:
                migrated.append({"record_id": record_id, "person_identity_key": person_key, "stage": stage})
                continue
            result = self.crm_writer.add_person_to_crm(
                person_identity_key=person_key,
                candidate_identity_key=str(record.get("candidate_identity_key") or person_key),
                collection_id=str(record.get("source_collection_id") or ""),
                source_projection_id=str(record.get("source_projection_id") or ""),
                source_run_id=str(record.get("source_run_id") or record.get("job_id") or ""),
                source_collection_id=str(record.get("source_collection_id") or ""),
                public_summary=summary,
                workspace_id=workspace_id,
                actor_type="migration",
                actor_id=self.writer_id,
                idempotency_key=f"crm:migrate-target:{workspace_id}:{record_id}",
                pipeline_id="default_sourcing",
                stage=stage,
                source_reason="legacy_target_candidate_migration",
            )
            assertion = self._migrate_primary_email_assertion(
                record,
                person_identity_key=person_key,
                source_crm_event_id=str(dict(result.get("crm_event") or {}).get("event_id") or ""),
                crm_record_id=str(dict(result.get("crm_record") or {}).get("crm_record_id") or ""),
                workspace_id=workspace_id,
            )
            if assertion:
                assertions_created += 1
            migrated.append(
                {
                    "record_id": record_id,
                    "person_identity_key": person_key,
                    "crm_record_id": str(dict(result.get("crm_record") or {}).get("crm_record_id") or ""),
                    "status": str(result.get("status") or ""),
                    "stage": stage,
                }
            )
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "workspace_id": str(workspace_id or "default").strip() or "default",
            "eligible_count": len(records),
            "migrated_count": len(migrated),
            "skipped_count": len(skipped),
            "assertions_created": assertions_created,
            "migrated": migrated[:100],
            "skipped": skipped[:100],
            "read_contract": {
                "source": "target_candidates",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def backfill_public_web_promotions(
        self,
        *,
        workspace_id: str = "default",
        record_id: str = "",
        limit: int = 5_000,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        promotions = list_legacy_target_public_web_promotions(
            self.store,
            record_id=str(record_id or "").strip(),
            limit=max(1, int(limit or 5_000)),
        )
        migrated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        assertions_created = 0
        events_created = 0
        for promotion in promotions:
            promotion_id = str(promotion.get("promotion_id") or "").strip()
            if str(promotion.get("action") or "").strip() != "promote":
                skipped.append({"promotion_id": promotion_id, "reason": "promotion_action_not_promote"})
                continue
            person_key = str(promotion.get("person_identity_key") or "").strip()
            if not person_key:
                skipped.append({"promotion_id": promotion_id, "reason": "missing_person_identity"})
                continue
            assertion_type = _public_web_promotion_assertion_type(promotion)
            assertion_value = str(promotion.get("new_value") or promotion.get("normalized_value") or promotion.get("value") or "").strip()
            if not assertion_type or not assertion_value:
                skipped.append({"promotion_id": promotion_id, "reason": "unsupported_or_empty_assertion"})
                continue
            if dry_run:
                migrated.append(
                    {
                        "promotion_id": promotion_id,
                        "person_identity_key": person_key,
                        "assertion_type": assertion_type,
                        "value": assertion_value,
                    }
                )
                continue
            crm_record = self.store.get_crm_record_by_person_identity(
                person_key,
                workspace_id=str(workspace_id or "default").strip() or "default",
            )
            assertion_id = f"pass_public_web_{_safe_assertion_token(promotion_id or person_key)}"
            assertion = self.person_asset_writer.record_assertion(
                {
                    "assertion_id": assertion_id,
                    "person_identity_key": person_key,
                    "assertion_type": assertion_type,
                    "value": assertion_value,
                    "normalized_value": assertion_value.lower() if assertion_type == "primary_email" else assertion_value,
                    "authority": "operator_confirmed",
                    "verification_status": "active",
                    "source_run_id": str(promotion.get("run_id") or ""),
                    "confidence_score": promotion.get("confidence_score"),
                    "metadata": {
                        "writer_id": self.writer_id,
                        "source": "legacy_target_candidate_public_web_promotion",
                        "legacy_public_web_promotion_id": promotion_id,
                        "legacy_target_candidate_record_id": str(promotion.get("record_id") or ""),
                        "signal_id": str(promotion.get("signal_id") or ""),
                        "source_url": str(promotion.get("source_url") or ""),
                        "source_domain": str(promotion.get("source_domain") or ""),
                        "source_family": str(promotion.get("source_family") or ""),
                        "promoted_field": str(promotion.get("promoted_field") or ""),
                        "export_policy": "default_human_promoted_assertion",
                    },
                }
            )
            assertions_created += 1
            crm_event = self.crm_writer.record_person_assertion_linked(
                workspace_id=workspace_id,
                crm_record_id=str(crm_record.get("crm_record_id") or ""),
                engagement_id=str(crm_record.get("current_engagement_id") or ""),
                person_identity_key=person_key,
                assertion_id=str(assertion.get("assertion_id") or ""),
                assertion_type=assertion_type,
                verification_status="active",
                actor_type="migration",
                actor_id=self.writer_id,
                idempotency_key=f"crm:migrate-public-web-promotion:{promotion_id}",
                source_run_id=str(promotion.get("run_id") or ""),
                reason="legacy_public_web_promotion_migration",
                payload={
                    "legacy_public_web_promotion_id": promotion_id,
                    "legacy_target_candidate_record_id": str(promotion.get("record_id") or ""),
                    "crm_record_found": bool(crm_record),
                },
            )
            if crm_event:
                events_created += 1
            migrated.append(
                {
                    "promotion_id": promotion_id,
                    "person_identity_key": person_key,
                    "assertion_id": str(assertion.get("assertion_id") or ""),
                    "assertion_type": assertion_type,
                    "crm_event_id": str(crm_event.get("event_id") or ""),
                    "crm_record_id": str(crm_record.get("crm_record_id") or ""),
                }
            )
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "workspace_id": str(workspace_id or "default").strip() or "default",
            "eligible_count": len(promotions),
            "migrated_count": len(migrated),
            "skipped_count": len(skipped),
            "assertions_created": assertions_created,
            "crm_events_created": events_created,
            "migrated": migrated[:100],
            "skipped": skipped[:100],
            "read_contract": {
                "source": "target_candidate_public_web_promotions",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def _migrate_primary_email_assertion(
        self,
        record: dict[str, Any],
        *,
        person_identity_key: str,
        source_crm_event_id: str,
        crm_record_id: str,
        workspace_id: str,
    ) -> dict[str, Any]:
        primary_email = str(record.get("primary_email") or "").strip()
        if not primary_email or "@" not in primary_email:
            return {}
        record_id = str(record.get("id") or record.get("record_id") or "").strip()
        assertion = self.person_asset_writer.record_assertion(
            {
                "assertion_id": f"pass_legacy_email_{_safe_assertion_token(record_id or person_identity_key)}",
                "person_identity_key": person_identity_key,
                "assertion_type": "primary_email",
                "value": primary_email,
                "normalized_value": primary_email.lower(),
                "authority": "legacy_migrated",
                "verification_status": "needs_review",
                "source_crm_event_id": source_crm_event_id,
                "source_run_id": str(record.get("source_run_id") or record.get("job_id") or ""),
                "metadata": {
                    "writer_id": self.writer_id,
                    "legacy_target_candidate_record_id": record_id,
                    "export_policy": "requires_review_before_default_contact_export",
                },
            }
        )
        self.store.append_crm_event(
            {
                "workspace_id": str(workspace_id or "default").strip() or "default",
                "crm_record_id": crm_record_id,
                "person_identity_key": person_identity_key,
                "event_type": "person_assertion_linked",
                "actor_type": "migration",
                "actor_id": self.writer_id,
                "idempotency_key": f"crm:migrate-target-email:{record_id}:{primary_email.lower()}",
                "payload": {
                    "assertion_id": assertion.get("assertion_id") or "",
                    "assertion_type": "primary_email",
                    "verification_status": "needs_review",
                },
                "metadata": {"writer_id": self.writer_id},
            }
        )
        return assertion


def _target_candidate_person_identity_key(record: dict[str, Any]) -> str:
    explicit = str(record.get("person_identity_key") or "").strip()
    if explicit:
        return explicit
    profile_key = resolve_profile_url_key(record.get("profile_url_key"), record.get("linkedin_url"))
    return resolve_person_identity_key(
        profile_url_key=profile_key,
        linkedin_url=str(record.get("linkedin_url") or ""),
        candidate_identity_key=str(record.get("candidate_identity_key") or ""),
        candidate_id=str(record.get("candidate_id") or ""),
    )


def _public_web_promotion_assertion_type(promotion: dict[str, Any]) -> str:
    promoted_field = str(promotion.get("promoted_field") or "").strip()
    signal_kind = str(promotion.get("signal_kind") or "").strip()
    signal_type = str(promotion.get("signal_type") or "").strip()
    if promoted_field == "primary_email" or signal_kind == "email_candidate":
        return "primary_email"
    mapping = {
        "personal_homepage": "homepage_url",
        "homepage": "homepage_url",
        "github_url": "github_url",
        "github": "github_url",
        "x_url": "x_url",
        "twitter_url": "x_url",
        "substack_url": "substack_url",
        "substack": "substack_url",
        "scholar_url": "scholar_url",
        "google_scholar_url": "scholar_url",
        "linkedin_url": "linkedin_url",
    }
    return mapping.get(signal_type, "profile_link_url" if signal_kind == "profile_link" else "")


def _safe_assertion_token(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in str(value or "").strip())[:96] or "unknown"
