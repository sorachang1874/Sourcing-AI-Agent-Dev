"""Track B B4.2 — person / company asset + evidence + assertion control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.
"""

from __future__ import annotations

from ..control_plane_repository import Column, Kind, TableDescriptor

PERSON_ASSETS = TableDescriptor(
    table="person_assets",
    pk=("asset_id",),
    columns=(
        Column("asset_id"),
        Column("person_identity_key"),
        Column("asset_type"),
        Column("source_kind"),
        Column("source_run_id"),
        Column("source_projection_id"),
        Column("content_ref"),
        Column("content_hash"),
        Column("source_url"),
        Column("fetched_at"),
        Column("visibility_scope", read_default="internal"),
        Column("status", read_default="available"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


PERSON_EVIDENCE = TableDescriptor(
    table="person_evidence",
    pk=("evidence_id",),
    columns=(
        Column("evidence_id"),
        Column("person_identity_key"),
        Column("asset_id"),
        Column("evidence_type"),
        Column("value"),
        Column("normalized_value"),
        Column("source_url"),
        Column("source_domain"),
        Column("confidence_score", Kind.FLOAT),
        Column("identity_match_score", Kind.FLOAT),
        Column("publishable", Kind.BOOL_INT),
        Column("evidence_excerpt"),
        Column("artifact_refs_json", Kind.JSON, field="artifact_refs"),
        Column("status", read_default="observed"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


PERSON_ASSERTIONS = TableDescriptor(
    table="person_assertions",
    pk=("assertion_id",),
    columns=(
        Column("assertion_id"),
        Column("person_identity_key"),
        Column("assertion_type"),
        Column("value"),
        Column("normalized_value"),
        Column("authority", read_default="provider_observed"),
        Column("verification_status", read_default="needs_review"),
        Column("source_evidence_id"),
        Column("source_crm_event_id"),
        Column("source_run_id"),
        Column("confidence_score", Kind.FLOAT),
        Column("valid_from"),
        Column("valid_to"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


COMPANY_ASSETS = TableDescriptor(
    table="company_assets",
    pk=("asset_id",),
    columns=(
        Column("asset_id"),
        Column("workspace_id", read_default="default"),
        Column("company_key"),
        Column("target_company"),
        Column("asset_type"),
        Column("source_kind"),
        Column("source_run_id"),
        Column("source_command_id"),
        Column("activity_run_id"),
        Column("content_ref"),
        Column("content_hash"),
        Column("source_url"),
        Column("fetched_at"),
        Column("visibility_scope", read_default="internal"),
        Column("status", read_default="available"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


COMPANY_EVIDENCE = TableDescriptor(
    table="company_evidence",
    pk=("evidence_id",),
    columns=(
        Column("evidence_id"),
        Column("workspace_id", read_default="default"),
        Column("company_key"),
        Column("target_company"),
        Column("asset_id"),
        Column("evidence_type"),
        Column("value"),
        Column("normalized_value"),
        Column("source_url"),
        Column("source_domain"),
        Column("confidence_score", Kind.FLOAT),
        Column("evidence_excerpt"),
        Column("artifact_refs_json", Kind.JSON, field="artifact_refs"),
        Column("status", read_default="observed"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


COMPANY_ASSERTIONS = TableDescriptor(
    table="company_assertions",
    pk=("assertion_id",),
    columns=(
        Column("assertion_id"),
        Column("workspace_id", read_default="default"),
        Column("company_key"),
        Column("target_company"),
        Column("assertion_type"),
        Column("value"),
        Column("normalized_value"),
        Column("authority", read_default="provider_observed"),
        Column("verification_status", read_default="needs_review"),
        Column("source_evidence_id"),
        Column("source_run_id"),
        Column("source_command_id"),
        Column("confidence_score", Kind.FLOAT),
        Column("valid_from"),
        Column("valid_to"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


RAW_PROFILE_INDEX = TableDescriptor(
    table="raw_profile_index",
    pk=("person_identity_key",),
    columns=(
        Column("person_identity_key"),
        Column("indexed_text"),
        Column("raw_profile_terms_json", Kind.JSON_LIST, field="raw_profile_terms"),
        Column("source_asset_ids_json", Kind.JSON_LIST, field="source_asset_ids"),
        Column("indexed_field_sources_json", Kind.JSON, field="indexed_field_sources"),
        Column("raw_profile_index_watermark"),
        Column("profile_fetched_at"),
        Column("profile_indexed_at"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


CANDIDATE_EVIDENCE_INDEX = TableDescriptor(
    table="candidate_evidence_index",
    pk=("person_identity_key",),
    columns=(
        Column("person_identity_key"),
        Column("indexed_text"),
        Column("evidence_terms_json", Kind.JSON_LIST, field="evidence_terms"),
        Column("assertion_terms_json", Kind.JSON_LIST, field="assertion_terms"),
        Column("source_evidence_ids_json", Kind.JSON_LIST, field="source_evidence_ids"),
        Column("source_assertion_ids_json", Kind.JSON_LIST, field="source_assertion_ids"),
        Column("indexed_field_sources_json", Kind.JSON, field="indexed_field_sources"),
        Column("evidence_index_watermark"),
        Column("evidence_indexed_at"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


# Read-path descriptors keyed by the ControlPlaneStore mapper method they replace.
FROM_ROW_DESCRIPTORS = {
    "_person_asset_from_row": PERSON_ASSETS,
    "_person_evidence_from_row": PERSON_EVIDENCE,
    "_person_assertion_from_row": PERSON_ASSERTIONS,
    "_company_asset_from_row": COMPANY_ASSETS,
    "_company_evidence_from_row": COMPANY_EVIDENCE,
    "_company_assertion_from_row": COMPANY_ASSERTIONS,
    "_raw_profile_index_from_row": RAW_PROFILE_INDEX,
    "_candidate_evidence_index_from_row": CANDIDATE_EVIDENCE_INDEX,
}
