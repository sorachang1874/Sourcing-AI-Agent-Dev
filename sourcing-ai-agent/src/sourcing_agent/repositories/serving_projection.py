"""Track B B4.2 — serving-projection control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.
"""

from __future__ import annotations

from ..control_plane_repository import Column, Kind, TableDescriptor

SERVING_PROJECTIONS = TableDescriptor(
    table="serving_projections",
    pk=("projection_id",),
    columns=(
        Column("projection_id"),
        Column("projection_type"),
        Column("collection_id"),
        Column("source_run_id"),
        Column("projection_version", read_default="serving_projection_v1"),
        Column("state"),
        Column("scope_label"),
        Column("scope_spec_json", Kind.JSON, field="scope_spec"),
        Column("candidate_identity_manifest_ref"),
        Column("source_collection_version"),
        Column("raw_profile_index_watermark"),
        Column("evidence_index_watermark"),
        Column("counts_json", Kind.JSON, field="counts"),
        Column("readiness_json", Kind.JSON, field="readiness"),
        Column("provenance_json", Kind.JSON, field="provenance"),
        Column("manual_overlay_version"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("published_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


PROJECTION_PERSON_SEARCH_INDEX = TableDescriptor(
    table="projection_person_search_index",
    pk=("projection_id", "candidate_identity_key"),
    columns=(
        Column("projection_id"),
        Column("candidate_identity_key"),
        Column("person_identity_key"),
        Column("indexed_text"),
        Column("raw_profile_terms_json", Kind.JSON_LIST, field="raw_profile_terms"),
        Column("evidence_terms_json", Kind.JSON_LIST, field="evidence_terms"),
        Column("assertion_terms_json", Kind.JSON_LIST, field="assertion_terms"),
        Column("indexed_field_sources_json", Kind.JSON, field="indexed_field_sources"),
        Column("raw_profile_index_watermark"),
        Column("evidence_index_watermark"),
        Column("count_scope"),
        Column("profile_fetched_at"),
        Column("profile_indexed_at"),
        Column("evidence_indexed_at"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


RUN_PROJECTION_LINKS = TableDescriptor(
    table="run_projection_links",
    pk=("run_id", "link_type"),
    columns=(
        Column("run_id"),
        Column("projection_id"),
        Column("link_type"),
        Column("projection_type"),
        Column("collection_id"),
        Column("state"),
        Column("created_by"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


COLLECTION_AUTHORITATIVE_POINTERS = TableDescriptor(
    table="collection_authoritative_pointers",
    pk=("collection_id",),
    columns=(
        Column("collection_id"),
        Column("active_projection_id"),
        Column("active_collection_version"),
        Column("previous_projection_id"),
        Column("state"),
        Column("writer_id"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("published_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


# Read-path descriptors keyed by the ControlPlaneStore mapper method they replace.
FROM_ROW_DESCRIPTORS = {
    "_serving_projection_from_row": SERVING_PROJECTIONS,
    "_projection_person_search_index_from_row": PROJECTION_PERSON_SEARCH_INDEX,
    "_run_projection_link_from_row": RUN_PROJECTION_LINKS,
    "_collection_authoritative_pointer_from_row": COLLECTION_AUTHORITATIVE_POINTERS,
}
