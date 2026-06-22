"""Track B B4.2 — public-web domain table descriptors.

Declarative single-source-of-truth descriptors for the ten public-web control-plane tables. They
replace the inherited hand-written ``_*_public_web_*_from_row`` mappers (~250 lines of per-field
``str(_row_value(...) or "")`` / ``_loads_json_*`` / ``_coerce_public_web_float`` coercion). This batch
converts the READ path only: each ``ControlPlaneStore._*_from_row`` delegates to ``DESCRIPTOR.from_row``.
The write builders (``_*_row_payload`` + ``_normalize_*_payload``) keep their domain logic for now; the
write-path conversion is a follow-up that must reconcile the full column set against the table schema.

Mapper idioms encoded as descriptor features (all byte-equivalent to the storage helpers):
- ``str(x or "")``                        -> Kind.STR
- ``str(x or "queued")``                  -> Kind.STR + read_default="queued"
- ``int(x or 0)``                         -> Kind.INT
- ``_coerce_public_web_float(x)``         -> Kind.FLOAT
- ``bool(x)`` on a 0/1 column             -> Kind.BOOL_INT
- ``_loads_json_dict(x)``                 -> Kind.JSON        (-> {} on non-dict/parse-fail)
- ``_loads_json_list(x, default=[])``     -> Kind.JSON_LIST   (-> [] on non-list/parse-fail)
- two public keys from one column         -> a single Column + a ``derived`` read-alias

The ``*_json`` TEXT columns map to suffix-stripped public names via ``field=``; the B4.2 schema
migration flips them to ``jsonb`` by changing only the column ``Kind`` here.
"""

from __future__ import annotations

from ..control_plane_repository import Column, Kind, TableDescriptor

# Shorthands keep the 10 descriptors readable at a glance.
_STR = Kind.STR
_INT = Kind.INT
_FLOAT = Kind.FLOAT
_BOOL = Kind.BOOL_INT
_JSON = Kind.JSON
_JLIST = Kind.JSON_LIST


def _str_alias(source_key: str):
    return lambda mapped: mapped.get(source_key, "")


def _list_alias(source_key: str):
    # Mirror the mapper, which builds an independent list object for each alias.
    return lambda mapped: list(mapped.get(source_key, []))


TARGET_CANDIDATE_PUBLIC_WEB_BATCHES = TableDescriptor(
    table="target_candidate_public_web_batches",
    pk=("batch_id",),
    columns=(
        Column("batch_id"),
        Column("idempotency_key"),
        Column("status", read_default="queued"),
        Column("requested_record_ids_json", _JLIST, field="requested_record_ids"),
        Column("source_families_json", _JLIST, field="source_families"),
        Column("options_json", _JSON, field="options"),
        Column("run_ids_json", _JLIST, field="run_ids"),
        Column("summary_json", _JSON, field="summary"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("requested_by"),
        Column("force_refresh", _BOOL),
        Column("started_at"),
        Column("completed_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


TARGET_CANDIDATE_PUBLIC_WEB_RUNS = TableDescriptor(
    table="target_candidate_public_web_runs",
    pk=("run_id",),
    columns=(
        Column("run_id"),
        Column("batch_id"),
        Column("record_id"),
        Column("candidate_id"),
        Column("candidate_name"),
        Column("current_company"),
        Column("linkedin_url"),
        Column("linkedin_url_key"),
        Column("person_identity_key"),
        Column("idempotency_key"),
        Column("status", read_default="queued"),
        Column("phase", read_default="queued"),
        Column("source_families_json", _JLIST, field="source_families"),
        Column("options_json", _JSON, field="options"),
        Column("query_manifest_json", _JLIST, field="query_manifest"),
        Column("search_checkpoint_json", _JSON, field="search_checkpoint"),
        Column("fetch_checkpoint_json", _JSON, field="fetch_checkpoint"),
        Column("analysis_checkpoint_json", _JSON, field="analysis_checkpoint"),
        Column("summary_json", _JSON, field="summary"),
        Column("artifact_root"),
        Column("worker_key"),
        Column("lease_owner"),
        Column("lease_expires_at"),
        Column("attempt_count", _INT),
        Column("last_error"),
        Column("started_at"),
        Column("completed_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


CRM_PUBLIC_WEB_BATCHES = TableDescriptor(
    table="crm_public_web_batches",
    pk=("batch_id",),
    columns=(
        Column("batch_id"),
        Column("idempotency_key"),
        Column("workspace_id", read_default="default"),
        Column("status", read_default="queued"),
        Column("requested_crm_record_ids_json", _JLIST, field="requested_crm_record_ids"),
        Column("source_families_json", _JLIST, field="source_families"),
        Column("options_json", _JSON, field="options"),
        Column("run_ids_json", _JLIST, field="run_ids"),
        Column("summary_json", _JSON, field="summary"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("requested_by"),
        Column("force_refresh", _BOOL),
        Column("execution_backend", read_default="crm_public_web_v1"),
        Column("source_target_batch_id"),
        Column("started_at"),
        Column("completed_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
    # `requested_record_ids` is an alias of the same column the mapper reads twice.
    derived=(("requested_record_ids", _list_alias("requested_crm_record_ids")),),
)


CRM_PUBLIC_WEB_RUNS = TableDescriptor(
    table="crm_public_web_runs",
    pk=("run_id",),
    columns=(
        Column("run_id"),
        Column("batch_id"),
        Column("crm_record_id"),
        Column("workspace_id", read_default="default"),
        Column("candidate_id"),
        Column("candidate_name"),
        Column("current_company"),
        Column("linkedin_url"),
        Column("linkedin_url_key"),
        Column("person_identity_key"),
        Column("idempotency_key"),
        Column("status", read_default="queued"),
        Column("phase", read_default="queued"),
        Column("source_families_json", _JLIST, field="source_families"),
        Column("options_json", _JSON, field="options"),
        Column("query_manifest_json", _JLIST, field="query_manifest"),
        Column("search_checkpoint_json", _JSON, field="search_checkpoint"),
        Column("fetch_checkpoint_json", _JSON, field="fetch_checkpoint"),
        Column("analysis_checkpoint_json", _JSON, field="analysis_checkpoint"),
        Column("summary_json", _JSON, field="summary"),
        Column("artifact_root"),
        Column("worker_key"),
        Column("lease_owner"),
        Column("lease_expires_at"),
        Column("attempt_count", _INT),
        Column("last_error"),
        Column("execution_backend", read_default="crm_public_web_v1"),
        Column("source_target_run_id"),
        Column("started_at"),
        Column("completed_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
    # `record_id` is an alias of `crm_record_id` (mapper reads the column once, emits both keys).
    derived=(("record_id", _str_alias("crm_record_id")),),
)


PERSON_PUBLIC_WEB_ASSETS = TableDescriptor(
    table="person_public_web_assets",
    pk=("asset_id",),
    columns=(
        Column("asset_id"),
        Column("person_identity_key"),
        Column("linkedin_url_key"),
        Column("latest_run_id"),
        Column("target_candidate_record_id"),
        Column("candidate_name"),
        Column("current_company"),
        Column("status", read_default="completed"),
        Column("summary_json", _JSON, field="summary"),
        Column("signals_json", _JSON, field="signals"),
        Column("source_run_ids_json", _JLIST, field="source_run_ids"),
        Column("artifact_root"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


PERSON_PUBLIC_WEB_SIGNALS = TableDescriptor(
    table="person_public_web_signals",
    pk=("signal_id",),
    columns=(
        Column("signal_id"),
        Column("run_id"),
        Column("asset_id"),
        Column("person_identity_key"),
        Column("record_id"),
        Column("candidate_id"),
        Column("candidate_name"),
        Column("current_company"),
        Column("linkedin_url_key"),
        Column("signal_kind"),
        Column("signal_type"),
        Column("email_type"),
        Column("value"),
        Column("normalized_value"),
        Column("url"),
        Column("source_url"),
        Column("source_domain"),
        Column("source_family"),
        Column("source_title"),
        Column("confidence_label"),
        Column("confidence_score", _FLOAT),
        Column("identity_match_label"),
        Column("identity_match_score", _FLOAT),
        Column("publishable", _BOOL),
        Column("promotion_status"),
        Column("suppression_reason"),
        Column("evidence_excerpt"),
        Column("artifact_refs_json", _JSON, field="artifact_refs"),
        Column("model_provider"),
        Column("model_version"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


TARGET_CANDIDATE_PUBLIC_WEB_PROMOTIONS = TableDescriptor(
    table="target_candidate_public_web_promotions",
    pk=("promotion_id",),
    columns=(
        Column("promotion_id"),
        Column("signal_id"),
        Column("run_id"),
        Column("asset_id"),
        Column("person_identity_key"),
        Column("record_id"),
        Column("candidate_id"),
        Column("candidate_name"),
        Column("current_company"),
        Column("linkedin_url_key"),
        Column("signal_kind"),
        Column("signal_type"),
        Column("email_type"),
        Column("value"),
        Column("normalized_value"),
        Column("url"),
        Column("source_url"),
        Column("source_domain"),
        Column("source_family"),
        Column("source_title"),
        Column("confidence_label"),
        Column("confidence_score", _FLOAT),
        Column("identity_match_label"),
        Column("identity_match_score", _FLOAT),
        Column("publishable", _BOOL),
        Column("clean_profile_link", _BOOL),
        Column("link_shape_warnings_json", _JLIST, field="link_shape_warnings"),
        Column("action"),
        Column("promotion_status"),
        Column("promoted_field"),
        Column("previous_value"),
        Column("new_value"),
        Column("operator"),
        Column("note"),
        Column("evidence_excerpt"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


CRM_PUBLIC_WEB_PROMOTIONS = TableDescriptor(
    table="crm_public_web_promotions",
    pk=("promotion_id",),
    columns=(
        Column("promotion_id"),
        Column("signal_id"),
        Column("run_id"),
        Column("asset_id"),
        Column("person_identity_key"),
        Column("crm_record_id"),
        Column("workspace_id", read_default="default"),
        Column("candidate_id"),
        Column("candidate_name"),
        Column("current_company"),
        Column("linkedin_url_key"),
        Column("signal_kind"),
        Column("signal_type"),
        Column("email_type"),
        Column("value"),
        Column("normalized_value"),
        Column("url"),
        Column("source_url"),
        Column("source_domain"),
        Column("source_family"),
        Column("source_title"),
        Column("confidence_label"),
        Column("confidence_score", _FLOAT),
        Column("identity_match_label"),
        Column("identity_match_score", _FLOAT),
        Column("publishable", _BOOL),
        Column("clean_profile_link", _BOOL),
        Column("link_shape_warnings_json", _JLIST, field="link_shape_warnings"),
        Column("action"),
        Column("promotion_status"),
        Column("promoted_field"),
        Column("previous_value"),
        Column("new_value"),
        Column("operator"),
        Column("note"),
        Column("evidence_excerpt"),
        Column("execution_backend", read_default="crm_public_web_v1"),
        Column("source_target_promotion_id"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
    # `record_id` is an alias of `crm_record_id`.
    derived=(("record_id", _str_alias("crm_record_id")),),
)


COMPANY_PUBLIC_WEB_ASSET_RUNS = TableDescriptor(
    table="company_public_web_asset_runs",
    pk=("run_id",),
    columns=(
        Column("run_id"),
        Column("target_company"),
        Column("company_key"),
        Column("idempotency_key"),
        Column("status", read_default="queued"),
        Column("phase", read_default="queued"),
        Column("source_families_json", _JLIST, field="source_families"),
        Column("seed_urls_json", _JLIST, field="seed_urls"),
        Column("options_json", _JSON, field="options"),
        Column("discovered_assets_json", _JLIST, field="discovered_assets"),
        Column("summary_json", _JSON, field="summary"),
        Column("artifact_root"),
        Column("requested_by"),
        Column("force_refresh", _BOOL),
        Column("started_at"),
        Column("completed_at"),
        Column("last_error"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


COMPANY_PUBLIC_WEB_ASSETS = TableDescriptor(
    table="company_public_web_assets",
    pk=("asset_id",),
    columns=(
        Column("asset_id"),
        Column("company_key"),
        Column("target_company"),
        Column("latest_run_id"),
        Column("source_family"),
        Column("asset_kind", read_default="company_public_web_asset"),
        Column("title"),
        Column("url"),
        Column("normalized_url_key"),
        Column("summary"),  # NOTE: plain text column here (not JSON, unlike the run/asset summaries)
        Column("model_safe_payload_json", _JSON, field="model_safe_payload"),
        Column("source_run_ids_json", _JLIST, field="source_run_ids"),
        Column("artifact_refs_json", _JSON, field="artifact_refs"),
        Column("status", read_default="active"),
        Column("metadata_json", _JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


# Registry of the read-path descriptors keyed by the ControlPlaneStore mapper method they replace.
PUBLIC_WEB_FROM_ROW_DESCRIPTORS = {
    "_target_candidate_public_web_batch_from_row": TARGET_CANDIDATE_PUBLIC_WEB_BATCHES,
    "_target_candidate_public_web_run_from_row": TARGET_CANDIDATE_PUBLIC_WEB_RUNS,
    "_crm_public_web_batch_from_row": CRM_PUBLIC_WEB_BATCHES,
    "_crm_public_web_run_from_row": CRM_PUBLIC_WEB_RUNS,
    "_person_public_web_asset_from_row": PERSON_PUBLIC_WEB_ASSETS,
    "_person_public_web_signal_from_row": PERSON_PUBLIC_WEB_SIGNALS,
    "_target_candidate_public_web_promotion_from_row": TARGET_CANDIDATE_PUBLIC_WEB_PROMOTIONS,
    "_crm_public_web_promotion_from_row": CRM_PUBLIC_WEB_PROMOTIONS,
    "_company_public_web_asset_run_from_row": COMPANY_PUBLIC_WEB_ASSET_RUNS,
    "_company_public_web_asset_from_row": COMPANY_PUBLIC_WEB_ASSETS,
}
