from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "pro_consultation_contract.py"


def _load_contract():
    spec = importlib.util.spec_from_file_location("pro_consultation_contract_tests", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT.parent,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _valid_bundle(contract, root: Path, *, branch_lookup: str = "verified") -> dict[str, object]:
    artifact_path = "sourcing-ai-agent/docs/pro-consults/test/response.md"
    sha = _git("rev-parse", "HEAD")
    repository = contract._local_origin_repository()
    assert repository is not None
    branch = _git("branch", "--show-current")
    dirty_paths = sorted(
        line[3:] for line in _git("status", "--porcelain=v1", "--untracked-files=all").splitlines() if len(line) >= 4
    )
    local_state = "dirty" if dirty_paths else "clean"
    connector_scope = {
        "mode": "files",
        "repository": repository,
        "commit_sha": sha,
        "required_files": ["/AGENTS.md"],
    }
    request = (
        "# Request\n\n"
        "Purpose: approach_review\n"
        "Secondary question sets: none\n"
        "Authority: ADVISORY_ONLY\n"
        "Surface required: Chat\n"
        "Model required: GPT-5.6 Sol\n"
        "Mode required: Pro\n"
        "Browser required: Codex in-app browser\n"
        f"Local state: {local_state}\n"
        "Connector sees dirty scope: false\n"
        "Dirty scope provided to Pro: false\n"
        f"Branch: {branch}\n"
        "Branch requirement: provenance_only\n"
        f"Repository: {repository}\n"
        f"Commit authority: {sha}\n"
        "Consultation status: planned\n"
        "Connector status: attached_pending\n"
        "Redaction status: verified\n\n"
        "ADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        f"{contract.CONNECTOR_SCOPE_PREFIX} {contract._canonical_json(connector_scope)}\n\n"
        "Read /AGENTS.md at the pinned commit.\n"
    )
    response = (
        f"BEGIN_ARTIFACT path={artifact_path}\n"
        "ADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "# Verdict\n"
        "Raw Pro verdict: keep\n\n"
        "# Scope understood\n"
        "Reviewed the pinned file.\n"
        f"Connector citation: https://github.com/{repository}/blob/{sha}/AGENTS.md\n\n"
        "# Assumptions and missing information\nNone.\n\n"
        "# Findings\n"
        "## P0\nNone.\n\n"
        "## P1\nNone.\n\n"
        "## P2\nNone.\n\n"
        "# Recommended sequence\nKeep the approach.\n\n"
        "# Validation and failure modes\nRun targeted tests.\n\n"
        "# Deferred decisions\nNone.\n\n"
        "# Owner decisions required\nNone.\n"
        "END_ARTIFACT\n"
    )
    decision = (
        "# Decision\n\n"
        "## Authority\n\n"
        "ADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "## Local disposition\n\n"
        "P0 disposition: none — no P0 finding.\n"
        "P1 disposition: none — no P1 finding.\n"
        "P2 disposition: none — no P2 finding.\n"
        "Validation: targeted contract tests passed.\n\n"
        "## Follow-up\n\nRun repository validation and the independent review gate separately.\n"
    )
    excluded_categories = sorted(contract.REQUIRED_EXCLUDED_CATEGORIES)
    redaction = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=excluded_categories,
    )
    metadata: dict[str, object] = {
        "contract_schema_version": contract.CONTRACT_SCHEMA_VERSION,
        "purpose": "approach_review",
        "secondary_question_sets": [],
        "advisory_only": True,
        "formal_gate_eligible": False,
        "formal_review_status": "not_run",
        "consultation_status": "complete_validated",
        "request_status": "planned",
        "request_connector_status": "attached_pending",
        "consultation_valid": True,
        "usable_for_advisory_decision": True,
        "expected_response_path": artifact_path,
        "ui_state_verification": "verified",
        "required_surface": "Chat",
        "required_model": "GPT-5.6 Sol",
        "required_mode": "Pro",
        "required_browser": "Codex in-app browser",
        "observed_surface": "Chat",
        "observed_model": "GPT-5.6 Sol",
        "observed_mode": "Pro",
        "browser": "Codex in-app browser",
        "response_complete": True,
        "response_marker_envelope_preserved": True,
        "response_schema_valid": True,
        "raw_verdict": "keep",
        "normalized_recommendation": "keep",
        "request_sha256": _sha256_text(request),
        "response_sha256": _sha256_text(response),
        "decision_sha256": _sha256_text(decision),
        "validator_sha256": hashlib.sha256(SCRIPT_PATH.read_bytes()).hexdigest(),
        "workflow_sha256": hashlib.sha256(
            (REPO_ROOT / "docs/CHATGPT_PRO_CONSULTATION_WORKFLOW.md").read_bytes()
        ).hexdigest(),
        "redaction_preflight": redaction,
        "redaction_status": "verified",
        "transfer_content_preflight": contract.scan_transfer_content(connector_scope),
        "retention_safe": True,
        "connector_sees_dirty_scope": False,
        "dirty_scope_provided_to_pro": False,
        "dirty_scope_transfer": None,
        "dirty_diff_sha256": None,
        "local_state": local_state,
        "local_dirty_inventory_status": "recorded_not_transferred" if dirty_paths else "clean",
        "local_dirty_paths": dirty_paths,
        "local_dirty_path_count": len(dirty_paths),
        "local_dirty_inventory_sha256": _sha256_text("\n".join(dirty_paths)),
        "connector_status": "commit_pinned",
        "connector_evidence_valid": True,
        "outcome_code": "connector_ok_commit_pinned",
        "connector_scope_manifest": connector_scope,
        "connector_scope_manifest_sha256": _sha256_text(contract._canonical_json(connector_scope)),
        "repository": repository,
        "requested_commit_sha": sha,
        "observed_commit_sha": sha,
        "commit_verification": "verified",
        "connector_file_proof": [
            {
                "path": "/AGENTS.md",
                "observed_commit_sha": sha,
                "retrieval_status": "complete",
                "citation": f"https://github.com/{repository}/blob/{sha}/AGENTS.md",
            }
        ],
        "branch": branch,
        "branch_requirement": "provenance_only",
        "branch_lookup_outcome": branch_lookup,
        "branch_verification": {
            "verified": "verified",
            "not_found": "not_found",
            "lookup_unsupported": "lookup_unsupported",
            "resolved_other_sha": "mismatch",
            "not_requested": "not_applicable",
        }[branch_lookup],
        "branch_resolved_sha": (
            sha
            if branch_lookup == "verified"
            else _git("rev-parse", "HEAD^")
            if branch_lookup == "resolved_other_sha"
            else None
        ),
    }
    root.mkdir(parents=True)
    (root / "request.md").write_text(request, encoding="utf-8")
    (root / "response.md").write_text(response, encoding="utf-8")
    (root / "decision.md").write_text(decision, encoding="utf-8")
    (root / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return metadata


def test_redaction_preflight_is_allowlist_first_and_never_returns_secret_values() -> None:
    contract = _load_contract()
    safe = contract.scan_redacted_request(
        "Review /AGENTS.md in this commit-pinned contract.",
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    assert safe["status"] == "passed"
    assert safe["secret_match_count"] == 0

    token = "sk-" + "proj-" + "abcdefghijklmnopqrstuvwxyz123456"
    blocked = contract.scan_redacted_request(
        f"Review /AGENTS.md. credential={token}",
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    assert blocked["status"] == "blocked"
    assert blocked["secret_match_count"] == 1
    assert token not in json.dumps(blocked)

    denied_path = contract.scan_redacted_request(
        "Do not read /Users/example/.env.local.",
        included_paths=["/Users/example/.env.local"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    assert denied_path["allowlist_status"] == "blocked"

    unreferenced = contract.scan_redacted_request(
        "Review /AGENTS.md only.",
        included_paths=["/definitely/not/in/request.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    assert unreferenced["allowlist_status"] == "blocked"


def test_valid_commit_pinned_bundle_computes_one_valid_result(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "valid"
    _valid_bundle(contract, bundle)
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is True
    assert result["storage_valid"] is True
    assert result["errors"] == []
    assert result["consistency_errors"] == []


def test_valid_diff_scope_requires_base_head_bound_compare_proof(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "diff"
    metadata = _valid_bundle(contract, bundle)
    old_scope = metadata["connector_scope_manifest"]
    assert isinstance(old_scope, dict)
    base_sha = _git("rev-parse", "HEAD^")
    head_sha = str(metadata["requested_commit_sha"])
    changed_files = sorted(f"/{path}" for path in _git("diff", "--name-only", base_sha, head_sha).splitlines() if path)
    new_scope = {
        "mode": "diff",
        "repository": str(metadata["repository"]),
        "base_sha": base_sha,
        "head_sha": head_sha,
        "changed_files": changed_files,
    }
    request = (bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace(contract._canonical_json(old_scope), contract._canonical_json(new_scope))
    request = request.replace("Read /AGENTS.md at the pinned commit.", f"Compare {base_sha}..{head_sha}.")
    (bundle / "request.md").write_text(request, encoding="utf-8")
    response = (
        (bundle / "response.md")
        .read_text(encoding="utf-8")
        .replace(
            f"https://github.com/{metadata['repository']}/blob/{head_sha}/AGENTS.md",
            f"https://github.com/{metadata['repository']}/compare/{base_sha}...{head_sha}",
        )
    )
    (bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["connector_scope_manifest"] = new_scope
    metadata["connector_scope_manifest_sha256"] = _sha256_text(contract._canonical_json(new_scope))
    metadata["base_commit_sha"] = base_sha
    metadata["connector_file_proof"] = []
    metadata["connector_diff_proof"] = {
        "base_sha": base_sha,
        "head_sha": head_sha,
        "retrieval_status": "complete",
        "citation": f"https://github.com/{metadata['repository']}/compare/{base_sha}...{head_sha}",
    }
    metadata["request_sha256"] = _sha256_text(request)
    metadata["response_sha256"] = _sha256_text(response)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=contract._scope_allowlist_entries(new_scope),
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    metadata["transfer_content_preflight"] = contract.scan_transfer_content(new_scope)
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is True, result
    assert result["storage_valid"] is True


def test_provenance_only_branch_outcomes_remain_distinct_and_non_blocking(tmp_path: Path) -> None:
    contract = _load_contract()
    for branch_lookup in ("not_found", "lookup_unsupported", "resolved_other_sha", "not_requested"):
        bundle = tmp_path / branch_lookup
        _valid_bundle(contract, bundle, branch_lookup=branch_lookup)
        result = contract.validate_bundle(bundle)
        assert result["consultation_valid"] is True, (branch_lookup, result)


def test_required_branch_fails_closed_without_verified_resolution(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "required"
    metadata = _valid_bundle(contract, bundle, branch_lookup="not_found")
    metadata["branch_requirement"] = "required"
    metadata["consultation_status"] = "incomplete"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "decision.md").write_text(
        "# Decision\n\nADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "This is not Connector-grounded decision evidence.\n",
        encoding="utf-8",
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["storage_valid"] is True
    assert "required branch was not verified" in result["errors"]


def test_uncited_connector_claim_must_be_honestly_classified_legacy(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "legacy"
    metadata = _valid_bundle(contract, bundle)
    metadata = copy.deepcopy(metadata)
    metadata["connector_file_proof"][0]["citation"] = None  # type: ignore[index]
    metadata["consultation_status"] = "legacy_advisory_unverified"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "decision.md").write_text(
        "# Decision\n\nADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "This is not Connector-grounded decision evidence.\n",
        encoding="utf-8",
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["storage_valid"] is True
    assert "required Connector file proof is incomplete or uncited" in result["errors"]
    strict = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "validate-bundle", str(bundle)], check=False, capture_output=True, text=True
    )
    storage_only = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "validate-bundle", str(bundle), "--allow-invalid-storage"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert strict.returncode == 1
    assert storage_only.returncode == 0


def test_invalid_bundle_cannot_claim_complete_or_usable(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "contradictory"
    metadata = _valid_bundle(contract, bundle)
    metadata["connector_file_proof"][0]["citation"] = None  # type: ignore[index]
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["storage_valid"] is False
    assert "computed validity disagrees with metadata" in result["consistency_errors"]


def test_formal_gate_fields_are_computed_fail_closed(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "formal-drift"
    metadata = _valid_bundle(contract, bundle)
    metadata["advisory_only"] = False
    metadata["formal_gate_eligible"] = True
    metadata["formal_review_status"] = "GO"
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "Pro consultation metadata must remain advisory-only and formal-gate ineligible" in result["errors"]


def test_connector_proofs_must_exactly_match_machine_readable_request_scope(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "scope-drift"
    metadata = _valid_bundle(contract, bundle)
    old_scope = metadata["connector_scope_manifest"]
    assert isinstance(old_scope, dict)
    new_scope = {**old_scope, "required_files": ["/AGENTS.md", "/SECOND.md"]}
    request = (bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace(contract._canonical_json(old_scope), contract._canonical_json(new_scope))
    request += "Read /SECOND.md too.\n"
    (bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["connector_scope_manifest"] = new_scope
    metadata["connector_scope_manifest_sha256"] = _sha256_text(contract._canonical_json(new_scope))
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md", "/SECOND.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    metadata["consultation_status"] = "legacy_advisory_unverified"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "decision.md").write_text(
        "# Decision\n\nADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "This is not Connector-grounded decision evidence.\n",
        encoding="utf-8",
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["storage_valid"] is True
    assert "Connector file proofs do not exactly match the required-file manifest" in result["errors"]


def test_blocked_connector_cannot_validate_a_connector_required_request(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "blocked-connector"
    metadata = _valid_bundle(contract, bundle)
    metadata["connector_status"] = "blocked"
    metadata["connector_evidence_valid"] = False
    metadata["connector_file_proof"] = []
    metadata["consultation_status"] = "blocked_connector"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "decision.md").write_text(
        "# Decision\n\nADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "This is not Connector-grounded decision evidence.\n",
        encoding="utf-8",
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "Connector-required scope must have validated commit-pinned evidence" in result["errors"]
    assert "blocked Connector capture must not retain a grounded verdict or proof" in result["errors"]


def test_honest_blocked_connector_capture_has_no_verdict_and_remains_safe_to_store(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "honest-blocked"
    metadata = _valid_bundle(contract, bundle)
    response = (
        f"BEGIN_ARTIFACT path={metadata['expected_response_path']}\n"
        "ADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "CONNECTOR_BLOCKED — the requested commit was unavailable.\n"
        "END_ARTIFACT\n"
    )
    decision = (
        "# Decision\n\n"
        "## Authority\n\n"
        "ADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "## Local disposition\n\nThis is not Connector-grounded decision evidence.\n\n"
        "## Follow-up\n\nRetry only after the immutable commit is accessible.\n"
    )
    (bundle / "response.md").write_text(response, encoding="utf-8")
    (bundle / "decision.md").write_text(decision, encoding="utf-8")
    metadata.update(
        {
            "connector_status": "blocked",
            "connector_evidence_valid": False,
            "outcome_code": "connector_blocked",
            "connector_file_proof": [],
            "consultation_status": "blocked_connector",
            "consultation_valid": False,
            "usable_for_advisory_decision": False,
            "response_schema_valid": False,
            "raw_verdict": None,
            "normalized_recommendation": None,
            "response_sha256": _sha256_text(response),
            "decision_sha256": _sha256_text(decision),
        }
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["storage_valid"] is True, result
    assert "blocked Connector capture must not retain a grounded verdict or proof" not in result["errors"]


def test_redaction_metadata_is_recomputed_from_request_and_scope(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "forged-redaction"
    metadata = _valid_bundle(contract, bundle)
    forged = copy.deepcopy(metadata["redaction_preflight"])
    assert isinstance(forged, dict)
    forged["included_paths"] = ["/definitely/not/in/request.md"]
    forged["included_paths_sha256"] = _sha256_text("/definitely/not/in/request.md")
    forged["included_path_count"] = 1
    forged["allowlist_status"] = "passed"
    forged["status"] = "passed"
    metadata["redaction_preflight"] = forged
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "redaction_preflight does not match a fresh scan of the persisted request" in result["errors"]


def test_response_schema_is_derived_from_required_sections(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "response-shape"
    metadata = _valid_bundle(contract, bundle)
    response = (
        (bundle / "response.md")
        .read_text(encoding="utf-8")
        .replace("# Owner decisions required", "# Missing owner section")
    )
    (bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    metadata["consultation_status"] = "legacy_advisory_unverified"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "decision.md").write_text(
        "# Decision\n\nADVISORY_ONLY — not an independent-review artifact or formal GO.\n\n"
        "This is not Connector-grounded decision evidence.\n",
        encoding="utf-8",
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "required response heading missing or duplicated: # Owner decisions required" in result["errors"]


def test_json_secret_and_credential_carrier_paths_fail_redaction_without_echoing_values() -> None:
    contract = _load_contract()
    token = "secret-value-" + "x" * 24
    request = f'Review /AGENTS.md with {{"api_key":"{token}"}}.'
    blocked = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    assert blocked["status"] == "blocked"
    assert blocked["secret_match_count"] == 1
    assert token not in json.dumps(blocked)

    for carrier in ("/.npmrc", "/.netrc", "/token.json", "/oauth_token.json"):
        result = contract.scan_redacted_request(
            f"Review {carrier}.",
            included_paths=[carrier],
            excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
        )
        assert result["allowlist_status"] == "blocked", carrier


def test_secret_in_persisted_bundle_is_never_safe_to_retain(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "unsafe-retention"
    metadata = _valid_bundle(contract, bundle)
    token = "sk-" + "proj-" + "z" * 28
    response = (
        (bundle / "response.md")
        .read_text(encoding="utf-8")
        .replace("# Deferred decisions", f"Leaked credential: {token}\n\n# Deferred decisions")
    )
    (bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    metadata["retention_safe"] = False
    metadata["consultation_status"] = "incomplete"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["retention_safe"] is False
    assert result["storage_valid"] is False
    storage_only = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "validate-bundle", str(bundle), "--allow-invalid-storage"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert storage_only.returncode == 1


def test_response_parser_rejects_extra_verdicts_and_fenced_schema(tmp_path: Path) -> None:
    contract = _load_contract()
    extra_bundle = tmp_path / "extra-verdict"
    metadata = _valid_bundle(contract, extra_bundle)
    response = (
        (extra_bundle / "response.md")
        .read_text(encoding="utf-8")
        .replace("END_ARTIFACT", "Raw Pro verdict: GO\nEND_ARTIFACT")
    )
    (extra_bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    (extra_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    assert contract.validate_bundle(extra_bundle)["consultation_valid"] is False

    fenced_bundle = tmp_path / "fenced-schema"
    metadata = _valid_bundle(contract, fenced_bundle)
    response = (fenced_bundle / "response.md").read_text(encoding="utf-8")
    lines = response.splitlines()
    response = "\n".join([lines[0], "```markdown", *lines[1:-1], "```", lines[-1]]) + "\n"
    (fenced_bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    (fenced_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(fenced_bundle)
    assert result["consultation_valid"] is False
    assert "response must contain exactly one unfenced exact Raw Pro verdict" in result["errors"]


def test_required_ui_and_dirty_scope_metadata_cannot_drift(tmp_path: Path) -> None:
    contract = _load_contract()
    for field, value in (
        ("required_surface", "Work"),
        ("required_model", "Other"),
        ("required_mode", "Ultra"),
        ("required_browser", "Chrome"),
        ("browser", "Chrome"),
        ("connector_sees_dirty_scope", True),
        ("dirty_scope_provided_to_pro", True),
    ):
        bundle = tmp_path / field
        metadata = _valid_bundle(contract, bundle)
        metadata[field] = value
        (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        assert contract.validate_bundle(bundle)["consultation_valid"] is False, field


def test_connector_scope_requires_branch_classification_and_exact_diff_inventory(tmp_path: Path) -> None:
    contract = _load_contract()
    branch_bundle = tmp_path / "branch-not-applicable"
    metadata = _valid_bundle(contract, branch_bundle)
    metadata["branch_requirement"] = "not_applicable"
    (branch_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(branch_bundle)
    assert result["consultation_valid"] is False
    assert "Connector-backed scope must explicitly classify branch evidence" in result["errors"]

    diff_bundle = tmp_path / "denied-diff-inventory"
    metadata = _valid_bundle(contract, diff_bundle)
    old_scope = metadata["connector_scope_manifest"]
    assert isinstance(old_scope, dict)
    base_sha = _git("rev-parse", "HEAD^")
    head_sha = _git("rev-parse", "HEAD")
    new_scope = {
        "mode": "diff",
        "repository": str(metadata["repository"]),
        "base_sha": base_sha,
        "head_sha": head_sha,
        "changed_files": ["/.env"],
    }
    request = (diff_bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace(contract._canonical_json(old_scope), contract._canonical_json(new_scope))
    request += "The exact changed file is /.env.\n"
    (diff_bundle / "request.md").write_text(request, encoding="utf-8")
    metadata.update(
        {
            "connector_scope_manifest": new_scope,
            "connector_scope_manifest_sha256": _sha256_text(contract._canonical_json(new_scope)),
            "request_sha256": _sha256_text(request),
            "requested_commit_sha": head_sha,
            "observed_commit_sha": head_sha,
            "connector_file_proof": [],
            "connector_diff_proof": {
                "base_sha": base_sha,
                "head_sha": head_sha,
                "retrieval_status": "complete",
                "citation": f"https://github.com/{metadata['repository']}/compare/{base_sha}...{head_sha}",
            },
            "redaction_preflight": contract.scan_redacted_request(
                request,
                included_paths=contract._scope_allowlist_entries(new_scope),
                excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
            ),
            "transfer_content_preflight": contract.scan_transfer_content(new_scope),
        }
    )
    (diff_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(diff_bundle)
    assert result["consultation_valid"] is False
    assert "exact committed Connector payload failed transfer preflight" in result["errors"]


def test_unused_connector_scope_cannot_retain_commit_or_branch_evidence(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "unused-with-evidence"
    metadata = _valid_bundle(contract, bundle)
    old_scope = metadata["connector_scope_manifest"]
    assert isinstance(old_scope, dict)
    new_scope = {"mode": "unused"}
    request = (bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace(contract._canonical_json(old_scope), contract._canonical_json(new_scope))
    (bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["connector_scope_manifest"] = new_scope
    metadata["connector_scope_manifest_sha256"] = _sha256_text(contract._canonical_json(new_scope))
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["connector:none"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    metadata["transfer_content_preflight"] = contract.scan_transfer_content(new_scope)
    metadata["connector_status"] = "unused"
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "unused Connector scope has inconsistent status or proof" in result["errors"]
    assert "unused Connector scope has inconsistent branch evidence" in result["errors"]


def test_transfer_scan_checks_committed_content_without_returning_it() -> None:
    contract = _load_contract()
    token = "ghp_" + "x" * 24
    original_run_git = contract._run_git
    try:
        contract._run_git = (  # type: ignore[assignment]
            lambda *args: "blob\n" if args[:2] == ("cat-file", "-t") else f"credential={token}\n"
        )
        scope = {
            "mode": "files",
            "repository": contract._local_origin_repository(),
            "commit_sha": _git("rev-parse", "HEAD"),
            "required_files": ["/AGENTS.md"],
        }
        result = contract.scan_transfer_content(scope)
    finally:
        contract._run_git = original_run_git
    assert result["status"] == "blocked"
    assert result["secret_match_count"] >= 1
    assert token not in json.dumps(result)


def test_decision_is_hash_bound_structured_and_cannot_claim_formal_go(tmp_path: Path) -> None:
    contract = _load_contract()
    hash_bundle = tmp_path / "decision-hash"
    _valid_bundle(contract, hash_bundle)
    with (hash_bundle / "decision.md").open("a", encoding="utf-8") as stream:
        stream.write("\nUnbound mutation.\n")
    result = contract.validate_bundle(hash_bundle)
    assert result["consultation_valid"] is False
    assert "decision_sha256 mismatch" in result["errors"]

    formal_bundle = tmp_path / "formal-claim"
    metadata = _valid_bundle(contract, formal_bundle)
    decision = (
        (formal_bundle / "decision.md")
        .read_text(encoding="utf-8")
        .replace(
            "Run repository validation and the independent review gate separately.",
            "Formal review status: GO. Accept everything.",
        )
    )
    (formal_bundle / "decision.md").write_text(decision, encoding="utf-8")
    metadata["decision_sha256"] = _sha256_text(decision)
    (formal_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(formal_bundle)
    assert result["consultation_valid"] is False
    assert "decision must not claim a formal GO" in result["errors"]


def test_metadata_cannot_downgrade_valid_evidence_while_strict_cli_exits_zero(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "asserted-incomplete"
    metadata = _valid_bundle(contract, bundle)
    metadata["consultation_status"] = "incomplete"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["storage_valid"] is False
    assert "computed validity disagrees with metadata" in result["consistency_errors"]
    strict = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "validate-bundle", str(bundle)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert strict.returncode == 1


def test_request_headers_and_local_state_are_machine_checked(tmp_path: Path) -> None:
    contract = _load_contract()
    header_bundle = tmp_path / "header-drift"
    metadata = _valid_bundle(contract, header_bundle)
    request = (
        (header_bundle / "request.md")
        .read_text(encoding="utf-8")
        .replace("Surface required: Chat", "Surface required: Chat\nSurface required: Work")
    )
    (header_bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    (header_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(header_bundle)
    assert result["consultation_valid"] is False
    assert "request header missing or duplicated: Surface required" in result["errors"]

    dirty_bundle = tmp_path / "dirty-drift"
    metadata = _valid_bundle(contract, dirty_bundle)
    metadata["local_state"] = "clean"
    (dirty_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(dirty_bundle)
    assert result["consultation_valid"] is False
    assert "v2 dirty scope must be inventoried locally and never transferred" in result["errors"]


def test_request_cannot_smuggle_absolute_paths_outside_connector_manifest(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "extra-path"
    metadata = _valid_bundle(contract, bundle)
    request = (bundle / "request.md").read_text(encoding="utf-8") + "Read /.env and /oauth_token.json too.\n"
    (bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "request mentions absolute paths outside the Connector scope manifest" in result["errors"]


def test_unused_connector_scope_is_valid_only_without_repository_paths_or_evidence(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "unused"
    metadata = _valid_bundle(contract, bundle)
    old_scope = metadata["connector_scope_manifest"]
    assert isinstance(old_scope, dict)
    new_scope = {"mode": "unused"}
    request = (bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace(contract._canonical_json(old_scope), contract._canonical_json(new_scope))
    request = request.replace("Read /AGENTS.md at the pinned commit.", "No Connector content is requested.")
    request = request.replace(f"Branch: {metadata['branch']}", "Branch: none")
    request = request.replace("Branch requirement: provenance_only", "Branch requirement: not_applicable")
    request = request.replace(f"Repository: {metadata['repository']}", "Repository: none")
    request = request.replace(f"Commit authority: {metadata['requested_commit_sha']}", "Commit authority: none")
    request = request.replace("Connector status: attached_pending", "Connector status: unused")
    (bundle / "request.md").write_text(request, encoding="utf-8")
    response = (bundle / "response.md").read_text(encoding="utf-8")
    response = response.replace(
        f"Reviewed the pinned file.\nConnector citation: https://github.com/{metadata['repository']}/blob/"
        f"{metadata['requested_commit_sha']}/AGENTS.md",
        "Reviewed only the bounded context supplied in the request; no Connector content was used.",
    )
    (bundle / "response.md").write_text(response, encoding="utf-8")
    metadata.update(
        {
            "connector_scope_manifest": new_scope,
            "connector_scope_manifest_sha256": _sha256_text(contract._canonical_json(new_scope)),
            "request_sha256": _sha256_text(request),
            "response_sha256": _sha256_text(response),
            "redaction_preflight": contract.scan_redacted_request(
                request,
                included_paths=["connector:none"],
                excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
            ),
            "transfer_content_preflight": contract.scan_transfer_content(new_scope),
            "connector_status": "unused",
            "request_connector_status": "unused",
            "connector_evidence_valid": False,
            "outcome_code": "none",
            "connector_file_proof": [],
            "connector_diff_proof": None,
            "repository": None,
            "requested_commit_sha": None,
            "observed_commit_sha": None,
            "commit_verification": "not_requested",
            "branch": None,
            "branch_requirement": "not_applicable",
            "branch_lookup_outcome": "not_requested",
            "branch_verification": "not_applicable",
            "branch_resolved_sha": None,
        }
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    assert contract.validate_bundle(bundle)["consultation_valid"] is True

    request += "Read /AGENTS.md anyway.\n"
    (bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["connector:none"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "request mentions absolute paths outside the Connector scope manifest" in result["errors"]


def test_additional_token_families_fail_request_and_retention_scans(tmp_path: Path) -> None:
    contract = _load_contract()
    token = "glpat-" + "x" * 24
    scan = contract.scan_redacted_request(
        f"Review /AGENTS.md. token={token}",
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    assert scan["status"] == "blocked"
    assert scan["secret_match_counts"]["gitlab_token"] == 1
    assert token not in json.dumps(scan)

    bundle = tmp_path / "gitlab-token"
    metadata = _valid_bundle(contract, bundle)
    response = (
        (bundle / "response.md")
        .read_text(encoding="utf-8")
        .replace("# Deferred decisions", f"Token: {token}\n\n# Deferred decisions")
    )
    (bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    metadata["retention_safe"] = False
    metadata["consultation_status"] = "incomplete"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["retention_safe"] is False
    assert result["storage_valid"] is False


def test_response_rejects_blockquoted_verdict_and_moved_authority_label(tmp_path: Path) -> None:
    contract = _load_contract()
    verdict_bundle = tmp_path / "blockquote-verdict"
    metadata = _valid_bundle(contract, verdict_bundle)
    response = (
        (verdict_bundle / "response.md")
        .read_text(encoding="utf-8")
        .replace("END_ARTIFACT", "> Raw Pro verdict: GO\nEND_ARTIFACT")
    )
    (verdict_bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    (verdict_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(verdict_bundle)
    assert result["consultation_valid"] is False
    assert "response must contain exactly one unfenced exact Raw Pro verdict" in result["errors"]

    label_bundle = tmp_path / "moved-label"
    metadata = _valid_bundle(contract, label_bundle)
    response = (label_bundle / "response.md").read_text(encoding="utf-8")
    response = response.replace(f"{contract.ADVISORY_LABEL}\n\n", "", 1)
    response = response.replace("# Findings\n", f"# Findings\n{contract.ADVISORY_LABEL}\n\n")
    (label_bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    (label_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(label_bundle)
    assert result["consultation_valid"] is False
    assert "exact advisory label must immediately follow the response BEGIN marker" in result["errors"]


def test_broad_formal_gate_claim_is_rejected_even_with_recomputed_hash(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "gate-go"
    metadata = _valid_bundle(contract, bundle)
    decision = (
        (bundle / "decision.md")
        .read_text(encoding="utf-8")
        .replace(
            "Run repository validation and the independent review gate separately.",
            "Repository independent review gate verdict: GO. Ship this scope.",
        )
    )
    (bundle / "decision.md").write_text(decision, encoding="utf-8")
    metadata["decision_sha256"] = _sha256_text(decision)
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "decision must not claim a formal GO" in result["errors"]


def test_connector_repository_and_verified_branch_are_bound_to_local_git(tmp_path: Path) -> None:
    contract = _load_contract()
    repo_bundle = tmp_path / "repo-mismatch"
    metadata = _valid_bundle(contract, repo_bundle)
    old_scope = metadata["connector_scope_manifest"]
    assert isinstance(old_scope, dict)
    new_scope = {**old_scope, "repository": "different-owner/different-repo"}
    request = (
        (repo_bundle / "request.md")
        .read_text(encoding="utf-8")
        .replace(contract._canonical_json(old_scope), contract._canonical_json(new_scope))
    )
    (repo_bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["repository"] = "different-owner/different-repo"
    metadata["connector_scope_manifest"] = new_scope
    metadata["connector_scope_manifest_sha256"] = _sha256_text(contract._canonical_json(new_scope))
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    metadata["transfer_content_preflight"] = contract.scan_transfer_content(new_scope)
    metadata["connector_file_proof"][0]["citation"] = (  # type: ignore[index]
        f"https://github.com/different-owner/different-repo/blob/{metadata['requested_commit_sha']}/AGENTS.md"
    )
    (repo_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(repo_bundle)
    assert result["consultation_valid"] is False
    assert "Connector repository does not match the request scope manifest and local GitHub origin" in result["errors"]

    branch_bundle = tmp_path / "branch-sha-mismatch"
    metadata = _valid_bundle(contract, branch_bundle)
    metadata["branch_resolved_sha"] = _git("rev-parse", "HEAD^")
    (branch_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(branch_bundle)
    assert result["consultation_valid"] is False
    assert "verified branch must resolve to the requested immutable commit" in result["errors"]


def test_literal_git_pathspec_cannot_hide_changed_file_content(tmp_path: Path) -> None:
    contract = _load_contract()
    repository = tmp_path / "literal-pathspec-repo"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.email", "fixture@example.invalid"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repository, check=True)
    (repository / "base.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "add", "base.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "base"], cwd=repository, check=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    tricky_name = ":(exclude,glob)**"
    token = "sk-" + "proj-" + "x" * 28
    (repository / tricky_name).write_text(f"credential={token}\n", encoding="utf-8")
    subprocess.run(["git", "--literal-pathspecs", "add", "--", tricky_name], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "tricky"], cwd=repository, check=True)
    head_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    original_git_root = contract._git_root
    try:
        contract._git_root = lambda: repository  # type: ignore[assignment]
        result = contract.scan_transfer_content(
            {
                "mode": "diff",
                "repository": "owner/repo",
                "base_sha": base_sha,
                "head_sha": head_sha,
                "changed_files": [f"/{tricky_name}"],
            }
        )
    finally:
        contract._git_root = original_git_root
    assert result["changed_file_inventory_matches"] is True
    assert result["secret_match_count"] >= 1
    assert result["status"] == "blocked"
    assert token not in json.dumps(result)


def test_diff_scan_uses_same_three_dot_range_as_connector_compare(tmp_path: Path) -> None:
    contract = _load_contract()
    repository = tmp_path / "three-dot-repo"
    repository.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.email", "fixture@example.invalid"], cwd=repository, check=True)
    subprocess.run(["git", "config", "user.name", "Fixture"], cwd=repository, check=True)
    (repository / "root.txt").write_text("root\n", encoding="utf-8")
    subprocess.run(["git", "add", "root.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "root"], cwd=repository, check=True)
    root_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    token = "sk-" + "proj-" + "y" * 28
    (repository / "shared-secret.txt").write_text(f"credential={token}\n", encoding="utf-8")
    subprocess.run(["git", "add", "shared-secret.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "base-side"], cwd=repository, check=True)
    base_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    subprocess.run(["git", "checkout", "-q", "-b", "head-side", root_sha], cwd=repository, check=True)
    (repository / "shared-secret.txt").write_text(f"credential={token}\n", encoding="utf-8")
    (repository / "benign.txt").write_text("benign\n", encoding="utf-8")
    subprocess.run(["git", "add", "shared-secret.txt", "benign.txt"], cwd=repository, check=True)
    subprocess.run(["git", "commit", "-qm", "head-side"], cwd=repository, check=True)
    head_sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    original_git_root = contract._git_root
    try:
        contract._git_root = lambda: repository  # type: ignore[assignment]
        result = contract.scan_transfer_content(
            {
                "mode": "diff",
                "repository": "owner/repo",
                "base_sha": base_sha,
                "head_sha": head_sha,
                "changed_files": ["/benign.txt", "/shared-secret.txt"],
            }
        )
    finally:
        contract._git_root = original_git_root
    assert result["actual_changed_files"] == ["/benign.txt", "/shared-secret.txt"]
    assert result["changed_file_inventory_matches"] is True
    assert result["secret_match_count"] >= 1
    assert result["status"] == "blocked"


def test_file_scope_rejects_git_tree_instead_of_scanning_directory_listing() -> None:
    contract = _load_contract()
    result = contract.scan_transfer_content(
        {
            "mode": "files",
            "repository": contract._local_origin_repository(),
            "commit_sha": _git("rev-parse", "HEAD"),
            "required_files": ["/sourcing-ai-agent/docs"],
        }
    )
    assert result["non_blob_path_count"] == 1
    assert result["status"] == "blocked"


def test_bundle_inventory_is_exact_and_extra_file_is_scanned_for_retention(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "extra-file"
    _valid_bundle(contract, bundle)
    token = "ghp_" + "q" * 24
    (bundle / "raw-session.json").write_text(json.dumps({"token": token}) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["retention_safe"] is False
    assert result["storage_valid"] is False
    assert "bundle directory must contain exactly four regular contract files" in result["errors"]
    assert token not in json.dumps(result)


def test_decoded_secret_representations_and_aws_secret_assignment_fail_closed(tmp_path: Path) -> None:
    contract = _load_contract()
    encoded_json = "sk" + "\\u002d" + "proj" + "\\u002d" + "a" * 28
    escaped_markdown = "sk" + "\\-" + "proj" + "\\-" + "b" * 28
    aws_assignment = "AWS_SECRET_ACCESS_KEY=" + "c" * 40
    for value in (encoded_json, escaped_markdown, aws_assignment):
        result = contract.scan_redacted_request(
            f"Review /AGENTS.md. value={value}",
            included_paths=["/AGENTS.md"],
            excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
        )
        assert result["status"] == "blocked", value[:12]
        assert result["secret_match_count"] >= 1
        assert value not in json.dumps(result)

    bundle = tmp_path / "encoded-metadata"
    metadata = _valid_bundle(contract, bundle)
    metadata["unexpected_notes"] = encoded_json
    metadata["retention_safe"] = False
    metadata["consultation_status"] = "incomplete"
    metadata["consultation_valid"] = False
    metadata["usable_for_advisory_decision"] = False
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert result["retention_safe"] is False
    assert result["storage_valid"] is False


def test_connector_citation_inside_fence_does_not_count_as_evidence(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "fenced-citation"
    metadata = _valid_bundle(contract, bundle)
    citation = str(metadata["connector_file_proof"][0]["citation"])  # type: ignore[index]
    response = (bundle / "response.md").read_text(encoding="utf-8")
    response = response.replace(f"Connector citation: {citation}", f"```text\n{citation}\n```")
    (bundle / "response.md").write_text(response, encoding="utf-8")
    metadata["response_sha256"] = _sha256_text(response)
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "required Connector file proof is incomplete or uncited" in result["errors"]

    for fence in ("````", "~~~~"):
        nested_bundle = tmp_path / f"nested-{ord(fence[0])}"
        metadata = _valid_bundle(contract, nested_bundle)
        citation = str(metadata["connector_file_proof"][0]["citation"])  # type: ignore[index]
        response = (nested_bundle / "response.md").read_text(encoding="utf-8")
        response = response.replace(
            f"Connector citation: {citation}",
            f"{fence}text\n```\n{citation}\n```\n{fence}",
        )
        (nested_bundle / "response.md").write_text(response, encoding="utf-8")
        metadata["response_sha256"] = _sha256_text(response)
        (nested_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        result = contract.validate_bundle(nested_bundle)
        assert result["consultation_valid"] is False, fence
        assert "required Connector file proof is incomplete or uncited" in result["errors"]


def test_request_authority_headers_are_hash_bound_and_cannot_contradict_scope(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "request-authority"
    metadata = _valid_bundle(contract, bundle)
    request = (bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace(f"Repository: {metadata['repository']}", "Repository: another/repository")
    (bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    (bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(bundle)
    assert result["consultation_valid"] is False
    assert "request authority/UI/local/branch headers are invalid or inconsistent with metadata" in result["errors"]

    sent_bundle = tmp_path / "sent-request"
    metadata = _valid_bundle(contract, sent_bundle)
    request = (sent_bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace("Consultation status: planned", "Consultation status: sent")
    (sent_bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["request_status"] = "sent"
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    (sent_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    assert contract.preflight_request(sent_bundle / "request.md")["preflight_status"] == "blocked"
    result = contract.validate_bundle(sent_bundle)
    assert result["consultation_valid"] is False
    assert "persisted request failed the mandatory pre-send contract" in result["errors"]

    unused_status_bundle = tmp_path / "wrong-connector-status"
    metadata = _valid_bundle(contract, unused_status_bundle)
    request = (unused_status_bundle / "request.md").read_text(encoding="utf-8")
    request = request.replace("Connector status: attached_pending", "Connector status: unused")
    (unused_status_bundle / "request.md").write_text(request, encoding="utf-8")
    metadata["request_connector_status"] = "unused"
    metadata["request_sha256"] = _sha256_text(request)
    metadata["redaction_preflight"] = contract.scan_redacted_request(
        request,
        included_paths=["/AGENTS.md"],
        excluded_categories=sorted(contract.REQUIRED_EXCLUDED_CATEGORIES),
    )
    (unused_status_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    assert contract.preflight_request(unused_status_bundle / "request.md")["preflight_status"] == "blocked"
    result = contract.validate_bundle(unused_status_bundle)
    assert result["consultation_valid"] is False
    assert "request_connector_status is inconsistent with the Connector scope" in result["errors"]


def test_authority_claim_without_literal_go_and_empty_decision_closure_fail(tmp_path: Path) -> None:
    contract = _load_contract()
    authority_bundle = tmp_path / "authority-claim"
    metadata = _valid_bundle(contract, authority_bundle)
    decision = (authority_bundle / "decision.md").read_text(encoding="utf-8")
    decision = decision.replace(
        "Run repository validation and the independent review gate separately.",
        "Independent review completed; this is approved for production.",
    )
    (authority_bundle / "decision.md").write_text(decision, encoding="utf-8")
    metadata["decision_sha256"] = _sha256_text(decision)
    (authority_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(authority_bundle)
    assert result["consultation_valid"] is False
    assert "consultation artifacts must not claim formal or independent-review approval" in result["errors"]

    for index, claim in enumerate(
        (
            "Formal review passed; this scope is cleared.",
            "Independent audit succeeded; continue.",
            "This output constitutes an independent review artifact.",
        )
    ):
        claim_bundle = tmp_path / f"authority-claim-{index}"
        metadata = _valid_bundle(contract, claim_bundle)
        decision = (claim_bundle / "decision.md").read_text(encoding="utf-8")
        decision = decision.replace(
            "Run repository validation and the independent review gate separately.",
            claim,
        )
        (claim_bundle / "decision.md").write_text(decision, encoding="utf-8")
        metadata["decision_sha256"] = _sha256_text(decision)
        (claim_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        result = contract.validate_bundle(claim_bundle)
        assert result["consultation_valid"] is False, claim
        assert "consultation artifacts must not claim formal or independent-review approval" in result["errors"]

    empty_bundle = tmp_path / "empty-decision"
    metadata = _valid_bundle(contract, empty_bundle)
    decision = f"# Decision\n\n## Authority\n\n{contract.ADVISORY_LABEL}\n\n## Local disposition\n\n## Follow-up\n"
    (empty_bundle / "decision.md").write_text(decision, encoding="utf-8")
    metadata["decision_sha256"] = _sha256_text(decision)
    (empty_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(empty_bundle)
    assert result["consultation_valid"] is False
    assert "decision must contain one reasoned P0 disposition" in result["errors"]
    assert "decision must contain one non-empty validation record" in result["errors"]
    assert "decision Follow-up section must not be empty" in result["errors"]

    whitespace_bundle = tmp_path / "whitespace-decision"
    metadata = _valid_bundle(contract, whitespace_bundle)
    decision = (whitespace_bundle / "decision.md").read_text(encoding="utf-8")
    decision = decision.replace("P0 disposition: none — no P0 finding.", "P0 disposition: none —   ")
    decision = decision.replace("Validation: targeted contract tests passed.", "Validation:   ")
    decision = decision.replace(
        "P1 disposition: none — no P1 finding.",
        "P1 disposition: none — no P1 finding.\nP1 disposition: malformed duplicate",
    )
    (whitespace_bundle / "decision.md").write_text(decision, encoding="utf-8")
    metadata["decision_sha256"] = _sha256_text(decision)
    (whitespace_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(whitespace_bundle)
    assert result["consultation_valid"] is False
    assert "decision must contain one reasoned P0 disposition" in result["errors"]
    assert "decision must contain one reasoned P1 disposition" in result["errors"]
    assert "decision must contain one non-empty validation record" in result["errors"]


def test_unknown_skill_hash_field_and_extra_symlink_fail_closed(tmp_path: Path) -> None:
    contract = _load_contract()
    hash_bundle = tmp_path / "unknown-hash"
    metadata = _valid_bundle(contract, hash_bundle)
    metadata["consultation_skill_sha256"] = "0" * 64
    (hash_bundle / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    result = contract.validate_bundle(hash_bundle)
    assert result["consultation_valid"] is False
    assert "metadata contains fields outside the v2 allowlist" in result["errors"]

    symlink_bundle = tmp_path / "extra-symlink"
    _valid_bundle(contract, symlink_bundle)
    (symlink_bundle / "alias.md").symlink_to(symlink_bundle / "request.md")
    result = contract.validate_bundle(symlink_bundle)
    assert result["bundle_inventory_valid"] is False
    assert result["storage_valid"] is False


def test_preflight_request_cli_gates_exact_committed_scope_before_send(tmp_path: Path) -> None:
    contract = _load_contract()
    bundle = tmp_path / "preflight"
    _valid_bundle(contract, bundle)
    request_path = bundle / "request.md"
    result = contract.preflight_request(request_path)
    assert result["preflight_status"] == "passed", result
    completed = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "preflight-request", str(request_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout

    unsafe_request = request_path.read_text(encoding="utf-8") + "Read /oauth_token.json too.\n"
    request_path.write_text(unsafe_request, encoding="utf-8")
    result = contract.preflight_request(request_path)
    assert result["preflight_status"] == "blocked"
    assert "request mentions absolute paths outside the Connector scope manifest" in result["errors"]
    completed = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "preflight-request", str(request_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 1
