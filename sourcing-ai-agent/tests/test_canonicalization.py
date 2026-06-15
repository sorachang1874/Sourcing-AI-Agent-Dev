from sourcing_agent.canonicalization import canonicalize_company_records
from sourcing_agent.domain import Candidate


def test_canonicalization_merges_source_seed_keyword_provenance() -> None:
    profile_rich_candidate = Candidate(
        candidate_id="rich_agent_candidate",
        name_en="Pat Agent",
        display_name="Pat Agent",
        category="employee",
        target_company="Acme",
        organization="Acme",
        employment_status="current",
        role="Member of Technical Staff",
        linkedin_url="https://www.linkedin.com/in/pat-agent/",
        education="MIT",
        work_history="Acme",
        source_dataset="acme_search_seed_candidates",
        metadata={
            "seed_query": "Reasoning",
            "seed_source_type": "harvest_profile_search",
        },
    )
    current_query_seed_candidate = Candidate(
        candidate_id="current_query_seed_candidate",
        name_en="Pat Agent",
        display_name="Pat Agent",
        category="employee",
        target_company="Acme",
        organization="Acme",
        employment_status="current",
        role="Member of Technical Staff",
        linkedin_url="https://www.linkedin.com/in/pat-agent/",
        source_dataset="acme_search_seed_candidates",
        metadata={
            "seed_query": "Agent",
            "seed_source_type": "harvest_profile_search",
        },
    )

    candidates, _, summary = canonicalize_company_records(
        [profile_rich_candidate, current_query_seed_candidate],
        [],
    )

    assert summary["merged_candidate_count"] == 1
    assert len(candidates) == 1
    assert candidates[0].metadata["seed_query"] == "Reasoning"
    assert candidates[0].metadata["matched_keywords"] == ["Reasoning", "Agent"]
    assert [
        {key: item.get(key) for key in ("field", "matched_on", "source_type", "source_query")}
        for item in candidates[0].metadata["source_matches"]
    ] == [
        {
            "field": "source_seed_query",
            "matched_on": "Reasoning",
            "source_type": "harvest_profile_search",
            "source_query": "Reasoning",
        },
        {
            "field": "source_seed_query",
            "matched_on": "Agent",
            "source_type": "harvest_profile_search",
            "source_query": "Agent",
        },
    ]
