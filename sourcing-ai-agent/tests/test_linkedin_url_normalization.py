from sourcing_agent.linkedin_url_normalization import (
    normalize_linkedin_profile_url_key,
    normalize_linkedin_profile_url_list,
)


def test_normalize_linkedin_profile_url_key_matches_existing_contract() -> None:
    assert (
        normalize_linkedin_profile_url_key("linkedin.com/in/ALICE-EXAMPLE//")
        == "https://linkedin.com/in/alice-example"
    )
    assert (
        normalize_linkedin_profile_url_key("https://www.linkedin.com/in/Alice-Example/?trk=public_profile")
        == "https://www.linkedin.com/in/alice-example"
    )


def test_normalize_linkedin_profile_url_list_dedupes_by_key() -> None:
    assert normalize_linkedin_profile_url_list(
        [
            "https://www.linkedin.com/in/alice-example/",
            "https://www.linkedin.com/in/ALICE-EXAMPLE",
            "",
            "linkedin.com/in/alice-example",
        ]
    ) == [
        "https://www.linkedin.com/in/alice-example/",
        "linkedin.com/in/alice-example",
    ]
