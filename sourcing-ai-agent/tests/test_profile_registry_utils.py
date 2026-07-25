import unittest

from sourcing_agent.harvest_connectors import parse_harvest_profile_payload
from sourcing_agent.profile_registry_utils import (
    classify_harvest_profile_payload_status,
    extract_profile_registry_aliases_from_payload,
)


class ProfileRegistryUtilsTest(unittest.TestCase):
    def test_parse_harvest_profile_payload_supports_nested_result_profile_wrapper(self) -> None:
        payload = {
            "_harvest_request": {
                "profile_url": "https://www.linkedin.com/in/anna-walling/",
            },
            "result": {
                "data": {
                    "profile": {
                        "firstName": "Anna",
                        "lastName": "Walling",
                        "profileUrl": "https://www.linkedin.com/in/anna-walling/",
                        "public_identifier": "anna-walling",
                        "headline": "Member of Technical Staff at Physical Intelligence",
                        "positions": [
                            {
                                "companyName": "Physical Intelligence",
                                "title": "Member of Technical Staff",
                                "isCurrent": True,
                            }
                        ],
                        "schools": [{"schoolName": "Stanford University"}],
                        "languages": [{"name": "English"}],
                    }
                }
            },
        }

        parsed = parse_harvest_profile_payload(payload)

        self.assertEqual(parsed["full_name"], "Anna Walling")
        self.assertEqual(parsed["profile_url"], "https://www.linkedin.com/in/anna-walling/")
        self.assertEqual(parsed["current_company"], "Physical Intelligence")
        self.assertEqual(parsed["education"][0]["schoolName"], "Stanford University")
        self.assertEqual(parsed["experience"][0]["companyName"], "Physical Intelligence")

    def test_classify_harvest_profile_payload_status_accepts_nested_sparse_profile(self) -> None:
        payload = {
            "result": {
                "data": {
                    "profile": {
                        "headline": "Research Engineer at xAI",
                        "schools": [{"schoolName": "MIT"}],
                    }
                }
            }
        }

        self.assertEqual(classify_harvest_profile_payload_status(payload), "fetched")

    def test_classify_harvest_profile_payload_status_reads_nested_retryable_error_envelope(self) -> None:
        payload = {
            "result": {
                "statusText": "Gateway Timeout",
                "error": {
                    "message": "Temporary upstream timeout",
                    "code": 504,
                },
            }
        }

        self.assertEqual(classify_harvest_profile_payload_status(payload), "failed_retryable")

    def test_extract_profile_registry_aliases_reads_nested_profile_url(self) -> None:
        payload = {
            "_harvest_request": {
                "profile_url": "https://www.linkedin.com/in/ACwAACi6kRQBOm_dcRKuOnhoIVJZLitAECAHwp0",
                "raw_linkedin_url": "https://www.linkedin.com/in/ACwAACi6kRQBOm_dcRKuOnhoIVJZLitAECAHwp0",
            },
            "result": {
                "data": {
                    "profile": {
                        "profileUrl": "https://www.linkedin.com/in/adnan-esmail/",
                    }
                }
            },
        }

        aliases = extract_profile_registry_aliases_from_payload(payload)

        self.assertEqual(
            aliases["sanity_linkedin_url"],
            "https://www.linkedin.com/in/adnan-esmail/",
        )
        self.assertIn(
            "https://www.linkedin.com/in/ACwAACi6kRQBOm_dcRKuOnhoIVJZLitAECAHwp0",
            aliases["alias_urls"],
        )
