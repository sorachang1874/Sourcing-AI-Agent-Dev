import unittest
from unittest.mock import patch

from sourcing_agent.dataforseo_client import (
    DataForSeoClientError,
    DataForSeoGoogleOrganicClient,
    build_google_organic_task,
    extract_google_organic_submitted_tasks,
)


class DataForSeoClientTest(unittest.TestCase):
    def setUp(self) -> None:
        # Provider-mode default is now fail-closed (simulate). This suite exercises the
        # LIVE DataForSEO dispatch path with the network mocked, so it opts into live +
        # the non-production dual-confirm.
        _live = patch.dict(
            "os.environ",
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
                "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
                "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
            },
            clear=False,
        )
        _live.start()
        self.addCleanup(_live.stop)

    def test_task_post_many_posts_multiple_tasks(self) -> None:
        client = DataForSeoGoogleOrganicClient(login="login", password="password", timeout_seconds=30)
        payload = {
            "status_code": 20000,
            "tasks": [
                {"id": "task_1", "status_code": 20100, "result": None},
                {"id": "task_2", "status_code": 20100, "result": None},
            ],
        }
        tasks = [
            build_google_organic_task(keyword="Jane Doe Thinking Machines Lab", tag="q1"),
            build_google_organic_task(keyword="John Smith Thinking Machines Lab", tag="q2"),
        ]
        with patch("sourcing_agent.dataforseo_client.requests.request") as request_mock:
            request_mock.return_value.raise_for_status.return_value = None
            request_mock.return_value.json.return_value = payload
            response = client.task_post_many(tasks)

        self.assertEqual(response["tasks"][0]["id"], "task_1")
        self.assertEqual(request_mock.call_count, 1)
        self.assertEqual(
            request_mock.call_args.kwargs["json"],
            tasks,
        )

    def test_task_post_many_can_preserve_partial_task_errors_for_item_level_retry(self) -> None:
        client = DataForSeoGoogleOrganicClient(login="login", password="password", timeout_seconds=30)
        payload = {
            "status_code": 20000,
            "tasks": [
                {"id": "task_1", "status_code": 20100, "result": None},
                {"id": "", "status_code": 50000, "status_message": "Temporary provider error", "result": None},
            ],
        }
        tasks = [
            build_google_organic_task(keyword="Jane Doe Thinking Machines Lab", tag="q1"),
            build_google_organic_task(keyword="John Smith Thinking Machines Lab", tag="q2"),
        ]
        with patch("sourcing_agent.dataforseo_client.requests.request") as request_mock:
            request_mock.return_value.raise_for_status.return_value = None
            request_mock.return_value.json.return_value = payload
            response = client.task_post_many(tasks, allow_partial_task_errors=True)

        self.assertEqual(response["tasks"][0]["id"], "task_1")
        self.assertEqual(response["tasks"][1]["status_code"], 50000)

        with patch("sourcing_agent.dataforseo_client.requests.request") as request_mock:
            request_mock.return_value.raise_for_status.return_value = None
            request_mock.return_value.json.return_value = payload
            with self.assertRaises(DataForSeoClientError):
                client.task_post_many(tasks)

    def test_extract_google_organic_submitted_tasks_uses_fallback_when_data_missing(self) -> None:
        payload = {
            "tasks": [
                {"id": "task_1", "status_code": 20100, "result": None},
                {"id": "task_2", "status_code": 20100, "result": None},
            ]
        }
        submitted = extract_google_organic_submitted_tasks(
            payload,
            fallback_tasks=[
                {"keyword": "Jane Doe Thinking Machines Lab", "tag": "q1"},
                {"keyword": "John Smith Thinking Machines Lab", "tag": "q2"},
            ],
        )
        self.assertEqual(submitted[0]["task_id"], "task_1")
        self.assertEqual(submitted[0]["keyword"], "Jane Doe Thinking Machines Lab")
        self.assertEqual(submitted[1]["tag"], "q2")


if __name__ == "__main__":
    unittest.main()
