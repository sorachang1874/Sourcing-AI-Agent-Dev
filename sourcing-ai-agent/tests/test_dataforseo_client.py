import unittest
from unittest.mock import patch

from sourcing_agent.dataforseo_client import (
    DataForSeoGoogleOrganicClient,
    build_google_organic_task,
    extract_google_organic_submitted_tasks,
)


class DataForSeoClientTest(unittest.TestCase):
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
