from __future__ import annotations

import asyncio
import unittest

from sourcing_agent.api import _request_priority_lane, _RequestConcurrencyMiddleware


class C1aTransportContractTest(unittest.TestCase):
    def test_export_light_lane_classifier_is_exact(self) -> None:
        expected_light = {
            ("POST", "/api/projections/export"),
            ("POST", "/api/crm/records/public-web-export"),
            ("GET", "/api/exports/task-1"),
            ("get", "/api/exports/task-1"),
            # Pre-existing transport exception: every CORS preflight uses the
            # light lane, independent of the routed method's business weight.
            ("OPTIONS", "/api/projections/export"),
            ("OPTIONS", "/api/exports/task-1/artifact"),
        }
        expected_shared = {
            ("GET", "/api/projections/export"),
            ("POST", "/api/projections/export/"),
            ("POST", "/api/projections/export/extra"),
            ("GET", "/api/crm/records/public-web-export"),
            ("POST", "/api/crm/records/public-web-export/"),
            ("POST", "/api/crm/records/public-web-export/extra"),
            ("GET", "/api/exports/"),
            ("GET", "/api/exports/task-1/"),
            ("GET", "/api/exports/task-1/extra"),
            ("GET", "/api/exports/task-1/artifact"),
            ("POST", "/api/exports/task-1"),
        }

        for method, path in expected_light:
            with self.subTest(method=method, path=path):
                self.assertEqual(_request_priority_lane(method, path), "light")
        for method, path in expected_shared:
            with self.subTest(method=method, path=path):
                self.assertEqual(_request_priority_lane(method, path), "shared")

    def test_export_submit_and_poll_use_reserved_lane_while_artifact_waits_for_shared(self) -> None:
        async def scenario() -> None:
            heavy_entered = asyncio.Event()
            release_heavy = asyncio.Event()
            artifact_entered = asyncio.Event()

            async def app(scope: dict[str, object], receive: object, send: object) -> None:
                path = str(scope.get("path") or "")
                if path == "/heavy-holder":
                    heavy_entered.set()
                    await release_heavy.wait()
                elif path == "/api/exports/task-1/artifact":
                    artifact_entered.set()

            middleware = _RequestConcurrencyMiddleware(app, shared_limit=1, light_reserved_limit=1)

            async def invoke(method: str, path: str) -> None:
                async def receive() -> dict[str, object]:
                    return {"type": "http.request", "body": b"", "more_body": False}

                async def send(message: dict[str, object]) -> None:
                    del message

                await middleware(
                    {"type": "http", "method": method, "path": path},
                    receive,
                    send,
                )

            heavy_task = asyncio.create_task(invoke("POST", "/heavy-holder"))
            await asyncio.wait_for(heavy_entered.wait(), timeout=1.0)

            await asyncio.wait_for(invoke("POST", "/api/projections/export"), timeout=0.5)
            await asyncio.wait_for(invoke("GET", "/api/exports/task-1"), timeout=0.5)

            artifact_task = asyncio.create_task(invoke("GET", "/api/exports/task-1/artifact"))
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(artifact_entered.wait(), timeout=0.05)

            release_heavy.set()
            await asyncio.wait_for(heavy_task, timeout=1.0)
            await asyncio.wait_for(artifact_task, timeout=1.0)
            self.assertTrue(artifact_entered.is_set())

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
