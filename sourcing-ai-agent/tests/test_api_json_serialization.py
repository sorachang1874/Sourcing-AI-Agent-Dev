import json
import threading
import unittest
from urllib import request as urllib_request

from sourcing_agent.api import create_server


class _BytesPlanOrchestrator:
    def plan_workflow(self, _payload):
        return {
            "status": b"ok",
            "intent_rewrite": {
                "request": {
                    "target_company": b"OpenAI",
                }
            },
            "plan_review_session": {
                "review_id": b"42",
            },
        }


class _ExcelWorkflowOrchestrator:
    def __init__(self) -> None:
        self.last_payload = {}

    def start_excel_intake_workflow(self, payload):
        self.last_payload = dict(payload or {})
        return {
            "status": "queued",
            "filename": self.last_payload.get("filename"),
            "attach_to_snapshot": self.last_payload.get("attach_to_snapshot"),
            "build_artifacts": self.last_payload.get("build_artifacts"),
        }


class ApiJsonSerializationTest(unittest.TestCase):
    def test_plan_route_normalizes_bytes_payload(self) -> None:
        orchestrator = _BytesPlanOrchestrator()
        server = create_server(orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            request = urllib_request.Request(
                f"http://{host}:{port}/api/plan",
                data=json.dumps({"raw_user_request": "我想要OpenAI做Reasoning方向的人"}, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["intent_rewrite"]["request"]["target_company"], "OpenAI")
            self.assertEqual(payload["plan_review_session"]["review_id"], "42")
        finally:
            server.shutdown()
            thread.join(timeout=5)

    def test_excel_workflow_route_accepts_multipart_uploads(self) -> None:
        orchestrator = _ExcelWorkflowOrchestrator()
        server = create_server(orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        boundary = "----CodexExcelBoundary"
        workbook_bytes = b"test-xlsx-payload"
        try:
            multipart_body = b"".join(
                [
                    f"--{boundary}\r\n".encode("utf-8"),
                    b'Content-Disposition: form-data; name="query_text"\r\n\r\n',
                    "Excel 批量导入 OpenAI 候选人\r\n".encode("utf-8"),
                    f"--{boundary}\r\n".encode("utf-8"),
                    b'Content-Disposition: form-data; name="attach_to_snapshot"\r\n\r\n',
                    b"true\r\n",
                    f"--{boundary}\r\n".encode("utf-8"),
                    b'Content-Disposition: form-data; name="build_artifacts"\r\n\r\n',
                    b"false\r\n",
                    f"--{boundary}\r\n".encode("utf-8"),
                    (
                        'Content-Disposition: form-data; name="file"; filename="contacts.xlsx"\r\n'
                        "Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n"
                    ).encode("utf-8"),
                    workbook_bytes,
                    b"\r\n",
                    f"--{boundary}--\r\n".encode("utf-8"),
                ]
            )
            request = urllib_request.Request(
                f"http://{host}:{port}/api/intake/excel/workflow",
                data=multipart_body,
                headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
                method="POST",
            )
            with opener.open(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(payload["status"], "queued")
            self.assertEqual(orchestrator.last_payload["filename"], "contacts.xlsx")
            self.assertEqual(orchestrator.last_payload["query_text"], "Excel 批量导入 OpenAI 候选人")
            self.assertTrue(orchestrator.last_payload["attach_to_snapshot"])
            self.assertFalse(orchestrator.last_payload["build_artifacts"])
            self.assertEqual(
                orchestrator.last_payload["file_content_base64"],
                "dGVzdC14bHN4LXBheWxvYWQ=",
            )
        finally:
            server.shutdown()
            thread.join(timeout=5)
