"""Committed DeepSeek chat-completions Luna transport (substitute judge binding).

The judge layer of :mod:`x_first.luna_batch_runner` is model-agnostic (operator
directive 2026-07-20): any provider that honors the judged-output contract may
serve it through a ``JudgmentModelBinding``.  This module is the committed
DeepSeek binding + transport, replacing the ad-hoc ``/tmp`` driver that ran the
OpenAI Layer1-3 batch (943/947 first pass; 947/947 after the tail retry round,
2026-07-20) — the third batch run must not be ad-hoc.

DeepSeek serves an OpenAI-compatible **chat-completions** API, not the OpenAI
Responses contract the runner's payload builder targets, so the transport
translates at the boundary (the translation is byte-identical to the proven
ad-hoc driver):

- request: Responses payload -> chat ``messages`` (system = the pinned prompt's
  ``developer_instructions`` + an output-contract addendum carrying the
  runner-derived strict judged-output schema, user = the candidate bundle
  payload text) with ``response_format: {"type": "json_object"}``;
- response: exactly-one-choice chat completion -> Responses-shaped envelope
  (``status: completed``, served ``model`` id passed through verbatim, the
  message text as one ``output_text`` item).

The transport deliberately owns NO judged-output validator: strict JSON decode,
exact returned-model check, axis-state/citation closure, and the
``x.source_neutral.mapping.luna_candidate_review.v1`` schema assertion all stay
in the runner's ``extract_judged_output`` / ``build_candidate_review`` path,
which composes unchanged with this transport.

Auth: the API key comes from the ``DEEPSEEK_API_KEY`` environment variable (or
constructor injection for tests) and is validated fail-closed at construction,
before any provider-costing call; it is never logged, receipted, or written to
artifacts.  The runner's approval-receipt gate still runs first, so no call
leaves this transport without a candidate-bound approval receipt upstream.

Failure surface (deterministic, no silent fallbacks):

- transport-level errors (DNS/socket/timeout, surfaced by the HTTP client as
  ``RuntimeError``), HTTP 429/5xx, and an undecodable 200 body are retried up
  to ``max_attempts`` with linear backoff; exhaustion raises
  ``LunaBatchRunnerError("luna_transport_retry_exhausted")``;
- any other non-200 status fails immediately with
  ``LunaBatchRunnerError("luna_http_status_rejected:<status>")``;
- a well-formed 200 whose chat envelope violates the one-choice contract fails
  immediately with ``LunaBatchRunnerError("luna_response_invalid")``.

Every attempt appends a sanitized receipt (candidate_ref, attempt ordinal,
HTTP status, outcome, error code — never the key or payload) to the
lock-guarded ``attempts`` log, so the transport is stateless per call and safe
for the runner's worker pool.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from x_first import luna_batch_runner as lbr
from x_first.luna_batch_runner import LunaBatchRunnerError
from x_first.luna_live_canary import HttpClient, HttpResponse

PROVIDER_ID = "deepseek_chat_completions_translated"
CHAT_COMPLETIONS_URL = "https://api.deepseek.com/chat/completions"
REQUEST_MODEL_ID = "deepseek-reasoner"  # reasoning-mode alias on the DeepSeek router
SERVED_MODEL_ID = "deepseek-v4-flash"  # served id observed 2026-07-19/20 (aliases reroute here)
KEY_ENVIRONMENT_VARIABLE = "DEEPSEEK_API_KEY"

DEFAULT_MAX_ATTEMPTS = 3
MAX_ATTEMPTS = 8
DEFAULT_RETRY_BACKOFF_S = 1.0
DEFAULT_MAX_OUTPUT_TOKENS = 8192
MAX_RESPONSE_BYTES = 1_048_576
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

_KEY_RE = re.compile(r"sk-[A-Za-z0-9_-]{16,252}")


def deepseek_judgment_binding() -> lbr.JudgmentModelBinding:
    """The committed DeepSeek substitute binding for the Luna judge layer.

    The pinned candidate-review prompt is rebound to the SERVED model id only
    (every other byte identical); its canonical sha256 is recomputed by
    ``judgment_binding_for_model`` so receipts stay hash-auditable.  The runner
    records this provider/endpoint/model in every execution receipt, and the
    exact returned-model check binds DeepSeek's served ``model`` field to
    ``SERVED_MODEL_ID`` fail-closed.
    """

    return lbr.judgment_binding_for_model(
        provider_id=PROVIDER_ID,
        endpoint=CHAT_COMPLETIONS_URL,
        model_id=SERVED_MODEL_ID,
    )


class _NoRedirectHandler(urllib_request.HTTPRedirectHandler):
    def redirect_request(  # type: ignore[override]
        self,
        req: urllib_request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


class UrllibChatCompletionsClient:
    """Stdlib POST client for the chat-completions route.

    Redirects are disabled, response bytes are bounded, and the route is pinned
    to the constructor endpoint.  Transport failures (DNS/socket/timeout) are
    normalized to ``RuntimeError("deepseek_http_transport_failed")``; HTTP error
    statuses are returned as bounded ``HttpResponse`` rows so the transport can
    classify retryable vs terminal.
    """

    def __init__(self, *, endpoint: str) -> None:
        self._endpoint = endpoint

    @staticmethod
    def _read_bounded(stream: Any, maximum: int) -> bytes:
        value = stream.read(maximum + 1)
        if len(value) > maximum:
            raise ValueError("response_byte_budget_exceeded")
        return value

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout_ms: int,
        max_response_bytes: int,
    ) -> HttpResponse:
        if method != "POST" or url != self._endpoint:
            raise ValueError("closed_http_route_required")
        opener = urllib_request.build_opener(_NoRedirectHandler())
        outbound = urllib_request.Request(url=url, data=body, headers=dict(headers), method=method)
        try:
            with opener.open(outbound, timeout=max(1, timeout_ms) / 1000) as response:
                payload = self._read_bounded(response, max_response_bytes)
                return HttpResponse(
                    status_code=response.status,
                    headers={"content-type": response.headers.get("Content-Type", "")},
                    body=payload,
                )
        except urllib_error.HTTPError as exc:
            try:
                payload = self._read_bounded(exc, max_response_bytes)
            finally:
                exc.close()
            return HttpResponse(
                status_code=exc.code,
                headers={"content-type": exc.headers.get("Content-Type", "") if exc.headers else ""},
                body=payload,
            )
        except (urllib_error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError("deepseek_http_transport_failed") from exc


class DeepSeekChatCompletionsLunaTransport:
    """``LunaTransport`` over DeepSeek's OpenAI-compatible chat-completions API.

    Never constructed by offline tests with a live client; tests inject a
    scripted ``HttpClient`` fake.  The only mutable state is the lock-guarded
    ``attempts`` receipt log, so one instance is safe for the runner's full
    worker pool.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        environ: Mapping[str, str] | None = None,
        endpoint: str = CHAT_COMPLETIONS_URL,
        request_model: str = REQUEST_MODEL_ID,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        retry_backoff_s: float = DEFAULT_RETRY_BACKOFF_S,
        max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
        http_client: HttpClient | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        environment = os.environ if environ is None else environ
        key = api_key if api_key is not None else environment.get(KEY_ENVIRONMENT_VARIABLE)
        if not isinstance(key, str) or _KEY_RE.fullmatch(key) is None:
            raise PermissionError("deepseek_api_key_missing_or_invalid")
        if not 1 <= max_attempts <= MAX_ATTEMPTS:
            raise ValueError("deepseek_max_attempts_invalid")
        if retry_backoff_s < 0:
            raise ValueError("deepseek_retry_backoff_invalid")
        if not 1 <= max_output_tokens <= 65_536:
            raise ValueError("deepseek_max_output_tokens_invalid")
        self._api_key = key
        self._endpoint = endpoint
        self._request_model = request_model
        self._max_attempts = max_attempts
        self._retry_backoff_s = retry_backoff_s
        self._max_output_tokens = max_output_tokens
        self._client = http_client if http_client is not None else UrllibChatCompletionsClient(endpoint=endpoint)
        self._sleeper = sleeper
        self.attempts: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # LunaTransport entrypoint.
    # ------------------------------------------------------------------

    def complete(self, *, payload: Mapping[str, Any], timeout_ms: int) -> Mapping[str, Any]:
        """Run one translated chat-completions call with bounded retries."""

        body = self._translate(payload)
        metadata = payload.get("metadata")
        candidate_ref = metadata.get("candidate_ref") if isinstance(metadata, Mapping) else None
        encoded = json.dumps(body).encode("utf-8")
        for attempt in range(1, self._max_attempts + 1):
            response: HttpResponse | None = None
            error_code: str | None = None
            try:
                response = self._client.request(
                    method="POST",
                    url=self._endpoint,
                    headers={
                        "Authorization": f"Bearer {self._api_key}",
                        "Content-Type": "application/json",
                    },
                    body=encoded,
                    timeout_ms=timeout_ms,
                    max_response_bytes=MAX_RESPONSE_BYTES,
                )
            except RuntimeError as exc:
                error_code = (
                    exc.args[0] if exc.args and isinstance(exc.args[0], str) else "deepseek_http_transport_failed"
                )
            if response is not None and response.status_code == 200:
                try:
                    envelope = self._decode_envelope(response.body)
                except LunaBatchRunnerError as exc:
                    code = exc.args[0] if exc.args and isinstance(exc.args[0], str) else "luna_response_invalid"
                    self._record(candidate_ref, attempt, 200, "failed", code)
                    raise
                if envelope is not None:
                    self._record(candidate_ref, attempt, 200, "completed", None)
                    return envelope
                error_code = "luna_response_json_invalid"
            elif response is not None:
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    code = f"luna_http_status_rejected:{response.status_code}"
                    self._record(candidate_ref, attempt, response.status_code, "failed", code)
                    raise LunaBatchRunnerError(code)
                error_code = f"luna_http_status_retryable:{response.status_code}"
            status_code = response.status_code if response is not None else None
            outcome = "retry_scheduled" if attempt < self._max_attempts else "failed"
            self._record(candidate_ref, attempt, status_code, outcome, error_code)
            if attempt < self._max_attempts:
                self._sleeper(self._retry_backoff_s * attempt)
        raise LunaBatchRunnerError("luna_transport_retry_exhausted")

    # ------------------------------------------------------------------
    # Translation + envelope (byte-identical to the proven ad-hoc driver).
    # ------------------------------------------------------------------

    def _translate(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        try:
            instructions = payload["instructions"]
            user_text = payload["input"][0]["content"][0]["text"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LunaBatchRunnerError("luna_request_payload_invalid") from exc
        if not isinstance(instructions, str) or not isinstance(user_text, str):
            raise LunaBatchRunnerError("luna_request_payload_invalid")
        system = (
            instructions
            + "\n\n# Output contract\n"
            "Respond with exactly one JSON object conforming to this JSON Schema. "
            "No prose, no markdown fences, no extra keys:\n"
            + json.dumps(lbr.judged_output_schema(), ensure_ascii=False)
        )
        return {
            "model": self._request_model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user_text},
            ],
            "response_format": {"type": "json_object"},
            "max_tokens": self._max_output_tokens,
        }

    def _decode_envelope(self, body: bytes) -> dict[str, Any] | None:
        """Decode a 200 body; ``None`` marks a retryable undecodable body.

        A well-formed body that violates the one-choice chat envelope is a
        deterministic provider-contract violation and raises instead of
        retrying: replaying the same payload cannot fix the shape.
        """

        try:
            text = body.decode("utf-8")
            parsed = lbr._strict_json_loads(text)
        except (UnicodeError, ValueError, RecursionError):
            return None
        if not isinstance(parsed, dict):
            return None
        choices = parsed.get("choices")
        if not isinstance(choices, list) or len(choices) != 1 or not isinstance(choices[0], dict):
            raise LunaBatchRunnerError("luna_response_invalid")
        message = choices[0].get("message")
        if not isinstance(message, dict):
            raise LunaBatchRunnerError("luna_response_invalid")
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise LunaBatchRunnerError("luna_response_invalid")
        return {
            "status": "completed",
            "model": parsed.get("model"),
            "output": [{"type": "message", "content": [{"type": "output_text", "text": content}]}],
        }

    def _record(
        self,
        candidate_ref: Any,
        attempt: int,
        status_code: int | None,
        outcome: str,
        error_code: str | None,
    ) -> None:
        with self._lock:
            self.attempts.append(
                {
                    "candidate_ref": candidate_ref if isinstance(candidate_ref, str) else None,
                    "attempt": attempt,
                    "status_code": status_code,
                    "outcome": outcome,
                    "error_code": error_code,
                }
            )
