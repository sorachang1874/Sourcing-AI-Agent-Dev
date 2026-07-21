from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import check_grok_transport_health  # noqa: E402

from x_first.grok_transport_health import (  # noqa: E402
    STATUS_BLOCKED_BACKEND,
    STATUS_BLOCKED_UNKNOWN_MODEL,
    STATUS_DEGRADED,
    STATUS_READY,
    evaluate_grok_transport_health,
    parse_grok_models_output,
    sealed_predecessor_model_is_adoptable,
)


class GrokTransportHealthTest(unittest.TestCase):
    def test_parses_default_model_after_fetch_warnings(self) -> None:
        output = """
You are logged in with grok.com.
WARN Failed to fetch models: Network(reqwest::Error { source: TimedOut })
WARN Settings fetch failed after 3 attempts
Default model: grok-build

Available models:
  * grok-build (default)
"""

        listing = parse_grok_models_output(output)

        self.assertEqual(listing.default_model, "grok-build")
        self.assertEqual(listing.available_models, ("grok-build",))
        self.assertTrue(listing.model_fetch_network_failed)
        self.assertTrue(listing.settings_fetch_network_failed)

    def test_unknown_requested_model_blocks_before_large_wave(self) -> None:
        health = evaluate_grok_transport_health(
            requested_model="grok-4.5",
            models_output="Default model: grok-build\n\nAvailable models:\n  * grok-build (default)\n",
            canary_exit_code=1,
            canary_output='Error: Couldn\'t set model "grok-4.5": Invalid params: "unknown model id".',
        )

        self.assertEqual(health.status, STATUS_BLOCKED_UNKNOWN_MODEL)
        self.assertEqual(health.selected_model, "grok-build")
        self.assertTrue(health.stale_requested_model)
        self.assertFalse(health.large_wave_allowed)
        self.assertIn("requested_model_rejected_by_cli", health.diagnostics)

    def test_backend_timeout_requires_single_call_canary_not_large_wave(self) -> None:
        health = evaluate_grok_transport_health(
            requested_model="grok-build",
            models_output=(
                "WARN Settings fetch network error: error sending request for url "
                "(https://cli-chat-proxy.grok.com/v1/settings)\n"
                "Default model: grok-build\n\nAvailable models:\n  * grok-build (default)\n"
            ),
        )

        self.assertEqual(health.status, STATUS_BLOCKED_BACKEND)
        self.assertTrue(health.native_x_canary_required)
        self.assertFalse(health.large_wave_allowed)

    def test_successful_canary_allows_large_wave(self) -> None:
        health = evaluate_grok_transport_health(
            requested_model="grok-build",
            models_output="Default model: grok-build\n\nAvailable models:\n  * grok-build (default)\n",
            canary_exit_code=0,
            canary_output='{"native_x_used":true}',
        )

        self.assertEqual(health.status, STATUS_READY)
        self.assertFalse(health.native_x_canary_required)
        self.assertTrue(health.large_wave_allowed)

    def test_failed_canary_degrades_even_when_model_listing_is_current(self) -> None:
        health = evaluate_grok_transport_health(
            requested_model="grok-build",
            models_output="Default model: grok-build\n\nAvailable models:\n  * grok-build (default)\n",
            canary_exit_code=130,
            canary_output="interrupted",
        )

        self.assertEqual(health.status, STATUS_DEGRADED)
        self.assertFalse(health.large_wave_allowed)
        self.assertIn("native_x_canary_process_failed", health.diagnostics)

    def test_sealed_predecessor_model_can_differ_from_local_model(self) -> None:
        self.assertTrue(
            sealed_predecessor_model_is_adoptable(
                predecessor_model_id="grok-4.5",
                local_model_id="grok-build",
                predecessor_replay_sealed=True,
            )
        )
        self.assertFalse(
            sealed_predecessor_model_is_adoptable(
                predecessor_model_id="grok-4.5",
                local_model_id="grok-build",
                predecessor_replay_sealed=False,
            )
        )

    def test_cli_reads_models_output_file_and_returns_two_when_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            models_output = Path(directory) / "models.txt"
            models_output.write_text(
                "WARN Settings fetch failed after 3 attempts\n"
                "Default model: grok-build\n\nAvailable models:\n  * grok-build (default)\n",
                encoding="utf-8",
            )
            with mock.patch("sys.stdout", new_callable=lambda: _StringSink()) as stdout:
                exit_code = check_grok_transport_health.main(
                    ["--requested-model", "grok-build", "--models-output", str(models_output)]
                )

        self.assertEqual(exit_code, 2)
        self.assertIn('"status":"blocked_backend_unstable"', stdout.value)
        self.assertIn('"large_wave_allowed":false', stdout.value)

    def test_cli_accepts_canary_success_and_returns_zero(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            models_output = Path(directory) / "models.txt"
            canary_output = Path(directory) / "canary.txt"
            models_output.write_text(
                "Default model: grok-build\n\nAvailable models:\n  * grok-build (default)\n",
                encoding="utf-8",
            )
            canary_output.write_text('{"native_x_used":true}\n', encoding="utf-8")
            with mock.patch("sys.stdout", new_callable=lambda: _StringSink()) as stdout:
                exit_code = check_grok_transport_health.main(
                    [
                        "--requested-model",
                        "grok-build",
                        "--models-output",
                        str(models_output),
                        "--canary-exit-code",
                        "0",
                        "--canary-output",
                        str(canary_output),
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertIn('"status":"ready_for_native_x_canary"', stdout.value)
        self.assertIn('"large_wave_allowed":true', stdout.value)

    def test_cli_can_run_grok_models_without_native_x_canary(self) -> None:
        completed = mock.Mock(stdout="Default model: grok-build\n", stderr="", returncode=0)
        with (
            mock.patch("check_grok_transport_health.subprocess.run", return_value=completed) as run,
            mock.patch("sys.stdout", new_callable=lambda: _StringSink()) as stdout,
        ):
            exit_code = check_grok_transport_health.main(["--requested-model", "grok-build", "--run-grok-models"])

        self.assertEqual(exit_code, 2)
        run.assert_called_once()
        self.assertEqual(run.call_args.args[0], ["grok", "models"])
        self.assertIn('"native_x_canary_required":true', stdout.value)


class _StringSink:
    def __init__(self) -> None:
        self.value = ""

    def write(self, value: str) -> int:
        self.value += value
        return len(value)

    def flush(self) -> None:
        return None


if __name__ == "__main__":
    unittest.main()
