"""Tests for repeated V2 load and exact Server wall classification."""
import unittest
from pathlib import Path
import subprocess
import sys

from run_rpc_repeatability import PROFILE_SPECS, summarize
from run_rpc_scalability import SERVER_PRESSURE_FIELDS


def result(ok_rps=39000.0, signals=None,
           command_hits=0, command_peak=45, tls=False, compression="none"):
    signal_list = list(signals or [])
    server = {key: 0 for key in SERVER_PRESSURE_FIELDS.values()}
    server.update(command_budget_hits=command_hits,
                  command_queue_peak=command_peak,
                  rpc_executor_threads=4,
                  rpc_task_queue_capacity=256,
                  rpc_calls_started=10,
                  rpc_calls_finished=10)
    client = dict(
        scheduler_dropped=0, scheduler_late_p99_us=100.0,
        scheduler_interval_us=800.0, submit_again=0, submit_errors=0,
        deadlines=0, rpc_errors=0, unavailable=0, resource_exhausted=0,
        invalid_responses=0, ok_rps=ok_rps,
    )
    if "reactor_command_queue_full" in signal_list:
        server["command_queue_full_events"] = 1
    if "rx_pool_exhausted" in signal_list:
        server["rx_pool_exhausted_events"] = 1
    if "load_generator_late" in signal_list:
        client["scheduler_late_p99_us"] = 1000.0
    return {
        "schema": 2, "api_generation": "rpc_v2",
        "case": {"rate_rps": 40000, "tls": tls,
                 "compression": compression}, "server_exit": server,
        "client": client, "wall_signals": signal_list,
    }


class RepeatabilityTests(unittest.TestCase):
    def test_cli_exposes_tls_and_compression_modes(self):
        script = Path(__file__).with_name("run_rpc_repeatability.py")
        completed = subprocess.run(
            [sys.executable, str(script), "--help"],
            check=False, capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--compression", completed.stdout)
        self.assertIn("--tls-ca", completed.stdout)
        self.assertIn("--tls-cert", completed.stdout)
        self.assertIn("--tls-key", completed.stdout)

    def test_cli_rejects_tls_without_all_credentials(self):
        script = Path(__file__).with_name("run_rpc_repeatability.py")
        output = (Path(__file__).resolve().parents[1] / "build" / "test-tmp" /
                  "repeatability-invalid-tls.jsonl")
        output.unlink(missing_ok=True)
        completed = subprocess.run(
            [sys.executable, str(script), "--binary", sys.executable,
             "--output", str(output), "--tls"],
            check=False, capture_output=True, text=True)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("--tls requires --tls-ca, --tls-cert, and --tls-key",
                      completed.stderr)
        self.assertFalse(output.exists())

    def test_profiles_compare_only_public_call_limits(self):
        small = PROFILE_SPECS["small_call_limit"]
        large = PROFILE_SPECS["large_call_limit"]
        self.assertEqual(small, {"capacity": 32, "bulk_bytes": 16384})
        self.assertEqual(large, {"capacity": 256, "bulk_bytes": 65536})

    def test_summary_counts_recurrent_and_transient_walls(self):
        rows = [
            result(39500.0),
            result(2100.0, ["reactor_command_queue_full", "load_generator_late"],
                   command_hits=50, command_peak=4096),
            result(39200.0),
        ]
        summary = summarize("default_call_limit", rows)
        self.assertEqual(summary["schema"], 2)
        self.assertEqual(summary["api_generation"], "rpc_v2")
        self.assertEqual(summary["trials"], 3)
        self.assertEqual(summary["clean_trials"], 2)
        self.assertEqual(summary["server_pressure_trials"], 1)
        self.assertEqual(summary["classification"],
                         "non_reproducible_server_wall")
        self.assertEqual(summary["reproducible_server_signals"], [])
        self.assertEqual(summary["sporadic_server_signals"],
                         ["reactor_command_queue_full"])
        self.assertEqual(summary["server_signal_counts"],
                         {"reactor_command_queue_full": 1})
        self.assertEqual(summary["signal_counts"]["reactor_command_queue_full"], 1)
        self.assertEqual(summary["signal_counts"]["load_generator_late"], 1)
        self.assertEqual(summary["ok_rps_median"], 39200.0)
        self.assertEqual(summary["command_budget_hits_max"], 50)
        self.assertEqual(summary["command_queue_peak_max"], 4096)

    def test_summary_clean_trials_have_no_pressure(self):
        rows = [result(), result(39800.0), result(39300.0)]
        summary = summarize("large_call_limit", rows)
        self.assertEqual(summary["clean_trials"], 3)
        self.assertEqual(summary["server_pressure_trials"], 0)
        self.assertEqual(summary["classification"], "no_server_wall")
        self.assertEqual(summary["server_signal_counts"], {})
        self.assertEqual(summary["reproducible_server_signals"], [])
        self.assertEqual(summary["sporadic_server_signals"], [])
        self.assertEqual(summary["signal_counts"], {})

    def test_summary_preserves_tls_and_compression_configuration(self):
        summary = summarize("default_call_limit", [
            result(tls=True, compression="zstd")])
        self.assertTrue(summary.get("tls_enabled", False))
        self.assertEqual(summary.get("compression"), "zstd")

    def test_summary_marks_recurrent_server_wall(self):
        rows = [
            result(2500.0, ["reactor_command_queue_full"], 40, 4096),
            result(2600.0, ["reactor_command_queue_full"], 45, 4096),
            result(2400.0, ["reactor_command_queue_full"], 50, 4096),
        ]
        summary = summarize("small_call_limit", rows)
        self.assertEqual(summary["server_pressure_trials"], 3)
        self.assertEqual(summary["classification"], "reproducible_server_wall")
        self.assertEqual(summary["server_signal_counts"],
                         {"reactor_command_queue_full": 3})
        self.assertEqual(summary["reproducible_server_signals"],
                         ["reactor_command_queue_full"])
        self.assertEqual(summary["sporadic_server_signals"], [])


if __name__ == "__main__":
    unittest.main()
