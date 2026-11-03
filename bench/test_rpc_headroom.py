"""Tests for public V2 limit headroom comparisons."""
import unittest

from run_rpc_headroom import compare_profiles
from run_rpc_scalability import SERVER_PRESSURE_FIELDS


def result(rate, capacity, bulk_bytes, *, ok=10, calls=10, pressure=None):
    server = {key: 0 for key in SERVER_PRESSURE_FIELDS.values()}
    if pressure:
        server[pressure] = 1
    server["rpc_calls_started"] = calls
    return {
        "schema": 2, "api_generation": "rpc_v2",
        "case": {"rate_rps": rate, "capacity": capacity,
                 "bulk_bytes": bulk_bytes},
        "server": {}, "server_exit": server,
        "client": {"ok": ok}, "wall_signals": ([pressure] if pressure else []),
    }


class HeadroomTests(unittest.TestCase):
    def test_public_capacity_and_message_limit_are_reported(self):
        baseline = [result(100, 32, 16384), result(200, 32, 16384)]
        headroom = [result(100, 64, 16384), result(200, 64, 16384)]
        comparison = compare_profiles(
            baseline, headroom, "small_limits", "larger_call_limit")
        self.assertEqual(comparison["schema"], 2)
        self.assertEqual(comparison["api_generation"], "rpc_v2")
        self.assertEqual(comparison["before_capacity"], 32)
        self.assertEqual(comparison["after_capacity"], 64)
        self.assertEqual(comparison["before_bulk_bytes"], 16384)
        self.assertEqual(comparison["after_bulk_bytes"], 16384)
        self.assertNotIn("executor_queue", comparison)
        self.assertNotIn("rpc_message_pool", comparison)

    def test_actual_v2_pressure_changes_are_attributed(self):
        baseline = [result(100, 32, 16384),
                    result(200, 32, 16384,
                           pressure="reassembly_pool_exhausted_events")]
        headroom = [result(100, 32, 65536), result(200, 32, 65536)]
        comparison = compare_profiles(
            baseline, headroom, "small_limits", "larger_message_limit")
        self.assertEqual(comparison["before_first_server_pressure_rps"], 200)
        self.assertIsNone(comparison["after_first_server_pressure_rps"])
        self.assertIn("200", comparison["server_signal_changes"])

    def test_profiles_require_matching_rates(self):
        with self.assertRaises(ValueError):
            compare_profiles([result(100, 32, 16384)],
                             [result(200, 64, 16384)], "before", "after")


if __name__ == "__main__":
    unittest.main()
