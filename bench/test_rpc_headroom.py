"""Tests for bounded-resource headroom A/B attribution."""
import unittest

from run_rpc_capacity import (
    server_resource_args,
    validate_server_ready_resources,
)
from run_rpc_headroom import compare_profiles


PRESSURE_KEYS = (
    "rpc_admission_limit_hits", "rpc_hard_full_events",
    "command_queue_full_events", "completion_queue_full_events",
    "rx_pool_exhausted_events", "tx_pool_exhausted_events",
    "control_tx_pool_exhausted_events",
    "rpc_message_pool_exhausted_events",
    "reassembly_pool_exhausted_events",
)


def result(rate, rx_capacity, rx_exhausted=0, signals=None):
    server = {key: 0 for key in PRESSURE_KEYS}
    server.update(
        rx_pool_capacity=rx_capacity,
        rx_pool_exhausted_events=rx_exhausted,
    )
    return {
        "case": {"rate_rps": rate},
        "server_exit": server,
        "wall_signals": list(signals or []),
    }


class HeadroomTests(unittest.TestCase):
    def test_server_resource_args_are_server_only_overrides(self):
        case = {
            "rx_buffer_count": 1024,
            "rpc_message_pool_count": 512,
            "reassembly_pool_count": 256,
        }
        self.assertEqual(
            server_resource_args(case),
            ["--rx-buffers", "1024",
             "--rpc-message-pool", "512",
             "--reassembly-pool", "256"])

    def test_ready_override_validation(self):
        case = {
            "rx_buffer_count": 1024,
            "rpc_message_pool_count": 0,
            "reassembly_pool_count": 0,
        }
        ready = {
            "rx_buffers": 1024,
            "rpc_message_pool": 272,
            "reassembly_pool": 136,
        }
        validate_server_ready_resources(ready, case)
        ready["rx_buffers"] = 272
        with self.assertRaises(ValueError):
            validate_server_ready_resources(ready, case)

    def test_compare_reports_removed_rx_exhaustion(self):
        baseline = [
            result(10000, 272),
            result(20000, 272, 3, ["rx_pool_exhausted"]),
            result(40000, 272, 8, ["rx_pool_exhausted"]),
        ]
        headroom = [
            result(10000, 1024),
            result(20000, 1024),
            result(40000, 1024, 2, ["rx_pool_exhausted"]),
        ]
        comparison = compare_profiles(baseline, headroom)
        self.assertEqual(comparison["baseline_rx_capacity"], 272)
        self.assertEqual(comparison["headroom_rx_capacity"], 1024)
        self.assertEqual(comparison["baseline_first_server_pressure_rps"], 20000)
        self.assertEqual(comparison["headroom_first_server_pressure_rps"], 40000)
        self.assertEqual(comparison["rx_exhaustion_removed_rates"], [20000])
        self.assertEqual(comparison["rx_exhaustion_persisted_rates"], [40000])
        self.assertIn("20000", comparison["server_signal_changes"])

    def test_compare_requires_matching_rates(self):
        with self.assertRaises(ValueError):
            compare_profiles(
                [result(10000, 272)],
                [result(20000, 1024)])


if __name__ == "__main__":
    unittest.main()
