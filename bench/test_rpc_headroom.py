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


def result(rate, rx_capacity, rx_exhausted=0, hard_full=0,
           control_capacity=128, control_exhausted=0,
           executor_queue=64, signals=None):
    server = {key: 0 for key in PRESSURE_KEYS}
    server.update(
        rx_pool_capacity=rx_capacity,
        rx_pool_exhausted_events=rx_exhausted,
        rpc_hard_full_events=hard_full,
        control_tx_pool_capacity=control_capacity,
        control_tx_pool_exhausted_events=control_exhausted,
    )
    return {
        "case": {"rate_rps": rate},
        "server": {"executor_queue": executor_queue},
        "server_exit": server,
        "wall_signals": list(signals or []),
    }


class HeadroomTests(unittest.TestCase):
    def test_server_resource_args_are_server_only_overrides(self):
        case = {
            "rx_buffer_count": 1024,
            "rpc_message_pool_count": 512,
            "reassembly_pool_count": 256,
            "control_tx_item_count": 512,
        }
        self.assertEqual(
            server_resource_args(case),
            ["--rx-buffers", "1024",
             "--rpc-message-pool", "512",
             "--reassembly-pool", "256",
             "--control-tx-items", "512"])

    def test_ready_override_validation(self):
        case = {
            "rx_buffer_count": 1024,
            "rpc_message_pool_count": 0,
            "reassembly_pool_count": 0,
            "control_tx_item_count": 512,
        }
        ready = {
            "rx_buffers": 1024,
            "rpc_message_pool": 272,
            "reassembly_pool": 136,
            "control_tx_items": 512,
        }
        validate_server_ready_resources(ready, case)
        ready["rx_buffers"] = 272
        with self.assertRaises(ValueError):
            validate_server_ready_resources(ready, case)

    def test_compare_reports_removed_rx_exhaustion(self):
        baseline = [
            result(10000, 272),
            result(20000, 272, rx_exhausted=3,
                   signals=["rx_pool_exhausted"]),
            result(40000, 272, rx_exhausted=8,
                   signals=["rx_pool_exhausted"]),
        ]
        headroom = [
            result(10000, 1024),
            result(20000, 1024),
            result(40000, 1024, rx_exhausted=2,
                   signals=["rx_pool_exhausted"]),
        ]
        comparison = compare_profiles(
            baseline, headroom, "baseline", "rx_headroom")
        self.assertEqual(comparison["before_rx_capacity"], 272)
        self.assertEqual(comparison["after_rx_capacity"], 1024)
        self.assertEqual(comparison["before_executor_queue"], 64)
        self.assertEqual(comparison["after_executor_queue"], 64)
        self.assertEqual(comparison["before_first_server_pressure_rps"], 20000)
        self.assertEqual(comparison["after_first_server_pressure_rps"], 40000)
        self.assertEqual(comparison["rx_exhaustion_removed_rates"], [20000])
        self.assertEqual(comparison["rx_exhaustion_persisted_rates"], [40000])
        self.assertIn("20000", comparison["server_signal_changes"])

    def test_compare_reports_removed_executor_hard_full(self):
        before = [
            result(10000, 1024),
            result(20000, 1024, hard_full=12,
                   executor_queue=64,
                   signals=["executor_hard_full"]),
        ]
        after = [
            result(10000, 1024, executor_queue=256),
            result(20000, 1024, executor_queue=256),
        ]
        comparison = compare_profiles(
            before, after, "rx_headroom", "rx_executor_headroom")
        self.assertEqual(comparison["before_executor_queue"], 64)
        self.assertEqual(comparison["after_executor_queue"], 256)
        self.assertEqual(
            comparison["executor_hard_full_removed_rates"], [20000])
        self.assertEqual(
            comparison["executor_hard_full_persisted_rates"], [])

    def test_compare_reports_removed_control_tx_exhaustion(self):
        before = [
            result(20000, 1024, control_capacity=128),
            result(40000, 1024, control_capacity=128,
                   control_exhausted=9,
                   signals=["control_tx_pool_exhausted"]),
        ]
        after = [
            result(20000, 1024, control_capacity=512),
            result(40000, 1024, control_capacity=512),
        ]
        comparison = compare_profiles(
            before, after,
            "rx_executor_headroom", "rx_executor_control_headroom")
        self.assertEqual(comparison["before_control_tx_capacity"], 128)
        self.assertEqual(comparison["after_control_tx_capacity"], 512)
        self.assertEqual(
            comparison["control_tx_exhaustion_removed_rates"], [40000])
        self.assertEqual(
            comparison["control_tx_exhaustion_persisted_rates"], [])

    def test_compare_requires_matching_rates(self):
        with self.assertRaises(ValueError):
            compare_profiles(
                [result(10000, 272)],
                [result(20000, 1024)],
                "before", "after")


if __name__ == "__main__":
    unittest.main()
