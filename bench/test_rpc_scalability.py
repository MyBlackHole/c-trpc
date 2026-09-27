"""Accounting and attribution tests for the scalability matrix."""
import unittest

from run_rpc_scalability import (
    exact_wall_signals,
    rates_for,
    summarize_group,
)


def result(rate=100, workers=1, handler_ms=10):
    client = dict(
        scheduler_dropped=0, submit_again=0, submit_errors=0,
        deadlines=0, rpc_errors=0, unavailable=0,
        resource_exhausted=0, invalid_responses=0,
        ok=32, ok_rps=float(rate),
    )
    server = dict(
        reactor_busy_ratio=0.1,
        rpc_queue_wait_p99_us=10.0,
        rpc_handler_p99_us=10000.0,
        rpc_admission_limit_hits=0,
        rpc_hard_full_events=0,
        command_queue_full_events=0,
        completion_queue_full_events=0,
        rx_pool_exhausted_events=0,
        tx_pool_exhausted_events=0,
        control_tx_pool_exhausted_events=0,
        rpc_message_pool_exhausted_events=0,
        reassembly_pool_exhausted_events=0,
    )
    return dict(
        type="scalability_case", schema=1,
        case=dict(trial=1, rate_rps=rate, workers=workers,
                  slow_ms=handler_ms, executor_queue=64, window=128),
        client=client, server_exit=server,
    )


class ScalabilityAttributionTests(unittest.TestCase):
    def test_rates_for_nonzero_handler_track_nominal_capacity(self):
        self.assertEqual(
            rates_for(2, 10, [1000], [0.5, 1.0, 2.0, 4.0]),
            [100, 200, 400, 800])

    def test_rates_for_zero_handler_are_explicit(self):
        self.assertEqual(
            rates_for(8, 0, [20000, 1000, 5000, 5000], [0.5, 4.0]),
            [1000, 5000, 20000])

    def test_exact_wall_signals_distinguish_generator_and_server(self):
        r = result()
        r["client"]["scheduler_dropped"] = 1
        r["client"]["resource_exhausted"] = 2
        r["server_exit"]["rpc_hard_full_events"] = 2
        r["server_exit"]["rx_pool_exhausted_events"] = 1
        self.assertEqual(
            exact_wall_signals(r),
            ["load_generator_drop", "rpc_resource_exhausted",
             "executor_hard_full", "rx_pool_exhausted"])

    def test_summary_keeps_clean_and_pressure_boundaries_separate(self):
        clean = result(100)
        queued = result(200)
        queued["server_exit"]["rpc_queue_wait_p99_us"] = 8000.0
        # Queueing without an exact exhaustion event remains evidence, not a wall.
        pressure = result(400)
        pressure["client"]["rpc_errors"] = 1
        pressure["client"]["resource_exhausted"] = 1
        pressure["server_exit"]["rpc_hard_full_events"] = 1
        pressure["server_exit"]["reactor_busy_ratio"] = 0.04
        pressure["server_exit"]["rpc_queue_wait_p99_us"] = 134000.0

        summary = summarize_group([pressure, clean, queued])
        self.assertEqual(summary["max_clean_offered_rps"], 200)
        self.assertEqual(summary["first_server_pressure_rps"], 400)
        self.assertIsNone(summary["first_load_generator_drop_rps"])
        self.assertEqual(summary["nominal_handler_capacity_rps"], 100.0)
        self.assertEqual(summary["max_clean_vs_nominal"], 2.0)
        self.assertIn("executor_hard_full", summary["wall_signals"])
        self.assertIn("rpc_resource_exhausted", summary["wall_signals"])

    def test_zero_delay_summary_has_no_nominal_capacity(self):
        r = result(5000, workers=4, handler_ms=0)
        summary = summarize_group([r])
        self.assertIsNone(summary["nominal_handler_capacity_rps"])
        self.assertIsNone(summary["max_clean_vs_nominal"])
        self.assertEqual(summary["max_clean_offered_rps"], 5000)


if __name__ == "__main__":
    unittest.main()
