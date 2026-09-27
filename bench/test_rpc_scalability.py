"""Accounting and attribution tests for the scalability matrix."""
import unittest

from run_rpc_scalability import (
    aggregate_client_rows,
    exact_wall_signals,
    generator_count_for,
    rates_for,
    split_rates,
    split_weighted_total,
    summarize_group,
)


def result(rate=100, workers=1, handler_ms=10):
    client = dict(
        scheduler_dropped=0, submit_again=0, submit_errors=0,
        deadlines=0, rpc_errors=0, unavailable=0,
        resource_exhausted=0, invalid_responses=0,
        scheduler_late_p99_us=10.0,
        ok=32, ok_rps=float(rate),
    )
    server = dict(
        reactor_busy_ratio=0.1,
        rpc_queue_wait_p99_us=10.0,
        rpc_handler_p99_us=10000.0,
        rpc_queue_peak_per_peer=1,
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

    def test_generator_fanout_and_rate_partition(self):
        self.assertEqual(generator_count_for(3999, 16, 4000), 1)
        self.assertEqual(generator_count_for(4001, 16, 4000), 2)
        self.assertEqual(generator_count_for(100000, 16, 4000), 16)
        self.assertEqual(generator_count_for(40000, 32, 1250), 32)
        rates = split_rates(10001, 3)
        self.assertEqual(sum(rates), 10001)
        self.assertLessEqual(max(rates) - min(rates), 1)

    def test_weighted_request_partition_preserves_total(self):
        counts = split_weighted_total(257, [3, 3, 4])
        self.assertEqual(sum(counts), 257)
        self.assertTrue(all(count > 0 for count in counts))

    def test_aggregate_client_rows_preserves_accounting(self):
        rows = []
        for index, rate in enumerate((2000, 3000)):
            rows.append(dict(
                start_ns=1_000_000_000 + index * 100,
                end_ns=2_000_000_000 + index * 100,
                offered=100, scheduler_dropped=0, attempted=100,
                accepted=100, completed=100, ok=100,
                submit_again=0, submit_errors=0, deadlines=0,
                rpc_errors=0, unavailable=0, resource_exhausted=0,
                invalid_responses=0, scheduler_late_p50_us=10.0,
                scheduler_late_p99_us=20.0 + index,
                scheduler_late_max_us=30.0 + index,
                client_cpu_s=0.1, client_peak_rss_kib=1000))
        aggregate = aggregate_client_rows(rows, [2000, 3000])
        self.assertEqual(aggregate["generator_count"], 2)
        self.assertEqual(aggregate["offered"], 200)
        self.assertEqual(aggregate["ok"], 200)
        self.assertEqual(aggregate["generator_rates_rps"], [2000, 3000])
        self.assertAlmostEqual(aggregate["scheduler_interval_us"], 1000000.0 / 3000)
        self.assertEqual(aggregate["scheduler_late_p99_us"], 21.0)

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
        self.assertEqual(summary["max_clean_scheduled_rps"], 200)
        self.assertEqual(summary["first_server_pressure_rps"], 400)
        self.assertIsNone(summary["first_load_generator_drop_rps"])
        self.assertEqual(summary["nominal_handler_capacity_rps"], 100.0)
        self.assertEqual(summary["max_clean_vs_nominal"], 2.0)
        self.assertIn("executor_hard_full", summary["wall_signals"])
        self.assertIn("rpc_resource_exhausted", summary["wall_signals"])

    def test_generator_lateness_is_not_server_pressure(self):
        r = result(10000, workers=4, handler_ms=0)
        r["client"]["scheduler_late_p99_us"] = 500.0
        summary = summarize_group([r])
        self.assertIn("load_generator_late", summary["wall_signals"])
        self.assertEqual(summary["first_load_generator_late_rps"], 10000)
        self.assertIsNone(summary["first_server_pressure_rps"])
        self.assertIsNone(summary["max_clean_scheduled_rps"])

    def test_zero_delay_summary_has_no_nominal_capacity(self):
        r = result(5000, workers=4, handler_ms=0)
        summary = summarize_group([r])
        self.assertIsNone(summary["nominal_handler_capacity_rps"])
        self.assertIsNone(summary["max_clean_vs_nominal"])
        self.assertEqual(summary["max_clean_scheduled_rps"], 5000)


if __name__ == "__main__":
    unittest.main()
