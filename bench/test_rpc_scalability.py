"""Tests for public-limit V2 scalability accounting and signal attribution."""
import copy
import unittest

from run_rpc_scalability import (
    aggregate_client_rows,
    clean_result,
    exact_wall_signals,
    generator_count_for,
    split_rates,
    split_weighted_total,
    summarize_group,
)


def client_row(rate=100, requests=100):
    return dict(
        schema=2, api_generation="rpc_v2", type="open_phase",
        offered=requests, scheduler_dropped=0, attempted=requests,
        accepted=requests, completed=requests, ok=requests,
        submit_again=0, submit_errors=0, deadlines=0, rpc_errors=0,
        unavailable=0, resource_exhausted=0, invalid_responses=0,
        start_ns=1_000_000_000, end_ns=2_000_000_000,
        drain_elapsed_s=1.0, ok_rps=float(requests),
        scheduler_interval_us=1_000_000.0 / rate,
        scheduler_late_p50_us=1.0, scheduler_late_p99_us=2.0,
        scheduler_late_max_us=3.0, client_cpu_s=0.1,
        client_peak_rss_kib=1024,
    )


def result(rate=100, capacity=64, slow_ms=10, generators=1):
    server = dict(
        command_queue_full_events=0, completion_queue_full_events=0,
        rx_pool_exhausted_events=0, tx_pool_exhausted_events=0,
        control_tx_pool_exhausted_events=0,
        reassembly_pool_exhausted_events=0,
    )
    client = client_row(rate)
    return {
        "type": "scalability_case", "schema": 2,
        "api_generation": "rpc_v2",
        "case": {"trial": 1, "capacity": capacity, "slow_ms": slow_ms,
                 "window": 16, "rate_rps": rate, "generators": generators},
        "server_exit": server, "client": client,
    }


class ScalabilityTests(unittest.TestCase):
    def test_generator_split_preserves_rate_and_budget(self):
        rates = split_rates(101, 3)
        budgets = split_weighted_total(64, rates)
        self.assertEqual(sum(rates), 101)
        self.assertEqual(sum(budgets), 64)
        self.assertEqual(generator_count_for(900, 4, 300), 3)

    def test_client_rows_keep_full_accounting(self):
        rows = [client_row(50, 40), client_row(50, 40)]
        rows[1]["start_ns"] += 10_000_000
        rows[1]["end_ns"] += 10_000_000
        aggregate = aggregate_client_rows(rows, [50, 50])
        self.assertEqual(aggregate["offered"], 80)
        self.assertEqual(aggregate["attempted"], 80)
        self.assertEqual(aggregate["completed"], 80)
        self.assertEqual(aggregate["ok"], 80)
        self.assertEqual(aggregate["generator_count"], 2)

    def test_clean_and_pressure_signals(self):
        item = result()
        self.assertTrue(clean_result(item))
        item["server_exit"]["reassembly_pool_exhausted_events"] = 1
        self.assertFalse(clean_result(item))
        self.assertEqual(exact_wall_signals(item), ["reassembly_pool_exhausted"])

    def test_generator_drop_and_rpc_error_are_distinct(self):
        item = result()
        item["client"].update(scheduler_dropped=1, offered=101,
                              scheduler_interval_us=10_000.0)
        self.assertIn("load_generator_drop", exact_wall_signals(item))
        item["client"].update(scheduler_dropped=0, offered=100,
                              resource_exhausted=1, rpc_errors=1)
        self.assertIn("rpc_resource_exhausted", exact_wall_signals(item))

    def test_summary_tracks_public_limit_dimensions_only(self):
        group = [result(100), result(200)]
        group[1]["case"]["rate_rps"] = 200
        summary = summarize_group(group)
        self.assertEqual(summary["schema"], 2)
        self.assertEqual(summary["api_generation"], "rpc_v2")
        self.assertEqual(summary["capacity"], 64)
        self.assertEqual(summary["max_clean_scheduled_rps"], 200)
        self.assertNotIn("executor_queue", summary)
        self.assertNotIn("rpc_queue_wait_p99_us", summary)


if __name__ == "__main__":
    unittest.main()
