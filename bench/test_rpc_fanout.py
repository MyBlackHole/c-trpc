"""Tests for generator fanout sensitivity summaries."""
import unittest

from run_rpc_fanout import (
    slots_per_generator,
    summarize_fanout,
    trial_order,
)


PRESSURE_KEYS = (
    "rpc_admission_limit_hits", "rpc_hard_full_events",
    "command_queue_full_events", "completion_queue_full_events",
    "rx_pool_exhausted_events", "tx_pool_exhausted_events",
    "control_tx_pool_exhausted_events",
    "rpc_message_pool_exhausted_events",
    "reassembly_pool_exhausted_events",
)


def result(generators=8, offered=10000, accepted=10000,
           dropped=0, late_p99=50.0, interval=200.0,
           ok_rps=39500.0, client_cpu=0.5, client_rss=100000,
           busy=0.3, command_peak=100, control_peak=100,
           command_full=0):
    server = {key: 0 for key in PRESSURE_KEYS}
    server.update(
        reactor_busy_ratio=busy,
        command_queue_peak=command_peak,
        control_tx_pool_peak=control_peak,
        command_queue_full_events=command_full,
    )
    client = dict(
        offered=offered,
        scheduler_dropped=dropped,
        attempted=accepted,
        accepted=accepted,
        completed=accepted,
        ok=accepted,
        submit_again=0,
        submit_errors=0,
        deadlines=0,
        rpc_errors=0,
        unavailable=0,
        resource_exhausted=0,
        invalid_responses=0,
        generator_count=generators,
        generator_rates_rps=[40000 // generators] * generators,
        scheduler_interval_us=interval,
        scheduler_late_p99_us=late_p99,
        ok_rps=ok_rps,
        client_cpu_s=client_cpu,
        client_peak_rss_kib=client_rss,
    )
    return {
        "case": {
            "rate_rps": 40000,
            "generators": generators,
            "executor_queue": 1024,
            "generator_window": 1024 // generators,
            "generator_capacity": 1024 // generators,
        },
        "server_exit": server,
        "client": client,
        "wall_signals": [],
    }


class FanoutTests(unittest.TestCase):
    def test_slots_per_generator_keeps_total_constant(self):
        self.assertEqual(slots_per_generator(1024, 4), 256)
        self.assertEqual(slots_per_generator(1024, 8), 128)
        self.assertEqual(slots_per_generator(1024, 16), 64)
        self.assertEqual(slots_per_generator(1024, 32), 32)
        with self.assertRaises(ValueError):
            slots_per_generator(1000, 32)
        with self.assertRaises(ValueError):
            slots_per_generator(1024, 2)

    def test_trial_order_alternates_to_reduce_order_bias(self):
        fanouts = [32, 4, 16, 8]
        self.assertEqual(trial_order(fanouts, 1), [4, 8, 16, 32])
        self.assertEqual(trial_order(fanouts, 2), [32, 16, 8, 4])
        self.assertEqual(trial_order(fanouts, 3), [4, 8, 16, 32])

    def test_summary_separates_generator_from_server_pressure(self):
        rows = [
            result(generators=16),
            result(generators=16, accepted=8500, dropped=1500,
                   late_p99=900.0, interval=400.0,
                   ok_rps=33000.0, client_cpu=0.9),
        ]
        summary = summarize_fanout(16, rows)
        self.assertEqual(summary["trials"], 2)
        self.assertEqual(summary["per_generator_slots"], 64)
        self.assertEqual(summary["aggregate_generator_slots"], 1024)
        self.assertEqual(summary["generator_drop_trials"], 1)
        self.assertEqual(summary["generator_late_trials"], 1)
        self.assertEqual(summary["server_pressure_trials"], 0)
        self.assertEqual(summary["server_signal_counts"], {})
        self.assertAlmostEqual(summary["accepted_fraction_median"], 0.925)
        self.assertEqual(summary["scheduler_dropped_max"], 1500)

    def test_summary_retains_exact_server_pressure(self):
        rows = [
            result(generators=32, command_full=3,
                   command_peak=16384, busy=0.5),
            result(generators=32, command_peak=4000, busy=0.4),
        ]
        summary = summarize_fanout(32, rows)
        self.assertEqual(summary["server_pressure_trials"], 1)
        self.assertEqual(
            summary["server_signal_counts"],
            {"reactor_command_queue_full": 1})
        self.assertEqual(summary["command_queue_peak_max"], 16384)

    def test_summary_rejects_mixed_fanouts(self):
        with self.assertRaises(ValueError):
            summarize_fanout(
                8, [result(generators=8), result(generators=16)])

    def test_summary_rejects_mixed_slot_controls(self):
        rows = [result(generators=8), result(generators=8)]
        rows[1]["case"]["generator_capacity"] = 64
        with self.assertRaises(ValueError):
            summarize_fanout(8, rows)


if __name__ == "__main__":
    unittest.main()
