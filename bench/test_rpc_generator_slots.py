"""Tests for aggregate generator slot sensitivity."""
import unittest

from run_rpc_generator_slots import slot_order, summarize_slots


PRESSURE_KEYS = (
    "rpc_admission_limit_hits", "rpc_hard_full_events",
    "command_queue_full_events", "completion_queue_full_events",
    "rx_pool_exhausted_events", "tx_pool_exhausted_events",
    "control_tx_pool_exhausted_events",
    "rpc_message_pool_exhausted_events",
    "reassembly_pool_exhausted_events",
)


def result(aggregate_slots=4096, accepted=9000, dropped=1000,
           late_p99=500.0, command_full=0):
    generators = 32
    per_generator_slots = aggregate_slots // generators
    server = {key: 0 for key in PRESSURE_KEYS}
    server.update(
        reactor_busy_ratio=0.35,
        command_queue_peak=500,
        control_tx_pool_peak=480,
        command_queue_full_events=command_full,
    )
    client = dict(
        offered=10000,
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
        generator_rates_rps=[1250] * generators,
        scheduler_interval_us=800.0,
        scheduler_late_p99_us=late_p99,
        ok_rps=14000.0,
        client_cpu_s=0.8,
        client_peak_rss_kib=600000,
    )
    return {
        "case": {
            "rate_rps": 40000,
            "generators": generators,
            "executor_queue": 1024,
            "generator_window": per_generator_slots,
            "generator_capacity": per_generator_slots,
        },
        "server_exit": server,
        "client": client,
        "wall_signals": [],
    }


class GeneratorSlotTests(unittest.TestCase):
    def test_slot_order_alternates_to_reduce_order_bias(self):
        values = [8192, 1024, 4096, 2048]
        self.assertEqual(slot_order(values, 1), [1024, 2048, 4096, 8192])
        self.assertEqual(slot_order(values, 2), [8192, 4096, 2048, 1024])

    def test_summary_keeps_aggregate_slot_identity(self):
        rows = [
            result(4096, accepted=9000, dropped=1000, late_p99=500.0),
            result(4096, accepted=9200, dropped=800, late_p99=400.0),
        ]
        summary = summarize_slots(4096, 32, rows)
        self.assertEqual(summary["type"], "generator_slot_summary")
        self.assertEqual(summary["aggregate_generator_slots"], 4096)
        self.assertEqual(summary["per_generator_slots"], 128)
        self.assertEqual(summary["generator_drop_trials"], 2)
        self.assertEqual(summary["generator_late_trials"], 0)
        self.assertEqual(summary["server_pressure_trials"], 0)
        self.assertAlmostEqual(summary["accepted_fraction_median"], 0.91)

    def test_summary_retains_server_pressure(self):
        rows = [
            result(8192, accepted=9800, dropped=200, command_full=1),
            result(8192, accepted=9900, dropped=100),
        ]
        summary = summarize_slots(8192, 32, rows)
        self.assertEqual(summary["server_pressure_trials"], 1)
        self.assertEqual(
            summary["server_signal_counts"],
            {"reactor_command_queue_full": 1})

    def test_summary_rejects_wrong_aggregate_slot_label(self):
        with self.assertRaises(ValueError):
            summarize_slots(2048, 32, [result(4096), result(4096)])


if __name__ == "__main__":
    unittest.main()
