"""Tests for repeated high-rate wall classification."""
import unittest

from run_rpc_repeatability import summarize


def result(ok_rps=39000.0, busy=0.3, signals=None,
           command_hits=0, command_peak=45):
    signal_list = list(signals or [])
    server = dict(
        reactor_busy_ratio=busy,
        command_budget_hits=command_hits,
        command_queue_peak=command_peak,
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
    client = dict(
        scheduler_dropped=0,
        scheduler_late_p99_us=100.0,
        scheduler_interval_us=800.0,
        submit_again=0,
        submit_errors=0,
        deadlines=0,
        rpc_errors=0,
        unavailable=0,
        resource_exhausted=0,
        invalid_responses=0,
        ok_rps=ok_rps,
    )
    if "reactor_command_queue_full" in signal_list:
        server["command_queue_full_events"] = 1
    if "rx_pool_exhausted" in signal_list:
        server["rx_pool_exhausted_events"] = 1
    if "load_generator_late" in signal_list:
        client["scheduler_late_p99_us"] = 1000.0
    return {
        "case": {"rate_rps": 40000},
        "server_exit": server,
        "client": client,
        "wall_signals": signal_list,
    }


class RepeatabilityTests(unittest.TestCase):
    def test_summary_counts_recurrent_and_transient_walls(self):
        rows = [
            result(39500.0, 0.29),
            result(2100.0, 0.15,
                   ["reactor_command_queue_full", "load_generator_late"],
                   command_hits=50, command_peak=4096),
            result(39200.0, 0.30),
        ]
        summary = summarize("baseline", rows)
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
        self.assertEqual(
            summary["signal_counts"]["reactor_command_queue_full"], 1)
        self.assertEqual(
            summary["signal_counts"]["load_generator_late"], 1)
        self.assertEqual(summary["ok_rps_median"], 39200.0)
        self.assertEqual(summary["command_budget_hits_max"], 50)
        self.assertEqual(summary["command_queue_peak_max"], 4096)

    def test_summary_clean_trials_have_no_pressure(self):
        rows = [result(), result(39800.0, 0.31), result(39300.0, 0.28)]
        summary = summarize("ceiling_headroom", rows)
        self.assertEqual(summary["clean_trials"], 3)
        self.assertEqual(summary["server_pressure_trials"], 0)
        self.assertEqual(summary["classification"], "no_server_wall")
        self.assertEqual(summary["server_signal_counts"], {})
        self.assertEqual(summary["reproducible_server_signals"], [])
        self.assertEqual(summary["sporadic_server_signals"], [])
        self.assertEqual(summary["signal_counts"], {})

    def test_summary_marks_same_server_wall_as_reproducible(self):
        rows = [
            result(2500.0, 0.16, ["reactor_command_queue_full"],
                   command_hits=40, command_peak=4096),
            result(2600.0, 0.17, ["reactor_command_queue_full"],
                   command_hits=45, command_peak=4096),
            result(2400.0, 0.15, ["reactor_command_queue_full"],
                   command_hits=50, command_peak=4096),
        ]
        summary = summarize("command_headroom", rows)
        self.assertEqual(summary["server_pressure_trials"], 3)
        self.assertEqual(summary["classification"],
                         "reproducible_server_wall")
        self.assertEqual(summary["server_signal_counts"],
                         {"reactor_command_queue_full": 3})
        self.assertEqual(summary["reproducible_server_signals"],
                         ["reactor_command_queue_full"])
        self.assertEqual(summary["sporadic_server_signals"], [])


if __name__ == "__main__":
    unittest.main()
