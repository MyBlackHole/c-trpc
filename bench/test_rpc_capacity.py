"""Accounting tests for the fixed-rate RPC capacity diagnostic."""
import copy
import unittest

from run_rpc_capacity import validate_open_phase


def row():
    return dict(
        type="open_phase", schema=1, phase="measure", scenario="open",
        pid=1, window=8, rate_rps=10, start_ns=1_000_000_000,
        schedule_horizon_ns=2_000_000_000, end_ns=2_100_000_000,
        arrival_window_s=1.0, drain_elapsed_s=1.1, drain_tail_ms=100.0,
        client_cpu_s=0.1, client_peak_rss_kib=1024,
        offered=10, scheduler_dropped=0, attempted=10, accepted=10,
        completed=10, ok=10, submit_again=0, submit_errors=0,
        deadlines=0, rpc_errors=0, unavailable=0, resource_exhausted=0,
        invalid_responses=0,
        ok_ratio=1.0, ok_rps=9.09, payload_MiB_s=0.001,
        ok_p50_us=100.0, ok_p99_us=200.0, accepted_p99_us=200.0,
        scheduler_late_p50_us=5.0, scheduler_late_p99_us=10.0,
        scheduler_late_max_us=12.0)


class OpenLoopAccountingTests(unittest.TestCase):
    def test_valid(self):
        validate_open_phase(row(), 10, 10, 8)

    def test_scheduler_drop_is_explicit(self):
        r = row()
        r["scheduler_dropped"] = 1
        r["attempted"] = 9
        r["accepted"] = 9
        r["completed"] = 9
        r["ok"] = 9
        r["ok_ratio"] = 0.9
        validate_open_phase(r, 10, 10, 8)

    def test_hidden_offered_arrival(self):
        r = row()
        r["scheduler_dropped"] = 1
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_hidden_submit_rejection(self):
        r = row()
        r["submit_again"] = 1
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_missing_completion(self):
        r = row()
        r["completed"] = 9
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_unavailable_must_be_rpc_error(self):
        r = row()
        r["unavailable"] = 1
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_resource_exhausted_must_be_rpc_error(self):
        r = row()
        r["resource_exhausted"] = 1
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_rpc_error_subsets_cannot_overlap_total(self):
        r = row()
        r["rpc_errors"] = 1
        r["unavailable"] = 1
        r["resource_exhausted"] = 1
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_bad_scheduler_order(self):
        r = row()
        r["scheduler_late_p99_us"] = 20.0
        r["scheduler_late_max_us"] = 15.0
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_wrong_rate_or_window(self):
        with self.assertRaises(ValueError):
            validate_open_phase(copy.deepcopy(row()), 10, 11, 8)
        with self.assertRaises(ValueError):
            validate_open_phase(copy.deepcopy(row()), 10, 10, 9)

    def test_bad_arrival_window(self):
        r = row()
        r["arrival_window_s"] = 2.0
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)


if __name__ == "__main__":
    unittest.main()
