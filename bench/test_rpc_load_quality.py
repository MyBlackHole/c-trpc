"""入口校准负例与不同窗口口径回归。"""
import copy
import unittest

from rpc_load_quality import load_quality
from run_rpc_fanout import request_budget
from test_rpc_capacity import row


def result():
    r = row()
    r.update(submission_schema=1, first_submit_ns=1_000_001_000,
             last_submit_ns=1_900_001_000, submitted_in_window=10,
             accepted_in_window=10, completed_in_window=9,
             submit_late_p99_us=1.0, submit_late_max_us=2.0)
    return {"case": {"rate_rps": 10}, "client": r, "generators": [r]}


class LoadQualityTests(unittest.TestCase):
    def test_pass_is_only_local_proxy(self):
        report = load_quality(result(), 2)
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["server_arrival_fidelity"], "unknown")
        self.assertEqual(report["server_capacity"], "not_established")
        self.assertAlmostEqual(report["common_window_rates_rps"]["completed"], 10/1.1)

    def test_old_evidence_is_unknown(self):
        r = result()
        del r["client"]["submission_schema"]
        self.assertEqual(load_quality(r, 100)["status"], "unknown")

    def test_max_not_p99_controls_screen(self):
        self.assertEqual(load_quality(result(), 1)["status"], "fail")

    def test_late_entry_beyond_horizon(self):
        r = result()
        r["client"].update(submitted_in_window=9, accepted_in_window=9,
                           last_submit_ns=2_000_000_000, submit_late_max_us=100000)
        self.assertEqual(load_quality(r, 100)["status"], "fail")

    def test_invalid_evidence(self):
        for key, value in (("submission_schema", True),
                           ("last_submit_ns", 2_000_000_000),
                           ("submit_late_max_us", 0.5),
                           ("submit_late_p99_us", float("nan")),
                           ("submit_late_max_us", True),
                           ("submitted_in_window", True),
                           ("submitted_in_window", 11),
                           ("accepted_in_window", 9),
                           ("completed_in_window", 11),
                           ("first_submit_ns", 0),
                           ("schedule_horizon_ns", 2_000_000_001)):
            with self.subTest(key=key, value=value):
                r = result()
                r["client"][key] = value
                with self.assertRaises(ValueError):
                    load_quality(r, 100)

    def test_common_window_not_sum_of_process_rates(self):
        r = result()
        second = copy.deepcopy(r["client"])
        for key in ("start_ns", "schedule_horizon_ns", "end_ns", "first_submit_ns", "last_submit_ns"):
            second[key] += 50_000_000
        r["case"].update(rate_rps=20, generators=2)
        second["pid"] = 2
        r["generators"].append(second)
        report = load_quality(r, 100)
        self.assertEqual(report["status"], "pass")
        self.assertAlmostEqual(report["common_window_rates_rps"]["attempted"], 20/1.15)
        self.assertEqual(len(report["generators"]), 2)
        second["start_ns"] += 1
        second["schedule_horizon_ns"] += 1
        self.assertEqual(load_quality(r, 100)["status"], "fail")

    def test_missing_generator_or_wrong_case(self):
        for key, value in (("generators", 2), ("rate_rps", 20), ("requests", 20), ("window", 16)):
            with self.subTest(key=key):
                r = result()
                r["case"][key] = value
                with self.assertRaises(ValueError):
                    load_quality(r, 100)

    def test_accounted_drop_and_rejection_fail(self):
        r = result()
        r["client"].update(scheduler_dropped=1, attempted=9, accepted=9,
                           completed=9, ok=9, submitted_in_window=9, accepted_in_window=9)
        self.assertEqual(load_quality(r, 100)["status"], "fail")
        r = result()
        r["client"].update(submit_again=1, accepted=9, completed=9, ok=9,
                           accepted_in_window=9)
        self.assertEqual(load_quality(r, 100)["status"], "fail")

    def test_real_phase_validator_checks_new_evidence(self):
        from run_rpc_capacity import validate_open_phase
        r = result()["client"]
        r["completed_in_window"] = 11
        with self.assertRaises(ValueError):
            validate_open_phase(r, 10, 10, 8)

    def test_duration_overrides_request_floor(self):
        self.assertEqual(request_budget(100, 100000, 250, 1000), 100)
        self.assertEqual(request_budget(100, 1000, 250, None), 1000)

    def test_invalid_threshold(self):
        for value in (-1, float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                load_quality(result(), value)


if __name__ == "__main__":
    unittest.main()
