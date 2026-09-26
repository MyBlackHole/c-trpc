"""Tests for benchmark accounting, not synthetic substitutes for the RPC smoke."""
import copy
import unittest

from run_rpc_bench import validate_phase


def group(n=0):
    return dict(attempted=n, accepted=n, completed=n, ok=n, submit_again=0,
                submit_errors=0, deadlines=0, rpc_errors=0, invalid_responses=0,
                ok_rps=float(n), payload_MiB_s=0.0, ok_p50_us=1.0 if n else 0.0,
                ok_p99_us=2.0 if n else 0.0, accepted_p99_us=2.0 if n else 0.0)


def row():
    return dict(type="phase", schema=1, scenario="small", elapsed_s=1.0,
                start_ns=1000, end_ns=1000001000, client_cpu_s=0.5,
                client_peak_rss_kib=1024, all=group(10), small=group(10),
                bulk=group(), slow=group())


class AccountingTests(unittest.TestCase):
    def test_valid(self):
        validate_phase(row(), 10, "small", clean=True)

    def test_missing_completion(self):
        r = row()
        r["all"]["completed"] -= 1
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "small", clean=True)

    def test_hidden_submit_rejection(self):
        r = row()
        r["all"]["submit_again"] += 1
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "small", clean=True)

    def test_wrong_class_sum(self):
        r = row()
        r["small"] = group(9)
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "small", clean=True)

    def test_corrupt_reply(self):
        r = row()
        for key in ("all", "small"):
            r[key]["ok"] -= 1
            r[key]["invalid_responses"] += 1
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "small", clean=False)

    def test_bad_numbers(self):
        for value in (-1, float("nan"), float("inf")):
            with self.subTest(value=value):
                r = row()
                r["all"]["ok_p99_us"] = value
                with self.assertRaises(ValueError):
                    validate_phase(r, 10, "small", clean=True)

    def test_bad_time(self):
        r = row()
        r["end_ns"] += 1000000000
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "small", clean=True)

    def test_empty_pressure_is_not_coverage(self):
        r = row()
        r["scenario"] = "pressure"
        r["slow"], r["small"] = group(10), group()
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "pressure", clean=False)
        for key in ("all", "slow"):
            r[key]["ok"] = 0
            r[key]["deadlines"] = 10
        validate_phase(r, 10, "pressure", clean=False)

    def test_failed_recovery(self):
        r = row()
        for key in ("all", "small"):
            r[key]["ok"] -= 1
            r[key]["rpc_errors"] += 1
        validate_phase(copy.deepcopy(r), 10, "small", clean=False)
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "small", clean=True)

    def test_fake_mixed_workload(self):
        r = row()
        r["scenario"] = "mixed"
        with self.assertRaises(ValueError):
            validate_phase(r, 10, "mixed", clean=True)


if __name__ == "__main__":
    unittest.main()
