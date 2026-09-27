"""Tests for benchmark accounting, not synthetic substitutes for the RPC smoke."""
import copy
import unittest

from run_rpc_bench import validate_phase, validate_server_exit


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

    def test_server_observability(self):
        server = dict(
            type="server_exit", drain_status=0, cpu_s=0.1,
            peak_rss_kib=1024, reactor_busy_ratio=0.5,
            reactor_busy_ns=100, reactor_poll_ns=100, reactor_turns=10,
            command_budget_limit=64, command_processed_total=20,
            command_max_per_turn=4, command_budget_hits=0,
            epoll_polls=2, epoll_waits=8,
            command_send_enqueued=10, command_send_full=0,
            command_resume_rx_enqueued=5, command_resume_rx_full=0,
            command_call_enqueued=3, command_call_full=0,
            command_other_enqueued=2, command_other_full=0,
            command_queue_capacity=8, command_queue_peak=2,
            completion_queue_peak=3,
            command_queue_full_events=0, completion_queue_full_events=0,
            rx_pool_capacity=16, rx_pool_peak=4,
            tx_pool_capacity=16, tx_pool_peak=5,
            control_tx_pool_capacity=8, control_tx_pool_peak=6,
            rx_pool_exhausted_events=0, tx_pool_exhausted_events=0,
            control_tx_pool_exhausted_events=0,
            rpc_queue_peak_per_peer=7, rpc_admission_limit_hits=0,
            rpc_hard_full_events=0, rpc_queue_wait_p99_us=10.0,
            rpc_handler_p99_us=20.0, rpc_message_pool_capacity=32,
            rpc_message_pool_peak=8, reassembly_pool_capacity=16,
            reassembly_pool_peak=1, rpc_message_pool_exhausted_events=0,
            reassembly_pool_exhausted_events=0, peers_ready_total=1,
            peers_reaped_total=1)
        validate_server_exit(server)
        broken = copy.deepcopy(server)
        broken["reactor_busy_ratio"] = 1.1
        with self.assertRaises(ValueError):
            validate_server_exit(broken)
        broken = copy.deepcopy(server)
        broken["rpc_queue_wait_p99_us"] = float("nan")
        with self.assertRaises(ValueError):
            validate_server_exit(broken)
        broken = copy.deepcopy(server)
        broken["command_max_per_turn"] = 65
        with self.assertRaises(ValueError):
            validate_server_exit(broken)
        broken = copy.deepcopy(server)
        broken["command_send_full"] = 1
        with self.assertRaises(ValueError):
            validate_server_exit(broken)
        broken = copy.deepcopy(server)
        broken["reactor_busy_ns"] = 0
        broken["reactor_poll_ns"] = 0
        with self.assertRaises(ValueError):
            validate_server_exit(broken)


if __name__ == "__main__":
    unittest.main()
