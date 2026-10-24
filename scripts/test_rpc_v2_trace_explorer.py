#!/usr/bin/env python3
"""RPC V2 固定种子轨迹探索与缩减器回归。"""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

import rpc_v2_trace_explorer
import rpc_v2_model


ROOT = Path(__file__).resolve().parent.parent


def _mutant_trace():
    trace = {
        "schema_version": 1,
        "trace_id": "test-c1-shrinker-mutant",
        "task_ids": ["#247/TEST-A", "#247/TEST-C"],
        "invariant_ids": sorted(rpc_v2_model.INVARIANT_IDS),
        "seed": 33047,
        "limits": {"memory_bytes": 16, "max_events": 7},
        "finish": "PARTIAL",
        "events": [
            {"op": "OPEN", "slot": 0, "generation": 0,
             "request_mode": "ONE", "response_mode": "ONE",
             "expect": "APPLIED"},
            {"op": "MESSAGE", "slot": 0, "generation": 0,
             "direction": "REQUEST", "bytes": 0, "expect": "APPLIED"},
            {"op": "ACCEPT", "slot": 0, "generation": 0,
             "expect": "APPLIED"},
            {"op": "HALF_CLOSE", "slot": 0, "generation": 0,
             "direction": "REQUEST", "expect": "APPLIED"},
            {"op": "WORK_START", "slot": 0, "generation": 0,
             "expect": "APPLIED"},
            {"op": "WORK_DONE", "slot": 0, "generation": 0,
             "expect": "APPLIED"},
            {"op": "CANCEL", "slot": 0, "generation": 0,
             "expect": "APPLIED"},
        ],
    }
    return rpc_v2_model.parse_trace(json.dumps(trace).encode("utf-8"))


def _duplicate_terminal_callback(trace):
    state = rpc_v2_model.ModelState(trace.memory_bytes)
    for event in trace.events:
        actual = state.apply(event)
        if actual != event.expect:
            raise rpc_v2_model.ReplayMismatch(
                trace.trace_id, 0, event.op, event.expect, actual)
        if actual == "APPLIED" and event.op in {
                "REJECT", "FINAL_STATUS", "CANCEL", "DEADLINE", "RESET",
                "OWNER_STOP"}:
            state.calls[(event.slot, event.generation)].terminal_callbacks = 2
            state.check_invariants()


class TraceGenerationTests(unittest.TestCase):
    def test_same_seed_produces_replayable_identical_trace(self):
        first = rpc_v2_trace_explorer.generate_trace(33047, 48)
        second = rpc_v2_trace_explorer.generate_trace(33047, 48)

        self.assertEqual(rpc_v2_trace_explorer.serialize_trace(first),
                         rpc_v2_trace_explorer.serialize_trace(second))
        replayable = rpc_v2_model.parse_trace(
            rpc_v2_trace_explorer.serialize_trace(first).encode("utf-8"))
        self.assertEqual(replayable.seed, 33047)
        self.assertIn("#247/TEST-C", replayable.task_ids)
        rpc_v2_model.run_trace(replayable)

    def test_zero_seed_is_retained_while_prng_avoids_zero_state(self):
        trace = rpc_v2_trace_explorer.generate_trace(0, 8)

        self.assertEqual(trace.seed, 0)
        self.assertNotEqual(rpc_v2_trace_explorer._XorShift32(0).next(), 0)

    def test_different_seed_changes_the_generated_trace(self):
        first = rpc_v2_trace_explorer.generate_trace(33047, 48)
        second = rpc_v2_trace_explorer.generate_trace(33048, 48)

        self.assertNotEqual(rpc_v2_trace_explorer.serialize_trace(first),
                            rpc_v2_trace_explorer.serialize_trace(second))

    def test_seed_and_event_limit_must_stay_inside_declared_bounds(self):
        for seed in (-1, 1 << 32, True):
            with self.subTest(seed=seed):
                with self.assertRaises(rpc_v2_trace_explorer.TraceExplorerInputError):
                    rpc_v2_trace_explorer.generate_trace(seed, 1)
        for max_events in (0, 49, True):
            with self.subTest(max_events=max_events):
                with self.assertRaises(rpc_v2_trace_explorer.TraceExplorerInputError):
                    rpc_v2_trace_explorer.generate_trace(1, max_events)

    def test_fixed_seed_corpus_covers_schema_operations_and_failure_results(self):
        operations = set()
        results = set()
        duplicate_terminal_seen = False
        duplicate_release_seen = False
        for seed in range(64):
            trace = rpc_v2_trace_explorer.generate_trace(seed, 48)
            self.assertEqual(len(trace.events), 48)
            parsed = rpc_v2_model.parse_trace(
                rpc_v2_trace_explorer.serialize_trace(trace).encode("utf-8"))
            rpc_v2_model.run_trace(parsed)
            operations.update(event.op for event in trace.events)
            results.update(event.expect for event in trace.events)
            terminal_success = set()
            released_leases = set()
            for event in trace.events:
                call_key = (event.slot, event.generation)
                if event.op in {"REJECT", "FINAL_STATUS", "CANCEL", "DEADLINE",
                                "RESET", "OWNER_STOP"}:
                    if event.expect == "APPLIED":
                        terminal_success.add(call_key)
                    elif event.expect == "REJECTED" and call_key in terminal_success:
                        duplicate_terminal_seen = True
                if event.op in {"DROP_RESERVATION", "RELEASE_STORAGE"}:
                    lease_key = (*call_key, event.value("lease"))
                    if event.expect == "APPLIED":
                        released_leases.add(lease_key)
                    elif event.expect == "REJECTED" and lease_key in released_leases:
                        duplicate_release_seen = True

        self.assertEqual(operations, rpc_v2_model.OPERATIONS)
        self.assertIn("STALE", results)
        self.assertIn("REJECTED", results)
        self.assertIn("RESOURCE_LIMIT", results)
        self.assertTrue(duplicate_terminal_seen)
        self.assertTrue(duplicate_release_seen)


class TraceShrinkerTests(unittest.TestCase):
    def test_model_invariant_failure_during_generation_becomes_report(self):
        with patch.object(rpc_v2_model.ModelState, "apply",
                          side_effect=rpc_v2_model.TraceInvariantError(
                              "注入的模型不变量异常")):
            report = rpc_v2_trace_explorer.explore(33047, 1, 8)

        self.assertIsInstance(report, rpc_v2_trace_explorer.FailureReport)
        self.assertEqual(report.signature.message, "注入的模型不变量异常")
        self.assertEqual(len(report.original.events), 1)
        self.assertEqual(len(report.minimized.events), 1)

    def test_known_duplicate_callback_failure_shrinks_deterministically(self):
        trace = _mutant_trace()
        signature = rpc_v2_trace_explorer.failure_signature(
            trace, _duplicate_terminal_callback)

        self.assertIsNotNone(signature)
        self.assertEqual(signature.kind, "TraceInvariantError")
        self.assertEqual(signature.message, "终局回调不满足至多一次")
        self.assertIsNone(rpc_v2_trace_explorer.failure_signature(
            trace, rpc_v2_model.run_trace))

        minimized = rpc_v2_trace_explorer.shrink_trace(
            trace, _duplicate_terminal_callback, signature)
        repeated = rpc_v2_trace_explorer.shrink_trace(
            trace, _duplicate_terminal_callback, signature)

        self.assertEqual([event.op for event in minimized.events],
                         ["OPEN", "CANCEL"])
        self.assertEqual(rpc_v2_trace_explorer.serialize_trace(minimized),
                         rpc_v2_trace_explorer.serialize_trace(repeated))
        self.assertEqual(rpc_v2_trace_explorer.failure_signature(
            minimized, _duplicate_terminal_callback), signature)

    def test_shrinker_preserves_original_finish_contract(self):
        data = json.loads(rpc_v2_trace_explorer.serialize_trace(_mutant_trace()))
        data["finish"] = "QUIESCENT"
        trace = rpc_v2_model.parse_trace(json.dumps(data).encode("utf-8"))
        signature = rpc_v2_trace_explorer.failure_signature(
            trace, _duplicate_terminal_callback)

        minimized = rpc_v2_trace_explorer.shrink_trace(
            trace, _duplicate_terminal_callback, signature)

        self.assertEqual(minimized.finish, "QUIESCENT")

    def test_exploration_summary_is_stable_and_counts_all_events(self):
        first = rpc_v2_trace_explorer.explore(33047, 4, 24)
        second = rpc_v2_trace_explorer.explore(33047, 4, 24)

        self.assertEqual(first, second)
        self.assertEqual(first.seed, 33047)
        self.assertEqual(first.cases, 4)
        self.assertEqual(first.max_events, 24)
        self.assertEqual(first.events, 96)
        self.assertEqual(
            rpc_v2_trace_explorer.serialize_summary(first),
            rpc_v2_trace_explorer.serialize_summary(second))
        self.assertEqual(list(first.operation_counts),
                         sorted(first.operation_counts))
        self.assertEqual(list(first.result_counts), sorted(first.result_counts))

    def test_injected_mutant_returns_schema_valid_original_and_minimized_report(self):
        report = rpc_v2_trace_explorer.explore(
            33047, 1, 48, failure_probe=_duplicate_terminal_callback)

        self.assertIsInstance(report, rpc_v2_trace_explorer.FailureReport)
        self.assertEqual(report.seed, 33047)
        self.assertEqual(report.signature.kind, "TraceInvariantError")
        self.assertLess(len(report.minimized.events), len(report.original.events))
        encoded = rpc_v2_trace_explorer.serialize_failure_report(report)
        data = json.loads(encoded)
        self.assertEqual(data["signature"]["message"], report.signature.message)
        for field in ("original", "minimized"):
            parsed = rpc_v2_model.parse_trace(
                json.dumps(data[field]).encode("utf-8"))
            rpc_v2_model.run_trace(parsed)


class TraceExplorerCliTests(unittest.TestCase):
    def setUp(self):
        self.failure_dir = ROOT / "build" / "test-tmp" / "rpc-v2-explorer-cli-test"
        if self.failure_dir.exists():
            shutil.rmtree(self.failure_dir)

    def tearDown(self):
        if self.failure_dir.exists():
            shutil.rmtree(self.failure_dir)

    def test_cli_success_is_stable_and_emits_json_summary(self):
        args = ["--seed", "33047", "--cases", "4", "--max-events", "24"]
        first_stdout = io.StringIO()
        second_stdout = io.StringIO()
        with redirect_stdout(first_stdout):
            first_status = rpc_v2_trace_explorer.main(args)
        with redirect_stdout(second_stdout):
            second_status = rpc_v2_trace_explorer.main(args)

        self.assertEqual(first_status, 0)
        self.assertEqual(second_status, 0)
        self.assertEqual(first_stdout.getvalue(), second_stdout.getvalue())
        self.assertEqual(json.loads(first_stdout.getvalue())["events"], 96)
        self.assertFalse(self.failure_dir.exists())

    def test_cli_rejects_unknown_and_out_of_range_arguments(self):
        for args in (["--unknown"], ["--seed", "-1"],
                     ["--seed", "4294967296"], ["--cases", "0"],
                     ["--cases", "65"], ["--max-events", "0"],
                     ["--max-events", "49"],
                     ["--seed", "1", "--seed", "2"]):
            with self.subTest(args=args):
                captured = io.StringIO()
                with redirect_stderr(captured):
                    self.assertEqual(rpc_v2_trace_explorer.main(args), 2)
                self.assertTrue(captured.getvalue())

    def test_cli_failure_writes_replayable_original_and_minimized_artifacts(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            status = rpc_v2_trace_explorer.main(
                ["--seed", "33047", "--cases", "1", "--max-events", "48",
                 "--failure-dir", str(self.failure_dir)],
                failure_probe=_duplicate_terminal_callback)

        self.assertEqual(status, 1)
        report = json.loads(stdout.getvalue())
        self.assertIn("signature", report)
        for name in ("original", "minimized"):
            artifact = json.loads((self.failure_dir / f"{name}.json").read_text(
                encoding="utf-8"))
            parsed = rpc_v2_model.parse_trace(json.dumps(artifact).encode("utf-8"))
            rpc_v2_model.run_trace(parsed)
            self.assertEqual(artifact, report[name])

    def test_cli_reports_failure_directory_write_error(self):
        blocker = ROOT / "build" / "test-tmp" / "rpc-v2-explorer-cli-blocker"
        blocker.parent.mkdir(parents=True, exist_ok=True)
        if blocker.exists():
            blocker.unlink()
        blocker.write_text("file blocks directory creation", encoding="utf-8")
        stderr = io.StringIO()
        stdout = io.StringIO()
        with redirect_stderr(stderr), redirect_stdout(stdout):
            status = rpc_v2_trace_explorer.main(
                ["--seed", "33047", "--cases", "1", "--max-events", "48",
                 "--failure-dir", str(blocker / "child")],
                failure_probe=_duplicate_terminal_callback)

        self.assertEqual(status, 2)
        self.assertIn("失败轨迹写入失败", stderr.getvalue())
        blocker.unlink()

    def test_command_line_help_and_seeded_summary_run_as_a_process(self):
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["TMPDIR"] = str(ROOT / "build" / "test-tmp")
        script = ROOT / "scripts" / "rpc_v2_trace_explorer.py"
        help_result = subprocess.run(
            [sys.executable, str(script), "--help"], cwd=ROOT, env=env,
            capture_output=True, text=True, check=False)
        self.assertEqual(help_result.returncode, 0)
        self.assertIn("--failure-dir", help_result.stdout)

        result = subprocess.run(
            [sys.executable, str(script), "--seed", "42", "--cases", "2",
             "--max-events", "8"], cwd=ROOT, env=env,
            capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["events"], 16)


if __name__ == "__main__":
    unittest.main()
