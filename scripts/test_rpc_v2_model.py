#!/usr/bin/env python3
"""RPC V2 有限状态模型和轨迹格式回归。"""
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

import rpc_v2_model
from rpc_v2_model import TraceFormatError, parse_trace


ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "rpc_v2_model"
EMPTY_TRACE = FIXTURES / "empty-quiescent-v1.json"


def minimal_trace():
    return json.loads(EMPTY_TRACE.read_text(encoding="utf-8"))


def make_trace(events, memory_bytes=0, finish="QUIESCENT"):
    trace = minimal_trace()
    trace["trace_id"] = "inline-state-test-v1"
    trace["invariant_ids"] = [
        "CALL-01", "CALL-02", "OWN-01", "DIR-01", "RES-01", "RES-02",
        "GEN-01",
    ]
    trace["limits"]["memory_bytes"] = memory_bytes
    trace["finish"] = finish
    trace["events"] = events
    trace["limits"]["max_events"] = max(1, len(events))
    return parse_trace(json.dumps(trace).encode("utf-8"))


def event(op, expect="APPLIED", slot=0, generation=0, **fields):
    return {
        "op": op,
        "slot": slot,
        "generation": generation,
        **fields,
        "expect": expect,
    }


class TraceParserTests(unittest.TestCase):
    def test_empty_quiescent_fixture_preserves_limits_and_ids(self):
        trace = parse_trace(EMPTY_TRACE.read_bytes())

        self.assertEqual(trace.schema_version, 1)
        self.assertEqual(trace.trace_id, "empty-quiescent-v1")
        self.assertEqual(trace.task_ids, ("#247/TEST-A",))
        self.assertEqual(trace.invariant_ids, ("CALL-01",))
        self.assertEqual(trace.memory_bytes, 0)
        self.assertEqual(trace.max_events, 1)
        self.assertEqual(trace.finish, "QUIESCENT")
        self.assertEqual(trace.events, ())

    def test_duplicate_json_key_is_rejected_before_mapping(self):
        raw = b'{"schema_version":1,"schema_version":1}'

        with self.assertRaises(TraceFormatError):
            parse_trace(raw)

    def test_duplicate_nested_json_key_is_rejected(self):
        raw = (b'{"schema_version":1,"trace_id":"empty-quiescent-v1",'
               b'"task_ids":["#247/TEST-A"],"invariant_ids":["CALL-01"],'
               b'"seed":null,"limits":{"memory_bytes":0,"memory_bytes":0,'
               b'"max_events":1},"finish":"QUIESCENT","events":[]}')

        with self.assertRaises(TraceFormatError):
            parse_trace(raw)

    def test_unknown_top_level_field_is_rejected(self):
        trace = minimal_trace()
        trace["extension"] = 1

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_missing_required_field_is_rejected(self):
        trace = minimal_trace()
        del trace["finish"]

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_boolean_is_not_accepted_as_integer_budget(self):
        trace = minimal_trace()
        trace["limits"]["memory_bytes"] = True

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_unknown_schema_version_is_rejected(self):
        trace = minimal_trace()
        trace["schema_version"] = 2

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_unknown_task_or_invariant_id_is_rejected(self):
        for field, value in (("task_ids", ["#247/TEST-Z"]),
                             ("invariant_ids", ["CALL-UNKNOWN"])):
            with self.subTest(field=field):
                trace = minimal_trace()
                trace[field] = value

                with self.assertRaises(TraceFormatError):
                    parse_trace(json.dumps(trace).encode("utf-8"))

    def test_event_with_unknown_field_is_rejected(self):
        trace = minimal_trace()
        trace["events"] = [{
            "op": "OPEN",
            "slot": 0,
            "generation": 0,
            "request_mode": "ONE",
            "response_mode": "ONE",
            "expect": "APPLIED",
            "extension": 1,
        }]

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_event_count_cannot_exceed_declared_maximum(self):
        trace = minimal_trace()
        trace["events"] = [
            {"op": "OPEN", "slot": 0, "generation": 0,
             "request_mode": "ONE", "response_mode": "ONE",
             "expect": "APPLIED"},
            {"op": "REJECT", "slot": 0, "generation": 0,
             "expect": "APPLIED"},
        ]

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_boolean_is_not_accepted_as_message_length(self):
        trace = minimal_trace()
        trace["events"] = [{
            "op": "MESSAGE", "slot": 0, "generation": 0,
            "direction": "REQUEST", "bytes": True, "expect": "APPLIED",
        }]

        with self.assertRaises(TraceFormatError):
            parse_trace(json.dumps(trace).encode("utf-8"))

    def test_oversized_trace_is_rejected_before_json_parse(self):
        raw = EMPTY_TRACE.read_bytes() + b" " * (65 * 1024)

        with self.assertRaises(TraceFormatError):
            parse_trace(raw)

    def test_non_finite_json_number_is_rejected(self):
        raw = EMPTY_TRACE.read_bytes().replace(
            b'"memory_bytes": 0', b'"memory_bytes": NaN')

        with self.assertRaises(TraceFormatError):
            parse_trace(raw)


class ReferenceStateTests(unittest.TestCase):
    def test_cancel_then_late_work_keeps_one_terminal_callback(self):
        trace = make_trace([
            event("OPEN", request_mode="ONE", response_mode="ONE"),
            event("MESSAGE", direction="REQUEST", bytes=3),
            event("ACCEPT"),
            event("HALF_CLOSE", direction="REQUEST"),
            event("WORK_START"),
            event("CANCEL"),
            event("WORK_DONE"),
            event("FINAL_STATUS", "REJECTED", status="ERROR"),
            event("SLOT_RELEASE"),
            event("OPEN", "STALE", generation=0,
                  request_mode="MANY", response_mode="MANY"),
            event("OPEN", generation=1, request_mode="MANY",
                  response_mode="MANY"),
            event("REJECT", generation=1),
        ])

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.event_results[5:8],
                         ("APPLIED", "APPLIED", "REJECTED"))
        self.assertEqual(result.calls[0]["outcome"], "CANCELLED")
        self.assertEqual(result.calls[0]["terminal_callbacks"], 1)
        self.assertFalse(result.calls[0]["work_active"])
        self.assertEqual(result.calls[1]["outcome"], "REJECTED")
        self.assertEqual(result.calls[1]["terminal_callbacks"], 1)
        self.assertTrue(result.quiescent)

    def test_one_direction_counts_zero_byte_message_as_a_message(self):
        trace = make_trace([
            event("OPEN", request_mode="ONE", response_mode="ONE"),
            event("MESSAGE", direction="REQUEST", bytes=0),
            event("HALF_CLOSE", direction="REQUEST"),
            event("ACCEPT"),
            event("MESSAGE", direction="RESPONSE", bytes=0),
            event("FINAL_STATUS", status="OK"),
        ])

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.calls[0]["outcome"], "OK")
        self.assertEqual(result.calls[0]["message_counts"],
                         {"REQUEST": 1, "RESPONSE": 1})
        self.assertEqual(result.calls[0]["terminal_callbacks"], 1)

    def test_many_success_can_have_no_response_messages(self):
        trace = make_trace([
            event("OPEN", request_mode="MANY", response_mode="MANY"),
            event("ACCEPT"),
            event("HALF_CLOSE", direction="REQUEST"),
            event("FINAL_STATUS", status="OK"),
        ])

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.calls[0]["outcome"], "OK")
        self.assertEqual(result.calls[0]["message_counts"]["RESPONSE"], 0)
        self.assertTrue(result.quiescent)

    def test_resource_limit_and_terminal_storage_release_conserve_budget(self):
        trace = make_trace([
            event("OPEN", request_mode="MANY", response_mode="MANY"),
            event("ACCEPT"),
            event("RESERVE", lease=0, bytes=8),
            event("RESERVE", "RESOURCE_LIMIT", lease=1, bytes=1),
            event("TRANSFER", lease=0),
            event("FINAL_STATUS", status="ERROR"),
            event("RELEASE_STORAGE", lease=0),
            event("RELEASE_STORAGE", "REJECTED", lease=0),
            event("SLOT_RELEASE"),
        ], memory_bytes=8)

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.event_results[3], "RESOURCE_LIMIT")
        self.assertEqual(result.event_results[7], "REJECTED")
        self.assertEqual(result.budget,
                         {"limit": 8, "available": 8, "reserved": 0, "held": 0})
        self.assertTrue(result.quiescent)

    def test_unaccepted_call_cannot_start_work_or_reserve_memory(self):
        trace = make_trace([
            event("OPEN", request_mode="MANY", response_mode="MANY"),
            event("WORK_START", "REJECTED"),
            event("RESERVE", "REJECTED", lease=0, bytes=1),
            event("REJECT"),
        ], memory_bytes=8)

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.calls[0]["accepted"], 0)
        self.assertEqual(result.calls[0]["work_active"], False)
        self.assertEqual(result.calls[0]["outcome"], "REJECTED")
        self.assertEqual(result.budget["available"], 8)

    def test_event_expectation_mismatch_reports_exact_index(self):
        trace = make_trace([
            event("OPEN", "REJECTED", request_mode="MANY",
                  response_mode="MANY"),
        ], finish="PARTIAL")

        with self.assertRaises(rpc_v2_model.ReplayMismatch) as raised:
            rpc_v2_model.run_trace(trace)

        self.assertEqual(raised.exception.event_index, 0)
        self.assertEqual(raised.exception.expected, "REJECTED")
        self.assertEqual(raised.exception.actual, "APPLIED")

    def test_stale_and_resource_failures_leave_state_unchanged(self):
        trace = make_trace([
            event("OPEN", request_mode="MANY", response_mode="MANY"),
            event("RESERVE", "REJECTED", lease=0, bytes=1),
            event("ACCEPT"),
            event("RESERVE", "RESOURCE_LIMIT", lease=0, bytes=3),
            event("OPEN", "STALE", generation=1,
                  request_mode="ONE", response_mode="ONE"),
            event("REJECT"),
            event("SLOT_RELEASE"),
        ], memory_bytes=2)
        state = rpc_v2_model.ModelState(trace.memory_bytes)

        for item in trace.events:
            before = state.snapshot()
            actual = state.apply(item)
            self.assertEqual(actual, item.expect)
            if actual in {"STALE", "REJECTED", "RESOURCE_LIMIT"}:
                self.assertEqual(state.snapshot(), before)

    def test_generation_three_retires_slot_without_wrapping(self):
        events = []
        for generation in range(4):
            events.extend([
                event("OPEN", generation=generation,
                      request_mode="MANY", response_mode="MANY"),
                event("REJECT", generation=generation),
                event("SLOT_RELEASE", generation=generation),
            ])
        events.append(event("OPEN", "REJECTED", generation=3,
                            request_mode="ONE", response_mode="ONE"))

        result = rpc_v2_model.run_trace(make_trace(events))

        self.assertEqual(result.slots[0], {
            "slot": 0, "generation": 3, "state": "RETIRED"})
        self.assertEqual(len(result.calls), 4)
        self.assertTrue(result.quiescent)

    def test_direction_gates_and_one_cardinality_are_enforced(self):
        trace = make_trace([
            event("OPEN", request_mode="ONE", response_mode="ONE"),
            event("MESSAGE", "REJECTED", direction="RESPONSE", bytes=1),
            event("HALF_CLOSE", "REJECTED", direction="REQUEST"),
            event("MESSAGE", direction="REQUEST", bytes=0),
            event("MESSAGE", "REJECTED", direction="REQUEST", bytes=0),
            event("HALF_CLOSE", direction="REQUEST"),
            event("MESSAGE", "REJECTED", direction="REQUEST", bytes=1),
            event("ACCEPT"),
            event("MESSAGE", direction="RESPONSE", bytes=1),
            event("FINAL_STATUS", status="OK"),
        ])

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.event_results[1:7], (
            "REJECTED", "REJECTED", "APPLIED", "REJECTED", "APPLIED",
            "REJECTED"))
        self.assertTrue(result.quiescent)

    def test_partial_trace_reports_nonquiescent_without_claiming_completion(self):
        trace = make_trace([
            event("OPEN", request_mode="MANY", response_mode="MANY"),
        ], finish="PARTIAL")

        result = rpc_v2_model.run_trace(trace)

        self.assertFalse(result.quiescent)
        with self.assertRaises(rpc_v2_model.TraceInvariantError):
            rpc_v2_model.run_trace(make_trace([
                event("OPEN", request_mode="MANY", response_mode="MANY"),
            ]))

    def test_quiescent_finish_rejects_active_work_and_unreturned_reservations(self):
        traces = (
            make_trace([
                event("OPEN", request_mode="MANY", response_mode="MANY"),
                event("ACCEPT"),
                event("WORK_START"),
                event("CANCEL"),
            ]),
            make_trace([
                event("OPEN", request_mode="MANY", response_mode="MANY"),
                event("ACCEPT"),
                event("RESERVE", lease=0, bytes=1),
                event("CANCEL"),
            ], memory_bytes=1),
        )

        for trace in traces:
            with self.subTest(trace=trace.events):
                with self.assertRaises(rpc_v2_model.TraceInvariantError):
                    rpc_v2_model.run_trace(trace)

    def test_untransferred_reservation_can_be_dropped_once(self):
        trace = make_trace([
            event("OPEN", request_mode="MANY", response_mode="MANY"),
            event("ACCEPT"),
            event("RESERVE", lease=0, bytes=2),
            event("DROP_RESERVATION", lease=0),
            event("DROP_RESERVATION", "REJECTED", lease=0),
            event("REJECT"),
            event("SLOT_RELEASE"),
        ], memory_bytes=2)

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual(result.event_results[3:5], ("APPLIED", "REJECTED"))
        self.assertEqual(result.budget["available"], 2)
        self.assertTrue(result.quiescent)

    def test_two_slots_keep_calls_and_lease_ownership_independent(self):
        trace = make_trace([
            event("OPEN", slot=0, request_mode="MANY", response_mode="MANY"),
            event("OPEN", slot=1, request_mode="MANY", response_mode="MANY"),
            event("ACCEPT", slot=0),
            event("ACCEPT", slot=1),
            event("RESERVE", slot=0, lease=0, bytes=2),
            event("TRANSFER", "REJECTED", slot=1, lease=0),
            event("TRANSFER", slot=0, lease=0),
            event("CANCEL", slot=0),
            event("RELEASE_STORAGE", slot=0, lease=0),
            event("SLOT_RELEASE", slot=0),
            event("FINAL_STATUS", slot=1, status="ERROR"),
            event("SLOT_RELEASE", slot=1),
        ], memory_bytes=2)

        result = rpc_v2_model.run_trace(trace)

        self.assertEqual([call["slot"] for call in result.calls], [0, 1])
        self.assertEqual([call["outcome"] for call in result.calls],
                         ["CANCELLED", "ERROR"])
        self.assertEqual(result.event_results[5], "REJECTED")
        self.assertTrue(result.quiescent)


class TraceFixtureAndCliTests(unittest.TestCase):
    FIXTURE_NAMES = (
        "unaccepted-reject-v1.json",
        "cancel-late-work-v1.json",
        "budget-transfer-v1.json",
        "stale-generation-v1.json",
        "invalid-double-terminal-v1.json",
    )

    def _cli(self, fixture):
        test_tmp = ROOT / "build" / "test-tmp"
        test_tmp.mkdir(parents=True, exist_ok=True)
        environment = os.environ.copy()
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment["TMPDIR"] = str(test_tmp)
        return subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "rpc_v2_model.py"),
             "--trace", str(fixture)],
            cwd=ROOT, env=environment, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def test_valid_fixtures_have_stable_replay_output(self):
        for name in self.FIXTURE_NAMES[:-1]:
            with self.subTest(fixture=name):
                path = FIXTURES / name
                trace = parse_trace(path.read_bytes())
                result = rpc_v2_model.run_trace(trace)
                first = self._cli(path)
                second = self._cli(path)

                self.assertEqual(first.returncode, 0, first.stderr.decode())
                self.assertEqual(first.stdout, second.stdout)
                self.assertEqual(first.stdout,
                                 (rpc_v2_model.serialize_result(result) + "\n")
                                 .encode("utf-8"))

    def test_invalid_double_terminal_cli_reports_event_index(self):
        path = FIXTURES / self.FIXTURE_NAMES[-1]
        result = self._cli(path)

        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"events[2]", result.stderr)
        self.assertEqual(result.stdout, b"")


if __name__ == "__main__":
    unittest.main(verbosity=2)
