#!/usr/bin/env python3
"""TEST-B1 Python 到真实 Endpoint 测试夹具的有限映射检查。"""
from dataclasses import replace
import json
from pathlib import Path
import unittest

import rpc_v2_component_replay
from rpc_v2_model import parse_trace, run_trace


ROOT = Path(__file__).resolve().parent.parent
TRACE_PATH = ROOT / "tests" / "fixtures" / "rpc_v2_model" / \
    "component-cancel-late-work-v1.json"
DEADLINE_TRACE_PATH = ROOT / "tests" / "fixtures" / "rpc_v2_model" / \
    "component-deadline-late-work-v1.json"
STALE_MESSAGE_TRACE_PATH = ROOT / "tests" / "fixtures" / "rpc_v2_model" / \
    "component-stale-message-after-reuse-v1.json"


class ComponentMappingTests(unittest.TestCase):
    def setUp(self):
        self.trace = parse_trace(TRACE_PATH.read_bytes())
        self.model = run_trace(self.trace)

    def test_known_trace_maps_to_fixed_component_event_sequence(self):
        self.assertEqual(
            rpc_v2_component_replay.component_event_argument(self.trace),
            "OPEN,ACCEPT,MESSAGE,WORK_START,CANCEL,WORK_DONE,"
            "FINAL_STATUS,SLOT_RELEASE")
        self.assertEqual(self.model.calls[0]["outcome"], "CANCELLED")
        self.assertEqual(self.model.calls[0]["terminal_callbacks"], 1)
        self.assertFalse(self.model.calls[0]["work_active"])
        self.assertTrue(self.model.quiescent)
        self.assertEqual(self.model.slots[0]["generation"], 1)
        self.assertEqual(self.model.budget["available"], 0)
        self.assertEqual(self.model.budget["reserved"], 0)
        self.assertEqual(self.model.budget["held"], 0)

    def test_deadline_late_work_maps_to_fixed_component_event_sequence(self):
        trace = parse_trace(DEADLINE_TRACE_PATH.read_bytes())
        model = run_trace(trace)
        self.assertEqual(model.calls[0]["outcome"], "DEADLINE")
        self.assertEqual(
            rpc_v2_component_replay.component_event_argument(trace),
            "OPEN,ACCEPT,MESSAGE,WORK_START,DEADLINE,WORK_DONE,"
            "FINAL_STATUS,SLOT_RELEASE")
        observed = {
            "status": "DEADLINE",
            "terminal_callbacks": 1,
            "work_completed": True,
            "final_frames": 0,
            "retire_calls": 1,
            "budget_bytes": 0,
            "quiescent": True,
            "stale_messages_ignored": 0,
        }
        self.assertTrue(
            rpc_v2_component_replay.compare_observation(
                trace, model, observed)["matched"])

    def test_stale_message_after_slot_reuse_maps_to_fixed_component_sequence(self):
        trace = parse_trace(STALE_MESSAGE_TRACE_PATH.read_bytes())
        model = run_trace(trace)

        self.assertEqual(model.event_results[9], "STALE")
        self.assertEqual(model.calls[0]["outcome"], "CANCELLED")
        self.assertEqual(model.calls[1]["outcome"], "CANCELLED")
        self.assertTrue(model.quiescent)
        self.assertEqual(model.slots[0]["generation"], 2)
        self.assertEqual(
            rpc_v2_component_replay.component_event_argument(trace),
            "OPEN,ACCEPT,MESSAGE,WORK_START,CANCEL,WORK_DONE,"
            "FINAL_STATUS,SLOT_RELEASE,OPEN,MESSAGE_STALE,ACCEPT,MESSAGE,"
            "WORK_START,CANCEL,WORK_DONE,FINAL_STATUS,SLOT_RELEASE")

        observed = {
            "status": "CANCELLED",
            "terminal_callbacks": 2,
            "work_completed": True,
            "final_frames": 0,
            "retire_calls": 2,
            "budget_bytes": 0,
            "quiescent": True,
            "stale_messages_ignored": 1,
        }
        self.assertTrue(
            rpc_v2_component_replay.compare_observation(
                trace, model, observed)["matched"])

    def test_unsupported_event_order_fails_closed(self):
        changed = replace(self.trace, events=self.trace.events[:-1])

        with self.assertRaises(rpc_v2_component_replay.ComponentReplayError):
            rpc_v2_component_replay.component_event_argument(changed)

    def test_fixed_component_trace_rejects_random_seed(self):
        changed = replace(self.trace, seed=1)

        with self.assertRaises(rpc_v2_component_replay.ComponentReplayError) as raised:
            rpc_v2_component_replay.component_event_argument(changed)
        self.assertEqual(raised.exception.field, "seed")

    def test_component_observation_must_match_model_summary(self):
        observed = {
            "status": "CANCELLED",
            "terminal_callbacks": 1,
            "work_completed": True,
            "final_frames": 0,
            "retire_calls": 1,
            "budget_bytes": 0,
            "quiescent": True,
            "stale_messages_ignored": 0,
        }

        summary = rpc_v2_component_replay.compare_observation(
            self.trace, self.model, observed)

        self.assertEqual(summary["trace_id"], self.trace.trace_id)
        self.assertEqual(summary["event_results"], self.model.event_results)
        observed["terminal_callbacks"] = 2
        with self.assertRaises(rpc_v2_component_replay.ComponentReplayError) as raised:
            rpc_v2_component_replay.compare_observation(
                self.trace, self.model, observed)
        self.assertEqual(raised.exception.field, "terminal_callbacks")
        self.assertIn(self.trace.trace_id, str(raised.exception))

    def test_nonzero_component_exit_reports_trace_and_field(self):
        test_tmp = ROOT / "build" / "test-tmp"
        test_tmp.mkdir(parents=True, exist_ok=True)
        failing_binary = test_tmp / "component-replay-failure.py"
        failing_binary.write_text(
            "#!/usr/bin/env python3\n"
            "import sys\n"
            "print('injected component failure', file=sys.stderr)\n"
            "sys.exit(7)\n",
            encoding="utf-8")
        failing_binary.chmod(0o755)
        try:
            with self.assertRaises(
                    rpc_v2_component_replay.ComponentReplayError) as raised:
                rpc_v2_component_replay.run_component_replay(
                    self.trace, failing_binary)
        finally:
            failing_binary.unlink(missing_ok=True)
        self.assertEqual(raised.exception.field, "component_exit")
        self.assertIn(self.trace.trace_id, str(raised.exception))

    def test_fake_component_process_replays_matching_observation(self):
        test_tmp = ROOT / "build" / "test-tmp"
        test_tmp.mkdir(parents=True, exist_ok=True)
        fake_binary = test_tmp / "component-replay-success.py"
        observation = {
            "status": "CANCELLED",
            "terminal_callbacks": 1,
            "work_completed": True,
            "final_frames": 0,
            "retire_calls": 1,
            "budget_bytes": 0,
            "quiescent": True,
            "stale_messages_ignored": 0,
        }
        fake_binary.write_text(
            "#!/usr/bin/env python3\n"
            "print(" + repr(json.dumps(observation)) + ")\n",
            encoding="utf-8")
        fake_binary.chmod(0o755)
        try:
            result = rpc_v2_component_replay.run_component_replay(
                self.trace, fake_binary)
        finally:
            fake_binary.unlink(missing_ok=True)
        self.assertTrue(result["matched"])
        self.assertEqual(result["component"], observation)


class ComponentOutputValidationTests(unittest.TestCase):
    def test_json_output_rejects_invalid_and_duplicate_json(self):
        for raw in (b"not-json", b'{"status":"CANCELLED","status":"ERROR"}'):
            with self.subTest(raw=raw):
                with self.assertRaises(
                        rpc_v2_component_replay.ComponentReplayError):
                    rpc_v2_component_replay.parse_component_output(raw)

    def test_json_output_requires_exact_observation_fields(self):
        valid = {
            "status": "CANCELLED",
            "terminal_callbacks": 1,
            "work_completed": True,
            "final_frames": 0,
            "retire_calls": 1,
            "budget_bytes": 0,
            "quiescent": True,
            "stale_messages_ignored": 0,
        }
        self.assertEqual(
            rpc_v2_component_replay.parse_component_output(
                json.dumps(valid).encode("utf-8")), valid)
        valid["debug"] = "unexpected"
        with self.assertRaises(rpc_v2_component_replay.ComponentReplayError):
            rpc_v2_component_replay.parse_component_output(
                json.dumps(valid).encode("utf-8"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
