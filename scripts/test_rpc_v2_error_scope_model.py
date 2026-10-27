#!/usr/bin/env python3
"""#242/ERR-A 错误作用域参考模型回归。"""
import importlib
import importlib.util
import unittest


MODEL_NAME = "rpc_v2_error_scope_model"


def load_model(test_case):
    """把尚未新增的模型报告为断言失败，而不是测试导入异常。"""
    spec = importlib.util.find_spec(MODEL_NAME)
    test_case.assertIsNotNone(spec, "ERR-A 纯状态模型尚未实现")
    return importlib.import_module(MODEL_NAME)


def call(state, call_id):
    return next(item for item in state.calls if item.call_id == call_id)


class ErrorScopeClassificationTests(unittest.TestCase):
    def test_same_semantic_fault_scopes_by_parse_boundary_trust(self):
        model = load_model(self)
        state = model.ConnectionState.open(("target", "peer"))
        trusted = model.ParseTrust(
            frame_boundary_trusted=True,
            stream_identity_trusted=True,
            reset_supported=True)
        untrusted_boundary = model.ParseTrust(
            frame_boundary_trusted=False,
            stream_identity_trusted=True,
            reset_supported=True)

        isolated, isolated_action = state.apply(
            "target", model.FaultKind.STREAM_SEMANTIC, trusted)
        closed, closed_action = state.apply(
            None, model.FaultKind.STREAM_SEMANTIC, untrusted_boundary)

        self.assertEqual(isolated_action, model.Action.RESET_STREAM)
        self.assertFalse(isolated.is_closed)
        self.assertEqual(call(isolated, "target").terminal,
                         model.CallTerminal.RESET)
        self.assertIsNone(call(isolated, "peer").terminal)
        self.assertFalse(state.is_closed)
        self.assertTrue(all(item.terminal is None for item in state.calls))

        self.assertEqual(closed_action, model.Action.CLOSE_CONNECTION)
        self.assertTrue(closed.is_closed)
        self.assertEqual(call(closed, "target").terminal,
                         model.CallTerminal.CONNECTION_FAILED)
        self.assertEqual(call(closed, "peer").terminal,
                         model.CallTerminal.CONNECTION_FAILED)
        self.assertEqual([item.terminal_count for item in closed.calls], [1, 1])

    def test_stream_fault_without_trusted_identity_or_reset_fails_closed(self):
        model = load_model(self)
        state = model.ConnectionState.open(("target", "peer"))

        cases = (
            (None, model.ParseTrust(
                frame_boundary_trusted=True,
                stream_identity_trusted=False,
                reset_supported=True)),
            ("target", model.ParseTrust(
                frame_boundary_trusted=True,
                stream_identity_trusted=True,
                reset_supported=False)),
        )
        for call_id, trust in cases:
            with self.subTest(call_id=call_id, trust=trust):
                after, action = state.apply(
                    call_id, model.FaultKind.STREAM_SEMANTIC, trust)
                self.assertEqual(action, model.Action.CLOSE_CONNECTION)
                self.assertTrue(after.is_closed)
                self.assertTrue(all(item.terminal_count == 1
                                    for item in after.calls))

    def test_business_rejection_terminates_only_its_call(self):
        model = load_model(self)
        state = model.ConnectionState.open(("target", "peer"))

        after, action = state.apply(
            "target", model.FaultKind.BUSINESS_REJECTION,
            model.ParseTrust())

        self.assertEqual(action, model.Action.FINAL_STATUS)
        self.assertFalse(after.is_closed)
        self.assertEqual(call(after, "target").terminal,
                         model.CallTerminal.FINAL_STATUS)
        self.assertIsNone(call(after, "peer").terminal)

    def test_local_resource_pressure_is_explicit_and_preserves_peer(self):
        model = load_model(self)
        state = model.ConnectionState.open(("target", "peer"))

        after, action = state.apply(
            "target", model.FaultKind.LOCAL_RESOURCE_PRESSURE,
            model.ParseTrust())

        self.assertEqual(action, model.Action.RESOURCE_REFUSAL)
        self.assertFalse(after.is_closed)
        self.assertEqual(call(after, "target").terminal,
                         model.CallTerminal.RESOURCE_REFUSED)
        self.assertIsNone(call(after, "peer").terminal)

    def test_connection_level_and_unknown_faults_fail_all_active_calls(self):
        model = load_model(self)
        self.assertEqual(set(model.ERROR_CLASSIFICATION_TABLE),
                         set(model.FaultKind))
        faults = (model.FaultKind.UNTRUSTED_FRAME_BOUNDARY,
                  model.FaultKind.CONNECTION_NEGOTIATION,
                  model.FaultKind.TLS_INTEGRITY,
                  model.FaultKind.INTERNAL_CORRUPTION)

        for fault in faults:
            with self.subTest(fault=fault):
                state = model.ConnectionState.open(("first", "second"))
                after, action = state.apply(
                    None, fault, model.ParseTrust(True, True, True))
                self.assertEqual(action, model.Action.CLOSE_CONNECTION)
                self.assertTrue(after.is_closed)
                self.assertTrue(all(
                    item.terminal == model.CallTerminal.CONNECTION_FAILED
                    and item.terminal_count == 1 for item in after.calls))

    def test_connection_close_preserves_prior_terminal_and_is_idempotent(self):
        model = load_model(self)
        state = model.ConnectionState.open(("finished", "active"))
        state, _ = state.apply(
            "finished", model.FaultKind.BUSINESS_REJECTION,
            model.ParseTrust())

        closed, action = state.apply(
            "active", model.FaultKind.TLS_INTEGRITY,
            model.ParseTrust())
        repeated, repeated_action = closed.apply(
            None, model.FaultKind.INTERNAL_CORRUPTION,
            model.ParseTrust())

        self.assertEqual(action, model.Action.CLOSE_CONNECTION)
        self.assertEqual(repeated_action, model.Action.CLOSE_CONNECTION)
        self.assertEqual(call(closed, "finished").terminal,
                         model.CallTerminal.FINAL_STATUS)
        self.assertEqual(call(closed, "finished").terminal_count, 1)
        self.assertEqual(call(closed, "active").terminal_count, 1)
        self.assertEqual(repeated, closed)


if __name__ == "__main__":
    unittest.main()
