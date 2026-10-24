#!/usr/bin/env python3
"""将固定 TEST-B 与 TEST-C2 轨迹映射到真实 RPC Record Endpoint 夹具。"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from rpc_v2_model import Trace, TraceFormatError, parse_trace, run_trace


ROOT = Path(__file__).resolve().parent.parent
TRACE_PATH = ROOT / "tests" / "fixtures" / "rpc_v2_model" / \
    "component-cancel-late-work-v1.json"
EVENT_SEQUENCES = {
    "component-cancel-late-work-v1": (
        "OPEN", "ACCEPT", "MESSAGE", "WORK_START", "CANCEL", "WORK_DONE",
        "FINAL_STATUS", "SLOT_RELEASE",
    ),
    "component-deadline-late-work-v1": (
        "OPEN", "ACCEPT", "MESSAGE", "WORK_START", "DEADLINE",
        "WORK_DONE", "FINAL_STATUS", "SLOT_RELEASE",
    ),
    "component-stale-message-after-reuse-v1": (
        "OPEN", "ACCEPT", "MESSAGE", "WORK_START", "CANCEL", "WORK_DONE",
        "FINAL_STATUS", "SLOT_RELEASE", "OPEN", "MESSAGE_STALE", "ACCEPT",
        "MESSAGE", "WORK_START", "CANCEL", "WORK_DONE", "FINAL_STATUS",
        "SLOT_RELEASE",
    ),
    "component-final-reset-after-accepted-v1": (
        "OPEN", "ACCEPT", "MESSAGE", "HALF_CLOSE", "WORK_START", "WORK_DONE",
        "FINAL_STATUS", "RESET", "SLOT_RELEASE",
    ),
    "component-final-send-backpressure-v1": (
        "OPEN", "ACCEPT", "MESSAGE", "HALF_CLOSE", "WORK_START", "WORK_DONE",
        "FINAL_STATUS", "RESET", "SLOT_RELEASE",
    ),
}
FAULT_PROFILES = {
    "component-final-send-backpressure-v1": "--replay-send-backpressure",
}
OBSERVATION_FIELDS = frozenset({
    "status", "terminal_callbacks", "work_completed", "final_frames",
    "retire_calls", "budget_bytes", "quiescent", "stale_messages_ignored",
})


class ComponentReplayError(ValueError):
    """组件轨迹适配或 L1/L2 结果对照失败。"""

    def __init__(self, trace_id, field, message):
        self.trace_id = trace_id
        self.field = field
        self.message = message
        super().__init__(f"{trace_id}: {field}: {message}")


def _fail(trace, field, message):
    raise ComponentReplayError(trace.trace_id, field, message)


def _event_is(event, op, expect, fields, generation=0):
    return (event.op == op and event.slot == 0
            and event.generation == generation
            and event.expect == expect
            and event.fields == tuple(sorted(fields.items())))


def component_event_argument(trace: Trace) -> str:
    """只接受设计中定义的固定 Endpoint 回放轨迹。"""
    operations = EVENT_SEQUENCES.get(trace.trace_id)
    if operations is None:
        _fail(trace, "trace_id", "该组件夹具不支持此 trace_id")
    if not {"#247/TEST-A", "#247/TEST-B"}.issubset(trace.task_ids):
        _fail(trace, "task_ids", "组件轨迹必须关联 TEST-A 和 TEST-B")
    if not {"CALL-01", "CALL-02", "OWN-01"}.issubset(trace.invariant_ids):
        _fail(trace, "invariant_ids", "组件轨迹缺少终局/迟到工作/所有权不变量")
    if trace.memory_bytes != 0 or trace.finish != "QUIESCENT":
        _fail(trace, "limits/finish", "组件轨迹必须使用零预算并要求静默完成")
    if trace.seed is not None:
        _fail(trace, "seed", "固定组件轨迹不得携带随机 seed")

    if trace.trace_id == "component-stale-message-after-reuse-v1":
        if "GEN-01" not in trace.invariant_ids:
            _fail(trace, "invariant_ids", "旧代次轨迹必须关联 GEN-01")
        expected = (
            ("OPEN", "APPLIED", {"request_mode": "ONE", "response_mode": "ONE"}, 0),
            ("ACCEPT", "APPLIED", {}, 0),
            ("MESSAGE", "APPLIED", {"direction": "REQUEST", "bytes": 1}, 0),
            ("WORK_START", "APPLIED", {}, 0),
            ("CANCEL", "APPLIED", {}, 0),
            ("WORK_DONE", "APPLIED", {}, 0),
            ("FINAL_STATUS", "REJECTED", {"status": "ERROR"}, 0),
            ("SLOT_RELEASE", "APPLIED", {}, 0),
            ("OPEN", "APPLIED", {"request_mode": "ONE", "response_mode": "ONE"}, 1),
            ("MESSAGE", "STALE", {"direction": "REQUEST", "bytes": 1}, 0),
            ("ACCEPT", "APPLIED", {}, 1),
            ("MESSAGE", "APPLIED", {"direction": "REQUEST", "bytes": 1}, 1),
            ("WORK_START", "APPLIED", {}, 1),
            ("CANCEL", "APPLIED", {}, 1),
            ("WORK_DONE", "APPLIED", {}, 1),
            ("FINAL_STATUS", "REJECTED", {"status": "ERROR"}, 1),
            ("SLOT_RELEASE", "APPLIED", {}, 1),
        )
    elif trace.trace_id in {
            "component-final-reset-after-accepted-v1",
            "component-final-send-backpressure-v1"}:
        if trace.trace_id == "component-final-send-backpressure-v1" and \
                "#247/TEST-C" not in trace.task_ids:
            _fail(trace, "task_ids", "TEST-C2 轨迹必须关联 TEST-C")
        if "DIR-01" not in trace.invariant_ids:
            _fail(trace, "invariant_ids", "终局竞争轨迹必须关联 DIR-01")
        final_status = "ERROR" if trace.trace_id == \
            "component-final-send-backpressure-v1" else "OK"
        expected = (
            ("OPEN", "APPLIED", {"request_mode": "MANY", "response_mode": "MANY"}, 0),
            ("ACCEPT", "APPLIED", {}, 0),
            ("MESSAGE", "APPLIED", {"direction": "REQUEST", "bytes": 1}, 0),
            ("HALF_CLOSE", "APPLIED", {"direction": "REQUEST"}, 0),
            ("WORK_START", "APPLIED", {}, 0),
            ("WORK_DONE", "APPLIED", {}, 0),
            ("FINAL_STATUS", "APPLIED", {"status": final_status}, 0),
            ("RESET", "REJECTED", {}, 0),
            ("SLOT_RELEASE", "APPLIED", {}, 0),
        )
    else:
        expected = tuple(
            (op, expect, fields, 0)
            for op, expect, fields in (
                ("OPEN", "APPLIED", {"request_mode": "ONE", "response_mode": "ONE"}),
                ("ACCEPT", "APPLIED", {}),
                ("MESSAGE", "APPLIED", {"direction": "REQUEST", "bytes": 1}),
                ("WORK_START", "APPLIED", {}),
                (operations[4], "APPLIED", {}),
                ("WORK_DONE", "APPLIED", {}),
                ("FINAL_STATUS", "REJECTED", {"status": "ERROR"}),
                ("SLOT_RELEASE", "APPLIED", {}),
            ))
    if trace.max_events != len(expected) or len(trace.events) != len(expected):
        _fail(trace, "events", f"组件轨迹事件数必须精确为 {len(expected)}")
    for index, (event, (op, expect, fields, generation)) in enumerate(
            zip(trace.events, expected)):
        if not _event_is(event, op, expect, fields, generation):
            _fail(trace, f"events[{index}]", "事件字段/顺序超出组件适配范围")
    return ",".join(operations)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"重复 JSON 字段: {key}")
        result[key] = value
    return result


def parse_component_output(
        raw: bytes, trace_id="component-cancel-late-work-v1") -> dict:
    """严格验证 C 夹具输出的有限 JSON 观察对象。"""
    try:
        text = raw.decode("utf-8")
        result = json.loads(text, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise ComponentReplayError(
            trace_id, "component_output",
            f"无效 JSON: {error}") from error
    if type(result) is not dict or set(result) != OBSERVATION_FIELDS:
        raise ComponentReplayError(
            trace_id, "component_output",
            "字段集合必须与组件观察 schema 精确一致")
    if type(result["status"]) is not str:
        raise ComponentReplayError(
            trace_id, "status", "必须为字符串")
    for field in ("terminal_callbacks", "final_frames", "retire_calls",
                  "budget_bytes", "stale_messages_ignored"):
        if type(result[field]) is not int or result[field] < 0:
            raise ComponentReplayError(
                trace_id, field, "必须为非负整数")
    for field in ("work_completed", "quiescent"):
        if type(result[field]) is not bool:
            raise ComponentReplayError(
                trace_id, field, "必须为布尔值")
    return result


def compare_observation(trace: Trace, model_result, observation: dict) -> dict:
    """比较实际 Endpoint 观察值与 L1 最终 Call/账本状态。"""
    if trace.trace_id == "component-stale-message-after-reuse-v1":
        if len(model_result.calls) != 2:
            _fail(trace, "calls", "旧代次组件轨迹必须产生两个顺序 Call")
        calls = model_result.calls
        expected = {
            "status": calls[-1]["outcome"],
            "terminal_callbacks": sum(call["terminal_callbacks"] for call in calls),
            "work_completed": not any(call["work_active"] for call in calls),
            "final_frames": 0,
            "retire_calls": sum(
                result == "APPLIED" and event.op == "SLOT_RELEASE"
                for event, result in zip(trace.events, model_result.event_results)),
            "budget_bytes": model_result.budget["reserved"] + model_result.budget["held"],
            "quiescent": model_result.quiescent,
            "stale_messages_ignored": sum(
                event.op == "MESSAGE" and result == "STALE"
                for event, result in zip(trace.events, model_result.event_results)),
        }
    else:
        if len(model_result.calls) != 1:
            _fail(trace, "calls", "固定轨迹必须产生一个 Call")
        call = model_result.calls[0]
        status = call["outcome"]
        if trace.trace_id == "component-final-send-backpressure-v1":
            if status != "ERROR":
                _fail(trace, "status", "TEST-C2 最终状态必须为 ERROR")
            status = "UNIMPLEMENTED"
        expected = {
            "status": status,
            "terminal_callbacks": call["terminal_callbacks"],
            "work_completed": not call["work_active"],
            "final_frames": 0 if model_result.event_results[6] == "REJECTED" else 1,
            "retire_calls": sum(
                result == "APPLIED" and event.op == "SLOT_RELEASE"
                for event, result in zip(trace.events, model_result.event_results)),
            "budget_bytes": model_result.budget["reserved"] + model_result.budget["held"],
            "quiescent": model_result.quiescent,
            "stale_messages_ignored": 0,
        }
    if set(observation) != OBSERVATION_FIELDS:
        _fail(trace, "component_output", "观察字段集合不匹配")
    for field in sorted(OBSERVATION_FIELDS):
        if observation[field] != expected[field]:
            _fail(trace, field,
                  f"L1 期望 {expected[field]!r}，Endpoint 实际 {observation[field]!r}")
    return {
        "trace_id": trace.trace_id,
        "event_results": model_result.event_results,
        "component": observation,
        "matched": True,
    }


def run_component_replay(trace: Trace, binary: Path) -> dict:
    """先执行 L1，再把严格映射的操作序列交给实际 Endpoint 测试二进制。"""
    operations = component_event_argument(trace)
    replay_mode = FAULT_PROFILES.get(trace.trace_id, "--replay-events")
    model_result = run_trace(trace)
    test_tmp = ROOT / "build" / "test-tmp"
    test_tmp.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["TMPDIR"] = str(test_tmp)
    try:
        process = subprocess.run(
            [str(binary), replay_mode, operations],
            cwd=ROOT, env=environment, check=False,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as error:
        _fail(trace, "component_process", str(error))
    if process.returncode != 0:
        detail = process.stderr.decode("utf-8", errors="replace").strip()
        _fail(trace, "component_exit",
              f"Endpoint 夹具退出码 {process.returncode}: {detail}")
    observation = parse_component_output(process.stdout, trace.trace_id)
    return compare_observation(trace, model_result, observation)


def serialize_result(result: dict) -> str:
    return json.dumps(result, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="RPC V2 TEST-B/C2 Endpoint 确定性轨迹回放")
    parser.add_argument("--binary", required=True, type=Path,
                        help="既有 test_rpc_record_endpoint_contract 二进制")
    parser.add_argument("--trace", type=Path, default=TRACE_PATH,
                        help="固定 schema v1 Endpoint JSON 轨迹")
    args = parser.parse_args(argv)
    try:
        trace = parse_trace(args.trace.read_bytes())
        result = run_component_replay(trace, args.binary)
    except (OSError, TraceFormatError, ComponentReplayError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    sys.stdout.write(serialize_result(result) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
