#!/usr/bin/env python3
"""RPC V2 有限状态参考模型与版本化轨迹回放。"""
from dataclasses import dataclass
import argparse
import copy
import json
from pathlib import Path
import re
import sys


MAX_TRACE_BYTES = 64 * 1024
MAX_EVENTS = 256
MAX_MEMORY_BYTES = 16
MAX_SLOT = 1
MAX_GENERATION = 3
MAX_LEASE = 7

TASK_IDS = frozenset({"#247/TEST-A"})
INVARIANT_IDS = frozenset({
    "CALL-01", "CALL-02", "OWN-01", "DIR-01", "RES-01", "RES-02",
    "GEN-01",
})
EXPECTATIONS = frozenset({"APPLIED", "STALE", "REJECTED", "RESOURCE_LIMIT"})
OPERATIONS = frozenset({
    "OPEN", "ACCEPT", "REJECT", "MESSAGE", "HALF_CLOSE", "WORK_START",
    "WORK_DONE", "FINAL_STATUS", "CANCEL", "DEADLINE", "RESET",
    "OWNER_STOP", "RESERVE", "TRANSFER", "DROP_RESERVATION",
    "RELEASE_STORAGE", "SLOT_RELEASE",
})
TRACE_ID_PATTERN = re.compile(r"[a-z0-9._/-]{1,64}\Z")


class TraceFormatError(ValueError):
    """轨迹格式不符合严格的 schema v1。"""

    def __init__(self, field_path, message):
        self.field_path = field_path
        self.message = message
        super().__init__(f"{field_path}: {message}")


@dataclass(frozen=True)
class Event:
    op: str
    slot: int
    generation: int
    expect: str
    fields: tuple[tuple[str, int | str], ...]

    def value(self, name):
        for key, value in self.fields:
            if key == name:
                return value
        raise KeyError(name)


@dataclass(frozen=True)
class Trace:
    schema_version: int
    trace_id: str
    task_ids: tuple[str, ...]
    invariant_ids: tuple[str, ...]
    seed: int | None
    memory_bytes: int
    max_events: int
    finish: str
    events: tuple[Event, ...]


_EVENT_FIELDS = {
    "OPEN": {"request_mode", "response_mode"},
    "MESSAGE": {"direction", "bytes"},
    "HALF_CLOSE": {"direction"},
    "FINAL_STATUS": {"status"},
    "RESERVE": {"lease", "bytes"},
    "TRANSFER": {"lease"},
    "DROP_RESERVATION": {"lease"},
    "RELEASE_STORAGE": {"lease"},
}


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TraceFormatError("$", f"重复 JSON 字段: {key}")
        result[key] = value
    return result


def _reject_constant(value):
    raise TraceFormatError("$", f"不允许非有限 JSON 数值: {value}")


def _exact_keys(value, required, field_path):
    if type(value) is not dict:
        raise TraceFormatError(field_path, "必须为 JSON 对象")
    actual = set(value)
    missing = required - actual
    extra = actual - required
    if missing:
        raise TraceFormatError(field_path, f"缺少字段: {', '.join(sorted(missing))}")
    if extra:
        raise TraceFormatError(field_path, f"未知字段: {', '.join(sorted(extra))}")


def _integer(value, field_path, minimum, maximum):
    if type(value) is not int or value < minimum or value > maximum:
        raise TraceFormatError(
            field_path, f"必须为 {minimum} 到 {maximum} 之间的整数")
    return value


def _string(value, field_path, allowed=None):
    if type(value) is not str:
        raise TraceFormatError(field_path, "必须为字符串")
    if allowed is not None and value not in allowed:
        raise TraceFormatError(field_path, f"不支持的值: {value}")
    return value


def _identifier_list(value, field_path, allowed, required=None):
    if type(value) is not list or not value:
        raise TraceFormatError(field_path, "必须为非空字符串数组")
    for index, item in enumerate(value):
        _string(item, f"{field_path}[{index}]")
        if item not in allowed:
            raise TraceFormatError(f"{field_path}[{index}]", f"未知 ID: {item}")
    if len(value) != len(set(value)):
        raise TraceFormatError(field_path, "不得重复 ID")
    if required is not None and required not in value:
        raise TraceFormatError(field_path, f"必须包含 {required}")
    return tuple(value)


def _parse_event(value, index):
    path = f"events[{index}]"
    if type(value) is not dict:
        raise TraceFormatError(path, "必须为 JSON 对象")
    op = _string(value.get("op"), f"{path}.op", OPERATIONS)
    required = {"op", "slot", "generation", "expect"} | _EVENT_FIELDS.get(op, set())
    _exact_keys(value, required, path)
    slot = _integer(value["slot"], f"{path}.slot", 0, MAX_SLOT)
    generation = _integer(
        value["generation"], f"{path}.generation", 0, MAX_GENERATION)
    expect = _string(value["expect"], f"{path}.expect", EXPECTATIONS)
    if expect == "RESOURCE_LIMIT" and op != "RESERVE":
        raise TraceFormatError(f"{path}.expect", "仅 RESERVE 可预期 RESOURCE_LIMIT")

    fields = []
    for name in sorted(_EVENT_FIELDS.get(op, set())):
        item = value[name]
        field_path = f"{path}.{name}"
        if name in {"request_mode", "response_mode"}:
            item = _string(item, field_path, {"ONE", "MANY"})
        elif name == "direction":
            item = _string(item, field_path, {"REQUEST", "RESPONSE"})
        elif name == "status":
            item = _string(item, field_path, {"OK", "ERROR"})
        elif name == "lease":
            item = _integer(item, field_path, 0, MAX_LEASE)
        elif name == "bytes":
            minimum = 1 if op == "RESERVE" else 0
            item = _integer(item, field_path, minimum, MAX_MEMORY_BYTES)
        fields.append((name, item))
    return Event(op, slot, generation, expect, tuple(fields))


def parse_trace(raw: bytes) -> Trace:
    """将不可信 JSON 字节验证为有限、固定字段的轨迹。"""
    if type(raw) is not bytes:
        raise TraceFormatError("$", "输入必须为 bytes")
    if len(raw) > MAX_TRACE_BYTES:
        raise TraceFormatError("$", f"轨迹超过 {MAX_TRACE_BYTES} 字节")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise TraceFormatError("$", "轨迹不是有效 UTF-8") from error
    try:
        data = json.loads(
            text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except TraceFormatError:
        raise
    except json.JSONDecodeError as error:
        raise TraceFormatError("$", f"JSON 格式错误: {error.msg}") from error
    except RecursionError as error:
        raise TraceFormatError("$", "JSON 嵌套层级过深") from error

    top_fields = {
        "schema_version", "trace_id", "task_ids", "invariant_ids", "seed",
        "limits", "finish", "events",
    }
    _exact_keys(data, top_fields, "$")
    schema_version = _integer(data["schema_version"], "schema_version", 1, 1)
    trace_id = _string(data["trace_id"], "trace_id")
    if not TRACE_ID_PATTERN.fullmatch(trace_id):
        raise TraceFormatError("trace_id", "只能含小写字母、数字、点、下划线、斜线和短横线")
    task_ids = _identifier_list(
        data["task_ids"], "task_ids", TASK_IDS, required="#247/TEST-A")
    invariant_ids = _identifier_list(
        data["invariant_ids"], "invariant_ids", INVARIANT_IDS)
    seed = data["seed"]
    if seed is not None:
        seed = _integer(seed, "seed", 0, (1 << 32) - 1)

    limits = data["limits"]
    _exact_keys(limits, {"memory_bytes", "max_events"}, "limits")
    memory_bytes = _integer(
        limits["memory_bytes"], "limits.memory_bytes", 0, MAX_MEMORY_BYTES)
    max_events = _integer(limits["max_events"], "limits.max_events", 1, MAX_EVENTS)
    finish = _string(data["finish"], "finish", {"QUIESCENT", "PARTIAL"})

    raw_events = data["events"]
    if type(raw_events) is not list:
        raise TraceFormatError("events", "必须为 JSON 数组")
    if len(raw_events) > max_events:
        raise TraceFormatError("events", "事件数量超过 limits.max_events")
    events = tuple(_parse_event(event, index)
                   for index, event in enumerate(raw_events))
    return Trace(schema_version, trace_id, task_ids, invariant_ids, seed,
                 memory_bytes, max_events, finish, events)


class ReplayMismatch(ValueError):
    """轨迹期望结果与参考状态机结果不一致。"""

    def __init__(self, trace_id, event_index, operation, expected, actual):
        self.trace_id = trace_id
        self.event_index = event_index
        self.operation = operation
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"{trace_id}: events[{event_index}] {operation}: "
            f"期望 {expected}，实际 {actual}")


class TraceInvariantError(ValueError):
    """轨迹结束条件或模型内部不变量不成立。"""


@dataclass
class _Call:
    slot: int
    generation: int
    request_mode: str
    response_mode: str
    accepted: bool = False
    outcome: str | None = None
    terminal_callbacks: int = 0
    request_count: int = 0
    response_count: int = 0
    request_closed: bool = False
    response_closed: bool = False
    work_active: bool = False

    @property
    def key(self):
        return self.slot, self.generation

    def count(self, direction):
        return self.request_count if direction == "REQUEST" else self.response_count

    def set_count(self, direction, value):
        if direction == "REQUEST":
            self.request_count = value
        else:
            self.response_count = value

    def is_closed(self, direction):
        return self.request_closed if direction == "REQUEST" else self.response_closed

    def close(self, direction):
        if direction == "REQUEST":
            self.request_closed = True
        else:
            self.response_closed = True

    def mode(self, direction):
        return self.request_mode if direction == "REQUEST" else self.response_mode


@dataclass
class _Slot:
    generation: int = 0
    lifecycle: str = "FREE"
    call_key: tuple[int, int] | None = None


@dataclass
class _Lease:
    lease: int
    owner: tuple[int, int]
    size: int
    lifecycle: str


@dataclass
class ReplayResult:
    trace_id: str
    event_results: tuple[str, ...]
    calls: tuple[dict, ...]
    slots: tuple[dict, ...]
    budget: dict
    quiescent: bool


class ModelState:
    """独立的双槽 Call 状态机；不调用生产实现。"""

    def __init__(self, memory_bytes):
        self.limit = memory_bytes
        self.available = memory_bytes
        self.reserved = 0
        self.held = 0
        self.slots = [_Slot(), _Slot()]
        self.calls = {}
        self.leases = {}

    def snapshot(self):
        slots = tuple((slot.generation, slot.lifecycle, slot.call_key)
                      for slot in self.slots)
        calls = tuple(
            (key, call.request_mode, call.response_mode, call.accepted,
             call.outcome, call.terminal_callbacks, call.request_count,
             call.response_count, call.request_closed, call.response_closed,
             call.work_active)
            for key, call in sorted(self.calls.items()))
        leases = tuple(
            (lease_id, lease.owner, lease.size, lease.lifecycle)
            for lease_id, lease in sorted(self.leases.items()))
        budget = (self.limit, self.available, self.reserved, self.held)
        return slots, calls, leases, budget

    def _call_for_event(self, event):
        slot = self.slots[event.slot]
        if event.generation != slot.generation:
            return "STALE", None
        if slot.lifecycle in {"FREE", "RETIRED"} or slot.call_key is None:
            return "REJECTED", None
        call = self.calls[slot.call_key]
        return "APPLIED", call

    def _terminal(self, slot, call, outcome):
        if call.outcome is not None or call.terminal_callbacks != 0:
            return False
        call.outcome = outcome
        call.terminal_callbacks = 1
        slot.lifecycle = "TERMINAL"
        return True

    def _apply_mutating(self, event):
        slot = self.slots[event.slot]
        if event.op == "OPEN":
            if event.generation != slot.generation:
                return "STALE"
            if slot.lifecycle != "FREE":
                return "REJECTED"
            key = (event.slot, event.generation)
            if key in self.calls:
                return "REJECTED"
            call = _Call(
                event.slot, event.generation,
                event.value("request_mode"), event.value("response_mode"))
            self.calls[key] = call
            slot.lifecycle = "OPEN"
            slot.call_key = key
            return "APPLIED"

        lookup, call = self._call_for_event(event)
        if lookup != "APPLIED":
            return lookup

        if event.op == "ACCEPT":
            if slot.lifecycle != "OPEN":
                return "REJECTED"
            call.accepted = True
            slot.lifecycle = "ACCEPTED"
            return "APPLIED"

        if event.op == "REJECT":
            if slot.lifecycle not in {"OPEN", "ACCEPTED"}:
                return "REJECTED"
            return "APPLIED" if self._terminal(slot, call, "REJECTED") else "REJECTED"

        if event.op == "MESSAGE":
            direction = event.value("direction")
            if slot.lifecycle not in {"OPEN", "ACCEPTED"}:
                return "REJECTED"
            if direction == "RESPONSE" and slot.lifecycle != "ACCEPTED":
                return "REJECTED"
            if call.is_closed(direction):
                return "REJECTED"
            count = call.count(direction)
            maximum = 1 if call.mode(direction) == "ONE" else 2
            if count >= maximum:
                return "REJECTED"
            call.set_count(direction, count + 1)
            return "APPLIED"

        if event.op == "HALF_CLOSE":
            direction = event.value("direction")
            if slot.lifecycle not in {"OPEN", "ACCEPTED"}:
                return "REJECTED"
            if direction == "RESPONSE" and slot.lifecycle != "ACCEPTED":
                return "REJECTED"
            if call.is_closed(direction):
                return "REJECTED"
            if call.mode(direction) == "ONE" and call.count(direction) != 1:
                return "REJECTED"
            call.close(direction)
            return "APPLIED"

        if event.op == "WORK_START":
            if slot.lifecycle != "ACCEPTED" or call.work_active:
                return "REJECTED"
            call.work_active = True
            return "APPLIED"

        if event.op == "WORK_DONE":
            if not call.work_active:
                return "REJECTED"
            call.work_active = False
            return "APPLIED"

        if event.op == "FINAL_STATUS":
            if slot.lifecycle != "ACCEPTED":
                return "REJECTED"
            status = event.value("status")
            if status == "OK":
                if not call.request_closed:
                    return "REJECTED"
                if call.response_mode == "ONE" and call.response_count != 1:
                    return "REJECTED"
            return "APPLIED" if self._terminal(slot, call, status) else "REJECTED"

        if event.op in {"CANCEL", "DEADLINE", "RESET", "OWNER_STOP"}:
            if slot.lifecycle not in {"OPEN", "ACCEPTED"}:
                return "REJECTED"
            outcome = {
                "CANCEL": "CANCELLED",
                "DEADLINE": "DEADLINE",
                "RESET": "RESET",
                "OWNER_STOP": "OWNER_STOP",
            }[event.op]
            return "APPLIED" if self._terminal(slot, call, outcome) else "REJECTED"

        if event.op in {"RESERVE", "TRANSFER", "DROP_RESERVATION",
                        "RELEASE_STORAGE"}:
            lease_id = event.value("lease")
            if event.op == "RESERVE":
                if slot.lifecycle != "ACCEPTED":
                    return "REJECTED"
                size = event.value("bytes")
                if lease_id in self.leases:
                    return "REJECTED"
                if size > self.available:
                    return "RESOURCE_LIMIT"
                self.available -= size
                self.reserved += size
                self.leases[lease_id] = _Lease(
                    lease_id, call.key, size, "RESERVED")
                return "APPLIED"

            lease = self.leases.get(lease_id)
            if lease is None or lease.owner != call.key:
                return "REJECTED"
            if event.op == "TRANSFER":
                if slot.lifecycle != "ACCEPTED" or lease.lifecycle != "RESERVED":
                    return "REJECTED"
                self.reserved -= lease.size
                self.held += lease.size
                lease.lifecycle = "HELD"
                return "APPLIED"
            if event.op == "DROP_RESERVATION":
                if lease.lifecycle != "RESERVED":
                    return "REJECTED"
                self.reserved -= lease.size
                self.available += lease.size
                lease.lifecycle = "RELEASED"
                return "APPLIED"
            if lease.lifecycle != "HELD":
                return "REJECTED"
            self.held -= lease.size
            self.available += lease.size
            lease.lifecycle = "RELEASED"
            return "APPLIED"

        if event.op == "SLOT_RELEASE":
            if slot.lifecycle != "TERMINAL":
                return "REJECTED"
            if call.work_active:
                return "REJECTED"
            if any(lease.owner == call.key and lease.lifecycle in {"RESERVED", "HELD"}
                   for lease in self.leases.values()):
                return "REJECTED"
            slot.call_key = None
            if slot.generation == MAX_GENERATION:
                slot.lifecycle = "RETIRED"
            else:
                slot.generation += 1
                slot.lifecycle = "FREE"
            return "APPLIED"

        raise AssertionError(f"未处理的操作: {event.op}")

    def apply(self, event):
        """复制后执行，拒绝或资源失败时保留原始快照。"""
        candidate = copy.deepcopy(self)
        result = candidate._apply_mutating(event)
        if result == "APPLIED":
            candidate.check_invariants()
            self.__dict__.update(candidate.__dict__)
        return result

    def check_invariants(self):
        if self.available + self.reserved + self.held != self.limit:
            raise TraceInvariantError("预算账本不守恒")
        if min(self.available, self.reserved, self.held) < 0:
            raise TraceInvariantError("预算账本出现负数")
        expected_reserved = sum(
            lease.size for lease in self.leases.values()
            if lease.lifecycle == "RESERVED")
        expected_held = sum(
            lease.size for lease in self.leases.values()
            if lease.lifecycle == "HELD")
        if (expected_reserved, expected_held) != (self.reserved, self.held):
            raise TraceInvariantError("租约总额与预算账本不一致")
        for slot_index, slot in enumerate(self.slots):
            if slot.generation < 0 or slot.generation > MAX_GENERATION:
                raise TraceInvariantError("槽代次越界")
            if slot.lifecycle in {"FREE", "RETIRED"}:
                if slot.call_key is not None:
                    raise TraceInvariantError("空闲或退役槽仍关联 Call")
                if slot.lifecycle == "RETIRED" and slot.generation != MAX_GENERATION:
                    raise TraceInvariantError("槽在达到最大代次前退役")
            else:
                if slot.call_key is None or slot.call_key not in self.calls:
                    raise TraceInvariantError("活动槽缺少 Call")
                if slot.call_key[0] != slot_index or slot.call_key[1] != slot.generation:
                    raise TraceInvariantError("槽与 Call 代次不一致")
                call = self.calls[slot.call_key]
                if (slot.lifecycle == "OPEN" and call.accepted) or (
                        slot.lifecycle == "ACCEPTED" and not call.accepted):
                    raise TraceInvariantError("槽受理状态与 Call 不一致")
                if ((slot.lifecycle == "TERMINAL") != (call.outcome is not None)):
                    raise TraceInvariantError("槽终局状态与 Call 不一致")
        for call in self.calls.values():
            if call.terminal_callbacks not in (0, 1):
                raise TraceInvariantError("终局回调不满足至多一次")
            if ((call.outcome is None) != (call.terminal_callbacks == 0)):
                raise TraceInvariantError("Call 终局结果与回调计数不一致")
            if not (0 <= call.request_count <= 2 and 0 <= call.response_count <= 2):
                raise TraceInvariantError("方向消息计数越界")
            if call.request_mode == "ONE" and call.request_count > 1:
                raise TraceInvariantError("ONE 请求方向消息过多")
            if call.response_mode == "ONE" and call.response_count > 1:
                raise TraceInvariantError("ONE 响应方向消息过多")
        for lease_id, lease in self.leases.items():
            if lease_id != lease.lease or lease_id < 0 or lease_id > MAX_LEASE:
                raise TraceInvariantError("租约 ID 无效")
            if lease.owner not in self.calls or lease.size <= 0:
                raise TraceInvariantError("租约没有有效归属")

    def call_summaries(self):
        summaries = []
        for key, call in sorted(self.calls.items()):
            summaries.append({
                "slot": call.slot,
                "generation": call.generation,
                "request_mode": call.request_mode,
                "response_mode": call.response_mode,
                "accepted": call.accepted,
                "outcome": call.outcome,
                "terminal_callbacks": call.terminal_callbacks,
                "message_counts": {
                    "REQUEST": call.request_count,
                    "RESPONSE": call.response_count,
                },
                "half_closed": {
                    "REQUEST": call.request_closed,
                    "RESPONSE": call.response_closed,
                },
                "work_active": call.work_active,
            })
        return tuple(summaries)

    def is_quiescent(self):
        return (
            all(call.outcome is not None and call.terminal_callbacks == 1
                and not call.work_active for call in self.calls.values())
            and all(lease.lifecycle not in {"RESERVED", "HELD"}
                    for lease in self.leases.values())
            and self.reserved == 0 and self.held == 0
            and self.available == self.limit
        )


def run_trace(trace: Trace) -> ReplayResult:
    """确定性执行轨迹，并逐事件比较内嵌的预期结果。"""
    state = ModelState(trace.memory_bytes)
    event_results = []
    for index, event in enumerate(trace.events):
        before = state.snapshot()
        actual = state.apply(event)
        if actual in {"STALE", "REJECTED", "RESOURCE_LIMIT"}:
            if state.snapshot() != before:
                raise TraceInvariantError(
                    f"{trace.trace_id}: events[{index}] 失败事件改变状态")
        state.check_invariants()
        event_results.append(actual)
        if actual != event.expect:
            raise ReplayMismatch(
                trace.trace_id, index, event.op, event.expect, actual)

    quiescent = state.is_quiescent()
    if trace.finish == "QUIESCENT" and not quiescent:
        raise TraceInvariantError(f"{trace.trace_id}: 轨迹结束时尚未静默")
    slots = tuple({
        "slot": index,
        "generation": slot.generation,
        "state": slot.lifecycle,
    } for index, slot in enumerate(state.slots))
    return ReplayResult(
        trace.trace_id,
        tuple(event_results),
        state.call_summaries(),
        slots,
        {"limit": state.limit, "available": state.available,
         "reserved": state.reserved, "held": state.held},
        quiescent,
    )


def serialize_result(result: ReplayResult) -> str:
    """序列化稳定排序、无时间戳的 JSON 摘要。"""
    data = {
        "trace_id": result.trace_id,
        "event_results": result.event_results,
        "calls": result.calls,
        "slots": result.slots,
        "budget": result.budget,
        "quiescent": result.quiescent,
    }
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def main(argv=None):
    parser = argparse.ArgumentParser(description="RPC V2 状态轨迹确定性回放")
    parser.add_argument("--trace", required=True, type=Path,
                        help="UTF-8 JSON 轨迹文件")
    args = parser.parse_args(argv)
    try:
        trace = parse_trace(args.trace.read_bytes())
        result = run_trace(trace)
    except (OSError, TraceFormatError, ReplayMismatch, TraceInvariantError) as error:
        print(str(error), file=sys.stderr)
        return 1
    sys.stdout.write(serialize_result(result) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
