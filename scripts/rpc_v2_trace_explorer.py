#!/usr/bin/env python3
"""RPC V2 固定种子有限状态探索与失败轨迹缩减。"""
import argparse
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
import json
from pathlib import Path
import sys

from rpc_v2_model import (
    Event,
    INVARIANT_IDS,
    MAX_LEASE,
    MAX_MEMORY_BYTES,
    MAX_SLOT,
    ModelState,
    OPERATIONS,
    Trace,
    run_trace,
)


MAX_SEED = (1 << 32) - 1
MAX_CASES = 64
MAX_GENERATED_EVENTS = 48
ZERO_SEED_STATE = 0x6D2B79F5
TASK_IDS = ("#247/TEST-A", "#247/TEST-C")
ROOT = Path(__file__).resolve().parent.parent
_OPERATION_ORDER = tuple(sorted(OPERATIONS))
_PROGRESS_OPERATIONS = frozenset({
    "OPEN", "ACCEPT", "MESSAGE", "HALF_CLOSE", "WORK_START",
    "WORK_DONE", "RESERVE", "TRANSFER", "DROP_RESERVATION",
    "RELEASE_STORAGE", "SLOT_RELEASE",
})
_TERMINAL_OPERATIONS = frozenset({
    "REJECT", "FINAL_STATUS", "CANCEL", "DEADLINE", "RESET", "OWNER_STOP",
})


class _StoreOnce(argparse.Action):
    def __call__(self, parser: argparse.ArgumentParser,
                 namespace: argparse.Namespace, values: object,
                 option_string: str | None = None) -> None:
        if getattr(namespace, self.dest, None) is not None:
            parser.error(f"参数 {option_string} 不得重复")
        setattr(namespace, self.dest, values)


def _cli_decimal(value: str) -> int:
    if not value or any(character < "0" or character > "9"
                        for character in value):
        raise argparse.ArgumentTypeError("必须是十进制非负整数")
    return int(value, 10)


class TraceExplorerInputError(ValueError):
    """探索参数超出固定上限。"""


def _validate_seed(seed: int) -> None:
    if type(seed) is not int or not 0 <= seed <= MAX_SEED:
        raise TraceExplorerInputError(
            f"seed 必须是 0 到 {MAX_SEED} 之间的整数")


def _validate_max_events(max_events: int) -> None:
    if (type(max_events) is not int
            or not 1 <= max_events <= MAX_GENERATED_EVENTS):
        raise TraceExplorerInputError(
            f"max_events 必须是 1 到 {MAX_GENERATED_EVENTS} 之间的整数")


def _validate_cases(cases: int) -> None:
    if type(cases) is not int or not 1 <= cases <= MAX_CASES:
        raise TraceExplorerInputError(
            f"cases 必须是 1 到 {MAX_CASES} 之间的整数")


class _XorShift32:
    def __init__(self, seed: int) -> None:
        self.state = seed if seed else ZERO_SEED_STATE

    def next(self) -> int:
        value = self.state
        value ^= (value << 13) & MAX_SEED
        value ^= value >> 17
        value ^= (value << 5) & MAX_SEED
        self.state = value & MAX_SEED
        return self.state


def _event(op: str, slot: int, generation: int,
           **fields: int | str) -> Event:
    return Event(op, slot, generation, "APPLIED", tuple(sorted(fields.items())))


def _random_event(op: str, state: ModelState, rng: _XorShift32,
                  slot_index: int | None = None, stale: bool = False) -> Event:
    if slot_index is None:
        slot_index = rng.next() % (MAX_SLOT + 1)
    generation = state.slots[slot_index].generation
    if stale:
        generation = (generation - 1 if generation > 0 else generation + 1)

    fields = {}
    if op == "OPEN":
        fields = {
            "request_mode": ("ONE", "MANY")[rng.next() % 2],
            "response_mode": ("ONE", "MANY")[rng.next() % 2],
        }
    elif op == "MESSAGE":
        fields = {
            "direction": ("REQUEST", "RESPONSE")[rng.next() % 2],
            "bytes": rng.next() % (MAX_MEMORY_BYTES + 1),
        }
    elif op == "HALF_CLOSE":
        fields = {"direction": ("REQUEST", "RESPONSE")[rng.next() % 2]}
    elif op == "FINAL_STATUS":
        fields = {"status": ("OK", "ERROR")[rng.next() % 2]}
    elif op == "RESERVE":
        fields = {
            "lease": rng.next() % (MAX_LEASE + 1),
            "bytes": 1 + rng.next() % MAX_MEMORY_BYTES,
        }
    elif op in {"TRANSFER", "DROP_RESERVATION", "RELEASE_STORAGE"}:
        leases = [lease_id for lease_id in range(MAX_LEASE + 1)
                  if lease_id in state.leases]
        fields = {"lease": leases[rng.next() % len(leases)]
                  if leases else rng.next() % (MAX_LEASE + 1)}
    return _event(op, slot_index, generation, **fields)


def _executable_events(state: ModelState, rng: _XorShift32) -> list[Event]:
    candidates = []
    for slot_index, slot in enumerate(state.slots):
        call = state.calls.get(slot.call_key) if slot.call_key is not None else None
        if slot.lifecycle == "FREE":
            candidates.append(_event(
                "OPEN", slot_index, slot.generation,
                request_mode=("ONE", "MANY")[rng.next() % 2],
                response_mode=("ONE", "MANY")[rng.next() % 2]))
            continue
        if slot.lifecycle == "RETIRED":
            continue

        if slot.lifecycle == "OPEN":
            candidates.append(_event("ACCEPT", slot_index, slot.generation))
        if slot.lifecycle in {"OPEN", "ACCEPTED"}:
            candidates.extend(_event(op, slot_index, slot.generation)
                              for op in ("REJECT", "CANCEL", "DEADLINE",
                                         "RESET", "OWNER_STOP"))
            for direction in ("REQUEST", "RESPONSE"):
                if direction == "RESPONSE" and slot.lifecycle != "ACCEPTED":
                    continue
                if not call.is_closed(direction):
                    count = call.count(direction)
                    maximum = 1 if call.mode(direction) == "ONE" else 2
                    if count < maximum:
                        candidates.append(_event(
                            "MESSAGE", slot_index, slot.generation,
                            direction=direction,
                            bytes=rng.next() % (MAX_MEMORY_BYTES + 1)))
                    if call.mode(direction) == "MANY" or count == 1:
                        candidates.append(_event(
                            "HALF_CLOSE", slot_index, slot.generation,
                            direction=direction))

        if slot.lifecycle == "ACCEPTED":
            candidates.append(_event(
                "FINAL_STATUS", slot_index, slot.generation, status="ERROR"))
            if (call.request_closed
                    and (call.response_mode == "MANY" or call.response_count == 1)):
                candidates.append(_event(
                    "FINAL_STATUS", slot_index, slot.generation, status="OK"))
            if not call.work_active:
                candidates.append(_event("WORK_START", slot_index, slot.generation))
            if state.available > 0:
                free_leases = [lease for lease in range(MAX_LEASE + 1)
                               if lease not in state.leases]
                if free_leases:
                    candidates.append(_event(
                        "RESERVE", slot_index, slot.generation,
                        lease=free_leases[rng.next() % len(free_leases)],
                        bytes=1 + rng.next() % state.available))

        if call is not None and call.work_active:
            candidates.append(_event("WORK_DONE", slot_index, slot.generation))

        if call is not None:
            owned = [lease for lease in state.leases.values()
                     if lease.owner == call.key]
            for lease in owned:
                if lease.lifecycle == "RESERVED":
                    candidates.append(_event(
                        "DROP_RESERVATION", slot_index, slot.generation,
                        lease=lease.lease))
                    if slot.lifecycle == "ACCEPTED":
                        candidates.append(_event(
                            "TRANSFER", slot_index, slot.generation,
                            lease=lease.lease))
                elif lease.lifecycle == "HELD":
                    candidates.append(_event(
                        "RELEASE_STORAGE", slot_index, slot.generation,
                        lease=lease.lease))

            if slot.lifecycle == "TERMINAL":
                active_lease = any(lease.owner == call.key
                                   and lease.lifecycle in {"RESERVED", "HELD"}
                                   for lease in state.leases.values())
                if not call.work_active and not active_lease:
                    candidates.append(_event(
                        "SLOT_RELEASE", slot_index, slot.generation))

    return candidates


def _invalid_event(state: ModelState, rng: _XorShift32,
                   seed: int, index: int) -> Event:
    kind = (seed + index // 5) % 4
    if kind == 0:
        return _random_event(
            _OPERATION_ORDER[rng.next() % len(_OPERATION_ORDER)],
            state, rng, stale=True)

    if kind == 1:
        occupied = [index for index, slot in enumerate(state.slots)
                    if slot.lifecycle != "FREE"]
        if occupied:
            slot_index = occupied[rng.next() % len(occupied)]
            return _random_event("OPEN", state, rng, slot_index=slot_index)
        free = [index for index, slot in enumerate(state.slots)
                if slot.lifecycle == "FREE"]
        return _random_event("ACCEPT", state, rng,
                             slot_index=free[rng.next() % len(free)])

    if kind == 3:
        duplicates = []
        for slot_index, slot in enumerate(state.slots):
            if slot.lifecycle == "TERMINAL":
                duplicates.append(_event(
                    "CANCEL", slot_index, slot.generation))
                continue
            if slot.call_key is None:
                continue
            call = state.calls[slot.call_key]
            if slot.lifecycle not in {"OPEN", "ACCEPTED"}:
                continue
            for lease in state.leases.values():
                if lease.owner == call.key and lease.lifecycle == "RELEASED":
                    duplicates.append(_event(
                        "RELEASE_STORAGE", slot_index, slot.generation,
                        lease=lease.lease))
        if duplicates:
            return duplicates[rng.next() % len(duplicates)]

    for slot_index, slot in enumerate(state.slots):
        if slot.lifecycle != "ACCEPTED" or state.available >= MAX_MEMORY_BYTES:
            continue
        free_leases = [lease for lease in range(MAX_LEASE + 1)
                       if lease not in state.leases]
        if free_leases:
            return _event(
                "RESERVE", slot_index, slot.generation,
                lease=free_leases[rng.next() % len(free_leases)],
                bytes=MAX_MEMORY_BYTES)

    occupied = [index for index, slot in enumerate(state.slots)
                if slot.lifecycle != "FREE"]
    if occupied:
        slot_index = occupied[rng.next() % len(occupied)]
        return _random_event("OPEN", state, rng, slot_index=slot_index)
    free = [index for index, slot in enumerate(state.slots)
            if slot.lifecycle == "FREE"]
    return _random_event("ACCEPT", state, rng,
                         slot_index=free[rng.next() % len(free)])


def generate_trace(seed: int, max_events: int) -> Trace:
    """以固定 xorshift32 状态生成有界、可回放的 schema v1 轨迹。"""
    _validate_seed(seed)
    _validate_max_events(max_events)
    rng = _XorShift32(seed)
    state = ModelState(MAX_MEMORY_BYTES)
    events = []

    for index in range(max_events):
        if index % 5 == 4:
            event = _invalid_event(state, rng, seed, index)
        else:
            candidates = _executable_events(state, rng)
            if not candidates:
                event = _invalid_event(state, rng, seed, index)
            else:
                progress = [item for item in candidates
                            if item.op in _PROGRESS_OPERATIONS]
                terminal = [item for item in candidates
                            if item.op in _TERMINAL_OPERATIONS]
                if rng.next() % 5 == 0 and terminal:
                    event = terminal[rng.next() % len(terminal)]
                elif progress:
                    event = progress[rng.next() % len(progress)]
                elif terminal:
                    event = terminal[rng.next() % len(terminal)]
                else:
                    event = candidates[rng.next() % len(candidates)]

        result = "APPLIED"
        try:
            result = state.apply(event)
            state.check_invariants()
        except Exception:
            events.append(Event(event.op, event.slot, event.generation, result,
                                event.fields))
            break
        events.append(Event(event.op, event.slot, event.generation, result,
                            event.fields))

    trace = Trace(
        schema_version=1,
        trace_id=f"test-c1-seed-{seed:010d}",
        task_ids=TASK_IDS,
        invariant_ids=tuple(sorted(INVARIANT_IDS)),
        seed=seed,
        memory_bytes=MAX_MEMORY_BYTES,
        max_events=max_events,
        finish="PARTIAL",
        events=tuple(events),
    )
    return trace


def _trace_data(trace: Trace) -> dict[str, object]:
    return {
        "schema_version": trace.schema_version,
        "trace_id": trace.trace_id,
        "task_ids": list(trace.task_ids),
        "invariant_ids": list(trace.invariant_ids),
        "seed": trace.seed,
        "limits": {
            "memory_bytes": trace.memory_bytes,
            "max_events": trace.max_events,
        },
        "finish": trace.finish,
        "events": [
            {"op": event.op, "slot": event.slot,
             "generation": event.generation, **dict(event.fields),
             "expect": event.expect}
            for event in trace.events
        ],
    }


def serialize_trace(trace: Trace) -> str:
    """以固定键序和分隔符序列化 schema v1 轨迹。"""
    raw = json.dumps(
        _trace_data(trace), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"))
    return raw


@dataclass(frozen=True)
class FailureSignature:
    kind: str
    message: str


@dataclass(frozen=True)
class FailureReport:
    seed: int
    signature: FailureSignature
    original: Trace
    minimized: Trace


@dataclass(frozen=True)
class ExplorationSummary:
    seed: int
    cases: int
    max_events: int
    events: int
    operation_counts: dict
    result_counts: dict


def failure_signature(
        trace: Trace, probe: Callable[[Trace], None]) -> FailureSignature | None:
    """执行 probe；失败时返回稳定的异常类型和文本。"""
    try:
        probe(trace)
    except Exception as error:
        return FailureSignature(type(error).__name__, str(error))
    return None


def _normalized_candidate(trace: Trace, events: Sequence[Event]) -> Trace:
    state = ModelState(trace.memory_bytes)
    normalized = []
    for event in events:
        result = "APPLIED"
        try:
            result = state.apply(event)
            state.check_invariants()
        except Exception:
            normalized.append(replace(event, expect=result))
            return replace(trace, max_events=max(1, len(normalized)),
                           events=tuple(normalized))
        normalized.append(replace(event, expect=result))
    return replace(trace, max_events=max(1, len(normalized)),
                   events=tuple(normalized))


def shrink_trace(trace: Trace, probe: Callable[[Trace], None],
                 signature: FailureSignature) -> Trace:
    """按固定 ddmin 顺序删除事件，保留同一失败签名。"""
    if not isinstance(signature, FailureSignature):
        raise TypeError("signature 必须是 FailureSignature")
    if failure_signature(trace, probe) != signature:
        raise ValueError("输入轨迹未重现给定 failure signature")

    current = trace
    granularity = 2
    while len(current.events) >= 2:
        count = len(current.events)
        chunk_size = (count + granularity - 1) // granularity
        reduced = False
        for start in range(0, count, chunk_size):
            candidate_events = (current.events[:start]
                                + current.events[start + chunk_size:])
            candidate = _normalized_candidate(current, candidate_events)
            if failure_signature(candidate, probe) == signature:
                current = candidate
                granularity = max(2, granularity - 1)
                reduced = True
                break
        if reduced:
            continue
        if granularity >= count:
            break
        granularity = min(count, granularity * 2)

    changed = True
    while changed and current.events:
        changed = False
        for index in range(len(current.events)):
            candidate_events = (current.events[:index]
                                + current.events[index + 1:])
            candidate = _normalized_candidate(current, candidate_events)
            if failure_signature(candidate, probe) == signature:
                current = candidate
                changed = True
                break
    return current


def explore(seed: int, cases: int, max_events: int,
            failure_probe: Callable[[Trace], None] | None = None
            ) -> ExplorationSummary | FailureReport:
    """执行固定 seed 区间；首个失败返回原始和最小轨迹。"""
    _validate_seed(seed)
    _validate_cases(cases)
    _validate_max_events(max_events)
    probe = run_trace if failure_probe is None else failure_probe
    operation_counts = {}
    result_counts = {}
    event_total = 0

    for case_index in range(cases):
        case_seed = (seed + case_index) & MAX_SEED
        trace = generate_trace(case_seed, max_events)
        signature = failure_signature(trace, probe)
        if signature is not None:
            minimized = shrink_trace(trace, probe, signature)
            return FailureReport(case_seed, signature, trace, minimized)
        for event in trace.events:
            operation_counts[event.op] = operation_counts.get(event.op, 0) + 1
            result_counts[event.expect] = result_counts.get(event.expect, 0) + 1
            event_total += 1

    return ExplorationSummary(
        seed, cases, max_events, event_total,
        dict(sorted(operation_counts.items())),
        dict(sorted(result_counts.items())))


def serialize_summary(summary: ExplorationSummary) -> str:
    return json.dumps({
        "seed": summary.seed,
        "cases": summary.cases,
        "max_events": summary.max_events,
        "events": summary.events,
        "operation_counts": summary.operation_counts,
        "result_counts": summary.result_counts,
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def serialize_failure_report(report: FailureReport) -> str:
    return json.dumps({
        "seed": report.seed,
        "signature": {
            "kind": report.signature.kind,
            "message": report.signature.message,
        },
        "original": _trace_data(report.original),
        "minimized": _trace_data(report.minimized),
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def main(argv: list[str] | None = None, *,
         failure_probe: Callable[[Trace], None] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="RPC V2 固定种子有限状态模型探索与失败轨迹缩减")
    parser.add_argument("--seed", type=_cli_decimal, action=_StoreOnce,
                        help=f"起始 seed（0–{MAX_SEED}，默认 33047）")
    parser.add_argument("--cases", type=_cli_decimal, action=_StoreOnce,
                        help=f"连续探索 seed 数（1–{MAX_CASES}，默认 64）")
    parser.add_argument(
        "--max-events", type=_cli_decimal, action=_StoreOnce,
        help=f"每条轨迹事件上限（1–{MAX_GENERATED_EVENTS}，默认 48）")
    parser.add_argument(
        "--failure-dir", type=Path, action=_StoreOnce,
        help="失败时保存 original.json 与 minimized.json 的目录")
    try:
        args = parser.parse_args(argv)
    except SystemExit as error:
        return error.code

    seed = 33047 if args.seed is None else args.seed
    cases = MAX_CASES if args.cases is None else args.cases
    max_events = MAX_GENERATED_EVENTS if args.max_events is None else args.max_events
    failure_dir = (args.failure_dir if args.failure_dir is not None else
                   ROOT / "build" / "test-tmp" / "rpc-v2-trace-explorer")
    try:
        result = explore(seed, cases, max_events, failure_probe=failure_probe)
    except TraceExplorerInputError as error:
        sys.stderr.write(f"参数错误: {error}\n")
        return 2

    if isinstance(result, ExplorationSummary):
        sys.stdout.write(serialize_summary(result) + "\n")
        return 0

    sys.stdout.write(serialize_failure_report(result) + "\n")
    try:
        failure_dir.mkdir(parents=True, exist_ok=True)
        (failure_dir / "original.json").write_text(
            serialize_trace(result.original) + "\n", encoding="utf-8")
        (failure_dir / "minimized.json").write_text(
            serialize_trace(result.minimized) + "\n", encoding="utf-8")
    except OSError as error:
        sys.stderr.write(f"失败轨迹写入失败: {error}\n")
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
