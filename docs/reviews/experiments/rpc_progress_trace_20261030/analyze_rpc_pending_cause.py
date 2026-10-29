#!/usr/bin/env python3
"""复算 RPC pending OPEN/REFUSED hold 的解除来源与等待时长。"""
from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
from typing import Any, Optional


def json_lines(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"{path}:{line_number}: 非法 JSON") from error
        if isinstance(row, dict):
            rows.append(row)
    return rows


def nearest_rank(values: list[float], quantile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def summarize_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for event in events:
        key = (event["kind"], event["trigger"], event["end_reason"],
               event["send_accepted"])
        groups.setdefault(key, []).append(event)
    result = []
    for (kind, trigger, end_reason, send_accepted), items in sorted(groups.items()):
        durations_ms = [item["duration_ns"] / 1_000_000 for item in items]
        progress_histogram = Counter(item["progress_calls"] for item in items)
        attempt_histogram = Counter(item["send_attempts"] for item in items)
        result.append({
            "kind": kind,
            "trigger": trigger,
            "end_reason": end_reason,
            "send_accepted": send_accepted,
            "count": len(items),
            "duration_ms_nearest_rank": {
                "p50": nearest_rank(durations_ms, 0.50),
                "p90": nearest_rank(durations_ms, 0.90),
                "p99": nearest_rank(durations_ms, 0.99),
                "max": max(durations_ms),
            },
            "progress_calls_histogram": {
                str(key): value for key, value in sorted(progress_histogram.items())
            },
            "send_attempts_histogram": {
                str(key): value for key, value in sorted(attempt_histogram.items())
            },
        })
    return result


def analyze_profile(log_path: Path, case: dict[str, Any]) -> dict[str, Any]:
    rows = json_lines(log_path)
    events = [row for row in rows if row.get("type") == "channel_pending_probe"]
    summaries = [row for row in rows
                 if row.get("type") == "channel_pending_probe_summary"]
    if len(summaries) != 1:
        raise ValueError(f"{log_path}: 需要且仅需要一条探针汇总")
    probe_summary = summaries[0]
    if probe_summary["completed"] != len(events):
        raise ValueError(f"{log_path}: 事件数与 completed 不一致")
    if probe_summary["unpublished"] or probe_summary["overflow"]:
        raise ValueError(f"{log_path}: 探针记录不完整")
    if any(event["end_ns"] < event["start_ns"] for event in events):
        raise ValueError(f"{log_path}: 单调时钟区间逆序")
    server = case["server_exit"]
    return {
        "profile": case["public_profile"],
        "repeatability": {
            "wall_signals": case["wall_signals"],
            "clean": not case["wall_signals"],
            "ok_rps": case["client"]["ok_rps"],
            "ok": case["client"]["ok"],
            "offered": case["client"]["offered"],
        },
        "server": {
            "control_tx_pool_capacity": server["control_tx_pool_capacity"],
            "control_tx_pool_peak": server["control_tx_pool_peak"],
            "control_tx_pool_exhausted_events": server[
                "control_tx_pool_exhausted_events"],
            "command_queue_peak": server["command_queue_peak"],
            "command_queue_full_events": server["command_queue_full_events"],
            "rpc_calls_started": server["rpc_calls_started"],
            "rpc_calls_finished": server["rpc_calls_finished"],
        },
        "probe": {
            **probe_summary,
            "events": len(events),
            "groups": summarize_events(events),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", type=Path, required=True)
    parser.add_argument("--repeatability", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    rows = json_lines(args.repeatability)
    metadata = next((row for row in rows
                     if row.get("type") == "repeatability_metadata"), None)
    cases = [row for row in rows if row.get("type") == "repeatability_case"]
    if metadata is None or len(cases) != 3:
        parser.error("输入必须包含 metadata 与三个 profile case")
    profiles = []
    for case in sorted(cases, key=lambda row: row["public_profile"]):
        log_path = (args.logs_dir / f"t{case['case']['trial']}-"
                    f"{case['public_profile']}-{case['case']['rate_rps']}rps"
                    / "server.stderr")
        profiles.append(analyze_profile(log_path, case))

    result = {
        "type": "rpc_pending_cause_summary",
        "schema": 1,
        "git_revision": metadata["git_revision"],
        "binary_sha256": metadata["binary_sha256"],
        "environment": {
            "platform": metadata["platform"],
            "cpu_model": metadata["cpu_model"],
            "affinity": metadata["affinity"],
            "transport": metadata["transport"],
            "tls_enabled": metadata["tls_enabled"],
            "compression": metadata["compression"],
        },
        "workload": {
            "trials": metadata["trials"],
            "rate_rps": metadata["rate_rps"],
            "requests": metadata["requests"],
            "generators": metadata["generators"],
            "min_arrival_ms": 250,
            "generator_window": metadata["generator_window"],
            "generator_capacity": metadata["generator_capacity"],
        },
        "profiles": profiles,
        "limitations": [
            "单次诊断运行，且三个 profile 均未达到 clean 判定。",
            "本探针记录 pending 状态及解除来源，不采样 FIONREAD。",
            "hold 是部分请求等待的子集因素；本结果不证明其为整体长尾的唯一根因。",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")
    print(f"profiles={len(profiles)} events="
          f"{sum(item['probe']['events'] for item in profiles)} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
