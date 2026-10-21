#!/usr/bin/env python3
"""Compare public V2 call and message limits under fixed-rate TCP load."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

from run_rpc_bench import metadata as base_metadata
from run_rpc_capacity import run_case as run_capacity_case
from run_rpc_scalability import (
    exact_wall_signals,
    generator_count_for,
    parse_int_list,
    run_multisource_case,
    SERVER_PRESSURE_FIELDS,
)


def run_profile_case(binary: Path, case: dict[str, Any],
                     output_dir: Path) -> dict[str, Any]:
    result = (run_capacity_case(binary, case, output_dir)
              if case["generators"] == 1
              else run_multisource_case(binary, case, output_dir))
    result["type"] = "headroom_case"
    result["schema"] = 2
    result["api_generation"] = "rpc_v2"
    result["public_profile"] = case["public_profile"]
    result["wall_signals"] = exact_wall_signals(result)
    return result


def server_pressure(result: dict[str, Any]) -> bool:
    return any(result["server_exit"][key]
               for key in SERVER_PRESSURE_FIELDS.values())


def compare_profiles(before: list[dict[str, Any]], after: list[dict[str, Any]],
                     before_name: str, after_name: str) -> dict[str, Any]:
    if not before or len(before) != len(after):
        raise ValueError("profile comparison requires matching non-empty cases")
    before_by_rate = {row["case"]["rate_rps"]: row for row in before}
    after_by_rate = {row["case"]["rate_rps"]: row for row in after}
    if before_by_rate.keys() != after_by_rate.keys():
        raise ValueError("profile rates do not match")
    rates = sorted(before_by_rate)
    signal_changes = {}
    for rate in rates:
        before_signals = sorted(before_by_rate[rate]["wall_signals"])
        after_signals = sorted(after_by_rate[rate]["wall_signals"])
        if before_signals != after_signals:
            signal_changes[str(rate)] = {
                before_name: before_signals,
                after_name: after_signals,
            }
    before_pressure = [rate for rate in rates
                       if server_pressure(before_by_rate[rate])]
    after_pressure = [rate for rate in rates
                      if server_pressure(after_by_rate[rate])]
    before_case = before[0]["case"]
    after_case = after[0]["case"]
    return {
        "type": "headroom_comparison", "schema": 2,
        "api_generation": "rpc_v2",
        "before_profile": before_name, "after_profile": after_name,
        "rates_rps": rates,
        "before_capacity": before_case["capacity"],
        "after_capacity": after_case["capacity"],
        "before_bulk_bytes": before_case["bulk_bytes"],
        "after_bulk_bytes": after_case["bulk_bytes"],
        "before_first_server_pressure_rps": min(before_pressure) if before_pressure else None,
        "after_first_server_pressure_rps": min(after_pressure) if after_pressure else None,
        "before_ok": [before_by_rate[rate]["client"]["ok"] for rate in rates],
        "after_ok": [after_by_rate[rate]["client"]["ok"] for rate in rates],
        "before_calls_started": [before_by_rate[rate]["server_exit"]["rpc_calls_started"]
                                 for rate in rates],
        "after_calls_started": [after_by_rate[rate]["server_exit"]["rpc_calls_started"]
                                for rate in rates],
        "server_signal_changes": signal_changes,
    }


def metadata(binary: Path, args: argparse.Namespace) -> dict[str, Any]:
    row = base_metadata(binary, args.label, load_mode="open")
    row.update({
        "type": "headroom_metadata", "schema": 2,
        "api_generation": "rpc_v2",
        "profiles": args.profiles,
        "rates_rps": args.rates,
        "window": args.window,
        "max_generators": args.max_generators,
        "target_rate_per_generator": args.target_rate_per_generator,
        "slow_ms": args.slow_ms,
        "load_model": "fixed-rate V2 unary arrivals; profile dimensions are "
                       "public max_calls/max_streams and message limits",
    })
    return row


def validate_profiles(parser: argparse.ArgumentParser,
                      profiles: list[dict[str, int]], window: int) -> None:
    if len(profiles) < 2:
        parser.error("at least two public profiles are required")
    for profile in profiles:
        if not 1 <= profile["capacity"] <= 256:
            parser.error("profile capacity must be in 1..256")
        if profile["capacity"] < window:
            parser.error("every profile capacity must cover the client window")
        if not 8 <= profile["bulk_bytes"] <= 1_048_576:
            parser.error("profile bulk-bytes must be in 8..1048576")
        if profile["capacity"] * (profile["bulk_bytes"] + 512) > 67_108_864:
            parser.error("profile exceeds the benchmark memory guard")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="public-v2-headroom")
    parser.add_argument("--rates", type=parse_int_list, default=[250, 1000, 4000])
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--window", type=int, default=16)
    parser.add_argument("--slow-ms", type=int, default=0)
    parser.add_argument("--max-generators", type=int, default=4)
    parser.add_argument("--target-rate-per-generator", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not 16 <= args.requests <= 1_000_000 or not 1 <= args.window <= 256:
        parser.error("invalid request count or window")
    if not 0 <= args.slow_ms <= 1000 or not 1 <= args.max_generators <= 16:
        parser.error("invalid delay or generator count")
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")

    args.profiles = [
        {"name": "small_limits", "capacity": 32, "bulk_bytes": 16384},
        {"name": "larger_call_limit", "capacity": 64, "bulk_bytes": 16384},
        {"name": "larger_message_limit", "capacity": 32, "bulk_bytes": 65536},
    ]
    if args.smoke:
        args.rates = [100, 800]
        args.requests = 32
        args.window = min(args.window, 8)
        args.max_generators = 2
        args.target_rate_per_generator = 400
        args.profiles = args.profiles[:2]
    validate_profiles(parser, args.profiles, args.window)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    results_by_profile: dict[str, list[dict[str, Any]]] = {}
    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(args.binary, args), allow_nan=False) + "\n")
        output.flush()
        for profile in args.profiles:
            rows: list[dict[str, Any]] = []
            for index, rate in enumerate(args.rates):
                generators = generator_count_for(
                    rate, args.max_generators, args.target_rate_per_generator)
                case = {
                    "trial": 1, "rate_rps": rate,
                    "requests": max(args.requests, generators * 8),
                    "warmup": max(8, generators), "slow_ms": args.slow_ms,
                    "window": args.window, "capacity": profile["capacity"],
                    "bulk_bytes": profile["bulk_bytes"],
                    "generator_capacity": max(profile["capacity"], args.window),
                    "generator_window": args.window,
                    "timeout_ms": args.timeout_ms, "generators": generators,
                    "public_profile": profile["name"],
                }
                case_dir = (args.output.parent / (args.output.stem + "-logs") /
                            f"{profile['name']}-{index}-{rate}rps")
                result = run_profile_case(args.binary, case, case_dir)
                result["type"] = "headroom_case"
                result["schema"] = 2
                result["api_generation"] = "rpc_v2"
                result["public_profile"] = profile["name"]
                result["wall_signals"] = exact_wall_signals(result)
                rows.append(result)
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                print(f"profile={profile['name']} rate={rate}rps "
                      f"accepted={result['client']['accepted']}/"
                      f"{result['client']['offered']} "
                      f"calls={result['server_exit']['rpc_calls_finished']} "
                      f"signals={','.join(result['wall_signals']) or 'none'}",
                      flush=True)
            results_by_profile[profile["name"]] = rows

        for name, rows in results_by_profile.items():
            summary = {
                "type": "headroom_profile_summary", "schema": 2,
                "api_generation": "rpc_v2", "public_profile": name,
                "capacity": rows[0]["case"]["capacity"],
                "bulk_bytes": rows[0]["case"]["bulk_bytes"],
                "clean_cases": sum(not row["wall_signals"] for row in rows),
                "case_count": len(rows),
                "ok_total": sum(row["client"]["ok"] for row in rows),
                "server_calls_finished_total": sum(
                    row["server_exit"]["rpc_calls_finished"] for row in rows),
            }
            output.write(json.dumps(summary, allow_nan=False) + "\n")
            output.flush()

        names = list(results_by_profile)
        for before_name, after_name in zip(names, names[1:]):
            comparison = compare_profiles(
                results_by_profile[before_name], results_by_profile[after_name],
                before_name, after_name)
            output.write(json.dumps(comparison, allow_nan=False) + "\n")
            output.flush()

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"headroom benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
