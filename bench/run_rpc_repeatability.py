#!/usr/bin/env python3
"""Repeat 40k RPC load to distinguish stable walls from shared-runner noise."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
from typing import Any

from run_rpc_bench import metadata as base_metadata
from run_rpc_headroom import run_profile_case, server_pressure
from run_rpc_scalability import (
    SERVER_PRESSURE_FIELDS,
    clean_result,
    exact_wall_signals,
    generator_count_for,
)


PROFILE_SPECS = {
    "baseline": {
        "rx_buffer_count": 0,
        "executor_queue": 64,
        "control_tx_item_count": 0,
        "command_capacity": 0,
    },
    "command_headroom": {
        "rx_buffer_count": 8192,
        "executor_queue": 256,
        "control_tx_item_count": 2048,
        "command_capacity": 4096,
    },
    "ceiling_headroom": {
        "rx_buffer_count": 8192,
        "executor_queue": 1024,
        "control_tx_item_count": 8192,
        "command_capacity": 4096,
    },
    "command_ceiling_headroom": {
        "rx_buffer_count": 8192,
        "executor_queue": 1024,
        "control_tx_item_count": 8192,
        "command_capacity": 16384,
    },
}


def summarize(profile: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise ValueError("repeatability summary requires results")

    signal_counts: dict[str, int] = {}
    server_signal_counts: dict[str, int] = {}
    for result in results:
        for signal in exact_wall_signals(result):
            signal_counts[signal] = signal_counts.get(signal, 0) + 1
        server = result["server_exit"]
        for signal, key in SERVER_PRESSURE_FIELDS.items():
            if server[key]:
                server_signal_counts[signal] = (
                    server_signal_counts.get(signal, 0) + 1)

    trials = len(results)
    reproducible_server_signals = sorted(
        signal for signal, count in server_signal_counts.items()
        if count == trials)
    sporadic_server_signals = sorted(
        signal for signal, count in server_signal_counts.items()
        if count < trials)
    pressure_trials = sum(server_pressure(result) for result in results)
    if reproducible_server_signals:
        classification = "reproducible_server_wall"
    elif pressure_trials:
        classification = "non_reproducible_server_wall"
    else:
        classification = "no_server_wall"

    ok_rps = [result["client"]["ok_rps"] for result in results]
    busy = [result["server_exit"]["reactor_busy_ratio"] for result in results]
    command_hits = [
        result["server_exit"]["command_budget_hits"] for result in results]
    command_peak = [
        result["server_exit"]["command_queue_peak"] for result in results]

    return {
        "type": "repeatability_summary",
        "schema": 1,
        "profile": profile,
        "trials": trials,
        "clean_trials": sum(clean_result(result) for result in results),
        "server_pressure_trials": pressure_trials,
        "classification": classification,
        "signal_counts": dict(sorted(signal_counts.items())),
        "server_signal_counts": dict(sorted(server_signal_counts.items())),
        "reproducible_server_signals": reproducible_server_signals,
        "sporadic_server_signals": sporadic_server_signals,
        "ok_rps_min": min(ok_rps),
        "ok_rps_median": statistics.median(ok_rps),
        "ok_rps_max": max(ok_rps),
        "reactor_busy_min": min(busy),
        "reactor_busy_median": statistics.median(busy),
        "reactor_busy_max": max(busy),
        "command_budget_hits_median": statistics.median(command_hits),
        "command_budget_hits_max": max(command_hits),
        "command_queue_peak_max": max(command_peak),
    }


def validate_args(parser: argparse.ArgumentParser,
                  args: argparse.Namespace) -> None:
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not 1 <= args.trials <= 10:
        parser.error("trials must be in 1..10")
    if not 1 <= args.rate <= 1_000_000:
        parser.error("rate must be in 1..1000000")
    if not 1 <= args.workers <= 32:
        parser.error("workers must be in 1..32")
    if not 1 <= args.max_generators <= 32:
        parser.error("max-generators must be in 1..32")
    if not 100 <= args.target_rate_per_generator <= 1_000_000:
        parser.error("invalid target-rate-per-generator")
    if not 1 <= args.generator_workers <= 8:
        parser.error("generator-workers must be in 1..8")
    if not 1 <= args.generator_window <= args.generator_capacity <= 256:
        parser.error("invalid generator window/capacity")
    if not 10 <= args.min_arrival_ms <= 10000:
        parser.error("invalid min-arrival-ms")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="40k-repeatability")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--rate", type=int, default=40000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--min-arrival-ms", type=int, default=250)
    parser.add_argument("--max-generators", type=int, default=32)
    parser.add_argument("--target-rate-per-generator", type=int, default=1250)
    parser.add_argument("--generator-workers", type=int, default=1)
    parser.add_argument("--generator-window", type=int, default=256)
    parser.add_argument("--generator-capacity", type=int, default=256)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    validate_args(parser, args)

    generators = generator_count_for(
        args.rate, args.max_generators, args.target_rate_per_generator)
    requests = max(
        args.requests, math.ceil(args.rate * args.min_arrival_ms / 1000.0))
    args.output.parent.mkdir(parents=True, exist_ok=True)

    metadata = base_metadata(args.binary, args.label)
    metadata.update({
        "type": "repeatability_metadata",
        "schema": 1,
        "trials": args.trials,
        "rate_rps": args.rate,
        "workers": args.workers,
        "generators": generators,
        "requests": requests,
        "generator_workers": args.generator_workers,
        "generator_window": args.generator_window,
        "generator_capacity": args.generator_capacity,
        "profiles": PROFILE_SPECS,
        "note": (
            "No performance threshold. Repeats classify whether exact Server "
            "wall signals recur on the shared CI runner."),
    })

    profile_results: dict[str, list[dict[str, Any]]] = {
        name: [] for name in PROFILE_SPECS}

    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata, allow_nan=False) + "\n")
        output.flush()

        for trial in range(1, args.trials + 1):
            for profile, spec in PROFILE_SPECS.items():
                case = {
                    "trial": trial,
                    "rate_rps": args.rate,
                    "requests": requests,
                    "warmup": 32,
                    "workers": args.workers,
                    "slow_ms": 0,
                    "executor_queue": spec["executor_queue"],
                    "window": 128,
                    "capacity": 128,
                    "generator_window": args.generator_window,
                    "generator_capacity": args.generator_capacity,
                    "timeout_ms": args.timeout_ms,
                    "generators": generators,
                    "generator_workers": args.generator_workers,
                    "resource_profile": profile,
                    "rx_buffer_count": spec["rx_buffer_count"],
                    "control_tx_item_count": spec["control_tx_item_count"],
                    "command_capacity": spec["command_capacity"],
                    "rpc_message_pool_count": 0,
                    "reassembly_pool_count": 0,
                }
                case_dir = (
                    args.output.parent / (args.output.stem + "-logs") /
                    f"t{trial}-{profile}-{args.rate}rps")
                try:
                    result = run_profile_case(args.binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({
                        "type": "failure", "trial": trial,
                        "resource_profile": profile, "case": case,
                        "error": str(error), "logs": str(case_dir),
                    }) + "\n")
                    output.flush()
                    raise

                result["type"] = "repeatability_case"
                result["schema"] = 1
                result["wall_signals"] = exact_wall_signals(result)
                profile_results[profile].append(result)
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                print(
                    f"trial={trial} profile={profile} "
                    f"ok_rps={result['client']['ok_rps']:.1f} "
                    f"busy={result['server_exit']['reactor_busy_ratio']:.3f} "
                    f"cmd_hits={result['server_exit']['command_budget_hits']} "
                    f"signals={','.join(result['wall_signals']) or 'none'}",
                    flush=True)

        for profile, results in profile_results.items():
            summary = summarize(profile, results)
            output.write(json.dumps(summary, allow_nan=False) + "\n")
            output.flush()
            print(
                f"repeatability profile={profile} "
                f"clean={summary['clean_trials']}/{summary['trials']} "
                f"server_pressure={summary['server_pressure_trials']}/"
                f"{summary['trials']} signals={summary['signal_counts']}",
                flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"repeatability benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
