#!/usr/bin/env python3
"""Repeat fixed V2 public-limit workloads to classify shared-runner noise."""
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
    "small_call_limit": {"capacity": 32, "bulk_bytes": 16384},
    "default_call_limit": {"capacity": 128, "bulk_bytes": 65536},
    "large_call_limit": {"capacity": 256, "bulk_bytes": 65536},
}


def summarize(profile: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise ValueError("repeatability summary requires results")
    signal_counts: dict[str, int] = {}
    server_signal_counts: dict[str, int] = {}
    for result in results:
        for signal in exact_wall_signals(result):
            signal_counts[signal] = signal_counts.get(signal, 0) + 1
        for signal, key in SERVER_PRESSURE_FIELDS.items():
            if result["server_exit"][key]:
                server_signal_counts[signal] = server_signal_counts.get(signal, 0) + 1

    trials = len(results)
    recurrent = sorted(signal for signal, count in server_signal_counts.items()
                       if count == trials)
    transient = sorted(signal for signal, count in server_signal_counts.items()
                       if count < trials)
    pressure_trials = sum(server_pressure(result) for result in results)
    classification = (
        "reproducible_server_wall" if recurrent else
        "non_reproducible_server_wall" if pressure_trials else
        "no_server_wall")
    rates = [result["client"]["ok_rps"] for result in results]
    command_hits = [result["server_exit"]["command_budget_hits"]
                    for result in results]
    command_peak = [result["server_exit"]["command_queue_peak"]
                    for result in results]
    return {
        "type": "repeatability_summary", "schema": 2,
        "api_generation": "rpc_v2", "profile": profile,
        "tls_enabled": bool(results[0]["case"].get("tls", False)),
        "compression": results[0]["case"].get("compression", "none"),
        "trials": trials,
        "clean_trials": sum(clean_result(result) for result in results),
        "server_pressure_trials": pressure_trials,
        "classification": classification,
        "signal_counts": dict(sorted(signal_counts.items())),
        "server_signal_counts": dict(sorted(server_signal_counts.items())),
        "reproducible_server_signals": recurrent,
        "sporadic_server_signals": transient,
        "ok_rps_min": min(rates),
        "ok_rps_median": statistics.median(rates),
        "ok_rps_max": max(rates),
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
    if not 1 <= args.max_generators <= 16:
        parser.error("max-generators must be in 1..16")
    if not 100 <= args.target_rate_per_generator <= 1_000_000:
        parser.error("invalid target-rate-per-generator")
    if not 1 <= args.generator_window <= args.generator_capacity <= 256:
        parser.error("invalid generator window/capacity")
    if not 10 <= args.min_arrival_ms <= 10000:
        parser.error("invalid min-arrival-ms")
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")
    if args.tls:
        paths = ("tls_ca", "tls_cert", "tls_key")
        if any(getattr(args, name) is None for name in paths):
            parser.error("--tls requires --tls-ca, --tls-cert, and --tls-key")
        for name in paths:
            path = getattr(args, name)
            try:
                path = path.resolve(strict=True)
            except OSError as error:
                parser.error(f"{getattr(args, name)}: {error}")
            if not path.is_file():
                parser.error(f"{path}: TLS credential must be a file")
            setattr(args, name, path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="v2-repeatability")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--rate", type=int, default=40000)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--min-arrival-ms", type=int, default=250)
    parser.add_argument("--max-generators", type=int, default=16)
    parser.add_argument("--target-rate-per-generator", type=int, default=2500)
    parser.add_argument("--generator-window", type=int, default=64)
    parser.add_argument("--generator-capacity", type=int, default=64)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--compression", choices=("none", "request", "response", "zstd"),
                        default="none")
    parser.add_argument("--tls", action="store_true",
                        help="require TLS 1.3 on both endpoints")
    parser.add_argument("--tls-ca", type=Path)
    parser.add_argument("--tls-cert", type=Path)
    parser.add_argument("--tls-key", type=Path)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    validate_args(parser, args)

    generators = generator_count_for(
        args.rate, args.max_generators, args.target_rate_per_generator)
    requests = max(args.requests,
                   math.ceil(args.rate * args.min_arrival_ms / 1000.0))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    metadata = base_metadata(args.binary, args.label, load_mode="open",
                             compression=args.compression, tls=args.tls)
    metadata.update({
        "type": "repeatability_metadata", "schema": 2,
        "api_generation": "rpc_v2", "trials": args.trials,
        "rate_rps": args.rate, "generators": generators,
        "requests": requests, "generator_window": args.generator_window,
        "generator_capacity": args.generator_capacity,
        "public_profiles": PROFILE_SPECS,
        "note": "Repeated public V2 call-limit profiles classify exact "
                "Server wall signals; no performance threshold is applied.",
    })
    results_by_profile: dict[str, list[dict[str, Any]]] = {
        name: [] for name in PROFILE_SPECS}
    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata, allow_nan=False) + "\n")
        output.flush()
        for trial in range(1, args.trials + 1):
            for name, spec in PROFILE_SPECS.items():
                case = {
                    "trial": trial, "rate_rps": args.rate,
                    "requests": requests, "warmup": 32, "slow_ms": 0,
                    "window": spec["capacity"], "capacity": spec["capacity"],
                    "bulk_bytes": spec["bulk_bytes"],
                    "generator_window": args.generator_window,
                    "generator_capacity": args.generator_capacity,
                    "compression": args.compression, "tls": args.tls,
                    "timeout_ms": args.timeout_ms, "generators": generators,
                    "public_profile": name,
                }
                if args.tls:
                    case.update({
                        "tls_ca": str(args.tls_ca),
                        "tls_cert": str(args.tls_cert),
                        "tls_key": str(args.tls_key),
                    })
                case_dir = (args.output.parent / (args.output.stem + "-logs") /
                            f"t{trial}-{name}-{args.rate}rps")
                try:
                    result = run_profile_case(args.binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({
                        "type": "failure", "schema": 2,
                        "api_generation": "rpc_v2", "trial": trial,
                        "public_profile": name, "case": case,
                        "error": str(error), "logs": str(case_dir),
                    }) + "\n")
                    output.flush()
                    raise
                result["type"] = "repeatability_case"
                result["schema"] = 2
                result["api_generation"] = "rpc_v2"
                result["wall_signals"] = exact_wall_signals(result)
                results_by_profile[name].append(result)
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                print(f"trial={trial} profile={name} "
                      f"ok_rps={result['client']['ok_rps']:.1f} "
                      f"command_hits={result['server_exit']['command_budget_hits']} "
                      f"signals={','.join(result['wall_signals']) or 'none'}",
                      flush=True)
        for name, rows in results_by_profile.items():
            summary = summarize(name, rows)
            output.write(json.dumps(summary, allow_nan=False) + "\n")
            output.flush()
            print(f"repeatability profile={name} "
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
