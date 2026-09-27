#!/usr/bin/env python3
"""Measure fixed-rate RPC load sensitivity to generator process fanout."""
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
    parse_int_list,
    scheduler_late_one_interval,
    split_rates,
)


def trial_order(fanouts: list[int], trial: int) -> list[int]:
    ordered = sorted(set(fanouts))
    return ordered if trial % 2 else list(reversed(ordered))


def slots_per_generator(aggregate_slots: int, generators: int) -> int:
    if aggregate_slots < 1 or generators < 1:
        raise ValueError("aggregate slots and generators must be positive")
    if aggregate_slots % generators:
        raise ValueError("aggregate generator slots must divide every fanout")
    slots = aggregate_slots // generators
    if not 1 <= slots <= 256:
        raise ValueError("per-generator slots must be in 1..256")
    return slots


def summarize_fanout(generators: int,
                     results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise ValueError("fanout summary requires results")
    if any(result["case"]["generators"] != generators for result in results):
        raise ValueError("fanout summary mixed generator counts")

    rate_rps = results[0]["case"]["rate_rps"]
    if any(result["case"]["rate_rps"] != rate_rps for result in results):
        raise ValueError("fanout summary mixed offered rates")

    per_generator_slots = results[0]["case"]["generator_capacity"]
    if any(result["case"]["generator_capacity"] != per_generator_slots or
           result["case"]["generator_window"] != per_generator_slots
           for result in results):
        raise ValueError("fanout summary mixed generator slot controls")
    aggregate_generator_slots = per_generator_slots * generators

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

    accepted_fraction = [
        result["client"]["accepted"] / result["client"]["offered"]
        if result["client"]["offered"] else 0.0
        for result in results
    ]
    ok_rps = [result["client"]["ok_rps"] for result in results]
    late_p99 = [
        result["client"]["scheduler_late_p99_us"] for result in results]
    dropped = [result["client"]["scheduler_dropped"] for result in results]
    client_cpu = [result["client"]["client_cpu_s"] for result in results]
    client_rss = [
        result["client"]["client_peak_rss_kib"] for result in results]
    busy = [result["server_exit"]["reactor_busy_ratio"] for result in results]
    command_peak = [
        result["server_exit"]["command_queue_peak"] for result in results]
    control_peak = [
        result["server_exit"]["control_tx_pool_peak"] for result in results]

    return {
        "type": "fanout_summary",
        "schema": 2,
        "generators": generators,
        "trials": len(results),
        "rate_rps": rate_rps,
        "per_generator_rates_rps": split_rates(rate_rps, generators),
        "per_generator_slots": per_generator_slots,
        "aggregate_generator_slots": aggregate_generator_slots,
        "clean_trials": sum(clean_result(result) for result in results),
        "generator_drop_trials": sum(
            result["client"]["scheduler_dropped"] > 0 for result in results),
        "generator_late_trials": sum(
            scheduler_late_one_interval(result) for result in results),
        "server_pressure_trials": sum(
            server_pressure(result) for result in results),
        "signal_counts": dict(sorted(signal_counts.items())),
        "server_signal_counts": dict(sorted(server_signal_counts.items())),
        "accepted_fraction_min": min(accepted_fraction),
        "accepted_fraction_median": statistics.median(accepted_fraction),
        "accepted_fraction_max": max(accepted_fraction),
        "ok_rps_min": min(ok_rps),
        "ok_rps_median": statistics.median(ok_rps),
        "ok_rps_max": max(ok_rps),
        "scheduler_late_p99_us_median": statistics.median(late_p99),
        "scheduler_late_p99_us_max": max(late_p99),
        "scheduler_dropped_median": statistics.median(dropped),
        "scheduler_dropped_max": max(dropped),
        "client_cpu_s_median": statistics.median(client_cpu),
        "client_peak_rss_kib_median": statistics.median(client_rss),
        "reactor_busy_median": statistics.median(busy),
        "reactor_busy_max": max(busy),
        "command_queue_peak_max": max(command_peak),
        "control_tx_pool_peak_max": max(control_peak),
    }


def validate_args(parser: argparse.ArgumentParser,
                  args: argparse.Namespace) -> None:
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not 1 <= args.trials <= 5:
        parser.error("trials must be in 1..5")
    if not 1 <= args.rate <= 1_000_000:
        parser.error("rate must be in 1..1000000")
    if (not args.fanouts or len(set(args.fanouts)) != len(args.fanouts) or
            any(value < 2 or value > 32 or value > args.rate
                for value in args.fanouts)):
        parser.error("fanouts must be unique values in 2..min(32, rate)")
    if not 1 <= args.workers <= 32:
        parser.error("workers must be in 1..32")
    if not 1 <= args.requests <= 10_000_000:
        parser.error("requests must be in 1..10000000")
    if not 10 <= args.min_arrival_ms <= 10000:
        parser.error("min-arrival-ms must be in 10..10000")
    if not 1 <= args.generator_workers <= 8:
        parser.error("generator-workers must be in 1..8")
    if not 1 <= args.aggregate_generator_slots <= 8192:
        parser.error("aggregate-generator-slots must be in 1..8192")
    try:
        for generators in args.fanouts:
            slots_per_generator(args.aggregate_generator_slots, generators)
    except ValueError as error:
        parser.error(str(error))
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")
    if not 16 <= args.executor_queue <= 65536:
        parser.error("executor-queue must be in 16..65536")
    for value in (args.rx_buffers, args.control_tx_items):
        if not 1 <= value <= 65536:
            parser.error("pool headroom must be in 1..65536")
    if not 1 <= args.command_capacity <= 65536:
        parser.error("command-capacity must be in 1..65536")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="generator-fanout-sensitivity")
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--rate", type=int, default=40000)
    parser.add_argument("--fanouts", type=parse_int_list,
                        default=parse_int_list("4,8,16,32"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--min-arrival-ms", type=int, default=250)
    parser.add_argument("--generator-workers", type=int, default=1)
    parser.add_argument("--aggregate-generator-slots", type=int, default=1024)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--rx-buffers", type=int, default=8192)
    parser.add_argument("--executor-queue", type=int, default=1024)
    parser.add_argument("--control-tx-items", type=int, default=8192)
    parser.add_argument("--command-capacity", type=int, default=16384)
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    validate_args(parser, args)

    requests = max(
        args.requests, math.ceil(args.rate * args.min_arrival_ms / 1000.0))
    args.output.parent.mkdir(parents=True, exist_ok=True)

    metadata = base_metadata(args.binary, args.label)
    metadata.update({
        "type": "fanout_metadata",
        "schema": 2,
        "trials": args.trials,
        "rate_rps": args.rate,
        "fanouts": args.fanouts,
        "requests": requests,
        "workers": args.workers,
        "generator_workers": args.generator_workers,
        "aggregate_generator_slots": args.aggregate_generator_slots,
        "per_fanout_generator_slots": {
            str(generators):
                slots_per_generator(args.aggregate_generator_slots, generators)
            for generators in args.fanouts
        },
        "server_headroom": {
            "rx_buffer_count": args.rx_buffers,
            "executor_queue": args.executor_queue,
            "control_tx_item_count": args.control_tx_items,
            "command_capacity": args.command_capacity,
        },
        "note": (
            "Fanout order alternates by trial. Aggregate generator in-flight "
            "slots and Server bounded resources remain fixed, so process fanout "
            "is isolated from total Client concurrency. No production defaults "
            "change."),
    })

    results_by_fanout: dict[int, list[dict[str, Any]]] = {
        value: [] for value in args.fanouts}

    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata, allow_nan=False) + "\n")
        output.flush()

        for trial in range(1, args.trials + 1):
            for generators in trial_order(args.fanouts, trial):
                generator_slots = slots_per_generator(
                    args.aggregate_generator_slots, generators)
                case = {
                    "trial": trial,
                    "rate_rps": args.rate,
                    "requests": requests,
                    "warmup": 32,
                    "workers": args.workers,
                    "slow_ms": 0,
                    "executor_queue": args.executor_queue,
                    "window": 128,
                    "capacity": 128,
                    "generator_window": generator_slots,
                    "generator_capacity": generator_slots,
                    "timeout_ms": args.timeout_ms,
                    "generators": generators,
                    "generator_workers": args.generator_workers,
                    "resource_profile": "generator_fanout_headroom",
                    "rx_buffer_count": args.rx_buffers,
                    "control_tx_item_count": args.control_tx_items,
                    "command_capacity": args.command_capacity,
                    "rpc_message_pool_count": 0,
                    "reassembly_pool_count": 0,
                }
                case_dir = (
                    args.output.parent / (args.output.stem + "-logs") /
                    f"t{trial}-g{generators}-{args.rate}rps")
                try:
                    result = run_profile_case(args.binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({
                        "type": "failure",
                        "trial": trial,
                        "generators": generators,
                        "case": case,
                        "error": str(error),
                        "logs": str(case_dir),
                    }) + "\n")
                    output.flush()
                    raise

                result["type"] = "fanout_case"
                result["schema"] = 2
                result["wall_signals"] = exact_wall_signals(result)
                results_by_fanout[generators].append(result)
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                print(
                    f"trial={trial} generators={generators} "
                    f"slots={generator_slots}x{generators} "
                    f"accepted={result['client']['accepted']}/"
                    f"{result['client']['offered']} "
                    f"ok_rps={result['client']['ok_rps']:.1f} "
                    f"late_p99_us={result['client']['scheduler_late_p99_us']:.1f} "
                    f"client_cpu_s={result['client']['client_cpu_s']:.3f} "
                    f"busy={result['server_exit']['reactor_busy_ratio']:.3f} "
                    f"signals={','.join(result['wall_signals']) or 'none'}",
                    flush=True)

        for generators in sorted(results_by_fanout):
            summary = summarize_fanout(
                generators, results_by_fanout[generators])
            output.write(json.dumps(summary, allow_nan=False) + "\n")
            output.flush()
            print(
                f"fanout={generators} "
                f"clean={summary['clean_trials']}/{summary['trials']} "
                f"accepted_median={summary['accepted_fraction_median']:.3f} "
                f"ok_rps_median={summary['ok_rps_median']:.1f} "
                f"generator_pressure="
                f"{summary['generator_drop_trials']}/"
                f"{summary['generator_late_trials']} "
                f"server_pressure={summary['server_pressure_trials']}",
                flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"fanout benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
