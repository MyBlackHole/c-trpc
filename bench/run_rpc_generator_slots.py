#!/usr/bin/env python3
"""Measure fixed-rate RPC sensitivity to aggregate generator in-flight slots."""
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
from run_rpc_fanout import slots_per_generator, summarize_fanout
from run_rpc_headroom import run_profile_case
from run_rpc_scalability import exact_wall_signals, parse_int_list


def slot_order(values: list[int], trial: int) -> list[int]:
    ordered = sorted(set(values))
    return ordered if trial % 2 else list(reversed(ordered))


def summarize_slots(aggregate_slots: int, generators: int,
                    results: list[dict[str, Any]]) -> dict[str, Any]:
    summary = summarize_fanout(generators, results)
    if summary["aggregate_generator_slots"] != aggregate_slots:
        raise ValueError("slot summary aggregate capacity mismatch")
    summary["type"] = "generator_slot_summary"
    summary["schema"] = 1
    return summary


def validate_args(parser: argparse.ArgumentParser,
                  args: argparse.Namespace) -> None:
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not 1 <= args.trials <= 5:
        parser.error("trials must be in 1..5")
    if not 1 <= args.rate <= 1_000_000:
        parser.error("rate must be in 1..1000000")
    if not 2 <= args.generators <= 32:
        parser.error("generators must be in 2..32")
    if (not args.aggregate_generator_slots or
            len(set(args.aggregate_generator_slots)) !=
            len(args.aggregate_generator_slots)):
        parser.error("aggregate-generator-slots must be unique and non-empty")
    try:
        for aggregate_slots in args.aggregate_generator_slots:
            slots_per_generator(aggregate_slots, args.generators)
    except ValueError as error:
        parser.error(str(error))
    if not 1 <= args.workers <= 32:
        parser.error("workers must be in 1..32")
    if not 1 <= args.requests <= 10_000_000:
        parser.error("requests must be in 1..10000000")
    if not 10 <= args.min_arrival_ms <= 10000:
        parser.error("min-arrival-ms must be in 10..10000")
    if not 1 <= args.generator_workers <= 8:
        parser.error("generator-workers must be in 1..8")
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")
    if not 16 <= args.executor_queue <= 65536:
        parser.error("executor-queue must be in 16..65536")
    for value in (args.rx_buffers, args.control_tx_items):
        if not 1 <= value <= 8192:
            parser.error("pool headroom must be in 1..8192")
    if not 1 <= args.command_capacity <= 65536:
        parser.error("command-capacity must be in 1..65536")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="generator-slot-sensitivity")
    parser.add_argument("--trials", type=int, default=2)
    parser.add_argument("--rate", type=int, default=40000)
    parser.add_argument("--generators", type=int, default=32)
    parser.add_argument("--aggregate-generator-slots", type=parse_int_list,
                        default=parse_int_list("1024,2048,4096,8192"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--min-arrival-ms", type=int, default=250)
    parser.add_argument("--generator-workers", type=int, default=1)
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
        "type": "generator_slot_metadata",
        "schema": 1,
        "trials": args.trials,
        "rate_rps": args.rate,
        "generators": args.generators,
        "aggregate_generator_slots": args.aggregate_generator_slots,
        "per_generator_slots": {
            str(value): slots_per_generator(value, args.generators)
            for value in args.aggregate_generator_slots
        },
        "requests": requests,
        "workers": args.workers,
        "generator_workers": args.generator_workers,
        "server_headroom": {
            "rx_buffer_count": args.rx_buffers,
            "executor_queue": args.executor_queue,
            "control_tx_item_count": args.control_tx_items,
            "command_capacity": args.command_capacity,
        },
        "note": (
            "Generator count, total offered rate and Server headroom remain "
            "fixed. Only aggregate Client in-flight capacity changes. Slot "
            "order alternates by trial to reduce fixed-order runner bias."),
    })

    results_by_slots: dict[int, list[dict[str, Any]]] = {
        value: [] for value in args.aggregate_generator_slots}

    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata, allow_nan=False) + "\n")
        output.flush()

        for trial in range(1, args.trials + 1):
            for aggregate_slots in slot_order(
                    args.aggregate_generator_slots, trial):
                generator_slots = slots_per_generator(
                    aggregate_slots, args.generators)
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
                    "generators": args.generators,
                    "generator_workers": args.generator_workers,
                    "resource_profile": "generator_slot_headroom",
                    "rx_buffer_count": args.rx_buffers,
                    "control_tx_item_count": args.control_tx_items,
                    "command_capacity": args.command_capacity,
                    "rpc_message_pool_count": 0,
                    "reassembly_pool_count": 0,
                }
                case_dir = (
                    args.output.parent / (args.output.stem + "-logs") /
                    f"t{trial}-s{aggregate_slots}-{args.rate}rps")
                try:
                    result = run_profile_case(args.binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({
                        "type": "failure",
                        "trial": trial,
                        "aggregate_generator_slots": aggregate_slots,
                        "case": case,
                        "error": str(error),
                        "logs": str(case_dir),
                    }) + "\n")
                    output.flush()
                    raise

                result["type"] = "generator_slot_case"
                result["schema"] = 1
                result["aggregate_generator_slots"] = aggregate_slots
                result["wall_signals"] = exact_wall_signals(result)
                results_by_slots[aggregate_slots].append(result)
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                print(
                    f"trial={trial} aggregate_slots={aggregate_slots} "
                    f"per_generator_slots={generator_slots} "
                    f"accepted={result['client']['accepted']}/"
                    f"{result['client']['offered']} "
                    f"dropped={result['client']['scheduler_dropped']} "
                    f"late_p99_us={result['client']['scheduler_late_p99_us']:.1f} "
                    f"ok_rps={result['client']['ok_rps']:.1f} "
                    f"signals={','.join(result['wall_signals']) or 'none'}",
                    flush=True)

        for aggregate_slots in sorted(results_by_slots):
            summary = summarize_slots(
                aggregate_slots, args.generators,
                results_by_slots[aggregate_slots])
            output.write(json.dumps(summary, allow_nan=False) + "\n")
            output.flush()
            print(
                f"aggregate_slots={aggregate_slots} "
                f"per_generator_slots={summary['per_generator_slots']} "
                f"accepted_median={summary['accepted_fraction_median']:.3f} "
                f"drop_trials={summary['generator_drop_trials']}/"
                f"{summary['trials']} "
                f"late_trials={summary['generator_late_trials']}/"
                f"{summary['trials']} "
                f"server_pressure={summary['server_pressure_trials']}",
                flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"generator slot benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
