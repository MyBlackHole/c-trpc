#!/usr/bin/env python3
"""A/B bounded-resource headroom diagnostic for RPC scalability."""
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
    SERVER_PRESSURE_FIELDS,
    exact_wall_signals,
    generator_count_for,
    parse_int_list,
    run_multisource_case,
    summarize_group,
)


def run_profile_case(binary: Path, case: dict[str, Any],
                     output_dir: Path) -> dict[str, Any]:
    result = (
        run_capacity_case(binary, case, output_dir)
        if case["generators"] == 1
        else run_multisource_case(binary, case, output_dir)
    )
    result["type"] = "headroom_case"
    result["schema"] = 1
    result["resource_profile"] = case["resource_profile"]
    result["wall_signals"] = exact_wall_signals(result)
    return result


def server_pressure(result: dict[str, Any]) -> bool:
    server = result["server_exit"]
    return any(server[key] for key in SERVER_PRESSURE_FIELDS.values())


def compare_profiles(before_results: list[dict[str, Any]],
                     after_results: list[dict[str, Any]],
                     before_name: str, after_name: str) -> dict[str, Any]:
    if not before_results or len(before_results) != len(after_results):
        raise ValueError("profile comparison requires matching non-empty cases")

    before_by_rate = {r["case"]["rate_rps"]: r for r in before_results}
    after_by_rate = {r["case"]["rate_rps"]: r for r in after_results}
    if before_by_rate.keys() != after_by_rate.keys():
        raise ValueError("profile rates do not match")

    rates = sorted(before_by_rate)
    removed_rx: list[int] = []
    persisted_rx: list[int] = []
    removed_executor_full: list[int] = []
    persisted_executor_full: list[int] = []
    removed_control_tx: list[int] = []
    persisted_control_tx: list[int] = []
    removed_command_full: list[int] = []
    persisted_command_full: list[int] = []
    pressure_changed: dict[str, dict[str, list[str]]] = {}

    for rate in rates:
        before = before_by_rate[rate]
        after = after_by_rate[rate]
        before_rx = before["server_exit"]["rx_pool_exhausted_events"] > 0
        after_rx = after["server_exit"]["rx_pool_exhausted_events"] > 0
        if before_rx and not after_rx:
            removed_rx.append(rate)
        if before_rx and after_rx:
            persisted_rx.append(rate)

        before_executor = before["server_exit"]["rpc_hard_full_events"] > 0
        after_executor = after["server_exit"]["rpc_hard_full_events"] > 0
        if before_executor and not after_executor:
            removed_executor_full.append(rate)
        if before_executor and after_executor:
            persisted_executor_full.append(rate)

        before_control = (
            before["server_exit"]["control_tx_pool_exhausted_events"] > 0)
        after_control = (
            after["server_exit"]["control_tx_pool_exhausted_events"] > 0)
        if before_control and not after_control:
            removed_control_tx.append(rate)
        if before_control and after_control:
            persisted_control_tx.append(rate)

        before_command = (
            before["server_exit"]["command_queue_full_events"] > 0)
        after_command = (
            after["server_exit"]["command_queue_full_events"] > 0)
        if before_command and not after_command:
            removed_command_full.append(rate)
        if before_command and after_command:
            persisted_command_full.append(rate)

        before_signals = sorted(
            signal for signal in before["wall_signals"]
            if signal not in ("load_generator_late", "load_generator_drop"))
        after_signals = sorted(
            signal for signal in after["wall_signals"]
            if signal not in ("load_generator_late", "load_generator_drop"))
        if before_signals != after_signals:
            pressure_changed[str(rate)] = {
                before_name: before_signals,
                after_name: after_signals,
            }

    before_pressure = [
        r["case"]["rate_rps"] for r in before_results if server_pressure(r)]
    after_pressure = [
        r["case"]["rate_rps"] for r in after_results if server_pressure(r)]

    return {
        "type": "headroom_comparison",
        "schema": 1,
        "before_profile": before_name,
        "after_profile": after_name,
        "rates_rps": rates,
        "before_rx_capacity":
            before_results[0]["server_exit"]["rx_pool_capacity"],
        "after_rx_capacity":
            after_results[0]["server_exit"]["rx_pool_capacity"],
        "before_executor_queue":
            before_results[0]["server"]["executor_queue"],
        "after_executor_queue":
            after_results[0]["server"]["executor_queue"],
        "before_control_tx_capacity":
            before_results[0]["server_exit"]["control_tx_pool_capacity"],
        "after_control_tx_capacity":
            after_results[0]["server_exit"]["control_tx_pool_capacity"],
        "before_command_capacity":
            before_results[0]["server_exit"]["command_queue_capacity"],
        "after_command_capacity":
            after_results[0]["server_exit"]["command_queue_capacity"],
        "before_first_server_pressure_rps":
            min(before_pressure) if before_pressure else None,
        "after_first_server_pressure_rps":
            min(after_pressure) if after_pressure else None,
        "rx_exhaustion_removed_rates": removed_rx,
        "rx_exhaustion_persisted_rates": persisted_rx,
        "executor_hard_full_removed_rates": removed_executor_full,
        "executor_hard_full_persisted_rates": persisted_executor_full,
        "control_tx_exhaustion_removed_rates": removed_control_tx,
        "control_tx_exhaustion_persisted_rates": persisted_control_tx,
        "command_full_removed_rates": removed_command_full,
        "command_full_persisted_rates": persisted_command_full,
        "server_signal_changes": pressure_changed,
    }


def metadata(binary: Path, args: argparse.Namespace) -> dict[str, Any]:
    row = base_metadata(binary, args.label)
    row.update({
        "type": "headroom_metadata",
        "schema": 1,
        "workers": args.workers,
        "handler_ms": args.handler_ms,
        "rates_rps": args.rates,
        "executor_queue": args.executor_queue,
        "window": args.window,
        "capacity": args.capacity,
        "generator_window": args.generator_window,
        "generator_capacity": args.generator_capacity,
        "baseline": {
            "rx_buffer_count": 0,
            "executor_queue": args.executor_queue,
            "control_tx_item_count": 0,
            "command_capacity": 0,
            "rpc_message_pool_count": 0,
            "reassembly_pool_count": 0,
        },
        "rx_headroom": {
            "rx_buffer_count": args.headroom_rx_buffers,
            "executor_queue": args.executor_queue,
            "control_tx_item_count": 0,
            "command_capacity": 0,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "rx_executor_headroom": {
            "rx_buffer_count": args.headroom_rx_buffers,
            "executor_queue": args.headroom_executor_queue,
            "control_tx_item_count": 0,
            "command_capacity": 0,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "rx_executor_control_headroom": {
            "rx_buffer_count": args.headroom_rx_buffers,
            "executor_queue": args.headroom_executor_queue,
            "control_tx_item_count": args.headroom_control_tx_items,
            "command_capacity": 0,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "full_headroom": {
            "rx_buffer_count": args.full_headroom_rx_buffers,
            "executor_queue": args.headroom_executor_queue,
            "control_tx_item_count": args.headroom_control_tx_items,
            "command_capacity": 0,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "rx_ceiling_headroom": {
            "rx_buffer_count": args.ceiling_rx_buffers,
            "executor_queue": args.headroom_executor_queue,
            "control_tx_item_count": args.headroom_control_tx_items,
            "command_capacity": 0,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "command_headroom": {
            "rx_buffer_count": args.ceiling_rx_buffers,
            "executor_queue": args.headroom_executor_queue,
            "control_tx_item_count": args.headroom_control_tx_items,
            "command_capacity": args.headroom_command_capacity,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "control_ceiling_headroom": {
            "rx_buffer_count": args.ceiling_rx_buffers,
            "executor_queue": args.headroom_executor_queue,
            "control_tx_item_count": args.ceiling_control_tx_items,
            "command_capacity": args.headroom_command_capacity,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "executor_ceiling_headroom": {
            "rx_buffer_count": args.ceiling_rx_buffers,
            "executor_queue": args.ceiling_executor_queue,
            "control_tx_item_count": args.ceiling_control_tx_items,
            "command_capacity": args.headroom_command_capacity,
            "rpc_message_pool_count": args.headroom_rpc_message_pool,
            "reassembly_pool_count": args.headroom_reassembly_pool,
        },
        "max_generators": args.max_generators,
        "target_rate_per_generator": args.target_rate_per_generator,
        "min_arrival_ms": args.min_arrival_ms,
        "generator_workers": args.generator_workers,
        "note": (
            "0 resource value means keep the benchmark-derived baseline; "
            "profiles differ only by explicit headroom overrides"),
    })
    return row


def validate_args(parser: argparse.ArgumentParser,
                  args: argparse.Namespace) -> None:
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not args.rates or any(rate < 1 or rate > 1_000_000 for rate in args.rates):
        parser.error("rates must be in 1..1000000")
    if not 1 <= args.workers <= 32 or not 0 <= args.handler_ms <= 1000:
        parser.error("invalid workers or handler-ms")
    if not 16 <= args.executor_queue <= 65536:
        parser.error("executor-queue must be in 16..65536")
    if not 1 <= args.window <= args.capacity <= 256:
        parser.error("require 1 <= window <= capacity <= 256")
    if not 1 <= args.generator_window <= args.generator_capacity <= 256:
        parser.error("require 1 <= generator-window <= generator-capacity <= 256")
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")
    if not 1 <= args.max_generators <= 32:
        parser.error("max-generators must be in 1..32")
    if not 100 <= args.target_rate_per_generator <= 1_000_000:
        parser.error("invalid target-rate-per-generator")
    if not 10 <= args.min_arrival_ms <= 10000:
        parser.error("min-arrival-ms must be in 10..10000")
    if not 1 <= args.generator_workers <= 8:
        parser.error("generator-workers must be in 1..8")
    for value in (args.headroom_rx_buffers,
                  args.full_headroom_rx_buffers,
                  args.ceiling_rx_buffers,
                  args.headroom_control_tx_items,
                  args.headroom_rpc_message_pool,
                  args.headroom_reassembly_pool):
        if not 0 <= value <= 8192:
            parser.error("headroom pool overrides must be in 0..8192")
    if not 16 <= args.headroom_executor_queue <= 65536:
        parser.error("headroom-executor-queue must be in 16..65536")
    if not args.headroom_executor_queue <= args.ceiling_executor_queue <= 65536:
        parser.error("ceiling-executor-queue must be >= headroom executor queue")
    if args.headroom_control_tx_items == 0:
        parser.error("headroom-control-tx-items must be nonzero")
    if not args.headroom_control_tx_items <= args.ceiling_control_tx_items <= 8192:
        parser.error("ceiling-control-tx-items must be >= headroom CONTROL TX")
    if args.full_headroom_rx_buffers < args.headroom_rx_buffers:
        parser.error("full-headroom-rx-buffers must be >= headroom-rx-buffers")
    if args.ceiling_rx_buffers < args.full_headroom_rx_buffers:
        parser.error("ceiling-rx-buffers must be >= full-headroom-rx-buffers")
    if not 1 <= args.headroom_command_capacity <= 65536:
        parser.error("headroom-command-capacity must be in 1..65536")
    if (args.headroom_rx_buffers == 0 and
            args.headroom_control_tx_items == 0 and
            args.headroom_rpc_message_pool == 0 and
            args.headroom_reassembly_pool == 0 and
            args.headroom_executor_queue == args.executor_queue):
        parser.error("headroom profiles must change at least one resource")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="resource-headroom-ab")
    parser.add_argument("--rates", type=parse_int_list,
                        default=parse_int_list("10000,20000,40000"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--handler-ms", type=int, default=0)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--executor-queue", type=int, default=64)
    parser.add_argument("--window", type=int, default=128)
    parser.add_argument("--capacity", type=int, default=128)
    parser.add_argument("--generator-window", type=int, default=256)
    parser.add_argument("--generator-capacity", type=int, default=256)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--max-generators", type=int, default=32)
    parser.add_argument("--target-rate-per-generator", type=int, default=1250)
    parser.add_argument("--min-arrival-ms", type=int, default=250)
    parser.add_argument("--generator-workers", type=int, default=1)
    parser.add_argument("--headroom-rx-buffers", type=int, default=1024)
    parser.add_argument("--full-headroom-rx-buffers", type=int, default=4096)
    parser.add_argument("--ceiling-rx-buffers", type=int, default=8192)
    parser.add_argument("--headroom-executor-queue", type=int, default=256)
    parser.add_argument("--ceiling-executor-queue", type=int, default=1024)
    parser.add_argument("--headroom-control-tx-items", type=int, default=2048)
    parser.add_argument("--ceiling-control-tx-items", type=int, default=8192)
    parser.add_argument("--headroom-command-capacity", type=int, default=4096)
    parser.add_argument("--headroom-rpc-message-pool", type=int, default=0)
    parser.add_argument("--headroom-reassembly-pool", type=int, default=0)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)

    if args.smoke:
        args.rates = [1000, 5000]
        args.requests = 32
        args.max_generators = 2
        args.target_rate_per_generator = 2500
        args.min_arrival_ms = 20
        args.generator_workers = 1
        args.headroom_rx_buffers = 512
        args.generator_window = 64
        args.generator_capacity = 64
        args.full_headroom_rx_buffers = 1024
        args.ceiling_rx_buffers = 2048
        args.headroom_executor_queue = 128
        args.ceiling_executor_queue = 256
        args.headroom_control_tx_items = 256
        args.ceiling_control_tx_items = 512
        args.headroom_command_capacity = 2048
    validate_args(parser, args)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    profiles = (
        ("baseline", 0, args.executor_queue, 0, 0, 0, 0),
        ("rx_headroom", args.headroom_rx_buffers, args.executor_queue, 0, 0,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("rx_executor_headroom", args.headroom_rx_buffers,
         args.headroom_executor_queue, 0, 0,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("rx_executor_control_headroom", args.headroom_rx_buffers,
         args.headroom_executor_queue, args.headroom_control_tx_items, 0,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("full_headroom", args.full_headroom_rx_buffers,
         args.headroom_executor_queue, args.headroom_control_tx_items, 0,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("rx_ceiling_headroom", args.ceiling_rx_buffers,
         args.headroom_executor_queue, args.headroom_control_tx_items, 0,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("command_headroom", args.ceiling_rx_buffers,
         args.headroom_executor_queue, args.headroom_control_tx_items,
         args.headroom_command_capacity,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("control_ceiling_headroom", args.ceiling_rx_buffers,
         args.headroom_executor_queue, args.ceiling_control_tx_items,
         args.headroom_command_capacity,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
        ("executor_ceiling_headroom", args.ceiling_rx_buffers,
         args.ceiling_executor_queue, args.ceiling_control_tx_items,
         args.headroom_command_capacity,
         args.headroom_rpc_message_pool, args.headroom_reassembly_pool),
    )
    profile_results: dict[str, list[dict[str, Any]]] = {}

    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(args.binary, args), allow_nan=False) + "\n")
        output.flush()

        for (profile, rx_count, executor_queue, control_count,
             command_count, rpc_count, reassembly_count) in profiles:
            results: list[dict[str, Any]] = []
            for index, rate in enumerate(sorted(set(args.rates))):
                generators = generator_count_for(
                    rate, args.max_generators,
                    args.target_rate_per_generator)
                requests = max(
                    args.requests,
                    math.ceil(rate * args.min_arrival_ms / 1000.0))
                case = {
                    "trial": 1,
                    "rate_rps": rate,
                    "requests": requests,
                    "warmup": 8 if args.smoke else 32,
                    "workers": args.workers,
                    "slow_ms": args.handler_ms,
                    "executor_queue": executor_queue,
                    "window": args.window,
                    "capacity": args.capacity,
                    "generator_window": args.generator_window,
                    "generator_capacity": args.generator_capacity,
                    "timeout_ms": args.timeout_ms,
                    "generators": generators,
                    "generator_workers": args.generator_workers,
                    "resource_profile": profile,
                    "rx_buffer_count": rx_count,
                    "control_tx_item_count": control_count,
                    "command_capacity": command_count,
                    "rpc_message_pool_count": rpc_count,
                    "reassembly_pool_count": reassembly_count,
                }
                case_dir = (
                    args.output.parent / (args.output.stem + "-logs") /
                    f"{profile}-{index}-{rate}rps")
                try:
                    result = run_profile_case(args.binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({
                        "type": "failure", "resource_profile": profile,
                        "case": case, "error": str(error),
                        "logs": str(case_dir),
                    }) + "\n")
                    output.flush()
                    raise
                results.append(result)
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                print(
                    f"profile={profile} rate={rate}rps generators={generators} "
                    f"rx={result['server_exit']['rx_pool_peak']}/"
                    f"{result['server_exit']['rx_pool_capacity']} "
                    f"signals={','.join(result['wall_signals']) or 'none'}",
                    flush=True)

            summary = summarize_group(results)
            summary["type"] = "headroom_profile_summary"
            summary["resource_profile"] = profile
            summary["rx_pool_capacity"] = results[0]["server_exit"]["rx_pool_capacity"]
            summary["control_tx_pool_capacity"] = (
                results[0]["server_exit"]["control_tx_pool_capacity"])
            summary["command_queue_capacity"] = (
                results[0]["server_exit"]["command_queue_capacity"])
            output.write(json.dumps(summary, allow_nan=False) + "\n")
            output.flush()
            profile_results[profile] = results

        comparisons = (
            compare_profiles(
                profile_results["baseline"],
                profile_results["rx_headroom"],
                "baseline", "rx_headroom"),
            compare_profiles(
                profile_results["rx_headroom"],
                profile_results["rx_executor_headroom"],
                "rx_headroom", "rx_executor_headroom"),
            compare_profiles(
                profile_results["rx_executor_headroom"],
                profile_results["rx_executor_control_headroom"],
                "rx_executor_headroom", "rx_executor_control_headroom"),
            compare_profiles(
                profile_results["rx_executor_control_headroom"],
                profile_results["full_headroom"],
                "rx_executor_control_headroom", "full_headroom"),
            compare_profiles(
                profile_results["full_headroom"],
                profile_results["rx_ceiling_headroom"],
                "full_headroom", "rx_ceiling_headroom"),
            compare_profiles(
                profile_results["rx_ceiling_headroom"],
                profile_results["command_headroom"],
                "rx_ceiling_headroom", "command_headroom"),
            compare_profiles(
                profile_results["command_headroom"],
                profile_results["control_ceiling_headroom"],
                "command_headroom", "control_ceiling_headroom"),
            compare_profiles(
                profile_results["control_ceiling_headroom"],
                profile_results["executor_ceiling_headroom"],
                "control_ceiling_headroom", "executor_ceiling_headroom"),
        )
        for comparison in comparisons:
            output.write(json.dumps(comparison, allow_nan=False) + "\n")
            output.flush()
            print(
                "comparison "
                f"{comparison['before_profile']}->{comparison['after_profile']} "
                f"pressure={comparison['before_first_server_pressure_rps']}->"
                f"{comparison['after_first_server_pressure_rps']} "
                f"rx_removed={comparison['rx_exhaustion_removed_rates']} "
                f"executor_removed="
                f"{comparison['executor_hard_full_removed_rates']} "
                f"control_removed="
                f"{comparison['control_tx_exhaustion_removed_rates']} "
                f"command_removed="
                f"{comparison['command_full_removed_rates']}",
                flush=True)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"headroom benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
