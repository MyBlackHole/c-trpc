#!/usr/bin/env python3
"""RPC worker/handler scalability matrix with explicit bottleneck evidence."""
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
from run_rpc_capacity import (
    nominal_handler_capacity_rps,
    run_case as run_capacity_case,
)


SERVER_PRESSURE_FIELDS = {
    "executor_admission_limit": "rpc_admission_limit_hits",
    "executor_hard_full": "rpc_hard_full_events",
    "reactor_command_queue_full": "command_queue_full_events",
    "reactor_completion_queue_full": "completion_queue_full_events",
    "rx_pool_exhausted": "rx_pool_exhausted_events",
    "tx_pool_exhausted": "tx_pool_exhausted_events",
    "control_tx_pool_exhausted": "control_tx_pool_exhausted_events",
    "rpc_message_pool_exhausted": "rpc_message_pool_exhausted_events",
    "reassembly_pool_exhausted": "reassembly_pool_exhausted_events",
}


def parse_int_list(text: str) -> list[int]:
    try:
        values = [int(item) for item in text.split(",") if item]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or any(value < 0 for value in values):
        raise argparse.ArgumentTypeError("values must be non-negative integers")
    return values


def parse_ratio_list(text: str) -> list[float]:
    try:
        values = [float(item) for item in text.split(",") if item]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated ratios") from error
    if not values or any(not math.isfinite(value) or value <= 0 for value in values):
        raise argparse.ArgumentTypeError("ratios must be finite positive numbers")
    return values


def rates_for(workers: int, handler_ms: int, zero_rates: list[int],
              ratios: list[float]) -> list[int]:
    if handler_ms == 0:
        return sorted(set(zero_rates))
    nominal = nominal_handler_capacity_rps(workers, handler_ms)
    assert nominal is not None
    return sorted({max(1, round(nominal * ratio)) for ratio in ratios})


def scheduler_late_one_interval(result: dict[str, Any]) -> bool:
    rate = result["case"]["rate_rps"]
    interval_us = 1_000_000.0 / rate
    return result["client"]["scheduler_late_p99_us"] > interval_us


def exact_wall_signals(result: dict[str, Any]) -> list[str]:
    client = result["client"]
    server = result["server_exit"]
    signals: list[str] = []

    if client["scheduler_dropped"]:
        signals.append("load_generator_drop")
    if scheduler_late_one_interval(result):
        signals.append("load_generator_late")
    if client["submit_again"]:
        signals.append("client_submit_again")
    if client["submit_errors"]:
        signals.append("client_submit_error")
    if client["deadlines"]:
        signals.append("rpc_deadline")
    if client["unavailable"]:
        signals.append("rpc_unavailable")
    if client["resource_exhausted"]:
        signals.append("rpc_resource_exhausted")
    for signal, key in SERVER_PRESSURE_FIELDS.items():
        if server[key]:
            signals.append(signal)
    return signals


def clean_result(result: dict[str, Any]) -> bool:
    client = result["client"]
    server = result["server_exit"]
    if any(client[key] for key in (
            "scheduler_dropped", "submit_again", "submit_errors", "deadlines",
            "rpc_errors", "invalid_responses")):
        return False
    if scheduler_late_one_interval(result):
        return False
    return not any(server[key] for key in SERVER_PRESSURE_FIELDS.values())


def summarize_group(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise ValueError("cannot summarize an empty scalability group")

    ordered = sorted(results, key=lambda result: result["case"]["rate_rps"])
    first = ordered[0]["case"]
    clean_rates = [r["case"]["rate_rps"] for r in ordered if clean_result(r)]
    server_pressure = [
        r["case"]["rate_rps"] for r in ordered
        if any(r["server_exit"][key] for key in SERVER_PRESSURE_FIELDS.values())
    ]
    generator_drop = [
        r["case"]["rate_rps"] for r in ordered if r["client"]["scheduler_dropped"]
    ]
    generator_late = [
        r["case"]["rate_rps"] for r in ordered if scheduler_late_one_interval(r)
    ]
    observed_signals = sorted({
        signal for result in ordered for signal in exact_wall_signals(result)
    })
    nominal = nominal_handler_capacity_rps(first["workers"], first["slow_ms"])
    max_clean = max(clean_rates) if clean_rates else None

    return {
        "type": "scalability_summary",
        "schema": 1,
        "trial": first["trial"],
        "workers": first["workers"],
        "handler_ms": first["slow_ms"],
        "executor_queue": first["executor_queue"],
        "window": first["window"],
        "nominal_handler_capacity_rps": nominal,
        "max_clean_scheduled_rps": max_clean,
        "max_clean_vs_nominal":
            (max_clean / nominal if max_clean is not None and nominal else None),
        "first_server_pressure_rps":
            min(server_pressure) if server_pressure else None,
        "first_load_generator_drop_rps":
            min(generator_drop) if generator_drop else None,
        "first_load_generator_late_rps":
            min(generator_late) if generator_late else None,
        "max_observed_ok_rps": max(r["client"]["ok_rps"] for r in ordered),
        "max_executor_queue_utilization":
            max(r["server_exit"]["rpc_queue_peak_per_peer"] /
                r["case"]["executor_queue"] for r in ordered),
        "max_reactor_busy_ratio":
            max(r["server_exit"]["reactor_busy_ratio"] for r in ordered),
        "max_rpc_queue_wait_p99_us":
            max(r["server_exit"]["rpc_queue_wait_p99_us"] for r in ordered),
        "max_rpc_handler_p99_us":
            max(r["server_exit"]["rpc_handler_p99_us"] for r in ordered),
        "wall_signals": observed_signals,
        "rate_points": [r["case"]["rate_rps"] for r in ordered],
    }


def metadata(binary: Path, label: str, workers: list[int],
             handlers: list[int], ratios: list[float],
             zero_rates: list[int], executor_queue: int,
             window: int) -> dict[str, Any]:
    row = base_metadata(binary, label)
    row["type"] = "scalability_metadata"
    row["schema"] = 1
    row["workers"] = workers
    row["handler_ms"] = handlers
    row["nonzero_handler_rate_ratios"] = ratios
    row["zero_handler_rates_rps"] = zero_rates
    row["executor_queue"] = executor_queue
    row["window"] = window
    row["load_model"] = (
        "fixed-rate scheduled arrivals; bounded in-flight slots; scheduler "
        "drops and scheduler lateness are reported separately")
    row["latency"] = (
        "actual submission to result callback; schedule-to-submit lateness is "
        "kept separate from RPC latency")
    row["attribution"] = (
        "queue/pool/executor exhaustion events are server wall signals; "
        "P99 schedule lateness above one arrival interval marks generator "
        "backlog; reactor busy ratio and latency histograms remain evidence")
    return row


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not 1 <= args.trials <= 20 or not 16 <= args.requests <= 1_000_000:
        parser.error("invalid trials or request count")
    if any(worker < 1 or worker > 32 for worker in args.workers):
        parser.error("workers must be in 1..32")
    if any(handler > 1000 for handler in args.handler_ms):
        parser.error("handler-ms must be in 0..1000")
    if any(rate < 1 or rate > 1_000_000 for rate in args.zero_rates):
        parser.error("zero-rates must be in 1..1000000")
    if not 16 <= args.executor_queue <= 65536:
        parser.error("executor-queue must be in 16..65536")
    if not 1 <= args.window <= args.capacity <= 256:
        parser.error("require 1 <= window <= capacity <= 256")
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="unspecified-build")
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--workers", type=parse_int_list, default=parse_int_list("1,2,4,8"))
    parser.add_argument("--handler-ms", type=parse_int_list, default=parse_int_list("0,1,10"))
    parser.add_argument("--ratios", type=parse_ratio_list,
                        default=parse_ratio_list("0.5,1,2,4"))
    parser.add_argument("--zero-rates", type=parse_int_list,
                        default=parse_int_list("1000,5000,10000,20000"))
    parser.add_argument("--executor-queue", type=int, default=64)
    parser.add_argument("--window", type=int, default=128)
    parser.add_argument("--capacity", type=int, default=128)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--smoke", action="store_true",
                        help="small matrix for runner correctness, not scaling conclusions")
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)

    if args.smoke:
        args.workers = [1, 2]
        args.handler_ms = [0, 10]
        args.ratios = [0.5, 2.0]
        args.zero_rates = [1000, 5000]
        args.requests = 32
        args.trials = 1
    validate_args(parser, args)

    warmup = 8 if args.smoke else 32
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(
            args.binary, args.label, args.workers, args.handler_ms,
            args.ratios, args.zero_rates, args.executor_queue,
            args.window), allow_nan=False) + "\n")
        output.flush()

        for trial in range(1, args.trials + 1):
            for handler_ms in args.handler_ms:
                for workers in args.workers:
                    group: list[dict[str, Any]] = []
                    rates = rates_for(
                        workers, handler_ms, args.zero_rates, args.ratios)
                    for index, rate in enumerate(rates):
                        case = {
                            "trial": trial,
                            "rate_rps": rate,
                            "requests": args.requests,
                            "warmup": warmup,
                            "workers": workers,
                            "slow_ms": handler_ms,
                            "executor_queue": args.executor_queue,
                            "window": args.window,
                            "capacity": args.capacity,
                            "timeout_ms": args.timeout_ms,
                        }
                        case_dir = (
                            args.output.parent / (args.output.stem + "-logs") /
                            f"t{trial}-h{handler_ms}-w{workers}-{index}-{rate}rps")
                        try:
                            result = run_capacity_case(
                                args.binary, case, case_dir)
                        except Exception as error:
                            output.write(json.dumps({
                                "type": "failure", "case": case,
                                "error": str(error), "logs": str(case_dir)
                            }) + "\n")
                            output.flush()
                            raise
                        result["type"] = "scalability_case"
                        result["schema"] = 1
                        result["wall_signals"] = exact_wall_signals(result)
                        output.write(json.dumps(result, allow_nan=False) + "\n")
                        output.flush()
                        group.append(result)
                        print(
                            f"trial={trial} handler={handler_ms}ms "
                            f"workers={workers} rate={rate}rps "
                            f"ok={result['client']['ok']} "
                            f"busy={result['server_exit']['reactor_busy_ratio']:.3f} "
                            f"signals={','.join(result['wall_signals']) or 'none'}",
                            flush=True)

                    summary = summarize_group(group)
                    output.write(json.dumps(summary, allow_nan=False) + "\n")
                    output.flush()
                    print(
                        f"summary handler={handler_ms}ms workers={workers} "
                        f"clean_schedule={summary['max_clean_scheduled_rps']} "
                        f"server_pressure={summary['first_server_pressure_rps']} "
                        f"generator_late={summary['first_load_generator_late_rps']} "
                        f"generator_drop={summary['first_load_generator_drop_rps']}",
                        flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"scalability benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
