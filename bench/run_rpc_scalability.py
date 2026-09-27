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
import time
from typing import Any, TextIO

from run_rpc_bench import (
    metadata as base_metadata,
    read_ready,
    stop_owned,
    validate_server_exit,
)
from run_rpc_capacity import (
    nominal_handler_capacity_rps,
    run_case as run_capacity_case,
    server_resource_args,
    validate_open_phase,
    validate_server_ready_resources,
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


def generator_count_for(rate_rps: int, max_generators: int,
                        target_rate_per_generator: int) -> int:
    needed = max(1, math.ceil(rate_rps / target_rate_per_generator))
    return min(max_generators, needed)


def split_rates(total_rate: int, generators: int) -> list[int]:
    if generators < 1 or total_rate < generators:
        raise ValueError("cannot split offered rate across generators")
    base, remainder = divmod(total_rate, generators)
    return [base + (1 if i < remainder else 0) for i in range(generators)]


def split_weighted_total(total: int, weights: list[int]) -> list[int]:
    weight_sum = sum(weights)
    raw = [total * weight / weight_sum for weight in weights]
    values = [math.floor(value) for value in raw]
    remainder = total - sum(values)
    order = sorted(range(len(weights)),
                   key=lambda i: raw[i] - values[i], reverse=True)
    for i in order[:remainder]:
        values[i] += 1
    if any(value <= 0 for value in values):
        raise ValueError("request budget too small for generator count")
    return values


def aggregate_client_rows(rows: list[dict[str, Any]],
                          rates: list[int]) -> dict[str, Any]:
    if not rows or len(rows) != len(rates):
        raise ValueError("missing generator results")
    start_ns = min(row["start_ns"] for row in rows)
    end_ns = max(row["end_ns"] for row in rows)
    elapsed = (end_ns - start_ns) / 1e9
    summed = {}
    for key in (
            "offered", "scheduler_dropped", "attempted", "accepted",
            "completed", "ok", "submit_again", "submit_errors",
            "deadlines", "rpc_errors", "unavailable", "resource_exhausted",
            "invalid_responses"):
        summed[key] = sum(row[key] for row in rows)
    summed.update({
        "generator_count": len(rows),
        "generator_rates_rps": rates,
        "start_ns": start_ns,
        "end_ns": end_ns,
        "drain_elapsed_s": elapsed,
        "ok_rps": summed["ok"] / elapsed if elapsed > 0 else 0.0,
        "scheduler_interval_us": min(1_000_000.0 / rate for rate in rates),
        "scheduler_late_p50_us":
            max(row["scheduler_late_p50_us"] for row in rows),
        "scheduler_late_p99_us":
            max(row["scheduler_late_p99_us"] for row in rows),
        "scheduler_late_max_us":
            max(row["scheduler_late_max_us"] for row in rows),
        "client_cpu_s": sum(row["client_cpu_s"] for row in rows),
        "client_peak_rss_kib": sum(row["client_peak_rss_kib"] for row in rows),
    })
    return summed


def run_multisource_case(binary: Path, case: dict[str, Any],
                         output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    common = ["--capacity", str(case["capacity"]), "--bulk-bytes", "65536"]
    server_cmd = [
        str(binary), "server", *common,
        "--workers", str(case["workers"]),
        "--slow-ms", str(case["slow_ms"]),
        "--executor-queue", str(case["executor_queue"]),
        *server_resource_args(case),
    ]
    generator_rates = split_rates(case["rate_rps"], case["generators"])
    request_counts = split_weighted_total(
        case["requests"], generator_rates)
    warmups = split_weighted_total(
        max(case["generators"], case["warmup"]), generator_rates)
    children: list[subprocess.Popen[str]] = []
    logs: list[TextIO] = []
    client_cmds: list[list[str]] = []
    started = time.monotonic()

    try:
        server_err = (output_dir / "server.stderr").open("w", encoding="utf-8")
        logs.append(server_err)
        server = subprocess.Popen(
            server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=server_err, text=True)
        children.append(server)
        ready = read_ready(server, "ready")
        if (ready.get("workers") != case["workers"] or
                ready.get("executor_queue") != case["executor_queue"] or
                ready.get("slow_ms") != case["slow_ms"]):
            raise ValueError("server did not apply requested capacity controls")
        validate_server_ready_resources(ready, case)

        clients: list[subprocess.Popen[str]] = []
        for index, (rate, requests, warmup) in enumerate(
                zip(generator_rates, request_counts, warmups)):
            cmd = [
                str(binary), "client", *common,
                "--port", str(ready["port"]),
                "--window", str(case["window"]),
                "--scenario", "open",
                "--requests", str(requests),
                "--warmup", str(warmup),
                "--timeout-ms", str(case["timeout_ms"]),
                "--rate-rps", str(rate),
                "--start-gate", "1",
            ]
            err = (output_dir / f"client-{index}.stderr").open(
                "w", encoding="utf-8")
            logs.append(err)
            client = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=err, text=True)
            children.append(client)
            clients.append(client)
            client_cmds.append(cmd)

            # Warmup is not part of the offered-load experiment. Let each
            # generator finish its window=1 warmup before starting the next,
            # otherwise generator fanout can turn warmup itself into a
            # saturation workload. Measurement still starts synchronously
            # below, after every Client has reached the start gate.
            read_ready(client, "client_ready", timeout=30)

        common_start_ns = time.monotonic_ns() + 500_000_000
        global_interval_ns = max(1, 1_000_000_000 // case["rate_rps"])
        for index, client in enumerate(clients):
            assert client.stdin is not None
            phase_ns = common_start_ns + index * global_interval_ns
            client.stdin.write(f"t:{phase_ns}\n")
            client.stdin.flush()

        client_rows: list[dict[str, Any]] = []
        for index, (client, requests, rate) in enumerate(
                zip(clients, request_counts, generator_rates)):
            stdout, _ = client.communicate(timeout=180)
            (output_dir / f"client-{index}.jsonl").write_text(
                stdout, encoding="utf-8")
            if client.returncode:
                raise RuntimeError(
                    f"client {index} exited {client.returncode}; see {output_dir}")
            rows = [json.loads(line) for line in stdout.splitlines() if line]
            if len(rows) != 1:
                raise ValueError("missing or extra generator phase output")
            row = rows[0]
            if row.get("pid") != client.pid:
                raise ValueError("wrong generator identity")
            validate_open_phase(row, requests, rate, case["window"])
            client_rows.append(row)

        server_stdout, _ = server.communicate(timeout=30)
        (output_dir / "server.jsonl").write_text(
            json.dumps(ready) + "\n" + server_stdout, encoding="utf-8")
        if server.returncode:
            raise RuntimeError(
                f"server exited {server.returncode}; see {output_dir}")
        exit_row = json.loads(server_stdout)
        validate_server_exit(exit_row)
        if exit_row["peers_ready_total"] < case["generators"]:
            raise ValueError("server peer accounting lost a load generator")

        aggregate = aggregate_client_rows(client_rows, generator_rates)
        return {
            "type": "scalability_case",
            "schema": 1,
            "case": case,
            "nominal_handler_capacity_rps":
                nominal_handler_capacity_rps(
                    case["workers"], case["slow_ms"]),
            "server_command": server_cmd,
            "client_commands": client_cmds,
            "server": ready,
            "server_exit": exit_row,
            "client": aggregate,
            "generators": client_rows,
            "runner_elapsed_s": time.monotonic() - started,
        }
    finally:
        for child in reversed(children):
            stop_owned(child)
            for pipe in (child.stdin, child.stdout):
                if pipe is not None:
                    pipe.close()
        for log in logs:
            log.close()


def scheduler_late_one_interval(result: dict[str, Any]) -> bool:
    rate = result["case"]["rate_rps"]
    interval_us = result["client"].get(
        "scheduler_interval_us", 1_000_000.0 / rate)
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
        "max_generators_used": max(r["case"].get("generators", 1)
                                   for r in ordered),
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
             window: int, max_generators: int,
             target_rate_per_generator: int,
             min_arrival_ms: int) -> dict[str, Any]:
    row = base_metadata(binary, label)
    row["type"] = "scalability_metadata"
    row["schema"] = 1
    row["workers"] = workers
    row["handler_ms"] = handlers
    row["nonzero_handler_rate_ratios"] = ratios
    row["zero_handler_rates_rps"] = zero_rates
    row["executor_queue"] = executor_queue
    row["window"] = window
    row["max_generators"] = max_generators
    row["target_rate_per_generator"] = target_rate_per_generator
    row["min_arrival_ms"] = min_arrival_ms
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
    if not 1 <= args.max_generators <= 16:
        parser.error("max-generators must be in 1..16")
    if not 100 <= args.target_rate_per_generator <= 1_000_000:
        parser.error("invalid target-rate-per-generator")
    if not 10 <= args.min_arrival_ms <= 10_000:
        parser.error("min-arrival-ms must be in 10..10000")


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
                        default=parse_int_list("5000,10000,20000,40000"))
    parser.add_argument("--executor-queue", type=int, default=64)
    parser.add_argument("--window", type=int, default=128)
    parser.add_argument("--capacity", type=int, default=128)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--max-generators", type=int, default=16)
    parser.add_argument("--target-rate-per-generator", type=int, default=4000)
    parser.add_argument("--min-arrival-ms", type=int, default=250)
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
        args.max_generators = 2
        args.target_rate_per_generator = 2500
        args.min_arrival_ms = 20
    validate_args(parser, args)

    warmup = 8 if args.smoke else 32
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(
            args.binary, args.label, args.workers, args.handler_ms,
            args.ratios, args.zero_rates, args.executor_queue,
            args.window, args.max_generators,
            args.target_rate_per_generator,
            args.min_arrival_ms), allow_nan=False) + "\n")
        output.flush()

        for trial in range(1, args.trials + 1):
            for handler_ms in args.handler_ms:
                for workers in args.workers:
                    group: list[dict[str, Any]] = []
                    rates = rates_for(
                        workers, handler_ms, args.zero_rates, args.ratios)
                    for index, rate in enumerate(rates):
                        generators = generator_count_for(
                            rate, args.max_generators,
                            args.target_rate_per_generator)
                        requests = max(
                            args.requests,
                            math.ceil(rate * args.min_arrival_ms / 1000.0))
                        case = {
                            "trial": trial,
                            "rate_rps": rate,
                            "requests": requests,
                            "warmup": warmup,
                            "workers": workers,
                            "slow_ms": handler_ms,
                            "executor_queue": args.executor_queue,
                            "window": args.window,
                            "capacity": args.capacity,
                            "timeout_ms": args.timeout_ms,
                            "generators": generators,
                        }
                        case_dir = (
                            args.output.parent / (args.output.stem + "-logs") /
                            f"t{trial}-h{handler_ms}-w{workers}-{index}-{rate}rps")
                        try:
                            result = (
                                run_capacity_case(args.binary, case, case_dir)
                                if generators == 1
                                else run_multisource_case(
                                    args.binary, case, case_dir))
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
                            f"generators={generators} "
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
