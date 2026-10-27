#!/usr/bin/env python3
"""Public-limit RPC scalability matrix with bounded open-loop generators."""
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
from run_rpc_capacity import run_case as run_capacity_case
from run_rpc_capacity import transport_cli_args
from run_rpc_capacity import validate_open_phase


SERVER_PRESSURE_FIELDS = {
    "reactor_command_queue_full": "command_queue_full_events",
    "reactor_completion_queue_full": "completion_queue_full_events",
    "rx_pool_exhausted": "rx_pool_exhausted_events",
    "tx_pool_exhausted": "tx_pool_exhausted_events",
    "control_tx_pool_exhausted": "control_tx_pool_exhausted_events",
    "reassembly_pool_exhausted": "reassembly_pool_exhausted_events",
}


def parse_int_list(text: str) -> list[int]:
    try:
        values = [int(item) for item in text.split(",") if item]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or any(value <= 0 for value in values):
        raise argparse.ArgumentTypeError("values must be positive integers")
    return values


def parse_nonnegative_int_list(text: str) -> list[int]:
    try:
        values = [int(item) for item in text.split(",") if item]
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected comma-separated integers") from error
    if not values or any(value < 0 for value in values):
        raise argparse.ArgumentTypeError("values must be non-negative integers")
    return values


def split_rates(total_rate: int, generators: int) -> list[int]:
    if generators < 1 or total_rate < generators:
        raise ValueError("cannot split offered rate across generators")
    base, remainder = divmod(total_rate, generators)
    return [base + (1 if index < remainder else 0)
            for index in range(generators)]


def split_weighted_total(total: int, weights: list[int]) -> list[int]:
    weight_sum = sum(weights)
    raw = [total * weight / weight_sum for weight in weights]
    values = [math.floor(value) for value in raw]
    remainder = total - sum(values)
    order = sorted(range(len(weights)),
                   key=lambda index: raw[index] - values[index], reverse=True)
    for index in order[:remainder]:
        values[index] += 1
    if any(value <= 0 for value in values):
        raise ValueError("request budget too small for generator count")
    return values


def generator_count_for(rate_rps: int, maximum: int,
                        target_rate_per_generator: int) -> int:
    return min(maximum, max(1, math.ceil(rate_rps / target_rate_per_generator)))


def scheduler_late_one_interval(result: dict[str, Any]) -> bool:
    interval = result["client"].get("scheduler_interval_us", 0.0)
    return bool(interval and
                result["client"]["scheduler_late_p99_us"] > interval)


def aggregate_client_rows(rows: list[dict[str, Any]],
                          rates: list[int]) -> dict[str, Any]:
    if not rows or len(rows) != len(rates):
        raise ValueError("missing generator results")
    start_ns = min(row["start_ns"] for row in rows)
    end_ns = max(row["end_ns"] for row in rows)
    elapsed = (end_ns - start_ns) / 1e9
    keys = (
        "offered", "scheduler_dropped", "attempted", "accepted",
        "completed", "ok", "submit_again", "submit_errors", "deadlines",
        "rpc_errors", "unavailable", "resource_exhausted",
        "invalid_responses",
    )
    summed = {key: sum(row[key] for row in rows) for key in keys}
    summed.update({
        "generator_count": len(rows),
        "generator_rates_rps": rates,
        "start_ns": start_ns,
        "end_ns": end_ns,
        "drain_elapsed_s": elapsed,
        "ok_rps": summed["ok"] / elapsed if elapsed > 0 else 0.0,
        "scheduler_interval_us": min(1_000_000.0 / rate for rate in rates),
        "scheduler_late_p50_us": max(row["scheduler_late_p50_us"] for row in rows),
        "scheduler_late_p99_us": max(row["scheduler_late_p99_us"] for row in rows),
        "scheduler_late_max_us": max(row["scheduler_late_max_us"] for row in rows),
        "client_cpu_s": sum(row["client_cpu_s"] for row in rows),
        "client_peak_rss_kib": sum(row["client_peak_rss_kib"] for row in rows),
    })
    return summed


def run_multisource_case(binary: Path, case: dict[str, Any],
                         output_dir: Path) -> dict[str, Any]:
    case = {**case, "compression": case.get("compression", "none"),
            "tls": case.get("tls", False)}
    transport_args = transport_cli_args(case)
    output_dir.mkdir(parents=True, exist_ok=True)
    bulk_bytes = int(case.get("bulk_bytes", 65536))
    server_common = ["--capacity", str(case["capacity"]),
                     "--bulk-bytes", str(bulk_bytes)]
    generator_capacity = int(case.get("generator_capacity", case["capacity"]))
    generator_window = int(case.get("generator_window", case["window"]))
    client_common = ["--capacity", str(generator_capacity),
                     "--bulk-bytes", str(int(case.get("generator_bulk_bytes", bulk_bytes)))]
    server_cmd = [str(binary), "server", *server_common,
                  *transport_args, "--slow-ms", str(case["slow_ms"])]
    rates = split_rates(case["rate_rps"], case["generators"])
    requests = split_weighted_total(case["requests"], rates)
    warmups = split_weighted_total(max(case["generators"], case["warmup"]), rates)
    children: list[subprocess.Popen[str]] = []
    logs: list[TextIO] = []
    client_commands: list[list[str]] = []
    started = time.monotonic()

    try:
        server_err = (output_dir / "server.stderr").open("w", encoding="utf-8")
        logs.append(server_err)
        server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=server_err,
                                  text=True)
        children.append(server)
        ready = read_ready(server, "ready")
        if ready.get("capacity") != case["capacity"] or ready.get("slow_ms") != case["slow_ms"]:
            raise ValueError("server did not apply public RPC limits or work delay")

        clients: list[subprocess.Popen[str]] = []
        for index, (rate, request_count, warmup_count) in enumerate(
                zip(rates, requests, warmups)):
            command = [str(binary), "client", *client_common,
                       *transport_args,
                       "--port", str(ready["port"]),
                       "--window", str(generator_window),
                       "--scenario", "open",
                       "--requests", str(request_count),
                       "--warmup", str(warmup_count),
                       "--timeout-ms", str(case["timeout_ms"]),
                       "--rate-rps", str(rate), "--start-gate", "1"]
            client_err = (output_dir / f"client-{index}.stderr").open(
                "w", encoding="utf-8")
            logs.append(client_err)
            client = subprocess.Popen(command, stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE,
                                      stderr=client_err, text=True)
            children.append(client)
            clients.append(client)
            client_commands.append(command)
            read_ready(client, "client_ready", timeout=30)

        common_start_ns = time.monotonic_ns() + 500_000_000
        global_interval_ns = max(1, 1_000_000_000 // case["rate_rps"])
        for index, client in enumerate(clients):
            assert client.stdin is not None
            client.stdin.write(f"t:{common_start_ns + index * global_interval_ns}\n")
            client.stdin.flush()

        rows: list[dict[str, Any]] = []
        for index, (client, count, rate) in enumerate(zip(clients, requests, rates)):
            stdout, _ = client.communicate(timeout=180)
            (output_dir / f"client-{index}.jsonl").write_text(stdout, encoding="utf-8")
            if client.returncode:
                raise RuntimeError(f"client {index} exited {client.returncode}; see {output_dir}")
            records = [json.loads(line) for line in stdout.splitlines() if line]
            if len(records) != 1:
                raise ValueError("missing or extra generator phase output")
            row = records[0]
            if row.get("pid") != client.pid:
                raise ValueError("wrong generator identity")
            validate_open_phase(row, count, rate, generator_window)
            rows.append(row)

        server_stdout, _ = server.communicate(timeout=30)
        (output_dir / "server.jsonl").write_text(
            json.dumps(ready) + "\n" + server_stdout, encoding="utf-8")
        if server.returncode:
            raise RuntimeError(f"server exited {server.returncode}; see {output_dir}")
        exit_row = json.loads(server_stdout)
        validate_server_exit(exit_row)
        if exit_row["peers_ready_total"] < case["generators"]:
            raise ValueError("server peer accounting lost a load generator")

        return {
            "type": "scalability_case", "schema": 2,
            "api_generation": "rpc_v2", "case": case,
            "server_command": server_cmd,
            "client_commands": client_commands,
            "server": ready, "server_exit": exit_row,
            "client": aggregate_client_rows(rows, rates),
            "generators": rows,
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


def exact_wall_signals(result: dict[str, Any]) -> list[str]:
    client = result["client"]
    server = result["server_exit"]
    signals: list[str] = []
    interval = client.get("scheduler_interval_us", 0.0)
    if client["scheduler_dropped"]:
        signals.append("load_generator_drop")
    if interval and client["scheduler_late_p99_us"] > interval:
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
    if any(client[key] for key in (
            "scheduler_dropped", "submit_again", "submit_errors", "deadlines",
            "rpc_errors", "invalid_responses")):
        return False
    interval = client.get("scheduler_interval_us", 0.0)
    if interval and client["scheduler_late_p99_us"] > interval:
        return False
    return not any(result["server_exit"][key]
                   for key in SERVER_PRESSURE_FIELDS.values())


def summarize_group(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        raise ValueError("cannot summarize an empty scalability group")
    ordered = sorted(results, key=lambda item: item["case"]["rate_rps"])
    first = ordered[0]["case"]
    clean_rates = [item["case"]["rate_rps"] for item in ordered
                   if clean_result(item)]
    pressure_rates = [item["case"]["rate_rps"] for item in ordered
                      if any(item["server_exit"][key]
                             for key in SERVER_PRESSURE_FIELDS.values())]
    drops = [item["case"]["rate_rps"] for item in ordered
             if item["client"]["scheduler_dropped"]]
    late = [item["case"]["rate_rps"] for item in ordered
            if item["client"].get("scheduler_interval_us", 0.0) and
            item["client"]["scheduler_late_p99_us"] >
            item["client"]["scheduler_interval_us"]]
    return {
        "type": "scalability_summary", "schema": 2,
        "api_generation": "rpc_v2", "trial": first["trial"],
        "capacity": first["capacity"], "slow_ms": first["slow_ms"],
        "window": first["window"],
        "max_generators_used": max(item["case"]["generators"] for item in ordered),
        "max_clean_scheduled_rps": max(clean_rates) if clean_rates else None,
        "first_server_pressure_rps": min(pressure_rates) if pressure_rates else None,
        "first_load_generator_drop_rps": min(drops) if drops else None,
        "first_load_generator_late_rps": min(late) if late else None,
        "max_observed_ok_rps": max(item["client"]["ok_rps"] for item in ordered),
        "wall_signals": sorted({signal for item in ordered
                                for signal in exact_wall_signals(item)}),
        "rate_points": [item["case"]["rate_rps"] for item in ordered],
    }


def metadata(binary: Path, label: str, capacities: list[int],
             handlers: list[int], rates: list[int], window: int,
             max_generators: int, target_rate_per_generator: int) -> dict[str, Any]:
    row = base_metadata(binary, label, load_mode="open")
    row.update({
        "type": "scalability_metadata", "schema": 2,
        "api_generation": "rpc_v2", "capacities": capacities,
        "slow_ms": handlers, "offered_rates_rps": rates,
        "window": window, "max_generators": max_generators,
        "target_rate_per_generator": target_rate_per_generator,
        "tls_enabled": False, "compression": "none",
        "load_model": "fixed-rate V2 unary arrivals; bounded public call limits; "
                       "scheduler drops and terminal latency are reported",
        "attribution": "client accounting and existing V2 Reactor/pool signals; "
                       "no V1 executor or message-pool counters",
    })
    return row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="unspecified-build")
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--capacities", type=parse_int_list, default=[64, 128])
    parser.add_argument("--slow-ms", type=parse_nonnegative_int_list, default=[0, 10])
    parser.add_argument("--rates", type=parse_int_list, default=[250, 1000, 4000])
    parser.add_argument("--window", type=int, default=32)
    parser.add_argument("--max-generators", type=int, default=4)
    parser.add_argument("--target-rate-per-generator", type=int, default=1000)
    parser.add_argument("--timeout-ms", type=int, default=3000)
    parser.add_argument("--smoke", action="store_true",
                        help="small public-limit matrix for runner correctness")
    args = parser.parse_args()
    args.binary = args.binary.resolve(strict=True)
    if not os.access(args.binary, os.X_OK):
        parser.error("binary is not executable")
    if not 1 <= args.trials <= 20 or not 16 <= args.requests <= 1_000_000:
        parser.error("invalid trials or request count")
    if any(value > 256 for value in args.capacities):
        parser.error("capacities must be in 1..256")
    if any(value > 1000 for value in args.slow_ms):
        parser.error("slow-ms must be in 0..1000")
    if not 1 <= args.window <= min(args.capacities):
        parser.error("window must fit every public call capacity")
    if not 1 <= args.max_generators <= 16 or not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid generator count or timeout")
    if args.smoke:
        args.capacities = [16, 64]
        args.slow_ms = [0, 10]
        args.rates = [100, 800]
        args.window = min(args.window, 8)
        args.requests = 32
        args.trials = 1
        args.max_generators = 2
        args.target_rate_per_generator = 400

    warmup = 8
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(
            args.binary, args.label, args.capacities, args.slow_ms,
            args.rates, args.window, args.max_generators,
            args.target_rate_per_generator), allow_nan=False) + "\n")
        output.flush()
        for trial in range(1, args.trials + 1):
            for capacity in args.capacities:
                for slow_ms in args.slow_ms:
                    group: list[dict[str, Any]] = []
                    for index, rate in enumerate(args.rates):
                        generators = generator_count_for(
                            rate, args.max_generators,
                            args.target_rate_per_generator)
                        case = {
                            "trial": trial, "rate_rps": rate,
                            "requests": max(args.requests, generators * 8),
                            "warmup": max(warmup, generators),
                            "slow_ms": slow_ms, "window": args.window,
                            "capacity": capacity, "timeout_ms": args.timeout_ms,
                            "generators": generators,
                        }
                        case_dir = (args.output.parent /
                                    (args.output.stem + "-logs") /
                                    f"t{trial}-c{capacity}-h{slow_ms}-{index}-{rate}")
                        try:
                            result = run_multisource_case(
                                args.binary, case, case_dir)
                        except Exception as error:
                            output.write(json.dumps({
                                "type": "failure", "schema": 2,
                                "api_generation": "rpc_v2", "case": case,
                                "error": str(error), "logs": str(case_dir),
                            }) + "\n")
                            output.flush()
                            raise
                        result["type"] = "scalability_case"
                        result["schema"] = 2
                        result["api_generation"] = "rpc_v2"
                        result["wall_signals"] = exact_wall_signals(result)
                        output.write(json.dumps(result, allow_nan=False) + "\n")
                        output.flush()
                        group.append(result)
                        print(f"trial={trial} capacity={capacity} slow={slow_ms}ms "
                              f"rate={rate}rps generators={generators} "
                              f"ok={result['client']['ok']} "
                              f"signals={','.join(result['wall_signals']) or 'none'}",
                              flush=True)
                    summary = summarize_group(group)
                    output.write(json.dumps(summary, allow_nan=False) + "\n")
                    output.flush()
                    print(f"summary capacity={capacity} slow={slow_ms}ms "
                          f"max_clean_rate={summary['max_clean_scheduled_rps']}",
                          flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"scalability benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
