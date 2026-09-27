#!/usr/bin/env python3
"""Fixed-rate RPC capacity diagnostics with explicit overload accounting."""
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

from run_rpc_bench import metadata as closed_loop_metadata
from run_rpc_bench import read_ready, stop_owned, validate_server_exit


OPEN_COUNTS = ("offered", "scheduler_dropped", "attempted", "accepted",
               "completed", "ok", "submit_again", "submit_errors",
               "deadlines", "rpc_errors", "unavailable",
               "resource_exhausted", "invalid_responses")


def validate_open_phase(row: dict[str, Any], expected: int, rate_rps: int,
                        window: int) -> None:
    if row.get("type") != "open_phase" or row.get("schema") != 1:
        raise ValueError("missing open-loop phase/schema")
    if row.get("phase") != "measure" or row.get("scenario") != "open":
        raise ValueError("unexpected open-loop phase")
    if row.get("rate_rps") != rate_rps or row.get("window") != window:
        raise ValueError("wrong open-loop rate/window")

    for key in OPEN_COUNTS:
        if type(row.get(key)) is not int or row[key] < 0:
            raise ValueError(f"{key}: invalid count")
    if row["offered"] != expected:
        raise ValueError("wrong number of offered arrivals")
    if row["scheduler_dropped"] + row["attempted"] != row["offered"]:
        raise ValueError("unaccounted offered arrivals")
    if row["accepted"] + row["submit_again"] + row["submit_errors"] != row["attempted"]:
        raise ValueError("unaccounted submission attempts")
    if row["completed"] != row["accepted"]:
        raise ValueError("accepted work did not drain")
    if row["completed"] != sum(row[k] for k in
                               ("ok", "deadlines", "rpc_errors", "invalid_responses")):
        raise ValueError("unaccounted completions")
    if row["unavailable"] > row["rpc_errors"]:
        raise ValueError("UNAVAILABLE is not a subset of RPC errors")
    if row["resource_exhausted"] > row["rpc_errors"]:
        raise ValueError("RESOURCE_EXHAUSTED is not a subset of RPC errors")
    if row["unavailable"] + row["resource_exhausted"] > row["rpc_errors"]:
        raise ValueError("RPC error subsets overlap or exceed total")
    if row["invalid_responses"]:
        raise ValueError("payload validation failed")

    numeric = ("arrival_window_s", "drain_elapsed_s", "drain_tail_ms",
               "client_cpu_s", "client_peak_rss_kib", "ok_ratio", "ok_rps",
               "payload_MiB_s", "ok_p50_us", "ok_p99_us", "accepted_p99_us",
               "scheduler_late_p50_us", "scheduler_late_p99_us",
               "scheduler_late_max_us")
    for key in numeric:
        value = row.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"{key}: invalid measurement type")
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{key}: invalid measurement")

    if not 0 <= row["ok_ratio"] <= 1:
        raise ValueError("invalid success ratio")
    if row["ok_p50_us"] > row["ok_p99_us"]:
        raise ValueError("unordered success percentiles")
    if not (row["scheduler_late_p50_us"] <= row["scheduler_late_p99_us"] <=
            row["scheduler_late_max_us"]):
        raise ValueError("unordered scheduler lateness")
    if row["start_ns"] >= row["schedule_horizon_ns"] or row["end_ns"] < row["start_ns"]:
        raise ValueError("invalid open-loop timestamps")

    expected_window = expected / rate_rps
    if not math.isclose(row["arrival_window_s"], expected_window,
                        rel_tol=1e-6, abs_tol=1e-9):
        raise ValueError("arrival window does not match offered rate")
    if not math.isclose(row["drain_elapsed_s"],
                        (row["end_ns"] - row["start_ns"]) / 1e9,
                        rel_tol=1e-6, abs_tol=1e-9):
        raise ValueError("inconsistent drain interval")


def nominal_handler_capacity_rps(workers: int, slow_ms: int) -> float | None:
    if slow_ms == 0:
        return None
    return workers * 1000.0 / slow_ms


SERVER_RESOURCE_OVERRIDES = (
    ("rx_buffer_count", "--rx-buffers", "rx_buffers"),
    ("rpc_message_pool_count", "--rpc-message-pool", "rpc_message_pool"),
    ("reassembly_pool_count", "--reassembly-pool", "reassembly_pool"),
)


def server_resource_args(case: dict[str, Any]) -> list[str]:
    args: list[str] = []
    for case_key, cli_flag, _ready_key in SERVER_RESOURCE_OVERRIDES:
        value = int(case.get(case_key, 0) or 0)
        if value:
            args.extend([cli_flag, str(value)])
    return args


def validate_server_ready_resources(ready: dict[str, Any],
                                    case: dict[str, Any]) -> None:
    for case_key, _cli_flag, ready_key in SERVER_RESOURCE_OVERRIDES:
        value = int(case.get(case_key, 0) or 0)
        if value and ready.get(ready_key) != value:
            raise ValueError(f"server did not apply {case_key}={value}")


def run_case(binary: Path, case: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    common = ["--capacity", str(case["capacity"]), "--bulk-bytes", "65536"]
    server_cmd = [str(binary), "server", *common,
                  "--workers", str(case["workers"]),
                  "--slow-ms", str(case["slow_ms"]),
                  "--executor-queue", str(case["executor_queue"]),
                  *server_resource_args(case)]
    client_cmd: list[str] = []
    children: list[subprocess.Popen[str]] = []
    logs: list[TextIO] = []
    started = time.monotonic()

    try:
        server_err = (output_dir / "server.stderr").open("w", encoding="utf-8")
        logs.append(server_err)
        server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=server_err, text=True)
        children.append(server)
        ready = read_ready(server, "ready")
        if (ready.get("workers") != case["workers"] or
                ready.get("executor_queue") != case["executor_queue"] or
                ready.get("slow_ms") != case["slow_ms"]):
            raise ValueError("server did not apply requested capacity controls")
        validate_server_ready_resources(ready, case)

        client_cmd = [str(binary), "client", *common, "--port", str(ready["port"]),
                      "--window", str(case["window"]), "--scenario", "open",
                      "--requests", str(case["requests"]),
                      "--warmup", str(case["warmup"]),
                      "--timeout-ms", str(case["timeout_ms"]),
                      "--rate-rps", str(case["rate_rps"]), "--start-gate", "1"]
        client_err = (output_dir / "client.stderr").open("w", encoding="utf-8")
        logs.append(client_err)
        client = subprocess.Popen(client_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=client_err, text=True)
        children.append(client)
        read_ready(client, "client_ready")
        assert client.stdin is not None
        client.stdin.write("g")
        client.stdin.flush()

        stdout, _ = client.communicate(timeout=180)
        (output_dir / "client.jsonl").write_text(stdout, encoding="utf-8")
        if client.returncode:
            raise RuntimeError(f"client exited {client.returncode}; see {output_dir}")
        rows = [json.loads(line) for line in stdout.splitlines() if line]
        if len(rows) != 1:
            raise ValueError("missing or extra open-loop phase output")
        row = rows[0]
        if row.get("pid") != client.pid:
            raise ValueError("wrong client identity")
        validate_open_phase(row, case["requests"], case["rate_rps"], case["window"])

        server_stdout, _ = server.communicate(timeout=30)
        (output_dir / "server.jsonl").write_text(
            json.dumps(ready) + "\n" + server_stdout, encoding="utf-8")
        if server.returncode:
            raise RuntimeError(f"server exited {server.returncode}; see {output_dir}")
        exit_row = json.loads(server_stdout)
        validate_server_exit(exit_row)

        return {"type": "capacity_case", "case": case,
                "nominal_handler_capacity_rps":
                    nominal_handler_capacity_rps(case["workers"], case["slow_ms"]),
                "server_command": server_cmd, "client_command": client_cmd,
                "server": ready, "server_exit": exit_row, "client": row,
                "runner_elapsed_s": time.monotonic() - started}
    finally:
        for child in reversed(children):
            stop_owned(child)
            for pipe in (child.stdin, child.stdout):
                if pipe is not None:
                    pipe.close()
        for log in logs:
            log.close()


def metadata(binary: Path, label: str, workers: int, slow_ms: int,
             executor_queue: int, window: int) -> dict[str, Any]:
    row = closed_loop_metadata(binary, label)
    row["type"] = "capacity_metadata"
    row["load_model"] = (
        "fixed-rate scheduled arrivals; bounded in-flight slots; every missed "
        "local slot is counted as scheduler_dropped; no coordinated omission")
    row["latency"] = (
        "actual submission to result callback; scheduler lateness is reported "
        "separately for every offered arrival")
    row["workers"] = workers
    row["slow_ms"] = slow_ms
    row["executor_queue"] = executor_queue
    row["window"] = window
    row["nominal_handler_capacity_rps"] = nominal_handler_capacity_rps(workers, slow_ms)
    row["nominal_capacity_note"] = (
        "worker-count / handler-delay arithmetic only; not a measured system capacity")
    return row


def default_rates(workers: int, slow_ms: int) -> list[int]:
    nominal = workers * 1000.0 / slow_ms
    return sorted({max(1, round(nominal * ratio))
                   for ratio in (0.25, 0.5, 1.0, 2.0, 4.0)})


def parse_rates(text: str) -> list[int]:
    try:
        rates = [int(item) for item in text.split(",") if item]
    except ValueError as error:
        raise argparse.ArgumentTypeError("rates must be comma-separated integers") from error
    if not rates or any(rate <= 0 or rate > 1_000_000 for rate in rates):
        raise argparse.ArgumentTypeError("rates must be in 1..1000000")
    return rates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="unspecified-build")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--requests", type=int, default=256)
    parser.add_argument("--rates", type=parse_rates)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--slow-ms", type=int, default=10)
    parser.add_argument("--executor-queue", type=int, default=16)
    parser.add_argument("--window", type=int, default=128)
    parser.add_argument("--capacity", type=int, default=128)
    parser.add_argument("--timeout-ms", type=int, default=1000)
    parser.add_argument("--smoke", action="store_true",
                        help="two short arrival rates; correctness, not capacity")
    args = parser.parse_args()

    binary = args.binary.resolve(strict=True)
    if not os.access(binary, os.X_OK):
        parser.error("binary is not executable")
    if not 1 <= args.trials <= 100 or not 16 <= args.requests <= 1_000_000:
        parser.error("invalid trials or request count")
    if not 1 <= args.workers <= 32 or not 1 <= args.slow_ms <= 1000:
        parser.error("invalid workers or slow-ms")
    if not 16 <= args.executor_queue <= 65536:
        parser.error("executor-queue must be in 16..65536")
    if not 1 <= args.window <= args.capacity <= 256:
        parser.error("require 1 <= window <= capacity <= 256")
    if not 1 <= args.timeout_ms <= 30000:
        parser.error("invalid timeout")

    rates = args.rates or default_rates(args.workers, args.slow_ms)
    count = 48 if args.smoke else args.requests
    warmup = 8 if args.smoke else 32
    trials = 1 if args.smoke else args.trials
    if args.smoke:
        nominal = args.workers * 1000.0 / args.slow_ms
        rates = sorted({max(1, round(nominal * 0.5)),
                        max(1, round(nominal * 4.0))})
    if count / min(rates) > 300:
        parser.error("slowest rate would make one arrival window exceed 300 seconds")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(binary, args.label, args.workers,
                                         args.slow_ms, args.executor_queue,
                                         args.window)) + "\n")
        output.flush()
        for trial in range(1, trials + 1):
            for index, rate in enumerate(rates):
                case = {"trial": trial, "rate_rps": rate, "requests": count,
                        "warmup": warmup, "workers": args.workers,
                        "slow_ms": args.slow_ms,
                        "executor_queue": args.executor_queue,
                        "window": args.window, "capacity": args.capacity,
                        "timeout_ms": args.timeout_ms}
                case_dir = (args.output.parent / (args.output.stem + "-logs") /
                            f"{trial}-{index}-{rate}rps")
                try:
                    result = run_case(binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({"type": "failure", "case": case,
                                             "error": str(error),
                                             "logs": str(case_dir)}) + "\n")
                    output.flush()
                    raise
                output.write(json.dumps(result, allow_nan=False) + "\n")
                output.flush()
                client = result["client"]
                print(f"trial={trial} rate={rate}rps ok={client['ok']} "
                      f"resource_exhausted={client['resource_exhausted']} "
                      f"unavailable={client['unavailable']} "
                      f"dropped={client['scheduler_dropped']}: passed",
                      flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError,
            subprocess.TimeoutExpired) as error:
        print(f"capacity benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
