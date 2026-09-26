#!/usr/bin/env python3
"""Reproducible separate-process RPC diagnostics; no absolute timing CI gates."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import selectors
import subprocess
import sys
import time
from typing import Any, TextIO


COUNTS = ("attempted", "accepted", "completed", "ok", "submit_again",
          "submit_errors", "deadlines", "rpc_errors", "invalid_responses")


def validate_phase(row: dict[str, Any], expected: int, scenario: str,
                   *, clean: bool) -> None:
    """A fast run with dropped work is a failure, not a performance improvement."""
    if row.get("type") != "phase" or row.get("schema") != 1:
        raise ValueError("missing phase/schema")
    if row.get("scenario") != scenario:
        raise ValueError("unexpected scenario")
    for group in ("all", "small", "bulk", "slow"):
        item = row[group]
        for key in COUNTS:
            if type(item[key]) is not int or item[key] < 0:
                raise ValueError(f"{group}.{key}: invalid count")
        if item["attempted"] != item["accepted"] + item["submit_again"] + item["submit_errors"]:
            raise ValueError("unaccounted submissions")
        if item["accepted"] != item["completed"] or item["completed"] != sum(
                item[k] for k in ("ok", "deadlines", "rpc_errors", "invalid_responses")):
            raise ValueError("unaccounted completions")
        if item["invalid_responses"]:
            raise ValueError("payload validation failed")
        for key in ("ok_rps", "payload_MiB_s", "ok_p50_us", "ok_p99_us", "accepted_p99_us"):
            if not math.isfinite(item[key]) or item[key] < 0:
                raise ValueError(f"{group}.{key}: invalid measurement")
        if item["ok_p50_us"] > item["ok_p99_us"]:
            raise ValueError("unordered percentiles")
    total = row["all"]
    if total["attempted"] != expected:
        raise ValueError("wrong number of attempts")
    for key in COUNTS:
        if total[key] != sum(row[g][key] for g in ("small", "bulk", "slow")):
            raise ValueError(f"class sum mismatch: {key}")
    if not 0 < row["elapsed_s"] < 65 or row["end_ns"] <= row["start_ns"]:
        raise ValueError("invalid timing interval")
    if not math.isclose(row["elapsed_s"], (row["end_ns"] - row["start_ns"]) / 1e9,
                        rel_tol=1e-6, abs_tol=1e-9):
        raise ValueError("inconsistent timing interval")
    for key in ("client_cpu_s", "client_peak_rss_kib"):
        if not math.isfinite(row[key]) or row[key] < 0:
            raise ValueError("invalid resource measurement")
    if clean and total["ok"] != expected:
        raise ValueError("ordinary/recovery scenario did not complete every request")
    if scenario == "pressure" and not (total["deadlines"] + total["rpc_errors"] + total["submit_again"]):
        raise ValueError("pressure scenario did not exercise a failure path")
    if scenario == "mixed" and not (row["small"]["attempted"] and row["bulk"]["attempted"]):
        raise ValueError("mixed scenario is missing one traffic class")


def read_ready(process: subprocess.Popen[str], kind: str, timeout: float = 15) -> dict[str, Any]:
    assert process.stdout is not None
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(timeout):
            raise TimeoutError(f"{kind}: startup timeout")
        line = process.stdout.readline()
    try:
        row = json.loads(line)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{kind}: invalid readiness {line!r}") from error
    if row.get("type") != kind or row.get("pid") != process.pid:
        raise RuntimeError(f"{kind}: wrong readiness record")
    return row


def stop_owned(process: subprocess.Popen[str]) -> None:
    """Only terminate the child this runner created; always reap it."""
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run_case(binary: Path, case: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    common = ["--capacity", "64", "--bulk-bytes", str(case["bulk_bytes"])]
    server_cmd = [str(binary), "server", *common, "--workers", "2", "--slow-ms", "25"]
    children: list[subprocess.Popen[str]] = []
    logs: list[TextIO] = []
    start = time.monotonic()
    try:
        server_err = (output_dir / "server.stderr").open("w", encoding="utf-8")
        logs.append(server_err)
        server = subprocess.Popen(server_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=server_err, text=True)
        children.append(server)
        ready = read_ready(server, "ready")
        clients: list[subprocess.Popen[str]] = []
        commands: list[list[str]] = []
        for client_index in range(case["clients"]):
            cmd = [str(binary), "client", *common, "--port", str(ready["port"]),
                   "--window", str(case["window"]), "--scenario", case["scenario"],
                   "--requests", str(case["requests"]), "--warmup", str(case["warmup"]),
                   "--timeout-ms", "5" if case["scenario"] == "pressure" else "5000",
                   "--start-gate", "1"]
            err = (output_dir / f"client-{client_index}.stderr").open("w", encoding="utf-8")
            logs.append(err)
            child = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=err, text=True)
            children.append(child)
            clients.append(child)
            commands.append(cmd)
        # All connections and warmups complete before any measured client starts.
        for child in clients:
            read_ready(child, "client_ready")
        for child in clients:
            assert child.stdin is not None
            child.stdin.write("g")
            child.stdin.flush()
        results = []
        for index, child in enumerate(clients):
            stdout, _ = child.communicate(timeout=135)
            (output_dir / f"client-{index}.jsonl").write_text(stdout, encoding="utf-8")
            if child.returncode:
                raise RuntimeError(f"client {index} exited {child.returncode}; see {output_dir}")
            rows = [json.loads(line) for line in stdout.splitlines() if line]
            expected_rows = 2 if case["scenario"] == "pressure" else 1
            if len(rows) != expected_rows or rows[0].get("phase") != "measure":
                raise ValueError("missing, extra, or unexpected phase output")
            for row in rows:
                if row.get("pid") != child.pid or row.get("window") != case["window"]:
                    raise ValueError("wrong client identity/window")
            validate_phase(rows[0], case["requests"], case["scenario"],
                           clean=case["scenario"] != "pressure")
            if expected_rows == 2:
                if rows[1].get("phase") != "recovery":
                    raise ValueError("missing recovery phase")
                validate_phase(rows[1], 64, "small", clean=True)
                if rows[1]["start_ns"] < rows[0]["end_ns"]:
                    raise ValueError("recovery started before pressure finished")
            results.append(rows)
        if len(results) > 1 and max(r[0]["start_ns"] for r in results) >= min(
                r[0]["end_ns"] for r in results):
            raise ValueError("multi-client measurements did not overlap")
        stdout, _ = server.communicate(timeout=15)
        (output_dir / "server.jsonl").write_text(json.dumps(ready) + "\n" + stdout,
                                                encoding="utf-8")
        if server.returncode:
            raise RuntimeError(f"server exited {server.returncode}; see {output_dir}")
        exit_row = json.loads(stdout)
        if exit_row.get("type") != "server_exit" or exit_row.get("drain_status") != 0:
            raise ValueError("server did not drain normally")
        return {"type": "case", "case": case, "server_command": server_cmd,
                "client_commands": commands, "server": ready, "server_exit": exit_row,
                "clients": results, "runner_elapsed_s": time.monotonic() - start}
    finally:
        for child in reversed(children):
            stop_owned(child)
            for pipe in (child.stdin, child.stdout):
                if pipe is not None:
                    pipe.close()
        for log in logs:
            log.close()


def metadata(binary: Path, label: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root,
                              text=True, capture_output=True, check=False)
    cpu = Path("/proc/cpuinfo").read_text(encoding="utf-8") if Path("/proc/cpuinfo").exists() else ""
    model = next((line.split(":", 1)[1].strip() for line in cpu.splitlines()
                  if line.startswith("model name")), platform.machine())
    quota = Path("/sys/fs/cgroup/cpu.max")
    return {"type": "metadata", "schema": 1, "label": label,
            "git_revision": revision.stdout.strip() if revision.returncode == 0 else None,
            "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
            "platform": platform.platform(), "cpu_model": model,
            "affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
            "cgroup_cpu_max": quota.read_text().strip() if quota.exists() else None,
            "clock": "CLOCK_MONOTONIC", "transport": "TCP IPv4 loopback",
            "load_model": "closed-loop bounded window; no coordinated-omission correction",
            "latency": "before submission to result callback entry; successful and accepted separately",
            "goodput": "verified successful request+response application bytes; excludes wire overhead",
            "server_resources": "lifetime CPU/RSS including startup/warmup/drain; not phase-only"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="unspecified-build")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--smoke", action="store_true", help="small counts; correctness, not capacity")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if not os.access(binary, os.X_OK) or not 1 <= args.trials <= 100 or not 32 <= args.requests <= 1000000:
        parser.error("invalid executable, trials or request count")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    count = 64 if args.smoke else args.requests
    warmup = 8 if args.smoke else 100
    trials = 1 if args.smoke else args.trials
    cases = [("small", 1, 1, 65536), ("small", 8, 1, 65536),
             ("small", 32, 1, 65536), ("bulk", 8, 1, 65536),
             ("mixed", 8, 1, 65536), ("mixed", 8, 2, 65536),
             ("pressure", 16, 1, 65536)]
    with args.output.open("w", encoding="utf-8") as output:
        output.write(json.dumps(metadata(binary, args.label)) + "\n")
        output.flush()
        for trial in range(trials):
            for index, (scenario, window, clients, bulk_bytes) in enumerate(cases):
                case = {"scenario": scenario, "window": window, "clients": clients,
                        "bulk_bytes": bulk_bytes, "warmup": warmup,
                        "requests": min(count, 128) if scenario == "pressure" else count,
                        "trial": trial + 1}
                case_dir = args.output.parent / (args.output.stem + "-logs") / f"{trial+1}-{index}-{scenario}"
                try:
                    row = run_case(binary, case, case_dir)
                except Exception as error:
                    output.write(json.dumps({"type": "failure", "case": case,
                                             "error": str(error), "logs": str(case_dir)}) + "\n")
                    output.flush()
                    raise
                output.write(json.dumps(row, allow_nan=False) + "\n")
                output.flush()
                print(f"trial={trial+1} {scenario} window={window} clients={clients}: passed", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, TimeoutError, subprocess.TimeoutExpired) as error:
        print(f"benchmark failed: {error}", file=sys.stderr)
        raise SystemExit(1)
