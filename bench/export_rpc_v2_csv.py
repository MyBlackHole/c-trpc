#!/usr/bin/env python3
"""Export validated RPC V2 benchmark JSONL into a flat, reviewable CSV."""

import argparse
import csv
import hashlib
import json
from pathlib import Path


METADATA_FIELDS = [
    "schema",
    "api_generation",
    "label",
    "git_revision",
    "binary_sha256",
    "platform",
    "cpu_model",
    "cpu_affinity",
    "cgroup_cpu_max",
    "clock",
    "transport",
    "load_model",
    "latency",
    "goodput",
    "server_resources",
]
CASE_FIELDS = [
    "scenario",
    "trial",
    "window",
    "configured_clients",
    "client_index",
    "bulk_bytes",
    "warmup",
    "requests",
    "runner_elapsed_s",
]
PHASE_FIELDS = [
    "phase",
    "phase_scenario",
    "phase_start_ns",
    "phase_end_ns",
    "phase_elapsed_s",
    "client_cpu_s",
    "client_peak_rss_kib",
]
BUCKETS = ("all", "small", "bulk", "slow")
COUNT_METRICS = (
    "attempted",
    "accepted",
    "ok",
    "completed",
    "submit_again",
    "submit_errors",
    "deadlines",
    "rpc_errors",
    "invalid_responses",
)
RATE_METRICS = (
    "ok_rps",
    "payload_MiB_s",
    "ok_p50_us",
    "ok_p99_us",
    "accepted_p99_us",
)
SERVER_METADATA_FIELDS = {"type", "schema", "api_generation"}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _load_jsonl(path):
    records = []
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid JSON at line {line_number}: {exc.msg}"
                ) from exc
    _require(records, "input JSONL is empty")
    return records


def _require_v2(record, record_type):
    _require(record.get("type") == record_type,
             f"expected record type {record_type}")
    _require(record.get("schema") == 2, "schema must be 2")
    _require(record.get("api_generation") == "rpc_v2",
             "api_generation must be rpc_v2")


def _metadata_row(metadata):
    row = {key: metadata.get(key) for key in METADATA_FIELDS}
    row["cpu_affinity"] = json.dumps(metadata.get("affinity"),
                                      separators=(",", ":"))
    row["cgroup_cpu_max"] = json.dumps(metadata.get("cgroup_cpu_max"),
                                       separators=(",", ":"))
    return row


def _validate_bucket_accounting(phase):
    _require(isinstance(phase.get("all"), dict),
             "phase is missing all bucket counters")
    for bucket in BUCKETS:
        _require(isinstance(phase.get(bucket), dict),
                 f"phase is missing {bucket} bucket counters")
        for metric in COUNT_METRICS:
            _require(metric in phase[bucket],
                     f"phase bucket {bucket} is missing {metric}")
    for metric in COUNT_METRICS:
        classified = sum(phase[bucket][metric]
                         for bucket in ("small", "bulk", "slow"))
        _require(classified == phase["all"][metric],
                 f"bucket accounting mismatch for {metric}")
    for bucket in BUCKETS:
        for metric in RATE_METRICS:
            _require(metric in phase[bucket],
                     f"phase bucket {bucket} is missing {metric}")


def _server_field_names(records):
    first = records[0].get("server_exit")
    _require(isinstance(first, dict), "case is missing server_exit")
    _require_v2(first, "server_exit")
    names = set(first) - SERVER_METADATA_FIELDS
    for record in records:
        stats = record.get("server_exit")
        _require(isinstance(stats, dict), "case is missing server_exit")
        _require_v2(stats, "server_exit")
        _require(set(stats) - SERVER_METADATA_FIELDS == names,
                 "server_exit fields differ between cases")
        _require(stats.get("drain_status") == 0,
                 "server_exit drain_status must be zero")
        _require(stats.get("rpc_calls_started") ==
                 stats.get("rpc_calls_finished"),
                 "server RPC call accounting did not converge")
    return sorted(names)


def export_jsonl(input_path, output_path, binary_path):
    """Validate and export one V2 run; return the number of CSV data rows."""
    records = _load_jsonl(input_path)
    metadata = records[0]
    _require_v2(metadata, "metadata")
    revision = metadata.get("git_revision", "")
    _require(len(revision) == 40 and all(ch in "0123456789abcdef"
                                         for ch in revision.lower()),
             "git_revision must be a 40-character commit hash")
    binary = Path(binary_path)
    _require(binary.is_file(), f"benchmark binary does not exist: {binary}")
    actual_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
    _require(actual_hash == metadata.get("binary_sha256"),
             "binary_sha256 does not match the supplied benchmark binary")

    case_records = records[1:]
    _require(case_records, "input JSONL contains no case records")
    server_fields = _server_field_names(case_records)
    fieldnames = (
        METADATA_FIELDS
        + CASE_FIELDS
        + PHASE_FIELDS
        + [f"{bucket}_{metric}" for bucket in BUCKETS
           for metric in COUNT_METRICS + RATE_METRICS]
        + [f"server_{field}" for field in server_fields]
    )
    shared_metadata = _metadata_row(metadata)
    output_rows = []

    for record in case_records:
        _require_v2(record, "case")
        case = record.get("case")
        clients = record.get("clients")
        _require(isinstance(case, dict) and isinstance(clients, list),
                 "case record is missing case or clients")
        stats = record["server_exit"]
        for client_index, phases in enumerate(clients):
            _require(isinstance(phases, list), "client phases must be a list")
            for phase in phases:
                _require_v2(phase, "phase")
                _validate_bucket_accounting(phase)
                row = dict(shared_metadata)
                row.update({
                    "scenario": case["scenario"],
                    "trial": case["trial"],
                    "window": phase["window"],
                    "configured_clients": case["clients"],
                    "client_index": client_index,
                    "bulk_bytes": case["bulk_bytes"],
                    "warmup": case["warmup"],
                    "requests": case["requests"],
                    "runner_elapsed_s": record["runner_elapsed_s"],
                    "phase": phase["phase"],
                    "phase_scenario": phase["scenario"],
                    "phase_start_ns": phase["start_ns"],
                    "phase_end_ns": phase["end_ns"],
                    "phase_elapsed_s": phase["elapsed_s"],
                    "client_cpu_s": phase["client_cpu_s"],
                    "client_peak_rss_kib": phase["client_peak_rss_kib"],
                })
                for bucket in BUCKETS:
                    for metric in COUNT_METRICS + RATE_METRICS:
                        row[f"{bucket}_{metric}"] = phase[bucket][metric]
                row.update({f"server_{field}": stats[field]
                            for field in server_fields})
                output_rows.append(row)

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames,
                                extrasaction="raise", lineterminator="\n")
        writer.writeheader()
        writer.writerows(output_rows)
    return len(output_rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path,
                        help="V2 benchmark JSONL emitted by run_rpc_bench.py")
    parser.add_argument("--binary", required=True, type=Path,
                        help="exact bench_rpc binary used to produce the JSONL")
    parser.add_argument("--output", required=True, type=Path,
                        help="destination CSV path")
    args = parser.parse_args()
    count = export_jsonl(args.input, args.output, args.binary)
    print(f"csv={args.output} rows={count} schema=2 api_generation=rpc_v2")


if __name__ == "__main__":
    main()
