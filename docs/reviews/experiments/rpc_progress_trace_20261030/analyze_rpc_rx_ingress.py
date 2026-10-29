#!/usr/bin/env python3
"""按成功调用复算 RPC 帧发送、接收调用与完整帧派发边界。"""
from __future__ import annotations

import json
import math
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[4]
ARCHIVE = ROOT / "docs/reviews/data/rpc_ingress_ready_probe_20261030_raw.zip"
OUTPUT = ROOT / "docs/reviews/data/rpc_ingress_ready_probe_20261030_summary.json"
PROFILES = ("small_call_limit", "default_call_limit", "large_call_limit")


def rows(archive: ZipFile, path: str) -> list[dict]:
    return [json.loads(line) for line in archive.read(path).decode().splitlines()]


def pick(rows_: list[dict], kind: str) -> dict:
    matching = [row for row in rows_ if row["type"] == kind]
    assert len(matching) == 1, (kind, len(matching))
    return matching[0]


def frame(events: list[list[int]], local: int, peer: int, direction: int,
          kind: int, stream: int, message: int | None = None) -> list[int]:
    matching = [event for event in events
                if event[:5] == [local, peer, direction, kind, stream]
                and (message is None or event[5] == message)]
    assert len(matching) == 1, (local, peer, direction, kind, stream,
                                message, len(matching))
    return matching[0]


def percentile(values: list[int], p: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * p) - 1)]


def summarize(values: list[int]) -> dict:
    assert values
    return {
        "count": len(values),
        "p50_ns": percentile(values, 0.50),
        "p99_ns": percentile(values, 0.99),
        "mean_ns": sum(values) // len(values),
        "min_ns": min(values),
        "max_ns": max(values),
    }


def main() -> None:
    with ZipFile(ARCHIVE) as archive:
        cases = {
            row["public_profile"]: row
            for row in rows(archive, "repeatability.jsonl")
            if row["type"] == "repeatability_case"
        }
        results = []
        for profile in PROFILES:
            directory = f"plain-logs/t1-{profile}-40000rps/"
            server_rows = rows(archive, directory + "server.stderr")
            server_frames = pick(server_rows, "reactor_frame_path_probe")["events"]
            stages = {
                key: [] for key in (
                    "open_send_record_span_ns",
                    "open_send_begin_to_recv_begin_ns",
                    "open_recv_call_ns",
                    "open_recv_end_to_frame_dispatch_ns",
                    "request_send_record_span_ns",
                    "request_send_begin_to_recv_begin_ns",
                    "request_recv_call_ns",
                    "request_recv_end_to_frame_dispatch_ns",
                    "credit_send_record_span_ns",
                    "credit_send_begin_to_recv_begin_ns",
                    "credit_recv_call_ns",
                    "credit_recv_end_to_frame_dispatch_ns",
                )
            }
            ready_before_tx = {"open": 0, "request": 0}
            matched = 0
            case = cases[profile]
            for generator in range(16):
                client_name = directory + f"client-{generator}"
                client_summary = json.loads(archive.read(client_name + ".jsonl").decode())
                client_rows = rows(archive, client_name + ".stderr")
                admission = pick(client_rows, "rpc_admission_probe")
                credit = pick(client_rows, "client_credit_stage_probe")
                client_frames = pick(client_rows, "reactor_frame_path_probe")["events"]
                open_pairs = {(event[0], event[1]) for event in client_frames
                              if event[2:4] == [2, 2]}
                assert len(open_pairs) == 1, (profile, generator, open_pairs)
                local, peer = next(iter(open_pairs))
                stream_map = dict(credit["stream_map"])
                for index, sample in enumerate(admission["samples"]):
                    if not sample[8]:
                        continue
                    stream = stream_map[(client_summary["pid"] << 32) | index]
                    samples = (
                        ("open", frame(client_frames, local, peer, 2, 2, stream),
                         frame(server_frames, peer, local, 1, 2, stream)),
                        ("request", frame(client_frames, local, peer, 2, 1,
                                           stream, 2),
                         frame(server_frames, peer, local, 1, 1, stream, 2)),
                    )
                    for name, tx, rx in samples:
                        assert len(rx) == 12
                        stages[f"{name}_send_record_span_ns"].append(tx[7] - tx[6])
                        stages[f"{name}_send_begin_to_recv_begin_ns"].append(
                            rx[10] - tx[6])
                        stages[f"{name}_recv_call_ns"].append(rx[11] - rx[10])
                        stages[f"{name}_recv_end_to_frame_dispatch_ns"].append(
                            rx[7] - rx[11])
                        if rx[9] < tx[6]:
                            ready_before_tx[name] += 1
                    server_credit = frame(server_frames, peer, local, 2, 4, stream)
                    client_credit = frame(client_frames, local, peer, 1, 4, stream)
                    assert len(client_credit) == 12
                    stages["credit_send_record_span_ns"].append(
                        server_credit[7] - server_credit[6])
                    stages["credit_send_begin_to_recv_begin_ns"].append(
                        client_credit[10] - server_credit[6])
                    stages["credit_recv_call_ns"].append(
                        client_credit[11] - client_credit[10])
                    stages["credit_recv_end_to_frame_dispatch_ns"].append(
                        client_credit[7] - client_credit[11])
                    matched += 1
            assert matched == case["client"]["ok"], (
                profile, matched, case["client"]["ok"])
            results.append({
                "profile": profile,
                "matched_ok": matched,
                "accepted": case["client"]["accepted"],
                "dropped": case["client"]["scheduler_dropped"],
                "wall_signals": case["wall_signals"],
                "connection_ready_anchor_precedes_frame_send": ready_before_tx,
                "nearest_rank": True,
                "stages": {name: summarize(values)
                           for name, values in stages.items()},
            })
        output = {
            "binary_sha256": json.loads(archive.read("execution.json"))["binary_sha256"],
            "run_count": 1,
            "profiles": results,
        }
    OUTPUT.write_text(json.dumps(output, indent=2) + "\n")
    print(OUTPUT.relative_to(ROOT))
    print(json.dumps(output, separators=(",", ":")))


if __name__ == "__main__":
    main()
