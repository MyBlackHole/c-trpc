#!/usr/bin/env python3
"""从固定追踪 ZIP 复算发送返回至对端完整帧分派的时间。"""
from __future__ import annotations

import json
import math
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[4]
ARCHIVE = ROOT / "docs/reviews/data/rpc_progress_trace_20261030_raw.zip"
OUTPUT = ROOT / "docs/reviews/data/rpc_progress_trace_20261030_send_return_gap.json"
PROFILES = ("small_call_limit", "default_call_limit", "large_call_limit")


def rows_from_zip(archive: ZipFile, name: str) -> list[dict]:
    return [json.loads(line) for line in archive.read(name).decode().splitlines()]


def pick(rows: list[dict], kind: str) -> dict:
    matches = [row for row in rows if row["type"] == kind]
    assert len(matches) == 1, (kind, len(matches))
    return matches[0]


def frame(events: list[list[int]], local: int, peer: int, direction: int,
          kind: int, stream: int, message: int | None = None) -> list[int]:
    matches = [event for event in events
               if event[:5] == [local, peer, direction, kind, stream]
               and (message is None or event[5] == message)]
    assert len(matches) == 1, (local, peer, direction, kind, stream,
                               message, len(matches))
    return matches[0]


def percentile(values: list[int], q: float) -> int:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * q) - 1)]


def stats(values: list[int]) -> dict:
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
    results = []
    with ZipFile(ARCHIVE) as archive:
        for profile in PROFILES:
            base = (f"rpc-frame-path-probe-v182/plain-logs/"
                    f"t1-{profile}-40000rps/")
            server_rows = rows_from_zip(archive, base + "server.stderr")
            server_frames = pick(server_rows, "reactor_frame_path_probe")["events"]
            measurements = {
                "open_send_span_ns": [],
                "open_sender_end_to_server_rx_ns": [],
                "open_sender_end_after_server_rx": 0,
                "request_send_span_ns": [],
                "request_sender_end_to_server_rx_ns": [],
                "request_sender_end_after_server_rx": 0,
                "credit_send_span_ns": [],
                "credit_sender_end_to_client_rx_ns": [],
                "credit_sender_end_after_client_rx": 0,
            }
            matched = 0
            expected = 0
            for generator in range(16):
                client_name = base + f"client-{generator}"
                client_summary = json.loads(
                    archive.read(client_name + ".jsonl").decode())
                expected += client_summary["ok"]
                client_rows = rows_from_zip(archive, client_name + ".stderr")
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
                    client_open = frame(client_frames, local, peer, 2, 2, stream)
                    server_open = frame(server_frames, peer, local, 1, 2, stream)
                    server_credit = frame(server_frames, peer, local, 2, 4, stream)
                    client_credit = frame(client_frames, local, peer, 1, 4, stream)
                    client_request = frame(client_frames, local, peer, 2, 1,
                                           stream, 2)
                    server_request = frame(server_frames, peer, local, 1, 1,
                                           stream, 2)
                    for tx, rx, send_span, return_gap, crossed in (
                        (client_open, server_open, "open_send_span_ns",
                         "open_sender_end_to_server_rx_ns",
                         "open_sender_end_after_server_rx"),
                        (client_request, server_request, "request_send_span_ns",
                         "request_sender_end_to_server_rx_ns",
                         "request_sender_end_after_server_rx"),
                        (server_credit, client_credit, "credit_send_span_ns",
                         "credit_sender_end_to_client_rx_ns",
                         "credit_sender_end_after_client_rx"),
                    ):
                        measurements[send_span].append(tx[7] - tx[6])
                        gap = rx[6] - tx[7]
                        measurements[return_gap].append(gap)
                        if gap < 0:
                            measurements[crossed] += 1
                    matched += 1
            assert matched == expected, (profile, matched, expected)
            results.append({
                "profile": profile,
                "matched_ok": matched,
                "nearest_rank": True,
                "stages": {
                    name: stats(values)
                    for name, values in measurements.items()
                    if isinstance(values, list)
                },
                "receiver_before_sender_record_count": {
                    name.removesuffix("_sender_end_after_server_rx")
                        .removesuffix("_sender_end_after_client_rx"): count
                    for name, count in measurements.items()
                    if isinstance(count, int)
                },
            })
    OUTPUT.write_text(json.dumps(results, indent=2) + "\n")
    print(OUTPUT.relative_to(ROOT))
    for result in results:
        print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
