#!/usr/bin/env python3
"""复算 RPC 完整帧到达前的 Reactor RX 排队区间。"""
from __future__ import annotations

import json
import math
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[4]
ARCHIVE = ROOT / "docs/reviews/data/rpc_ingress_schedule_probe_20261030_raw.zip"
OUTPUT = ROOT / "docs/reviews/data/rpc_ingress_schedule_probe_20261030_summary.json"
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
    with ZipFile(ARCHIVE) as archive:
        cases = {
            row["public_profile"]: row
            for row in rows(archive, "repeatability.jsonl")
            if row["type"] == "repeatability_case"
        }
        profiles = []
        for profile in PROFILES:
            directory = f"plain-logs/t1-{profile}-40000rps/"
            server_rows = rows(archive, directory + "server.stderr")
            server_frames = pick(server_rows, "reactor_frame_path_probe")["events"]
            names = ("send_record_span", "send_begin_to_recv_begin",
                     "rx_schedule_to_owner", "owner_entry_to_recv_begin",
                     "recv_call", "recv_end_to_frame_dispatch")
            all_values = {name: {stage: [] for stage in names}
                          for name in ("open", "request")}
            aligned_values = {name: {stage: [] for stage in names[:4]}
                              for name in ("open", "request")}
            stale = {name: {"event_ready_before_send": 0,
                            "rx_schedule_before_send": 0,
                            "both_anchors_after_send": 0,
                            "count": 0}
                     for name in ("open", "request")}
            case = cases[profile]
            matched = 0
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
                    call_frames = (
                        ("open", frame(client_frames, local, peer, 2, 2, stream),
                         frame(server_frames, peer, local, 1, 2, stream)),
                        ("request", frame(client_frames, local, peer, 2, 1,
                                           stream, 2),
                         frame(server_frames, peer, local, 1, 1, stream, 2)),
                    )
                    for name, tx, rx in call_frames:
                        assert len(rx) == 14
                        tx_begin, tx_record = tx[6], tx[7]
                        ready, read_begin, read_end = rx[9], rx[10], rx[11]
                        schedule, owner_begin = rx[12], rx[13]
                        dispatch = rx[7]
                        assert schedule <= owner_begin <= read_begin <= read_end <= dispatch
                        values = all_values[name]
                        values["send_record_span"].append(tx_record - tx_begin)
                        values["send_begin_to_recv_begin"].append(read_begin - tx_begin)
                        values["rx_schedule_to_owner"].append(owner_begin - schedule)
                        values["owner_entry_to_recv_begin"].append(read_begin - owner_begin)
                        values["recv_call"].append(read_end - read_begin)
                        values["recv_end_to_frame_dispatch"].append(dispatch - read_end)
                        counts = stale[name]
                        counts["count"] += 1
                        ready_current = ready >= tx_begin
                        schedule_current = schedule >= tx_begin
                        counts["event_ready_before_send"] += not ready_current
                        counts["rx_schedule_before_send"] += not schedule_current
                        counts["both_anchors_after_send"] += (
                            ready_current and schedule_current)
                        if ready_current and schedule_current:
                            selected = aligned_values[name]
                            selected["send_begin_to_recv_begin"].append(
                                read_begin - tx_begin)
                            selected["rx_schedule_to_owner"].append(
                                owner_begin - schedule)
                            selected["owner_entry_to_recv_begin"].append(
                                read_begin - owner_begin)
                            selected["send_record_span"].append(tx_record - tx_begin)
                    matched += 1
            assert matched == case["client"]["ok"], (
                profile, matched, case["client"]["ok"])
            profiles.append({
                "profile": profile,
                "matched_ok": matched,
                "accepted": case["client"]["accepted"],
                "dropped": case["client"]["scheduler_dropped"],
                "wall_signals": case["wall_signals"],
                "connection_anchor_audit": stale,
                "all_matched_samples": {
                    name: {stage: stats(values) for stage, values in stages.items()}
                    for name, stages in all_values.items()
                },
                "both_readiness_and_queue_anchors_after_send": {
                    name: {"count": len(stages["send_begin_to_recv_begin"]),
                           "stages": {stage: stats(values)
                                     for stage, values in stages.items()}}
                    for name, stages in aligned_values.items()
                },
            })
        result = {
            "binary_sha256": json.loads(archive.read("execution.json"))["binary_sha256"],
            "run_count": 1,
            "percentile_method": "nearest-rank",
            "profiles": profiles,
        }
    OUTPUT.write_text(json.dumps(result, indent=2) + "\n")
    print(OUTPUT.relative_to(ROOT))
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
