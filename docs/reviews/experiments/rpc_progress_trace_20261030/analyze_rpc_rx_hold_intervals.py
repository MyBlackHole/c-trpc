#!/usr/bin/env python3
"""汇总 v5 RX hold 区间与同连接的客户端发送/服务端接收时间线。"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

PROFILES = ("small_call_limit", "default_call_limit", "large_call_limit")


def records(path: Path) -> dict[str, list[dict[str, Any]]]:
    found: dict[str, list[dict[str, Any]]] = {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.startswith("{"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            kind = row.get("type")
            if kind:
                found.setdefault(kind, []).append(row)
    return found


def nearest(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, (len(ordered) * int(percentile * 100) + 99) // 100 - 1)
    return ordered[min(index, len(ordered) - 1)]


def summarize(values: list[float], unit: str) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "p50_" + unit: nearest(values, 0.50),
        "p90_" + unit: nearest(values, 0.90),
        "p99_" + unit: nearest(values, 0.99),
        "max_" + unit: max(values) if values else None,
        "mean_" + unit: statistics.fmean(values) if values else None,
    }


def analyze_case(logs_root: Path, profile: str) -> dict[str, Any]:
    case_dir = logs_root / f"t1-{profile}-40000rps"
    server = records(case_dir / "server.stderr")
    holds_rows = server["reactor_rx_hold_interval_probe"]
    frame_rows = server["reactor_frame_path_probe"]
    holds_probe = holds_rows[0]
    server_frames = frame_rows[0]["events"]
    hold_events = holds_probe["events"]
    intervals_by_conn: dict[tuple[int, int], list[list[int]]] = {}
    for event in hold_events:
        intervals_by_conn.setdefault((event[0], event[1]), []).append(event)

    client_frames: list[list[int]] = []
    measured_routes: dict[int, set[tuple[int, int, int]]] = {}
    client_count = 0
    for path in sorted(case_dir.glob("client-*.stderr")):
        client_count += 1
        client = records(path)
        file_frames: list[list[int]] = []
        file_id_streams: dict[int, set[int]] = {}
        for frame_probe in client.get("reactor_frame_path_probe", []):
            file_frames.extend(frame_probe["events"])
        for credit in client.get("client_credit_stage_probe", []):
            for call_id, stream_id in credit.get("stream_map", []):
                file_id_streams.setdefault(call_id, set()).add(stream_id)
        client_frames.extend(file_frames)
        for event in file_frames:
            local, peer, direction, frame_type, stream, message = event[:6]
            if direction == 2 and frame_type == 1 and message == 2:
                for call_id, stream_ids in file_id_streams.items():
                    if stream in stream_ids:
                        measured_routes.setdefault(call_id, set()).add(
                            (local, peer, stream)
                        )

    client_data: dict[tuple[int, int, int, int], list[list[int]]] = {}
    server_data: dict[tuple[int, int, int, int], list[list[int]]] = {}
    client_data_frame_count = 0
    server_data_frame_count = 0
    for event in client_frames:
        local, peer, direction, frame_type, stream, message = event[:6]
        if direction == 2 and frame_type == 1:
            client_data_frame_count += 1
            if message == 2:
                key = (local, peer, stream, message)
                client_data.setdefault(key, []).append(event)
    for event in server_frames:
        local, peer, direction, frame_type, stream, message = event[:6]
        if direction == 1 and frame_type == 1:
            server_data_frame_count += 1
            if message == 2:
                server_data.setdefault((peer, local, stream, message), []).append(event)

    stage_events = server.get("unary_server_stage_probe", [{}])[0].get("events", [])
    measured_stages = [event for event in stage_events if (event[0] >> 32) != 0]
    warmup_stages = [event for event in stage_events if (event[0] >> 32) == 0]
    mapped_stages = 0
    stages_with_message_frame = 0
    successful_business_stages = [
        event for event in measured_stages if event[3] == 0
    ]
    business_frame_keys: set[tuple[int, int, int, int]] = set()
    for call_id, _begin_ns, _end_ns, _status in measured_stages:
        routes = measured_routes.get(call_id, set())
        if routes:
            mapped_stages += 1
        for local, peer, stream in routes:
            key = (local, peer, stream, 2)
            if server_data.get(key):
                stages_with_message_frame += 1
                break
    for call_id, _begin_ns, _end_ns, _status in successful_business_stages:
        for local, peer, stream in measured_routes.get(call_id, set()):
            business_frame_keys.add((local, peer, stream, 2))

    matched = []
    pending_intervals_in_gap: set[tuple[int, int, int]] = set()
    sent_before_pending_intervals_in_gap: set[tuple[int, int, int]] = set()
    unmatched_client = 0
    unmatched_server = 0
    for key, tx_events in client_data.items():
        if key not in business_frame_keys:
            continue
        rx_events = server_data.get(key, [])
        if not rx_events:
            unmatched_client += len(tx_events)
            continue
        paired = min(len(tx_events), len(rx_events))
        for tx, rx in zip(tx_events[:paired], rx_events[:paired]):
            tx_begin, tx_end = tx[6], tx[7]
            rx_begin = rx[10]
            if not rx_begin:
                rx_begin = rx[6]
            if rx_begin < tx_begin:
                continue
            server_local, server_peer = key[1], key[0]
            connection_intervals = intervals_by_conn.get(
                (server_local, server_peer), [])
            overlaps = []
            pending_releases = []
            sent_before_pending_releases = []
            for interval_index, interval in enumerate(connection_intervals):
                start, end, complete = interval[4], interval[5], interval[6]
                if not complete:
                    continue
                overlap = min(end, rx_begin) - max(start, tx_begin)
                if overlap > 0:
                    overlaps.append((interval, overlap))
                if interval[7] > 0 and tx_begin <= end <= rx_begin:
                    pending_releases.append(interval)
                    pending_intervals_in_gap.add(
                        (server_local, server_peer, interval_index))
                    if tx_end <= end:
                        sent_before_pending_releases.append(interval)
                        sent_before_pending_intervals_in_gap.add(
                            (server_local, server_peer, interval_index))
            overlap_ns = sum(overlap for _, overlap in overlaps)
            matched.append({
                "key": key,
                "tx_begin_ns": tx_begin,
                "tx_end_ns": tx_end,
                "rx_begin_ns": rx_begin,
                "wire_gap_ns": rx_begin - tx_begin,
                "hold_overlap_ns": overlap_ns,
                "hold_overlap_count": len(overlaps),
                "pending_release_count": len(pending_releases),
                "sent_before_pending_release_count": len(sent_before_pending_releases),
            })
    for key, rx_events in server_data.items():
        tx_count = len(client_data.get(key, []))
        if len(rx_events) > tx_count:
            unmatched_server += len(rx_events) - tx_count

    valid = [row for row in matched if row["wire_gap_ns"] >= 0]
    gap_ms = [row["wire_gap_ns"] / 1e6 for row in valid]
    overlap_pct = [
        100.0 * row["hold_overlap_ns"] / row["wire_gap_ns"]
        for row in valid if row["wire_gap_ns"] > 0
    ]
    hold_overlap = [row for row in valid if row["hold_overlap_ns"] > 0]
    pending_overlap = [row for row in valid if row["pending_release_count"] > 0]
    overlap_and_pending = [
        row for row in valid
        if row["hold_overlap_ns"] > 0 and row["pending_release_count"] > 0
    ]
    overlap_without_pending = [
        row for row in valid
        if row["hold_overlap_ns"] > 0 and row["pending_release_count"] == 0
    ]
    pending_without_overlap = [
        row for row in valid
        if row["hold_overlap_ns"] == 0 and row["pending_release_count"] > 0
    ]
    sent_before_pending = [
        row for row in valid if row["sent_before_pending_release_count"] > 0
    ]
    overlap_pct_nonzero = [value for value in overlap_pct if value > 0]
    complete = [event for event in hold_events if event[6] == 1]
    incomplete = [event for event in hold_events if event[6] == 0]
    positive = [event for event in complete if event[7] > 0]
    no_pending = [event for event in complete if event[7] == 0]
    ioctl_failed = [event for event in complete if event[7] == -2]
    positive_pending_bytes = [float(event[7]) for event in positive]
    positive_parser_at_frame_boundary = [
        event for event in positive
        if event[9] == 0 and event[10] == 0 and event[11] == 0
    ]
    duration_ms = [(event[5] - event[4]) / 1e6 for event in complete]
    connection_generations = {
        (event[0], event[1], event[2], event[3]) for event in hold_events
    }
    return {
        "profile": profile,
        "client_process_count": client_count,
        "server_rx_data_frames_all_records": server_data_frame_count,
        "client_tx_data_frames_all_records": client_data_frame_count,
    "server_business_message_callbacks_measured_pid": len(measured_stages),
        "server_business_message_callbacks_pid_zero_warmup": len(warmup_stages),
        "server_business_message_callbacks_status_ok": len(
            successful_business_stages
        ),
        "server_business_ids_mapped_to_client_stream": mapped_stages,
        "server_business_ids_with_message_id_2_rx_frame": stages_with_message_frame,
        "client_tx_message_record_frames": sum(map(len, client_data.values())),
        "server_rx_message_record_frames": sum(map(len, server_data.values())),
        "matched_successful_business_message_frames": len(valid),
        "unmatched_client_message_record_tx_frames": unmatched_client,
        "unmatched_server_message_record_rx_frames": unmatched_server,
        "server_socket_tuple_count": len({(e[0], e[1]) for e in hold_events}),
        "hold_slot_generation_count": len(connection_generations),
        "hold_capacity": holds_probe["capacity"],
        "hold_overflow": holds_probe["overflow"],
        "hold_complete_count": len(complete),
        "hold_incomplete_close_count": len(incomplete),
        "hold_release_pending_positive_count": len(positive),
        "hold_release_pending_zero_count": len(no_pending),
        "hold_release_ioctl_failure_count": len(ioctl_failed),
        "hold_release_pending_positive_percent": (
            100.0 * len(positive) / len(complete) if complete else None
        ),
        "hold_release_pending_bytes": summarize(
            positive_pending_bytes, "bytes"
        ),
        "hold_release_pending_at_parser_frame_boundary_count": len(
            positive_parser_at_frame_boundary
        ),
        "hold_duration": summarize(duration_ms, "ms"),
        "send_begin_to_server_recv_begin": summarize(gap_ms, "ms"),
        "matched_frames_with_hold_overlap": len(hold_overlap),
        "matched_frames_with_hold_overlap_percent": (
            100.0 * len(hold_overlap) / len(valid) if valid else None
        ),
        "hold_overlap_fraction_percent": summarize(overlap_pct, "percent"),
        "matched_frames_with_pending_release_in_gap": len(pending_overlap),
        "matched_frames_with_hold_overlap_and_pending_release": len(
            overlap_and_pending
        ),
        "matched_frames_with_hold_overlap_without_pending_release": len(
            overlap_without_pending
        ),
        "matched_frames_with_pending_release_without_hold_overlap": len(
            pending_without_overlap
        ),
        "matched_frames_with_pending_release_in_gap_percent": (
            100.0 * len(pending_overlap) / len(valid) if valid else None
        ),
        "matched_frames_with_send_return_before_pending_release_in_gap": len(
            sent_before_pending
        ),
        "matched_frames_with_send_return_before_pending_release_in_gap_percent": (
            100.0 * len(sent_before_pending) / len(valid) if valid else None
        ),
        "distinct_pending_release_intervals_in_any_gap": len(
            pending_intervals_in_gap
        ),
        "distinct_pending_release_intervals_after_send_return_in_gap": len(
            sent_before_pending_intervals_in_gap
        ),
        "hold_overlap_fraction_when_nonzero": summarize(
            overlap_pct_nonzero, "percent"
        ),
        "probe_note": (
            "Unary request record sequence is OPEN=message_id 1, MESSAGE=2, "
            "HALF_CLOSE=3; only the MESSAGE frame is used for the per-call "
            "window. Client TX is paired with server RX by reversed TCP ports, "
            "stream_id and message_id. FIONREAD is sampled at hold release and "
            "proves queued bytes at that instant, but does not identify their "
            "stream. Per-call hold overlap is a temporal correlation."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    out = {
        "schema": 1,
        "date": "2026-10-30",
        "experiment": "rpc-ingress-rx-hold-v5",
        "profiles": [analyze_case(args.logs_dir, profile)
                     for profile in PROFILES],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(out, indent=2, allow_nan=False) + "\n",
                           encoding="utf-8")
    for row in out["profiles"]:
        print(
            f"{row['profile']}: matched={row['matched_successful_business_message_frames']} "
            f"hold={row['hold_complete_count']} "
            f"pending={row['hold_release_pending_positive_count']}/"
            f"{row['hold_complete_count']} "
            f"overlap={row['matched_frames_with_hold_overlap']}/"
            f"{row['matched_successful_business_message_frames']} "
            f"pending_in_gap={row['matched_frames_with_pending_release_in_gap']}/"
            f"{row['matched_successful_business_message_frames']} "
            f"gap_p50={row['send_begin_to_server_recv_begin']['p50_ms']:.3f}ms "
            f"coverage_p50={row['hold_overlap_fraction_percent']['p50_percent']:.2f}%"
        )


if __name__ == "__main__":
    main()
