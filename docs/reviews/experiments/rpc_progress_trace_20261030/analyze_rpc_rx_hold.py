#!/usr/bin/env python3
"""按成功调用复算 Reactor RX hold 与收包等待的已记录区间重叠。"""
from __future__ import annotations

import json
import math
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[4]
ARCHIVE = ROOT / "docs/reviews/data/rpc_ingress_poll_hold_probe_20261030_raw.zip"
OUTPUT = ROOT / "docs/reviews/data/rpc_ingress_hold_probe_20261030_summary.json"
PROFILES = ("small_call_limit", "default_call_limit", "large_call_limit")


def rows(archive: ZipFile, path: str) -> list[dict]:
    return [json.loads(line) for line in archive.read(path).decode().splitlines()
            if line.strip()]


def pick(rows_: list[dict], kind: str) -> dict:
    matching = [row for row in rows_ if row.get("type") == kind]
    assert len(matching) == 1, (kind, len(matching))
    return matching[0]


def index_events(events: list[list[int]]) -> dict[tuple[int, ...], list[list[int]]]:
    indexed: dict[tuple[int, ...], list[list[int]]] = {}
    for event in events:
        indexed.setdefault(tuple(event[:5]), []).append(event)
    return indexed


def frame(index: dict[tuple[int, ...], list[list[int]]], key: tuple[int, ...],
          message: int | None = None) -> list[int]:
    matching = index[key]
    if message is not None:
        matching = [event for event in matching if event[5] == message]
    assert len(matching) == 1, (key, message, len(matching))
    return matching[0]


def percentile(values: list[int | float], p: float) -> int | float:
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
            for row in rows(archive, "v4/repeatability.jsonl")
            if row["type"] == "repeatability_case"
        }
        profiles = []
        for profile in PROFILES:
            directory = f"v4/plain-logs/t1-{profile}-40000rps/"
            server = index_events(pick(rows(archive, directory + "server.stderr"),
                                       "reactor_frame_path_probe")["events"])
            by_kind = {"open": [], "request": []}
            for generator in range(16):
                client_name = directory + f"client-{generator}"
                client_rows = rows(archive, client_name + ".stderr")
                pid = rows(archive, client_name + ".jsonl")[0]["pid"]
                admission = pick(client_rows, "rpc_admission_probe")
                credit = pick(client_rows, "client_credit_stage_probe")
                client = index_events(
                    pick(client_rows, "reactor_frame_path_probe")["events"])
                open_pairs = {(key[0], key[1]) for key in client
                              if key[2:4] == (2, 2)}
                assert len(open_pairs) == 1, (profile, generator, open_pairs)
                local, peer = next(iter(open_pairs))
                stream_map = dict(credit["stream_map"])
                for sample_index, sample in enumerate(admission["samples"]):
                    if not sample[8]:
                        continue
                    stream = stream_map[(pid << 32) | sample_index]
                    frames = (
                        ("open", frame(client, (local, peer, 2, 2, stream)),
                         frame(server, (peer, local, 1, 2, stream))),
                        ("request", frame(client, (local, peer, 2, 1, stream), 2),
                         frame(server, (peer, local, 1, 1, stream), 2)),
                    )
                    for kind, tx, rx in frames:
                        tx_begin, recv_begin = tx[6], rx[10]
                        wait = max(0, recv_begin - tx_begin)
                        hold_start, hold_end = rx[17], rx[18]
                        pause_start, pause_end = rx[22], rx[23]
                        hold_overlap = (
                            max(0, min(hold_end, recv_begin) -
                                max(hold_start, tx_begin))
                            if hold_start and hold_end else 0
                        )
                        pause_overlap = (
                            max(0, min(pause_end, recv_begin) -
                                max(pause_start, tx_begin))
                            if pause_start and pause_end else 0
                        )
                        by_kind[kind].append({
                            "wait_ns": wait,
                            "hold_overlap_ns": hold_overlap,
                            "pause_overlap_ns": pause_overlap,
                            "hold_count": rx[21],
                            "pause_count": rx[26],
                            "hold_start_ns": hold_start,
                            "hold_end_ns": hold_end,
                            "tx_begin_ns": tx_begin,
                            "recv_begin_ns": recv_begin,
                        })
            case = cases[profile]
            matched = len(by_kind["open"])
            assert len(by_kind["request"]) == matched == case["client"]["ok"], (
                profile, matched, case["client"]["ok"])
            kinds = {}
            for kind, samples in by_kind.items():
                waits = [sample["wait_ns"] for sample in samples]
                overlaps = [sample["hold_overlap_ns"] for sample in samples]
                ratios = [sample["hold_overlap_ns"] / sample["wait_ns"]
                          for sample in samples if sample["wait_ns"]]
                kinds[kind] = {
                    "matched_ok": len(samples),
                    "send_begin_to_recv_begin": stats(waits),
                    "last_recorded_hold_overlap": {
                        "samples_with_overlap": sum(value > 0 for value in overlaps),
                        "fraction": sum(value > 0 for value in overlaps) / len(samples),
                        "overlap_ns": stats(overlaps),
                        "wait_covered_fraction_p50": percentile(ratios, 0.50),
                        "wait_covered_fraction_p99": percentile(ratios, 0.99),
                        "samples_covering_at_least_50_percent": sum(
                            2 * sample["hold_overlap_ns"] >= sample["wait_ns"]
                            for sample in samples if sample["wait_ns"]),
                        "samples_covering_at_least_90_percent": sum(
                            10 * sample["hold_overlap_ns"] >= 9 * sample["wait_ns"]
                            for sample in samples if sample["wait_ns"]),
                    },
                    "pool_pause_overlap_samples": sum(
                        sample["pause_overlap_ns"] > 0 for sample in samples),
                    "hold_count_nonzero_samples": sum(
                        sample["hold_count"] > 0 for sample in samples),
                    "max_hold_count": max(sample["hold_count"] for sample in samples),
                    "max_pool_pause_count": max(sample["pause_count"]
                                                 for sample in samples),
                }
            profiles.append({
                "profile": profile,
                "matched_ok": matched,
                "accepted": case["client"]["accepted"],
                "scheduler_dropped": case["client"]["scheduler_dropped"],
                "wall_signals": case["wall_signals"],
                "kinds": kinds,
            })
        result = {
            "v3_binary_sha256": json.loads(
                archive.read("v3/repeatability.jsonl").decode().splitlines()[0]
            )["binary_sha256"],
            "v4_binary_sha256": json.loads(
                archive.read("v4/repeatability.jsonl").decode().splitlines()[0]
            )["binary_sha256"],
            "run_count_per_version": 1,
            "percentile_method": "nearest-rank",
            "hold_overlap_definition": (
                "仅使用 RX 事件保存的该连接最近一个已完成 rx_held 区间，"
                "与 [客户端 TX begin, 服务端 recv begin] 求交；不代表该区间内全部 hold。"
            ),
            "profiles": profiles,
        }
    OUTPUT.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(OUTPUT.relative_to(ROOT))
    print(json.dumps(result, separators=(",", ":"), ensure_ascii=False))


if __name__ == "__main__":
    main()
