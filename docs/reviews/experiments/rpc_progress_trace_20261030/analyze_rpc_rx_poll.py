#!/usr/bin/env python3
"""复算 epoll 返回、Reactor RX owner 与成功帧接收之间的阶段。"""
from __future__ import annotations

import json
import math
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[4]
ARCHIVE = ROOT / "docs/reviews/data/rpc_ingress_poll_hold_probe_20261030_raw.zip"
OUTPUT = ROOT / "docs/reviews/data/rpc_ingress_poll_probe_20261030_summary.json"
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
            for row in rows(archive, "v3/repeatability.jsonl")
            if row["type"] == "repeatability_case"
        }
        profiles = []
        for profile in PROFILES:
            directory = f"v3/plain-logs/t1-{profile}-40000rps/"
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
                    for kind, tx, rx in (
                        ("open", frame(client, (local, peer, 2, 2, stream)),
                         frame(server, (peer, local, 1, 2, stream))),
                        ("request", frame(client, (local, peer, 2, 1, stream), 2),
                         frame(server, (peer, local, 1, 1, stream), 2)),
                    ):
                        assert len(rx) == 17, len(rx)
                        tx_begin = tx[6]
                        poll_return, ready = rx[14], rx[9]
                        schedule, owner_begin = rx[12], rx[13]
                        recv_begin = rx[10]
                        by_kind[kind].append({
                            "tx_begin_ns": tx_begin,
                            "poll_return_ns": poll_return,
                            "event_ready_ns": ready,
                            "schedule_ns": schedule,
                            "owner_begin_ns": owner_begin,
                            "recv_begin_ns": recv_begin,
                            "poll_event_index": rx[15],
                            "poll_event_count": rx[16],
                        })
            case = cases[profile]
            matched = len(by_kind["open"])
            assert len(by_kind["request"]) == matched == case["client"]["ok"], (
                profile, matched, case["client"]["ok"])
            kinds = {}
            for kind, samples in by_kind.items():
                fresh = [sample for sample in samples
                         if sample["poll_return_ns"] >= sample["tx_begin_ns"]]
                assert fresh
                kinds[kind] = {
                    "matched_ok": len(samples),
                    "fresh_poll_return_count": len(fresh),
                    "fresh_poll_return_fraction": len(fresh) / len(samples),
                    "send_begin_to_poll_return": stats([
                        sample["poll_return_ns"] - sample["tx_begin_ns"]
                        for sample in fresh]),
                    "poll_return_to_event_ready": stats([
                        sample["event_ready_ns"] - sample["poll_return_ns"]
                        for sample in fresh]),
                    "poll_return_to_owner_begin": stats([
                        sample["owner_begin_ns"] - sample["poll_return_ns"]
                        for sample in fresh]),
                    "schedule_to_owner_begin": stats([
                        sample["owner_begin_ns"] - sample["schedule_ns"]
                        for sample in fresh]),
                    "owner_begin_to_recv_begin": stats([
                        sample["recv_begin_ns"] - sample["owner_begin_ns"]
                        for sample in fresh]),
                    "event_index": stats([sample["poll_event_index"]
                                          for sample in fresh]),
                    "event_count": stats([sample["poll_event_count"]
                                           for sample in fresh]),
                    "schedule_precedes_tx_count": sum(
                        sample["schedule_ns"] < sample["tx_begin_ns"]
                        for sample in fresh),
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
            "binary_sha256": json.loads(
                archive.read("v3/repeatability.jsonl").decode().splitlines()[0]
            )["binary_sha256"],
            "run_count": 1,
            "percentile_method": "nearest-rank",
            "fresh_definition": (
                "仅保留 Reactor epoll_wait 返回时间不早于关联客户端 TX begin 的成功帧；"
                "该条件不能证明对应 socket 字节在 poll 返回前已完整可读。"
            ),
            "profiles": profiles,
        }
    OUTPUT.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(OUTPUT.relative_to(ROOT))
    print(json.dumps(result, separators=(",", ":"), ensure_ascii=False))


if __name__ == "__main__":
    main()
