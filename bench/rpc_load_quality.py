"""固定速率负载的入口近似校准；不推断服务端到达或业务 SLO。"""
from __future__ import annotations

import math
from typing import Any



def validate_submission_evidence(row: dict[str, Any]) -> None:
    """旧记录无此字段时兼容读取，新记录必须完整自洽。"""
    if "submission_schema" not in row:
        return
    if type(row["submission_schema"]) is not int or row["submission_schema"] != 1:
        raise ValueError("invalid submission schema")
    for key in ("first_submit_ns", "last_submit_ns", "submitted_in_window",
                "accepted_in_window", "completed_in_window"):
        if type(row.get(key)) is not int or row[key] < 0:
            raise ValueError(f"invalid submission evidence: {key}")
    if not 0 <= row["submitted_in_window"] <= row["attempted"]:
        raise ValueError("inconsistent in-window submissions")
    if not 0 <= row["accepted_in_window"] <= min(row["accepted"], row["submitted_in_window"]):
        raise ValueError("inconsistent in-window accepted submissions")
    if row["submitted_in_window"] - row["accepted_in_window"] > row["submit_again"] + row["submit_errors"]:
        raise ValueError("inconsistent in-window submission failures")
    if not 0 <= row["completed_in_window"] <= min(row["completed"], row["accepted_in_window"]):
        raise ValueError("inconsistent in-window completions")
    if row["attempted"]:
        if not (row["start_ns"] <= row["first_submit_ns"] <=
                row["last_submit_ns"] <= row["end_ns"]):
            raise ValueError("unordered submission timestamps")
    elif row["first_submit_ns"] or row["last_submit_ns"]:
        raise ValueError("timestamps without submission")
    p99, maximum = row["submit_late_p99_us"], row["submit_late_max_us"]
    if not all(type(v) in (int, float) and math.isfinite(v) for v in (p99, maximum)) or not 0 <= p99 <= maximum:
        raise ValueError("invalid submission lateness")
    if row["attempted"]:
        horizon = row["schedule_horizon_ns"]
        if ((row["last_submit_ns"] < horizon) !=
                (row["submitted_in_window"] == row["attempted"])):
            raise ValueError("submission extrema contradict in-window count")
        if ((row["first_submit_ns"] >= horizon) !=
                (row["submitted_in_window"] == 0)):
            raise ValueError("first submission contradicts in-window count")
        last_scheduled = row["start_ns"] + (row["offered"] - 1) * 1_000_000_000 // row["rate_rps"]
        if maximum + 0.0005 < max(0, row["last_submit_ns"] - last_scheduled) / 1000:
            raise ValueError("submission extrema contradict maximum lateness")


def load_quality(result: dict[str, Any], max_submit_late_us: float) -> dict[str, Any]:
    if type(max_submit_late_us) not in (int, float) or not math.isfinite(max_submit_late_us) or max_submit_late_us < 0:
        raise ValueError("max-submit-late-us must be finite and non-negative")
    from run_rpc_capacity import validate_open_phase

    rows = result.get("generators", [result["client"]])
    case = result["case"]
    if not rows or len(rows) != case.get("generators", 1):
        raise ValueError("generator count does not match case")
    if sum(row["rate_rps"] for row in rows) != case["rate_rps"]:
        raise ValueError("generator rates do not match case")
    if "requests" in case and sum(row["offered"] for row in rows) != case["requests"]:
        raise ValueError("generator requests do not match case")
    if len({row["pid"] for row in rows}) != len(rows):
        raise ValueError("duplicate generator identity")
    expected_window = case.get("generator_window", case.get("window"))
    if expected_window is not None and any(row["window"] != expected_window for row in rows):
        raise ValueError("generator slots do not match case")
    reports = []
    for row in rows:
        validate_open_phase(row, row["offered"], row["rate_rps"], row["window"])
        for key in ("start_ns", "schedule_horizon_ns", "end_ns"):
            if type(row[key]) is not int or row[key] < 0:
                raise ValueError("invalid window timestamp")
        if row["schedule_horizon_ns"] != row["start_ns"] + row["offered"] * 1_000_000_000 // row["rate_rps"]:
            raise ValueError("inconsistent schedule horizon")
        report = {
            "pid": row["pid"], "planned": row["offered"],
            "submission_attempts": row["attempted"],
            "not_accepted": row["offered"] - row["accepted"],
            "accepted": row["accepted"], "completed": row["completed"],
            "planned_rps": row["rate_rps"],
            "scheduled_start_ns": row["start_ns"],
            "schedule_horizon_ns": row["schedule_horizon_ns"],
            "drain_end_ns": row["end_ns"],
            "errors": {key: row[key] for key in (
                "scheduler_dropped", "submit_again", "submit_errors", "deadlines",
                "rpc_errors", "unavailable", "resource_exhausted", "invalid_responses")},
            "ok_p99_us": row["ok_p99_us"],
            "accepted_p99_us": row["accepted_p99_us"],
        }
        reasons = []
        if "submission_schema" in row and type(row["submission_schema"]) is not int:
            raise ValueError("invalid submission schema")
        if row.get("submission_schema") != 1:
            report.update(status="unknown", reasons=["missing_submission_evidence"])
        else:
            p99, maximum = row["submit_late_p99_us"], row["submit_late_max_us"]
            if row["scheduler_dropped"]:
                reasons.append("scheduler_dropped")
            if row["accepted"] != row["offered"]:
                reasons.append("not_all_planned_accepted")
            if row["submitted_in_window"] != row["offered"]:
                reasons.append("not_all_submitted_in_own_arrival_window")
            if maximum > max_submit_late_us:
                reasons.append("submission_lateness_exceeds_explicit_budget")
            report.update(
                status="fail" if reasons else "pass", reasons=reasons,
                first_submit_ns=row["first_submit_ns"], last_submit_ns=row["last_submit_ns"],
                submitted_in_own_window=row["submitted_in_window"],
                accepted_by_entry_in_own_window=row["accepted_in_window"],
                completed_in_own_window=row["completed_in_window"],
                submit_late_p99_us=p99, submit_late_max_us=maximum,
                submitted_in_own_window_rps=row["submitted_in_window"] / row["arrival_window_s"],
            )
        reports.append(report)
    start = min(row["start_ns"] for row in rows)
    end = max(row["end_ns"] for row in rows)
    duration = (end - start) / 1e9
    if duration <= 0:
        raise ValueError("empty common observation window")
    statuses = {report["status"] for report in reports}
    interval_ns = max(1, 1_000_000_000 // result["case"]["rate_rps"])
    offsets_valid = all(row["start_ns"] - rows[0]["start_ns"] == index * interval_ns
                        for index, row in enumerate(rows))
    if not offsets_valid:
        statuses.add("fail")
    return {
        "schema": 1,
        "scope": "local_submission_proxy_only",
        "status": "fail" if "fail" in statuses else "unknown" if "unknown" in statuses else "pass",
        "max_submit_late_us": max_submit_late_us,
        "scheduled_start_offsets_match_runner": offsets_valid,
        "generators": reports,
        "scheduled_start_spread_us": (max(row["start_ns"] for row in rows) - start) / 1000,
        "common_observation_start_ns": start, "common_observation_end_ns": end,
        "common_observation_s": duration,
        "common_window_rates_rps": {
            key: sum(row[key] for row in rows) / duration
            for key in ("attempted", "accepted", "completed", "ok")
        },
        "arrival_window_counts_are_per_generator": True,
        "pooled_percentiles_available": False,
        "server_arrival_fidelity": "unknown",
        "server_capacity": "not_established",
    }
