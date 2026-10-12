#!/usr/bin/env python3
"""按完整位置与检查项核对静态分析告警；对新增、消失或无法解析的告警关闭门禁。"""
from collections import Counter
import json
from pathlib import Path
import re
import sys

from tool_i18n import parser_for

ROOT = Path(__file__).resolve().parent.parent
LINE = re.compile(
    r"(?P<file>.+?):(?P<line>[1-9][0-9]*):(?P<column>[1-9][0-9]*): "
    r"warning: (?P<message>.+) \[(?P<check>[A-Za-z0-9_.-]+)\]")
VERSION = re.compile(r"\bversion\s+([0-9]+)(?:\.|\b)", re.IGNORECASE)
SHA256 = re.compile(r"[0-9a-f]{64}")
SHA1 = re.compile(r"[0-9a-f]{40}")
CHECKS = ("clang-analyzer-core.CallAndMessage", "clang-analyzer-unix.Malloc")
FIELDS = ("source", "file", "line", "column", "check", "message")
PROFILE = {"mode": "release", "toolchain": "clang", "plat": "linux",
           "arch": "x86_64", "crc32c_portable": False}


def source_name(value):
    if not isinstance(value, str) or not value.startswith("src/") or not value.endswith(".c"):
        raise ValueError("Analysis report contains a non-production source")
    path = (ROOT / value).resolve()
    if not path.is_relative_to(ROOT.resolve() / "src") or path.relative_to(ROOT.resolve()).as_posix() != value:
        raise ValueError("Invalid production source path")
    return value


def normalize_file(value):
    if not isinstance(value, str):
        raise ValueError("Invalid warning location")
    path = Path(value)
    path = (path if path.is_absolute() else ROOT / path).resolve()
    if path.is_relative_to(ROOT.resolve()):
        return path.relative_to(ROOT.resolve()).as_posix()
    return path.as_posix()


def record(source, file, line, column, check, message):
    if not isinstance(line, int) or isinstance(line, bool) or line < 1:
        raise ValueError("Invalid warning line")
    if not isinstance(column, int) or isinstance(column, bool) or column < 1:
        raise ValueError("Invalid warning column")
    if not isinstance(check, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+", check):
        raise ValueError("Invalid warning check")
    if not isinstance(message, str) or not message.strip():
        raise ValueError("Invalid warning message")
    return {"source": source_name(source), "file": normalize_file(file),
            "line": line, "column": column, "check": check, "message": message}


def key(diagnostic):
    return tuple(diagnostic[field] for field in FIELDS)


def parse_log(source, output):
    result = []
    for line in output.splitlines():
        # 未能识别的新诊断不能凭统计器的宽松匹配绕过阻断。
        if "warning:" not in line:
            continue
        match = LINE.fullmatch(line)
        if not match:
            raise ValueError("Unparsed analyzer warning in " + source + ": " + line[:300])
        fields = match.groupdict()
        result.append(record(source, fields["file"], int(fields["line"]),
                             int(fields["column"]), fields["check"], fields["message"]))
    return result


def load_baseline(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema") != 1 or data.get("analyzer_major") != 14:
        raise ValueError("Invalid baseline schema or analyzer version")
    if data.get("profile") != PROFILE or data.get("review_state") != "unverified":
        raise ValueError("Baseline profile/review state mismatch")
    entries = data.get("warnings")
    if not isinstance(entries, list):
        raise ValueError("Baseline warnings must be an array")
    warnings = []
    for item in entries:
        if not isinstance(item, dict) or set(item) != set(FIELDS):
            raise ValueError("Malformed baseline warning")
        # 基线必须是仓库相对位置，禁止机器绝对路径和含糊匹配。
        if not isinstance(item["file"], str) or not item["file"].startswith(("src/", "include/")):
            raise ValueError("Baseline location must be repository relative")
        converted = record(**item)
        if converted != item:
            raise ValueError("Baseline location is not canonical")
        warnings.append(converted)
    if len({key(item) for item in warnings}) != len(warnings):
        raise ValueError("Duplicate warning in baseline")
    return data, warnings


def compare(report_dir, baseline_file):
    base, permitted = load_baseline(baseline_file)
    report = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
    if not isinstance(report, dict) or report.get("schema") != 1 or report.get("strict") is not False:
        raise ValueError("Invalid static analysis report")
    version = VERSION.search(str(report.get("version", "")))
    if (not version or int(version.group(1)) != base["analyzer_major"]
            or Path(str(report.get("analyzer", ""))).name != "clang-tidy-14"):
        raise ValueError("Static analysis was not performed by pinned clang-tidy-14")
    if not set(CHECKS).issubset(set(report.get("checks", []))):
        raise ValueError("Required analyzer checks were not enabled")
    identity = report.get("identity", {})
    if (not isinstance(identity, dict) or identity.get("target") != "trcore"
            or not SHA1.fullmatch(str(identity.get("git_commit", "")))
            or not SHA256.fullmatch(str(report.get("database_sha256", "")))):
        raise ValueError("Invalid production build identity")
    config = identity.get("config", {})
    if not isinstance(config, dict) or any(config.get(name) != value
                                            for name, value in PROFILE.items()):
        raise ValueError("Analysis profile differs from reviewed baseline")
    results = report.get("results")
    if (not isinstance(results, list) or not results
            or report.get("analyzed_units") != len(results)
            or report.get("expected_units") != len(results)
            or report.get("failed_sources") != []):
        raise ValueError("Incomplete production static analysis report")

    observed = []
    seen = set()
    for unit in results:
        if not isinstance(unit, dict):
            raise ValueError("Malformed analysis unit")
        source = source_name(unit.get("source"))
        if source in seen or unit.get("returncode") != 0:
            raise ValueError("Duplicate or failed analysis unit: " + source)
        seen.add(source)
        expected_log = "logs/" + source + ".log"
        if unit.get("log") != expected_log:
            raise ValueError("Unexpected analysis log location: " + source)
        logfile = report_dir / expected_log
        if logfile.is_symlink() or not logfile.is_file():
            raise ValueError("Missing or unsafe analysis log: " + source)
        diagnostics = parse_log(source, logfile.read_text(encoding="utf-8"))
        if len(diagnostics) != unit.get("warnings"):
            raise ValueError("Warning count does not agree with original analyzer log: " + source)
        observed.extend(diagnostics)
    if len(observed) != report.get("reported_warnings"):
        raise ValueError("Summary warning total disagrees with original analyzer logs")

    actual = Counter(key(item) for item in observed)
    expected = Counter(key(item) for item in permitted)
    # 按出现次数比较，不能靠相同文件/行/检查项吞掉第二条不同告警。
    added, removed = actual - expected, expected - actual
    evidence = {"schema": 1, "policy": "exact-reviewed-warning-set",
                "analyzer_major": base["analyzer_major"], "profile": base["profile"],
                "review_state": base["review_state"],
                "production_units": len(results), "observed_warnings": len(observed),
                "baseline_warnings": len(permitted),
                "unreviewed_warnings": [dict(zip(FIELDS, value)) for value in sorted(added.elements())],
                "missing_baseline_warnings": [dict(zip(FIELDS, value)) for value in sorted(removed.elements())]}
    (report_dir / "warning-gate.json").write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8")
    if added or removed:
        raise ValueError("Static analysis warning set changed: new={new}, missing={missing}"
                         .format(new=sum(added.values()), missing=sum(removed.values())))
    return evidence


def main():
    parser, _ = parser_for("Compare actual production static analysis warnings with an exact review ledger.")
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = compare(args.report_dir.resolve(), args.baseline.resolve())
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(_("Static analysis warning regression gate failed: {error}")
              .format(error=error), file=sys.stderr)
        return 1
    print(_("Verified {units} production units and {warnings} exact reviewed warning locations")
          .format(units=result["production_units"], warnings=result["observed_warnings"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
