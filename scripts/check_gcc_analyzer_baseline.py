#!/usr/bin/env python3
"""GCC -fanalyzer 全量运行后的精确诊断回归门禁；不关闭或隐瞒检查项。"""
from collections import Counter
import json
from pathlib import Path
import re
import sys

from tool_i18n import parser_for

ROOT = Path(__file__).resolve().parent.parent
FIELDS = ("unit", "file", "line", "column", "check", "message")
PROFILE = {"mode": "release", "toolchain": "gcc", "plat": "linux",
           "arch": "x86_64", "crc32c_portable": False}
LINE = re.compile(
    r"^(?P<file>.+?):(?P<line>[1-9]\d*):(?P<column>[1-9]\d*): "
    r"warning: (?P<message>.+) \[-W(?P<check>analyzer-[A-Za-z0-9-]+)\]$")
MAJOR = re.compile(r"^(\d+)(?:\.\d+)*$")
SHA1 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def relative_path(value):
    if not isinstance(value, str) or not value:
        raise ValueError("Invalid diagnostic source path")
    path = Path(value)
    path = (path if path.is_absolute() else ROOT / path).resolve()
    if not path.is_relative_to(ROOT.resolve()):
        raise ValueError("Diagnostic path is outside the repository")
    return path.relative_to(ROOT.resolve()).as_posix()


def canonical(item):
    if not isinstance(item, dict) or set(item) != set(FIELDS):
        raise ValueError("Invalid GCC warning record")
    unit = relative_path(item["unit"])
    file = relative_path(item["file"])
    if not unit.startswith("src/") or not unit.endswith(".c"):
        raise ValueError("Non-production GCC warning unit")
    for name in ("line", "column"):
        if (not isinstance(item[name], int) or isinstance(item[name], bool)
                or item[name] <= 0):
            raise ValueError("Invalid GCC warning location")
    check = item["check"]
    if not isinstance(check, str) or not re.fullmatch(r"analyzer-[A-Za-z0-9-]+", check):
        raise ValueError("Invalid GCC analyzer check name")
    if not isinstance(item["message"], str) or not item["message"].strip():
        raise ValueError("Invalid GCC diagnostic message")
    return dict(item, unit=unit, file=file)


def key(item):
    return tuple(item[name] for name in FIELDS)


def read_baseline(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(data, dict) or data.get("schema") != 1
            or data.get("gcc_major") != 11 or data.get("profile") != PROFILE
            or data.get("review_state") != "unverified"):
        raise ValueError("Incorrect GCC analyzer baseline configuration")
    if not isinstance(data.get("warnings"), list):
        raise ValueError("Missing GCC diagnostic baseline")
    expected = []
    for item in data["warnings"]:
        record = canonical(item)
        if record != item or not item["file"].startswith(("src/", "include/")):
            raise ValueError("Noncanonical GCC baseline diagnostic")
        expected.append(record)
    if len({key(item) for item in expected}) != len(expected):
        raise ValueError("Duplicated GCC baseline diagnostic")
    return data, expected


def parse_log(unit, text):
    warnings = []
    for line in text.splitlines():
        if "warning:" not in line and "error:" not in line:
            continue
        match = LINE.fullmatch(line)
        if not match:
            raise ValueError("Unrecognized GCC diagnostic: " + line[:280])
        fields = match.groupdict()
        warnings.append(canonical({
            "unit": unit, "file": fields["file"],
            "line": int(fields["line"]), "column": int(fields["column"]),
            "check": fields["check"], "message": fields["message"]}))
    return warnings


def compare(report_dir, baseline_file):
    baseline, expected = read_baseline(baseline_file)
    report = json.loads((report_dir / "summary.json").read_text(encoding="utf-8"))
    if (not isinstance(report, dict) or report.get("schema") != 1
            or report.get("analyzer") != "gcc -fanalyzer"
            or report.get("profile") != PROFILE or report.get("strict") is not False):
        raise ValueError("Invalid GCC static analysis report")
    major = MAJOR.fullmatch(str(report.get("gcc_version", "")))
    if not major or int(major.group(1)) != baseline["gcc_major"]:
        raise ValueError("Incorrect GCC analyzer tool version")
    identity = report.get("identity", {})
    if (not isinstance(identity, dict) or identity.get("target") != "trcore"
            or identity.get("config", {}).get("toolchain") != "gcc"
            or not SHA1.fullmatch(str(identity.get("git_commit", "")))
            or not SHA256.fullmatch(str(report.get("database_sha256", "")))):
        raise ValueError("Invalid GCC production compilation database identity")
    controls = report.get("controls", {})
    if (controls.get("cleanup", {}).get("exit_code") != 0
            or controls.get("cleanup", {}).get("analyzer_diagnostics") != 0
            or controls.get("leak", {}).get("exit_code") == 0
            or controls.get("leak", {}).get("analyzer_diagnostics", 0) < 1):
        raise ValueError("GCC analyzer positive/negative controls did not pass")

    results = report.get("results")
    if (not isinstance(results, list) or not results
            or len(results) != report.get("production_units")
            or report.get("failed_sources") != []):
        raise ValueError("Incomplete GCC production analysis")
    seen = set()
    observed = []
    for item in results:
        if not isinstance(item, dict):
            raise ValueError("Invalid GCC analysis unit")
        unit = relative_path(item.get("source"))
        if unit in seen or not unit.startswith("src/") or not unit.endswith(".c"):
            raise ValueError("Duplicate or non-production GCC unit")
        seen.add(unit)
        if item.get("returncode") != 0 or item.get("log") != "logs/" + unit + ".log":
            raise ValueError("Missing or failed GCC analysis unit")
        path = report_dir / item["log"]
        if path.is_symlink() or not path.is_file():
            raise ValueError("Missing GCC diagnostic log")
        warnings = parse_log(unit, path.read_text(encoding="utf-8"))
        if len(warnings) != item.get("analyzer_diagnostics"):
            raise ValueError("GCC diagnostic log disagrees with analysis summary")
        observed.extend(warnings)
    if len(observed) != report.get("analyzer_diagnostics"):
        raise ValueError("GCC warning count does not match original logs")

    actual = Counter(key(x) for x in observed)
    prior = Counter(key(x) for x in expected)
    added = actual - prior
    removed = prior - actual
    differences = {
        "schema": 1, "policy": "exact-tracked-gcc-warning-set",
        "review_state": baseline["review_state"], "gcc_major": baseline["gcc_major"],
        "production_units": len(results),
        "observed_warnings": len(observed), "baseline_warnings": len(expected),
        "unreviewed_warnings": [dict(zip(FIELDS, x)) for x in sorted(added.elements())],
        "missing_baseline_warnings": [dict(zip(FIELDS, x)) for x in sorted(removed.elements())]
    }
    (report_dir / "warning-gate.json").write_text(
        json.dumps(differences, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if added or removed:
        raise ValueError("GCC analyzer warning set changed: added={new}, removed={removed}"
                         .format(new=sum(added.values()), removed=sum(removed.values())))
    return differences


def main():
    parser, _ = parser_for("Compare GCC production analyzer findings with the exact unresolved ledger.")
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    args = parser.parse_args()
    try:
        outcome = compare(args.report_dir.resolve(), args.baseline.resolve())
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(_("GCC analyzer warning gate failed: {error}").format(error=error),
              file=sys.stderr)
        return 1
    print(_("Verified {units} GCC production units and {warnings} tracked warnings")
          .format(units=outcome["production_units"],
                  warnings=outcome["observed_warnings"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
