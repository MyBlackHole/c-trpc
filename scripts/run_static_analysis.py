#!/usr/bin/env python3
"""仅使用已校验的生产编译数据库执行 Clang 静态分析，并保留逐单元证据。"""
import json
from pathlib import Path
import re
import subprocess
import sys

import check_compile_commands as compdb
from tool_i18n import parser_for

REQUIRED_CHECKS = ("clang-analyzer-core.CallAndMessage", "clang-analyzer-unix.Malloc")
VERSION_PATTERN = re.compile(r"\bversion\s+(\d+)(?:\.|\b)", re.IGNORECASE)
WARNING_PATTERN = re.compile(r"^.+:\d+:\d+: warning:", re.MULTILINE)


def verified_sources(directory, xmake, expected):
    """拒绝过时数据库以及非生产、缺失、重复的编译命令。"""
    state = compdb.snapshot(xmake)
    compdb.check_config(state, expected)
    data = (directory / compdb.DATABASE).read_bytes()
    entries = json.loads(data)
    count = compdb.validate(entries, state)
    manifest = json.loads((directory / compdb.MANIFEST).read_bytes())
    compdb.check_manifest(manifest, data, state)

    root = compdb.ROOT.resolve()
    sources = sorted({compdb.source_path(entry) for entry in entries
                      if compdb.source_path(entry).is_relative_to(root / "src")})
    if len(sources) != count or not sources:
        raise ValueError("Production compilation units do not match validated coverage")
    return sources, manifest


def verify_analyzer(executable, version_major, config, source):
    """固定分析器主版本；确保实际启用 core/unix 检查而非空跑。"""
    version = subprocess.run([executable, "--version"], cwd=compdb.ROOT, check=True,
                             capture_output=True, text=True)
    match = VERSION_PATTERN.search(version.stdout + version.stderr)
    if not match or int(match.group(1)) != version_major:
        raise ValueError("Analyzer major version mismatch: " +
                         (version.stdout + version.stderr).strip())
    checks = subprocess.run([executable, "--list-checks", "--config-file=" + str(config),
                             str(source)], cwd=compdb.ROOT, check=True,
                            capture_output=True, text=True)
    active = {line.strip() for line in checks.stdout.splitlines()}
    if not set(REQUIRED_CHECKS).issubset(active):
        raise ValueError("Required clang-analyzer checks are inactive")
    return (version.stdout + version.stderr).strip(), sorted(active)


def analyze(sources, directory, report_dir, analyzer, strict, timeout, version, checks,
            manifest):
    report_dir.mkdir(parents=True, exist_ok=True)
    log_dir = report_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    results = []
    root = compdb.ROOT.resolve()
    for source in sources:
        relative = source.relative_to(root).as_posix()
        log = log_dir / (relative + ".log")
        log.parent.mkdir(parents=True, exist_ok=True)
        command = [analyzer, "--quiet", "-p=" + str(directory),
                   "--config-file=" + str(root / ".clang-tidy")]
        if strict:
            command.append("--warnings-as-errors=*")
        command.append(str(source))
        try:
            process = subprocess.run(command, cwd=root, capture_output=True, text=True,
                                     timeout=timeout, errors="replace")
            output = process.stdout + process.stderr
            status = process.returncode
        except subprocess.TimeoutExpired as error:
            output = (error.stdout or b"")
            if isinstance(output, bytes):
                output = output.decode(errors="replace")
            stderr = error.stderr or b""
            if isinstance(stderr, bytes):
                stderr = stderr.decode(errors="replace")
            output += stderr + "\nAnalysis timed out\n"
            status = 124
        log.write_text(output)
        results.append({"source": relative, "returncode": status,
                        "warnings": len(WARNING_PATTERN.findall(output)),
                        "log": log.relative_to(report_dir).as_posix()})

    failed = [r["source"] for r in results if r["returncode"] != 0]
    report = {"schema": 1, "analyzer": analyzer, "version": version,
              "checks": checks, "strict": strict,
              "identity": manifest["identity"],
              "database_sha256": manifest["database_sha256"],
              "expected_units": len(sources), "analyzed_units": len(results),
              "reported_warnings": sum(item["warnings"] for item in results),
              "failed_sources": failed, "results": results}
    (report_dir / "summary.json").write_text(json.dumps(report, indent=2,
                                                        ensure_ascii=False) + "\n")
    return report


def main():
    parser, _ = parser_for("Run Clang analysis on validated Xmake production compile commands.")
    parser.add_argument("--database-dir", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--xmake", default="xmake")
    parser.add_argument("--analyzer", default="clang-tidy-14")
    parser.add_argument("--expected-major", type=int, default=14)
    parser.add_argument("--mode", choices=("debug", "release", "asan", "tsan"), required=True)
    parser.add_argument("--toolchain", choices=("clang", "gcc"), required=True)
    parser.add_argument("--plat", default="linux")
    parser.add_argument("--arch", required=True)
    parser.add_argument("--crc32c-portable", choices=("y", "n"), required=True)
    parser.add_argument("--strict", action="store_true",
                        help=_("Fail on diagnostics as well as analyzer failures"))
    parser.add_argument("--timeout", type=int, default=90)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        directory = args.database_dir.resolve()
        report_dir = args.report_dir.resolve()
        sources, manifest = verified_sources(directory, args.xmake,
                                             {"mode": args.mode, "toolchain": args.toolchain,
                                              "plat": args.plat, "arch": args.arch,
                                              "crc32c_portable": args.crc32c_portable == "y"})
        version, checks = verify_analyzer(args.analyzer, args.expected_major,
                                          compdb.ROOT / ".clang-tidy", sources[0])
        report = analyze(sources, directory, report_dir, args.analyzer, args.strict,
                         args.timeout, version, checks, manifest)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError,
            subprocess.CalledProcessError) as error:
        print(_("Static analysis setup failed: {error}").format(error=error), file=sys.stderr)
        return 2
    print(_("Analyzed {count} production units, reported {warnings} warnings, failures: {failures}")
          .format(count=report["analyzed_units"], warnings=report["reported_warnings"],
                  failures=len(report["failed_sources"])))
    return 1 if report["failed_sources"] else 0


if __name__ == "__main__":
    sys.exit(main())
