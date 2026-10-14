#!/usr/bin/env python3
"""使用经校验的 Xmake 生产编译命令，运行真正生成对象文件的 GCC 静态分析。"""
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

import check_compile_commands as compdb
from tool_i18n import parser_for

VERSION = re.compile(r"^(\d+)(?:\.\d+)*$")
ANALYZER_DIAGNOSTIC = re.compile(
    r"(?m)^.*:\d+(?::\d+)?: (?:warning|error): .+\[-W(?:error=)?analyzer-[^\]]+\]")

CLEAN_CONTROL = """#include <stdlib.h>
static void release(void *address)
{
    void **value = address;
    free(*value);
    *value = NULL;
}
int cleanup_control(int stop)
{
    void *value __attribute__((cleanup(release))) = malloc(32U);
    if (!value) return -2;
    if (stop) return -1;
    return 0;
}
"""
LEAK_CONTROL = """#include <stdlib.h>
int leak_control(int stop)
{
    void *value = malloc(32U);
    if (!value) return -2;
    if (stop) return -1;
    free(value);
    return 0;
}
"""


def verified_entries(directory, xmake):
    state = compdb.snapshot(xmake)
    compdb.check_config(
        state, {"mode": "release", "toolchain": "gcc", "plat": "linux",
                "arch": "x86_64", "crc32c_portable": False})
    raw = (directory / compdb.DATABASE).read_bytes()
    entries = json.loads(raw)
    count = compdb.validate(entries, state)
    manifest = json.loads((directory / compdb.MANIFEST).read_bytes())
    compdb.check_manifest(manifest, raw, state)

    root = compdb.ROOT.resolve()
    production = {}
    for item in entries:
        source = compdb.source_path(item)
        if not source.is_relative_to(root / "src"):
            continue
        relative = source.relative_to(root).as_posix()
        if relative in production:
            raise ValueError("Duplicate production compilation unit")
        production[relative] = item["arguments"]
    if not production or len(production) != count:
        raise ValueError("Validated source count disagrees with GCC production units")
    return sorted(production.items()), manifest


def check_gcc(compiler, expected_major):
    version = subprocess.run([compiler, "-dumpfullversion", "-dumpversion"],
                             cwd=compdb.ROOT, check=True, capture_output=True, text=True)
    value = version.stdout.strip()
    if not VERSION.fullmatch(value) or int(value.split(".", 1)[0]) != expected_major:
        raise ValueError("GCC analyzer compiler version mismatch: " + value)
    return value


def analysis_command(original, source, output):
    """不重建生产选项，仅换输出位置并附加 GCC 分析器；必须实际执行 -c。"""
    if (not isinstance(original, list) or not original or not original[0]
            or original.count("-c") != 1 or original.count("-o") != 1
            or "-fsyntax-only" in original or "-fanalyzer" in original):
        raise ValueError("GCC production argv lacks one real compile and output")
    index = original.index("-o")
    if index + 1 >= len(original):
        raise ValueError("Missing GCC object path")
    if original[-1] != source or not original[index + 1].endswith(".o"):
        raise ValueError("Unexpected production GCC source or object path")
    args = list(original)
    args[index + 1] = str(output)
    # 保持原始 -Werror/-O2、平台选项、宏、源文件及包含路径不变。
    args.extend(("-fanalyzer", "-fdiagnostics-color=never"))
    return args


def verify_controls(compiler, report_dir, timeout):
    """检测自动清理模型，并证明分析器的真实泄漏诊断未被跳过。"""
    controls = report_dir / "controls"
    controls.mkdir(parents=True, exist_ok=True)
    data = {}
    for name, contents, must_fail in (
            ("cleanup", CLEAN_CONTROL, False), ("leak", LEAK_CONTROL, True)):
        source = controls / (name + ".c")
        object_file = controls / (name + ".o")
        source.write_text(contents, encoding="utf-8")
        command = [compiler, "-c", "-O2", "-std=c11", "-Wall", "-Wextra",
                   "-Werror", "-pedantic", "-fanalyzer",
                   "-fdiagnostics-color=never", str(source), "-o", str(object_file)]
        result = subprocess.run(command, cwd=compdb.ROOT, timeout=timeout,
                                capture_output=True, text=True, errors="replace")
        output = result.stdout + result.stderr
        (controls / (name + ".log")).write_text(output, encoding="utf-8")
        detected = ANALYZER_DIAGNOSTIC.findall(output)
        if must_fail:
            if result.returncode == 0 or not any(
                    "analyzer-malloc-leak" in message for message in detected):
                raise ValueError("GCC negative control failed to detect an actual allocation leak")
        elif result.returncode != 0 or detected or not object_file.is_file():
            raise ValueError("GCC cleanup control failed: " + output[-800:])
        data[name] = {"exit_code": result.returncode,
                      "analyzer_diagnostics": len(detected)}
    return data


def analyze(units, compiler, directory, report_dir, timeout, version, manifest,
            controls):
    logs = report_dir / "logs"
    objects = report_dir / "objects"
    results = []
    for source, original in units:
        if original[0] != compiler:
            raise ValueError("Production compiler differs from validated GCC driver")
        object_file = objects / (source + ".o")
        log_file = logs / (source + ".log")
        object_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        command = analysis_command(original, source, object_file)
        try:
            process = subprocess.run(command, cwd=compdb.ROOT, timeout=timeout,
                                     capture_output=True, text=True, errors="replace")
            output = process.stdout + process.stderr
            status = process.returncode
        except subprocess.TimeoutExpired as error:
            out = error.stdout or b""
            err = error.stderr or b""
            output = (out.decode(errors="replace") if isinstance(out, bytes) else out)
            output += (err.decode(errors="replace") if isinstance(err, bytes) else err)
            output += "\nGCC static analysis timed out\n"
            status = 124
        log_file.write_text(output, encoding="utf-8")
        reports = len(ANALYZER_DIAGNOSTIC.findall(output))
        # 无对象文件即使进程返回 0 也不算完成编译；禁止退化为语法检查。
        if status == 0 and not object_file.is_file():
            status = 125
            with log_file.open("a", encoding="utf-8") as stream:
                stream.write("GCC did not emit the expected analysis object file\n")
        results.append({"source": source, "returncode": status,
                        "analyzer_diagnostics": reports,
                        "object": object_file.relative_to(report_dir).as_posix(),
                        "log": log_file.relative_to(report_dir).as_posix()})
    failed = [item["source"] for item in results if item["returncode"] != 0]
    report = {
        "schema": 1, "analyzer": "gcc -fanalyzer", "gcc_version": version,
        "strict": True, "production_units": len(units),
        "profile": {"mode": "release", "toolchain": "gcc", "plat": "linux",
                    "arch": "x86_64", "crc32c_portable": False},
        "identity": manifest["identity"],
        "database_sha256": manifest["database_sha256"],
        "controls": controls, "analyzer_diagnostics": sum(
            item["analyzer_diagnostics"] for item in results),
        "failed_sources": failed, "results": results,
    }
    (report_dir / "summary.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main():
    parser, _ = parser_for("Analyze actual Xmake production C objects with GCC -fanalyzer.")
    parser.add_argument("--database-dir", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--xmake", default="xmake")
    parser.add_argument("--expected-major", type=int, default=11)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    try:
        directory = args.database_dir.resolve()
        report_dir = args.report_dir.resolve()
        units, manifest = verified_entries(directory, args.xmake)
        compilers = {argv[0] for _, argv in units}
        if len(compilers) != 1:
            raise ValueError("Expected exactly one production GCC compiler")
        compiler = next(iter(compilers))
        version = check_gcc(compiler, args.expected_major)
        report_dir.mkdir(parents=True, exist_ok=True)
        controls = verify_controls(compiler, report_dir, args.timeout)
        report = analyze(units, compiler, directory, report_dir, args.timeout,
                         version, manifest, controls)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError,
            subprocess.TimeoutExpired) as error:
        print(_("GCC production analysis setup failed: {error}").format(error=error),
              file=sys.stderr)
        return 2
    print(_("GCC analyzed {count} production units: {diagnostics} analyzer diagnostics, {failures} failed")
          .format(count=report["production_units"],
                  diagnostics=report["analyzer_diagnostics"],
                  failures=len(report["failed_sources"])))
    return 1 if report["failed_sources"] or report["analyzer_diagnostics"] else 0


if __name__ == "__main__":
    sys.exit(main())
