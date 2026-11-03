#!/usr/bin/env python3
"""区分编译器实际执行 cleanup 与静态分析器未建模 cleanup 的已知差异。"""
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from tool_i18n import parser_for


PROBE = """#include <stdlib.h>
static int cleaned_count;
static void free_cleaned(void *opaque)
{
    void **resource = (void **)opaque;
    if (*resource) {
        free(*resource);
        ++cleaned_count;
    }
}
int automatic_cleanup(int fail)
{
    void *resource __attribute__((cleanup(free_cleaned))) = malloc(32);
    if (!resource) return -2;
    if (fail) return -1;
    return 0;
}
int actual_leak(int fail)
{
    void *resource = malloc(32);
    if (!resource) return -2;
    if (fail) return -1;
    free(resource);
    return 0;
}
int main(void)
{
    if (automatic_cleanup(1) != -1 || cleaned_count != 1) return 1;
    if (automatic_cleanup(0) != 0 || cleaned_count != 2) return 2;
    return 0;
}
"""
WARNING = re.compile(
    r"^.+?:(?P<line>\d+):\d+: warning: Potential leak of memory pointed to by "
    r"'resource' \[unix.Malloc\]$", re.MULTILINE)


def verify(clang, major):
    with tempfile.TemporaryDirectory(prefix="trpc cleanup probe' ") as tmp:
        source = Path(tmp) / "cleanup.c"
        program = Path(tmp) / "cleanup"
        source.write_text(PROBE, encoding="utf-8")
        version = subprocess.run([clang, "--version"], check=True,
                                 capture_output=True, text=True)
        match = re.search(r"\bversion\s+(\d+)(?:\.|\b)", version.stdout)
        if not match or int(match.group(1)) != major:
            raise ValueError("Clang compiler major version mismatch")

        # 真实编译与执行验证两个返回出口均调用 cleanup，而非只观察静态告警。
        subprocess.run([clang, "-std=c11", "-O0", "-Wall", "-Wextra", "-Werror",
                        str(source), "-o", str(program)], check=True,
                       capture_output=True, text=True)
        subprocess.run([str(program)], check=True, capture_output=True, text=True)

        # 纯诊断对照：自动清理和真实泄漏应被该版本的 Malloc checker 同时报出。
        result = subprocess.run(
            [clang, "--analyze", "-Xanalyzer", "-analyzer-checker=unix.Malloc",
             str(source)], capture_output=True, text=True)
        if result.returncode != 0:
            raise ValueError("Clang analyzer probe invocation failed: " + result.stderr[:600])
        output = result.stdout + result.stderr
        locations = [int(item.group("line")) for item in WARNING.finditer(output)]
        lines = PROBE.splitlines()
        first = lines.index("int automatic_cleanup(int fail)") + 1
        second = lines.index("int actual_leak(int fail)") + 1
        third = lines.index("int main(void)") + 1
        if len(locations) != 2 or not any(first < line < second for line in locations) or not any(
                second < line < third for line in locations):
            raise ValueError("Cleanup model diagnostic contrast differs from pinned tool behavior: "
                             + output[-1200:])
        return {"clang": version.stdout.splitlines()[0],
                "compiled_cleanup_execution": "passed",
                "auto_cleanup_reported": True, "real_leak_reported": True,
                "warnings": len(locations)}


def main():
    parser, _ = parser_for("Probe Clang cleanup attribute execution and static analyzer limitations.")
    parser.add_argument("--clang", default="clang-14")
    parser.add_argument("--major", type=int, default=14)
    parser.add_argument("--report-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = verify(args.clang, args.major)
        args.report_dir.mkdir(parents=True, exist_ok=True)
        (args.report_dir / "cleanup-model-probe.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(_("Clang cleanup model probe failed: {error}").format(error=error), file=sys.stderr)
        return 1
    print(_("Verified compiler cleanup execution and analyzer model contrast"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
