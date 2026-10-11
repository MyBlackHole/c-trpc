#!/usr/bin/env python3
"""检查已提交 Git 快照中的 C 系列修改行，不写回文件。"""
import bisect
import difflib
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

from tool_i18n import parser_for

VERSION = "18.1.8"
EXTENSIONS = {".c", ".h", ".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx"}


def run(*args, data=None):
    return subprocess.run(args, input=data, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=True).stdout


def git(*args):
    return run("git", *args)


def commit(ref):
    # 先解析为提交 SHA，避免把传入引用解释成 Git 选项。
    # 历史缺失或浅克隆时必须失败，不能静默退回 HEAD。
    return git("rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode().strip()


def changes(base, head):
    fields = git("diff", "--name-status", "-z", "--find-renames", base, head, "--").split(b"\0")
    index = 0
    while index < len(fields) - 1:
        status = fields[index].decode("ascii")
        old = new = os.fsdecode(fields[index + 1])
        index += 2
        if status.startswith(("R", "C")):
            new = os.fsdecode(fields[index])
            index += 1
        if status != "D" and Path(new).suffix.lower() in EXTENSIONS:
            yield status, old, new


def changed_lines(old, new):
    result = set()
    for tag, _, _, start, end in difflib.SequenceMatcher(
            None, old.splitlines(keepends=True), new.splitlines(keepends=True),
            autojunk=False).get_opcodes():
        if tag in ("insert", "replace"):
            result.update(range(start + 1, end + 1))
    return result


def affected_lines(data, offset, length):
    # Clang 偏移量以字节计，不是 Unicode 字符索引。
    # 跨行边界的空白也属于后一行的缩进。
    starts = [0] + [m.end() for m in re.finditer(b"\n", data)]
    first = bisect.bisect_right(starts, offset)
    last = bisect.bisect_right(starts, offset + length)
    return range(first, last + 1)


def check(base_ref, head_ref, formatter, _=lambda message: message):
    version = run(formatter, "--version").decode().strip()
    if not re.search(r"\bversion " + re.escape(VERSION) + r"(?:\s|$)", version):
        raise ValueError(_("clang-format {required} is required; found {actual}").format(required=VERSION, actual=version))
    base, head = commit(base_ref), commit(head_ref)
    if commit("HEAD") != head:
        raise ValueError(_("Check out the --head commit first to use its .clang-format"))
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError(_("Tracked files must be clean; commit changes before checking"))
    base = git("merge-base", base, head).decode().strip()
    # 即使仅修改文档或差异为空，也要校验格式配置。
    run(formatter, "--style=file", "--fallback-style=none", "--dump-config")
    failures = 0
    checked = 0
    for status, old_path, path in changes(base, head):
        mode = git("--literal-pathspecs", "ls-tree", head, "--", path).split(b" ", 1)[0]
        if mode not in (b"100644", b"100755"):
            raise ValueError(_("Source must be a regular file: {path!r}").format(path=path))
        new = git("show", f"{head}:{path}")
        new_source = (status == "A" or status.startswith("C")
                      or Path(old_path).suffix.lower() not in EXTENSIONS)
        old = b"" if new_source else git("show", f"{base}:{old_path}")
        lines = changed_lines(old, new)
        if not lines:
            continue
        checked += 1
        xml = ET.fromstring(run(formatter, "--style=file", "--fallback-style=none",
                                "--assume-filename=" + str(Path(path).absolute()),
                                "--output-replacements-xml", data=new))
        if xml.attrib.get("incomplete_format") == "true":
            raise ValueError(_("Formatting is incomplete: {path!r}").format(path=path))
        bad = set()
        for replacement in xml.findall("replacement"):
            bad.update(lines.intersection(affected_lines(
                new, int(replacement.attrib["offset"]), int(replacement.attrib["length"]))))
        if bad:
            failures += 1
            print(_("{path!r}: changed lines need formatting: {lines}").format(path=path, lines=','.join(map(str, sorted(bad)))))
    print(_("clang-format {version}: checked {checked} changed source files, {failures} failed; merge base {base}").format(version=VERSION, checked=checked, failures=failures, base=base))
    return bool(failures)


def main():
    parser, _ = parser_for("Check changed C-family lines in committed Git snapshots without modifying files.")
    parser.add_argument("--base", required=True, help=_("PR target branch SHA/ref; compare from the merge base"))
    parser.add_argument("--head", default="HEAD", help=_("Commit to check (default: HEAD)"))
    parser.add_argument("--clang-format", default="clang-format", help=_("Path to the clang-format executable"))
    args = parser.parse_args()
    try:
        os.chdir(git("rev-parse", "--show-toplevel").decode().strip())
        return int(check(args.base, args.head, args.clang_format, _))
    except (ValueError, OSError, subprocess.CalledProcessError, ET.ParseError) as error:
        print(_("Source format check failed: {error}").format(error=error), file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr.decode(errors="replace"), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
