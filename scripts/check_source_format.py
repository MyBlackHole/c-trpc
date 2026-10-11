#!/usr/bin/env python3
"""Check changed C-family lines in committed Git snapshots, without rewriting files."""
import argparse
import bisect
import difflib
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

VERSION = "18.1.8"
EXTENSIONS = {".c", ".h", ".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx"}


def run(*args, data=None):
    return subprocess.run(args, input=data, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=True).stdout


def git(*args):
    return run("git", *args)


def commit(ref):
    # Resolve before use in other Git commands; never interpret a supplied ref
    # as an option or silently fall back to HEAD on shallow/missing history.
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
    # Clang offsets are bytes, not Unicode character indexes. Whitespace at an
    # inter-line boundary also belongs to the following line (its indentation).
    starts = [0] + [m.end() for m in re.finditer(b"\n", data)]
    first = bisect.bisect_right(starts, offset)
    last = bisect.bisect_right(starts, offset + length)
    return range(first, last + 1)


def check(base_ref, head_ref, formatter):
    version = run(formatter, "--version").decode().strip()
    if not re.search(r"\bversion " + re.escape(VERSION) + r"(?:\s|$)", version):
        raise ValueError(f"Expected clang-format {VERSION}; got {version}")
    base, head = commit(base_ref), commit(head_ref)
    if commit("HEAD") != head:
        raise ValueError("Check out --head first so its .clang-format files are used")
    if git("status", "--porcelain", "--untracked-files=no"):
        raise ValueError("Tracked working tree must be clean; commit changes before checking")
    base = git("merge-base", base, head).decode().strip()
    # Validate configuration even for documentation-only or empty diffs.
    run(formatter, "--style=file", "--fallback-style=none", "--dump-config")
    failures = 0
    checked = 0
    for status, old_path, path in changes(base, head):
        mode = git("--literal-pathspecs", "ls-tree", head, "--", path).split(b" ", 1)[0]
        if mode not in (b"100644", b"100755"):
            raise ValueError(f"Source must be a regular file: {path!r}")
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
            raise ValueError(f"Incomplete formatting: {path!r}")
        bad = set()
        for replacement in xml.findall("replacement"):
            bad.update(lines.intersection(affected_lines(
                new, int(replacement.attrib["offset"]), int(replacement.attrib["length"]))))
        if bad:
            failures += 1
            print(f"{path!r}: format changed lines {','.join(map(str, sorted(bad)))}")
    print(f"clang-format {VERSION}: {checked} changed source file(s), {failures} failing; merge-base {base}")
    return bool(failures)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="PR base SHA/ref; compared from merge-base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--clang-format", default="clang-format")
    args = parser.parse_args()
    try:
        os.chdir(git("rev-parse", "--show-toplevel").decode().strip())
        return int(check(args.base, args.head, args.clang_format))
    except (ValueError, OSError, subprocess.CalledProcessError, ET.ParseError) as error:
        print(f"Source format check failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr.decode(errors="replace"), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
