#!/usr/bin/env python3
"""生成 RPC V1 退役清单；源码扫描是线索，nm 对象引用是链接事实。"""
import argparse
import json
from pathlib import Path
import re
import subprocess

# 不将 tr_rpc_v2_*、tr_rpc_record_* 或通用 tr_rpc_semantic_* 误当 V1。
LEGACY = re.compile(
    r"\b(?:tr_client_legacy_[A-Za-z0-9_]*|"
    r"tr_client_create_with_tuning|tr_client_get_rpc_stats|"
    r"tr_server_create_with_tuning|tr_server_legacy_[A-Za-z0-9_]*|"
    r"tr_runtime_(?:shard_rpc_executor|rpc_executor_config|"
    r"legacy_rpc_execution_[A-Za-z0-9_]*)|"
    r"tr_rpc_(?:endpoint(?:_|(?![A-Za-z0-9_]))|"
    r"executor_group(?:_|(?![A-Za-z0-9_]))|wire_|"
    r"call_(?!options\b)|message_|"
    r"raw_|register_|unary_|in_worker_context)[A-Za-z0-9_]*)\b"
)
SERVER_FACADE_RETIREMENT = re.compile(
    r"^(?:tr_server_create_with_tuning|tr_server_legacy_[A-Za-z0-9_]+)$"
)
STRIP = re.compile(
    r"/\*[\s\S]*?\*/|//[^\n]*|"
    r'"(?:\\.|[^"\\])*"|'
    r"'(?:\\.|[^'\\])*'",
    re.MULTILINE,
)
SEARCH_ROOTS = ("src", "include", "tests", "bench", "examples")
EXTENSIONS = {".c", ".h", ".cc", ".cpp", ".hpp"}


def remove_comments_and_literals(text):
    """保留原文本的换行位置，避免注释/字符串变成虚假调用点。"""
    return STRIP.sub(
        lambda match: re.sub(r"[^\n]", " ", match.group(0)), text
    )


def role(path):
    if path.startswith(("tests/", "bench/", "examples/")):
        return path.split("/", 1)[0]
    if path.startswith("include/"):
        return "public-header"
    if path.startswith(("src/rpc/rpc.c", "src/rpc/rpc_wire.c")):
        return "v1-core"
    if path.startswith("src/runtime/"):
        return "runtime"
    if path in ("src/client.c", "src/server.c"):
        return "facade"
    return "production-other"


def collect_code(root):
    symbols = {}
    files = 0
    for dirname in SEARCH_ROOTS:
        directory = root / dirname
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*")):
            if not path.is_file() or path.suffix not in EXTENSIONS:
                continue
            files += 1
            relative = path.relative_to(root).as_posix()
            content = remove_comments_and_literals(
                path.read_text(encoding="utf-8")
            )
            for number, line in enumerate(content.splitlines(), start=1):
                for match in LEGACY.finditer(line):
                    name = match.group(0)
                    per_file = symbols.setdefault(name, {}).setdefault(
                        relative, []
                    )
                    if not per_file or per_file[-1] != number:
                        per_file.append(number)
    return files, symbols


def parse_nm(output):
    """nm -A -g --defined-only/-u: 每个生产对象仍可单独追溯。"""
    found = {}
    for line in output.splitlines():
        match = re.match(
            r"^(.*):\s*(?:[0-9A-Fa-f]+\s+)?"
            r"([A-Za-z])\s+([A-Za-z_]\w*)\s*$",
            line,
        )
        if not match:
            continue
        member = match.group(1)
        symbol = match.group(3)
        if LEGACY.fullmatch(symbol):
            found.setdefault(symbol, set()).add(member)
    return {key: sorted(value) for key, value in sorted(found.items())}


def nm_inventory(archive):
    if not archive.is_file():
        raise FileNotFoundError("静态库不存在：" + str(archive))
    result = {}
    for key, flags in (
        ("definitions", ["--defined-only"]),
        ("undefined", ["--undefined-only"]),
    ):
        run = subprocess.run(
            ["nm", "-A", "-g", *flags, str(archive)],
            capture_output=True, text=True, check=True,
        )
        result[key] = parse_nm(run.stdout)
    return result


def denied_symbols(symbols, deny):
    """返回显式退役列表中仍被源码或归档引用的名字。"""
    return sorted(set(deny).intersection(symbols))


def audit(root, archive=None):
    files, sites = collect_code(root)
    build = (root / "xmake.lua").read_text(encoding="utf-8")
    build_uses = {}
    for number, line in enumerate(build.splitlines(), start=1):
        for match in LEGACY.finditer(line):
            build_uses.setdefault(match.group(0), []).append(number)
    nm = nm_inventory(archive) if archive else {
        "definitions": {}, "undefined": {}
    }
    names = sorted(
        set(sites) | set(build_uses) |
        set(nm["definitions"]) | set(nm["undefined"])
    )
    records = {}
    for name in names:
        uses = sites.get(name, {})
        records[name] = {
            "source_uses": [
                {"path": p, "role": role(p), "lines": line_numbers}
                for p, line_numbers in sorted(uses.items())
            ],
            "xmake_lines": build_uses.get(name, []),
            "defined_in": nm["definitions"].get(name, []),
            "undefined_in": nm["undefined"].get(name, []),
        }
    return {
        "schema": 1,
        "scope": "all C/C++ src/include/tests/bench/examples; xmake; optional nm",
        "c_family_files_scanned": files,
        "library_checked": str(archive) if archive else None,
        "symbols": records,
        "limitations": [
            "源代码引用是剔除注释/字面量后的词法扫描，非 C AST 调用图",
            "nm 只涵盖静态库对象的全局符号，不能证明函数指针或静态符号不可达",
            "xmake 的名称与 --wrap 用途单独记账，测试依赖不能静默删除",
            "声明中出现的名字不是运行时调用，删除前必须逐项审查所有权与链接",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deny", nargs="*", default=[],
                        help="Reject any retired symbol still in source or archive")
    args = parser.parse_args()
    root = args.root.resolve()
    archive = args.archive.resolve() if args.archive else None
    result = audit(root, archive)
    remaining = denied_symbols(result["symbols"], args.deny)
    if remaining:
        parser.error("退役符号仍存在源码或链接对象中：" + ", ".join(remaining))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n", encoding="utf-8",
    )
    groups = {}
    external = []
    for name, data in result["symbols"].items():
        roles = {entry["role"] for entry in data["source_uses"]}
        for item in roles:
            groups[item] = groups.get(item, 0) + 1
        if roles & {"facade", "runtime", "production-other"}:
            external.append(name)
    print("RPC V1 inventory: scanned C-family files:",
          result["c_family_files_scanned"])
    print("RPC V1 candidate names:", len(result["symbols"]))
    print("By role:", json.dumps(groups, ensure_ascii=False, sort_keys=True))
    print("Non-V1 production source references:", len(external))
    print("Archive global definitions:",
          len([x for x in result["symbols"].values() if x["defined_in"]]))
    print("Archive object undefined references:",
          len([x for x in result["symbols"].values() if x["undefined_in"]]))
    print("Detailed complete list:", args.output)


if __name__ == "__main__":
    main()
