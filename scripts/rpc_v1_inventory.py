#!/usr/bin/env python3
"""生成 RPC V1 退役清单；源码扫描是线索，nm 对象引用是链接事实。"""
import argparse
import json
from pathlib import Path
import re
import subprocess

V1_CORE_DENY = frozenset({
    # V1 Call 句柄、上下文、拦截器、回调及消息所有权类型。
    "tr_rpc_call_handle", "tr_rpc_context", "tr_rpc_interceptor_phase",
    "tr_rpc_interceptor_fn", "tr_rpc_interceptor",
    "tr_rpc_unary_response", "tr_rpc_unary_handler",
    "tr_rpc_unary_result_cb", "tr_rpc_message",
    "tr_rpc_message_disposition", "tr_rpc_call_event",
    "tr_rpc_message_cb", "tr_rpc_call_event_cb",
    "tr_rpc_call_callbacks", "tr_rpc_stream_open_handler",
    "tr_rpc_stream_message_handler", "tr_rpc_stream_half_close_handler",
    "tr_rpc_stream_writable_handler", "tr_rpc_stream_close_handler",
    "tr_rpc_stream_handlers", "TR_RPC_MESSAGE_PRIVATE_WORDS",
    "TR_RPC_CALL_PRIVATE_WORDS",
    "TR_RPC_INTERCEPTOR_CLIENT_PRE_CALL",
    "TR_RPC_INTERCEPTOR_SERVER_PRE_HANDLER",
    "TR_RPC_INTERCEPTOR_SERVER_POST_HANDLER",
    "TR_RPC_INTERCEPTOR_CLIENT_POST_CALL", "TR_RPC_MESSAGE_RELEASE",
    "TR_RPC_MESSAGE_TAKE_OWNERSHIP", "TR_RPC_CALL_EVENT_OPENED",
    "TR_RPC_CALL_EVENT_WRITABLE", "TR_RPC_CALL_EVENT_REMOTE_CLOSED",
    "TR_RPC_CALL_EVENT_FINISHED", "TR_RPC_CALL_EVENT_ERROR",

    # V1 Endpoint 和 Executor Group 类型/配置。
    "tr_rpc_endpoint", "tr_rpc_role", "tr_rpc_endpoint_config",
    "tr_rpc_endpoint_stats", "TR_RPC_CLIENT", "TR_RPC_SERVER",
    "tr_rpc_executor_group",

    # 旧 Wire 类型、字段常量和消息类型。
    "tr_rpc_wire_header", "tr_rpc_wire_type", "TR_RPC_WIRE_HEADER_SIZE",
    "TR_RPC_WIRE_VERSION", "TR_RPC_WIRE_OFF_MAGIC",
    "TR_RPC_WIRE_OFF_VERSION", "TR_RPC_WIRE_OFF_TYPE",
    "TR_RPC_WIRE_OFF_SERVICE_ID", "TR_RPC_WIRE_OFF_METHOD_ID",
    "TR_RPC_WIRE_OFF_CODEC_ID", "TR_RPC_WIRE_OFF_FLAGS",
    "TR_RPC_WIRE_OFF_STATUS", "TR_RPC_WIRE_OFF_PAYLOAD_LEN",
    "TR_RPC_WIRE_F_METADATA", "TR_RPC_WIRE_VALID_FLAGS",
    "TR_RPC_WIRE_METADATA_PREFIX_SIZE", "TR_RPC_WIRE_MAX_METADATA_BYTES",
    "TR_RPC_WIRE_REQUEST", "TR_RPC_WIRE_RESPONSE",
    "TR_RPC_WIRE_CANCEL", "TR_RPC_WIRE_STATUS",

    # V1 Call 续处理、注册、Endpoint 生命周期和 Wire 函数。
    "tr_rpc_call_send", "tr_rpc_call_send_buffer",
    "tr_rpc_call_close_send", "tr_rpc_call_finish", "tr_rpc_call_cancel",
    "tr_rpc_call_is_cancelled", "tr_rpc_call_set_metadata",
    "tr_rpc_call_get_peer_metadata", "tr_rpc_call_set_trailing_metadata",
    "tr_rpc_call_get_peer_trailing_metadata", "tr_rpc_call_get_context",
    "tr_rpc_call_start", "tr_rpc_call_start_ex",
    "tr_rpc_unary_call", "tr_rpc_unary_call_ex",
    "tr_rpc_register_method", "tr_rpc_register_stream_method",
    "tr_rpc_message_release", "tr_rpc_message_stream_internal",
    "tr_rpc_endpoint_create", "tr_rpc_endpoint_destroy",
    "tr_rpc_endpoint_create_with_executor_group",
    "tr_rpc_endpoint_destroy_with_stats", "tr_rpc_endpoint_get_stats",
    "tr_rpc_endpoint_get_semantic_stats",
    "tr_rpc_endpoint_detach_for_finalize",
    "tr_rpc_endpoint_arm_detached_finalizer",
    "tr_rpc_endpoint_release_detached_owner",
    "tr_rpc_endpoint_finalize_detached_with_stats",
    "tr_rpc_endpoint_flush", "tr_rpc_endpoint_flush_on_owner",
    "tr_rpc_deadline_heap_snapshot", "tr_rpc_executor_group_create",
    "tr_rpc_executor_group_can_start", "tr_rpc_executor_group_start",
    "tr_rpc_executor_group_stop", "tr_rpc_executor_group_destroy_checked",
    "tr_rpc_wire_encode", "tr_rpc_wire_decode", "tr_rpc_wire_decode_ex",
    "tr_rpc_in_worker_context",
})

EXACT_RETIREMENT_DENY = frozenset({
    # R11-R13 已移除的门面/Runtime V1 接口和构建 wrapper 名称。
    "tr_client_legacy_create", "tr_client_legacy_register_method",
    "tr_client_legacy_unary_call", "tr_client_legacy_unary_call_ex",
    "tr_client_legacy_call_start", "tr_client_legacy_call_start_ex",
    "tr_client_create_with_tuning", "tr_client_get_rpc_stats",
    "tr_server_create_with_tuning", "tr_server_legacy_create",
    "tr_server_legacy_register_method",
    "tr_server_legacy_register_stream_method",
    "tr_runtime_shard_rpc_executor", "tr_runtime_rpc_executor_config",
    "tr_runtime_legacy_rpc_execution_create",
    "tr_runtime_legacy_rpc_execution_can_start",
    "tr_runtime_legacy_rpc_execution_start",
    "tr_runtime_legacy_rpc_execution_stop",
    "tr_runtime_legacy_rpc_execution_destroy",
    "tr_runtime_legacy_rpc_execution_group",
    "tr_runtime_legacy_rpc_execution_raw",
})
LEGACY = re.compile(
    r"\b(?:" + "|".join(
        re.escape(name) for name in sorted(V1_CORE_DENY | EXACT_RETIREMENT_DENY)
    ) + r")\b"
)
FORBIDDEN_ARCHIVE_MEMBERS = frozenset({"rpc.c.o", "rpc_wire.c.o"})
RAW_CODEC_SYMBOLS = frozenset({"tr_rpc_raw_encode", "tr_rpc_raw_decode"})
SERVER_FACADE_RETIREMENT = frozenset({
    "tr_server_create_with_tuning", "tr_server_legacy_create",
    "tr_server_legacy_register_method",
    "tr_server_legacy_register_stream_method",
})
STRIP = re.compile(
    r"/\*[\s\S]*?\*/|//[^\n]*|"
    r'"(?:\\.|[^"\\])*"|'
    r"'(?:\\.|[^'\\])*'",
    re.MULTILINE,
)
XMAKE_SYMBOL_GUARD = re.compile(
    r"(?ms)^[ \t]*local v1_symbols = \{\n.*?^[ \t]*\}[^\n]*"
)
SEARCH_ROOTS = ("src", "include", "tests", "bench", "examples")
EXTENSIONS = {".c", ".h", ".cc", ".cpp", ".hpp"}


def remove_comments_and_literals(text):
    """保留原文本的换行位置，避免注释/字符串变成虚假调用点。"""
    return STRIP.sub(
        lambda match: re.sub(r"[^\n]", " ", match.group(0)), text
    )


def remove_xmake_symbol_guard(text):
    """排除 after_link 只用于断言的符号拒绝表。"""
    return XMAKE_SYMBOL_GUARD.sub(
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
    all_symbols = parse_nm_all(output)
    return {
        name: members for name, members in all_symbols.items()
        if LEGACY.fullmatch(name)
    }


def parse_nm_all(output):
    """解析全局 nm 结果，不预先丢弃允许保留的 RAW Codec 符号。"""
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
        result["all_" + key] = parse_nm_all(run.stdout)
    members = subprocess.run(
        ["ar", "t", str(archive)], capture_output=True, text=True, check=True,
    )
    result["members"] = [line.strip() for line in members.stdout.splitlines()
                         if line.strip()]
    result["archive_problems"] = validate_archive_inventory(
        result["members"], result["all_definitions"], result["all_undefined"]
    )
    return result


def validate_archive_inventory(members, definitions, undefined):
    """验证旧对象已移除、旧符号无定义/未解析引用且 RAW 归属正确。"""
    problems = []
    member_names = [Path(member).name for member in members]
    for forbidden in sorted(FORBIDDEN_ARCHIVE_MEMBERS):
        if forbidden in member_names:
            problems.append("静态库仍包含退役对象：" + forbidden)
    if "rpc_codec.c.o" not in member_names:
        problems.append("静态库缺少保留对象：rpc_codec.c.o")

    legacy_definitions = {
        name: providers for name, providers in definitions.items()
        if LEGACY.fullmatch(name)
    }
    legacy_undefined = {
        name: providers for name, providers in undefined.items()
        if LEGACY.fullmatch(name)
    }
    if legacy_definitions:
        problems.append("静态库仍定义退役全局符号：" + ", ".join(
            sorted(legacy_definitions)))
    if legacy_undefined:
        problems.append("静态库仍有退役符号未解析引用：" + ", ".join(
            sorted(legacy_undefined)))

    for symbol in sorted(RAW_CODEC_SYMBOLS):
        providers = definitions.get(symbol, [])
        if not providers:
            problems.append("静态库缺少 RAW Codec 定义：" + symbol)
            continue
        provider_members = [Path(provider.rsplit(":", 1)[-1]).name
                            for provider in providers]
        if any(member != "rpc_codec.c.o" for member in provider_members):
            problems.append(
                "RAW Codec 定义不来自 rpc_codec.c.o：" + symbol + " (" +
                ", ".join(provider_members) + ")"
            )
    return problems


def denied_symbols(symbols, deny):
    """返回显式退役列表中仍被源码或归档引用的名字。"""
    return sorted(set(deny).intersection(symbols))


def audit(root, archive=None):
    files, sites = collect_code(root)
    build = remove_xmake_symbol_guard(
        (root / "xmake.lua").read_text(encoding="utf-8")
    )
    build_uses = {}
    for number, line in enumerate(build.splitlines(), start=1):
        for match in LEGACY.finditer(line):
            build_uses.setdefault(match.group(0), []).append(number)
    nm = nm_inventory(archive) if archive else {
        "definitions": {}, "undefined": {}, "all_definitions": {},
        "all_undefined": {}, "members": [], "archive_problems": [],
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
        "archive_members": nm["members"],
        "archive_contract_problems": nm["archive_problems"],
        "raw_codec_definitions": {
            name: nm["all_definitions"].get(name, [])
            for name in sorted(RAW_CODEC_SYMBOLS)
        },
        "symbols": records,
        "limitations": [
            "源代码引用是剔除注释/字面量后的词法扫描，非 C AST 调用图",
            "nm 只涵盖静态库对象的全局符号，不能证明函数指针或静态符号不可达",
            "xmake 的名称与 --wrap 用途单独记账，测试依赖不能静默删除",
            "xmake 中 after_link 的 V1 符号拒绝表仅用于断言，不计为构建引用",
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
    parser.add_argument("--deny-v1-core", action="store_true",
                        help="Reject the exact retired V1 Core symbol/type list")
    args = parser.parse_args()
    root = args.root.resolve()
    archive = args.archive.resolve() if args.archive else None
    result = audit(root, archive)
    deny = set(args.deny)
    if args.deny_v1_core:
        deny.update(V1_CORE_DENY)
    remaining = denied_symbols(result["symbols"], deny)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n", encoding="utf-8",
    )
    failures = []
    if remaining:
        failures.append("退役符号仍存在源码或归档中：" + ", ".join(remaining))
    failures.extend(result["archive_contract_problems"])
    if failures:
        parser.error("；".join(failures))
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
    if archive:
        print("Archive members:", len(result["archive_members"]))
        print("RAW Codec definitions:", json.dumps(
            result["raw_codec_definitions"], ensure_ascii=False, sort_keys=True))
        print("Archive contract: passed")
    print("Detailed complete list:", args.output)


if __name__ == "__main__":
    main()
