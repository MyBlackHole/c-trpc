#!/usr/bin/env python3
"""导出并校验真实 Xmake 编译数据库；不运行静态分析器。"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from tool_i18n import parser_for

ROOT = Path(__file__).resolve().parent.parent
DATABASE = "compile_commands.json"
MANIFEST = "configuration.json"


def run(*args):
    return subprocess.run(args, cwd=ROOT, check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE).stdout


def digest(data):
    return hashlib.sha256(data).hexdigest()


def snapshot(xmake):
    # 直接询问 Xmake 已解析的构建图；不解析 Lua，也不手写预期编译命令。
    with tempfile.TemporaryDirectory(prefix="trpc-compdb-") as tmp:
        output = Path(tmp) / "config.json"
        run(xmake, "lua", str(ROOT / "scripts/compilation_database_config.lua"),
            str(output))
        state = json.loads(output.read_bytes())
    state["git_commit"] = run("git", "rev-parse", "HEAD").decode().strip()
    state["build_definition_sha256"] = digest((ROOT / "xmake.lua").read_bytes())
    state["compiler_versions"] = {
        command: run(command, "--version").decode().splitlines()[0]
        for command in sorted({entry["arguments"][0] for entry in state["commands"]})
    }
    return state


def identity(state):
    return {"schema": 1, "target": "trcore",
            **{key: value for key, value in state.items() if key != "commands"}}


def check_config(state, expected, _=lambda message: message):
    for key, value in expected.items():
        actual = state["config"].get(key)
        if actual != value:
            raise ValueError(_("Configuration mismatch: {key}: expected {expected!r}, found {actual!r}")
                             .format(key=key, expected=value, actual=actual))
    if Path(state["project_dir"]).resolve() != ROOT:
        raise ValueError(_("Xmake project directory does not match this checkout"))


def source_path(entry):
    return (Path(entry["directory"]) / entry["file"]).resolve()


def driver_options(args):
    # 仅解释要保护的 GCC/Clang 选项；其余 argv 仍完整比较，不重建命令。
    result = {"-I": [], "-D": [], "-U": [], "-x": []}
    macros = {}
    index = 1
    while index < len(args):
        arg = args[index]
        prefix = arg[:2]
        if prefix in result:
            value = arg[2:]
            if not value:
                index += 1
                value = args[index] if index < len(args) else ""
            result[prefix].append(value)
            if prefix in ("-D", "-U"):
                # -DNDEBUG(x)=x 仍定义 NDEBUG，不能把形参当作宏名的一部分。
                name = value.split("=", 1)[0].split("(", 1)[0].strip()
                macros[name] = prefix == "-D"
        index += 1
    return result, macros


def validate(entries, state, _=lambda message: message):
    expected = {(ROOT / item["file"]).resolve(): item["arguments"]
                for item in state["commands"]}
    sources = {path.resolve() for path in (ROOT / "src").rglob("*.c")}
    if not expected or set(expected) != sources:
        raise ValueError(_("Current Linux production sources differ from the Xmake trcore target: {files}")
                         .format(files=sorted(map(str, sources.symmetric_difference(expected)))))
    if not isinstance(entries, list) or not entries:
        raise ValueError(_("Compilation database must be a nonempty JSON array"))
    found = {}
    for item in entries:
        if (not isinstance(item, dict) or not isinstance(item.get("file"), str)
                or not isinstance(item.get("directory"), str)
                or not isinstance(item.get("arguments"), list)
                or not item["arguments"]
                or not all(isinstance(arg, str) for arg in item["arguments"])):
            raise ValueError(_("Invalid Xmake compilation database entry"))
        source = source_path(item)
        # 原始导出可能包含测试；它们不能替代任何生产翻译单元。
        if source not in expected:
            if source.is_relative_to(ROOT / "src"):
                raise ValueError(_("Unexpected production entry: {file}").format(file=source))
            continue
        if source in found:
            raise ValueError(_("Duplicate production entry: {file}").format(file=source))
        if Path(item["directory"]).resolve() != ROOT:
            raise ValueError(_("Wrong working directory: {file}").format(file=source))
        args = item["arguments"]
        # 逐项比较原始 argv，包含编译器、宏、包含路径、对象路径及参数顺序。
        # 不允许后续 -std/-D/-U 覆盖被子串检查误判为有效的前序参数。
        if args != expected[source]:
            raise ValueError(_("Arguments differ from the current Xmake build: {file}")
                             .format(file=source))
        standards = [arg for arg in args if arg.startswith("-std=")]
        options, macros = driver_options(args)
        includes = [(ROOT / value).resolve() for value in options["-I"]]
        portable = macros.get("TR_CRC32C_FORCE_PORTABLE", False)
        if (standards != ["-std=c11"] or ROOT / "include" not in includes
                or portable != state["config"]["crc32c_portable"]
                or any(flag not in args for flag in ("-c", "-pthread", "-Wall", "-Wextra", "-Werror", "-pedantic"))
                or any(arg == "-w" or arg.startswith("-Wno-") for arg in args)
                or macros.get("NDEBUG", False)
                or any(language != "c" for language in options["-x"])):
            raise ValueError(_("Production C11, include, macro or warning contract failed: {file}")
                             .format(file=source))
        found[source] = item
    missing = set(expected) - set(found)
    if missing:
        raise ValueError(_("Missing production entries: {files}").format(files=sorted(map(str, missing))))
    return len(found)


def check_manifest(manifest, data, state, _=lambda message: message):
    if manifest.get("identity") != identity(state):
        raise ValueError(_("Compilation database configuration is stale; export it again"))
    if manifest.get("database_sha256") != digest(data):
        raise ValueError(_("Compilation database digest does not match its configuration"))


def check(args, _=lambda message: message):
    state = snapshot(args.xmake)
    check_config(state, {"mode": args.mode, "toolchain": args.toolchain,
                         "plat": args.plat, "arch": args.arch,
                         "crc32c_portable": args.crc32c_portable == "y"}, _)
    output = args.output_dir.resolve()
    if args.check_only:
        data = (output / DATABASE).read_bytes()
        count = validate(json.loads(data), state, _)
        check_manifest(json.loads((output / MANIFEST).read_bytes()), data, state, _)
    else:
        output.mkdir(parents=True, exist_ok=True)
        # 在临时目录完成原生导出与校验；失败时不把半成品标成有效数据库。
        with tempfile.TemporaryDirectory(prefix=".export-", dir=output) as tmp:
            run(args.xmake, "project", "-k", "compile_commands", "--lsp=clangd",
                "-t", "trcore", tmp)
            data = (Path(tmp) / DATABASE).read_bytes()
            count = validate(json.loads(data), state, _)
            if snapshot(args.xmake) != state:
                raise ValueError(_("Xmake configuration changed during export"))
            manifest = {"identity": identity(state), "database_sha256": digest(data),
                        "production_units": count, "total_entries": len(json.loads(data))}
            (Path(tmp) / MANIFEST).write_text(json.dumps(manifest, ensure_ascii=False,
                                                       indent=2, sort_keys=True) + "\n")
            os.replace(Path(tmp) / DATABASE, output / DATABASE)
            os.replace(Path(tmp) / MANIFEST, output / MANIFEST)
    print(_("Verified {count} production compilation commands: {directory}")
          .format(count=count, directory=output))


def main():
    parser, _ = parser_for("Export and verify the real Xmake production compilation database without running analyzers.")
    parser.add_argument("--xmake", default=os.environ.get("XMAKE", "xmake"),
                        help=_("Path to the Xmake executable"))
    parser.add_argument("--output-dir", type=Path, required=True,
                        help=_("Directory for the database and configuration identity"))
    parser.add_argument("--mode", choices=("debug", "release", "asan", "tsan"), required=True)
    parser.add_argument("--toolchain", choices=("gcc", "clang"), required=True)
    parser.add_argument("--crc32c-portable", choices=("y", "n"), required=True)
    parser.add_argument("--plat", default="linux")
    parser.add_argument("--arch", required=True)
    parser.add_argument("--check-only", action="store_true",
                        help=_("Verify an existing export against the current Xmake configuration"))
    args = parser.parse_args()
    try:
        check(args, _)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(_("Compilation database check failed: {error}").format(error=error), file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr.decode(errors="replace"), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
