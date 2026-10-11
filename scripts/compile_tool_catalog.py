#!/usr/bin/env python3
"""用 GNU gettext 校验并确定性生成工具翻译目录，仅维护阶段需要 msgfmt。"""
from collections import Counter
import gettext
from io import BytesIO
import os
from string import Formatter
import sys
import subprocess

from tool_i18n import DOMAIN, LOCALE_DIR, parser_for

ROOT = LOCALE_DIR


def fields(message):
    result = Counter()
    for _, name, spec, conversion in Formatter().parse(message):
        if name is not None:
            result[(name, spec, conversion)] += 1
            result.update(fields(spec))
    return result


def compile_catalog(source):
    # 使用 GNU 工具解析 PO、校验占位符，不复制解析器或编译器。
    data = subprocess.run([os.environ.get("MSGFMT", "msgfmt"), "--check",
                           "--check-format", "--no-hash", "--output-file=-", str(source)],
                          check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    # GNU msgfmt 不要求 !r 等转换标记一致；在已解析 MO 上补充精确检查。
    for msgid, msgstr in gettext.GNUTranslations(BytesIO(data))._catalog.items():
        if isinstance(msgid, str) and msgid and fields(msgid) != fields(msgstr):
            raise ValueError("Translation placeholders differ")
    return data


def main():
    parser, _ = parser_for("Validate and reproducibly compile tool translations.")
    parser.add_argument("--check", action="store_true",
                        help=_("Check that committed MO files are reproducible"))
    args = parser.parse_args()
    source = ROOT / "zh_CN" / "LC_MESSAGES" / (DOMAIN + ".po")
    destination = source.with_suffix(".mo")
    try:
        data = compile_catalog(source)
        if args.check:
            if destination.read_bytes() != data:
                parser.error(_("Translation catalog is stale; run scripts/compile_tool_catalog.py"))
        else:
            destination.write_bytes(data)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr.decode(errors="replace"), end="", file=sys.stderr)
        parser.error(_("Could not compile translation catalog: {error}").format(error=_(str(error))))


if __name__ == "__main__":
    main()
