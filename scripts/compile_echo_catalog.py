#!/usr/bin/env python3
"""用 GNU msgfmt 编译 Echo 消息目录；--check 校验已提交产物。"""

import argparse
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description="Compile the Echo gettext catalog with GNU msgfmt.")
    parser.add_argument("--msgfmt", default="msgfmt", help="GNU msgfmt executable")
    parser.add_argument("--check", action="store_true", help="verify the committed MO matches the PO")
    args = parser.parse_args()
    directory = (Path(__file__).resolve().parent.parent / "locales" / "zh_CN"
                 / "LC_MESSAGES")
    source = directory / "c-trpc-examples.po"
    destination = source.with_suffix(".mo")
    with tempfile.TemporaryDirectory() as temporary:
        compiled = Path(temporary) / destination.name
        subprocess.run([args.msgfmt, "--check", "--check-format", "--no-hash",
                        "-o", str(compiled), str(source)], check=True)
        if args.check:
            if compiled.read_bytes() != destination.read_bytes():
                parser.error("catalog is outdated; rebuild and commit the PO and MO")
        else:
            destination.write_bytes(compiled.read_bytes())
    print("Echo catalog verified" if args.check else "Echo catalog compiled")


if __name__ == "__main__":
    main()
