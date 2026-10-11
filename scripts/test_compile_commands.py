#!/usr/bin/env python3
"""从真实 Xmake 导出派生正负例，不维护手写编译数据库。"""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

import check_compile_commands as checker


class CompilationDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original_root = checker.ROOT
        cls.workspace = tempfile.TemporaryDirectory(prefix="trpc compdb's ")
        cls.root = Path(cls.workspace.name) / "project"
        shutil.copytree(checker.ROOT, cls.root, ignore=shutil.ignore_patterns(
            ".git", ".xmake", "build", "__pycache__"))
        checker.ROOT = cls.root
        cls.xmake = os.environ.get("XMAKE", "xmake")
        for command in (("git", "init", "-q"),
                        ("git", "-c", "user.name=compdb-test", "-c",
                         "user.email=compdb-test@example.invalid", "commit", "-qm",
                         "fixture", "--allow-empty")):
            checker.run(*command)
        checker.run(cls.xmake, "f", "-c", "-y", "-m", "release",
                    "--toolchain=gcc", "--crc32c_portable=n")
        cls.args = SimpleNamespace(xmake=cls.xmake, mode="release", toolchain="gcc",
                                   crc32c_portable="n", plat="linux", arch="x86_64",
                                   output_dir=cls.root / "build/compdb", check_only=False)
        with contextlib.redirect_stdout(io.StringIO()):
            checker.check(cls.args)
        cls.state = checker.snapshot(cls.xmake)
        cls.data = (cls.args.output_dir / checker.DATABASE).read_bytes()
        cls.entries = json.loads(cls.data)
        cls.manifest = json.loads((cls.args.output_dir / checker.MANIFEST).read_bytes())

    @classmethod
    def tearDownClass(cls):
        checker.ROOT = cls.original_root
        cls.workspace.cleanup()

    def rejects(self, mutate, diagnostic):
        entries = copy.deepcopy(self.entries)
        mutate(entries)
        with self.assertRaisesRegex(ValueError, diagnostic):
            checker.validate(entries, self.state)

    def test_real_export(self):
        self.assertEqual(checker.validate(self.entries, self.state),
                         len(list((self.root / "src").rglob("*.c"))))
        checker.check_manifest(self.manifest, self.data, self.state)
        args = copy.copy(self.args)
        args.check_only = True
        with contextlib.redirect_stdout(io.StringIO()):
            checker.check(args)

    def test_missing(self):
        self.rejects(lambda items: items.pop(0), "Missing production entries")

    def test_duplicate(self):
        self.rejects(lambda items: items.append(copy.deepcopy(items[0])), "Duplicate")

    def test_wrong_arguments(self):
        for value in ("-std=gnu11", "-Iwrong-include", "-DTR_CRC32C_FORCE_PORTABLE",
                      "-DNDEBUG", "-UNDEBUG", "-O0", "-DTR_INJECTED_TEST=1",
                      "-UTR_CRC32C_FORCE_PORTABLE", "-std=c17"):
            with self.subTest(value=value):
                self.rejects(lambda items: items[0]["arguments"].append(value),
                             "Arguments differ")
        for value in ("-std=c11", "-Iinclude", "-pthread", "-Wall", "-Werror"):
            with self.subTest(removed=value):
                self.rejects(lambda items: items[0]["arguments"].remove(value),
                             "Arguments differ")

    def test_wrong_compiler_and_object(self):
        self.rejects(lambda items: items[0]["arguments"].__setitem__(0, "/wrong/gcc"),
                     "Arguments differ")
        self.rejects(lambda items: items[0]["arguments"].__setitem__(-2, "other.o"),
                     "Arguments differ")

    def test_wrong_directory(self):
        def change(items):
            items[0]["file"] = str(self.root / items[0]["file"])
            items[0]["directory"] = str(self.root.parent)
        self.rejects(change, "Wrong working directory")

    def test_extra_production_source(self):
        self.rejects(lambda items: items.append({**items[0], "file": "src/unknown.c"}),
                     "Unexpected production entry")

    def test_xmake_coverage_omission(self):
        state = copy.deepcopy(self.state)
        state["commands"].pop()
        with self.assertRaisesRegex(ValueError, "production sources differ"):
            checker.validate(self.entries, state)

    def test_tests_cannot_replace_production(self):
        def change(items):
            test = next(item for item in items if item["file"].startswith("tests/"))
            items[0]["arguments"] = test["arguments"]
        self.rejects(change, "Arguments differ")

    def test_contract_even_when_build_graph_agrees(self):
        # 防止同时误改构建定义与导出时，仅靠两者相等就通过 C11/断言基线。
        for before, after in (("-std=c11", "-std=gnu11"),
                              ("-Iinclude", "-Ielsewhere"),
                              ("-pthread", "-DNDEBUG"),
                              ("-g", "-DTR_CRC32C_FORCE_PORTABLE=0"),
                              ("-g", "-xc++")):
            entries, state = copy.deepcopy(self.entries), copy.deepcopy(self.state)
            source = entries[0]["file"]
            expected = next(item for item in state["commands"] if item["file"] == source)
            for args in (entries[0]["arguments"], expected["arguments"]):
                args[args.index(before)] = after
            with self.subTest(after=after), self.assertRaisesRegex(ValueError, "contract failed"):
                checker.validate(entries, state)

    def test_malformed_database(self):
        for entries in (None, {}, [], [{"file": "src/status.c"}],
                        [{"file": "src/status.c", "directory": str(self.root),
                          "arguments": [None]}]):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                checker.validate(entries, self.state)

    def test_identity_and_digest(self):
        for key in ("mode", "toolchain", "arch", "plat", "crc32c_portable", "make_cflags"):
            state = copy.deepcopy(self.state)
            state["config"][key] = "different"
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "stale"):
                checker.check_manifest(self.manifest, self.data, state)
        for key in ("git_commit", "build_definition_sha256", "xmake_version", "compiler_versions"):
            state = copy.deepcopy(self.state)
            state[key] = "different"
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "stale"):
                checker.check_manifest(self.manifest, self.data, state)
        with self.assertRaisesRegex(ValueError, "digest"):
            checker.check_manifest(self.manifest, self.data + b"\n", self.state)

    def test_requested_config(self):
        for key in ("mode", "toolchain", "arch", "plat", "crc32c_portable"):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "Configuration mismatch"):
                checker.check_config(self.state, {key: "wrong"})

    def test_cli_failure_exit(self):
        args = ["python3", str(self.root / "scripts/check_compile_commands.py"),
                "--xmake", self.xmake, "--output-dir", str(self.args.output_dir),
                "--mode", "debug", "--toolchain", "gcc", "--arch", "x86_64",
                "--crc32c-portable", "n", "--check-only", "--lang", "en"]
        result = subprocess.run(args, cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2, result.stdout)
        self.assertIn("Configuration mismatch", result.stderr)

    def test_y_reject_real_contract_overrides(self):
        # 真正重配构建图与原生导出，验证独立约束能发现两者同时发生的覆盖。
        try:
            for flag in ("-DNDEBUG(x)=x", "-DTR_CRC32C_FORCE_PORTABLE(x)=x",
                         "-w", "-Wno-error", "-Wno-error=unused-variable"):
                checker.run(self.xmake, "f", "-c", "-y", "-m", "release",
                            "--toolchain=gcc", "--crc32c_portable=n",
                            "--make_cflags=" + flag)
                with self.subTest(flag=flag), self.assertRaisesRegex(ValueError, "contract failed"):
                    checker.check(self.args)
        finally:
            checker.run(self.xmake, "f", "-c", "-y", "-m", "release",
                        "--toolchain=gcc", "--crc32c_portable=n")

    def test_z_reconfigure_real_macros_and_quoted_flags(self):
        include = self.root / "extra include's directory"
        include.mkdir()
        flags = f'-DTR_COMPDB_LABEL=\'"two words"\' -I"{include}"'
        flags += f' -isystem "{include}"'
        checker.run(self.xmake, "f", "-c", "-y", "-m", "debug", "--toolchain=gcc",
                    "--crc32c_portable=y", "--make_cflags=" + flags)
        state = checker.snapshot(self.xmake)
        with self.assertRaisesRegex(ValueError, "Arguments differ"):
            checker.validate(self.entries, state)
        with self.assertRaisesRegex(ValueError, "stale"):
            checker.check_manifest(self.manifest, self.data, state)
        args = copy.copy(self.args)
        args.mode, args.crc32c_portable = "debug", "y"
        args.output_dir = self.root / "build/compdb-portable"
        with contextlib.redirect_stdout(io.StringIO()):
            checker.check(args)
        entries = json.loads((args.output_dir / checker.DATABASE).read_bytes())
        self.assertIn("-DTR_CRC32C_FORCE_PORTABLE", entries[0]["arguments"])
        self.assertIn('-DTR_COMPDB_LABEL="two words"', entries[0]["arguments"])
        self.assertIn("-I" + str(include), entries[0]["arguments"])
        self.assertIn("-isystem", entries[0]["arguments"])
        self.assertIn(str(include), entries[0]["arguments"])


if __name__ == "__main__":
    unittest.main()
