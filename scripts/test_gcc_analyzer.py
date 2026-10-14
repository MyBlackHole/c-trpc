#!/usr/bin/env python3
"""验证 GCC 静态分析必然实际编译、保留构建身份并能正确传播失败。"""
import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import run_gcc_analyzer as target


class GCCAnalyzerTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory(prefix="trpc gcc analyzer' ")
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        root_patch = patch.object(target.compdb, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        (self.root / "src").mkdir()
        (self.root / "src/a.c").write_text("int a(void){return 0;}\n")
        self.args = ["/usr/bin/gcc", "-c", "-m64", "-g", "-Wall", "-Wextra",
                     "-Werror", "-O2", "-std=c11", "-Iinclude", "-pedantic",
                     "-pthread", "-o", "build/production/a.o", "src/a.c"]
        self.directory = self.root / "build/compdb"
        self.directory.mkdir(parents=True)
        self.report_dir = self.root / "build/gcc-analysis"
        self.entries = [{"file": "src/a.c", "directory": str(self.root),
                         "arguments": list(self.args)},
                        {"file": "tests/a.c", "directory": str(self.root),
                         "arguments": list(self.args[:-1]) + ["tests/a.c"]}]
        (self.directory / target.compdb.DATABASE).write_text(json.dumps(self.entries))
        self.manifest = {"identity": {"git_commit": "a" * 40},
                         "database_sha256": "b" * 64}
        (self.directory / target.compdb.MANIFEST).write_text(json.dumps(self.manifest))

    def test_command_retains_actual_production_flags_and_compiles(self):
        output = self.report_dir / "objects/src/a.c.o"
        actual = target.analysis_command(self.args, "src/a.c", output)
        self.assertEqual(actual[actual.index("-o") + 1], str(output))
        self.assertIn("-fanalyzer", actual)
        self.assertIn("-Werror", actual)
        self.assertIn("-O2", actual)
        self.assertIn("-c", actual)
        self.assertNotIn("-fsyntax-only", actual)
        self.assertEqual(actual[self.args.index("src/a.c")], "src/a.c")
        for check in target.REVIEW_CHECKS:
            self.assertIn("-Wno-error=" + check, actual)
            self.assertNotIn("-Wno-" + check, actual)
        self.assertEqual(self.args[-3:],
                         ["-o", "build/production/a.o", "src/a.c"])

    def test_rejects_syntax_only_or_missing_production_compile(self):
        for mutation in (lambda a: a.append("-fsyntax-only"),
                         lambda a: a.remove("-c"),
                         lambda a: a.append("-c"),
                         lambda a: a.remove("-o"),
                         lambda a: a.append("-fanalyzer")):
            original = list(self.args)
            mutation(original)
            with self.subTest(original=original), self.assertRaises(ValueError):
                target.analysis_command(original, "src/a.c", self.root / "test.o")

    def test_rejects_wrong_source_or_object_boundary(self):
        for mutation in (lambda a: a.__setitem__(-1, "src/other.c"),
                         lambda a: a.__setitem__(-2, "not-an-object"),
                         lambda a: a.__setitem__(0, "")):
            args = list(self.args)
            mutation(args)
            with self.assertRaises(ValueError):
                target.analysis_command(args, "src/a.c", self.root / "test.o")

    def test_snapshot_uses_real_compiler_and_excludes_test_entries(self):
        with patch.object(target.compdb, "snapshot", return_value={"commands": []}), \
             patch.object(target.compdb, "check_config") as config, \
             patch.object(target.compdb, "validate", return_value=1) as validate, \
             patch.object(target.compdb, "check_manifest") as manifest:
            units, identity = target.verified_entries(self.directory, "xmake")
        self.assertEqual(units, [("src/a.c", self.args)])
        self.assertEqual(identity, self.manifest)
        self.assertTrue(config.called)
        self.assertTrue(validate.called)
        self.assertTrue(manifest.called)

    def test_verified_coverage_rejects_mismatch(self):
        with patch.object(target.compdb, "snapshot", return_value={}), \
             patch.object(target.compdb, "check_config"), \
             patch.object(target.compdb, "validate", return_value=2), \
             patch.object(target.compdb, "check_manifest"):
            with self.assertRaisesRegex(ValueError, "count disagrees"):
                target.verified_entries(self.directory, "xmake")

    def test_gcc_compiler_version_is_pinned(self):
        good = subprocess.CompletedProcess([], 0, "11.4.0\n", "")
        with patch.object(target.subprocess, "run", return_value=good) as run:
            self.assertEqual(target.check_gcc("/usr/bin/gcc", 11), "11.4.0")
        self.assertEqual(run.call_args.args[0],
                         ["/usr/bin/gcc", "-dumpfullversion", "-dumpversion"])
        with patch.object(target.subprocess, "run", return_value=good):
            with self.assertRaisesRegex(ValueError, "version mismatch"):
                target.check_gcc("gcc", 14)

    def test_real_leak_probe_must_fail_with_malloc_diagnostic(self):
        def fake_run(command, **_):
            if str(command[-1]).endswith("cleanup.o"):
                Path(command[-1]).write_bytes(b"object")
                return subprocess.CompletedProcess(command, 0, "", "")
            return subprocess.CompletedProcess(command, 1, "",
                "/tmp/leak.c:7:5: error: leak of 'value' [-Werror=analyzer-malloc-leak]\n")
        with patch.object(target.subprocess, "run", side_effect=fake_run):
            result = target.verify_controls("/usr/bin/gcc", self.report_dir, 10)
        self.assertEqual(result["cleanup"]["exit_code"], 0)
        self.assertEqual(result["leak"]["analyzer_diagnostics"], 1)

    def test_false_green_leak_detection_must_be_rejected(self):
        def always_green(command, **_):
            Path(command[-1]).write_bytes(b"object")
            return subprocess.CompletedProcess(command, 0, "", "")
        with patch.object(target.subprocess, "run", side_effect=always_green):
            with self.assertRaisesRegex(ValueError, "failed to detect"):
                target.verify_controls("/usr/bin/gcc", self.report_dir, 10)

    def test_analyzer_cannot_pass_without_object_file(self):
        with patch.object(target.subprocess, "run",
                          return_value=subprocess.CompletedProcess([], 0, "", "")):
            report = target.analyze([("src/a.c", self.args)], "/usr/bin/gcc",
                                    self.directory, self.report_dir, 10, "11.4.0",
                                    self.manifest, {})
        self.assertEqual(report["failed_sources"], ["src/a.c"])
        self.assertEqual(report["results"][0]["returncode"], 125)
        self.assertTrue((self.report_dir / "summary.json").is_file())

    def test_diagnostic_nonzero_exit_is_not_hidden(self):
        log = ("src/a.c:3:4: error: leak of 'value' "
               "[-Werror=analyzer-malloc-leak]\n")
        with patch.object(target.subprocess, "run",
                          return_value=subprocess.CompletedProcess([], 1, "", log)):
            report = target.analyze([("src/a.c", self.args)], "/usr/bin/gcc",
                                    self.directory, self.report_dir, 10, "11.4.0",
                                    self.manifest, {})
        self.assertEqual(report["analyzer_diagnostics"], 1)
        self.assertEqual(report["failed_sources"], ["src/a.c"])

    def test_timeout_is_failure_not_success(self):
        def fail(*_, **__):
            raise subprocess.TimeoutExpired("gcc", 10, output=b"partial\n")
        with patch.object(target.subprocess, "run", side_effect=fail):
            report = target.analyze([("src/a.c", self.args)], "/usr/bin/gcc",
                                    self.directory, self.report_dir, 10, "11.4.0",
                                    self.manifest, {})
        self.assertEqual(report["results"][0]["returncode"], 124)
        self.assertIn("timed out", (self.report_dir / "logs/src/a.c.log").read_text())

    def test_unknown_production_compiler_rejected(self):
        altered = list(self.args)
        altered[0] = "a-different-compiler"
        with self.assertRaisesRegex(ValueError, "differs"):
            target.analyze([("src/a.c", altered)], "/usr/bin/gcc",
                           self.directory, self.report_dir, 10, "11.4.0",
                           self.manifest, {})


if __name__ == "__main__":
    unittest.main()
