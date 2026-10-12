#!/usr/bin/env python3
"""静态分析驱动器的确定性输入、退出码和证据覆盖回归。"""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import run_static_analysis as target


class StaticAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory(prefix="c-trpc analysis' ")
        self.addCleanup(self.workspace.cleanup)
        self.root = Path(self.workspace.name)
        self.original_root = target.compdb.ROOT
        target.compdb.ROOT = self.root
        self.addCleanup(setattr, target.compdb, "ROOT", self.original_root)
        self.sources = [self.root / "src/a.c", self.root / "src/sub/b.c"]
        for source in self.sources:
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("int main(void) { return 0; }\n")
        (self.root / ".clang-tidy").write_text("Checks: 'clang-analyzer-core.*'\n")
        self.database = self.root / "build/compdb"
        self.database.mkdir(parents=True)
        self.entries = [{"file": "src/a.c", "directory": str(self.root),
                         "arguments": ["clang", "-c", "src/a.c"]},
                        {"file": "tests/a.c", "directory": str(self.root),
                         "arguments": ["clang", "-c", "tests/a.c"]},
                        {"file": "src/sub/b.c", "directory": str(self.root),
                         "arguments": ["clang", "-c", "src/sub/b.c"]}]
        (self.database / target.compdb.DATABASE).write_text(json.dumps(self.entries))
        self.manifest = {"identity": {"git_commit": "example"}, "database_sha256": "test"}
        (self.database / target.compdb.MANIFEST).write_text(json.dumps(self.manifest))
        self.state = {"fake": "resolved-xmake-config"}
        for name, replacement in (("snapshot", lambda *_: self.state),
                                  ("check_config", lambda *_: None),
                                  ("validate", lambda *_: 2),
                                  ("check_manifest", lambda *_: None)):
            p = patch.object(target.compdb, name, side_effect=replacement)
            p.start()
            self.addCleanup(p.stop)

    def test_selects_only_production_after_validation(self):
        sources, manifest = target.verified_sources(self.database, "xmake", {"mode": "release"})
        self.assertEqual(sources, sorted(self.sources))
        self.assertEqual(manifest, self.manifest)

    def test_rejects_disagreement_with_validated_count(self):
        with patch.object(target.compdb, "validate", return_value=3):
            with self.assertRaisesRegex(ValueError, "coverage"):
                target.verified_sources(self.database, "xmake", {})

    def test_rejects_bad_configuration_identity(self):
        with patch.object(target.compdb, "check_manifest", side_effect=ValueError("stale")):
            with self.assertRaisesRegex(ValueError, "stale"):
                target.verified_sources(self.database, "xmake", {})

    def test_rejects_missing_unit(self):
        self.entries = [self.entries[1], self.entries[2]]
        (self.database / target.compdb.DATABASE).write_text(json.dumps(self.entries))
        with self.assertRaisesRegex(ValueError, "coverage"):
            target.verified_sources(self.database, "xmake", {})

    def test_version_and_check_selection(self):
        responses = [subprocess.CompletedProcess([], 0, "Ubuntu LLVM version 14.0.0\n", ""),
                     subprocess.CompletedProcess([], 0, "Enabled checks:\n  clang-analyzer-core.CallAndMessage\n  clang-analyzer-unix.Malloc\n", "")]
        with patch.object(target.subprocess, "run", side_effect=responses) as runner:
            version, checks = target.verify_analyzer("clang-tidy-14", 14,
                                                      self.root / ".clang-tidy", self.sources[0])
        self.assertIn("version 14", version)
        self.assertIn(target.REQUIRED_CHECKS[0], checks)
        self.assertIn("--config-file=" + str(self.root / ".clang-tidy"), runner.call_args.args[0])

    def test_rejects_wrong_version_and_disabled_checks(self):
        with patch.object(target.subprocess, "run", return_value=subprocess.CompletedProcess(
                [], 0, "Debian LLVM version 15.0.0\n", "")):
            with self.assertRaisesRegex(ValueError, "version mismatch"):
                target.verify_analyzer("clang-tidy", 14, self.root / ".clang-tidy", self.sources[0])
        responses = [subprocess.CompletedProcess([], 0, "LLVM version 14.0.1", ""),
                     subprocess.CompletedProcess([], 0, "Enabled checks:\n  misc-unused-parameters\n", "")]
        with patch.object(target.subprocess, "run", side_effect=responses):
            with self.assertRaisesRegex(ValueError, "inactive"):
                target.verify_analyzer("clang-tidy", 14, self.root / ".clang-tidy", self.sources[0])

    def test_diagnostics_are_retained_without_false_failure(self):
        log = "src/a.c:3:1: warning: simulated issue [clang-analyzer-core.CallAndMessage]\n"
        commands = []
        def run(command, **_):
            commands.append(command)
            return subprocess.CompletedProcess(command, 0, log, "")
        with patch.object(target.subprocess, "run", side_effect=run):
            report = target.analyze(self.sources, self.database, self.root / "build/report", "clang-tidy-14",
                                    False, 5, "LLVM version 14", list(target.REQUIRED_CHECKS), self.manifest)
        self.assertEqual(report["expected_units"], 2)
        self.assertEqual(report["analyzed_units"], 2)
        self.assertEqual(report["reported_warnings"], 2)
        self.assertFalse(report["failed_sources"])
        self.assertEqual(len(commands), 2)
        self.assertNotIn("--warnings-as-errors=*", commands[0])
        self.assertTrue((self.root / "build/report/summary.json").is_file())
        for entry in report["results"]:
            self.assertIn("warning: simulated issue", (self.root / "build/report" / entry["log"]).read_text())

    def test_nonzero_result_is_preserved_and_strict_flag_is_forwarded(self):
        commands = []
        def run(command, **_):
            commands.append(command)
            return subprocess.CompletedProcess(command, 1 if len(commands) == 1 else 0,
                                               "first failed\n", "")
        with patch.object(target.subprocess, "run", side_effect=run):
            report = target.analyze(self.sources, self.database, self.root / "build/report", "clang-tidy-14",
                                    True, 5, "LLVM version 14", list(target.REQUIRED_CHECKS), self.manifest)
        self.assertEqual(report["failed_sources"], ["src/a.c"])
        self.assertIn("--warnings-as-errors=*", commands[0])
        self.assertEqual(len(report["results"]), 2)

    def test_timeout_is_failure_not_success(self):
        def fail(*_, **__):
            raise subprocess.TimeoutExpired("clang-tidy", 5, output=b"partial warning:\n")
        with patch.object(target.subprocess, "run", side_effect=fail):
            report = target.analyze(self.sources, self.database, self.root / "build/report", "clang-tidy-14",
                                    False, 5, "LLVM version 14", [], self.manifest)
        self.assertEqual(len(report["failed_sources"]), 2)
        self.assertTrue(all(result["returncode"] == 124 for result in report["results"]))
        self.assertIn("Analysis timed out", (self.root / "build/report/logs/src__a.c.log").read_text())


if __name__ == "__main__":
    unittest.main()
