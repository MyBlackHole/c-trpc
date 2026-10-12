#!/usr/bin/env python3
"""确保告警基线门禁精确、失败闭合，不掩盖新增诊断。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_static_analysis_baseline as gate


class AnalysisWarningGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="trpc warning gate' ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        root_patch = patch.object(gate, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        self.source = "src/a.c"
        (self.root / self.source).parent.mkdir(parents=True)
        (self.root / self.source).write_text("int f(void) { return 0; }\n")
        self.dir = self.root / "build/report"
        self.log = self.dir / "logs/src/a.c.log"
        self.log.parent.mkdir(parents=True)
        self.warning = {"source": self.source, "file": self.source,
                        "line": 3, "column": 1, "check": "clang-analyzer-unix.Malloc",
                        "message": "Potential leak of memory pointed to by 'p'"}
        self.report = {"schema": 1, "analyzer": "clang-tidy-14",
                       "version": "Ubuntu LLVM version 14.0.0",
                       "checks": list(gate.CHECKS), "strict": False,
                       "identity": {"target": "trcore", "git_commit": "a" * 40,
                                    "config": copy.deepcopy(gate.PROFILE)},
                       "database_sha256": "f" * 64,
                       "expected_units": 1, "analyzed_units": 1,
                       "reported_warnings": 1, "failed_sources": [],
                       "results": [{"source": self.source, "returncode": 0,
                                    "warnings": 1, "log": "logs/src/a.c.log"}]}
        self.baseline = {"schema": 1, "analyzer_major": 14,
                         "profile": copy.deepcopy(gate.PROFILE),
                         "review_state": "unverified",
                         "warnings": [copy.deepcopy(self.warning)]}
        self.baseline_file = self.root / "docs/known.json"
        self.baseline_file.parent.mkdir()

    def warning_line(self, warning=None, path=None):
        warning = warning or self.warning
        path = path or warning["file"]
        return (f'{path}:{warning["line"]}:{warning["column"]}: warning: '
                f'{warning["message"]} [{warning["check"]}]\n')

    def save(self, log=None):
        self.baseline_file.write_text(json.dumps(self.baseline, ensure_ascii=False))
        (self.dir / "summary.json").write_text(json.dumps(self.report))
        self.log.write_text(self.warning_line() if log is None else log)

    def check(self):
        return gate.compare(self.dir, self.baseline_file)

    def test_exact_warning_remains_visible(self):
        self.save()
        result = self.check()
        self.assertEqual(result["observed_warnings"], 1)
        self.assertEqual(result["baseline_warnings"], 1)
        self.assertEqual(result["unreviewed_warnings"], [])
        self.assertTrue((self.dir / "warning-gate.json").exists())

    def test_absolute_runner_path_has_same_identity(self):
        self.save(log=self.warning_line(path=str(self.root / self.source)))
        self.assertEqual(self.check()["observed_warnings"], 1)

    def test_changed_warning_message_blocks(self):
        self.save(log=self.warning_line().replace("by 'p'", "by 'q'"))
        with self.assertRaisesRegex(ValueError, "changed"):
            self.check()
        evidence = json.loads((self.dir / "warning-gate.json").read_text())
        self.assertEqual(len(evidence["unreviewed_warnings"]), 1)
        self.assertEqual(len(evidence["missing_baseline_warnings"]), 1)

    def test_new_warning_on_same_file_and_line_blocks(self):
        extra = copy.deepcopy(self.warning)
        extra["message"] = "Potential leak of memory pointed to by 'q'"
        self.report["reported_warnings"] = 2
        self.report["results"][0]["warnings"] = 2
        self.save(log=self.warning_line() + self.warning_line(extra))
        with self.assertRaisesRegex(ValueError, "new=1"):
            self.check()

    def test_duplicate_existing_warning_cannot_eat_second_occurrence(self):
        self.report["reported_warnings"] = 2
        self.report["results"][0]["warnings"] = 2
        self.save(log=self.warning_line() * 2)
        with self.assertRaisesRegex(ValueError, "new=1"):
            self.check()

    def test_removed_warning_requires_baseline_review(self):
        self.report["reported_warnings"] = 0
        self.report["results"][0]["warnings"] = 0
        self.save(log="")
        with self.assertRaisesRegex(ValueError, "missing=1"):
            self.check()

    def test_different_checker_blocks(self):
        self.save(log=self.warning_line().replace("clang-analyzer-unix.Malloc",
                                                   "clang-analyzer-core.NullDereference"))
        with self.assertRaisesRegex(ValueError, "new=1"):
            self.check()

    def test_unparseable_warning_fails_closed(self):
        self.save(log="src/a.c:3:1: warning: missing check suffix\n")
        with self.assertRaisesRegex(ValueError, "Unparsed"):
            self.check()

    def test_mismatched_summary_count_blocks(self):
        self.report["reported_warnings"] = 0
        self.save()
        with self.assertRaisesRegex(ValueError, "Summary warning total"):
            self.check()

    def test_missing_or_moved_log_blocks(self):
        self.save()
        self.log.unlink()
        with self.assertRaisesRegex(ValueError, "Missing or unsafe"):
            self.check()
        self.report["results"][0]["log"] = "../other.log"
        self.save()
        with self.assertRaisesRegex(ValueError, "Unexpected analysis log"):
            self.check()

    def test_wrong_tool_or_profile_blocks(self):
        for key, change in (("analyzer", lambda: self.report.update(analyzer="clang-tidy-15")),
                            ("version", lambda: self.report.update(version="LLVM version 15")),
                            ("checks", lambda: self.report.update(checks=[])),
                            ("strict", lambda: self.report.update(strict=True)),
                            ("config", lambda: self.report["identity"]["config"].update(mode="debug")),
                            ("hash", lambda: self.report.update(database_sha256="broken"))):
            with self.subTest(key=key):
                old = copy.deepcopy(self.report)
                change()
                self.save()
                with self.assertRaises(ValueError):
                    self.check()
                self.report = old

    def test_bad_baseline_cannot_be_wildcard(self):
        for change in ("wrong_schema", "wrong_major", "wrong_profile", "unverified_missing",
                       "extra_field", "duplicate", "absolute", "traversal"):
            with self.subTest(change=change):
                original = copy.deepcopy(self.baseline)
                if change == "wrong_schema":
                    self.baseline["schema"] = 2
                elif change == "wrong_major":
                    self.baseline["analyzer_major"] = 15
                elif change == "wrong_profile":
                    self.baseline["profile"]["arch"] = "arm64"
                elif change == "unverified_missing":
                    self.baseline.pop("review_state")
                elif change == "extra_field":
                    self.baseline["warnings"][0]["wildcard"] = "*"
                elif change == "duplicate":
                    self.baseline["warnings"].append(copy.deepcopy(self.warning))
                elif change == "absolute":
                    self.baseline["warnings"][0]["file"] = str(self.root / self.source)
                elif change == "traversal":
                    self.baseline["warnings"][0]["file"] = "src/../other.c"
                self.save()
                with self.assertRaises(ValueError):
                    self.check()
                self.baseline = original

    def test_failed_or_missing_production_unit_blocks(self):
        for change in ("failure", "missing", "duplicate"):
            with self.subTest(change=change):
                old = copy.deepcopy(self.report)
                if change == "failure":
                    self.report["results"][0]["returncode"] = 1
                elif change == "missing":
                    self.report["expected_units"] = 2
                else:
                    self.report["results"].append(copy.deepcopy(self.report["results"][0]))
                    self.report["expected_units"] = self.report["analyzed_units"] = 2
                self.save()
                with self.assertRaises(ValueError):
                    self.check()
                self.report = old

    def test_nonproduction_source_rejected(self):
        self.report["results"][0]["source"] = "tests/a.c"
        self.save()
        with self.assertRaisesRegex(ValueError, "non-production"):
            self.check()


if __name__ == "__main__":
    unittest.main()
