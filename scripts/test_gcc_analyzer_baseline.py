#!/usr/bin/env python3
"""GCC 静态分析精确基线门禁的正反例及错误传播。"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_gcc_analyzer_baseline as gate


class GCCWarningGateTests(unittest.TestCase):
    def setUp(self):
        workspace = tempfile.TemporaryDirectory(prefix="trpc gcc-warning' ")
        self.addCleanup(workspace.cleanup)
        self.root = Path(workspace.name)
        root_patch = patch.object(gate, "ROOT", self.root)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        (self.root / "src").mkdir()
        (self.root / "src/a.c").write_text("int f(void) { return 0; }\n")
        self.dir = self.root / "report"
        self.log = self.dir / "logs/src/a.c.log"
        self.log.parent.mkdir(parents=True)
        self.base_file = self.root / "baseline.json"
        self.warning = {
            "unit": "src/a.c", "file": "src/a.c", "line": 3, "column": 4,
            "check": "analyzer-malloc-leak",
            "message": "leak of ‘value’ [CWE-401]"}
        self.baseline = {
            "schema": 1, "gcc_major": 11, "review_state": "unverified",
            "profile": dict(gate.PROFILE),
            "warnings": [copy.deepcopy(self.warning)]}
        self.report = {
            "schema": 1, "analyzer": "gcc -fanalyzer",
            "gcc_version": "11.4.0", "strict": False,
            "production_units": 1, "profile": dict(gate.PROFILE),
            "identity": {"target": "trcore", "git_commit": "a" * 40,
                         "config": dict(gate.PROFILE)},
            "database_sha256": "b" * 64,
            "controls": {"cleanup": {"exit_code": 0, "analyzer_diagnostics": 0},
                         "leak": {"exit_code": 1, "analyzer_diagnostics": 1}},
            "analyzer_diagnostics": 1, "failed_sources": [],
            "results": [{"source": "src/a.c", "returncode": 0,
                         "analyzer_diagnostics": 1, "log": "logs/src/a.c.log"}]}

    def warning_line(self, record=None):
        record = self.warning if record is None else record
        return ("{file}:{line}:{column}: warning: {message} [-W{check}]\n"
                .format(**record))

    def save(self, log=None):
        self.base_file.write_text(json.dumps(self.baseline, ensure_ascii=False))
        (self.dir / "summary.json").write_text(json.dumps(self.report))
        self.log.write_text(self.warning_line() if log is None else log)

    def compare(self):
        return gate.compare(self.dir, self.base_file)

    def test_exact_known_warning_preserved(self):
        self.save()
        result = self.compare()
        self.assertEqual(result["observed_warnings"], 1)
        self.assertEqual(result["unreviewed_warnings"], [])
        self.assertTrue((self.dir / "warning-gate.json").is_file())

    def test_new_warning_on_same_source_is_blocked(self):
        extra = dict(self.warning, message="leak of ‘other’ [CWE-401]")
        self.report["analyzer_diagnostics"] = 2
        self.report["results"][0]["analyzer_diagnostics"] = 2
        self.save(self.warning_line() + self.warning_line(extra))
        with self.assertRaisesRegex(ValueError, "added=1"):
            self.compare()
        report = json.loads((self.dir / "warning-gate.json").read_text())
        self.assertEqual(len(report["unreviewed_warnings"]), 1)

    def test_changed_check_or_message_is_blocked(self):
        for variant in (dict(self.warning, check="analyzer-use-after-free"),
                        dict(self.warning, message="changed warning")):
            with self.subTest(variant=variant):
                self.save(self.warning_line(variant))
                with self.assertRaisesRegex(ValueError, "changed"):
                    self.compare()

    def test_removed_warning_requires_review(self):
        self.report["analyzer_diagnostics"] = 0
        self.report["results"][0]["analyzer_diagnostics"] = 0
        self.save("")
        with self.assertRaisesRegex(ValueError, "removed=1"):
            self.compare()

    def test_unparseable_diagnostic_is_rejected(self):
        self.save("src/a.c:3:4: warning: unknown diagnostic\n")
        with self.assertRaisesRegex(ValueError, "Unrecognized"):
            self.compare()

    def test_repeated_warning_cannot_hide_duplicate(self):
        self.report["analyzer_diagnostics"] = 2
        self.report["results"][0]["analyzer_diagnostics"] = 2
        self.save(self.warning_line() * 2)
        with self.assertRaisesRegex(ValueError, "added=1"):
            self.compare()

    def test_missing_log_or_mismatched_counter_fails(self):
        self.save()
        self.log.unlink()
        with self.assertRaisesRegex(ValueError, "Missing GCC"):
            self.compare()
        self.report["analyzer_diagnostics"] = 2
        self.save()
        with self.assertRaisesRegex(ValueError, "does not match"):
            self.compare()

    def test_disabled_controls_cannot_falsely_pass(self):
        for name in ("cleanup", "leak"):
            before = copy.deepcopy(self.report)
            if name == "cleanup":
                self.report["controls"][name]["exit_code"] = 1
            else:
                self.report["controls"][name]["analyzer_diagnostics"] = 0
            with self.subTest(control=name):
                self.save()
                with self.assertRaisesRegex(ValueError, "controls"):
                    self.compare()
            self.report = before

    def test_incomplete_production_coverage_fails(self):
        for change in ("failed", "missing", "duplicate"):
            before = copy.deepcopy(self.report)
            if change == "failed":
                self.report["failed_sources"] = ["src/a.c"]
            elif change == "missing":
                self.report["production_units"] = 2
            else:
                self.report["results"].append(copy.deepcopy(self.report["results"][0]))
                self.report["production_units"] = 2
            with self.subTest(change=change):
                self.save()
                with self.assertRaises(ValueError):
                    self.compare()
            self.report = before

    def test_version_and_identity_are_pinned(self):
        for field, mutated in (("gcc_version", "14.2.0"),
                               ("database_sha256", "broken"),
                               ("strict", True)):
            before = copy.deepcopy(self.report)
            self.report[field] = mutated
            with self.subTest(field=field):
                self.save()
                with self.assertRaises(ValueError):
                    self.compare()
            self.report = before

    def test_baseline_cannot_be_a_wildcard(self):
        for option in ("wrong-schema", "wrong-profile", "duplicate",
                       "extra-field", "traversal", "unreviewed-state"):
            before = copy.deepcopy(self.baseline)
            if option == "wrong-schema":
                self.baseline["schema"] = 2
            elif option == "wrong-profile":
                self.baseline["profile"]["mode"] = "debug"
            elif option == "duplicate":
                self.baseline["warnings"].append(copy.deepcopy(self.warning))
            elif option == "extra-field":
                self.baseline["warnings"][0]["wildcard"] = "*"
            elif option == "traversal":
                self.baseline["warnings"][0]["file"] = "src/../other.c"
            else:
                self.baseline["review_state"] = "approved"
            with self.subTest(option=option):
                self.save()
                with self.assertRaises(ValueError):
                    self.compare()
            self.baseline = before

    def test_gcc_error_must_not_be_treated_as_an_allowed_warning(self):
        self.save(self.warning_line().replace(": warning:", ": error:"))
        with self.assertRaisesRegex(ValueError, "Unrecognized"):
            self.compare()


if __name__ == "__main__":
    unittest.main()
