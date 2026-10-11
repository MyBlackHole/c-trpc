#!/usr/bin/env python3
"""独立 Git 仓库集成测试；使用与 CI 相同的固定版本格式工具。"""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("check_source_format.py").resolve()
FORMATTER = os.environ.get("CLANG_FORMAT", "clang-format")


class FormatGate(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git("init", "-b", "main")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("config", "user.name", "格式门禁自测")
        self.write(".clang-format", "BasedOnStyle: LLVM\nIndentWidth: 8\nUseTab: Always\n")
        self.write("debt.c", "int   old_debt;\n\n// 未修改分隔\n\nint good;\n")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.root).decode().strip()

    def write(self, path, content):
        (self.root / path).write_text(content)

    def commit(self):
        self.git("add", ".")
        self.git("commit", "-qm", "自测提交")
        return self.git("rev-parse", "HEAD")

    def gate(self, expected, base=None, formatter=None):
        outputs = []
        for language in ("en", "zh_CN"):
            result = subprocess.run(["python3", str(SCRIPT), "--base", base or self.base,
                                     "--clang-format", formatter or FORMATTER,
                                     "--lang=" + language], cwd=self.root,
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
            outputs.append(result.stdout + result.stderr)
        return outputs[0]

    def test_empty_diff(self):
        self.gate(0)

    def test_documentation_only(self):
        self.write("README.md", "文档\n")
        self.commit()
        self.gate(0)

    def test_added_good(self):
        self.write("new file.c", "int good;\n")
        self.commit()
        self.gate(0)

    def test_added_bad(self):
        self.write("new file.c", "int   bad;\n")
        self.commit()
        self.gate(1)

    def test_modified_good_with_old_debt(self):
        self.write("debt.c", "int   old_debt;\n\n// 未修改分隔\n\nint changed;\n")
        self.commit()
        self.gate(0)

    def test_modified_bad(self):
        self.write("debt.c", "int   old_debt;\n\n// 未修改分隔\n\nint   changed;\n")
        self.commit()
        self.gate(1)

    def test_rename_unchanged_with_spaces(self):
        self.git("mv", "debt.c", "renamed file.c")
        self.commit()
        self.gate(0)

    def test_rename_and_edit(self):
        self.git("mv", "debt.c", "renamed file.c")
        self.write("renamed file.c", "int   old_debt;\n\n// 未修改分隔\n\nint   changed;\n")
        self.commit()
        self.gate(1)

    def test_rename_into_source_scope(self):
        self.write("new.txt", "int   bad;\n")
        self.base = self.commit()
        self.git("mv", "new.txt", "new.c")
        self.commit()
        self.gate(1)

    def test_rename_special_path(self):
        self.git("mv", "debt.c", "quote\" tab\tline\n[1].c")
        self.commit()
        self.gate(0)

    def test_deleted_file(self):
        self.git("rm", "debt.c")
        self.commit()
        self.gate(0)

    def test_deleted_lines(self):
        self.write("debt.c", "int   old_debt;\n\n// 未修改分隔\n\n")
        self.commit()
        self.gate(0)

    def test_unicode_byte_offsets(self):
        self.write("unicode.c", "// 中文\nint   bad;\n")
        self.commit()
        self.gate(1)

    def test_no_final_newline(self):
        self.write("nonl.c", "int   bad;")
        self.commit()
        self.gate(1)

    def test_missing_base_fails(self):
        self.gate(2, base="deadbeef")

    def test_all_zero_base_fails(self):
        self.gate(2, base="0" * 40)

    def test_no_common_ancestor_fails(self):
        self.git("checkout", "--orphan", "unrelated")
        self.git("rm", "-rf", ".")
        self.write(".clang-format", "BasedOnStyle: LLVM\n")
        self.write("unrelated.c", "int unrelated;\n")
        self.commit()
        self.gate(2)

    def test_invalid_config_fails_on_empty_diff(self):
        self.write(".clang-format", "NotAClangFormatOption: true\n")
        self.base = self.commit()
        self.gate(2)

    def test_dirty_tracked_tree_fails(self):
        self.write("debt.c", "int changed;\n")
        self.gate(2)

    def test_symlink_source_fails(self):
        (self.root / "link.c").symlink_to("debt.c")
        self.commit()
        self.gate(2)

    def test_wrong_version_fails(self):
        binary = self.root / "fake-format"
        self.write("fake-format", "#!/bin/sh\necho 'clang-format version 17.0.0'\n")
        binary.chmod(0o755)
        self.commit()
        self.gate(2, formatter=str(binary))

    def test_diverged_base_uses_merge_base(self):
        self.git("checkout", "-qb", "feature")
        self.write("new.c", "int good;\n")
        self.commit()
        self.git("checkout", "-q", "main")
        # 目标分支已整理历史格式；若直接比较其最新提交，会错误地
        # 认为功能分支重新引入历史格式问题，从而产生误报。
        self.write("debt.c", "int old_debt;\n\n// 未修改分隔\n\nint good;\n")
        advanced_base = self.commit()
        self.git("checkout", "-q", "feature")
        self.gate(0, base=advanced_base)


if __name__ == "__main__":
    unittest.main(verbosity=2)
