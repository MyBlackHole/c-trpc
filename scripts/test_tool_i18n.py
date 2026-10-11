#!/usr/bin/env python3
"""工具语言选择、目录回退与命令行的独立回归测试。"""
import ast
from io import BytesIO
import gettext
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from compile_tool_catalog import compile_catalog
import tool_i18n as i18n

SCRIPT = Path(__file__).with_name("check_source_format.py").resolve()


class LanguageTests(unittest.TestCase):
    def test_precedence(self):
        key = i18n.ENVIRONMENT_VARIABLE
        for explicit, env, expected in [
            ("en", {key: "zh_CN", "LANG": "zh_CN.UTF-8"}, "en"),
            ("zh_CN", {"LC_ALL": "C"}, "zh_CN"),
            ("auto", {key: "zh_CN", "LANG": "en_US.UTF-8"}, "en_US.UTF-8"),
            (None, {key: "zh_CN", "LC_ALL": "C"}, "zh_CN"),
            (None, {key: "garbage", "LANG": "zh_CN.UTF-8"}, "en"),
            (None, {key: "", "LANG": "zh_CN.UTF-8"}, "zh_CN.UTF-8"),
            (None, {"LC_ALL": "en_US.UTF-8", "LC_MESSAGES": "zh_CN", "LANG": "zh_CN"}, "en_US.UTF-8"),
            (None, {"LC_MESSAGES": "zh_CN.UTF-8", "LANG": "en_US"}, "zh_CN.UTF-8"),
            (None, {"LANG": "zh_CN.UTF-8"}, "zh_CN.UTF-8"),
            (None, {"LANG": "not_installed"}, "not_installed"),
            (None, {}, "en"),
            (None, {"LANG": "en_US.UTF-8", "LANGUAGE": "fr:zh_CN:en"}, "fr:zh_CN:en"),
            (None, {"LANG": "zh_CN.UTF-8", "LANGUAGE": "en:zh_CN"}, "en:zh_CN"),
        ]:
            with self.subTest(explicit=explicit, env=env):
                self.assertEqual(i18n.resolve_languages(explicit, env), expected.split(":"))
        for locale in ("C", "POSIX", "C.UTF-8", "C.utf8"):
            self.assertEqual(i18n.resolve_languages(environ={"LC_ALL": locale, "LANGUAGE": "zh_CN"}), ["en"])
        with self.assertRaises(ValueError):
            i18n.resolve_languages("fr")

    def test_gnu_language_catalog_chain(self):
        message = "Human-readable output language (default: auto)"
        for preference in ("fr:zh_CN", "en:zh_CN", "zh_CN:en"):
            languages = i18n.resolve_languages(environ={"LANG": "en_US.UTF-8", "LANGUAGE": preference})
            self.assertIn("人读", i18n.translation(languages).gettext(message))
        for preference in ("C:zh_CN", "en", "unknown"):
            languages = i18n.resolve_languages(environ={"LANG": "zh_CN.UTF-8", "LANGUAGE": preference})
            self.assertEqual(i18n.translation(languages).gettext(message), message)

    def test_no_global_mutation_or_system_locale_dependency(self):
        before = dict(os.environ)
        with patch("locale.setlocale", side_effect=AssertionError("global locale changed")):
            self.assertEqual(i18n.resolve_languages(environ={"LANG": "zh_CN.UTF-8"}), ["zh_CN.UTF-8"])
            self.assertIn("人读", i18n.translation("zh_CN").gettext("Human-readable output language (default: auto)"))
        self.assertEqual(dict(os.environ), before)

    def test_locale_environment_cli(self):
        clean = {key: value for key, value in os.environ.items()
                 if key not in ("LC_ALL", "LC_MESSAGES", "LANG", "LANGUAGE", i18n.ENVIRONMENT_VARIABLE)}
        for env, expected in [({}, "Check changed C-family"),
                              ({"LANG": "C", "LANGUAGE": "zh_CN"}, "Check changed C-family"),
                              ({"LANG": "C.UTF-8", "LANGUAGE": "zh_CN"}, "Check changed C-family"),
                              ({"LANG": "unknown"}, "Check changed C-family"),
                              ({"LANG": "zh_CN.UTF-8"}, "检查已提交"),
                              ({i18n.ENVIRONMENT_VARIABLE: "zh_CN", "LANG": "C"}, "检查已提交")]:
            with self.subTest(env=env):
                result = subprocess.run([sys.executable, str(SCRIPT), "--help"],
                                        env={**clean, **env}, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(expected, result.stdout)

    def test_catalog_fallback(self):
        self.assertEqual(i18n.translation("zh_CN").gettext("Untranslated message"), "Untranslated message")
        with tempfile.TemporaryDirectory(prefix="catalog with spaces ") as tmp:
            self.assertEqual(i18n.translation("zh_CN", tmp).gettext("English"), "English")
            mo = Path(tmp) / ("zh_CN/LC_MESSAGES/" + i18n.DOMAIN + ".mo")
            mo.parent.mkdir(parents=True)
            original = (i18n.LOCALE_DIR / "zh_CN/LC_MESSAGES" / (i18n.DOMAIN + ".mo")).read_bytes()
            for data in (b"broken", b"\xde\x12\x04\x95" + b"\0" * 16,
                         original.replace(b"UTF-8", b"XXXXX")):
                mo.write_bytes(data)
                gettext._translations.clear()
                self.assertEqual(i18n.translation("zh_CN", tmp).gettext("English"), "English")

    def test_cli_and_script_relative_catalog(self):
        with tempfile.TemporaryDirectory(prefix="tool path with spaces ") as tmp:
            root = Path(tmp) / "scripts"
            root.mkdir()
            for name in ("check_source_format.py", "tool_i18n.py"):
                shutil.copy2(SCRIPT.with_name(name), root / name)
            shutil.copytree(i18n.LOCALE_DIR, root.parent / "locales")
            for lang, expected in (("en", "Check changed C-family"), ("zh_CN", "检查已提交")):
                result = subprocess.run([sys.executable, str(root / SCRIPT.name), "--lang=" + lang, "--help"], cwd="/", capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(expected, result.stdout)
            result = subprocess.run([sys.executable, str(root / SCRIPT.name), "--lang=invalid"], cwd="/", capture_output=True, text=True, env={**os.environ, i18n.ENVIRONMENT_VARIABLE: "en"})
            self.assertEqual(result.returncode, 2)
            self.assertIn("Invalid --lang value", result.stderr)


class CatalogTests(unittest.TestCase):
    def test_complete_project_messages(self):
        translator = i18n.translation("zh_CN")
        for filename in ("check_source_format.py", "tool_i18n.py", "compile_tool_catalog.py"):
            tree = ast.parse(SCRIPT.with_name(filename).read_text())
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                        and node.func.id in ("_", "translate", "parser_for") and node.args
                        and isinstance(node.args[0], ast.Constant)
                        and isinstance(node.args[0].value, str)):
                    message = node.args[0].value
                    self.assertNotEqual(translator.gettext(message), message, message)

    def test_reproducible(self):
        source = i18n.LOCALE_DIR / ("zh_CN/LC_MESSAGES/" + i18n.DOMAIN + ".po")
        self.assertEqual(compile_catalog(source), source.with_suffix(".mo").read_bytes())
        self.assertEqual(compile_catalog(source), compile_catalog(source))

    def test_invalid_catalogs_rejected(self):
        header = (i18n.LOCALE_DIR / ('zh_CN/LC_MESSAGES/' + i18n.DOMAIN + '.po')).read_text().split('\n\n')[0] + '\n\n'
        cases = [
            '#, python-brace-format\nmsgid "Hello {name}"\nmsgstr "你好 {wrong}"\n',
            '#, python-brace-format\nmsgid "Hello {name} {name}"\nmsgstr "你好 {name}"\n',
            '#, python-brace-format\nmsgid "Value {value:.2f}"\nmsgstr "值 {value:.3f}"\n',
            '#, python-brace-format\nmsgid "Hello {name!r}"\nmsgstr "你好 {name}"\n',
            '#, python-brace-format\nmsgid "Hello {name}"\nmsgstr "你好 {"\n',
            'msgid "unterminated\nmsgstr "文本"\n',
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.po"
            for value in cases:
                with self.subTest(value=value):
                    path.write_text(header + value)
                    with self.assertRaises((ValueError, subprocess.CalledProcessError)):
                        compile_catalog(path)

    def test_escaped_and_missing_translation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "escaped.po"
            path.write_text('msgid ""\nmsgstr "Content-Type: text/plain; charset=UTF-8\\n"\n\nmsgid "Quote \\\" and line\\n {name}"\nmsgstr "引号 \\\" 和行\\n {name}"\n\nmsgid "Missing"\nmsgstr ""\n')
            translator = gettext.GNUTranslations(BytesIO(compile_catalog(path)))
            self.assertEqual(translator.gettext('Quote " and line\n {name}'), '引号 " 和行\n {name}')
            self.assertEqual(translator.gettext("Missing"), "Missing")


if __name__ == "__main__":
    unittest.main(verbosity=2)
