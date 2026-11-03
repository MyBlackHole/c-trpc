"""项目工具的人读文本国际化；不修改进程环境或全局 locale。"""
import argparse
import gettext
import os
import struct
from pathlib import Path

LANGUAGES = ("auto", "en", "zh_CN")
LOCALE_DIR = Path(__file__).resolve().parent.parent / "locales"
DOMAIN = "c-trpc-tools"
ENVIRONMENT_VARIABLE = "CTRPC_LANG"


def resolve_languages(explicit=None, environ=None):
    environ = os.environ if environ is None else environ
    if explicit is not None and explicit not in LANGUAGES:
        raise ValueError("invalid explicit language")
    selected = explicit if explicit is not None else (environ.get(ENVIRONMENT_VARIABLE) or "auto")
    if selected != "auto":
        return [selected if selected in LANGUAGES else "en"]
    effective = next((environ[key] for key in ("LC_ALL", "LC_MESSAGES", "LANG")
                      if environ.get(key)), "C")
    if effective.split(".", 1)[0].split("@", 1)[0].upper() in ("C", "POSIX"):
        return ["en"]
    # 保留 GNU LANGUAGE 目录查找链；不自行实现地区别名或目录回退。
    return environ.get("LANGUAGE", "").split(":") if environ.get("LANGUAGE") else [effective]


def translation(languages, localedir=None):
    """目录缺失或损坏时安全返回英文 msgid。"""
    if isinstance(languages, str):
        languages = [languages]
    try:
        return gettext.translation(DOMAIN, localedir=localedir or LOCALE_DIR,
                                   languages=languages, fallback=True)
    except (OSError, EOFError, ValueError, UnicodeError, LookupError, struct.error):
        return gettext.NullTranslations()


def parser_for(description, argv=None):
    """先解析语言，再建立项目帮助；argparse 自带诊断仍由上游负责。"""
    preliminary = argparse.ArgumentParser(add_help=False)
    preliminary.add_argument("--lang")
    language_args, _ = preliminary.parse_known_args(argv)
    selected = language_args.lang
    invalid = selected is not None and selected not in LANGUAGES
    languages = resolve_languages(None if invalid else selected)
    translate = translation(languages).gettext
    parser = argparse.ArgumentParser(description=translate(description))
    parser.add_argument("--lang", choices=LANGUAGES,
                        help=translate("Human-readable output language (default: auto)"))
    if invalid:
        parser.error(translate("Invalid --lang value: {value}; expected auto, en or zh_CN").format(value=selected))
    return parser, translate
