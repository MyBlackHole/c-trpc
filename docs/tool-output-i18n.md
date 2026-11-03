# 工具输出国际化

当前覆盖 `scripts/check_source_format.py` 与 `scripts/check_compile_commands.py`
的项目自有帮助、错误上下文与摘要，
以及翻译目录维护入口。文档和源码注释保持中文；运行时以完整英文句子为 gettext
msgid，简体中文维护在 `locales/zh_CN/LC_MESSAGES/c-trpc-tools.po`，编译后的 MO
随仓库提供。缺译、目录缺失或损坏时回退英文，不改变检查结论和退出码。

## 选择语言

```sh
python3 scripts/check_source_format.py --lang=zh_CN --base origin/main
CTRPC_LANG=en python3 scripts/check_source_format.py --base origin/main
```

- `--lang=auto|en|zh_CN` 优先于 `CTRPC_LANG`；显式 `auto` 也覆盖仓库环境变量。
- 未指定选项时读取 `CTRPC_LANG`，未设置或空值时为 `auto`。未知环境变量值回退英文；
  非法显式选项以用法错误退出（2）。
- `auto` 按首个非空 `LC_ALL`、`LC_MESSAGES`、`LANG` 判定；没有值视作 `C`。
  `C`、`POSIX`、`C.UTF-8` 保持英文，忽略 `LANGUAGE`。
- 其他 locale 下，若设置 `LANGUAGE`，将其冒号分隔偏好链交给 gettext 的标准目录
  查找与回退；否则使用有效 locale。只有现有目录才能提供翻译，未知 locale 回退英文。
  当前未提供英语 MO，因此 `LANGUAGE=en:zh_CN` 会继续找到简中目录；要强制英语，
  使用 `--lang=en`。不自制区域别名映射，也不把繁体中文区域猜测成简中。
- 目录路径相对于脚本文件定位，与当前工作目录无关；源码目录路径中可含空格。

Python 使用标准库 `gettext.translation()` 对象直接读取 MO，不要求操作系统安装
`zh_CN` locale；原生 C gettext 程序通常需要系统提供有效消息 locale，不能把本 Python
工具的无系统 locale 保证套用于原生程序。此 Python 实现不调用 `setlocale()`、不修改 `os.environ`，也不把 `_` 注入 builtins
或替换 `argparse._`。本阶段 **没有** 国际化 argparse 上游自带的 `usage`、`options`、
`-h` 说明以及缺参数、未知参数等内置诊断。Git、clang-format、操作系统异常的原始
文本保持原样，仅翻译项目错误上下文。不能把本阶段描述为全 CLI 或全仓输出已完成。

退出码仍为：成功 0、修改行格式不合格 1、工具/配置/调用错误 2。
格式工具固定 **18.1.8**，原有 22 个格式判定 fixture 分别在英文与简中运行。
人读摘要不是 JSON/JUnit/CSV 等机器接口；本次未变更协议、机器字段、C ABI 或核心库依赖。

## 维护目录

运行工具只依赖 Python 标准库。新增语言时新增标准 PO/MO 目录，不在检查业务逻辑中
添加语言分支。本阶段选项只公开英文与简中；公开其他语言应同步支持列表和测试。
开发与 CI 生成目录需要 GNU gettext 的 `msgfmt`（可用 `MSGFMT` 指定可执行文件）。

```sh
python3 scripts/compile_tool_catalog.py
python3 scripts/compile_tool_catalog.py --check
python3 scripts/test_tool_i18n.py
python3 scripts/test_source_format.py
```

生成入口调用 `msgfmt --check --check-format --no-hash`，再用 Python gettext 读取生成的
MO，补充检查占位符名称、次数、转换（如 `!r`）和格式规格完全对应。GNU msgfmt 原生
负责 PO 语法与编译，不包含自制 PO 解析器。固定头部时间，不写入当前时间；`--check`
逐字节比较重建结果。gettext 缺失、编译失败或目录不一致会失败，不跳过校验。

PO 中每条格式消息保留 `python-brace-format` 标记。不要翻译 `{path!r}`、`{base}` 等
占位符；命令行参数、机器字段、状态码和第三方原始信息保持不变。英语直接使用 msgid，
不需要英语 MO。更改工具消息时同时维护 PO/MO，并运行语言回归与原判定 fixture。

官方参考：
- [Python gettext：类接口](https://docs.python.org/3/library/gettext.html#class-based-api)
- [GNU gettext：LANGUAGE 选择](https://www.gnu.org/software/gettext/manual/html_node/The-LANGUAGE-variable.html)
- [GNU gettext：msgfmt](https://www.gnu.org/software/gettext/manual/html_node/msgfmt-Invocation.html)
