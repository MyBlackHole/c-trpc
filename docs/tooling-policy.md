# 项目工具配置

当前 `.clang-format` 与 `.editorconfig` 是针对本项目重新编写的精简配置，
不再复制操作系统仓库的大型配置或其专用路径规则。

- C/C++：8 列制表符缩进，函数体大括号另起一行，控制语句使用同行大括号，保持已有 include 顺序；80 列为格式化目标。
- Makefile：保留制表符配方。Python/Lua/JSON 使用 4 空格，YAML 使用 2 空格；Markdown 保留有意义的行末空格。
- `.clang-tidy` 的 `FormatStyle: file` 统一读取项目格式，不再在自动修复时另套 4 空格格式。

规则依据 [ClangFormat 官方选项](https://clang.llvm.org/docs/ClangFormatStyleOptions.html)
和 [EditorConfig 官方属性](https://editorconfig.org/) 编写。CI 固定 clang-format
18.1.8，解析配置并对 `include/tr/status.h`、`src/status.c` 执行
`--dry-run --Werror`。这只验证代表文件与配置，不是假称历史全库已统一格式。
其他文件在修改时按需整理，本次不进行全库无关重排。

## 历史来源边界

替换前的两个配置来自 Linux：`.clang-format` 与 v6.10 同名文件逐字节一致，
`.editorconfig` 仅增加 `cpp` 扩展名；当时分别标记 GPL-2.0 与 GPL-2.0-only。
原文件与许可事实仍保留在 Git 历史中。替换当前配置不修改历史许可，也不代表
历史归档或旧版本分发的义务消失，更不构成其余源码授权权利的证明。

本次仅替换工具规则，没有据此新增项目级许可证或统一重标其他文件。
