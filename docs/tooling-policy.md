# 项目工具配置

当前 `.clang-format` 与 `.editorconfig` 是针对本项目重新编写的精简配置，
不再复制操作系统仓库的大型配置或其专用路径规则。

- C/C++：8 列制表符缩进，函数体大括号另起一行，控制语句使用同行大括号，保持已有 include 顺序；80 列为格式化目标。
- Makefile：保留制表符配方。Python/Lua/JSON 使用 4 空格，YAML 使用 2 空格；Markdown 保留有意义的行末空格。
- `.clang-tidy` 的 `FormatStyle: file` 统一读取项目格式，不再在自动修复时另套 4 空格格式。

## 新增和修改源码的格式门禁

`修改源码格式检查` 工作流固定使用 clang-format **18.1.8**，独立于日志文本
formatter 功能测试。它在普通 `pull_request`（包括 fork）和 main push 运行，只有
`contents: read` 权限，不使用 `pull_request_target`，不保留 checkout 凭据。

- 检出 PR 的实际 head SHA，并获取完整历史；比较 `merge-base(base SHA, head SHA)`
  到 head，不使用 GitHub 的临时 merge commit。push 使用事件的 before/head SHA。
  缺失、全零、无共同祖先或浅历史不足均报错，不能退回 HEAD 或假称通过。
- 覆盖 Git 已跟踪的 `.c/.h/.cc/.cpp/.cxx/.hh/.hpp/.hxx`，包括源码、头、测试、示例。
  新文件及从非 C-family 扩展名改名进入该范围的文件全检；修改文件只要求新侧
  新增/替换行符合格式。删除行、删除文件、纯改名和空 diff 无待检行，正常通过。
  NUL 分隔路径支持空格、引号、换行等；源码符号链接显式拒绝。
- clang-format 产生字节偏移 replacement，再与修改行相交；跨行空白同时涉及
  后一行缩进。可能需要整理与改动直接相邻的空白，但不要求清理无关历史文件。
  不写回文件、不全库重排、不以格式通过证明语义正确。模板 `.h.in` 不当成 C 头。
- 配置在空 diff 时也解析；版本不符、formatter 错误或 incomplete format 都失败。
  本地检查必须检出 head 且已提交修改，保证读取该版本的格式配置。新增忽略规则、
  `clang-format off` 或格式配置变更仍需人工审查，不能用来掩盖新增代码的格式问题。

本地复现（先提交修改，使用当前 PR 的目标分支引用或 base SHA）：

```sh
python3 -m venv /tmp/source-format-venv
/tmp/source-format-venv/bin/pip install 'clang-format==18.1.8'
export PATH="/tmp/source-format-venv/bin:$PATH"
python3 scripts/test_source_format.py
python3 scripts/check_source_format.py --base origin/main --head HEAD
```

自测在独立临时 Git 仓库覆盖合格/违规的新文件和修改行、历史债务保留、分叉后的
merge-base、空 diff、仅文档、删除、改名及特殊路径、UTF-8 字节偏移、无末尾换行、
缺失基准、脏工作区、错误版本和源码符号链接。它是门禁本身的测试，不是产品测试。
历史排版债务应使用独立纯格式 PR 渐进处理，不能与运行时修复混成整库格式化。

## clang-tidy 的实际覆盖边界

`.clang-tidy` 是供后续试运行的 **C 语言可选配置**，不是已接入的 CI 安全门。
`FormatStyle: file` 复用 `.clang-format`；仅保留 clang 编译诊断与 analyzer 的
core/unix/security 范围，移除 C++ modernize/google/performance 等宽泛选择；
项目头过滤范围明确为 include/src 下的 `.h`。WarningsAsErrors 暂为空，不把未经
建立基线的告警直接升为阻断。insecureAPI 家族沿用排除，须人工复核 API 契约。

本轮没有运行 clang-tidy，配置存在、格式门禁通过均不表示静态分析通过。
可靠接入前必须固定 analyzer 版本、生成真实 compile_commands.json、列明生产/测试
宏与平台组合、验证选中的检查及头文件覆盖，并为抑制项给出理由。禁止自动 `--fix`
制造大面积改动；格式工具也不证明线程安全、ownership、ABI 或所有故障路径正确。

## 真实编译数据库：仅完成分析输入准备

`python3 scripts/check_compile_commands.py` 使用 Xmake 原生
`xmake project -k compile_commands --lsp=clangd -t trcore <目录>` 导出数据库，不手写
`compile_commands.json`，不添加分析专用宏或替换编译器。运行需要 Python 3.9+
及已经配置的 Xmake；CI 仍固定 Xmake 3.1.1。最小复现：

```sh
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n
python3 scripts/check_compile_commands.py \
    --output-dir build/compdb/gcc-release --mode release \
    --toolchain gcc --crc32c-portable n --arch x86_64
# 在相同检出、工具链和配置下复查已有产物；不会重新导出。
python3 scripts/check_compile_commands.py \
    --output-dir build/compdb/gcc-release --mode release \
    --toolchain gcc --crc32c-portable n --arch x86_64 --check-only
python3 scripts/test_compile_commands.py -v
```

可用 `--xmake /路径/xmake` 或环境变量 `XMAKE` 指定单个可执行文件，
`--lang` 沿用[工具输出国际化](tool-output-i18n.md)。脚本不重新配置项目。
切换模式、编译器、CRC 后端或参数后必须用新配置导出，不能只改目录标签。
同一工作树不要并发执行重新配置与导出。

校验边界：

- 生产范围是当前 Linux 构建的 `trcore`，独立比较磁盘 `src/**/*.c`、
  Xmake 已解析的 `trcore:sourcebatches()` 和数据库条目，拒绝缺失、重复及
  额外生产条目。不硬编码源文件名单或数量；正常增删源码与 Xmake 定义即可。
  当前要求全部 `src` C 文件参加该目标；未来引入平台条件源或生成源时，必须
  有意识地更新覆盖策略。这不是通用 Xmake 项目校验器。
- 从 Xmake `compiler.compargv(..., rawargs=true)` 读取各源文件真实命令，
  逐项比较 `arguments`，包含编译器、对象路径、宏、包含路径与全部参数顺序；
  保留空格、引号和参数边界，不经命令行字符串重新拆分；显式选择原生
  `--lsp=clangd` 以保留 `-isystem`，避免默认导出将其改写为 `-I`。
- 另行检查 C11、项目 `include` 路径、`-pthread`、直接警告开关，以及直接
  `-D/-U` 命令行宏中的 `TR_CRC32C_FORCE_PORTABLE` 与选项相符、`NDEBUG`
  保持未定义（包括函数式宏定义）。当前基线拒绝 `-w` 与 `-Wno-*` 关闭、抑制或降级
  警告；若未来确需例外，应单独评审，而不是借额外参数绕过基线。
  不凭两个同源列表相等宣称覆盖完备，也不凭出现一个正确 flag 忽略后续覆盖。
  内部头的相对包含按真实命令保留，不人为补 `-Isrc` 或 feature-test 宏。
- Xmake 3.1.1 原生导出即使选择 `trcore`，也会附带注册测试条目；文件保持
  原样，测试条目不能替代生产条目。未来分析仍须明确选择 `src` 翻译单元，
  不能把整份数据库视为已验证的全目标分析输入。示例、基准和测试不计入这里的
  生产覆盖；测试的 `-UNDEBUG` 与链接期 `--wrap` 不混入生产命令。
- 同目录 `configuration.json` 记录 Git 提交、`xmake.lua` 摘要、Xmake 版本、
  公开配置（平台/架构/模式/工具链/CRC/额外参数等）、编译器路径与版本首行、
  数据库摘要及条目数量。复查时与当前配置比对，拒绝旧配置或文件混配。
  这些是配置身份记录，不是源码完整性签名、工具链供应链证明或编译语义证明。
  数据库含本机绝对路径，CI 归档用于诊断，换机器应重新导出。

CI 在既有 GCC debug/release、Clang release 作业内校验 Linux/x86_64 自动
CRC 配置，保留原有检查名称；CRC 专测另校验 GCC release 的可移植宏配置。
GCC 作业内运行从真实导出派生的正负例，包含实际 debug/portable 切换、
含空格和单引号的检出/包含路径与字符串宏，以及缺漏、重复、错误参数、错误配置、
摘要不匹配与失败退出码。消毒器数据库可按各自模式单独导出，此处不扩充其 CI 矩阵。

**本项只是 issue #154 P1-4 / D1 的前置输入准备，不标记 D1 完成。**
没有运行 clang-tidy、Clang analyzer 或 GCC `-fanalyzer`，没有固定分析器版本、
建立告警基线、验证所选分析检查或头文件分析覆盖。命令匹配也不证明预处理后
宏状态：`-Wp,...`、`@response`、强制包含头等转交/预处理途径未展开。
也不证明所有头文件独立可编译、运行时正确性或跨架构可移植性；仍以真实 Xmake
构建/测试及后续单独验收为准。未改变 `.clang-tidy` 的可选状态或告警升级策略。

数据库字段遵循 [Clang 编译数据库规范](https://clang.llvm.org/docs/JSONCompilationDatabase.html)。

## 历史来源边界

替换前的两个配置来自 Linux：`.clang-format` 与 v6.10 同名文件逐字节一致，
`.editorconfig` 仅增加 `cpp` 扩展名；当时分别标记 GPL-2.0 与 GPL-2.0-only。
原文件与许可事实仍保留在 Git 历史中。替换当前配置不修改历史许可，也不代表
历史归档或旧版本分发的义务消失，更不构成其余源码授权权利的证明。

配置替换本身没有据此统一重标其他文件。后续维护者单独确认项目自有内容的授权，
当前许可见[许可与来源范围](licensing.md)；历史第三方文件仍保留上述许可。

工具人读帮助与诊断的语言选择、翻译目录维护及当前覆盖边界见
[工具输出国际化](tool-output-i18n.md)。
