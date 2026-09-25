# Xmake 构建与测试

## 环境与唯一构建入口

项目要求 Linux、GCC 或 Clang，以及 Xmake 3.1.1 或更新版本。
CI 固定使用 3.1.1；安装方式见 [Xmake 官方文档](https://xmake.io/guide/quick-start.html)。
库本身不新增第三方包依赖，语言基线仍为 ISO C11，并使用 GCC/Clang
支持的 cleanup attribute。编译保持 `-Wall -Wextra -Werror -pedantic`
和 `-pthread`，链接也显式使用 `-pthread`。

`xmake.lua` 是唯一构建定义。Makefile 仅转发命令，不保留源码列表、
目标文件规则或第二套编译参数。新增/删除源码与目标只修改 Xmake 配置。

## 常用命令

```sh
xmake f -m release --toolchain=gcc
xmake                         # 默认构建静态库和两个示例
xmake build --all             # 同时构建两个测试程序
xmake test -v -j1             # 自动构建并执行全部测试
xmake test -v -j1 'test_timer_queue/*'
```

| 目标 | 类型 | 默认构建 |
|---|---|---|
| `trcore` | 静态库 `libtrcore.a` | 是 |
| `echo_server` / `echo_client` | 独立可执行程序 | 是 |
| `test_transport` | Transport/RPC 集成测试 | 否，由 `xmake test` 或 `build --all` 构建 |
| `test_timer_queue` | 定时器生命周期与模型测试 | 否，由 `xmake test` 或 `build --all` 构建 |

每个测试目标注册一个 `default` 测试。通配选择器使用引号，避免由 shell
展开。`-j1` 串行执行这两个测试程序，不改变它们内部的并发测试行为。
测试实时输出，每个测试程序的执行超时为 120 秒；失败直接返回非零状态。

输出目录为 `build/<platform>/<architecture>/<mode>/`，例如
`build/linux/x86_64/release/`。`.xmake/` 保存本地配置/缓存。
旧 Make 生成的源码目录内 `.o` / `.a` 或测试二进制不会被新构建引用。
Xmake 跟踪头文件依赖，不再手工维护 Make 中的内部头文件依赖表。

## 模式与编译器切换

| 模式 | 优化/调试信息 | 检查 |
|---|---|---|
| `debug` | `-O0 -g` | 保留断言 |
| `release`（默认） | `-O2 -g` | 保留断言，与原 Make 基线一致 |
| `asan` | `-O1 -g`、保留 frame pointer | AddressSanitizer + UndefinedBehaviorSanitizer |
| `tsan` | `-O1 -g`、保留 frame pointer | ThreadSanitizer |

这里没有使用会额外定义 `NDEBUG` 的 release 规则。测试还显式添加
`-UNDEBUG`，因为原有测试在 `assert()` 内包含函数调用，不能删掉这些操作。
不要将“release 构建成功”误当成测试执行成功。

```sh
# -c 清除已缓存的配置后重新配置；不会删除源码。
xmake f -c -y -m debug --toolchain=clang
xmake build --all
xmake test -v -j1

# 切回 GCC release
xmake f -c -y -m release --toolchain=gcc
xmake test -v -j1
```

不同模式的构建产物分目录保存。切换编译器也应明确重新配置；不要直接
运行上一次构建目录中的旧二进制来判断新配置是否有效。

## Sanitizer 验证

```sh
xmake f -c -y -m asan --toolchain=gcc
xmake build --all
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
    xmake test -v -j1

xmake f -c -y -m tsan --toolchain=gcc
xmake build --all
TSAN_OPTIONS=halt_on_error=1:second_deadlock_stack=1 \
    xmake test -v -j1
```

Sanitizer 通过 Xmake 内建策略同时作用于库、示例和测试的编译/链接，
不是只给最后的测试可执行文件加链接参数。ASan 与 TSan 不混在同一模式。
CI 不吞掉 sanitizer 失败，也不将不支持的运行环境报告为测试通过。

## 示例与 SDK 安装

先构建，在两个终端分别运行：

```sh
xmake run echo_server 9000
xmake run echo_client 127.0.0.1 9000 hello
```

安装静态库与公开头文件：

```sh
xmake f -c -y -m release --toolchain=gcc
xmake build trcore
xmake install -o /tmp/c-trpc-sdk trcore
```

安装后保留 `include/tr/*.h` 的路径，静态库位于 `lib/libtrcore.a`。
外部 C 程序使用 `-I/tmp/c-trpc-sdk/include -L/tmp/c-trpc-sdk/lib -ltrcore`
并在编译/链接时加 `-pthread`。CI 将 SDK 安装到带空格的路径，使用 GCC
和 Clang 分别单独编译每个已安装公开头文件，并链接、运行外部 C11 程序，
实际执行 Reactor 的 create / start / quiesce / stop / destroy 生命周期。
消费程序只使用安装后的 include/lib 路径，运行超时 10 秒；另有独立
Echo 服务端/客户端验证请求响应，不以仅调用 status helper 代替运行时验证。

## Make 兼容入口

```sh
make                          # xmake 配置后 build --all
make test CC=gcc
make test-timer CC=clang MODE=debug
make clean                    # xmake clean --all
```

`CC`、`AR` 只有被显式提供时才转发，避免 GNU Make 默认的 `CC=cc` 覆盖
Xmake 工具链。`CPPFLAGS` / `CFLAGS` 合并为额外 C flags，`LDFLAGS` 作为
额外链接 flags；还可通过 `XMAKE_CONFIG` 添加 Xmake 配置选项。
`XMAKE` 指定单个 Xmake 可执行文件路径（可以含空格或引号），不是带参数
的命令片段；`MODE` 默认 `release`。工具路径和 flags 值在 Make recipe
中作为单个 shell 参数传递，flags 内部的引号交给 Xmake 解析，不由 shell
提前拆分或展开。`XMAKE_CONFIG` 则仍是多个配置选项组成的 shell 片段，
由调用者自行引用其中的路径。

兼容入口使用两个内部字符串选项 `make_cflags` / `make_ldflags` 保存 flags。
Xmake 3.1.1 的原生 `--cflags` / `--ldflags` 会再次拆分包含空格的参数；
仅修复 shell 引用不能保住已解析的路径边界。因此 `xmake.lua` 使用原生
`os.argv` 解析一次，再以一个强制保留的参数组传给编译器/链接器，不自行
实现解析器。Make 配置时清空原生 flags，避免旧配置中的同一组 flags 重复生效。
直接切回原生 Xmake 使用时，可用 `xmake f -c ...` 清除兼容选项。

例如，包含空格的 include 路径与相对运行时搜索路径：

```sh
make all CC=gcc CFLAGS='-I"/opt/vendor sdk/include"' \
    LDFLAGS='-Wl,-rpath,$$ORIGIN/../lib'
```

通过 GNU Make 传递字面量 `$` 时写成 `$$`，并使用 shell 单引号保护命令行
值。这里的 `$$ORIGIN` 经 Make 后变成 `$ORIGIN`，不会被 recipe shell
作为环境变量提前展开。直接调用 `xmake f` 时没有 Make 这一层，不要照搬
双 `$` 写法。

这些入口都要求已安装 Xmake。旧的单个 `.o` / `libtrcore.a` Make 目标
不再提供，应使用 `xmake build <target>`；没有无 Xmake 的备用 Make 构建。
`make clean` 清理 Xmake 产物，不负责删除迁移前残留在源码目录中的旧产物。

## CI 范围

五项检查为 GCC（debug + release）、Clang（debug + release）、ASan + UBSan、
TSan，以及 Make 兼容/编译器切换/SDK 安装/独立进程示例。
前四项均构建全部目标并通过 `xmake test` 执行两个测试程序。
Make 兼容检查还使用带空格/单引号的工具与强制包含头文件路径、带引号的
字符串宏，实际构建全部目标并运行测试；通过 `readelf` 验证最终 ELF
保留字面量 `$ORIGIN`，不只检查配置命令的显示文本。
构建产物和缓存被 `.gitignore` 排除，安装检查还核对工作树没有新增文件。

Smoke 检查的构建、安装和示例日志作为 CI artifact 保留 7 天；日志截取仅
限制控制台输出，不改变测试退出码，完整日志仍可用于复现失败。
