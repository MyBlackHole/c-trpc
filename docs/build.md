# Xmake 构建与测试

## 环境与唯一构建入口

项目要求 Linux、GCC 或 Clang、OpenSSL 3.0 或更新版本的开发文件，以及
Xmake 3.1.1 或更新版本。Ubuntu 22.04 可安装 `libssl-dev`。
CI 固定使用 3.1.1；安装方式见 [Xmake 官方文档](https://xmake.io/guide/quick-start.html)。
消息变换后端通过 Xmake 引入静态 Zstandard 1.5.7；网络压缩仍未接入，
新增编译依赖不表示默认启用压缩。语言基线仍为 ISO C11，并使用 GCC/Clang
支持的 `cleanup` 属性。编译保持 `-Wall -Wextra -Werror -pedantic`
和 `-pthread`，链接也显式使用 `-pthread`。

`xmake.lua` 是唯一构建定义。Makefile 仅转发命令，不保留源码列表、
目标文件规则或第二套编译参数。新增或删除源码与目标时只修改 Xmake 配置。

## 常用命令

```sh
xmake f -m release --toolchain=gcc
xmake                         # 默认构建静态库和两个示例
xmake build --all             # 同时构建全部测试和基准目标
xmake test -v -j1             # 自动构建并执行全部测试
xmake test -v -j1 'test_timer_queue/*'
xmake test -v -j1 'test_runtime_threads/*'
```

默认构建 `trcore` 静态库和 `echo_server` / `echo_client`。
测试按基础原语、传输/RPC 集成、故障注入、生命周期、并发及期限边界组织，
测试与基准目标的完整清单以 `xmake.lua` 为单一来源，不在此手工维护数量。

每个测试目标注册一个 `default` 测试。通配选择器使用引号，避免由命令行解释器展开。
`-j1` 串行执行全部注册测试，不改变它们内部的并发测试行为。
测试实时输出，每个测试程序的执行超时为 120 秒；失败直接返回非零状态。

`test_runtime_threads` 仅在自身链接时包装 `pthread_create` / `pthread_join`，
记录库的线程增量并在指定位置返回 `EAGAIN`，验证正常销毁和七个启动失败点。
不使用进程线程总数或睡眠采样，不给 `trcore`、示例或安装后的 SDK 添加
测试钩子；GNU ld / 兼容链接器的 `--wrap` 支持仅由该测试目标使用。

输出目录为 `build/<platform>/<architecture>/<mode>/`，例如
`build/linux/x86_64/release/`。`.xmake/` 保存本地配置/缓存。
旧 Make 生成的源码目录内 `.o` / `.a` 或测试二进制不会被新构建引用。
Xmake 跟踪头文件依赖，不再手工维护 Make 中的内部头文件依赖表。

## 编译数据库

真实编译数据库使用 Xmake 原生导出，并校验当前 `trcore` 的源文件覆盖、
C11/包含路径/宏与配置身份。入口、配置范围及复现命令见
[编译数据库准备](tooling-policy.md#真实编译数据库仅完成分析输入准备)。
这一步不运行静态分析器，也不替代构建或测试。

## 模式与编译器切换

| 模式 | 优化/调试信息 | 检查 |
|---|---|---|
| `debug` | `-O0 -g` | 保留断言 |
| `release`（默认） | `-O2 -g` | 保留断言，与原 Make 基线一致 |
| `asan` | `-O1 -g`、保留帧指针 | AddressSanitizer + UndefinedBehaviorSanitizer |
| `tsan` | `-O1 -g`、保留帧指针 | ThreadSanitizer |

这里没有使用会额外定义 `NDEBUG` 的 `release` 规则。测试还显式添加
`-UNDEBUG`，因为原有测试在 `assert()` 内包含函数调用，不能删掉这些操作。
不要将“`release` 构建成功”误当成测试执行成功。

```sh
# -c 清除已缓存的配置后重新配置；不会删除源码。
xmake f -c -y -m debug --toolchain=clang
xmake build --all
xmake test -v -j1

# 切回 GCC 的 release 模式
xmake f -c -y -m release --toolchain=gcc
xmake test -v -j1
```

不同模式的构建产物分目录保存。切换编译器也应明确重新配置；
不要直接运行上一次构建目录中的旧二进制来判断新配置是否有效。

## 内存/线程检查器验证

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

各类检查器通过 Xmake 内建策略同时作用于库、示例和测试的编译/链接，
不是只给最后的测试可执行文件增加链接参数。ASan 与 TSan 不混在同一模式。
CI 不吞掉检查器失败，也不把不支持的运行环境报告为测试通过。

## 示例与 SDK 安装

先构建，在两个终端分别运行：

```sh
xmake run echo_server 9000
xmake run echo_client 127.0.0.1 9000 hello
```

Echo 客户端人读诊断支持英语和简体中文，选择方式见 [Echo 客户端诊断语言](echo-client-i18n.md)。

安装静态库与公开头文件：

```sh
xmake f -c -y -m release --toolchain=gcc
xmake build trcore
xmake install -o /tmp/c-trpc-sdk trcore
```

安装后保留 `include/tr/*.h` 的路径，静态库位于 `lib/libtrcore.a`。
`LICENSE`、`NOTICE` 和 `licensing.md` 安装到 `share/doc/c-trpc/`；源码归档
同样必须保留根目录许可文件及 `docs/licensing.md`，CI 逐字节校验两种分发。
外部 C/C++ 程序使用 `-I/tmp/c-trpc-sdk/include -L/tmp/c-trpc-sdk/lib`
并在 `-ltrcore` 后显式增加 `-lssl -lcrypto -pthread`。CI 将 SDK 安装到包含空格的路径，
使用 GCC 和 Clang 分别单独编译每个已安装公开头文件，并链接、运行外部 C11 程序，
校验门面默认配置与状态函数的公开链接；C++11 同样独立编译各头文件并链接运行此消费端。
配置消费端只引用安装后的包含目录/库目录，运行超时 10 秒。
另将 `tests/test_rpc_v2_sdk.c` 复制到仓库外目录，分别以 GCC、Clang 的 C11 和
C++11 编译运行真实 V2 Client/Server，单次运行超时 30 秒。该消费端仅包含公开头，
没有链接包装或私有源码路径，链接参数使用安装路径、`-ltrcore -lssl -lcrypto -pthread`。
它验证四种 ONE/MANY 基数、请求及响应元数据、复制发送、可写背压、取消、期限、排空
和 V2 调用语义统计，补齐配置消费端不能证明的运行时边界；独立进程 Echo 请求响应
检查继续保留。

## Make 兼容入口

```sh
make                          # xmake 配置后 build --all
make test CC=gcc
make test-timer CC=clang MODE=debug
make clean                    # xmake clean --all
```

`CC`、`AR` 只有被显式提供时才转发，避免 GNU Make 默认的 `CC=cc`
覆盖 Xmake 工具链。`CPPFLAGS` / `CFLAGS` 合并为额外 C 编译参数，
`LDFLAGS` 作为额外链接参数；还可通过 `XMAKE_CONFIG` 添加 Xmake 配置选项。
`XMAKE` 指定单个 Xmake 可执行文件路径（可以包含空格或引号），
不是带参数的命令片段；`MODE` 默认 `release`。
工具路径和参数值在 Make 规则中作为单个命令行参数传递，
参数内部的引号交给 Xmake 解析，不由命令行解释器提前拆分或展开。
`XMAKE_CONFIG` 则仍是由多个配置选项组成的命令行片段，
由调用者自行引用其中的路径。

兼容入口使用两个内部字符串选项 `make_cflags` / `make_ldflags` 保存参数。
Xmake 3.1.1 的原生 `--cflags` / `--ldflags` 会再次拆分包含空格的参数；
仅修复命令行引用不能保住已经解析的路径边界。
因此 `xmake.lua` 使用原生 `os.argv` 解析一次，
再以一个强制保留的参数组传给编译器/链接器，不自行实现解析器。
Make 配置时清空原生参数，避免旧配置中的同一组参数重复生效。
直接切回原生 Xmake 使用时，可以用 `xmake f -c ...` 清除兼容选项。

例如，包含空格的包含目录与相对运行时搜索路径：

```sh
make all CC=gcc CFLAGS='-I"/opt/vendor sdk/include"' \
    LDFLAGS='-Wl,-rpath,$$ORIGIN/../lib'
```

通过 GNU Make 传递字面量 `$` 时写成 `$$`，并使用命令行单引号保护参数值。
这里的 `$$ORIGIN` 经 Make 后变成 `$ORIGIN`，
不会被规则中的命令行解释器提前当作环境变量展开。
直接调用 `xmake f` 时没有 Make 这一层，不要照搬双 `$` 写法。

这些入口都要求已经安装 Xmake。
旧的单个 `.o` / `libtrcore.a` Make 目标不再提供，
应使用 `xmake build <target>`；没有无 Xmake 的备用 Make 构建。
`make clean` 清理 Xmake 产物，不负责删除迁移前残留在源码目录中的旧产物。

## CI 范围

检查类别包括 GCC（`debug + release`）、Clang（`release`）、
ASan + UBSan、TSan，以及 Make 兼容/编译器切换/SDK 安装/独立进程示例。
功能与内存/线程检查器检查均构建全部目标，并通过 `xmake test` 执行所有注册测试。
Make 兼容检查还使用包含空格/单引号的工具路径、强制包含头文件路径和带引号的字符串宏，
实际构建全部目标并运行测试；通过 `readelf` 验证最终 ELF
保留字面量 `$ORIGIN`，而不只是检查配置命令显示文本。
构建产物和缓存被 `.gitignore` 排除，安装检查还核对工作树没有新增文件。

冒烟检查的构建、安装和示例日志作为 CI 构建产物保留 7 天；
日志截取只限制控制台输出，不改变测试退出码，完整日志仍可用于复现失败。


## 消息变换后端的固定依赖

`xmake.lua` 固定 `zstd 1.5.7`，设置 `system = false` 和 `shared = false`，
使用官方包配方的 Xmake 源码构建路径，不自动复用系统动态库。
本后端使用 `ZSTD_STATIC_LINKING_ONLY` 的自定义分配器及帧头接口，
编译期检查头文件版本，运行前核对库版本；升级必须单独验证这些接口。
版本固定不是完整的包配方/工具链供应链锁定。首次配置需要获取第三方源码，
包校验或构建失败必须报错，不能悄悄删除后端或跳过测试。

```sh
xmake f -c -y -m release --toolchain=gcc
xmake test -v -j1 'test_message_transform/*'
```

该目标只复用三个实际生产对象 `message.c`、`transform.c`、`zstd.c` 和静态
Zstandard；不链接整个 `trcore`，不重复注册生产源，也不修改编译数据库覆盖规则。
既有测试和已安装 SDK 头文件清单不变。测试故障注入只在此目标链接时启用。

Runtime 已直接引用消息服务，静态链接即会带入 Zstandard 符号，不能再依赖仓库内
Xmake 对消费目标传递 `-lzstd`。`trcore` 使用 Xmake 3.1.1 原生
`build.merge_archive` 合并固定包的静态对象；安装后的 `libtrcore.a` 自包含该后端。
外部 C/C++ 消费端链接 `-L<SDK>/lib -ltrcore -lssl -lcrypto -pthread`；
不需要系统 Zstandard 或 Xmake 包缓存路径，但需安装 OpenSSL 3 运行时库。
构建和安装均不得静默切换到其他版本的动态库。

安装与源码归档同时保留 `third_party/zstd/LICENSE` 上游原始许可；SDK 路径为
`share/doc/c-trpc/third_party/zstd/LICENSE`。CI 不增加 `-lzstd` 来掩盖不完整 SDK，
而是检查安装归档内确有 Zstandard 的唯一符号定义，并按原外部消费方式运行程序。

合并归档不等于做了符号隔离；同时链接另一个 Zstandard 实现可能产生符号冲突或
版本选择问题，不属于当前保证范围。升级、符号命名/隔离及依赖锁定仍须独立验证。
当前不安装新的 Message 内部头文件。TLS 后端随静态库构建，但普通 V2 Client/Server
仍未接入 TLS；TLS 必需配置会明确返回 `TR_ERR_UNSUPPORTED`，具体阶段边界见 TLS-B 设计文档。
第三方来源与 MSG-B 阶段范围见 [MSG-B 实现审查](reviews/message_transform_zstd_20261015.md)，
实际 Runtime 接入及本次 SDK 修复见 [MSG-D 审查](reviews/runtime_message_recovery_20261015.md)。
