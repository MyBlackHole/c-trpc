# RPC V2 父子 Call 上下文审查记录

## 审查范围

本记录覆盖 `2.0.0-dev.155` 的 Server V2 父 Call 上下文持有、Client 子 Call 取消传播、deadline 继承及引用回收。代码分支为 `feat/rpc-v2-parent-context-243`，以 `feat/reactor-owner-wait-graph-243`（PR #328、dev.154）为基线。

该范围只完成 #243 的父子取消/deadline 子项。#243 仍为 Open；本文不代表异步操作内核或 RPC V2 整体验收通过。

## 行为与所有权复核

- `tr_rpc_v2_cancel_context_hold` 只允许在对应 Server V2 `work` 回调中调用。Dispatch 以 Call 容量预分配固定上下文池；池耗尽返回 `TR_AGAIN`，不动态分配上下文。
- 上下文句柄携带槽位代次。旧代次在槽位复用后被拒绝；公共应用句柄和每个受理的子 Call 各持一个引用。Dispatch 销毁在外部引用归零前返回 `TR_AGAIN`。
- 父 Call 的 deadline 使用单调时钟绝对时间。子 deadline 是父绝对期限与子相对期限的较早者；零子期限继承父期限。受理时已取消或已过期的上下文拒绝调用，且不改输出句柄、不调用应用回调。
- 父取消在子 Client 既有进度泵和收到记录的应用回调入口检查；deadline 终局保留 `DEADLINE_EXCEEDED`，其他父终局映射为 `CANCELLED`。已先被 Stream 接纳的 FINAL 继续遵守 first-final-wins。
- 子引用在 Client 槽位退役时释放；若 CallRef 仍钉住结果，则延后到 Client 清理。父网络槽仍按既有 Endpoint close/RESET 屏障回收，上下文池独立于网络槽。
- stopped-Reactor 回收会先发布取消并保留外部引用屏障。路由 detach 的防御分支也将 ACTIVE 状态原子发布为 `CANCELLED`，避免链路状态和子 Client 读取状态分离。
- 没有增加每上下文线程、无界队列或每上下文堆分配。新增状态池内存计入 Dispatch 预算；固定池扫描仅发生于首次 hold。

## 发现和处理

1. ASan 首次报告的 4 个异步 producer 泄漏来自 SDK 测试夹具：无 worker 线程的用例仍为不存在的 producer 线程保留引用。夹具现仅在线程确实创建时 retain；修正后标准构建目录的 ASan/UBSan 全套和 SDK 目标均无泄漏。
2. 一次全套 sanitizer 使用自定义 `-o` 构建目录，仓库 TLS 测试辅助程序无法从二进制路径推导 `build/test-tmp`，导致 TLS identity 初始化失败。该运行不作为代码失败或通过证据；改回标准 `build/linux/...` 路径后，全套 ASan/UBSan 为 92/92。
3. GCC 15 的 C++11 `-Wextra -Werror` 会对惯用 `{0}` 聚合零初始化报告缺字段警告；测试源对 GCC/Clang C++ 模式局部抑制此警告。Clang 23 会因测试命令尾部 `-x none` 报无效输入语言警告；CI 命令已删除多余选项。
4. 本地代码审查检查了引用增减、槽位代次、父 deadline 与子 deadline 竞争、终局先后、失败路径和 Server/Client destroy/reap 屏障。没有发现本子项内未处理的阻断问题；全局 #243 项目仍按下节保留。

## 验证证据

所有 Xmake 测试使用 `TMPDIR="$PWD/build/tmp"` 和 worktree 标准构建目录，没有向 `/tmp` 写测试文件或产物。

完整测试矩阵（92/92）曾在本分支相同的生产实现上通过；这些全套运行发生在最后补入“旧代次拒绝”和 detach 状态镜像防护之前。最终代码版本另对受影响的 V2 SDK 目标做了完整重跑：

| 配置 | 全套历史结果 | 最终代码 `test_rpc_v2_sdk` |
|---|---:|---:|
| GCC Debug | 92/92 | 1/1，通过 |
| GCC Release | 92/92 | — |
| GCC ASan/UBSan | 92/92 | 1/1，通过，无泄漏 |
| GCC TSan | 92/92 | 1/1，通过 |
| Clang 23 Release | 92/92 | 1/1，通过 |

最终聚焦验证使用：

```sh
TMPDIR="$PWD/build/tmp" xmake f -c -y -m debug --toolchain=gcc --crc32c_portable=n -P .
TMPDIR="$PWD/build/tmp" xmake test -P . -v -j1 "test_rpc_v2_sdk/*"
```

ASan/UBSan、TSan 和 Clang Release 使用相同命令，将 `-m debug --toolchain=gcc` 分别替换为 `-m asan --toolchain=gcc`、`-m tsan --toolchain=gcc` 和 `-m release --toolchain=clang`。四种最终 SDK 目标均通过；每个报告 1/1，覆盖 23 个 SDK 场景。

安装消费验证使用 GCC Release 重新构建并安装到 `build/sdk-install`，然后从 worktree 外层的 `build/sdk-consumer` 只包含安装头、链接安装静态库。所有安装公开头分别通过 C11/C++11 语法检查；`tests/test_rpc_v2_sdk.c` 作为外部 consumer 在 GCC、Clang、G++、Clang++ 下以 `-Werror` 编译并运行，4/4 通过。

源码格式使用项目固定的 clang-format 18.1.8，通过 `scripts/check_source_format.py` 按基线 `9f863d68503ed8f199151c95af878caff8b96db6` 检查 8 个改动源码文件，0 个失败。

## #243 仍待处理

- 通用异步操作提交、受理、完成发布和退役状态机，以及显式可持有 Call/操作引用。
- 鉴权、压缩及其他异步工作完成晚到时的 owner/Call/Attempt 代次校验和拒绝准入。
- 最终结果已选择、最后用户回调结束、任务与缓冲区全部回收这三个不同屏障的接口和证明。
- 单 worker、队列满、嵌套与跨 Reactor 等待场景，以及读写并发契约和拦截器观察/拒绝/异步认证语义。
- 与 #235 压缩和 #234 TLS 共用有界调度/取消基础的整体验收。

PR 只提交父子上下文切片，基于 PR #328 的分支；不合并 PR、不关闭 #243，也不宣称 V2 全部完成。
