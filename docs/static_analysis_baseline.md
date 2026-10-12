# 2026-10-12：生产静态分析首轮诊断基线

## 可追溯输入

- 原始分析提交：`a6d8f7d9df163e4c38aae771999cf08923bb2d70`（PR #223 首轮，非其最终提交）。
- 已验证的基础主分支：`29bf384aa59903b5d79453d5d5cf607c9bb6fa62`（PR #222）。
- 环境：Ubuntu 22.04 / x86_64，Clang release，CRC 自动后端，`clang-tidy-14`（LLVM 14.0.0）。
- 正式输入：真实 Xmake `trcore` 编译数据库，经生产源码与完整配置身份复查。
- 运行结果：31 个生产翻译单元全部执行，11 条日志级告警，分析器非零退出 0 个。
- [完整运行日志及数据库产物](https://github.com/MyBlackHole/c-trpc/actions/runs/38189946792/artifacts/11720818222)。

## 告警位置与初步成因

| 生产源文件 | 行号 | 告警数量 |
|---|---|---:|
| `src/execution/buffer.c` | 67、75、139 | 3 |
| `src/execution/command_queue.c` | 26 | 1 |
| `src/execution/completion_queue.c` | 27 | 1 |
| `src/execution/reactor.c` | 305 | 1 |
| `src/execution/timer_queue.c` | 123（两条独立分配） | 2 |
| `src/rpc/rpc.c` | 3539、3542 | 2 |
| `src/transport/channel/channel.c` | 2602 | 1 |

全部 11 条均为 `clang-analyzer-unix.Malloc` 的“潜在内存泄漏”告警。
逐个对照当前源代码，以上报告均涉及采用
`TR_AUTO(...cleanup)` 的作用域资源或封装构造状态；可在
`src/cleanup.h` 和各生产文件中找到清理或所有权转移定义。
Clang 14 分析器对 GCC/Clang `cleanup` 属性的处理能力可能影响这些路径的
泄漏推断。**这只是初步归因，尚未对每条路径完成独立正确性证明，
不能将 11 条直接登记为已确认误报，更不能删掉或全局关闭 malloc 检查。**

## 下一轮验收

1. 对每条告警还原错误分支以及对应 `cleanup` 释放或 `take()` 转移边界。
2. 对分配失败、互斥锁/条件变量初始化失败、复杂构造回滚设计可重复的故障注入。
3. 独立验证分析器是否未建模 `__attribute__((cleanup))`；必要时与编译后代码和其他分析器交叉核对。
4. 若需建立基线例外，必须精确绑定源文件、检查项和已复核的路径；禁止全局抑制 `clang-analyzer-unix.Malloc`。
5. 处理告警后才将 `--strict` 加入合并门禁，并在该模式下验证真实生产编译单元。

本记录不是“零告警”证书，也不意味着 #154 P1-4/D1 或正式发布就绪。
