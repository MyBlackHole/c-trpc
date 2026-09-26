# CRC32C 后端、兼容性与验证

## 接口不变，校验结果不变

公开入口仍是 `tr_crc32c_begin/update/finish` 和 `tr_crc32c`。算法仍为
CRC-32C / Castagnoli（反射多项式 `0x82f63b78`），初始状态 `0xffffffff`，
只在 finish 时异或 `0xffffffff`。update 接收和返回未 finish 的中间状态，
允许任意初始状态；多段 buffer 与一次性计算必须一致。长度为零允许 NULL，
并保留输入 state。非空输入必须指向至少 len 字节可读内存，不要求对齐。
本轮不修改 wire、Reactor、Channel、RPC 或资源所有权。

## 分派与实现边界

- 可移植路径：256 项只读表，共 1 KiB；按字节访问，不依赖主机字节序。
  表由旧算法的八次位迭代产生，无运行时表生成或动态分配。
- x86-64 路径：单个 `target("sse4.2")` 函数使用 CRC32C 指令，按 8/4/2/1
  字节处理。定长 memcpy 读取未对齐数据，只有剩余长度足够时才读取，不读越界。
- 默认自动模式：第一次非空 update 通过 pthread_once 检查 CPUID 的 SSE4.2
  能力并发布函数指针；后续 update 不再检查 CPUID。选择完成后指针不再变化。
  不使用 IFUNC、全局构造函数、环境变量开关或可在运行中修改的后端选择。
- CPU 不支持时回退到可移植路径。非 x86-64 编译也只使用可移植路径。
  本轮没有 ARM CRC 专用实现、PCLMUL 或多路折叠算法。

默认构建不增加全局 `-msse4.2` / `-march=native`；调用方自行增加更高 ISA
编译参数会改变其二进制最低 CPU 要求，不能再依赖本分派器保护其他代码。
新增的 `src/crc32c_internal.h` 及 backend 符号只供库内与差分测试使用，不安装
该头文件，不作为 SDK 稳定接口。直接调用硬件 backend 前必须检查 CPU 能力。

构建期可完全排除硬件路径和 CPU 分派，便于兼容性验证：

```sh
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=y
xmake test -v -j1
xmake build bench_crc32c
xmake run bench_crc32c

# 恢复默认自动选择
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n
```

## 确定性验证

`test_crc32c` 直接链接生产实现，以独立逐位算法为参考，分别测试自动入口、
可移植入口以及 CPU 支持时的硬件入口。任何测试都不需要全局开启 SSE4.2。

1. 16 个线程通过屏障同时进行首次非空调用，每个线程使用独立数据与中间状态。
2. 固定向量、NULL 空输入、64 种地址偏移和 0 至 256 字节短输入。
3. 短输入所有分割点；10,000 组固定种子的内容、长度、初始状态及分段测试。
4. 超过 1 MiB 的多段增量；保护页前精确结尾、只读输入与各种尾部长度。

CI 的 GCC/Clang debug/release、ASan/UBSan、TSan 都执行自动与纯软件构建，
包括既有 Transport/RPC、Timer、线程生命周期、预算、公平性和控制优先级测试。
另一个 QEMU job 使用自动构建的同一二进制，在隐藏 SSE4.2 的虚拟 CPU 下运行
`test_crc32c --expect-portable`。这项检查验证运行时回退，不拿纯软件构建替代它。
性能值不作为 CI pass/fail 门槛。

## 同环境组件基准（2026-09-26）

基线 main `85176b72bb593333cd2525723d9bf55aff65c0f0` 的逐位实现，CRC blob
`500b3ecd2335580f0ffa180476f9ed0e102d8916`。使用未修改的 `bench/bench_crc32c.c`
（blob `7dda455942c4a3e5bc7aab137aefbb910d60a7d2`）分别链接基线、强制纯软件
和自动后端。GCC 14.2 / Clang 17，`-O2 -pthread -mno-sse4.2`，无 LTO/sanitizer。

共享 Linux x86-64 容器，报告 AMD EPYC 9V74、SSE4.2 可用，cgroup 配额为 4 核
等价；未固定 CPU。每个进程每种长度 5 个样本、每样本累计 16 MiB；每种编译器
再轮换版本顺序运行 5 轮。表中为五个进程中位数的中位数，单位 MiB/s。
缓冲重复访问，属于缓存友好的单线程 CRC 组件微基准，硬件样本计时较短，
不能外推为冷缓存、内存带宽、TCP 吞吐、RPC QPS 或 P99。

| 编译器 | 输入字节 | 旧逐位 | 强制查表 | 自动（SSE4.2） |
|---|---:|---:|---:|---:|
| gcc | 64 | 101.65 | 503.86 | 7699.89 |
| gcc | 4096 | 100.68 | 418.20 | 9065.93 |
| gcc | 65536 | 98.23 | 417.92 | 8576.70 |
| gcc | 1048576 | 99.79 | 412.62 | 9020.93 |
| clang | 64 | 211.80 | 511.18 | 9407.44 |
| clang | 4096 | 208.38 | 420.22 | 8810.50 |
| clang | 65536 | 209.31 | 418.74 | 8619.91 |
| clang | 1048576 | 208.16 | 414.45 | 8882.61 |

原始进程样本及每进程范围见 [CSV](../bench/results/crc32c-20260926.csv)。
与 [控制优先级阶段的性能评估](performance_readiness.md) 相比，本次改变的是
校验成本，未建立新的端到端或生产就绪结论。后续优先加入真实 RPC/BULK 混合、
过载、CPU/RSS 与尾延迟测量，再决定 ownership/Runtime shard 的优化顺序。

## 实现依据

- [GCC x86 function target attributes](https://gcc.gnu.org/onlinedocs/gcc/x86-Attributes.html)：
  把专用指令限制在指定函数中，避免提高整个库的 ISA 基线。
- [LLVM CRC32C intrinsic definitions](https://clang.llvm.org/doxygen/crc32intrin_8h_source.html)：
  CRC32B/W/L/Q 对应 CRC-32C 更新，不是 IEEE CRC-32。
- [QEMU user-mode CPU selection](https://www.qemu.org/docs/master/user/main.html)：
  CI 中通过 CPU 特性选择验证缺少 SSE4.2 的运行条件。
