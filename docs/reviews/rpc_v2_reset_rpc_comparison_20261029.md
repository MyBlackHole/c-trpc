# RESET 位图改动的 RPC 开环对照

日期：2026-10-29；关联 #247、#241、#242；本次只运行既有基准，不改运行时或测试参数。

40k 计划到达速率下，旧版与候选的明文、TLS/Zstd 三种公开配置均为 0/3 干净样本。
36 次运行全部出现 CONTROL TX 池压力、发生器丢弃和迟到，容量门槛仍未通过。
[RESET 微观修复](rpc_v2_reset_sparse_scan_20261029.md)的锁次数与 CPU 成本证据保留；
本次没有从该局部改进推导实际 RPC 容量达标，也不因新旧某些中位数不同宣布收益。

## 固定输入与源码身份

- 旧版运行时：`1f33cddcfa27359ac860986f0535d3ca568278a2`（dev.181），在工具管理的
  独立工作树 `/home/black/.codex/worktrees/0feb/c-trpc` 构建，保持 detached/干净。
- 候选运行时：`0d81342e85739bcd8ff0253bdbcd894c602696c0`（dev.182）；脚本元数据
  记录账目校正 head `33f630056ef59db58a4e09e40ed01777084d8544`，其生产代码与 0d81342 相同。
- 两个 ELF 在运行前复制冻结。旧/新 SHA-256 分别为
  `522b0ee0754f486aac4cb024ff32215fd02026f98a1e04d3b1b8627e835c478f`、
  `55d1c4b990f1878f7999017960c2c6cf5443fdf998be9ece389c5fa946bd137e`。
- 两边均使用原 Xmake GCC Release / x86_64 / CRC portable=n 配置；真实编译数据库
  各验证 55 个 trcore 单元，路径归一化后的生产命令与编译器/配置相同。生产差异
  仅 Channel；bench 源码、目标定义和调用脚本未改，没有比较新增测试的代码路径。

共享 AMD Ryzen 7 PRO 4750U 开发机，亲和性 0–15，IPv4 loopback。两个版本先在
明文与 TLS/Zstd 下各做三种闭环窗口的 64 请求预检，共 12 个闭环场景全部通过。
随后每种传输做三轮，轮内按旧→新、新→旧、旧→新交错；一次只运行一方。

原 `run_rpc_repeatability.py` 入口保持 40000 RPS、最短 250ms、每场 10000 次计划
到达、16 个发生器各 2500 RPS/64 调用槽/64 窗口、32 次预热、3000ms 期限，
Server 工作延时为零。Server call limit 为 32/128/256，对应 bulk-bytes 参数
16384/65536/65536。open 场景发送默认 32 字节请求与 32 字节响应，不是 bulk
负载；576 个发生器记录的应用字节/成功 RPC 比例也核对为约 64 字节。
TLS 复用既有服务端认证 fixture，双向启用 Zstd，未验收 mTLS 或凭证轮换。

## 原判定与结果

使用原 clean_result/exact_wall_signals，不修改阈值或数据字段；丢弃、提交拒绝/
错误、期限、RPC 错误、无效正文、发生器 P99 迟到超过其 400us 间隔，或既有
Server 压力字段任一触发，都不能算干净。原判定、全部单独 PID 记录与命令均保存。

下表为受压开发机的观察值，所有样本都同时带发生器和 Server 信号，ok_rps
不能作为稳定吞吐量或 TLS/压缩开销。每格旧值在前、新值在后。

| 传输 | call limit | 观测 ok_rps 中位 | 发生器丢弃中位 | P99 调度迟到中位（us） | 干净样本 |
|---|---:|---:|---:|---:|---|
| plain | 32 | 4701.9 / 4625.2 | 7870 / 7918 | 1383.1 / 2676.6 | 0/3 / 0/3 |
| plain | 128 | 5884.5 / 5993.7 | 7737 / 7668 | 1994.7 / 1691.7 | 0/3 / 0/3 |
| plain | 256 | 5255.2 / 5863.4 | 7988 / 7814 | 2317.1 / 1244.0 | 0/3 / 0/3 |
| tls-zstd | 32 | 3386.3 / 3476.0 | 8444 / 8460 | 1131.5 / 1816.7 | 0/3 / 0/3 |
| tls-zstd | 128 | 3948.2 / 4097.9 | 8448 / 8516 | 2692.0 / 3679.5 | 0/3 / 0/3 |
| tls-zstd | 256 | 3757.6 / 3827.7 | 8534 / 8528 | 2540.7 / 3614.6 | 0/3 / 0/3 |

每个版本/传输组合的九个样本全有 CONTROL TX 池耗尽，36 场控制池容量均为 128，
未增大池或线程来消除信号。各组合 Client 总数如下：

| 传输/版本 | 计划 | 发生器丢弃 | 已接受/已完成 | OK | deadline | RPC RESOURCE_EXHAUSTED |
|---|---:|---:|---:|---:|---:|---:|
| plain / 旧 | 90000 | 70940 | 19060 / 19060 | 19016 | 0 | 44 |
| plain / 新 | 90000 | 70509 | 19429 / 19429 | 19372 | 0 | 57 |
| tls-zstd / 旧 | 90000 | 76245 | 13755 / 13755 | 13456 | 225 | 74 |
| tls-zstd / 新 | 90000 | 76274 | 13726 / 13726 | 13674 | 0 | 52 |

plain 新版另有 62 次 submit_again；全部场景 submit_errors=0、invalid_responses=0，
65970 项已接受工作全部完成，Server 排队/运行任务最终为零。基线 TLS 的 225 次
期限出现在两个样本，保留其结果；不同实际准入量与到达时间下不能把 225→0 当作
独立期限问题已修复。Server CPU/RSS 是构造/预热/测量/排空的生命周期值，不算
成单握手、单 RPC 或精确测量窗成本。

## 证据与后续

[汇总与身份](data/reset_rpc_comparison_20261029_summary.json)、
[36 场账本 CSV](data/reset_rpc_comparison_20261029_cases.csv)、
[原始记录 ZIP](data/reset_rpc_comparison_20261029_raw.zip)及
[执行器/复现说明](experiments/reset_rpc_comparison_20261029/README.md)均保留。
ZIP 收录 1313 份原始记录、stderr、命令、编译配置和执行脚本，INDEX.json 逐项列出
未压缩字节数与 SHA-256；读取后逐一核对摘要与 ZIP CRC，没有只收录成功调用。
证书/私钥和 ELF 未纳入 ZIP，原始数据与构建/测试临时目录均位于工作区。

账目校正 head `33f6300` 的关联检查已精确核对 13/13 成功；见
[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40899525420)、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40899525562)。本归档提交 `a3077c3` 后续也已精确核对 13/13 成功：
[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40904890923)、[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40904890915)。
该绿色记录不构成 40k 容量通过。下一步需分离发生器实际提交、Server CONTROL
进度与资源恢复的成本；当前混有过载、丢弃和迟到，无法做严格性能比例或 SLO
结论。固定验收主机/平台、预算总账、服务目标/Attempt、CTM1 生成工具/桩和后端
等价网络比较仍未完成；CTM1 共同描述设计仍待审阅。
后续定位结果与撤回的调度候选见[CONTROL TX 与准入诊断](rpc_v2_control_tx_admission_20261030.md)。
