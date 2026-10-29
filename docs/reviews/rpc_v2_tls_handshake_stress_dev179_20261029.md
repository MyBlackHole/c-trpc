# dev.179 持续 TLS 握手压力复跑

日期：2026-10-29；版本 `2.0.0-dev.179`；关联 #234、#241、#243、#247。
源码固定为 `631e918798a435c2810bb2c3da8ac789fec12fb6`，远端检查已精确核对
13/13 成功：[CI](https://github.com/MyBlackHole/c-trpc/actions/runs/40873100833)、
[RPC smoke](https://github.com/MyBlackHole/c-trpc/actions/runs/40873100832)。
这不是本文归档提交的 CI 结果，两者分开跟踪。

本次在[空闲写兴趣修复](rpc_v2_tls_idle_interest_20261029.md)后，使用同一
[持续握手驱动](experiments/tls_handshake_stress_20261029/README.md)再运行 600 秒。
旧 dev.178 的[原始结果](rpc_v2_tls_handshake_stress_20261029.md)保留，不改写版本或数据。
二进制 SHA-256 为 `19471c5a5fff52d58edf81bf1dd7ee773f492fe87b4a8d7ee7849eec6a64df28`；
脚本摘要与已完成标签修正/预检的归档版本一致。本次 metadata 的 TLS/Zstd 标签
与两端实际命令、ready 和 open_phase 一致，无需更正。

## 固定条件与结果

Server 仍为 32-peer、128-call、10000ms 握手期限；先建立的健康连接以 100 RPS、
4 KiB 正文、TLS 1.3 服务端认证与双向 Zstd 运行。另以每秒 64 次目标速率循环
静默、部分记录滴灌、ClientHello 后中止及明文，攻击 socket 并发上限 64。
压力结束后关闭源、空闲 12 秒，再建连接做 64 次恢复；没有增大队列/窗口/线程。

- offered/attempted/accepted/completed/ok 均为 60000，发生器丢弃、提交错误、
  deadline、RPC 错误及无效正文均为零。
- Server 含 32 次预热和 64 次恢复共 60096/60096，drain 成功，排队/运行任务归零。
- 四类 TCP 尝试/连接完成各 9600，共 38400；连接错误和发生器跳过均为零，
  每类远端关闭与主动关闭的合计准确覆盖连接数。它们不是 38400 次 TLS 准入。
- Server ready peer 共 2，reaped peer 为 5865；压力中大量 TCP 连接可能直接
  被 peer 容量拒绝，不据这些计数推导精确 TLS 握手次数或新连接准入保证。

617 个进程采样和完整计数分别见 [CSV](data/tls_handshake_stress_dev179_20261029_resources.csv)、
[汇总](data/tls_handshake_stress_dev179_20261029_summary.json)。所有原始文件摘要、
命令/环境、关闭分类和客户端观测寿命均保留。

| 观测项 | 采样范围 | 压力解除并完成恢复后 |
|---|---:|---:|
| RSS（KiB） | 11824–23904 | 18684 |
| PSS（KiB） | 5154–17233 | 14195 |
| fd 数 | 8–40 | 8 |
| 线程数 | 7–7 | 7 |

退出 getrusage 峰值为 27952 KiB，高于定时采样；不能把采样最大值当硬上限。
RSS 未随累计连接数持续增长，也未恢复初始值；没有 live allocator 和内核实际
驻留证据，不从这些读数推断全部释放、泄漏或具体缓存所有者。

## 调度成本与比较限制

Server 生命周期 CPU 为 161.582060 秒，Reactor 累计 844527 轮，其中
epoll_waits=843495、epoll_polls=1032。旧 dev.178 同一目标输入的对应值为
381.657915 秒、34969657 轮；它们是生命周期观察，包含构造/预热/排空。
两次实际 reaped peer 和 ClientHello 字节不同，且共享开发机未隔离，不能按
38400 作分母宣布每握手成本下降或将其当作严格配对容量实验。

本次成功延迟 p50/p99 为 34.290673/40.289016ms，提交迟到 p99 为 153.717us，
测量窗外排空尾部 27.833028ms。旧运行成功 p99 为 36.185598ms；保留此差异，
不宣称忙转修复已经改善所有负载的尾延迟。根因的确定性依据仍为旧实现空队列
保留写兴趣的红测、修复后的两端真实注册检查和独立空闲对照。

原始材料在忽略目录 build/performance-20261029/dev179-tls-handshake-600s/。
本次只运行既有 Xmake 构建产物和归档诊断驱动，未改生产实现或测试/CI 参数；
私钥、证书、日志与临时文件均只在工作区 build/。它不覆盖 mTLS、凭证轮换、
最坏证书/握手输入、OpenSSL/跨域总硬预算、内核驻留、固定基准机容量或全部
取消/故障组合，发布目标和相关任务继续开放。
