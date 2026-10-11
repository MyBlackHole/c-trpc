# 有界跨帧预读：仅本地 A/B 实验

此候选不启用生产默认，也不改变公开 API、协议或动态分配路径。不得仅凭
功能测试将其推广为默认配置；性能收益必须由同一基点、同一编译参数下的
交错 A/B 样本另行判断。

## 构建与范围

- A：`xmake f -c -y -m release --experimental_bounded_readahead=n`
- B：`xmake f -c -y -m release --experimental_bounded_readahead=y`
- 默认值为 `n`；宏为内部 `TR_EXPERIMENTAL_BOUNDED_READAHEAD`。
- 两种构建均使用原 `xmake test -v -j1` 入口；新增
  `test_bounded_readahead/default` 保护局部接收不变量。

B 在每个内部 parser 对象内增加 4096 字节 scratch 与两个 32 位游标。
总增量必须按对象数量（包括预分配但尚未活动的连接槽位）统计，不能只数
活跃连接。当前 Linux x86_64 GCC 测量由专测输出 `sizeof(struct tr_parser)`；
A 为 120 字节，B 为 4224 字节，即每个对象增加 4104 字节。Reactor 的连接
数组以 `max_connections * sizeof(struct tr_connection)` 分配和计账，已有
`sizeof` 计算包含此增量；实验没有另加堆分配或修改全局资源预算模型。
每个 Reactor 运行线程的 turn 另有一个 64 位私有缓存消费计数器。

## 接收约束

- preface 仍精确直收，不进入预读路径。
- HEADER 或已知载荷不超过 512 字节时，允许一次 recv 到最多 4096 字节
  的空 scratch；接收长度同时受现有网络 RX 额度限制。
- 按现有 `write_len` 逐段复制到 parser 原目标，继续使用原 CRC、头校验与
  payload pool。每完成一帧立即同步 dispatch，再检查连接状态。
- 大载荷先消费已经预读的前缀，剩余部分仍直收到最终 payload。其前缀可能
  多一次复制，不能把 B 的 bulk 接收称为全程零额外复制。
- scratch 用尽归零，不追加、不压缩、不创建额外帧队列；逐帧清理保留余数，
  reset 丢弃余数。pool 耗尽停止消费与 recv，并保持既有暂停行为。
- resume 成功且 scratch 非空时显式加入原 RX-ready 队列，因此不依赖新的
  EPOLLIN。暂停时不自调度；可运行工作因额度耗尽退出时续排。

真实 recv 返回的字节才扣网络额度与累计 RX 统计；缓存消费不重复计数。
B 每轮另有与配置 RX 字节额度等大的缓存消费上限，每次连接处理另限 64 KiB。
额度耗尽且还有缓存时下一轮继续，仍使用原 RX-ready/dispatch 上限。
此上限证明有界性，不声称 CPU 公平性与 A 完全相同。

## 测试与解释边界

专测以真实非阻塞 socketpair 加仅测试链接目标的 wrapper，检查半头/半体、
连续帧与尾半帧、空帧、512 阈值、4 KiB 边界、small/bulk/small、pool 暂停与
无新写入恢复、已读空内核后的跨轮次缓存、精确缓存/网络额度、EOF、EINTR、
EAGAIN、错误头/载荷 CRC、合法 CRC 下非法类型/超长、回调关闭、preface 与 reset。
多连接轮转保留原 `test_reactor_budget`，默认 A 的旧断言不变；B 区分真实 recv
与 parser 消费。若平台导致测试 wrapper 无法观测，测试必须失败并报告限制，
不能替换或绕过 Sanitizer 的系统调用插入。

这些 wrapper 只用于确定性正确性测试，不是 tracing 或性能 syscall 测量。
没有真实 syscall 观测就不能声称实测 recv 次数下降。实验不分析或修复计时
漏样，不扩大到整体生命周期、引用回收或资源预算审计，不修改系统设置。

## 本地实验结论与草稿 CI

[完整不利结果与测量限制](experimental_bounded_readahead_results.md)及
[全部30个样本CSV](experimental_bounded_readahead_samples.csv)保留原定组与
补充组，结论是继续默认关闭，不支持推广。草稿只追加 ON 的 release/asan
定向 CI，原默认 OFF 检查不变；ASan 项保持 LeakSanitizer 开启，失败照常阻断。
本地 LeakSanitizer 环境失败不能由地址/UB检查或未来 CI 全绿改写成性能收益。
