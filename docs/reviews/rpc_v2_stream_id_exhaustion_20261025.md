# RPC V2 本地流 ID 耗尽边界

## 变更

本地流 ID 仍按 Client 奇数、Server 偶数单调递增。达到 `UINT32_MAX` 或其前一
偶数后，分配器把游标饱和为 `0`；此后不回绕、不复用 ID，且失败时不覆盖调用者输出。
`tr_stream_open()` 观察到耗尽后先释放 Channel 锁，再通过现有 `tr_channel_begin_drain()`
启动 GOAWAY 排空，并在排空受理时返回 `TR_ERR_CLOSED`。若排空启动本身失败，则向调用者
返回该失败，游标仍保持耗尽，后续调用可以再次尝试启动排空。

分配 helper 声明位于 `channel_internal.h`，实现位于独立的 `channel_stream_id.c`，不属于
安装 SDK。将其保留为单独编译单元，使测试可通过 GNU ld `--wrap` 注入耗尽游标并观察真实
`tr_stream_open()` 分支；生产代码没有测试宏或可变测试回调。该设计增加一次仅在创建流时
发生的内部函数调用，不增加逐帧成本。它不改变公开 API、帧格式、流 ID 规则或流槽复用行为。
RESET tombstone 的有限容量、精确代次与 FIFO 规则没有改变。

## 验证

- 测试先行：正确选择当前工作区的 Xmake 项目后，新增回归在 helper 尚不存在时因未声明而编译失败。
- 集成测试先行：加入 GNU ld wrapper 后，目标链接因尚无可包装的 helper 符号而失败；helper 拆为独立内部编译单元后链接成功。
- `test_stream_reset` Release：通过；覆盖客户端最后两个奇数 ID、服务端最后一个偶数 ID、耗尽后保持耗尽以及输出参数不变。
- `test_stream_reset` 的真实 Channel 集成用例：测试链接 wrapper 将真实实例的下一 ID 游标置为 `0`；`tr_stream_open()` 返回 `TR_ERR_CLOSED`，真实对端收到 GOAWAY，Channel 先为 DRAINING，收到对端 GOAWAY 后变为 DRAINED；未添加生产测试开关。
- `test_channel_drain` Release：17 组通过。
- dev.41 CI 发现手工列举源码的 `test_rpc_record_endpoint_tcp` 缺少新 helper，导致 TSan 链接失败；dev.42 将 `channel_stream_id.c` 加入该清单。本地 TSan 下该目标和 `test_stream_reset` 均构建、运行通过，完整 CI 待新 head 复验。

## 剩余范围

本切片仅收口本地流 ID 耗尽这一边界。#242/ERR-B 的其余 RESET/FIFO 时序与代次组合、
ERR-C 至 ERR-E 和生产 C 全错误分类矩阵仍待实现或验证，不能据此声称 V2 已完成。
