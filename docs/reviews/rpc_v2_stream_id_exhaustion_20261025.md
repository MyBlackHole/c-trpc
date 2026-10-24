# RPC V2 本地流 ID 耗尽边界

## 变更

本地流 ID 仍按 Client 奇数、Server 偶数单调递增。达到 `UINT32_MAX` 或其前一
偶数后，分配器把游标饱和为 `0`；此后不回绕、不复用 ID，且失败时不覆盖调用者输出。
`tr_stream_open()` 观察到耗尽后先释放 Channel 锁，再通过现有 `tr_channel_begin_drain()`
启动 GOAWAY 排空，并在排空受理时返回 `TR_ERR_CLOSED`。若排空启动本身失败，则向调用者
返回该失败，游标仍保持耗尽，后续调用可以再次尝试启动排空。

分配 helper 位于 `channel_internal.h`，只供 Channel 实现和内部回归使用，不改变公开 API、
帧格式、流 ID 规则或流槽复用行为。RESET tombstone 的有限容量、精确代次与 FIFO 规则没有改变。

## 验证

- 测试先行：正确选择当前工作区的 Xmake 项目后，新增回归在 helper 尚不存在时因未声明而编译失败。
- `test_stream_reset` Release：通过；覆盖客户端最后两个奇数 ID、服务端最后一个偶数 ID、耗尽后保持耗尽以及输出参数不变。
- `test_channel_drain` Release：17 组通过。

边界回归直接调用纯分配 helper。当前 Channel 对象是不透明的，测试没有把真实实例的游标注入到 32 位上限，所以 `tr_stream_open()` 到 `tr_channel_begin_drain()` 的耗尽集成分支尚未由运行时回归直接触发；本记录不把它当作 ERR-B 全路径验收。

## 剩余范围

本切片仅收口本地流 ID 耗尽这一边界。#242/ERR-B 的 RESET/FIFO 时序与代次组合、
ERR-C 至 ERR-E 和生产 C 全错误分类矩阵仍待实现或验证，不能据此声称 V2 已完成。
