# RPC V2 压缩 DATA 类型标志隔离

日期：2026-10-25；版本 `2.0.0-dev.36`；关联 #242。

## 缺陷

TRP wire 解码器只验证标志是否为全局已知值。`TR_FRAME_F_LANE_BULK` 虽是合法线帧标志，
却不属于 DATA；DATA 只允许 `FIRST`、`LAST`，压缩 DATA 还允许 `COMPRESSED`。

普通 DATA 路径在活动流上把已定位的类型标志错误隔离为目标流 RESET，并在 RESET 丢弃态
拒绝该坏帧。压缩 DATA 走独立处理函数：活动流路径没有类型标志掩码，RESET 丢弃路径更早
返回到丢弃记账。结果是错误 lane 标志的有效压缩正文能被交付；同一坏帧在 RESET 状态还会
绕过应有的严格连接校验。

## 修复

- 活动流先验证身份与接收信用，再限制压缩 DATA 标志掩码；额外标志使用现有 `TR_ERR_STATE`
  RESET 只终止目标流。
- RESET 丢弃态也先检查相同掩码；额外标志按既有普通 DATA 合同作为 `TR_ERR_BAD_FLAGS`
  终止收到该帧的精确物理连接，不发放额外接收信用。
- 不改 wire 格式、RESET 状态机或合法 NONE/压缩 DATA。没有新增内存、任务或队列；检查为
  固定位运算，保持现有 Channel 锁范围。

## RED/GREEN 验证

- `test_stream_reset` 新用例在 split Channel 上先 RESET CONTROL 流，再发送带
  `COMPRESSED | LANE_BULK` 的坏 DATA。修复前未产生 CONTROL_DOWN，等待断言超时；修复后收到
  `TR_ERR_BAD_FLAGS`，BULK 连接的独立流仍能交付数据并完成 RESET。
- `test_transport` 使用真实协商和有效 Zstandard 正文向活动流注入额外 `LANE_BULK`。撤掉
  活动流掩码的敏感性试验将正文交付到回调并命中六项接收哨兵；恢复检查后只观察到目标流
  `TR_ERR_STATE` RESET，接收计数不增加。
- Release 全量结果：`xmake test -P . -v -j1`，90/90 通过，耗时 88.396s；
  其中 `test_stream_reset` 与 `test_transport` 均通过。
- Debug 聚焦结果：`test_stream_reset` 与 `test_transport` 均通过。
- ASan/UBSan 聚焦结果：`test_stream_reset` 与 `test_transport` 均通过，启用泄漏检测及
  sanitizer 即时报错退出。
- `.clang-format` 检查、`git diff --check` 均通过。

本记录只覆盖压缩 DATA 标志分类，不代表 #242 或 V2 完成。候选远端检查须按本次提交身份补录。
