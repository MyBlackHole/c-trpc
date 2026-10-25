# Split Channel 压缩 DATA 双向同时发送回归

日期：2026-10-25。版本：`2.0.0-dev.91 -> 2.0.0-dev.92`。关联 #241、PR #324。

## 目标

补齐 #241 的双向同时发送场景：当 Client→Server 的一条压缩 Stream 被自身网络窗口卡住时，验证反向 Server→Client 的压缩 Stream 仍能发送、解码和交付。

## 场景

- Client/Server 在真实独立 CONTROL/BULK TCP 连接上建立 Split Channel；两端都声明并协商 Zstandard 接收能力。
- Client 向 BULK Stream 提交 300 字节伪随机正文，Server 暂停 BULK 接收；等待 Client 的压缩 DATA 发送停在 128/128 字节且消息尚未完成。
- Client 先在 CONTROL Stream 发送小型压缩消息，Server 验证正文；BULK 大消息此时仍停在原信用边界。
- 在该窗口停滞仍存在期间，Server 在已打开的 CONTROL Stream 上反向发送小型压缩消息；Client 解码并按 Stream ID、消息 ID 和正文逐字节校验。
- 恢复 Server 的 BULK 接收，验证 Server 收到 Client 的完整正文，且 Client 的发送信用继续推进。

## 验证结果

- GCC Release 完整 `test_transport` 连续 5/5 通过；每次均观察到 BULK 停于 128/128、反向 CONTROL 回包完成、恢复后 BULK 推进至 330 字节。
- GCC Debug 完整 `test_transport` 通过。
- GCC Release 完整项目测试套件 91/91 通过，耗时 136.923 秒。
- 仅扩展测试夹具以在 Client 端启用 Zstandard 接收并捕获回调正文；不改生产代码、公开 API/ABI 或 wire 格式。

## 结论边界

该回归证明一端的 BULK 压缩 DATA 等待网络信用时，反向 CONTROL 压缩 DATA 可先完成解码和交付，之后原方向继续恢复。它只覆盖一个方向窗口受阻、反方向仍可写出的确定性交错，不覆盖双向窗口同时耗尽、共享 DATA TX 池满载、长期持续公平 SLA、每条消息等待界或更广预算/取消矩阵；#241/#235 仍未完成。
