# ERR-A 生产错误作用域对照审计

日期：2026-10-26。审计基线：`f6566e1733ddb27f0c2ae1dd66eda616a7715d9e`。
关联 #242/ERR-A、Draft PR #327。该审计核对分类表与生产 C 入口及现存集成测试；不把类别级对照扩大解释为 #242 或 V2 已完成。

## 生产路径对照

| 错误类别 | 生产 C 路径与行为 | 真实测试证据 | 结论 |
|---|---|---|---|
| 帧边界、CRC 或保留字段不可信任 | wire 解码拒绝帧；Channel 通过 `tr_channel_protocol_error()` 调用 `tr_reactor_abort_on_owner()` 关闭收到错误帧的物理连接 | `tests/test_transport.c::test_header_corruption`；`tests/test_rpc_record_endpoint_tcp.c::test_connection_level_error_closes_all_calls` | 已验证连接关闭，活动 Client/Server 调用各终结一次，已完成结果不被伪造为成功 |
| 控制帧身份、lane、代次、流状态或信用不合法 | OPEN、WINDOW_UPDATE、CLOSE、RESET、GOAWAY 等处理器在连接/流身份或状态无法证明时调用 `tr_channel_protocol_error()`；精确物理连接被终止 | `tests/test_stream_reset.c::test_invalid` 覆盖未知 RESET、冲突重复 RESET、晚到 DATA、超额信用及墓碑淘汰后的 RESET；`test_split_frontier` 和 `test_data_type_flag_without_reset` 覆盖物理连接归属与无 RESET 能力 | 已验证代表性连接级负例；RESET/DATA/FIFO 全组合仍由 ERR-B/C/D 矩阵继续验收 |
| 已知活动流上的可信边界语义错误 | `tr_channel_handle_data()` 与压缩 DATA 路径将可定位错误交给 `tr_channel_fail_stream_receive_on_owner()`；仅在流仍可 RESET、远端仍开放且信用可信时隔离目标流，否则关闭精确连接 | `tests/test_stream_reset.c::test_receive_isolation`、`test_data_type_flag_isolation`；`tests/test_rpc_record_stream.c::test_compressed_receive_rejects_invalid_zstd`、`test_compressed_receive_rejects_trailing_and_size_mismatch` | 已验证目标流 RESET、同连接健康流继续；流身份/信用不可信时不会降级成丢帧 |
| 异步变换内部损坏或违反成功交付契约 | `tr_channel_message_transform_done()` 将 `BAD_DATA`、`LIMIT`、`NOMEM` 保持在流级处置；`INTERNAL`、未知 outcome、成功 outcome 缺少有效输出或输出视图非法时，先释放任务载荷，再以 `TR_ERR_SYS` 中止精确物理连接 | `tests/test_transport.c::test_channel_compressed_internal_failure_closes_connection` 注入真实 Channel 异步完成边界，分别覆盖 `INTERNAL` 与缺少输出的 `OK`；同连接两条流均终结、不发送 RESET，双端连接关闭且预算归零 | 已补生产路径负例；坏压缩正文仍由既有测试验证为目标流 RESET |
| RPC Record 语义错误、业务拒绝、方法或路由失败 | Record 解析错误终结目标 RPC；业务拒绝和路由结果映射为调用 FINAL_STATUS，不把应用拒绝伪装成 Channel 连接故障 | `tests/test_rpc_record_endpoint_tcp.c::test_semantic_record_error_isolation`、`test_business_rejection_preserves_active_sibling`、`test_router_rejection_preserves_active_sibling` | 已验证重复 OPEN 只终止目标调用，活动及后续同伴调用仍在同一 TCP 连接完成 |
| 本地接收、解码或任务预算压力 | Channel 重组失败由上述单流 RESET 路径归为 `TR_ERR_NOMEM`；RPC Record quota/workspace 失败产生目标调用 `RESOURCE_EXHAUSTED`，不声称对端帧损坏 | `tests/test_stream_reset.c::test_receive_isolation(resource=1)`；`tests/test_rpc_record_stream.c::test_receive_quota_preserves_peer`、`test_compressed_workspace_exhaustion_preserves_peer`；控制面并进由 `docs/reviews/rpc_v2_compressed_quota_control_progress_20261025.md` 中的 `test_compressed_workspace_exhaustion_preserves_peer` 记录覆盖 | 目标调用隔离和控制路径前进已有证据；长时公平、全预算域和压力阈值仍属于 #241/#235 的未完成验收 |
| TLS 身份认证失败 | TLS 握手/验证失败使物理连接无法进入已认证的连接组路由；不会作为单流 RPC FINAL_STATUS 放行 | `tests/test_tls_openssl.c::test_required_client_auth_rejects_wrong_ca`、`test_optional_client_auth_still_rejects_wrong_ca`；`tests/test_pipeline_listener_tls.c::test_tls_group_routes_only_after_authentication` | 已验证认证失败关闭该连接并阻止其 DATA route；TLS 加密记录完整性损坏尚无端到端负例，继续由 #234 验收 |
| 终局、任务和缓冲区回收 | Channel 连接 abort 在所有者上退役该物理连接流；RPC Record 与异步压缩路径等待取消/完成后回收任务与缓冲 | `test_connection_level_error_closes_all_calls` 的终局计数和活动流归零；`tests/test_rpc_record_stream.c::test_compressed_receive_reset_retires_pending_stream`、`test_compressed_receive_owner_stopped_reaps_pending_task` | 对应路径有直接回归；所有故障交错下的统一资源审计仍未完成，不据此勾选 #242 的全局回收项 |

## 对 #242 七项验收的影响

本对照补齐 ERR-A 的分类到生产 C 入口族映射，并将每类代表性集成证据固定到具体测试。它支持 #242 的前两项代表性行为和第三项的部分资源/控制路径证据；不关闭以下整体验收：

- RESET 与 DATA/STATUS/半关闭、晚到帧及墓碑的完整并发矩阵；
- 单连接 GOAWAY 与连接组多成员接受边界、未知结果和 epoch 切换的一致语义；
- 配额压力下的长期公平恢复，以及所有终局路径的任务/缓冲回收证明。

因此 #242 仍保持开放。接下来的实现应继续按 ERR-B/C/D/E 与其原始验收项收敛，不以本审计文档代替运行时验收。
