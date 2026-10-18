# 早期 Endpoint 方案对照与真实网络回归补齐

## 范围与固定输入

以 #281 的 `913123ea6f3b695e64594c55dd61899e50c19ffe` 为基线，
完整树为 `cc8a152a3c236d073e8a4703a09ff04c6f9211e5`。
本轮比较 #266、#267、#268 的原生源码归档，不按标题或旧绿色检查推断覆盖。
三个旧候选的源码树分别核对为：

| 候选 | head | 完整树 |
|---|---|---|
| #266 | `f63fe8ad0f48760456da67efce07f974f712666c` | `a3a1240dc7d51df3ff36e1cffc77a91b2fb9d08e` |
| #267 | `bb368f4fc01a3bf6cca80e7ec8c8986e865ad3d4` | `f57ea80631bdda985bd25fdf5cb482d44ba94261` |
| #268 | `7bcd4e05397dc22ee86b881934b05c7b850cbf47` | `fbbaa9e1fec7cedadc0343a6b48e3992a1647a37` |

归档来自三个 PR 自身的 RPC 冒烟源码制品，分别为 12016740765、12018276146、
12045096974。ZIP 的大小、SHA-256 和解压后的完整树均已核对；源码归档不是
历史测试全部通过的证明。#266 的原始完整 CI 为失败，本轮不修改或重跑它。

## 不变量映射

表中函数来自当前源码；未注明文件的 `test_*` 为端点契约或真实 TCP 目标。
“契约”表示下层替身测试，不替代实际网络行为。

| 早期约束 | 当前实现与验证 | 处置 |
|---|---|---|
| 专用 Channel、匹配 owner、借用共享执行器 | `tr_endpoint_owner`、端点头文件生命周期合同；契约 `test_failures` 与停止域用例 | 保留，不恢复第二套线程池或 handler 注册表 |
| 有界周期推进、完成与发送公平性 | `pump_on_owner` 的 completion/progress 限额、已有定时器；契约延后事件超预算测试 | 保留，不宣称零轮询成本 |
| 分配、timer register/arm、handler 发布失败 | 契约 `test_failures` 三处分配及四类启动失败、原对象保留和预算归零 | 已覆盖，不能把替身失败全部称为真实 syscall 故障 |
| 独立错误帧、TX 引用长于端点 | `tr_endpoint_frames_put/frame_release`；契约 `test_tx_outlives_host` | 本轮另补真实 sendmsg 阻塞与对端收包 |
| 已租用错误帧不可被同槽新代次改写 | busy 的 release/acquire、独立 arena 引用；契约 `test_frame_slot_reuse` | 保留，禁止以墓碑退役代替 TX 释放 |
| 业务成功 FINAL 与本地错误 FINAL 均背压 | 派发器与宿主发送阶段分别持有；原 TCP 只单独覆盖错误 FINAL | 同一现有用例扩为成功/错误两支 |
| FINAL 接受先于普通 CLOSE，CLOSE AGAIN 不重复 FINAL | `tr_endpoint_progress`、信用停止、原顺序测试 | 两支 TCP 均检查等待和终局恰好一次 |
| 错误 FINAL 期限耗尽不得宣称已送达 | `final_due`、预留 RESET、契约 `test_deadline` | 补回真实 TCP 超时；不复制旧 CLOSE/UNAVAILABLE 期望 |
| 128 字节真实信用窗口恢复 | TCP `test_real_window` 保留原 RX 后释放，正文恢复后完成 | 已覆盖，非任意大消息活性证明 |
| 四种基数、空流/空消息、正文内容 | TCP `test_shapes` 和公共 SDK 对应场景 | 当前 BODY 上限更大，保留旧 4096 字节约束的实际内容验证 |
| 未知方法、非法元数据后同连接仍可用 | TCP `test_rejection`、底层记录校验及路由测试 | 已有，保留合法调用继续成功断言 |
| 调用容量拒绝不能覆盖未静默旧工作 | 宿主容量为传输槽+派发容量、精确三元身份；契约 `test_generations_under_load` | 保留，旧逐传输槽覆盖方案不再叠加 |
| 真实传输槽在完成双向关闭后复用 | 旧 #268 `test_real_slot_reuse` 与当前契约代次测试层次不同 | 本轮补回同一 TCP 槽位八代往返、绑定销毁及宿主退役 |
| 忽略取消的迟到业务不得产生成功 | 当前 RESET、派发器真实工作静默、TCP `test_peer_reset`、停止恢复 | 新协议可先 RESET 网络；业务通知仍等待工作退出 |
| sendv 栈内真正断连不能遗失或重入终局 | 有界 pending 事件队列、契约内联事件测试 | 本轮补真实 `abort_on_owner` 的网络断连 |
| handler 解绑失败后新的 OPEN 仍可安全拒绝 | TCP `test_shutdown_failure(0)`；dispatch 已销毁但 handler 残留 | 额外测试客户端完全不发 RPC OPEN 的情形 |
| timer 注销失败保留对象及残余回调 | TCP `test_shutdown_failure(1)`、契约 `test_failures` | 已覆盖，不忽略注销错误 |
| 父线程正常停止或 epoll EIO 后接管 | TCP `test_stopped_recovery(false/true)`、恢复租约和外层共享执行器收敛 | 已有两个真实路径，非任意致命退出状态空间证明 |
| 终局、FINAL 接受、网络退休与业务静默分层 | result.final_accepted、close/reset/dead、reported 与派发器任务责任 | 保留，accepted 不等于写出或对端确认 |
| 端点不得跨连接替换复用 | 当前专用 Channel 合同、门面重建/停止域回收 | 不恢复 #266 私有 abort/result 字段或连接下线自动排空策略 |

## 明确的协议演进，不是可机械搬运的旧断言

#267 曾要求 RPC OPEN 前拒绝不能发送 FINAL。当前 `rpc_record.c` 已明确允许
未 opened 状态接纳响应方向的非 OK FINAL，保持 opened=false，并禁止后续 OPEN
使调用复活；`test_rpc_record.c` 已覆盖其状态转移。当前客户端先建立绑定再接纳
传输 OPENED，也不同于早期夹具中的未绑定窗口。因此本轮保留当前早拒绝合同，
并增加不发送 RPC OPEN 的真实网络拒绝回归，而不是恢复旧禁止规则。

本轮初次尝试将旧“必须只 RESET”的断言直接搬入契约和 TCP 测试，GCC/Clang
契约以及 GCC TCP 均在断言处失败。继续核对记录状态机后确认该断言不符合现行
合同，已经完整撤回，没有修改生产代码。这些失败只作为废弃实验记录，不能算作
发现生产缺陷或有效故障注入证据。

另一处差异是最终错误发送超时：旧实现普通 CLOSE 后对端观察 UNAVAILABLE；
当前预留 RESET 使用有限传输原因映射。新测试中 UNIMPLEMENTED 未以 FINAL 送达，
客户端只看到 INTERNAL，服务端仍保存 UNIMPLEMENTED 与 final_accepted=false。
不得伪造详细错误到达，也不得为了复制旧测试而去掉 RESET。

## 本轮代码与本地验证

生产代码完全未改。只扩充既有 `test_rpc_record_endpoint_tcp`，并在该目标的
Xmake 链接包装中加入 sendmsg。现有23个生产对象的独立链接、旧RPC符号禁入、
85个测试目标、120秒超时和所有CI作业标识保持原样。测试阻塞的是指定服务器
描述符的真实 sendmsg，解除前确认对端无终局，端点已销毁但帧预算仍在；解除后
原始错误抵达且预算恢复到执行器基线，最终全部归零。

GCC14.2 O2、Clang17 O2、Clang ASan/UBSan、GCC TSan 四模式各编译上述23个
生产对象，并各运行端点契约与真实TCP两个程序，共8次程序通过。未出现对应
检查器诊断。这是补充直接编译，不是本地原生Xmake、全部45单元、完整85项或
安装SDK验收；系统没有Xmake，不能将此结果写成全库通过。

两个独立负向对照只在临时生产对象中去掉错误帧的TX强引用；GCC/Clang均成功
编译链接，在新增 `test_tx_survives_endpoint` 的“端点销毁后预算仍大于执行器
预算”断言处 SIGABRT。对照不进入产品，原源码不变。固定18.1.8只整理改变行；
格式门禁和自测结果按单独日志记录。

## 合并和发布边界

不合并或关闭旧 #266/#267/#268，不把其私有接口逐一恢复；以上映射解释为何保留
当前单一宿主，并补充实际缺失的网络层证据。这是指定端点及相邻合同的有限复核，
不是全仓库独立安全审计、维护者批准或生产发布。

本补丁进入 #281 后必须核对新 head 的 main-base 原生 CI、固定静态、格式和安装，
不能继承 `913123ea` 的绿灯。继续保持同一候选分支，不新增 HELLO 自动恢复、TLS、
压缩、V1删除或性能指标承诺；最终发布身份与主线落地另行确认。
