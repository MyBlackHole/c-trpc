# RPC V2 连接组成员代次防重放设计

## 问题

连接组 `DATA_OFFER` 的 `member_generation` 当前由单个 Pipeline 的槽位计数器
生成。CONTROL 关闭后，旧 Pipeline 会销毁；使用相同 `(pipeline_id, epoch)`
重新建立 CONTROL 时，新 Pipeline 的 DATA 槽位代次又从 1 开始。

若旧 DATA 套接字只发送了部分 TRR1 前导，它不会成为已附着成员，也不会随旧
CONTROL 拆除关闭。旧 CONTROL 退出、相同组身份重新建立并重新签发相同槽位后，
该套接字可以补齐旧前导。若槽位代次复用，注册表会把旧能力当作新预留并附着，
使旧连接跨过连接组 epoch 生命周期边界。

## 目标与非目标

- 同一个 Pipeline 注册表生命周期内，每次已发布的 DATA 能力都使用此前未用过的
  非零 `member_generation`，跨 Pipeline 实例也不复用。
- 旧 DATA 前导不能附着到同一 `(pipeline_id, epoch)` 的新 Pipeline，也不能通过
  迟到 `DATA_CANCEL` 取消新预留。
- 保留 TRR1/TRC1 长度和字段布局；不增加队列、线程或按 DATA 分配的历史表。
- 代次耗尽时明确拒绝新能力，不回绕、不复用；返回 `TR_ERR_STATE`，不改变
  Pipeline 预留状态。
- 不重新定义应用提供的 `pipeline_id/epoch`，不提供跨服务进程重启的持久防重放。
  调用方仍须为服务进程重启后的新连接组使用新的 epoch。

## 设计

把 DATA 成员代次分配器放到分片 Pipeline 注册表中。它保存一个 `uint32_t`
单调计数器，初值为 0。创建一个 DATA 预留时，在所属 Reactor 所有者的同一事务中：

1. 验证 Pipeline 的 CONTROL 仍有效，并确认有空闲 DATA 槽；容量不足时返回
   `TR_AGAIN`，不消耗代次。
2. 若计数器已为 `UINT32_MAX`，返回 `TR_ERR_STATE`，不更改 Pipeline。
3. 递增计数器，并把该唯一值写入本次空闲槽及对应 TRC1/TRR1 能力。

编码或发送准入失败时，释放精确槽位，但不退还已分配的代次。允许存在代次空洞，
避免任何已经暴露给调用方的能力再次生效。现有直接 Pipeline 单测仍可使用局部槽位
代次；生产连接组统一通过注册表分配器创建 DATA 能力。

## 不变量

- `member_generation != 0`。
- 在一个注册表生命周期内，任意两个不同 DATA 预留的代次不同。
- 只有完全匹配当前 `(pipeline_id, epoch, member_index, member_generation)` 的
  预留可以附着或取消。
- `TR_AGAIN`、代次耗尽及构造失败都不会留下预留或改变现有 DATA/Stream 状态。
- 注册表计数器只在 Reactor 所有者访问；无锁、无外部可见指针。

## 验收

在真实 TCP listener 上建立旧 CONTROL，获取 DATA 能力并只发送前 47 字节 TRR1；
关闭旧 CONTROL，保留尚未完成的 DATA 前导；用相同组 ID/epoch 建立新 CONTROL
并再次获取槽位 0；新代次必须不同。补齐旧前导后，旧连接必须被拒绝且不得消耗
新预留；用新前导连接仍须成功附着。另以确定性单元测试验证 `UINT32_MAX`
后的分配拒绝且不写输出、不修改计数器。

本切片只补 #242 的连接组 epoch/迟到 DATA 能力组合，不宣称完成该 Issue 的
GOAWAY 接受边界、全部 RESET/错误分类或完整故障矩阵。
