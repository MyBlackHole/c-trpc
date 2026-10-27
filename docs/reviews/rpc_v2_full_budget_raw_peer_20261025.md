# 监听器消息预算满额时 RAW 同伴进展回归

日期：2026-10-25。版本：`2.0.0-dev.80 -> 2.0.0-dev.81`。关联 #241、#235。

## 验收目标

验证 `pipeline_listener` 的消息预算已经精确满额，且一条真实 Zstandard 解码输出仍由应用持有时，另一条独立 DATA 成员上的 RAW 消息仍能交付。该场景补充 dev.80 的公开连接组慢消费者证据；本记录只覆盖监听器组件的满额路径。

## 场景与断言

- 使用真实回环 TCP 建立一个 CONTROL 与两个 DATA 成员，并检查两个 transfer 的 DATA 索引和代次不同。
- 第一 DATA 成员发送有效 Zstandard 帧。测试任务保留输入，随后调用真实消息变换完成解码，并在 listener 所有者 Reactor 上交付结果；应用回调接管解码缓冲。
- 读取预算当前值后，测试通过 `tr_memory_budget_reserve` 预约全部剩余额度，并断言 `current_bytes == limit_bytes`。该预约用于确定性模拟其他已记账持有者，不对应额外实际分配。
- 第二 DATA 成员发送一字节 RAW 消息。测试必须观察到回调、校验消息正文，并确认 RAW 交付前后预算仍精确满额，第一条应用持有输出也仍有效。
- 释放测试预约后，预算恢复到仅保留第一条输出的值；释放输出后恢复到 listener 常驻值；停止并销毁 listener 后预算归零。

## 测试灵敏度与验证

先运行当前实现，新增回归通过。随后临时注入负向变体，让 RAW 路径额外预约 1 字节消息预算；满额时第二 DATA 连接未能交付，`wait_data_frames` 按预期失败。变体已撤销，生产源码未保留改动；恢复后的 GCC Release 定向测试通过。

命令：

```sh
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
  xmake build -P . test_pipeline_listener
TMPDIR="$PWD/build/test-tmp" XMAKE_TMPDIR="$PWD/build/test-tmp" \
  xmake test -P . -v -j1 'test_pipeline_listener/*'
```

`-P .` 显式选择当前嵌套工作树的 Xmake 项目，避免误用其父目录项目配置。GCC Release 定向测试连续 5 次通过；GCC ASan/UBSan（`detect_leaks=1`）和 TSan 定向测试各通过一次；完整 GCC Release 套件 91/91 通过，耗时 140.952 秒。提交 `8f6fdb1cf47e17c3943e98087c71ed1cf0e759ea` 的 GitHub push CI、PR CI 与 RPC benchmark smoke 均通过：[`40166369925`](https://github.com/MyBlackHole/c-trpc/actions/runs/40166369925)、[`40166375224`](https://github.com/MyBlackHole/c-trpc/actions/runs/40166375224)、[`40166375029`](https://github.com/MyBlackHole/c-trpc/actions/runs/40166375029)。

## 结论边界

本次只增加测试、评估记录、剩余工作台账和开发版本号；没有改生产运行逻辑、API/ABI 或 wire。测试精确填满的是监听器使用的消息预算，预约额度由测试直接注入；这不证明普通 Server 派生全局预算能由自然应用持有耗尽，也不验证多个应用消息并发持有或持续饱和公平性。#241 仍保持开放。
