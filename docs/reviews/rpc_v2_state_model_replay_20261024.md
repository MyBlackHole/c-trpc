# RPC V2 TEST-A 状态模型与轨迹回放记录

日期：2026-10-24。关联 GitHub #247 的 TEST-A；设计与实施计划见
[状态模型设计](../superpowers/specs/2026-10-24-v2-state-model-replay-design.md)
和[实施计划](../superpowers/plans/2026-10-24-v2-state-model-replay.md)。

## 交付范围

本切片新增只供开发测试使用的 Python 标准库工具：严格校验版本化 JSON 轨迹，独立
重放最多两个槽、请求/响应方向、两位代次、单个活动工作项和 16 字节抽象预算。
它记录每个事件的手写预期结果，输出稳定排序的 JSON 摘要；未知字段、重复键、非法
数值、超限输入和事件结果不匹配会明确失败。

模型覆盖 Call 终局回调恰好一次、取消后的迟到工作不复活 Call、请求可在受理前发送而
响应须在受理后发送、ONE/MANY 基数、槽代次耗尽后退役、预约/持有/归还租约及账本守恒。
`QUIESCENT` 轨迹要求每个 Call 已终局、工作完成、租约归还且预算全额可用；`PARTIAL`
轨迹仍逐事件验证，只跳过最终静默要求。

文件只进入 `scripts/`、测试 fixtures、CI 与维护文档。没有修改生产 C、安装 SDK、公开
API/ABI、线协议或第三方依赖；没有新增 CI 作业或改名既有检查。模型不是生产并发代码
的验证结果，也不代表 #247 已完成。

## 本地验证

验证命令：

```sh
mkdir -p build/test-tmp
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/test_rpc_v2_model.py -v
PYTHONDONTWRITEBYTECODE=1 TMPDIR="$PWD/build/test-tmp" \
  python3 scripts/rpc_v2_model.py \
  --trace tests/fixtures/rpc_v2_model/cancel-late-work-v1.json
```

`python3 scripts/test_rpc_v2_model.py -v` 在本地 Python 3.14.8 下通过 **28 项测试**。
覆盖包括严格解析、状态转换、状态不变、资源失败与释放、双槽租约隔离、代次退役、
静默约束、fixture 回放、CLI 稳定输出和错误事件序号。两个 Python 源文件通过
`ast.parse(..., feature_version=(3, 10))` 语法兼容检查；本机没有 Python 3.10 解释器，
因此 Python 3.10 实际运行由 Ubuntu 22.04 的 CI 作业验证。`git diff --check` 通过。

有效 CLI fixture 输出稳定 JSON；重复运行字节相同。双重终局失败 fixture 退出非零，
stderr 定位到 `events[2]`，stdout 为空。所有本地 Python 命令设置
`TMPDIR="$PWD/build/test-tmp"`，没有在 `/tmp` 写测试文件、缓存或日志。

在该 worktree 中显式使用 `xmake ... -P .` 指定项目目录，GCC Debug 与 Release 均完成
全量构建及 Xmake 测试，各 **90/90** 通过。构建产物位于本 worktree 的 `build/`。

## CI 与剩余工作

Python 自测接入既有 `build-test (gcc)` 作业，并位于 Debug/Release Xmake 测试成功之后；
现有 job/check 名称保持不变。Draft [PR #317](https://github.com/MyBlackHole/c-trpc/pull/317)
以 `d51984ffb0435bebd75984502f3fa2af902db995` 为 head，基于
`fix/v2-data-tx-ready-247`；14/14 GitHub 检查首次运行全部通过，包括 GCC/Clang 构建、
ASan/UBSan、TSan、smoke、静态分析、安装与新增/修改行格式检查。PR 仍为 Draft/Open、
尚未合并；这表示 TEST-A 的分支 CI 通过，不表示 #247 或 V2 验收完成。

TEST-B1 单组件回放另见[组件轨迹回放记录](rpc_v2_component_replay_20261024.md)；它只覆盖一条
受控 Endpoint 交错。TEST-B 后续仍需增加组件交错与调度范围；TEST-C 负责故障注入与缩减；
TEST-D 覆盖集成和平台矩阵；TEST-E 覆盖分层诊断及容量/性能。#247、#241、#242 及 V2
整体验收仍未完成。
