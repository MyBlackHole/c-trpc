# Runtime 与 Reactor Shard

**状态：CURRENT → TARGET V1**

## 1. Server Runtime

CURRENT 已建立 Phase-3 的内部 Runtime ownership seam，但仍固定单 shard：

```text
tr_server
  ├─ tr_runtime
  │    └─ shard[0]
  │         ├─ Reactor
  │         ├─ listener
  │         ├─ RPC executor
  │         └─ peer table / counters
  ├─ accept thread
  └─ event-driven reaper thread
```

`tr_runtime` 现在负责 Reactor 生命周期；Server 不再直接拥有 Reactor。
Server listener 也已从 `tr_server` 下沉到 `tr_runtime_shard[0]`：listen/close
和最终 cleanup 都由 shard 负责，中央 accept thread 当前只借用该 fd。

该变化不增加线程。Peer slot storage、capacity/high-water、reaping/ready/rejection
计数也已经从 `tr_server` 下沉到 `tr_runtime_shard[0]`。Shard 额外拥有
peer lifecycle eventfd。Channel DOWN 的 Reactor callback 只 signal 该 eventfd，
不取得 `server->lock`；reaper 被事件唤醒后再扫描并执行安全的
quiesce/destroy。

accept/reaper 仍是中央线程，peer table 的 publish/detach 目前仍借用
`server->lock` 过渡串行化；Channel/RPC 构造与销毁仍由 Server 执行。

当前内部配置仍显式要求 `shard_count == 1`。

TARGET V1 使用单进程多 Reactor：

```mermaid
flowchart TB
    NET["Network"]
    subgraph PROC["Server Process"]
        direction LR
        R0["Reactor 0\nlisten_fd 0"]
        R1["Reactor 1\nlisten_fd 1"]
        R2["Reactor 2\nlisten_fd 2"]
        W["Blocking Worker Pool"]
    end

    NET --> R0
    NET --> R1
    NET --> R2

    R0 --> W
    R1 --> W
    R2 --> W
    W --> R0
    W --> R1
    W --> R2
```

### TARGET V1 不再需要

- 中央 accept thread；
- Server event-driven reaper thread（shard-local eventfd wake，无固定轮询）；
- 每 peer 独立 timer thread。

accept、connection error、peer reclaim 和 timer 应逐步收敛到 Reactor。

## 2. Per-Reactor Listener

Linux >= 3.10 允许将 `SO_REUSEPORT` 作为基础能力。

每个 shard：

```text
socket()
  -> SO_REUSEADDR
  -> SO_REUSEPORT
  -> bind(same address)
  -> listen()
  -> epoll add
```

目标是：

```text
accept creator
    =
initial fd owner
    =
initial protocol owner
```

避免所有连接先经过中央线程再转交。

## 3. Reactor Shard

CURRENT 已落地最小内部形状：

```c
struct tr_runtime {
    uint32_t shard_count;          /* CURRENT: exactly 1 */
    struct tr_runtime_shard *shards;
};

struct tr_runtime_shard {
    uint32_t shard_id;
    struct tr_reactor *reactor;
    struct tr_rpc_executor_group *rpc_executor;
    int listen_fd;
    uint16_t bound_port;

    struct tr_runtime_peer *peers;
    uint32_t peer_capacity;
    uint32_t peer_count;
    uint32_t peer_reaping_count;
    int peer_event_fd;
};
```

Client/Server 都通过 `shard[0]` 取得 Reactor。Server 的 RPC worker group
也已从 `tr_server` 下沉为 shard-owned resource；每个 Server Endpoint 只绑定
所属 shard 的 executor。当前仍是单 shard，因此线程数量与原行为不变。

Client 暂时保留 Endpoint-local executor：Client 当前只有单 Endpoint，且 worker
生命周期与 connect/session 绑定；本阶段不为了“形式统一”改变其线程生命周期。

listener 与 peer resource ownership 已下沉；reaper wake 已事件化并归 shard。
accept execution、reaper cleanup execution、peer Channel/RPC lifecycle、
Pipeline 与 routing 仍未完全下沉，应在后续 PR 按 ownership 继续迁移。

目标逻辑结构：

```c
struct tr_runtime_shard {
    uint32_t shard_id;

    struct tr_reactor reactor;
    int listen_fd;

    struct tr_command_queue commands;
    struct tr_completion_queue completions;
    struct tr_rpc_executor_group rpc_executor;

    int listen_fd;
    struct tr_runtime_peer *peers;
    struct tr_connection_table connections;
    struct tr_pipeline_table pipelines;

    struct tr_timer_queue timers;
    struct tr_shard_stats stats;
};
```

这只是架构形状，不要求一次性引入所有字段。

## 4. Cross-Shard 操作

禁止非 owner 线程直接修改 shard-local protocol state。

统一模式：

```text
non-owner thread
    |
    | command
    v
owner Reactor
    |
    | mutate local state
    v
done
```

对于 DATA socket affinity，如果 socket 被错误 shard accept：

```text
R7 accept(fd)
  -> 读取固定 routing preface
  -> resolve owner = R3
  -> enqueue ADOPT_ROUTED_FD(fd) to R3
  -> ownership 成功转移
```

同进程内不需要 `SCM_RIGHTS`。

## 5. Client Runtime

Client CURRENT：

```text
application process
  └─ c-trpc client
       └─ internal tr_runtime
            └─ shard[0]
                 └─ 1 Reactor
```

Client 的外部 API 和线程行为未改变；Runtime 目前是内部 ownership layer。

FUTURE 才允许配置 N Reactor。

Client library 必须保持嵌入友好：

- 不 fork；
- 不 daemonize；
- 不改变宿主进程全局 signal policy；
- 不假设自己拥有整个进程；
- 不默认设置全局 CPU affinity。

## 6. Timer

CURRENT：RPC deadline 与 Channel keepalive 已迁移到 Reactor-local timer，
旧的 shared maintenance scheduler 已删除。reconnect/backoff 仍保留独立
reconnect thread，因为 connect/poll/backoff 不能直接塞进 Reactor timer callback。

Reactor-local timer 基础设施已经落地：

- 每 Reactor 一个 bounded timer queue；
- generation handle 防止 stale timer 操作；
- min-heap 维护最近 absolute `CLOCK_MONOTONIC` deadline；
- 最近 deadline 直接驱动 `epoll_wait(timeout)`；
- 单轮 timer callback 有固定 budget，避免 timer storm 长时间饿死 I/O；
- callback 只允许执行短小、非阻塞的 owner-side 状态推进。

TARGET：

```text
Reactor shard
  ├─ RPC deadline
  ├─ keepalive
  ├─ reconnect/backoff
  ├─ Pipeline timeout
  ├─ ACK batch timeout
  └─ checkpoint timer
```

迁移按 consumer 分步进行。RPC Endpoint 每个只占用一个 Reactor-local timer，
内部扫描 bounded Call table，不按 Call 创建 timer；每个 Channel 也只注册一个
默认 disarm 的 keepalive timer。reconnect 尚未迁移，legacy reconnect thread 暂时
保留；不允许为了迁移一次性同时改动 Multi-Reactor 和 Channel connection-group 语义。

## 7. CURRENT 创建阶段线程预算与回收

以下是库自身的线程增量，不包含应用、测试工具或 sanitizer 的后台线程：

| 操作 | 新增线程 |
|---|---|
| `tr_client_create()` | 启动 Runtime shard[0] 的 1 个 Reactor；尚未创建 RPC worker |
| `tr_server_create()` | 创建 single-shard Runtime（Reactor 尚未启动）+ shard[0] 的 `executor_threads` 个 RPC worker |
| `tr_server_listen()` | 0 |
| `tr_server_start()` | 3 个：Reactor、reaper、accept |

Client connect 才创建 RPC Endpoint worker；启用自动重连时仍可能增加
reconnect thread。deadline / keepalive 不再产生独立线程，也不存在每个
Client/Server 实例额外持有的闲置 maintenance scheduler。

`tests/test_runtime_threads.c` 使用测试目标独有的 pthread create/join 包装，
检查创建前后的精确增量、正常销毁、未 start 的 Server 销毁和部分启动失败回收。
它补充原有连接后线程上限测试，避免把 create 阶段的额外线程计入 baseline
后漏检。故障注入覆盖 Client Reactor、三个 shard-local worker，以及 Server 的
Reactor/reaper/accept 共七个启动点；每个成功创建的线程必须成功 join，
失败回滚不允许遗留线程或重复 join。
