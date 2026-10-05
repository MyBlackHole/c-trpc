# ADR-006：拆分门面使用显式连接组绑定标识

**状态：提议中**

## 背景

低层 `Channel` 已支持 CONTROL/BULK 两条独立物理连接，
但高层客户端/服务端门面当前只建立共享连接。

服务端进入拆分模式后，两条 TCP 套接字会被独立接收。
不能使用接收顺序、源 IP/端口或“第一条 CONTROL、第二条 BULK”这类启发式方法配对；
并发客户端、NAT 和重连都会造成歧义。错误配对会把不同客户端的协议状态拼到同一个
Channel 中。

## 决策

客户端在 Reactor 所有权转移前完成 `BIND/BIND_ACK`；
服务端在识别出完整连接组后才把两条 fd 交给 Reactor：

```text
客户端
  连接 CONTROL + BULK 原始非阻塞 fd
  -> BIND(group_id, CONTROL/BULK)
  -> 等待两条连接上匹配的 BIND_ACK
  -> Reactor 接管两条连接
  -> 创建拆分 Channel
  -> 进入现有 HELLO

服务端
  接收原始非阻塞 fd
  -> 有界等待绑定解析器
  -> 使用相同 group_id 配对一条 CONTROL + 一条 BULK
  -> Reactor 接管两条连接
  -> 安装最终延迟 Channel 处理器 + RPC 方法
  -> Reactor 在 CONTROL 和 BULK 上加入 BIND_ACK
  -> 启动 Channel，加入现有 HELLO
```

服务端不能在原始 ACK 完成后才接管：客户端收到 ACK 后可能立即发送 `HELLO`，
而服务端此时还没有 Reactor 处理器，会出现首帧被空处理器丢弃的窗口。

服务端也不使用临时 BIND 处理器。配对完成后直接安装最终 Channel 处理器，
然后把两个 `BIND_ACK` 命令先于 `tr_channel_start()` 的 `HELLO` 命令入队。
同一 Reactor 命令 FIFO 与每条 TCP 连接的 TX FIFO 保证该通道上
`BIND_ACK -> HELLO` 的线协议顺序。

客户端仍在 Reactor 外读取固定长度 ACK；如果 ACK 后的 `HELLO` 已经进入套接字接收队列，
客户端只消费 ACK，`HELLO` 留在内核缓冲区，fd 接管后由正常 Reactor 解析器处理。

接收/绑定循环把监听器与等待中的原始套接字一起 `poll()`，
每个 fd 只推进已经就绪的读取字节；慢速/部分发送对端不得阻塞其他连接接收或配对。

`BIND` / `BIND_ACK` 都是普通传输帧。载荷固定 32 字节：

```text
magic       4   "TRB1"
version     2   小端，V1 = 1
lane        2   CONTROL=0, BULK=1
identity   16   密码学强随机字节
reserved    8   必须为零
```

标识由 Linux `getrandom()` 生成；失败即连接失败，不允许弱随机回退。

服务端等待绑定状态必须满足固定容量、单调时钟超时、每 fd 独立增量读取偏移；
同一标识最多一条 CONTROL 和一条 BULK。重复通道、第三条连接、非法/全零标识、
超时/关闭/错误都必须拒绝并释放。配对完成前不得创建就绪对端。

## 安全边界

128 位不可预测标识用于抵抗**连接混淆/离路径劫持**，不是对端认证。
明文 TCP 上的路径内攻击者仍可观察/修改 `BIND`。
面向不可信网络时，必须先建立 TLS/mTLS，或未来使用已认证会话密钥保护绑定证明。

TLS 决定“对端是谁”；`BIND` 决定“两条传输连接属于哪个逻辑连接组”。

## 影响

优点：

- 不依赖源地址或时序启发式；
- 接收/绑定保持非阻塞；
- 服务端 Reactor 从第一帧开始就使用最终 Channel 处理器；
- 客户端不需要临时 Reactor 处理器；
- 与未来多 Reactor 路由/所有者选择兼容。

代价：

- 服务端增加有界等待绑定生命周期；
- 拆分客户端连接增加 `BIND/BIND_ACK` 握手；
- 服务端需要很小的专用 ACK 载荷资源池；
- 线协议增加 `BIND` / `BIND_ACK`；
- TLS 未接入前只解决配对标识，不解决对端认证。
