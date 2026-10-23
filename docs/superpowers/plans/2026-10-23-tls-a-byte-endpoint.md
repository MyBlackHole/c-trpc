# TLS-A 字节端点与 BIO 原型实施计划

**目标：** 为 #234 确定可在单 Reactor 所有者上安全推进的 TLS 字节端点，
用真实非阻塞连接验证 BIO 选择及状态/所有权合同。

**设计：** [ADR-009](../../architecture/decisions/ADR-009-tls-byte-endpoint.md)。
本计划只完成其中 TLS-A；TLS-B/C/D/E 仍须各自的实现与验收。

**技术栈：** C11、OpenSSL 3.0 兼容 API、Linux socketpair/epoll LT、
Xmake 3.1.1、GCC/Clang、ASan/UBSan/TSan。

**执行约束：** 由主会话内联实施，不使用子 agent。测试临时目录、
日志、证书、缓存和构建产物仅放工作区 `build/`；不写 `/tmp`。
版本号只随行为代码变更提升；ADR 文档本身不宣称 TLS 可用。

## 文件职责

- `tests/test_tls_bio_probe.c`：仅供测试的真实 TLS 1.3 socket BIO 与
  memory BIO 对照，不进入安装库；在内存中创建测试 CA、证书和密钥。
- `src/io/byte_endpoint.h`：内部字节端点的状态、等待方向、单次推进结果
  与稳定写责任；不暴露 OpenSSL 对象到公开 SDK。
- `src/io/byte_endpoint.c`：明文端点及可注入 TLS 驱动的单所有者状态机；
  Reactor 接入前不改变现有明文收发路径。
- `tests/test_byte_endpoint.c`：以可控的 TLS 驱动测试 WANT 交叉、
  无进度、取消、绝对期限、稳定写与关闭回收。
- `xmake.lua`：仅为 TLS-A 测试目标链接系统 OpenSSL；生产依赖和安装
  消费端在 TLS-B 的真实后端接入时确定。
- `docs/reviews/rpc_v2_tls_a_20261023.md`：记录 RED/GREEN、两种 BIO 的
 资源/复制/事件语义与选型证据，不写“已支持 TLS”。

## 任务 1：真实 BIO 对照

- [ ] 测试先定义可观察结果：同一测试证书、socketpair、TLS 1.3
  双端握手后双向传递确定字节；人为限制 socket 缓冲并施加拆包、
  EAGAIN、半关闭与中途 EOF。分别运行 socket BIO 和 memory BIO。
- [ ] 在 `tests/test_tls_bio_probe.c` 中用内存 EVP/X509 生成仅供测试的
  CA 与 SAN 为 `localhost` 的服务证书；客户端必须验证链和 SAN，
  错误 CA/主机名须失败。不得把测试私钥写入源码或工作区外文件。
- [ ] `xmake.lua` 添加非默认 `test_tls_bio_probe`，以系统 OpenSSL 3.0
  兼容头文件和库构建。运行时设置 `TMPDIR="$PWD/build/test-tmp"`
  与 `XMAKE_TMPDIR`，输出记录在 `build/test-tmp/`。
- [ ] 记录每种 BIO 的原操作重试约束、额外自管密文队列容量、
  复制次数、close_notify 与异常 EOF 结果。若 socket BIO 不能保持
  稳定重试或预算/事件闭环，依据实测更改 ADR-009 的候选选择。

## 任务 2：端点状态机的可失败合同

- [ ] 先在 `tests/test_byte_endpoint.c` 写失败测试：模拟 handshake
  `WANT_READ -> WANT_WRITE -> success`，写入 `WANT_WRITE -> WANT_READ
  -> success`，无进度不得重入自旋；模拟绝对期限到期、取消、
  fatal 和 close_notify 后必须只完成一次，底层对象仅在事件解绑后释放。
- [ ] 定义内部接口。`tr_byte_endpoint_step()` 返回状态、消费的明文字节
  和 `READ/WRITE` 等待位；`tr_byte_endpoint_read()` 与
  `tr_byte_endpoint_write()` 的 `TR_AGAIN` 保留输入所有权，成功只消费
  返回的长度。端点状态只能沿 `TCP_READY -> NEGOTIATING ->
  AUTHENTICATED -> CLOSING -> DETACHED -> RELEASED` 前进。
- [ ] 运行 `xmake test -v -j1 'test_byte_endpoint/*'` 得到由接口缺失
  导致的 RED，再实现 `src/io/byte_endpoint.c` 的最小状态驱动与明文
  适配。没有 TLS 后端时的 `required` 必须返回 `TR_ERR_UNSUPPORTED`，
  不准回退成明文成功。
- [ ] 在 `xmake.lua` 将状态驱动编入 `trcore`，测试通过后核对
  Reactor 原 `sendmsg`/`recv` 路径未被这个阶段悄悄改写。

## 任务 3：证据与集成

- [ ] 由主会话按安全与代码审查清单复核：状态单调、fd/SSL 所有权、
  失败回滚、整数界限、证书验证、待发送缓冲稳定、无明文降级。
- [ ] GCC release 全量、受影响 ASan/UBSan/TSan、Clang 和格式检查
  通过；CI 失败先取根因，不移除门禁。
- [ ] 更新 ADR-009 的 BIO 决策状态与证据，写评估文档。提交、
  创建 PR、等待全部 CI，合入后在实际 main 复核并更新 #234，
  用精确 lease 清理本次远程分支。

TLS-A 完成不代表认证、普通 V2 业务、mTLS、连接组或安全发行通过。
