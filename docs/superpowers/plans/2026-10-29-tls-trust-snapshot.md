# TLS 信任源构造期快照修复计划

关联 #234、#243、#247；主会话内联执行，不使用子 agent。

## 已运行的根因证据

既有 TLS-B 计划明确“profile 创建时同步读取完毕，不保存路径”，但 Client 的
`SSL_CTX_load_verify_locations(..., ca_directory)` 与系统默认路径注册延迟目录查找器。
OpenSSL 官方文档明确哈希目录按需读取。两项真实握手回归先红：

- 创建时只信任 CA B，之后往目录加入 CA A，旧 profile 对 CA A 服务端握手返回 TR_OK。
- 创建时信任 CA A，之后删除哈希文件/目录，旧 profile 对 CA A 握手返回 -12。

日志：`build/performance-20261029/trust-directory-snapshot-{add,remove}-red.log`。
这确认信任变化发生于握手，不能用不可变上下文指针掩盖文件查找。

## 修复边界

增加独立内部 trust snapshot 模块，在 profile 发布前读取文件与哈希目录，将
X509/CRL 对象装入 SSL_CTX 的内存 store，不注册目录或 URI 延迟查找器。
显式 ca_file/ca_directory 只使用这些来源；均未配置时解析 OpenSSL 默认文件/目录，
保留环境覆盖与 OPENSSL_issetugid 的环境隔离。缺失默认来源可跳过，已有但损坏的
来源构造失败；显式错误路径提前失败，不等待网络握手。

哈希目录保留下列边界：只处理小写 8 位 hash 的 .0/.r0 组头，按连续编号加载到
首个缺口，不读取越过缺口的文件；验证对象 subject/issuer hash 与文件名一致。
支持符号链接及冒号分隔目录列表；目录文件须为单个对应 PEM 证书或 CRL，拒绝
混入其他对象；不匹配的 hash 索引不被信任，以兼容目录中共存的旧 hash 别名，
避免急切加载扩大原目录选择范围。文件来源继续允许
原 PEM 证书/CRL 合集。私钥口令回调拒绝交互提示。

目录 fd 和文件 fd 全部在构造返回前关闭；使用 openat+CLOEXEC+NONBLOCK 并核对
普通文件，避免哈希命名的 FIFO 造成等待。构造 I/O 留在外部生命周期调用线程，
正常握手不再访问信任源路径。失败释放 PEM/目录/文件和部分 profile，OOM 不发布。
不新增公开结构字段、线程或队列；安装 SDK 仍为 11 个公开头。

## 实施与验证

- [x] 加入/删除目录 CA 的真实握手红测，定位 OpenSSL 延迟加载。
- [x] 实现内部快照模块并接入 Client profile，登记 Xmake 主库和 TCP 专测源对象闭包。
- [x] 回归显式目录、系统默认环境、符号链接/目录列表、hash 冲突与序号缺口、错误输入。
- [x] 覆盖 fd/内存回滚与最终 OpenSSL 账本，GCC 全量 95/95，Clang、ASan/UBSan、TSan 各八个 TLS 目标通过。
- [x] 安装 SDK、55 单元真实编译数据库、固定格式通过；dev.178 实现提交 8f49481 已推送，精确 head CI 13/13 成功。

新的信任源加载可能增加构造期 I/O/内存；没有把 OpenSSL/RSS 总量称作硬上限。
当前小时基准继续使用旧提交 97e106c 的显式 CA 文件路径，结果单独按该提交登记。
全部临时文件、证书、日志和构建产物仅使用工作区 build/，不写 /tmp。
