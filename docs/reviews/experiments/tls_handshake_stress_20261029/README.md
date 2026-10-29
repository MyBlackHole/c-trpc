# TLS 持续握手压力诊断驱动

这是对既有 `bench_rpc` 的独立诊断编排，不增加生产钩子、测试目标或 CI 参数。
只在 Linux x86_64、Python 标准库/OpenSSL 环境验证；结果边界见
[评估](../../rpc_v2_tls_handshake_stress_20261029.md)。

驱动维持同一 Server，先建立正常 RPC Client，再以每秒 64 次的目标速率混合
静默、部分 TLS 记录逐秒滴灌、发送 ClientHello 后本地中止及明文输入。
攻击端同时最多持有 64 个 socket，Server 使用基准已有的 32-peer 配置及默认
10000ms 握手期限。正常连接以 100 RPS 发送双向 Zstd RPC，结束后释放压力源、
空闲 12 秒，再用新连接完成 64 次恢复调用。它不保证压力期间的新连接准入。

所有目录、缓存与子进程输出只使用工作区。先从干净、已提交的检出构建：

```sh
mkdir -p build/test-tmp build/performance-20261029
export TMPDIR="$PWD/build/test-tmp"
export TMP="$TMPDIR"
export TEMP="$TMPDIR"
export XMAKE_TMPDIR="$TMPDIR"
export PYTHONDONTWRITEBYTECODE=1
xmake f -c -y -m release --toolchain=gcc --crc32c_portable=n
xmake build trcore bench_rpc
cp docs/reviews/experiments/tls_handshake_stress_20261029/run_probe.py \
  build/performance-20261029/run_tls_handshake_stress.py
```

驱动复用小时诊断的固定证书位置。已有有效夹具时不覆盖；干净检出可按下面命令
生成只供本地测试的 CA/serverAuth 证书。`mkdir(exist_ok=False)` 防止覆盖旧数据；
私钥、CSR 和证书均保留在忽略的 build/ 中，不提交。

```sh
umask 077
python3 - <<'PY'
from pathlib import Path
p = Path('build/performance-20261029/tls-zstd-hour-3600s')
p.mkdir(exist_ok=False)
(p/'server.cnf').write_text(
    '[leaf]\nbasicConstraints=critical,CA:FALSE\n'
    'keyUsage=critical,digitalSignature,keyEncipherment\n'
    'extendedKeyUsage=serverAuth\n'
    'subjectAltName=DNS:server.example.test\n')
PY
openssl req -x509 -newkey rsa:2048 -sha256 -noenc -days 2 \
  -subj '/CN=c-trpc local diagnostic CA' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign' \
  -keyout build/performance-20261029/tls-zstd-hour-3600s/ca.key \
  -out build/performance-20261029/tls-zstd-hour-3600s/ca.pem
openssl req -new -newkey rsa:2048 -sha256 -noenc \
  -subj '/CN=server.example.test' \
  -keyout build/performance-20261029/tls-zstd-hour-3600s/server.key \
  -out build/performance-20261029/tls-zstd-hour-3600s/server.csr
openssl x509 -req -sha256 -days 2 -CAcreateserial \
  -in build/performance-20261029/tls-zstd-hour-3600s/server.csr \
  -CA build/performance-20261029/tls-zstd-hour-3600s/ca.pem \
  -CAkey build/performance-20261029/tls-zstd-hour-3600s/ca.key \
  -extfile build/performance-20261029/tls-zstd-hour-3600s/server.cnf \
  -extensions leaf \
  -out build/performance-20261029/tls-zstd-hour-3600s/server.pem
```

每个 label 必须未存在；运行后检查 summary.json 与两个子进程的退出状态：

```sh
python3 build/performance-20261029/run_tls_handshake_stress.py \
  30 tls-handshake-local-smoke
python3 build/performance-20261029/run_tls_handshake_stress.py \
  600 tls-handshake-local-600s
```

驱动复制冻结基准二进制并记录源码/二进制/脚本摘要；参数时长限制为 25..3600 秒。
回收只作用于本次创建的子进程与 socket，正常退出和异常路径均 join，不使用全局
信号清理或系统临时目录。资源采样来自 /proc，含脚本观测间隔；不是连续峰值证明。

600 秒观测版脚本摘要为
`451a6ffa8af702d751c909e6689b9f1c52e6cb6d0db4f54322fc42f7590dc295`。
它调用 metadata 时遗漏 tls/compression 参数，导致原始顶层标签错误；两端实际
命令及 ready/open_phase 均确认 TLS/Zstd。原始文件保留，另附更正记录。
归档脚本只修正该调用为 `metadata(binary,label,load_mode='open',compression='zstd',tls=True)`，
其摘要为 `8f2e1c8fabfd383f891fd46c24f83b599cfa51d0b3406181ea95c23811b1f8b4`，
并已重新完成 30 秒/3000 RPC 及 64 次恢复；将此一行恢复到无两个参数即可核对
观测版摘要。没有用修正后的元数据覆盖原始记录，也没有据此重标生产行为。

remote_age_s 是从发起 TCP connect 到脚本发现远端关闭的寿命，含网络和调度；
不能区分每条连接被 peer 容量拒绝还是已进入 TLS。drip_writes 统计滴灌字节排队
次数，不保证每次都在远端关闭前写出；bytes_sent 才是 send 成功字节总量。
已连接 TCP 数不等于 TLS session 准入数。RSS、PSS、请求字节、allocator live
字节及内核驻留不能互相代替；本工具不安装 OpenSSL 全局分配回调。
