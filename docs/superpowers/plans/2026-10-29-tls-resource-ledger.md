# TLS 分配账本与构造回滚实施计划

关联 #234、#241、#247。主会话内联执行，不使用子 agent。

## 设计与边界

在独立 exec 测试进程、首次 OpenSSL 分配前安装三种内存回调，按请求字节记录 live、
peak、block 数与分配失败。使用 max_align_t 头部并在 realloc 失败时保留旧指针/账目；
回调计数由 mutex 保护，不在锁内调用 OpenSSL。正常库和其他测试不安装全局回调。
测试进程显式清理 OpenSSL 后不得重新初始化；每个场景都启动新进程。

真实 TCP/mTLS 同时保留 1/4/16 对会话，双向各传 64 KiB；调用方先释放 profile 引用，
验证会话持有与 BIO 方法表的完整寿命。记录 OpenSSL 请求峰值、本库 session/BIO 包装
预算和 SO_SNDBUF/SO_RCVBUF 配置容量；这三种数据不可合并冒充 RSS 或内核内存硬上限。
另注入会话构造中的第 1/2/3/4/8 次 OpenSSL 分配、本库 calloc、profile BIO 方法构造失败，
检查返回值、空输出、调用方 fd、预算回滚、重试及最终清理归零。

## 已发现根因及生产方案

1. session_create 的预算预留把 ret 置为 TR_OK，后续 calloc/SSL_new 失败跳转时未恢复
   NOMEM，出现“成功但 session 为空”。在完成预算预留后明确设置后续失败默认状态。
2. 全局 once BIO_METHOD 没有释放域，OpenSSL_cleanup 后实测仍有 138 字节/两个块。
   方法表改由不可变 profile 拥有，会话取得 profile 强引用；SSL/BIO 释放后再归还，
   最后一个 profile 引用释放时销毁方法表。无需注册跨库全局退出回调或新增锁/线程。

每个 profile 增加一个方法表和一个指针；每个 session 增加一个 profile 指针和一对
引用计数操作。session 的 sizeof 仍由原有预算分配前预留覆盖；profile/OpenSSL/内核
完整产品预算及恶意输入峰值仍未完成，不能以此关闭 TLS-E。

## 执行与验收

- [x] 先验证账本缺失会失败；实现回调自测，覆盖对齐、realloc 失败与清零释放。
- [x] 用真实 TLS 和 OOM 场景复现成功空会话与清理残留。
- [x] 修复返回状态与 profile/BIO 所有权，四个 TLS 目标首次通过。
- [x] 增补本库两处 calloc、方法表构造失败和提前释放 profile 场景，运行最终定向验证。
- [x] 自审补充 160 次 profile 轮换，先复现类型编号耗尽，再改为无堆 once 编号。
- [x] GCC Release 全量 95/95，Clang Release、ASan/UBSan、TSan 各 8/8 定向。
- [x] 固定版本格式整理、安装 SDK 四编译器、性能边界说明；记录请求峰值和剩余风险。
- [x] 已提交源码的 clang-format 18.1.8 检查通过；包含本修复的 dev.175 精确 head `f1c0b0f8d8a394bebaf0a0885ff3657b4ccbc2ba` 远端 13/13 成功。
- [x] dev.174 按 bug 提交格式提交为 `cd62a89d2c1f7946b6e6e91c560c3c739af16c53` 并推送至 Draft PR #333；#234/#241/#247 与发布目标保持开放。

全部临时文件、证书和日志使用工作区 build/；不在 /tmp 写入。
