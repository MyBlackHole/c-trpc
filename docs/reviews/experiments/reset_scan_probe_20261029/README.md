# RESET 全槽扫描临时计数

这是已恢复的诊断补丁，不参与 trcore、SDK 或正式测试构建。
固定基线为 `1f33cddcfa27359ac860986f0535d3ca568278a2`。

原始补丁逐字节保存在 probe.json 的 patch 字段，摘要对应提取后的 UTF-8 字节，
JSON 封装不改变补丁内容。在独立副本检出上述提交后，将该字段提取到副本内
build/probe.patch，使用 git apply 应用，再使用项目 Xmake
构建/测试入口：`xmake test -v -j1 'test_stream_reset/*'`。保留原 40k 输入和 120 秒
超时；将 TMPDIR、TMP、TEMP、XMAKE_TMPDIR 指向该副本内 build/test-tmp，并设置
PYTHONDONTWRITEBYTECODE=1。不要在 /tmp 创建测试、构建、缓存或日志文件。

补丁只为 max_streams=40000 的 RESET 进度调用计数：全槽访问量按实际完整循环
次数乘以固定容量，匹配量在 RESETTING 分支累加；每次调用边界读取所属线程 CPU
时钟，调用结束后才聚合原子计数，避免每次槽访问额外执行原子操作。
CPU 时间包括该函数同步调用的发送/回调，不是采样器给出的独占函数时间。

测试末尾读取计数并输出，原输入、状态机与断言不变。实验后已逐字节恢复两份
源码，再实现候选；正式测试 ELF 不含临时 tr_reset_scan_probe_read 符号。
当时 perf 被系统拒绝，未调整系统安全参数。原补丁和输出摘要用于复核实验范围，
不能当作固定机器的容量、RPC 尾延迟或目标平台验收。
