# 消息形状与演进的独立参考实验

本目录保存可丢弃的 Python 语义参考，不是生产 CTM1、上游 SBE 或 Protobuf 实现。
它不接入根 Xmake、trcore、公开安装或既有 CI。完整结论及限制见
[形状对照记录](../../custom_message_shape_comparison_20261029.md)。

## 复现

需要 Git、Python 3.9 或更新版和 Xmake；原运行 Python 3.14.8、Xmake 3.1.2，
Linux x86_64。从仓库根复制到新的工作区 build/ 子目录：

```sh
mkdir -p build/test-tmp build/codec-shapes-reproduce
cp docs/reviews/experiments/ctm_shape_reference_20261029/* build/codec-shapes-reproduce/
TMPDIR="$PWD/build/test-tmp" TMP="$PWD/build/test-tmp" TEMP="$PWD/build/test-tmp" \
XMAKE_TMPDIR="$PWD/build/test-tmp" PYTHONDONTWRITEBYTECODE=1 \
python3 -B build/codec-shapes-reproduce/run_probe.py
```

运行器强制自身位于仓库根的 build/ 直接子目录，避免直接在存档旁写结果。
它在该独立项目内通过 `xmake check_shapes -P .` 执行检查；`-P .` 不可省略。
Xmake 自定义 task 同时需要 menu 和 options 声明，不能把早期入口失败计为检查通过。
断言关闭时拒绝运行；使用 `-B` 和环境配置避免字节码缓存及 /tmp 临时文件。

两次完整基线各应退出 0，中间六个负对照应因对应断言退出非零。运行器核对负对照
的实际 mutation/format 事件与断言标记，避免把入口错误误认为捕获缺陷。
负对照只在各自 Python 子进程替换函数，结束后不改源文件；最后两次结果逐字节一致。

## 模型边界

输入 Layout 由实验预先构造，不是外部 schema 或 schema 编译器。每个样例拥有
匹配的固定块布局，只有稀疏样例预留 32 个槽；数组/bytes 样例没有借用这 32 个槽。
两种候选均使用四字节根长度，正文字节不计它。嵌套为有限、无环的子节点链。
字段均显式区分缺席、零、空 bytes 和空数组；数组为固定四字节小端 u32。

紧凑候选按稳定编号顺序使用最短 tag/length varint，标量值固定宽度；未知字段保留
完整原始片段。固定块候选使用 block length、writer revision、八字节 presence、
固定数值槽、带 u32 长度的变量段；v2 追加固定 counter 和变长 token。
presence 布局、头大小及尾部组织是本实验的选择，不是上游 SBE 的统一要求。

旧读者只按块边界携带未知固定区与尾部，不了解未来字段大小；新旧双方均保持有限
的结构工作、字段、元素、深度及未知字节计数。未知结构不递归解释，未来读者仍须
执行自己认识的检查。这些计数不等于 Python 分配峰值、CPU 硬预算或整个 RPC 预算。

源代码包含 Python 对象、复制和动态分配，没有 C/Rust 借用、ABI、分配器、
CPU/RSS/P99、完整 UTF-8、开放枚举、随机读取性能、RPC/TLS/Zstd 验证。
泛化代理还需要多子节点/复合数组、更多未知布局和跨安全域选择等验收。

## 证据

[结果与字节向量](../../data/ctm_shape_reference_20261029_results.json)包含 28 个形状、
224 个新旧读写组合、256 个固定 seed 往返和 112 个编码摘要；小于等于 512 字节的
帧同时存十六进制，其余可由冻结源码复现。六个手写黄金帧独立核对基本字节布局。
[运行与负对照记录](../../data/ctm_shape_reference_20261029_controls.json)绑定结果摘要，
结果再绑定四个输入源码摘要；source_commit 是当时仓库上下文 33292b6。
本机原日志位于 build/codec-shapes-20261029/，日志含路径，异路径摘要不保证相同。

开发中一项过强断言曾把所有重编码都要求为相同字节，最小反例为旧空消息通过新
固定块编码器后增加 12 字节。最终检查将这种显式 revision 升级与原字节往返区分；
28 项升级均另计，未知数据仍须语义保留。没有把该变化隐藏为所有编码恒等。
