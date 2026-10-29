# GCC Release 生产 include 依赖快照

日期：2026-10-30；关联 #248。此记录是当前构建配置的依赖证据，不定义允许边界。

## 采集口径

基于 `codex/release-readiness-20261029` 的提交
`4bc86551101c0e62704b9bd9608fcbceac9a0fce`，Linux x86_64、GCC 16.2.1、Xmake
3.1.2，配置为 `trcore` / Release / `crc32c_portable=n`。先用项目的
`check_compile_commands.py` 导出并校验当前 Xmake 数据库：55 个生产 C 翻译单元，数据库
共 161 条记录，SHA-256 为
`cce6abef1906f6d49d59a0fb09fd1d74da837c3cee53a7b97268bf49a4fbee20`。

随后逐条沿用数据库内的编译器、工作目录和参数，仅移除 `-c`、`-o <object>`，追加
`-fsyntax-only -H`。解析 GCC 实际启用的 include 树，只保留仓库内 `src/` 与
`include/tr/` 的直接边。完整边、来源翻译单元、重复包含次数和目录分组保存在
[JSON 快照](data/module_dependency_inventory_20261030.json)。快照包含数据库摘要与构建身份，
不包含本机绝对工作区路径。

## 观察结果

55 个生产翻译单元得到 389 条唯一的仓库内直接 include 边；按当前目录分组有 249 条
跨目录边、140 条同目录边。目录只用于呈现，不等同于已经批准的逻辑模块，跨目录数量也
不是违规数。

确认 Reactor 对 TRP 的耦合是直接编译依赖：`src/execution/reactor.c` 直接包含
`src/transport/protocol/parser.h` 和 `wire.h`，`src/execution/reactor.h` 直接包含
`src/transport/protocol/frame.h`。连接组适配、RPC、Runtime 等目录间也存在实际头文件边；
逐条清单见 JSON，不把它们预判为错误，因为组合根和合法适配器可以跨组件依赖。

## 验收边界

现有 `check_compile_commands.py` 验证生产源文件覆盖及 Xmake 实际编译参数；它不比较允许的
模块依赖图，也不检查私有符号/布局越界或受限链接。此快照只覆盖当前 Linux GCC Release
激活的预处理分支，不包含其他平台/工具链条件，也不表示符号调用、运行时回调、对象布局
访问或生命周期方向。

因此，本证据确认 #248 所需的现状图可以从真实构建导出，但不能关闭 M1/M6。仍需定义由谁拥有
模块状态、允许哪些编译/符号边，并加入跨模块负向门禁与受限链接测试；在策略获批前，不把
目录边自动标成允许或拒绝。
