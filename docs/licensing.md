# 许可与来源范围

Copyright 2026 Black Hole

当前版本的项目自有源码、公开头、测试、文档、脚本及独立编写的配置文件采用
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)。维护者已确认有权按该许可发布这些内容；
版权署名为 Black Hole。`NOTICE` 保留项目署名，不增加或修改 LICENSE 条款。
本项目不是 Apache Software Foundation 项目，不暗示其背书。

## 当前分发内容

源码树没有内嵌第三方库的实现或 Git 子模块，但包含固定版本 Zstandard 的
原始许可文本 `third_party/zstd/LICENSE`，不能将它归为项目独立编写的 Apache 内容。
构建从 Xmake 包来源取得静态 Zstandard 1.5.7，并合入 `libtrcore.a`；安装包同时
在 `share/doc/c-trpc/third_party/zstd/LICENSE` 携带原始 BSD 许可。
该文本直接来自 [上游 v1.5.7 LICENSE](https://github.com/facebook/zstd/blob/v1.5.7/LICENSE)，
Git blob 为 `75800288cc243f164b9d130f125e2ffae28f8e39`；保留上游版权、条件与免责声明。

操作系统 POSIX/Linux 接口、C/POSIX 运行库、Xmake、编译器、格式检查器及 CI
Actions 仍是外部接口或工具，不因本项目许可而被重新授权。重新分发这些工具或
系统库时，应另行遵守其各自许可。

源码归档须包含根目录 `LICENSE`、`NOTICE` 和本说明。安装的静态 SDK 在
`share/doc/c-trpc/` 携带这三份文件。下游分发应遵守 Apache-2.0 的相关条件，
包括随附许可、保留适用署名，以及对修改作出说明；实际条款以 LICENSE 为准。
Zstandard 原始许可也必须随源码归档和静态 SDK 保留，不能只分发 `.a` 而省略
随包法律材料。新增或升级第三方内容时重新核对独立许可、署名与分发范围。

## Git 历史中的 Linux 配置

PR #197 已用针对本项目独立编写的规则替换两个历史配置：原 `.clang-format`
与 Linux v6.10 同名文件一致，原 `.editorconfig` 在 Linux 文件上仅增加 `cpp`
扩展名；原文件分别标有 GPL-2.0 与 GPL-2.0-only。详见
源码树中的 `docs/tooling-policy.md` 工具配置来源说明。

本次 Apache-2.0 授权不重授这些第三方历史文件、不修改 Git 历史，也不消除
旧版本、历史归档或包含旧文件的 Git 镜像分发所需履行的义务。不能把当前
源码包的许可结论直接套用到所有历史版本。
