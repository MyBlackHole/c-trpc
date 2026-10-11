# 许可与来源范围

Copyright 2026 Black Hole

当前版本的项目自有源码、公开头、测试、文档、脚本及独立编写的配置文件采用
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)。维护者已确认有权按该许可发布这些内容；
版权署名为 Black Hole。`NOTICE` 保留项目署名，不增加或修改 LICENSE 条款。
本项目不是 Apache Software Foundation 项目，不暗示其背书。

## 当前分发内容

当前源码树未包含 vendored 第三方库或 Git submodule。构建依赖操作系统的
POSIX/Linux 接口、C/POSIX 运行库和编译器；Xmake、编译器、格式检查器及 CI
Actions 是外部工具，不因本项目许可而被重新授权。重新分发这些工具或系统库时，
应另行遵守其各自许可。

源码归档须包含根目录 `LICENSE`、`NOTICE` 和本说明。安装的静态 SDK 在
`share/doc/c-trpc/` 携带这三份文件。下游分发应遵守 Apache-2.0 的相关条件，
包括随附许可、保留适用署名，以及对修改作出说明；实际条款以 LICENSE 为准。
新增第三方内容必须保留其独立许可和署名，并重新核对分发范围。

## Git 历史中的 Linux 配置

PR #197 已用针对本项目独立编写的规则替换两个历史配置：原 `.clang-format`
与 Linux v6.10 同名文件一致，原 `.editorconfig` 在 Linux 文件上仅增加 `cpp`
扩展名；原文件分别标有 GPL-2.0 与 GPL-2.0-only。详见
源码树中的 `docs/tooling-policy.md` 工具配置来源说明。

本次 Apache-2.0 授权不重授这些第三方历史文件、不修改 Git 历史，也不消除
旧版本、历史归档或包含旧文件的 Git 镜像分发所需履行的义务。不能把当前
源码包的许可结论直接套用到所有历史版本。
