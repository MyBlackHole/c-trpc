# Echo 客户端诊断语言

`echo_client` 通过 GNU gettext 支持英语和简体中文。翻译只作用于连接失败、
客户端销毁失败和 RPC 失败这三类 `stderr` 消息，以及新增语言选项的用法错误。
`response:`、服务端监听端口行、请求负载、响应字节、库状态标记和退出码保持原契约。
原有调用方式不变：

```sh
xmake run echo_client 127.0.0.1 9000 hello
xmake run echo_client --lang=zh_CN 127.0.0.1 9000 hello
xmake run echo_client --lang=en 127.0.0.1 9000 hello
```

## 语言选择

地址之前的 `--lang=auto|en|zh_CN` 优先于 `CTRPC_LANG`。
`CTRPC_LANG` 接受相同的三个值；未设置、空值或 `auto` 使用标准 locale 环境；
未知非空值回退英语。显式 `auto` 忽略 `CTRPC_LANG`。

- `en` 将本进程的 `LC_MESSAGES` 设为 `C`，即使 `LANGUAGE=zh_CN` 也输出英语。
- `zh_CN` 在本进程中设置 `LANGUAGE=zh_CN`，依次尝试 `zh_CN.UTF-8`、`zh_CN.utf8`，
  再尝试原环境的消息 locale；全部不可用时回退英语。
- `auto` 调用 `setlocale(LC_MESSAGES, "")`，按标准的 `LC_ALL`、`LC_MESSAGES`、
  `LANG` 优先级初始化。非 C 消息 locale 下由 gettext 按 `LANGUAGE` 候选列表查找目录。
  `C`、`POSIX`、`C.UTF-8`、`C.utf8` 使用英语；未知或缺失的 locale 安全回退英语。
  不同 glibc 版本对 C.UTF-8 下的 `LANGUAGE` 处理不同，CLI 将返回的 `C.*` 消息 locale
  明确归一为 `C`，保持这些环境下的英语契约；真正翻译仍由 gettext 完成。

遵循 gettext 的目录查找规则，不自制语言名称解析器；只提供 `zh_CN` 目录，
不会把 `zh_TW`、`zh-Hant` 或裸 `zh` 映射为简体中文。
没有英语目录时，`LANGUAGE=en:zh_CN` 会继续找到中文目录；固定英语请使用 `--lang=en`。
中文翻译需要可用的非 C 消息 locale 和消息目录。Python 的 gettext 可以直接读取 MO，
但 C/glibc 的 gettext 还依赖系统消息 locale，不能据 Python 成功推断 C 环境完整。

非法显式语言值在创建客户端前打印用法并退出 `2`；现有业务成功、失败仍退出 `0`、`1`。
语言选项只在地址前解析一次，不扫描端口或负载，负载 `--lang=zh_CN` 会原样传输。
地址前的 `--` 终止选项识别。默认地址、端口、负载仍为 `127.0.0.1`、`9000`、`hello`；
原先多余的位置参数仍忽略。

## 消息目录与部署

目录域为 `c-trpc-examples`，与工具域分开，避免消息键冲突。
源码与已编译产物均位于 `locales/zh_CN/LC_MESSAGES/`；英语原文保留在 C 源码中。
普通 Xmake 构建复制已提交的 MO 到可执行文件旁的同名目录，不要求安装 msgfmt。
Linux 程序通过 `/proc/self/exe` 定位自己的目录，不依赖当前工作目录。
移动程序时一起复制相邻的 `locales/`；未复制目录时自动回退英语，不改变业务退出码。

维护翻译时安装 GNU gettext 工具后运行：

```sh
python3 scripts/compile_echo_catalog.py
python3 scripts/compile_echo_catalog.py --check
```

这个编译维护工具自身暂用英语原文输出，尚未加入翻译；测试程序自身的诊断也保留英语，
只有被测客户端的消息和测试预期包含翻译。

脚本调用真正的 `msgfmt --check --check-format --no-hash`，不实现 PO/MO 编译器。
RPC 的 PO 条目标记 `c-format`，两种语言保留单个 `%d`。
无参数消息使用 `fputs()`；资源只来自程序旁的受信任部署目录，不提供任意目录参数。

初始化发生在独立 CLI 创建线程前，只改变自身 `LC_MESSAGES` 与必要的 `LANGUAGE`。
`LC_NUMERIC`、`LC_CTYPE` 和宿主环境不变，核心库没有 locale 操作、ABI 或配置变化。
GNU/Linux glibc 已包含 libintl 接口，无需额外链接 `-lintl`；其他 libc 不在本轮验证范围。

## 验证

`xmake test -v -j1 'test_echo_client_i18n/*'` 在独立子进程中检查七种路径：
成功、连接失败、连接后销毁失败、注册后销毁失败、调用后销毁失败、RPC 失败、成功后销毁失败。
对照固定的英语与中文预期，逐字节检查 `stdout`、`stderr` 和退出码，并检查语言优先级、
显式覆盖、未知环境、选项样式负载及 `--`。测试注入非 C locale 初始化全部失败，
确认英语回退；不会以跳过中文断言冒充双语验证。

本地测试需要 `zh_CN.UTF-8`，可在私有目录生成，不改变系统配置：

```sh
mkdir -p /tmp/c-trpc-test-locale
localedef --no-archive -i zh_CN -f UTF-8 /tmp/c-trpc-test-locale/zh_CN.UTF-8
LOCPATH=/tmp/c-trpc-test-locale xmake test -v -j1 'test_echo_client_i18n/*'
```

CI 同样使用私有目录和仅作用于任务进程的 `LOCPATH`。
桩仅进入专用测试，不安装到 SDK；实际网络由既有集成测试和独立进程 Echo 验证。
