set_project("c-trpc")
set_version("2.0.0-dev.107")
set_xmakever("3.1.1")
set_allowedplats("linux")
set_allowedmodes("debug", "release", "asan", "tsan")
set_defaultmode("release")

-- 消息后端使用静态接口：固定版本、源码校验和静态链接，禁止系统库替代。
add_requires("zstd 1.5.7", {system = false, configs = {shared = false, cmake = false}})

option("crc32c_portable")
    set_default(false)
    set_showmenu(true)
    set_description("Use only the portable CRC32C backend (no CPU dispatch)")
option_end()

if has_config("crc32c_portable") then
    add_defines("TR_CRC32C_FORCE_PORTABLE")
end

set_languages("c11")
set_warnings("all", "extra", "error")
set_symbols("debug")
add_cflags("-pedantic", "-pthread", {force = true})
add_ldflags("-pthread", {force = true})

-- Make bridge: parse quoted flags once and preserve them as a group.
-- Xmake 3.1.1 reparses native --cflags/--ldflags entries containing spaces.
for _, kind in ipairs({"cflags", "ldflags"}) do
    option("make_" .. kind)
        set_default("")
        set_showmenu(true)
        set_description("Extra " .. kind .. " from the Make compatibility entry points")
    option_end()
end

-- os.argv belongs to the script scope. expand=false keeps the parsed argv
-- as one raw flag group, including paths with spaces and repeated switches.
on_load(function (target)
    import("core.project.config")
    for _, kind in ipairs({"cflags", "ldflags"}) do
        local flags = os.argv(config.get("make_" .. kind) or "")
        if #flags > 0 then
            target:add(kind, flags, {force = true, expand = false})
        end
    end
end)

-- Preserve the Make build's -O2 -g and enabled assertions in release mode.
-- In particular, assert() contains test operations, not just result checks.
if is_mode("debug") then
    set_optimize("none")
elseif is_mode("asan", "tsan") then
    set_optimize("fast")
    add_cflags("-fno-omit-frame-pointer", {force = true})
else
    set_optimize("faster")
end

if is_mode("asan") then
    set_policy("build.sanitizer.address", true)
    set_policy("build.sanitizer.undefined", true)
elseif is_mode("tsan") then
    set_policy("build.sanitizer.thread", true)
end

target("trcore")
    set_kind("static")
    -- Runtime 已引用消息后端；安装后的 .a 必须包含静态包，不能依赖
    -- 消费方的 Xmake 环境或系统 libzstd。3.1.1 原生支持合并 package。
    set_policy("build.merge_archive", true)
    add_packages("zstd", {public = true})
    add_syslinks("ssl", "crypto", {public = true})
    -- 源码与已安装 SDK 同时携带许可和适用署名。
    add_installfiles("LICENSE", "NOTICE", "docs/licensing.md", {prefixdir = "share/doc/c-trpc"})
    add_installfiles("third_party/zstd/LICENSE", {prefixdir = "share/doc/c-trpc/third_party/zstd"})
    add_includedirs("include", {public = true})
    -- Stable SDK compatibility boundary. Runtime/Transport engine headers
    -- stay repository-internal; internal tests include them from the source tree.
    add_headerfiles(
        "include/(tr/trpc.h)",
        "include/(tr/status.h)",
        "include/(tr/facade.h)",
        "include/(tr/client.h)",
        "include/(tr/peer_identity.h)",
        "include/(tr/server.h)",
        "include/(tr/tls.h)",
        "include/(tr/transport.h)",
        "include/(tr/rpc.h)",
        "include/(tr/rpc_v2.h)",
        "include/(tr/rpc_codec.h)")
    add_files(
        "src/status.c",
        "src/crc32c.c",
        "src/message/message.c",
        "src/message/builder.c",
        "src/message/transform.c",
        "src/message/zstd.c",
        "src/execution/buffer.c",
        "src/transport/protocol/wire.c",
        "src/transport/protocol/message_caps.c",
        "src/transport/protocol/message_envelope.c",
        "src/transport/protocol/frame.c",
        "src/transport/protocol/parser.c",
        "src/execution/task_executor.c",
        "src/execution/worker_context.c",
        "src/execution/command_queue.c",
        "src/execution/completion_queue.c",
        "src/execution/timer_queue.c",
        "src/execution/deadline.c",
        "src/io/socket.c",
        "src/io/connector.c",
        "src/io/byte_endpoint.c",
        "src/io/peer_identity_internal.c",
        "src/io/tls_provider_operation.c",
        "src/io/tls_config.c",
        "src/io/tls_openssl.c",
        "src/execution/reactor.c",
        "src/runtime/runtime.c",
        "src/runtime/message_service.c",
        "src/group/pipeline.c",
        "src/group/pipeline_route.c",
        "src/group/pipeline_registry.c",
        "src/group/pipeline_control.c",
        "src/group/pipeline_control_wire.c",
        "src/transport/group/pipeline_control_transport.c",
        "src/transport/group/pipeline_ingress.c",
        "src/transport/group/pipeline_listener.c",
        "src/transport/channel/channel.c",
        "src/transport/channel/channel_stream_id.c",
        "src/transport/channel/reset_tombstone_index.c",
        "src/rpc/rpc_codec.c",
        "src/rpc/rpc_record.c",
        "src/rpc/rpc_record_stream.c",
        "src/rpc/rpc_record_router.c",
        "src/rpc/rpc_record_dispatch.c",
        "src/rpc/rpc_record_producer.c",
        "src/rpc/rpc_record_endpoint.c",
        "src/rpc/rpc_record_client.c",
        "src/rpc/rpc_v2.c",
        "src/facade.c",
        "src/facade_binding.c",
        "src/transport/group/client_group.c",
        "src/client.c",
        "src/server.c")
target_end()

-- TLS-B backend 由生产归档提供；测试复用同一份已构建实现。
target("test_tls_bio_probe")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_tls_bio_probe.c")
    add_syslinks("ssl", "crypto")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_tls_config")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_tls_config.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_tls_provider_operation")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_tls_provider_operation.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_tls_provider")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_tls_provider.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_tls_openssl")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_tls_openssl.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_rpc_v2_tls_identity")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_v2_tls_identity.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    -- 仅在该 TLS 轮换测试链接中注入跨 shard profile retain 故障。
    add_ldflags("-Wl,--wrap=tr_tls_openssl_profile_retain",
                "-Wl,--wrap=tr_runtime_destroy", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- TLS 组监听器必须先完成 mTLS，再解析 CONTROL/DATA 路由并绑定主体。
target("test_pipeline_listener_tls")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_pipeline_listener_tls.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_connection_group_tls")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_connection_group_tls.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_ldflags("-Wl,--wrap=write",
                "-Wl,--wrap=tr_reactor_aux_event_register",
                "-Wl,--wrap=tr_reactor_send", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_byte_endpoint")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_byte_endpoint.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 已编译的 GNU gettext 目录随示例部署，普通构建不要求安装 msgfmt。
local function copy_echo_catalog(target)
    local relative = "locales/zh_CN/LC_MESSAGES/c-trpc-examples.mo"
    local destination = path.join(target:targetdir(), relative)
    os.mkdir(path.directory(destination))
    os.cp(relative, destination)
end

for _, name in ipairs({"echo_server", "echo_client"}) do
    target(name)
        set_kind("binary")
        add_files("examples/" .. name .. ".c")
        if name == "echo_client" then
            after_build(copy_echo_catalog)
        end
        add_deps("trcore")
    target_end()
end

for _, name in ipairs({"test_echo_client_i18n", "test_socket_atomic_flags", "test_server_stats_retiring", "test_reactor_timer_deadline", "test_connector_deadline", "test_inbound_handshake_deadline", "test_reactor_tokens", "test_channel_ready_deadline", "test_client_connect_deadline", "test_server_drain_deadline", "test_deadline", "test_transport", "test_connection_group_facade", "test_timer_queue", "test_completion_queue", "test_command_queue", "test_buffer_pool", "test_primitive_constructor_faults", "test_complex_constructor_faults", "test_connector", "test_pipeline", "test_pipeline_route", "test_pipeline_registry", "test_pipeline_ingress", "test_pipeline_control", "test_pipeline_control_wire", "test_pipeline_listener", "test_runtime", "test_runtime_threads", "test_reactor_fairness", "test_reactor_budget", "test_reactor_stats_reuse_race", "test_reactor_source_detach", "test_reactor_stop_full", "test_reactor_fatal_exit", "test_channel_detach", "test_channel_control_retry", "test_channel_protocol_error", "test_channel_reconnect_teardown", "test_channel_create_transaction", "test_channel_hello_ownership", "test_client_connect_rollback", "test_facade_constructor_ownership", "test_server_peer_rollback", "test_internal_constructor_ownership", "test_server_destroy_retry", "test_server_create_oom", "test_tx_priority", "test_crc32c", "test_rpc_message_ownership", "test_facade_binding"}) do
    target(name)
        set_kind("binary")
        set_default(false)
        add_files("tests/" .. name .. ".c")
        add_deps("trcore")
        add_undefines("NDEBUG")
        if name == "test_echo_client_i18n" then
            after_build(copy_echo_catalog)
        elseif name == "test_complex_constructor_faults" then
            -- 仅独立测试二进制截获构造器分配、锁和条件变量接口。
            add_ldflags("-Wl,--wrap=calloc", "-Wl,--wrap=malloc",
                        "-Wl,--wrap=free",
                        "-Wl,--wrap=pthread_mutex_init",
                        "-Wl,--wrap=pthread_mutex_destroy",
                        "-Wl,--wrap=pthread_cond_init",
                        "-Wl,--wrap=pthread_cond_destroy", {force = true})
        elseif name == "test_primitive_constructor_faults" then
            -- 仅测试二进制按链接符号故障注入，不向生产库插入分配钩子。
            add_ldflags("-Wl,--wrap=calloc", "-Wl,--wrap=malloc",
                        "-Wl,--wrap=free",
                        "-Wl,--wrap=pthread_mutex_init",
                        "-Wl,--wrap=pthread_mutex_destroy",
                        "-Wl,--wrap=pthread_cond_init",
                        "-Wl,--wrap=pthread_cond_destroy", {force = true})
        elseif name == "test_inbound_handshake_deadline" then
            -- 真实 TCP 与 owner timer 精确推进；故障注入只存在于本测试目标。
            add_ldflags("-Wl,--wrap=clock_gettime",
                        "-Wl,--wrap=tr_deadline_init_ms",
                        "-Wl,--wrap=tr_deadline_expired",
                        "-Wl,--wrap=tr_reactor_timer_register",
                        "-Wl,--wrap=tr_reactor_timer_arm",
                        "-Wl,--wrap=tr_reactor_timer_unregister",
                        "-Wl,--wrap=tr_reactor_abort_on_owner",
                        "-Wl,--wrap=tr_pipeline_ingress_attach_data_route_on_owner", {force = true})
        elseif name == "test_reactor_timer_deadline" then
            add_ldflags("-Wl,--wrap=clock_gettime", "-Wl,--wrap=epoll_wait", {force = true})
        elseif name == "test_connector_deadline" then
            add_ldflags("-Wl,--wrap=tr_tcp_connect_ipv4",
                        "-Wl,--wrap=tr_tcp_finish_connect",
                        "-Wl,--wrap=clock_gettime", "-Wl,--wrap=send",
                        "-Wl,--wrap=epoll_wait", "-Wl,--wrap=epoll_ctl",
                        "-Wl,--wrap=tr_reactor_aux_event_unregister", {force = true})
        elseif name == "test_channel_ready_deadline" then
            -- 只推进调用线程在取得 Channel 锁后的时间，不修改生产状态。
            add_ldflags("-Wl,--wrap=clock_gettime",
                        "-Wl,--wrap=pthread_mutex_lock", {force = true})
        elseif name == "test_client_connect_deadline" then
            -- 真实 Client/Channel 回滚；只模拟 TCP 耗时和 HELLO 等待结果。
            add_ldflags("-Wl,--wrap=clock_gettime",
                        "-Wl,--wrap=poll", "-Wl,--wrap=__poll_chk",
                        "-Wl,--wrap=tr_tcp_connect_ipv4",
                        "-Wl,--wrap=tr_tcp_finish_connect",
                        "-Wl,--wrap=pthread_cond_timedwait", {force = true})
        elseif name == "test_server_drain_deadline" then
            -- 真实 Server + 多 peer，拦截独立 Channel deadline wait 边界。
            -- clock 和 Channel wait 的故障只存在于测试链接阶段。
            add_ldflags("-Wl,--wrap=tr_channel_begin_drain",
                        "-Wl,--wrap=tr_channel_wait_drained_deadline",
                        "-Wl,--wrap=tr_deadline_init_ms",
                        "-Wl,--wrap=tr_deadline_expired",
                        {force = true})
        elseif name == "test_deadline" then
            -- 只在测试链接阶段模拟单调时钟及 poll 调度结果。
            add_ldflags("-Wl,--wrap=clock_gettime",
                        "-Wl,--wrap=poll",
                        "-Wl,--wrap=__poll_chk", {force = true})
        elseif name == "test_transport" then
            -- Observe facade/reconnect TCP_NODELAY policy without production hooks.
            add_ldflags("-Wl,--wrap=setsockopt", "-Wl,--wrap=sendmsg",
                        "-Wl,--wrap=tr_reactor_send_data_frame_on_owner",
                        "-Wl,--wrap=tr_reactor_watch_data_tx_on_owner",
                        {force = true})
            add_ldflags("-Wl,--wrap=tr_reactor_timer_arm", {force = true})
        elseif name == "test_connection_group_facade" then
            -- CONTROL 失败与 Client Group 截止时间/condwait 测试链接期注入。
            add_ldflags("-Wl,--wrap=tr_reactor_close_on_owner",
                        "-Wl,--wrap=tr_reactor_abort_on_owner",
                        "-Wl,--wrap=tr_reactor_send",
                        "-Wl,--wrap=tr_reactor_sendv_limited",
                        "-Wl,--wrap=ZSTD_compressStream2",
                        "-Wl,--wrap=ZSTD_decompressStream",
                        "-Wl,--wrap=tr_connector_create",
                        "-Wl,--wrap=tr_connector_start",
                        "-Wl,--wrap=tr_connector_cancel",
                        "-Wl,--wrap=tr_pipeline_listener_create",
                        "-Wl,--wrap=sendmsg",
                        "-Wl,--wrap=tr_deadline_init_ms",
                        "-Wl,--wrap=pthread_cond_timedwait", {force = true})
        elseif name == "test_channel_reconnect_teardown" then
            -- Deterministic in-flight Connector cancellation and aux source
            -- detach failure while Channel destroy owns callback storage.
            add_ldflags("-Wl,--wrap=tr_tcp_connect_ipv4",
                        "-Wl,--wrap=tr_reactor_aux_event_register",
                        "-Wl,--wrap=tr_reactor_aux_event_unregister",
                        {force = true})
        elseif name == "test_connector" then
            -- aux detach 故障，以及仅对模拟 preface fd 的 send/EINTR 注入。
            -- 合成单调时钟只作用于 Reactor owner，绝不注入生产 SDK。
            add_ldflags("-Wl,--wrap=tr_tcp_connect_ipv4",
                        "-Wl,--wrap=tr_reactor_aux_event_unregister",
                        "-Wl,--wrap=send",
                        "-Wl,--wrap=clock_gettime", {force = true})
        elseif name == "test_runtime_threads" then
            -- Test-only lifecycle accounting/fault injection, never part of the SDK.
            add_ldflags("-Wl,--wrap=pthread_create", "-Wl,--wrap=pthread_join", {force = true})
        elseif name == "test_runtime" then
            -- 预算释放失败后重试 Runtime peer 存储回收，仅在本测试链接注入。
            add_ldflags("-Wl,--wrap=tr_reactor_destroy", {force = true})
        elseif name == "test_pipeline_listener" then
            -- epoll/source 回滚和 MONOTONIC condwait 的测试链接期注入。
            add_ldflags("-Wl,--wrap=tr_command_queue_push",
                        "-Wl,--wrap=tr_reactor_listener_register_publish",
                        "-Wl,--wrap=epoll_ctl",
                        "-Wl,--wrap=tr_deadline_init_ms",
                        "-Wl,--wrap=pthread_cond_timedwait", {force = true})
        elseif name == "test_pipeline_ingress" then
            -- 仅在测试中注入 terminal DATA membership detach 失败，不污染生产代码。
            add_ldflags("-Wl,--wrap=tr_pipeline_registry_detach_data_route", {force = true})
        elseif name == "test_reactor_fairness" then
            -- Observe real queue operations and poll boundaries without production hooks.
            add_ldflags("-Wl,--wrap=tr_command_queue_push",
                        "-Wl,--wrap=tr_command_queue_push_wait",
                        "-Wl,--wrap=tr_command_queue_push_wait_force",
                        "-Wl,--wrap=tr_command_queue_pop_batch",
                        "-Wl,--wrap=tr_completion_queue_push_wait",
                        "-Wl,--wrap=epoll_wait", {force = true})
        elseif name == "test_tx_priority" then
            add_ldflags("-Wl,--wrap=sendmsg", {force = true})
        elseif name == "test_reactor_budget" then
            add_ldflags("-Wl,--wrap=tr_command_queue_pop_batch",
                        "-Wl,--wrap=sendmsg", "-Wl,--wrap=recv",
                        "-Wl,--wrap=tr_parser_produce",
                        "-Wl,--wrap=epoll_wait", {force = true})
        elseif name == "test_reactor_source_detach" then
            -- Inject EPOLL_CTL_DEL failures without production fault hooks.
            add_ldflags("-Wl,--wrap=epoll_ctl", {force = true})
        elseif name == "test_reactor_fatal_exit" then
            -- Inject EIO at the owner poll, capture accepted waiters and
            -- prove the bounded command/completion queues are drained.
            add_ldflags("-Wl,--wrap=epoll_wait",
                        "-Wl,--wrap=tr_command_queue_push_wait",
                        "-Wl,--wrap=tr_completion_queue_push_wait", {force = true})
        elseif name == "test_reactor_stop_full" then
            -- 固定满队列停机交错，验证 FIFO、join 重试及新 epoch。
            add_ldflags("-Wl,--wrap=tr_command_queue_push_wait_force",
                        "-Wl,--wrap=tr_command_queue_pop_batch",
                        "-Wl,--wrap=pthread_join", {force = true})

        elseif name == "test_channel_protocol_error" then
            -- Observe immediate owner abort while the actual command ring is full.
            add_ldflags("-Wl,--wrap=tr_reactor_abort_on_owner", {force = true})
        elseif name == "test_channel_control_retry" then
            -- Fail WINDOW_UPDATE send admission without impacting HELLO / DATA.
            add_ldflags("-Wl,--wrap=tr_reactor_send", {force = true})
        elseif name == "test_channel_detach" then
            -- 验证 handler/timer teardown barrier 在失败后仍可重试。
            add_ldflags("-Wl,--wrap=tr_reactor_set_handler",
                        "-Wl,--wrap=tr_reactor_timer_unregister", {force = true})
        elseif name == "test_channel_create_transaction" then
            -- 注入构造回滚的 handler/close 复合故障与握手静默屏障失败。
            add_ldflags("-Wl,--wrap=tr_reactor_set_handler",
                        "-Wl,--wrap=tr_reactor_close_on_owner",
                        "-Wl,--wrap=tr_reactor_timer_register",
                        "-Wl,--wrap=tr_reactor_timer_unregister",
                        "-Wl,--wrap=tr_reactor_send",
                        "-Wl,--wrap=tr_reactor_quiesce", {force = true})
        elseif name == "test_channel_hello_ownership" then
            -- 精确观察 HELLO/ACK 缓冲释放，仅此测试注入发送拒绝。
            add_ldflags("-Wl,--wrap=tr_reactor_send",
                        "-Wl,--wrap=free", {force = true})
        elseif name == "test_client_connect_rollback" then
            -- 注入 connect 后半段失败与 rollback 失败，验证 ownership 可重试。
            add_ldflags("-Wl,--wrap=tr_rpc_record_client_create_on_owner",
                        "-Wl,--wrap=tr_channel_destroy", {force = true})
        elseif name == "test_facade_constructor_ownership" then
            -- 验证 Client/Server constructor rollback 失败后仍保留显式 owner。
            add_ldflags("-Wl,--wrap=tr_runtime_start",
                        "-Wl,--wrap=tr_runtime_stop",
                        "-Wl,--wrap=tr_runtime_destroy",
                        "-Wl,--wrap=tr_buffer_pool_init_budgeted",
                        {force = true})
        elseif name == "test_server_peer_rollback" then
            -- 验证 accept rollback 在当前 owner turn 退休 connection。
            add_ldflags("-Wl,--wrap=tr_channel_set_lifecycle_observer",
                        "-Wl,--wrap=tr_reactor_close_on_owner",
                        "-Wl,--wrap=tr_reactor_close", {force = true})
        elseif name == "test_internal_constructor_ownership" then
            -- 验证内部 constructor rollback 失败时 partial owner 仍可重试。
            add_ldflags("-Wl,--wrap=tr_reactor_create",
                        "-Wl,--wrap=tr_reactor_destroy",
                        "-Wl,--wrap=tr_connector_create",
                        "-Wl,--wrap=tr_buffer_pool_destroy", {force = true})
        elseif name == "test_server_destroy_retry" then
            -- 注入 V2 Channel destroy 失败，验证 Server ownership 可重试。
            add_ldflags("-Wl,--wrap=tr_channel_destroy", {force = true})
        elseif name == "test_server_create_oom" then
            -- 真实分配失败：覆盖分片数组、各分片资源和二次回滚失败。
            add_ldflags("-Wl,--wrap=calloc", "-Wl,--wrap=malloc",
                        "-Wl,--wrap=tr_runtime_create",
                        "-Wl,--wrap=tr_runtime_destroy", {force = true})
        end
        if name == "test_socket_atomic_flags" or name == "test_server_stats_retiring" then
            add_includedirs("src")
        end
        if name == "test_socket_atomic_flags" then
            add_ldflags("-Wl,--wrap=socket", {force = true})
        end
        add_tests("default", {run_timeout = 120000, realtime_output = true})
    target_end()
end

-- 使用生产编译对象，但不链接 trcore，避免其他模块补齐隐藏依赖。
-- 不把相同生产源再次注册到测试目标，保持编译数据库中生产条目唯一。
target("test_message")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_message.c")
    add_includedirs("include")
    add_deps("trcore", {inherit = false})
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=realloc", {force = true})
    on_config(function (target)
        local message_count = 0
        local builder_count = 0
        for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
            for index, source in ipairs(batch.sourcefiles) do
                if path.absolute(source) == path.absolute("src/message/message.c") then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    message_count = message_count + 1
                elseif path.absolute(source) == path.absolute("src/message/builder.c") then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    builder_count = builder_count + 1
                end
            end
        end
        assert(message_count == 1,
               "test_message requires exactly one production message object")
        assert(builder_count == 1,
               "test_message requires exactly one production builder object")
    end)
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- Opt-in microbenchmark; never a CI timing gate or a default build target.
target("bench_crc32c")
    set_kind("binary")
    set_default(false)
    add_files("bench/bench_crc32c.c")
    add_deps("trcore")
target_end()

-- Explicit opt-in diagnostic: separate-process TCP/RPC workloads (docs/rpc_benchmark.md).
target("bench_rpc")
    set_kind("binary")
    set_default(false)
    add_files("bench/bench_rpc.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=realloc", {force = true})
target_end()

-- Opt-in diagnostic: public connection-group DATA with optional mTLS/Zstandard.
target("bench_connection_group")
    set_kind("binary")
    set_default(false)
    add_files("bench/bench_connection_group.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=realloc", {force = true})
target_end()

-- 同一生产对象仅注册一次；变换测试不借完整 trcore 补齐跨模块依赖。
target("test_message_transform")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_message_transform.c")
    add_includedirs("include")
    add_packages("zstd")
    add_deps("trcore", {inherit = false})
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=ZSTD_compressStream2",
                "-Wl,--wrap=ZSTD_decompressStream", {force = true})
    on_config(function (target)
        local expected = {
            [path.absolute("src/message/message.c")] = 0,
            [path.absolute("src/message/transform.c")] = 0,
            [path.absolute("src/message/zstd.c")] = 0
        }
        for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
            for index, source in ipairs(batch.sourcefiles) do
                local key = path.absolute(source)
                if expected[key] ~= nil then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    expected[key] = expected[key] + 1
                end
            end
        end
        for source, count in pairs(expected) do
            assert(count == 1, "生产消息对象必须唯一: " .. source)
        end
    end)
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 独立验证执行底座及 Message 组合，只复用真实生产对象，不链接整个库。
for _, name in ipairs({"test_task_executor", "test_task_message"}) do
    target(name)
        set_kind("binary")
        set_default(false)
        add_files("tests/" .. name .. ".c")
        add_includedirs("include")
        add_deps("trcore", {inherit = false})
        if name == "test_task_executor" then
            add_ldflags("-Wl,--wrap=calloc",
                        "-Wl,--wrap=pthread_mutex_init",
                        "-Wl,--wrap=pthread_cond_init",
                        "-Wl,--wrap=pthread_create",
                        "-Wl,--wrap=pthread_join",
                        "-Wl,--wrap=pthread_cond_destroy",
                        "-Wl,--wrap=pthread_mutex_destroy", {force = true})
        else
            add_packages("zstd")
        end
        on_config(function (target)
            local expected = {
                [path.absolute("src/execution/task_executor.c")] = 0,
                [path.absolute("src/execution/worker_context.c")] = 0
            }
            if target:name() == "test_task_message" then
                expected[path.absolute("src/message/message.c")] = 0
                expected[path.absolute("src/message/transform.c")] = 0
                expected[path.absolute("src/message/zstd.c")] = 0
            end
            for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
                for index, source in ipairs(batch.sourcefiles) do
                    local key = path.absolute(source)
                    if expected[key] ~= nil then
                        table.insert(target:objectfiles(), batch.objectfiles[index])
                        expected[key] = expected[key] + 1
                    end
                end
            end
            for source, count in pairs(expected) do
                assert(count == 1, "生产测试对象必须唯一: " .. source)
            end
        end)
        add_tests("default", {run_timeout = 120000, realtime_output = true})
    target_end()
end

-- 真实 Reactor 集成，不把事件循环替身作为生产接入证据。
target("test_message_service")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_message_service.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=calloc", "-Wl,--wrap=pthread_create",
                "-Wl,--wrap=eventfd", "-Wl,--wrap=read", "-Wl,--wrap=write",
                "-Wl,--wrap=close", "-Wl,--wrap=tr_task_executor_submit",
                "-Wl,--wrap=tr_task_executor_destroy",
                "-Wl,--wrap=tr_reactor_aux_event_register",
                "-Wl,--wrap=tr_reactor_aux_event_unregister",
                "-Wl,--wrap=tr_reactor_timer_register",
                "-Wl,--wrap=tr_reactor_timer_arm",
                "-Wl,--wrap=tr_reactor_timer_unregister",
                "-Wl,--wrap=ZSTD_compressStream2",
                "-Wl,--wrap=ZSTD_decompressStream", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- Runtime 正常/致命退出后的真实消息回收，不使用替身。
target("test_runtime_message_recovery")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_runtime_message_recovery.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=epoll_wait", "-Wl,--wrap=write",
                "-Wl,--wrap=tr_message_service_create_on_owner",
                "-Wl,--wrap=tr_reactor_destroy",
                "-Wl,--wrap=pthread_cond_destroy",
                "-Wl,--wrap=pthread_mutex_destroy",
                "-Wl,--wrap=pthread_cond_broadcast",
                "-Wl,--wrap=pthread_cond_signal",
                "-Wl,--wrap=pthread_cond_wait",
                "-Wl,--wrap=pthread_join",
                "-Wl,--wrap=pthread_create", "-Wl,--wrap=close",
                "-Wl,--wrap=tr_reactor_aux_event_register",
                "-Wl,--wrap=tr_reactor_aux_event_unregister",
                "-Wl,--wrap=tr_reactor_timer_register",
                "-Wl,--wrap=tr_reactor_timer_unregister",
                "-Wl,--wrap=tr_task_executor_destroy",
                "-Wl,--wrap=ZSTD_compressStream2", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 真实验证消息服务同步对象部分销毁后的跨线程接管与重试。
target("test_message_service_partial_destroy")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_message_service_partial_destroy.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=eventfd",
                "-Wl,--wrap=tr_task_executor_destroy",
                "-Wl,--wrap=pthread_cond_destroy",
                "-Wl,--wrap=pthread_mutex_destroy",
                "-Wl,--wrap=pthread_cond_signal",
                "-Wl,--wrap=pthread_cond_broadcast",
                "-Wl,--wrap=pthread_cond_wait", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 分片消息操作及执行域边界，使用真实 Runtime/Reactor/任务执行器。
target("test_runtime_message_api")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_runtime_message_api.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=tr_reactor_call",
                "-Wl,--wrap=ZSTD_compressStream2", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 覆盖分片消息取消、跨分片隔离与多分片失败回滚边界。
target("test_runtime_message_control")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_runtime_message_control.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=pthread_create",
                "-Wl,--wrap=ZSTD_compressStream2",
                "-Wl,--wrap=ZSTD_decompressStream", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- RAW Codec 独立测试真实公共编码对象，保留字节与参数边界契约。
target("test_rpc_raw_codec")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_raw_codec.c")
    add_deps("trcore")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 新调用记录只链接真实记录/消息对象，防止整库补齐意外依赖。
target("test_rpc_record")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record.c")
    add_includedirs("include")
    add_packages("zstd")
    add_deps("trcore", {inherit = false})
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=realloc", {force = true})
    on_config(function (target)
        local expected = {
            [path.absolute("src/rpc/rpc_record.c")] = 0,
            [path.absolute("src/message/message.c")] = 0,
            [path.absolute("src/message/transform.c")] = 0,
            [path.absolute("src/message/zstd.c")] = 0
        }
        for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
            for index, source in ipairs(batch.sourcefiles) do
                local key = path.absolute(source)
                if expected[key] ~= nil then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    expected[key] = expected[key] + 1
                end
            end
        end
        for source, count in pairs(expected) do
            assert(count == 1, "调用记录生产对象必须唯一: " .. source)
        end
    end)
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 发送排空必须跨过已接受命令、两种预留和部分写入，通知可重入登记。
target("test_reactor_tx_drain")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_reactor_tx_drain.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=sendmsg", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 控制容量恢复：跨连接公平通知、取消/复用、命令压力及跨线程归还唤醒。
target("test_reactor_control_ready")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_reactor_control_ready.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=sendmsg", "-Wl,--wrap=tr_wire_header_encode",
                "-Wl,--wrap=epoll_wait", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 分层排空：网络静默不能越过业务完成、终局回调和已受理的 OPEN。
target("test_rpc_record_drain")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_drain.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=tr_rpc_record_endpoint_create_on_owner",
                "-Wl,--wrap=tr_rpc_record_client_create_on_owner",
                "-Wl,--wrap=tr_rpc_record_dispatch_receive_on_owner",
                "-Wl,--wrap=tr_task_executor_dispatch",
                "-Wl,--wrap=tr_stream_sendv",
                "-Wl,--wrap=tr_deadline_init_ms",
                "-Wl,--wrap=tr_channel_wait_drained_deadline",
                "-Wl,--wrap=pthread_cond_timedwait", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- GOAWAY 精确边界、交叉 OPEN、共享/拆分物理连接及完整网络排空。
target("test_channel_drain")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_channel_drain.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=sendmsg", "-Wl,--wrap=tr_reactor_send",
                "-Wl,--wrap=tr_reactor_timer_arm", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 单流 RESET 的真实传输：预留容量、FIFO、短写、迟到帧和错误隔离。
target("test_stream_reset")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_stream_reset.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=tr_reactor_timer_arm", "-Wl,--wrap=sendmsg",
                "-Wl,--wrap=tr_channel_local_stream_id_take", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- RESET 墓碑固定索引的碰撞簇删除与 FIFO 槽复用边界。
target("test_reset_tombstone_index")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_reset_tombstone_index.c")
    add_deps("trcore")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- V2 调用记录在真实 TCP/Channel/Stream 上的绑定与背压、增量接收。
target("test_rpc_record_stream")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_stream.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=tr_stream_sendv", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 方法注册、完整 OPEN 准入与多流派发，使用真实 TCP/Channel/绑定。
target("test_rpc_record_router")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_router.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=tr_reactor_recovery_acquire", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 有界业务工作派发、拥有型输入/回复及真实 TCP 终局发送。
target("test_rpc_record_dispatch")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_dispatch.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=malloc", "-Wl,--wrap=calloc",
                "-Wl,--wrap=tr_stream_sendv", "-Wl,--wrap=clock_gettime", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 宿主契约测试使用明确下层替身，只复用实际宿主对象；不能当成真实 TCP 验收。
target("test_rpc_record_endpoint_contract")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_endpoint_contract.c")
    add_includedirs("include")
    add_deps("trcore", {inherit = false})
    add_ldflags("-Wl,--wrap=calloc", "-Wl,--wrap=clock_gettime", {force = true})
    on_config(function (target)
        local count = 0
        for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
            for index, source in ipairs(batch.sourcefiles) do
                if path.absolute(source) == path.absolute("src/rpc/rpc_record_endpoint.c") then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    count = count + 1
                end
            end
        end
        assert(count == 1, "端点宿主生产对象必须唯一")
    end)
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()


-- V2 宿主真实 TCP：不由测试额外定时驱动派发器，验证自动错误和关闭。
target("test_rpc_record_endpoint_tcp")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_endpoint_tcp.c")
    add_deps("trcore", {inherit = false})
    add_includedirs("include")
    on_config(function (target)
        local sources = {
            "src/crc32c.c",
            "src/execution/buffer.c",
            "src/execution/worker_context.c",
            "src/execution/task_executor.c",
            "src/execution/command_queue.c",
            "src/execution/completion_queue.c",
            "src/execution/timer_queue.c",
            "src/execution/deadline.c",
            "src/execution/reactor.c",
            "src/io/byte_endpoint.c",
            "src/io/peer_identity_internal.c",
            "src/io/tls_config.c",
            "src/io/tls_openssl.c",
            "src/io/socket.c",
            "src/io/connector.c",
            "src/message/message.c",
            "src/message/builder.c",
            "src/transport/protocol/wire.c",
            "src/transport/protocol/message_caps.c",
            "src/transport/protocol/message_envelope.c",
            "src/transport/protocol/frame.c",
            "src/transport/protocol/parser.c",
            "src/transport/channel/channel.c",
            "src/transport/channel/channel_stream_id.c",
            "src/transport/channel/reset_tombstone_index.c",
            "src/rpc/rpc_record.c",
            "src/rpc/rpc_record_stream.c",
            "src/rpc/rpc_record_router.c",
            "src/rpc/rpc_record_dispatch.c",
            "src/rpc/rpc_record_producer.c",
            "src/rpc/rpc_record_endpoint.c",
            "src/rpc/rpc_record_client.c",
            "src/rpc/rpc_v2.c"
        }
        local expected = {}
        for _, source in ipairs(sources) do
            expected[path.absolute(source)] = 0
        end
        for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
            for index, source in ipairs(batch.sourcefiles) do
                local key = path.absolute(source)
                if expected[key] ~= nil then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    expected[key] = expected[key] + 1
                end
            end
        end
        for source, count in pairs(expected) do
            assert(count == 1, "V2 独立网络验证的生产对象必须唯一: " .. source)
        end
    end)
    add_syslinks("ssl", "crypto")
    after_link(function (target)
        -- 防止以后重新继承 trcore 后把旧 RPC 实现悄悄带回这个证明目标。
        local v1_symbols = {
            tr_rpc_call_cancel = true,
            tr_rpc_call_close_send = true,
            tr_rpc_call_finish = true,
            tr_rpc_call_get_context = true,
            tr_rpc_call_get_peer_metadata = true,
            tr_rpc_call_get_peer_trailing_metadata = true,
            tr_rpc_call_is_cancelled = true,
            tr_rpc_call_send = true,
            tr_rpc_call_send_buffer = true,
            tr_rpc_call_set_metadata = true,
            tr_rpc_call_set_trailing_metadata = true,
            tr_rpc_call_start = true,
            tr_rpc_call_start_ex = true,
            tr_rpc_deadline_heap_snapshot = true,
            tr_rpc_endpoint_arm_detached_finalizer = true,
            tr_rpc_endpoint_create = true,
            tr_rpc_endpoint_create_with_executor_group = true,
            tr_rpc_endpoint_destroy = true,
            tr_rpc_endpoint_destroy_with_stats = true,
            tr_rpc_endpoint_detach_for_finalize = true,
            tr_rpc_endpoint_get_semantic_stats = true,
            tr_rpc_endpoint_get_stats = true,
            tr_rpc_endpoint_release_detached_owner = true,
            tr_rpc_executor_group_can_start = true,
            tr_rpc_executor_group_create = true,
            tr_rpc_executor_group_destroy_checked = true,
            tr_rpc_executor_group_start = true,
            tr_rpc_executor_group_stop = true,
            tr_rpc_in_worker_context = true,
            tr_rpc_message_release = true,
            tr_rpc_message_stream_internal = true,
            tr_rpc_register_method = true,
            tr_rpc_register_stream_method = true,
            tr_rpc_unary_call = true,
            tr_rpc_unary_call_ex = true,
            tr_rpc_wire_decode = true,
            tr_rpc_wire_decode_ex = true,
            tr_rpc_wire_encode = true
        }
        -- 这些名称是 V2 Record Endpoint 的类型/回调；不属于旧 V1
        -- 函数符号拒绝集合。tr_rpc_record_endpoint_* 也由精确集合自然放行。
        local v2_endpoint_names = {
            tr_rpc_endpoint_result = true,
            tr_rpc_endpoint_terminal_fn = true,
            tr_rpc_endpoint_progress_fn = true
        }
        local symbols = os.iorunv("nm", {"--defined-only", "--extern-only", target:targetfile()})
        for symbol in symbols:gmatch("[^\n]+") do
            local name = symbol:match("([%w_]+)%s*$") or ""
            assert(v2_endpoint_names[name] or not v1_symbols[name],
                   "V2 独立网络验证不允许 V1 定义: " .. name)
        end
    end)
    add_ldflags("-Wl,--wrap=tr_stream_sendv", "-Wl,--wrap=tr_stream_close",
                "-Wl,--wrap=tr_channel_set_handler",
                "-Wl,--wrap=tr_reactor_timer_register",
                "-Wl,--wrap=tr_reactor_timer_unregister",
                "-Wl,--wrap=epoll_wait", "-Wl,--wrap=sendmsg", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

target("test_server_record")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_server_record.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=tr_runtime_create",
                "-Wl,--wrap=tr_reactor_start",
                "-Wl,--wrap=tr_reactor_timer_arm",
                "-Wl,--wrap=tr_task_executor_create",
                "-Wl,--wrap=tr_task_executor_start",
                "-Wl,--wrap=tr_task_executor_join",
                "-Wl,--wrap=tr_task_executor_destroy",
                "-Wl,--wrap=tr_task_executor_cancel",
                "-Wl,--wrap=tr_task_executor_dispatch",
                "-Wl,--wrap=tr_rpc_record_endpoint_create_on_owner",
                "-Wl,--wrap=tr_rpc_record_endpoint_start_on_owner",
                "-Wl,--wrap=tr_rpc_record_endpoint_close_on_owner",
                "-Wl,--wrap=tr_rpc_record_endpoint_reap_stopped",
                "-Wl,--wrap=tr_buffer_pool_destroy",
                "-Wl,--wrap=tr_channel_set_handler",
                "-Wl,--wrap=pthread_create", "-Wl,--wrap=pthread_join",
                "-Wl,--wrap=epoll_wait",
                {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 真实 V2 两端门面：服务端期限、执行器排队、迟到完成及背压回收。
target("test_server_record_deadline")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_server_record_deadline.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=clock_gettime",
                "-Wl,--wrap=tr_stream_sendv",
                "-Wl,--wrap=tr_channel_create_deferred",
                "-Wl,--wrap=tr_channel_set_handler",
                "-Wl,--wrap=tr_rpc_record_dispatch_create_on_owner",
                "-Wl,--wrap=tr_rpc_record_dispatch_receive_on_owner",
                "-Wl,--wrap=tr_rpc_record_dispatch_pump_on_owner",
                "-Wl,--wrap=tr_task_executor_submit",
                "-Wl,--wrap=tr_task_executor_dispatch", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 真实 V2 两端：半关闭后取消、并行流隔离、迟到 FINAL 与安全槽位复用。
target("test_rpc_record_cancel")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_record_cancel.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=tr_rpc_record_client_create_on_owner",
                "-Wl,--wrap=tr_rpc_record_dispatch_create_on_owner",
                "-Wl,--wrap=tr_rpc_record_dispatch_receive_on_owner",
                "-Wl,--wrap=tr_stream_sendv",
                "-Wl,--wrap=tr_stream_close",
                "-Wl,--wrap=tr_stream_reset_on_owner", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 使用真实 Client 与 Server 门面，保护 V2 调用受理、背压和停止回收。
target("test_client_record")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_client_record.c")
    add_deps("trcore")
    add_ldflags("-Wl,--wrap=tr_stream_sendv",
                "-Wl,--wrap=tr_rpc_record_client_call_start_public_on_owner",
                "-Wl,--wrap=tr_rpc_record_client_start_on_owner",
                "-Wl,--wrap=tr_reactor_timer_arm",
                "-Wl,--wrap=tr_stream_open",
                "-Wl,--wrap=tr_rpc_record_stream_create_on_owner",
                "-Wl,--wrap=tr_channel_create_deferred",
                "-Wl,--wrap=tr_channel_set_handler",
                "-Wl,--wrap=tr_channel_destroy",
                "-Wl,--wrap=tr_buffer_pool_destroy",
                "-Wl,--wrap=tr_runtime_stop",
                "-Wl,--wrap=pthread_join",
                "-Wl,--wrap=tr_stream_release_payload",
                "-Wl,--wrap=clock_gettime", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 仅使用公开 SDK 的真实 V2 两端；同一源码还在安装目录外作为 C/C++ 消费端。
target("test_rpc_v2_sdk")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_v2_sdk.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 普通 V2 Reactor 的 TLS 端到端、认证顺序、分片与明文快速路径回归。
target("test_rpc_v2_tls_reactor")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_v2_tls_reactor.c", "tests/tls_test_identity.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_ldflags("-Wl,--wrap=sendmsg", "-Wl,--wrap=connect", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 公共 V2 Server 的回调身份和故障收尾；注入仅限本测试二进制。
target("test_rpc_v2_server")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_v2_server.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_ldflags("-Wl,--wrap=tr_rpc_record_stream_receive_on_owner",
                "-Wl,--wrap=calloc", "-Wl,--wrap=sendmsg", {force = true})
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 已受理的 V2 工作阻塞时，Server 销毁可重试且终局只发布一次。
target("test_rpc_v2_server_destroy_drain")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_rpc_v2_server_destroy_drain.c")
    add_deps("trcore")
    add_undefines("NDEBUG")
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()

-- 工作域只链接一个执行层对象，验证嵌套恢复和真正的线程局部隔离。
target("test_worker_context")
    set_kind("binary")
    set_default(false)
    add_files("tests/test_worker_context.c")
    add_includedirs("include")
    add_deps("trcore", {inherit = false})
    on_config(function (target)
        local count = 0
        for _, batch in pairs(target:dep("trcore"):sourcebatches()) do
            for index, source in ipairs(batch.sourcefiles) do
                if path.absolute(source) == path.absolute("src/execution/worker_context.c") then
                    table.insert(target:objectfiles(), batch.objectfiles[index])
                    count = count + 1
                end
            end
        end
        assert(count == 1, "工作域测试必须只复用一个生产工作域对象")
    end)
    add_tests("default", {run_timeout = 120000, realtime_output = true})
target_end()
