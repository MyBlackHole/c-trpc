set_project("c-trpc")
set_xmakever("3.1.1")
set_allowedplats("linux")
set_allowedmodes("debug", "release", "asan", "tsan")
set_defaultmode("release")

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
    add_includedirs("include", {public = true})
    -- Stable SDK compatibility boundary. Runtime/Transport engine headers
    -- stay repository-internal; internal tests include them from the source tree.
    add_headerfiles(
        "include/(tr/trpc.h)",
        "include/(tr/status.h)",
        "include/(tr/facade.h)",
        "include/(tr/client.h)",
        "include/(tr/server.h)",
        "include/(tr/transport.h)",
        "include/(tr/rpc.h)",
        "include/(tr/rpc_codec.h)")
    add_files(
        "src/status.c",
        "src/crc32c.c",
        "src/execution/buffer.c",
        "src/transport/protocol/wire.c",
        "src/transport/protocol/frame.c",
        "src/transport/protocol/parser.c",
        "src/execution/command_queue.c",
        "src/execution/completion_queue.c",
        "src/execution/timer_queue.c",
        "src/io/socket.c",
        "src/io/connector.c",
        "src/execution/reactor.c",
        "src/runtime/runtime.c",
        "src/group/pipeline.c",
        "src/group/pipeline_route.c",
        "src/group/pipeline_registry.c",
        "src/group/pipeline_control.c",
        "src/group/pipeline_control_wire.c",
        "src/transport/group/pipeline_control_transport.c",
        "src/transport/group/pipeline_ingress.c",
        "src/transport/group/pipeline_listener.c",
        "src/transport/channel/channel.c",
        "src/rpc/rpc_codec.c",
        "src/rpc/rpc_wire.c",
        "src/rpc/rpc.c",
        "src/facade.c",
        "src/facade_binding.c",
        "src/transport/group/client_group.c",
        "src/client.c",
        "src/server.c")
target_end()

for _, name in ipairs({"echo_server", "echo_client"}) do
    target(name)
        set_kind("binary")
        add_files("examples/" .. name .. ".c")
        add_deps("trcore")
    target_end()
end

for _, name in ipairs({"test_transport", "test_connection_group_facade", "test_timer_queue", "test_completion_queue", "test_command_queue", "test_buffer_pool", "test_connector", "test_pipeline", "test_pipeline_route", "test_pipeline_registry", "test_pipeline_ingress", "test_pipeline_control", "test_pipeline_control_wire", "test_pipeline_listener", "test_runtime", "test_runtime_threads", "test_reactor_fairness", "test_reactor_budget", "test_reactor_source_detach", "test_channel_detach", "test_channel_create_transaction", "test_client_connect_rollback", "test_facade_constructor_ownership", "test_tx_priority", "test_crc32c", "test_rpc_overload", "test_rpc_stream_overload", "test_rpc_stream_backpressure", "test_rpc_executor_reserve", "test_rpc_message_ownership", "test_facade_binding"}) do
    target(name)
        set_kind("binary")
        set_default(false)
        add_files("tests/" .. name .. ".c")
        add_deps("trcore")
        add_undefines("NDEBUG")
        if name == "test_transport" then
            -- Observe facade/reconnect TCP_NODELAY policy without production hooks.
            add_ldflags("-Wl,--wrap=setsockopt", {force = true})
        elseif name == "test_runtime_threads" then
            -- Test-only lifecycle accounting/fault injection, never part of the SDK.
            add_ldflags("-Wl,--wrap=pthread_create", "-Wl,--wrap=pthread_join", {force = true})
        elseif name == "test_pipeline_listener" then
            -- Force CONTROL SEND admission failures to prove soft-state rollback.
            add_ldflags("-Wl,--wrap=tr_command_queue_push", {force = true})
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
        elseif name == "test_channel_detach" then
            -- 验证 handler/timer teardown barrier 在失败后仍可重试。
            add_ldflags("-Wl,--wrap=tr_reactor_set_handler",
                        "-Wl,--wrap=tr_reactor_timer_unregister", {force = true})
        elseif name == "test_channel_create_transaction" then
            -- 注入构造阶段 publication 失败，验证同一 owner turn 内收敛。
            add_ldflags("-Wl,--wrap=tr_reactor_set_handler",
                        "-Wl,--wrap=tr_reactor_timer_register",
                        "-Wl,--wrap=tr_reactor_timer_unregister",
                        "-Wl,--wrap=tr_reactor_send", {force = true})
        elseif name == "test_client_connect_rollback" then
            -- 注入 connect 后半段失败与 rollback 失败，验证 ownership 可重试。
            add_ldflags("-Wl,--wrap=tr_rpc_endpoint_create_with_executor_group",
                        "-Wl,--wrap=tr_channel_destroy", {force = true})
        elseif name == "test_facade_constructor_ownership" then
            -- 验证 Client/Server constructor rollback 失败后仍保留显式 owner。
            add_ldflags("-Wl,--wrap=tr_runtime_start",
                        "-Wl,--wrap=tr_runtime_stop",
                        "-Wl,--wrap=tr_runtime_destroy",
                        "-Wl,--wrap=tr_buffer_pool_init_dynamic_budgeted",
                        {force = true})
        end
        add_tests("default", {run_timeout = 120000, realtime_output = true})
    target_end()
end

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
target_end()
