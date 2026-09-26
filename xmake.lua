set_project("c-trpc")
set_xmakever("3.1.1")
set_allowedplats("linux")
set_allowedmodes("debug", "release", "asan", "tsan")
set_defaultmode("release")

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
    add_headerfiles("include/(tr/*.h)")
    add_files(
        "src/status.c",
        "src/crc32c.c",
        "src/buffer.c",
        "src/wire.c",
        "src/frame.c",
        "src/parser.c",
        "src/command_queue.c",
        "src/completion_queue.c",
        "src/timer_queue.c",
        "src/socket.c",
        "src/reactor.c",
        "src/channel.c",
        "src/rpc_codec.c",
        "src/rpc_wire.c",
        "src/rpc.c",
        "src/facade.c",
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

for _, name in ipairs({"test_transport", "test_timer_queue", "test_runtime_threads", "test_reactor_fairness", "test_reactor_budget", "test_tx_priority"}) do
    target(name)
        set_kind("binary")
        set_default(false)
        add_files("tests/" .. name .. ".c")
        add_deps("trcore")
        add_undefines("NDEBUG")
        if name == "test_runtime_threads" then
            -- Test-only lifecycle accounting/fault injection, never part of the SDK.
            add_ldflags("-Wl,--wrap=pthread_create", "-Wl,--wrap=pthread_join", {force = true})
        elseif name == "test_reactor_fairness" then
            -- Observe real queue operations and poll boundaries without production hooks.
            add_ldflags("-Wl,--wrap=tr_command_queue_push",
                        "-Wl,--wrap=tr_command_queue_pop_batch",
                        "-Wl,--wrap=epoll_wait", {force = true})
        elseif name == "test_tx_priority" then
            add_ldflags("-Wl,--wrap=sendmsg", {force = true})
        elseif name == "test_reactor_budget" then
            add_ldflags("-Wl,--wrap=tr_command_queue_pop_batch",
                        "-Wl,--wrap=sendmsg", "-Wl,--wrap=recv",
                        "-Wl,--wrap=tr_parser_produce",
                        "-Wl,--wrap=epoll_wait", {force = true})
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
