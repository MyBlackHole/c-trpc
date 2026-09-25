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
        "src/maintenance.c",
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

for _, name in ipairs({"test_transport", "test_timer_queue"}) do
    target(name)
        set_kind("binary")
        set_default(false)
        add_files("tests/" .. name .. ".c")
        add_deps("trcore")
        add_undefines("NDEBUG")
        add_tests("default", {run_timeout = 120000, realtime_output = true})
    target_end()
end
