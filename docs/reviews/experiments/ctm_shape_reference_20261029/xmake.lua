set_project("ctm-shape-reference")
set_version("0.0.1")

task("check_shapes")
    set_category("action")
    set_menu {usage = "xmake check_shapes [options]", options = {}}
    on_run(function ()
        os.execv("python3", {"-B", path.join(os.scriptdir(), "check_shapes.py")})
    end)
task_end()
