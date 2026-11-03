-- 从已配置的 Xmake 构建图读取命令；不维护第二套源码列表或编译参数。
import("core.base.json")
import("core.base.task")
import("core.project.config")
import("core.project.project")
import("core.tool.compiler")

function main(output)
    task.run("config", {}, {loadonly = true})
    local target = assert(project.target("trcore"))
    local commands = {}
    for _, batch in pairs(target:sourcebatches()) do
        assert(batch.sourcekind == "cc" and batch.rulename == "c.build")
        for index, source in ipairs(batch.sourcefiles) do
            table.insert(commands, {
                file = source,
                arguments = table.join(compiler.compargv(source,
                    batch.objectfiles[index], {target = target,
                    sourcekind = batch.sourcekind, rawargs = true}))
            })
        end
    end
    table.sort(commands, function (left, right) return left.file < right.file end)
    local options = {}
    for key, value in pairs(io.load(config.filepath())) do
        if not key:startswith("__") then
            options[key] = value
        end
    end
    json.savefile(output, {
        project_dir = os.projectdir(),
        xmake_version = tostring(xmake.version()),
        config = options,
        commands = commands
    })
end
