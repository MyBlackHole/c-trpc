# c-trpc 项目协作约定

- 面向使用者和维护者的文档、代码注释使用简体中文；程序输出通过国际化机制按语言呈现，不直接固定替换成中文。完整边界以 `docs/PROJECT_LANGUAGE.md` 为准；代码标识符、机器兼容面和第三方原文保留。
- 先阅读 `README.md`、`docs/code_comments.md`、`docs/tooling-policy.md`、`docs/build.md` 与 `docs/testing_strategy.md`。本文件只补充语言约定，不改变现有架构或代码规范。
- 使用项目规定的 Xmake 构建与测试入口；测试与 CI 配置中的目标、参数和数据字段保持不变。
- `docs/testing_strategy.md` 中既有的 CI 检查标识约束继续有效。中文化普通步骤名称不能顺带重命名必需检查。
- `tr_status_str()` 的既有返回标记先按公开兼容字符串处理，不在纯翻译中改值；示例或工具可在展示处加入中文上下文。
- 语言清理与行为修改分开提交；历史审计、性能数据、第三方许可原文不重写。PR 以中文列出验证范围与实际结果。
