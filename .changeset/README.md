# Changesets 使用说明

本仓库使用 [Changesets](https://github.com/changesets/changesets) 管理 semver 版本号、更新日志与自动打包发版。

## 日常提交流程

1. 在功能或修复分支开发完成后，运行：

   ```bash
   npm run changeset
   ```

2. 按提示选择 `biliup` 及变更级别（`patch` / `minor` / `major`），填写中文变更说明。
3. 将生成的 `.changeset/*.md` 文件随 PR 一起提交并合并到 `dev-1.0.7`。
4. 合并到 `dev-1.0.7` 后，GitHub App 机器人会自动创建或更新 **Version Packages** PR；合并该 PR 后将自动打 `vX.Y.Z` tag 并触发全平台打包与 Release 发布。

详情参见 [发版指南](../.github/RELEASING.md)。
