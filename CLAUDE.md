# agent-bridge

`core/` 管理会话、路由、消息和平台无关接口；`agent/` 实现 Claude Code、Codex；`platform/` 实现微信、Telegram、QQ（OneBot）；`config/` 解析配置；`cmd/agent-bridge/` 是 CLI 与进程入口；`web/src/` 是管理页面源码。

只支持 macOS 和 Linux。界面和消息只用英文，直接写字面字符串，不引入 i18n。Agent 使用 Claude Code / Codex 自身的系统配置，不做自定义 API provider。

新增功能优先通过 `core` 中的可选接口连接，避免让 `core` 依赖具体平台。配置和秘密信息不写进源码或文档。Go 源码使用 `gofmt`；相关改动运行 `go test ./...`。Web 改动运行 `pnpm build`。本项目不自动同步上游；远程仓库为 GitHub `origin`。
