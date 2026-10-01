# agent-bridge 开发指南

`core/` 管理会话、路由、消息和平台无关接口；`agent/` 实现 Claude Code、Codex；`platform/` 实现微信、飞书、Telegram、QQ（OneBot 和官方机器人）；`config/` 解析配置；`cmd/agent-bridge/` 是 CLI 与进程入口；`web/src/` 是管理页面源码。

旧版数据目录到 `~/.agent-bridge` 的迁移是手动操作，步骤记录在 `docs/MIGRATION.md`。程序不应自动读取、复制或删除旧目录。不要重新加入内置 cron、timer、Agent heartbeat、多机器人中继、`/bind`、任何提示词注入（含 `inject_bridge_prompt`）、多语言/语言设置、检查更新与 `/upgrade`，以及 `/status`、`/usage`、`/version`、`/config`、`/memory`、`/doctor`、`/web`、`/ps` 命令。skill 发现/注册/注入与 `/skills`；未识别的 `/` 命令静默透传给 Agent；Provider 预设与 cc-switch 导入。用户可见文本只用英文；压缩上下文命令是 `/compact`。网络协议保活、管理页面实时聊天与会话超时属于运行控制，可以保留。

新增功能优先通过 `core` 中的可选接口连接，避免让 `core` 依赖具体平台。配置和秘密信息不写进源码或文档。Go 源码使用 `gofmt`；相关改动运行 `go test ./...`。Web 改动运行 `pnpm build`。本项目没有自动更新上游或现成 Git 仓库。
