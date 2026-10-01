# Agent-Bridge

[English](README.md) | **简体中文**

Agent-Bridge 把本地 AI 编程 Agent（**Claude Code**、**Codex**）连接到常用聊天软件：**微信**、**飞书/Lark**、**Telegram** 和 **QQ**（OneBot/NapCat 与 QQ 官方机器人）。在手机上发消息，Agent 就在你自己的机器、自己的项目里运行。

> Agent-Bridge 由 [chenhg5/cc-connect](https://github.com/chenhg5/cc-connect) 大量修改而来，已改名、精简并重构，作为独立源码树发布，不包含上游 Git 历史。感谢 cc-connect 作者的原始工作。

## 功能

- **Agent**：Claude Code（`claudecode`）、Codex（`codex`）。
- **平台**：微信个人号（ilink，扫码登录）、飞书/Lark、Telegram、QQ OneBot v11/NapCat（`qq`）、QQ 官方机器人（`qqbot`）。
- **多项目**：每个 `[[projects]]` 绑定一个 Agent + 工作目录，可接入多个平台。
- **会话管理**：在聊天中新建、列出、切换、重命名、搜索、删除会话。
- **运行中追加指令**：Agent 运行时直接发送新消息即可 steer 当前轮次。
- **进度显示**：可折叠工具活动，轮次结束后自动清理思考/工具消息。
- **附件**：图片和文件会暂存并随下一条提示交给 Agent。
- **Provider**：按项目管理 API Provider（CLI、聊天、管理页面）。
- **管理页面**：内置 Web 管理界面，支持 WebSocket 实时聊天。
- **守护进程**：可安装为 systemd / launchd / Windows 计划任务服务。
- **外部自动化**：通过 webhook 或 `agent-bridge send` 触发消息。
- **不注入提示词**：不会向 Agent 会话注入任何桥接或平台提示词。

## 与 cc-connect 的区别

- 只保留 Claude Code、Codex 两种 Agent，以及微信、飞书、Telegram、QQ 平台。
- 删除内置 cron、timer、Agent heartbeat；定时任务请用外部调度器调用 webhook 或 `agent-bridge send`。
- 删除多机器人中继、`/bind` 与 `relay send`。
- 不注入任何提示词，项目说明写在 `CLAUDE.md` / `AGENTS.md`。Claude Code 只透传你显式配置的 `system_prompt` / `append_system_prompt`。
- 界面与消息仅英文，删除语言设置、自动检测与 `/lang`。
- 删除检查更新与 `/upgrade`，以及 `/status`、`/usage`、`/version`、`/config`、`/memory`、`/doctor`、`/web`、`/ps`（`/btw`）。
- 压缩上下文命令为 `/compact`（原 `/compress`）。
- 不发现、注册、注入 skills（删除 `/skills`）。未识别的 `/` 命令原样交给 Agent。
- 删除 Provider 预设与 cc-switch 导入，Provider 只能手动添加或关联全局 Provider。
- 默认数据目录为 `~/.agent-bridge`。旧版迁移需手动操作，见 [docs/MIGRATION.md](docs/MIGRATION.md)。

## 环境要求

- Go 1.25+
- Node.js 与 pnpm（构建管理页面）
- 主机上已安装并登录 Claude Code 和/或 Codex CLI

## 构建

先构建 Web 页面，再构建 Go 二进制：

```sh
cd web && pnpm install --frozen-lockfile && pnpm build && cd ..
go build -o agent-bridge ./cmd/agent-bridge
```

交叉编译（如 Linux x86_64）：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o agent-bridge ./cmd/agent-bridge
```

`Makefile` 也提供 `make build`、`make build-noweb`、`make test`、`make lint`。

## 快速开始

```sh
mkdir -p ~/.agent-bridge
cp config.example.toml ~/.agent-bridge/config.toml   # 或：agent-bridge config example
# 编辑后运行：
agent-bridge
```

配置文件依次查找 `./config.toml`、`~/.agent-bridge/config.toml`，也可用 `--config /path/to/config.toml` 指定。秘密信息可写成 `${ENV_VAR}` 从环境变量读取。

最小示例：

```toml
[log]
level = "info"

[[projects]]
name = "my-project"

[projects.agent]
type = "claudecode" # "claudecode" | "codex"

[projects.agent.options]
work_dir = "/path/to/project"
mode = "default"

[[projects.platforms]]
type = "telegram"   # "feishu" | "weixin" | "telegram" | "qq" | "qqbot"

[projects.platforms.options]
token = "${TELEGRAM_BOT_TOKEN}"
allow_from = "*"
```

各平台完整写法见 [config.example.toml](config.example.toml)。

## 平台接入

| 平台 | 类型 | 文档 |
| --- | --- | --- |
| 飞书 / Lark | `feishu` | [docs/feishu.md](docs/feishu.md)（或 `agent-bridge feishu setup`） |
| 微信（ilink） | `weixin` | [docs/weixin.md](docs/weixin.md)（或 `agent-bridge weixin setup`） |
| Telegram | `telegram` | [docs/telegram.md](docs/telegram.md) |
| QQ OneBot / NapCat | `qq` | [docs/qq.md](docs/qq.md) |
| QQ 官方机器人 | `qqbot` | [docs/qqbot.md](docs/qqbot.md) |

## 聊天命令

| 命令 | 说明 |
| --- | --- |
| `/new` | 新建会话 |
| `/list`（`/sessions`） | 列出会话 |
| `/switch` | 切换会话 |
| `/name`（`/rename`） | 重命名当前会话 |
| `/current` | 查看当前会话 |
| `/history` | 查看最近消息 |
| `/search`（`/find`） | 搜索会话 |
| `/delete`（`/del`、`/rm`） | 删除会话 |
| `/stop`、`/cancel` | 停止当前轮次 |
| `/compact` | 压缩 Agent 上下文 |
| `/model` | 查看或切换模型 |
| `/effort` | 调整推理强度（保留历史） |
| `/mode` | 切换权限模式 |
| `/allow` | 批准工具权限 |
| `/provider` | 查看或切换 Provider |
| `/quiet` | 开关进度消息 |
| `/dir`（`/cd`） | 切换工作目录 |
| `/workspace`（`/ws`） | 管理工作区 |
| `/shell`（`/sh`、`/run`） | 执行 shell 命令 |
| `/diff` | 查看工作区 diff |
| `/show` | 查看文件 |
| `/tts` | 语音合成设置 |
| `/alias` | 管理命令别名 |
| `/commands` | 管理自定义命令 |
| `/whoami`（`/myid`） | 查看自己的用户 ID |
| `/restart` | 重启 Agent-Bridge |
| `/help` | 帮助 |

命令支持唯一前缀匹配。其他 `/…` 消息原样转发给 Agent。

## 进度显示

全局设置在 `[display]`，项目可在 `[projects.display]` 中覆盖。两项默认均为 `false`，也可在管理页面修改。

```toml
[display]
cleanup_progress_on_complete = true
collapse_tool_messages = true
```

- `cleanup_progress_on_complete`：在支持删除消息的渠道（Telegram、飞书）上，轮次结束后删除思考/工具消息。助手文本和权限请求保留。临时的 API 重试与网络诊断在最终回复送达后删除；失败时保留。
- `collapse_tool_messages`：只显示当前活动（如 "Running command"、"Reading files"），不显示工具输入输出，支持编辑的渠道会更新同一条进度消息。

空闲时单独发送的图片和文件会等待下一条提示。`/model`、`/effort`、`/mode`、`/provider` 保留暂存附件；`/new` 和成功的 `/switch` 会丢弃。Claude Code 的 API 重试和网络等待会作为非终止状态消息即时转发。

## 管理页面

在配置中启用：

```toml
[management]
enabled = true
port = 9820
token = "${AGENT_BRIDGE_ADMIN_TOKEN}"   # 必填
```

然后打开 `http://127.0.0.1:9820`。token 是唯一的认证方式，请只在本机、VPN 或带 TLS 的反向代理后访问。

## 命令行

```text
agent-bridge [--config path] [--force]
agent-bridge daemon install|uninstall|start|stop|restart|status|logs [-f] [-n N]
agent-bridge send -m "text" | --stdin  [-p project] [-s session]
agent-bridge sessions list | show <id> [-n N]
agent-bridge agent-sid
agent-bridge provider add|list|remove --project <name> ...
agent-bridge feishu setup|new|bind
agent-bridge weixin setup|new|bind
agent-bridge config example|format|path
```

## 定时任务

没有内置调度器，请使用 cron、systemd timer 等：

```cron
0 9 * * * agent-bridge send -p my-project -m "Summarize yesterday's commits"
```

或启用 `[webhook]`，由外部系统调用。

## 目录结构

```text
cmd/agent-bridge/  CLI 与进程入口
core/              会话、路由、消息、平台无关接口
agent/             Claude Code、Codex 适配
platform/          微信、飞书、Telegram、QQ、QQ 官方机器人
config/            配置解析
daemon/            systemd / launchd / Windows 服务管理
web/src/           管理页面源码
docs/              平台接入与迁移文档
```

## 开发

```sh
gofmt -w .
go test ./...
cd web && pnpm build
```

开发约束见 [AGENTS.md](AGENTS.md)。

## 致谢

基于 chenhg5 及贡献者的 [cc-connect](https://github.com/chenhg5/cc-connect)。原始许可条款请参阅上游仓库。
