# Agent-Bridge

[English](README.md) | **简体中文**

Agent-Bridge 把本地 AI 编程 Agent（**Claude Code**、**Codex**）连接到常用聊天软件：**微信**、**Telegram** 和 **QQ**（OneBot/NapCat）。在手机上发消息，Agent 就在你自己的机器、自己的项目里运行。

> Agent-Bridge 由 [chenhg5/cc-connect](https://github.com/chenhg5/cc-connect) 大量修改而来，已改名、精简并重构，作为独立源码树发布，不包含上游 Git 历史。感谢 cc-connect 作者的原始工作。

## 功能

- **Agent**：Claude Code（`claudecode`）、Codex（`codex`）。
- **平台**：微信个人号（ilink，扫码登录）、Telegram、QQ OneBot v11/NapCat（`qq`）。
- **多项目**：每个 `[[projects]]` 绑定一个 Agent + 工作目录，可接入多个平台。
- **会话管理**：在聊天中新建、列出、切换、重命名、搜索、删除会话。
- **运行中追加指令**：Agent 运行时直接发送新消息即可 steer 当前轮次。
- **进度显示**：可折叠工具活动，轮次结束后自动清理思考/工具消息。
- **附件**：图片和文件会暂存并随下一条提示交给 Agent。
- **管理页面**：内置 Web 管理界面，支持 WebSocket 实时聊天。
- **守护进程**：可安装为 systemd / launchd 服务。
- **外部自动化**：通过 webhook 触发 Agent 回合；用 `agent-bridge send` 发送文件和媒体。
- **不注入提示词**：不会向 Agent 会话注入任何桥接或平台提示词。

## 与 cc-connect 的区别

- 精简消息渠道，只保留微信、Telegram、QQ（OneBot）（精简的是渠道而非 Agent 支持）。
- 优化文件传送逻辑：附件暂存后随下一条提示可靠地交给 Agent。
- Agent 运行中发送的消息会 steer 当前对话，而不是排队（queue）。
- `/reasoning` 改为 `/effort`，调整后续接原会话。
- 优化提示文案，去除 emoji。
- QQ 支持群聊白名单（`allow_groups`）和群内仅被 @ 时回复。
- 其他若干 bug 修复。
- 删除内置 cron、timer、Agent heartbeat；定时任务请用外部调度器调用 webhook。
- 删除多机器人中继、`/bind` 与 `relay send`。
- 删除多工作区模式（`/workspace`）与 OS 用户隔离（`run_as_user`、`doctor user-isolation`）。
- `agent-bridge send` 只发送附件；`-m` 仅作为 `--image`/`--file` 的附带文字，不能单独发送纯文字。
- 不注入任何提示词，项目说明写在 `CLAUDE.md` / `AGENTS.md`。Claude Code 只透传你显式配置的 `system_prompt` / `append_system_prompt`。
- 界面与消息仅英文，删除语言设置、自动检测与 `/lang`。
- 删除检查更新与 `/upgrade`，以及 `/status`、`/usage`、`/version`、`/config`、`/memory`、`/doctor`、`/web`、`/ps`（`/btw`）。
- 压缩上下文命令为 `/compact`（原 `/compress`）。
- 不发现、注册、注入 skills（删除 `/skills`）。未识别的 `/` 命令原样交给 Agent。
- 删除自定义 API Provider（`/provider`、`agent-bridge provider`、`[[providers]]`），Claude Code 和 Codex 直接使用各自的系统配置。
- 删除 TTS 与语音转写（`/tts`、`[tts]`、`[speech]`），收到的语音消息作为音频文件附件交给 Agent。
- 删除 `card_mode` 以及 PowerShell/cmd shell 支持。
- 删除飞书/Lark 与 QQ 官方机器人（`qqbot`）。删除 Windows 支持，仅支持 macOS 和 Linux。
- 移除 Go 和管理页面中的 i18n 层（不再依赖 i18next）。
- 默认数据目录为 `~/.agent-bridge`。旧版迁移需手动操作。

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
type = "telegram"   # "weixin" | "telegram" | "qq"

[projects.platforms.options]
token = "${TELEGRAM_BOT_TOKEN}"
allow_from = "*"
```

各平台完整写法见 [config.example.toml](config.example.toml)。

## 平台接入

| 平台 | 类型 | 文档 |
| --- | --- | --- |
| 微信（ilink） | `weixin` | [docs/weixin.md](docs/weixin.md)（或 `agent-bridge weixin setup`） |
| Telegram | `telegram` | [docs/telegram.md](docs/telegram.md) |
| QQ OneBot / NapCat | `qq` | [docs/qq.md](docs/qq.md) |

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
| `/quiet` | 开关进度消息 |
| `/dir`（`/cd`） | 切换工作目录 |
| `/shell`（`/sh`、`/run`） | 执行 shell 命令 |
| `/diff` | 查看工作区 diff |
| `/show` | 查看文件 |
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

- `cleanup_progress_on_complete`：在支持删除消息的渠道（Telegram）上，轮次结束后删除思考/工具消息。助手文本和权限请求保留。临时的 API 重试与网络诊断在最终回复送达后删除；失败时保留。
- `collapse_tool_messages`：只显示当前活动（如 "Running command"、"Reading files"），不显示工具输入输出，支持编辑的渠道会更新同一条进度消息。

空闲时单独发送的图片和文件会等待下一条提示。`/model`、`/effort`、`/mode` 保留暂存附件；`/new` 和成功的 `/switch` 会丢弃。Claude Code 的 API 重试和网络等待会作为非终止状态消息即时转发。

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
agent-bridge send --image|--file|--audio|--video <path>  [-m caption] [-p project] [-s session]
agent-bridge sessions list | show <id> [-n N]
agent-bridge agent-sid
agent-bridge weixin setup|new|bind
agent-bridge config example|format|path
```

## 定时任务

没有内置调度器。请启用 `[webhook]`，由 cron、systemd timer 或其他外部系统调用；生成的文件可用 `agent-bridge send --file <path>` 发送。

## 目录结构

```text
cmd/agent-bridge/  CLI 与进程入口
core/              会话、路由、消息、平台无关接口
agent/             Claude Code、Codex 适配
platform/          微信、Telegram、QQ（OneBot）
config/            配置解析
daemon/            systemd / launchd 服务管理
web/src/           管理页面源码
docs/              平台接入与迁移文档
```

## 开发

```sh
gofmt -w .
go test ./...
cd web && pnpm build
```

开发约束见 [CLAUDE.md](CLAUDE.md)。

## 致谢

基于 chenhg5 及贡献者的 [cc-connect](https://github.com/chenhg5/cc-connect)。

## 许可证

[MIT](LICENSE)，保留 cc-connect 原版权声明。
