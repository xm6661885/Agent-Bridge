# Agent-Bridge

**English** | [简体中文](README.zh-CN.md)

Agent-Bridge connects local AI coding agents (**Claude Code** and **Codex**) to the chat apps you already use: **Weixin**, **Feishu/Lark**, **Telegram**, and **QQ** (OneBot/NapCat and the QQ Official Bot). Send a message from your phone, and the agent runs on your own machine against your own project.

> Agent-Bridge is a heavily modified fork of [chenhg5/cc-connect](https://github.com/chenhg5/cc-connect). It was renamed, trimmed down, and reworked; it ships as an independent source tree without the upstream Git history. Thanks to the cc-connect authors for the original work.

## Features

- **Agents**: Claude Code (`claudecode`) and Codex (`codex`).
- **Platforms**: Weixin personal account (ilink, QR login), Feishu/Lark, Telegram, QQ via OneBot v11/NapCat (`qq`), QQ Official Bot (`qqbot`).
- **Multiple projects**: each `[[projects]]` entry binds one agent + working directory to one or more platforms.
- **Session management**: create, list, switch, rename, search, and delete conversations from chat.
- **Live steering**: send a new message while the agent is running to add instructions to the current turn.
- **Progress display**: optional collapsed tool activity and automatic cleanup of thinking/tool messages.
- **Attachments**: images and files are staged and passed to the next agent prompt.
- **Providers**: manage API providers per project (CLI, chat, or Web Admin).
- **Web Admin**: built-in management UI with real-time chat over WebSocket.
- **Daemon mode**: install as a systemd / launchd / Windows scheduled-task service.
- **External automation**: trigger messages through the webhook or `agent-bridge send`.
- **No prompt injection**: Agent-Bridge never injects bridge or platform prompts into agent sessions.

## Differences from cc-connect

- Trimmed messaging channels down to Weixin, Feishu, Telegram, and QQ (agent support was not the focus of the trimming).
- Reworked file transfer logic: attachments are staged and delivered with the next prompt more reliably.
- Messages sent while the agent is running steer the current turn instead of being queued.
- `/reasoning` renamed to `/effort`; changing effort resumes the existing conversation.
- Polished user-facing messages and removed emoji.
- QQ: group whitelist (`allow_groups`) and reply-only-when-@mentioned in groups.
- Removed built-in cron, timers, and agent heartbeat. Use an external scheduler that calls the webhook or `agent-bridge send`.
- Removed multi-bot relay, `/bind`, and `relay send`.
- No prompt injection of any kind. Put project instructions in `CLAUDE.md` / `AGENTS.md`. Claude Code only receives the `system_prompt` / `append_system_prompt` you configure explicitly.
- English-only UI and messages. Removed language settings, auto detection, and `/lang`.
- Removed update checks and `/upgrade`, plus `/status`, `/usage`, `/version`, `/config`, `/memory`, `/doctor`, `/web`, and `/ps` (`/btw`).
- The context-compaction command is `/compact` (was `/compress`).
- No skill discovery, registration, or injection (`/skills` removed). Unrecognized `/` commands pass through to the agent unchanged.
- Removed provider presets and cc-switch import. Providers are added manually or linked to global providers.
- Default data directory is `~/.agent-bridge`. Migration from older versions is manual; see [docs/MIGRATION.md](docs/MIGRATION.md).

## Requirements

- Go 1.25+
- Node.js and pnpm (for the Web Admin)
- Claude Code and/or Codex CLI installed and logged in on the host

## Build

Build the Web UI first, then the Go binary:

```sh
cd web && pnpm install --frozen-lockfile && pnpm build && cd ..
go build -o agent-bridge ./cmd/agent-bridge
```

Cross-compile, e.g. for Linux x86_64:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o agent-bridge ./cmd/agent-bridge
```

The `Makefile` also provides `make build`, `make build-noweb`, `make test`, and `make lint`.

## Quick start

```sh
mkdir -p ~/.agent-bridge
cp config.example.toml ~/.agent-bridge/config.toml   # or: agent-bridge config example
# edit the file, then:
agent-bridge
```

The config is looked up at `./config.toml`, then `~/.agent-bridge/config.toml`, or pass `--config /path/to/config.toml`. Secrets can be referenced as `${ENV_VAR}`.

Minimal example:

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

See [config.example.toml](config.example.toml) for every platform.

## Platform setup

| Platform | Type | Guide |
| --- | --- | --- |
| Feishu / Lark | `feishu` | [docs/feishu.md](docs/feishu.md) (or `agent-bridge feishu setup`) |
| Weixin (ilink) | `weixin` | [docs/weixin.md](docs/weixin.md) (or `agent-bridge weixin setup`) |
| Telegram | `telegram` | [docs/telegram.md](docs/telegram.md) |
| QQ OneBot / NapCat | `qq` | [docs/qq.md](docs/qq.md) |
| QQ Official Bot | `qqbot` | [docs/qqbot.md](docs/qqbot.md) |

## Chat commands

| Command | Description |
| --- | --- |
| `/new` | Start a new session |
| `/list` (`/sessions`) | List sessions |
| `/switch` | Switch to another session |
| `/name` (`/rename`) | Rename the current session |
| `/current` | Show the current session |
| `/history` | Show recent messages |
| `/search` (`/find`) | Search sessions |
| `/delete` (`/del`, `/rm`) | Delete sessions |
| `/stop`, `/cancel` | Stop the running turn |
| `/compact` | Compact the agent context |
| `/model` | Show or change the model |
| `/effort` | Change reasoning effort (keeps history) |
| `/mode` | Change permission mode |
| `/allow` | Approve tool permissions |
| `/provider` | Show or switch provider |
| `/quiet` | Toggle progress messages |
| `/dir` (`/cd`) | Change working directory |
| `/workspace` (`/ws`) | Manage workspaces |
| `/shell` (`/sh`, `/run`) | Run a shell command |
| `/diff` | Show working-tree diff |
| `/show` | Show a file |
| `/tts` | Text-to-speech settings |
| `/alias` | Manage command aliases |
| `/commands` | Manage custom commands |
| `/whoami` (`/myid`) | Show your user ID |
| `/restart` | Restart Agent-Bridge |
| `/help` | Show help |

Commands accept unique prefixes. Any other `/…` message is forwarded to the agent as-is.

## Progress display

Global settings live in `[display]`; each project can override them in `[projects.display]`. Both default to `false` and are also editable in the Web Admin.

```toml
[display]
cleanup_progress_on_complete = true
collapse_tool_messages = true
```

- `cleanup_progress_on_complete` deletes thinking/tool messages when a turn ends on channels that support deletion (Telegram, Feishu). Assistant text and permission requests stay. Temporary API-retry and network diagnostics are removed after the final reply is delivered; on failure they stay visible.
- `collapse_tool_messages` shows only the current activity (e.g. "Running command", "Reading files") without tool inputs/results, editing one progress message where supported.

Images and files sent without text while idle wait for the next prompt. `/model`, `/effort`, `/mode`, and `/provider` keep staged attachments; `/new` and a successful `/switch` discard them. Claude Code API retries and network waits are forwarded as non-terminal status messages.

## Web Admin

Enable the management UI in the config:

```toml
[management]
enabled = true
port = 9820
token = "${AGENT_BRIDGE_ADMIN_TOKEN}"   # required
```

Then open `http://127.0.0.1:9820`. Keep it behind localhost, a VPN, or a reverse proxy with TLS; the token is the only authentication.

## CLI

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

## Scheduled tasks

There is no built-in scheduler. Use cron, systemd timers, or similar:

```cron
0 9 * * * agent-bridge send -p my-project -m "Summarize yesterday's commits"
```

Or enable `[webhook]` and call it from any external system.

## Project layout

```text
cmd/agent-bridge/  CLI and process entry point
core/              sessions, routing, messages, platform-agnostic interfaces
agent/             Claude Code and Codex adapters
platform/          Weixin, Feishu, Telegram, QQ, QQ Bot
config/            config parsing
daemon/            systemd / launchd / Windows service management
web/src/           Web Admin source
docs/              platform guides and migration notes
```

## Development

```sh
gofmt -w .
go test ./...
cd web && pnpm build
```

See [AGENTS.md](AGENTS.md) for contribution constraints.

## Credits

Based on [cc-connect](https://github.com/chenhg5/cc-connect) by chenhg5 and contributors. 

## License

[MIT](LICENSE). The original cc-connect copyright notice is retained.
