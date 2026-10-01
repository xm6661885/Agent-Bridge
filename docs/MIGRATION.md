# 从 cc-connect 手动升级到 agent-bridge

本次只修改本机源码，没有切换 wyse3040 上正在运行的服务。2026-09-27 盘点时，旧实例使用 `~/.config/systemd/user/cc-connect.service`、`~/.local/bin/cc-connect` 和 `~/.cc-connect`；旧数据目录里有 `config.toml`、`sessions/`、`projects/`、`weixin/`、`dir_history.json`、`relay_bindings.json`、`crons/`、`timers/` 等文件。旧 `timers/jobs.json` 有任务，切换前必须单独核对。

**新程序没有自动迁移代码。** 启动前需手动复制数据并改配置。旧目录和旧服务单元应保留到验证完成，以便回退。

## 需要迁移的数据

1. 停止旧服务，防止复制期间会话状态继续变化：`systemctl --user stop cc-connect.service`。
2. 创建 `~/.agent-bridge`，从 `~/.cc-connect` 复制 `config.toml`、`sessions/`、`projects/`、`weixin/` 和 `dir_history.json`。复制时保留权限和时间戳；旧目录不要删除。
3. 编辑新 `config.toml`：将显式 `data_dir` 和其他指向旧名称的绝对路径改成新路径。保留原项目名称可继续对应旧会话文件。Agent 只能用 `claudecode`、`codex`；消息渠道只能用 `feishu`/`lark`、`weixin`、`telegram`、`qq`、`qqbot`。删除旧配置中其他 Agent 和渠道的项目或平台块。
4. 删除旧配置中的 `[relay]`、`[projects.heartbeat]`、cron/timer 相关设置。`relay_bindings.json` 不迁移；`/bind` 和 `agent-bridge relay` 已删除。以前 `/bind setup` 写入项目 `AGENTS.md` 等文件的说明需手动检查，标记可能是 `cc-connect-instructions` 或 `agent-bridge-instructions`。
5. 删除旧配置中的顶层 `language`、Claude Code 的 `inject_bridge_prompt`；新版只有英文界面，不注入任何提示词（请自行维护 `CLAUDE.md`）。`/compress` 改名为 `/compact`，`/status`、`/usage`、`/version`、`/config`、`/memory`、`/doctor`、`/web`、`/ps`、`/lang`、`/upgrade` 已删除；`/skills` 与 skill 命令也已删除，未识别的 `/xxx` 会直接透传给 Agent。顶层 `provider_presets_url` 可删除（已无预设功能），`agent-bridge provider import`/`presets` 子命令已删除。若 `disabled_commands`、别名或自定义命令引用了它们，请一并修改。
6. `crons/`、`timers/`、`heartbeat_state.json`、`run/`、`logs/`、`agent-prompts/`、`daemon.json` 和配置锁文件不迁移。需要继续运行的任务应转到你的外部调度方案；新版不会执行旧任务，也不会清理旧任务文件。

例如，在旧服务停止后，可以按需运行以下复制命令（先检查源目录是否存在）：

```sh
mkdir -p ~/.agent-bridge
cp -a ~/.cc-connect/config.toml ~/.agent-bridge/
for name in sessions projects weixin; do
  if [ -d "$HOME/.cc-connect/$name" ]; then cp -a "$HOME/.cc-connect/$name" "$HOME/.agent-bridge/"; fi
done
if [ -f ~/.cc-connect/dir_history.json ]; then cp -a ~/.cc-connect/dir_history.json ~/.agent-bridge/; fi
```

## 服务切换

在 new-server 编译的 Linux x86_64 二进制确认可运行、新配置检查完成后，把二进制安装为 `~/.local/bin/agent-bridge`，创建 `~/.config/systemd/user/agent-bridge.service`。新单元的 `WorkingDirectory` 指向 `~/.agent-bridge`，`ExecStart` 指向新二进制，旧 `CC_` 环境变量改为 `AGENT_BRIDGE_`。重点检查 `CC_LOG_FILE`、`CC_LOG_MAX_SIZE`、`CC_LOG_MAX_BACKUPS`、`CC_PROJECT`、`CC_SESSION_KEY`、`CC_DATA_DIR` 对应的新名称。外部脚本中的 `cc-connect` 命令也要更新。

再执行 `systemctl --user daemon-reload`、`systemctl --user enable --now agent-bridge.service`，核对服务状态、监听端口、管理页面、项目与会话数量，以及微信、飞书、Telegram、QQ 的实际收发消息。验证前不要删除旧目录和旧服务单元。新程序不会自动切换或更新旧服务。
