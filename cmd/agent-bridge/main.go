package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	agentbridge "agent-bridge"
	"agent-bridge/config"
	"agent-bridge/core"
	"agent-bridge/daemon"
	// Agent and platform imports are in separate plugin_*.go files
	// controlled by build tags. See Makefile for selective compilation.
)

var (
	version   = "dev"
	commit    = "none"
	buildTime = "unknown"
)

// globalAPIServer holds the running API server so the config-reload path can
// re-apply hot-reloadable settings (e.g. max attachment size) without threading
// it through the engine's reload closure. nil when the API server is disabled.
var globalAPIServer *core.APIServer

// defaultResetOnIdleMins is applied when a project does not set
// reset_on_idle_mins. After this many minutes of user inactivity, agent-bridge
// rotates to a fresh session for the next message instead of resuming the
// previous transcript via --continue. This avoids "context drift" where stale
// chat history (failed commands, debugging noise, abandoned tangents) is
// repeatedly re-ingested and starts to dominate the model's attention. The
// previous session is preserved and remains accessible via /list and /switch.
//
// Set reset_on_idle_mins = 0 in config.toml to opt out and restore the
// previous behavior of always continuing the prior session.
const defaultResetOnIdleMins = 0

// resolveResetOnIdle returns the configured reset-on-idle duration for a
// project, applying defaultResetOnIdleMins when the field is unset. The second
// return value indicates whether the default was applied, so the caller can
// emit a one-time nudge log directing users to the docs.
func resolveResetOnIdle(configured *int) (time.Duration, bool) {
	if configured != nil {
		return time.Duration(*configured) * time.Minute, false
	}
	return time.Duration(defaultResetOnIdleMins) * time.Minute, true
}

// logSizeSource describes where the resolved log size came from, so the
// caller can log it and operators can audit the active setting without
// grepping systemd/launchd definitions.
type logSizeSource string

const (
	logSizeSourceFlag    logSizeSource = "flag"
	logSizeSourceEnv     logSizeSource = "env"
	logSizeSourceDefault logSizeSource = "default"
)

// resolveLogMaxSize picks the effective max log size in bytes, applying the
// priority order: explicit flag value > AGENT_BRIDGE_LOG_MAX_SIZE env var > built-in
// default. flagValue is the raw string from --log-max-size ("" if not set).
// Returns the byte count and which source won. Invalid flag/env values are
// logged to stderr and the value is ignored — a malformed setting must never
// silently downgrade to "0 bytes" or another surprise.
func resolveLogMaxSize(flagValue string) (int64, logSizeSource) {
	if strings.TrimSpace(flagValue) != "" {
		n, err := daemon.ParseLogSize(flagValue)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: ignoring --log-max-size=%q: %v\n", flagValue, err)
		} else {
			return n, logSizeSourceFlag
		}
	}
	if v := os.Getenv("AGENT_BRIDGE_LOG_MAX_SIZE"); v != "" {
		n, err := daemon.ParseLogSize(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: ignoring AGENT_BRIDGE_LOG_MAX_SIZE=%q: %v\n", v, err)
		} else {
			return n, logSizeSourceEnv
		}
	}
	return int64(daemon.DefaultLogMaxSize), logSizeSourceDefault
}

// preScanLogMaxSizeFlag returns the value passed via --log-max-size before
// flag.Parse() runs, so the rotating-writer setup can honour the flag too.
// Returns "" if the flag is absent. Both "--log-max-size VALUE" and
// "--log-max-size=VALUE" forms are recognised.
func preScanLogMaxSizeFlag(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--log-max-size" {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if strings.HasPrefix(a, "--log-max-size=") {
			return strings.TrimPrefix(a, "--log-max-size=")
		}
	}
	return ""
}

// logBackupsSource describes where the resolved max-backups count came
// from, mirroring logSizeSource so operators can audit the active value
// from the startup log line alone.
type logBackupsSource string

const (
	logBackupsSourceFlag    logBackupsSource = "flag"
	logBackupsSourceEnv     logBackupsSource = "env"
	logBackupsSourceDefault logBackupsSource = "default"
)

// resolveLogMaxBackups picks the effective number of rotated log backups
// to retain, with the same priority order as resolveLogMaxSize: explicit
// flag value > AGENT_BRIDGE_LOG_MAX_BACKUPS env var > built-in default. Returns
// the count and which source won. Invalid inputs are logged to stderr
// and the value is ignored so a typo never silently downgrades to "0".
func resolveLogMaxBackups(flagValue string) (int, logBackupsSource) {
	if strings.TrimSpace(flagValue) != "" {
		n, err := daemon.ParseLogBackups(flagValue)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: ignoring --log-max-backups=%q: %v\n", flagValue, err)
		} else {
			return n, logBackupsSourceFlag
		}
	}
	if v := os.Getenv("AGENT_BRIDGE_LOG_MAX_BACKUPS"); v != "" {
		n, err := daemon.ParseLogBackups(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: ignoring AGENT_BRIDGE_LOG_MAX_BACKUPS=%q: %v\n", v, err)
		} else {
			return n, logBackupsSourceEnv
		}
	}
	return daemon.DefaultLogMaxBackups, logBackupsSourceDefault
}

// preScanLogMaxBackupsFlag returns the value passed via --log-max-backups
// before flag.Parse() runs, mirroring preScanLogMaxSizeFlag. Returns ""
// if the flag is absent.
func preScanLogMaxBackupsFlag(args []string) string {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--log-max-backups" {
			if i+1 < len(args) {
				return args[i+1]
			}
			return ""
		}
		if strings.HasPrefix(a, "--log-max-backups=") {
			return strings.TrimPrefix(a, "--log-max-backups=")
		}
	}
	return ""
}

// resolveMaxAttachmentSize returns the per-attachment size limit in bytes for
// the /send API. Priority: AGENT_BRIDGE_MAX_ATTACHMENT_SIZE_MB env var (MiB) >
// config max_attachment_size_mb > core.DefaultMaxAttachmentSize. The env var
// intentionally uses the same MiB unit as the config field so the two knobs
// cannot silently disagree by a factor of 1<<20. A malformed or non-positive
// env value is ignored (falling through to config/default) rather than being
// fatal — the same lenient posture as resolveLogMaxSize, which also warns so
// a typo never silently downgrades the setting.
func resolveMaxAttachmentSize(cfg *config.Config) int64 {
	if v := strings.TrimSpace(os.Getenv("AGENT_BRIDGE_MAX_ATTACHMENT_SIZE_MB")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n << 20
		}
		fmt.Fprintf(os.Stderr, "warning: ignoring AGENT_BRIDGE_MAX_ATTACHMENT_SIZE_MB=%q: must be a positive integer (MiB)\n", v)
	}
	if cfg != nil && cfg.MaxAttachmentSizeMB > 0 {
		return int64(cfg.MaxAttachmentSizeMB) << 20
	}
	return core.DefaultMaxAttachmentSize
}

type initialModelRefreshStarter interface {
	StartInitialModelRefresh()
}

var topLevelCommandHandlers = map[string]func([]string){
	"config-example": func(_ []string) {
		fmt.Print(agentbridge.ConfigExampleTOML)
	},
	"config":    runConfig,
	"send":      runSend,
	"sessions":  runSessions,
	"agent-sid": runAgentSID,
	"daemon":    runDaemon,
	"weixin":    runWeixin,
	"web":       runWeb,
}

func main() {

	// When started as a daemon (AGENT_BRIDGE_LOG_FILE set), redirect logs to a rotating file.
	// Log file setup happens before flag.Parse() so the rotating writer is in
	// place before any slog output. To still honour --log-max-size, we
	// pre-scan os.Args here for the flag value; this is a small, deliberate
	// duplication of flag parsing for one well-known key.
	var logWriter io.Writer
	var logCloser io.Closer
	if logFile := os.Getenv("AGENT_BRIDGE_LOG_FILE"); logFile != "" {
		maxSize, maxSizeSrc := resolveLogMaxSize(preScanLogMaxSizeFlag(os.Args[1:]))
		maxBackups, maxBackupsSrc := resolveLogMaxBackups(preScanLogMaxBackupsFlag(os.Args[1:]))
		fmt.Fprintf(os.Stderr, "log: redirecting to %s with max_size=%d bytes (source: %s), max_backups=%d (source: %s)\n", logFile, maxSize, maxSizeSrc, maxBackups, maxBackupsSrc)
		w, err := daemon.NewRotatingWriter(logFile, maxSize, maxBackups)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open log file %s: %v\n", logFile, err)
			os.Exit(1)
		}
		logWriter = w
		logCloser = w
		slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelInfo})))
	}

	rootOpts, err := parseRootCLIOptions(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(2)
	}

	// Cross-check: the rotating-writer setup above consumed a pre-scanned
	// value of --log-max-size. Validate the parsed value too so malformed
	// values surface a clear warning.
	if strings.TrimSpace(rootOpts.logMaxSize) != "" {
		if _, err := daemon.ParseLogSize(rootOpts.logMaxSize); err != nil {
			fmt.Fprintf(os.Stderr, "warning: --log-max-size=%q: %v\n", rootOpts.logMaxSize, err)
		}
	}
	if rootOpts.logMaxBackups < 0 {
		fmt.Fprintf(os.Stderr, "warning: --log-max-backups=%d must be >= 0 (0 means use env/default)\n", rootOpts.logMaxBackups)
	}

	if rootOpts.showVersion {
		fmt.Printf("agent-bridge %s\ncommit:  %s\nbuilt:   %s\n", version, commit, buildTime)
		return
	}

	if runTopLevelCommand(rootOpts.args) {
		return
	}

	if err := validateNoExtraTopLevelArgs(rootOpts.args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n\n", err)
		printUsage()
		os.Exit(1)
	}

	core.CurrentVersion = version
	core.CurrentCommit = commit
	core.CurrentBuildTime = buildTime

	configPath := resolveConfigPath(rootOpts.configPath)

	// Handle --force: kill any existing instance before we try to acquire the lock
	if rootOpts.force {
		if KillExistingInstance(configPath) {
			slog.Info("killed existing instance via --force")
		}
	}

	// Acquire instance lock to prevent duplicate processes
	instanceLock, err := AcquireInstanceLock(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		fmt.Fprintf(os.Stderr, "Use --force to kill the existing instance.\n")
		os.Exit(1)
	}
	slog.Info("acquired instance lock", "path", instanceLock.Path())

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		if err := bootstrapConfig(configPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created default config at %s\n", configPath)
		fmt.Println("Please edit this file to add your agent and platform credentials, then run agent-bridge again.")
		os.Exit(0)
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config (%s): %v\n", configPath, err)
		os.Exit(1)
	}

	config.ConfigPath = configPath
	slog.Info("config loaded", "path", configPath)

	if len(cfg.Projects) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no projects configured in %s\n", configPath)
		fmt.Fprintln(os.Stderr, "Add at least one [[project]] section to your config.toml, or run:")
		fmt.Fprintln(os.Stderr, "  agent-bridge init")
		os.Exit(1)
	}

	setupLogger(cfg.Log.Level, logWriter)

	engines := make([]*core.Engine, 0, len(cfg.Projects))
	effectiveWorkDirs := make([]string, 0, len(cfg.Projects))

	for _, proj := range cfg.Projects {
		agent, err := core.CreateAgent(proj.Agent.Type, buildAgentOptions(cfg.DataDir, proj))
		if err != nil {
			slog.Error("failed to create agent", "project", proj.Name, "error", err)
			os.Exit(1)
		}

		var platforms []core.Platform
		for _, pc := range proj.Platforms {
			opts := make(map[string]any, len(pc.Options)+2)
			for k, v := range pc.Options {
				opts[k] = v
			}
			opts["cc_data_dir"] = cfg.DataDir
			opts["cc_project"] = proj.Name
			p, err := core.CreatePlatform(pc.Type, opts)
			if err != nil {
				slog.Error("failed to create platform", "project", proj.Name, "type", pc.Type, "error", err)
				os.Exit(1)
			}
			platforms = append(platforms, p)
		}

		workDir, _ := proj.Agent.Options["work_dir"].(string)
		projectState := core.NewProjectStateStore(projectStatePath(cfg.DataDir, proj.Name))
		effectiveWorkDir := applyProjectStateOverride(proj.Name, agent, workDir, projectState)
		startInitialRefresh(agent)
		sessionFile := sessionStorePath(cfg.DataDir, proj.Name, effectiveWorkDir)

		engine := core.NewEngine(proj.Name, agent, platforms, sessionFile)
		// Wire display settings including show_context_indicator and reply_footer
		// Global [display] config can be overridden by project-level settings
		_, _, _, _, _, showCtx, showFooter, _ := config.EffectiveDisplay(cfg, &proj)
		engine.SetShowContextIndicator(showCtx)
		showWorkdir := true
		if proj.ShowWorkdirIndicator != nil {
			showWorkdir = *proj.ShowWorkdirIndicator
		}
		engine.SetShowWorkdirIndicator(showWorkdir)
		engine.SetReplyFooterEnabled(showFooter)
		engine.SetAttachmentSendEnabled(cfg.AttachmentSend != "off")
		engine.SetFilterExternalSessions(proj.FilterExternalSessions != nil && *proj.FilterExternalSessions)
		engine.SetBaseWorkDir(workDir)
		engine.SetProjectStateStore(projectState)
		engine.SetDataDir(cfg.DataDir)

		// Wire terminal observation (--observe / [projects.observe])
		observeEnabled := rootOpts.observe
		obsChan := rootOpts.observeChannel
		if proj.Observe != nil {
			if !observeEnabled && proj.Observe.Enabled {
				observeEnabled = true
			}
			if obsChan == "" && proj.Observe.Channel != "" {
				obsChan = proj.Observe.Channel
			}
		}
		if observeEnabled {
			if obsChan == "" {
				slog.Error("observe: channel is required (use --observe-channel or set channel in [projects.observe])")
				os.Exit(1)
			}
			hasSlack := false
			for _, p := range platforms {
				if p.Name() == "slack" {
					hasSlack = true
					break
				}
			}
			if !hasSlack {
				slog.Warn("observe requires a Slack platform; ignoring")
			} else {
				projectDir := resolveClaudeProjectDir(workDir)
				if projectDir == "" {
					slog.Warn("observe: could not find Claude Code project directory", "workDir", workDir)
				} else {
					sessionKey := fmt.Sprintf("slack:%s", obsChan)
					engine.SetObserveConfig(projectDir, sessionKey)
				}
			}
		}

		// Wire global custom commands
		for _, c := range cfg.Commands {
			engine.AddCommand(c.Name, c.Description, c.Prompt, c.Exec, c.WorkDir, "config")
		}

		// Wire command persistence callbacks
		engine.SetCommandSaveAddFunc(func(name, description, prompt, exec, workDir string) error {
			return config.AddCommand(config.CommandConfig{Name: name, Description: description, Prompt: prompt, Exec: exec, WorkDir: workDir})
		})
		engine.SetCommandSaveDelFunc(func(name string) error {
			return config.RemoveCommand(name)
		})

		// Wire global aliases
		for _, a := range cfg.Aliases {
			engine.AddAlias(a.Name, a.Command)
		}
		engine.SetAliasSaveAddFunc(func(name, command string) error {
			return config.AddAlias(config.AliasConfig{Name: name, Command: command})
		})
		engine.SetAliasSaveDelFunc(func(name string) error {
			return config.RemoveAlias(name)
		})

		// Wire banned words
		if len(cfg.BannedWords) > 0 {
			engine.SetBannedWords(cfg.BannedWords)
		}

		// Wire disabled commands (project-level)
		if len(proj.DisabledCommands) > 0 {
			engine.SetDisabledCommands(proj.DisabledCommands)
		}

		// Wire admin allowlist for privileged commands
		engine.SetAdminFrom(proj.AdminFrom)

		// Wire per-user role-based policies
		if proj.Users != nil {
			engine.SetUserRoles(buildUserRoleManager(proj.Users))
		}

		// Wire display truncation settings (includes legacy quiet → display mapping)
		{
			mode, tm, tool, tmlen, toollen, _, _, hideAgentFooter := config.EffectiveDisplay(cfg, &proj)
			historyMaxLen := config.EffectiveHistoryMaxLen(cfg, &proj)
			cleanup, collapse := config.EffectiveProgressDisplay(cfg, &proj)
			engine.SetDisplayConfig(core.DisplayCfg{
				CleanupProgressOnComplete: cleanup,
				CollapseToolMessages:      collapse,
				Mode:                      mode,
				ThinkingMessages:          tm,
				ThinkingMaxLen:            tmlen,
				ToolMaxLen:                toollen,
				ToolMessages:              tool,
				HistoryMaxLen:             &historyMaxLen,
				HideAgentFooter:           hideAgentFooter,
			})
		}

		// Wire shell configuration
		shell, shellFlag, shellProfile := config.EffectiveShell(cfg, &proj)
		engine.SetShell(shell, shellFlag, shellProfile)

		// Wire hooks
		if len(cfg.Hooks) > 0 {
			coreHooks := make([]core.HookConfig, len(cfg.Hooks))
			for i, h := range cfg.Hooks {
				coreHooks[i] = core.HookConfig{
					Event:   h.Event,
					Type:    h.Type,
					Command: h.Command,
					URL:     h.URL,
					Timeout: h.Timeout,
					Async:   h.Async,
				}
			}
			engine.SetHooks(core.NewHookManager(proj.Name, coreHooks, shell, shellFlag, shellProfile))
		}

		// Wire local reference normalization / rendering
		engine.SetReferenceConfig(core.ReferenceRenderCfg{
			NormalizeAgents: proj.References.NormalizeAgents,
			RenderPlatforms: proj.References.RenderPlatforms,
			DisplayPath:     proj.References.DisplayPath,
			MarkerStyle:     proj.References.MarkerStyle,
			EnclosureStyle:  proj.References.EnclosureStyle,
		})

		// Wire streaming preview
		{
			spcfg := core.DefaultStreamPreviewCfg()
			if cfg.StreamPreview.Enabled != nil {
				spcfg.Enabled = *cfg.StreamPreview.Enabled
			}
			if cfg.StreamPreview.IntervalMs != nil {
				spcfg.IntervalMs = *cfg.StreamPreview.IntervalMs
			}
			if cfg.StreamPreview.MinDeltaChars != nil {
				spcfg.MinDeltaChars = *cfg.StreamPreview.MinDeltaChars
			}
			if cfg.StreamPreview.MaxChars != nil {
				spcfg.MaxChars = *cfg.StreamPreview.MaxChars
			}
			if cfg.StreamPreview.DisabledPlatforms != nil {
				spcfg.DisabledPlatforms = cfg.StreamPreview.DisabledPlatforms
			}
			engine.SetStreamPreviewCfg(spcfg)
		}

		// Wire instant reply
		if cfg.InstantReply.Enabled != nil && *cfg.InstantReply.Enabled {
			engine.SetInstantReply(core.InstantReplyCfg{
				Enabled: true,
				Content: cfg.InstantReply.Content,
			})
		}

		// Wire rate limiting
		{
			maxMsg := 20
			windowSecs := 60
			if cfg.RateLimit.MaxMessages != nil {
				maxMsg = *cfg.RateLimit.MaxMessages
			}
			if cfg.RateLimit.WindowSecs != nil {
				windowSecs = *cfg.RateLimit.WindowSecs
			}
			if maxMsg > 0 {
				engine.SetRateLimitCfg(core.RateLimitCfg{
					MaxMessages: maxMsg,
					Window:      time.Duration(windowSecs) * time.Second,
				})
			}
		}
		// Wire outgoing rate limiting
		{
			var maxPS float64
			if cfg.OutgoingRateLimit.MaxPerSecond != nil {
				maxPS = *cfg.OutgoingRateLimit.MaxPerSecond
			}
			var burst int
			if cfg.OutgoingRateLimit.Burst != nil {
				burst = *cfg.OutgoingRateLimit.Burst
			}
			defaults := core.OutgoingRateLimitCfg{MaxPerSecond: maxPS, Burst: burst}
			overrides := make(map[string]core.OutgoingRateLimitCfg)
			for name, pc := range cfg.OutgoingRateLimit.Platforms {
				var mps float64
				if pc.MaxPerSecond != nil {
					mps = *pc.MaxPerSecond
				}
				var b int
				if pc.Burst != nil {
					b = *pc.Burst
				}
				overrides[name] = core.OutgoingRateLimitCfg{MaxPerSecond: mps, Burst: b}
			}
			if maxPS > 0 || len(overrides) > 0 {
				engine.SetOutgoingRateLimitCfg(defaults, overrides)
			}
		}

		engine.SetDisplaySaveFunc(func(mode *string, thinkingMessages *bool, thinkingMaxLen, toolMaxLen *int, toolMessages *bool) error {
			return config.SaveDisplayConfig(mode, thinkingMessages, thinkingMaxLen, toolMaxLen, toolMessages)
		})

		// Wire idle timeout
		if cfg.IdleTimeoutMins != nil {
			mins := *cfg.IdleTimeoutMins
			if mins <= 0 {
				engine.SetEventIdleTimeout(0)
			} else {
				engine.SetEventIdleTimeout(time.Duration(mins) * time.Minute)
			}
		}

		// Wire max turn time (absolute per-turn wall-clock cap; 0 = disabled)
		if cfg.MaxTurnTimeMins != nil && *cfg.MaxTurnTimeMins > 0 {
			engine.SetMaxTurnTime(time.Duration(*cfg.MaxTurnTimeMins) * time.Minute)
		}

		// Wire queue depth
		if cfg.Queue.MaxDepth != nil && *cfg.Queue.MaxDepth > 0 {
			engine.SetMaxQueuedMessages(*cfg.Queue.MaxDepth)
		}

		// Wire auto-compress settings
		if proj.AutoCompress.Enabled != nil && *proj.AutoCompress.Enabled {
			minGap := 30 * time.Minute
			if proj.AutoCompress.MinGapMins != nil {
				minGap = time.Duration(*proj.AutoCompress.MinGapMins) * time.Minute
			}
			maxTokens := derefInt(proj.AutoCompress.MaxTokens)
			if maxTokens <= 0 {
				maxTokens = 12000
			}
			engine.SetAutoCompressConfig(true, maxTokens, minGap)
		}
		resetIdle, defaulted := resolveResetOnIdle(proj.ResetOnIdleMins)
		engine.SetResetOnIdle(resetIdle)
		if defaulted {
			slog.Info("project: reset_on_idle_mins not set, applying default — set reset_on_idle_mins = 0 to opt out, see docs/usage.md",
				"project", proj.Name, "default_minutes", defaultResetOnIdleMins)
		}
		if proj.AgentSessionIdleTimeoutMins != nil {
			mins := *proj.AgentSessionIdleTimeoutMins
			if mins <= 0 {
				engine.SetAgentSessionIdleTimeout(0)
			} else {
				engine.SetAgentSessionIdleTimeout(time.Duration(mins) * time.Minute)
			}
		}

		// Wire sender injection
		if proj.InjectSender != nil {
			engine.SetInjectSender(*proj.InjectSender)
		}

		projName := proj.Name
		engine.SetModelSaveFunc(func(model string) error {
			return config.SaveAgentModel(projName, model)
		})

		// Wire config reload
		capturedEngine := engine
		capturedProjName := projName
		engine.SetConfigReloadFunc(func() (*core.ConfigReloadResult, error) {
			return reloadConfig(configPath, capturedProjName, capturedEngine)
		})

		// Wire /web command callbacks
		engine.SetWebSetupFunc(func() (int, string, bool, error) {
			mgmtToken := core.GenerateToken(16)
			bridgeToken := core.GenerateToken(16)
			result, err := config.EnableWebAdmin(mgmtToken, bridgeToken)
			if err != nil {
				return 0, "", false, err
			}
			return result.ManagementPort, result.ManagementToken, !result.AlreadyEnabled, nil
		})
		engine.SetWebStatusFunc(func() string {
			if cfg.Management.Enabled == nil || !*cfg.Management.Enabled {
				return ""
			}
			port := cfg.Management.Port
			if port == 0 {
				port = 9820
			}
			return fmt.Sprintf("http://localhost:%d", port)
		})

		engines = append(engines, engine)
		effectiveWorkDirs = append(effectiveWorkDirs, effectiveWorkDir)
	}

	var startErrors []error
	for _, e := range engines {
		if err := e.Start(); err != nil {
			slog.Warn("engine start partially failed (some platforms may be unavailable)", "error", err)
			startErrors = append(startErrors, err)
		}
	}
	// Only exit if ALL engines failed to start
	if len(startErrors) > 0 && len(startErrors) == len(engines) {
		slog.Error("all engines failed to start, exiting")
		os.Exit(1)
	}

	// Web Admin chat uses a WebSocket transport shared by project engines.
	var bridgeSrv *core.BridgeServer
	if cfg.Bridge.Enabled != nil && *cfg.Bridge.Enabled {
		port := cfg.Bridge.Port
		if port <= 0 {
			port = 9810
		}
		path := cfg.Bridge.Path
		if path == "" {
			path = "/bridge/ws"
		}
		insecure := cfg.Bridge.Insecure != nil && *cfg.Bridge.Insecure
		if insecure {
			bridgeSrv = core.NewBridgeServerInsecure(port, cfg.Bridge.Token, path, cfg.Bridge.CORSOrigins)
		} else {
			bridgeSrv = core.NewBridgeServer(port, cfg.Bridge.Token, path, cfg.Bridge.CORSOrigins)
		}
		if bridgeSrv == nil {
			slog.Error("web chat: failed to create transport - token is required")
			os.Exit(1)
		}
		for i, e := range engines {
			bp := bridgeSrv.NewPlatform(cfg.Projects[i].Name)
			bridgeSrv.RegisterEngine(cfg.Projects[i].Name, e, bp)
			e.AddPlatform(bp)
		}
		bridgeSrv.Start()
	}

	// Start webhook server if enabled
	var webhookSrv *core.WebhookServer
	if cfg.Webhook.Enabled != nil && *cfg.Webhook.Enabled {
		port := cfg.Webhook.Port
		if port <= 0 {
			port = 9111
		}
		path := cfg.Webhook.Path
		if path == "" {
			path = "/hook"
		}
		webhookSrv = core.NewWebhookServer(port, cfg.Webhook.Token, path)
		for i, e := range engines {
			webhookSrv.RegisterEngine(cfg.Projects[i].Name, e)
		}
		webhookSrv.Start()
	}

	// Start management API server if enabled
	var mgmtSrv *core.ManagementServer
	if cfg.Management.Enabled != nil && *cfg.Management.Enabled {
		port := cfg.Management.Port
		if port <= 0 {
			port = 9820
		}
		mgmtSrv = core.NewManagementServer(port, cfg.Management.Token, cfg.Management.CORSOrigins)
		for i, e := range engines {
			mgmtSrv.RegisterEngine(cfg.Projects[i].Name, e)
		}
		if bridgeSrv != nil {
			mgmtSrv.SetBridgeServer(bridgeSrv)
		}
		mgmtSrv.SetSetupWeixinSave(func(req core.WeixinSetupSaveRequest) error {
			_, err := config.EnsureProjectWithWeixinPlatform(config.EnsureProjectWithWeixinOptions{
				ProjectName: req.ProjectName,
				WorkDir:     req.WorkDir,
				AgentType:   req.AgentType,
			})
			if err != nil {
				return fmt.Errorf("ensure project: %w", err)
			}
			_, err = config.SaveWeixinPlatformCredentials(config.WeixinCredentialUpdateOptions{
				ProjectName:       req.ProjectName,
				Token:             req.Token,
				BaseURL:           req.BaseURL,
				AccountID:         req.IlinkBotID,
				ScannedUserID:     req.IlinkUserID,
				SetAllowFromEmpty: true,
			})
			return err
		})
		mgmtSrv.SetAddPlatformToProject(func(projectName, platType string, opts map[string]any, workDir, agentType string) error {
			if opts == nil {
				opts = map[string]any{}
			}
			return config.AddPlatformToProject(projectName, config.PlatformConfig{Type: platType, Options: opts}, workDir, agentType)
		})
		mgmtSrv.SetRemoveProject(config.RemoveProject)
		mgmtSrv.SetSaveProjectSettings(func(name string, u core.ProjectSettingsUpdate) error {
			return config.SaveProjectSettings(name, config.ProjectSettingsUpdate{
				CleanupProgressOnComplete: u.CleanupProgressOnComplete,
				CollapseToolMessages:      u.CollapseToolMessages,

				AdminFrom:            u.AdminFrom,
				DisabledCommands:     u.DisabledCommands,
				WorkDir:              u.WorkDir,
				Mode:                 u.Mode,
				AgentType:            u.AgentType,
				ShowContextIndicator: u.ShowContextIndicator,
				ShowWorkdirIndicator: u.ShowWorkdirIndicator,
				ReplyFooter:          u.ReplyFooter,
				InjectSender:         u.InjectSender,
				PlatformAllowFrom:    u.PlatformAllowFrom,
			})
		})
		mgmtSrv.SetGetProjectConfig(config.GetProjectConfigDetails)
		mgmtSrv.SetConfigFilePath(configPath)
		mgmtSrv.SetGetGlobalSettings(config.GetGlobalSettings)
		mgmtSrv.SetSaveGlobalSettings(func(updates map[string]any) error {
			u := config.GlobalSettingsUpdate{}
			if v, ok := updates["cleanup_progress_on_complete"].(bool); ok {
				u.CleanupProgressOnComplete = &v
			}
			if v, ok := updates["collapse_tool_messages"].(bool); ok {
				u.CollapseToolMessages = &v
			}

			if v, ok := updates["attachment_send"].(string); ok {
				u.AttachmentSend = &v
			}
			if v, ok := updates["log_level"].(string); ok {
				u.LogLevel = &v
			}
			if v, ok := updates["idle_timeout_mins"].(float64); ok {
				iv := int(v)
				u.IdleTimeoutMins = &iv
			}
			if v, ok := updates["thinking_messages"].(bool); ok {
				u.ThinkingMessages = &v
			}
			if v, ok := updates["thinking_max_len"].(float64); ok {
				iv := int(v)
				u.ThinkingMaxLen = &iv
			}
			if v, ok := updates["tool_messages"].(bool); ok {
				u.ToolMessages = &v
			}
			if v, ok := updates["tool_max_len"].(float64); ok {
				iv := int(v)
				u.ToolMaxLen = &iv
			}
			if v, ok := updates["stream_preview_enabled"].(bool); ok {
				u.StreamPreviewOn = &v
			}
			if v, ok := updates["stream_preview_interval_ms"].(float64); ok {
				iv := int(v)
				u.StreamPreviewIntMs = &iv
			}
			if v, ok := updates["rate_limit_max_messages"].(float64); ok {
				iv := int(v)
				u.RateLimitMax = &iv
			}
			if v, ok := updates["rate_limit_window_secs"].(float64); ok {
				iv := int(v)
				u.RateLimitWindow = &iv
			}
			return config.SaveGlobalSettings(u)
		})
		mgmtSrv.Start()
	}

	// Start internal API server for CLI send
	apiSrv, err := core.NewAPIServer(cfg.DataDir)
	if err != nil {
		slog.Warn("api server unavailable", "error", err)
	} else {
		globalAPIServer = apiSrv
		apiSrv.SetMaxAttachmentSize(resolveMaxAttachmentSize(cfg))

		// Create shared DirHistory for all engines
		dirHistory := core.NewDirHistory(cfg.DataDir)

		for i, e := range engines {
			apiSrv.RegisterEngine(cfg.Projects[i].Name, e)
			e.SetDirHistory(dirHistory)

			// Ensure initial work_dir is in history
			if initWorkDir := effectiveWorkDirs[i]; initWorkDir != "" {
				if !dirHistory.Contains(cfg.Projects[i].Name, initWorkDir) {
					dirHistory.Add(cfg.Projects[i].Name, initWorkDir)
				}
			}
		}
		apiSrv.Start()
	}

	slog.Info("agent-bridge is running", "projects", len(engines))

	// After startup, check if we were restarted and queue the success
	// notification. The engine dispatches it on the first OnPlatformReady
	// for the target platform (or with a 10s safety timeout), so async
	// platforms that need 2-3s to actually connect (e.g. Telegram) do not
	// silently drop the notify. See issue #1383.
	if notify := core.ConsumeRestartNotify(cfg.DataDir); notify != nil {
		slog.Info("post-restart: queuing success notification", "platform", notify.Platform, "session", notify.SessionKey)
		for _, e := range engines {
			e.SetPendingRestartNotify(notify)
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var restartReq *core.RestartRequest
	select {
	case <-sigCh:
	case req := <-core.RestartCh:
		restartReq = &req
		slog.Info("restart requested via /restart command", "session", req.SessionKey, "platform", req.Platform)
	}

	slog.Info("shutting down...")
	if mgmtSrv != nil {
		mgmtSrv.Stop()
	}
	if bridgeSrv != nil {
		bridgeSrv.Stop()
	}
	if webhookSrv != nil {
		webhookSrv.Stop()
	}
	if apiSrv != nil {
		apiSrv.Stop()
	}
	for _, e := range engines {
		if err := e.Stop(); err != nil {
			slog.Error("shutdown error", "error", err)
		}
	}
	if logCloser != nil {
		logCloser.Close()
	}
	instanceLock.Release()

	if restartReq != nil {
		if err := core.SaveRestartNotify(cfg.DataDir, *restartReq); err != nil {
			slog.Error("restart: save notify failed", "error", err)
		}
		execPath, err := os.Executable()
		if err != nil {
			slog.Error("restart: cannot determine executable path", "error", err)
			os.Exit(1)
		}
		slog.Info("restarting...", "path", execPath, "args", os.Args)
		if err := restartProcess(execPath); err != nil {
			slog.Error("restart: failed", "error", err)
			os.Exit(1)
		}
	}

	slog.Info("bye")
}

func runTopLevelCommand(args []string) bool {
	if len(args) == 0 {
		return false
	}
	handler, ok := topLevelCommandHandlers[args[0]]
	if !ok {
		return false
	}
	handler(args[1:])
	return true
}

type rootCLIOptions struct {
	configPath     string
	force          bool
	observe        bool
	observeChannel string
	logMaxSize     string
	logMaxBackups  int
	showVersion    bool
	args           []string
}

func parseRootCLIOptions(args []string) (rootCLIOptions, error) {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = printUsage

	configPath := fs.String("config", "", "path to config file (default: ./config.toml or ~/.agent-bridge/config.toml)")
	force := fs.Bool("force", false, "kill any existing instance with the same config before starting")
	observe := fs.Bool("observe", false, "observe native terminal Claude Code sessions and forward to Slack")
	observeChannel := fs.String("observe-channel", "", "Slack channel ID to forward terminal observations to (requires --observe)")
	logMaxSize := fs.String("log-max-size", "", "max bytes for the rotating log file (e.g. 10MB, 512K, 10485760); overrides AGENT_BRIDGE_LOG_MAX_SIZE env var (default: 10MB)")
	logMaxBackups := fs.Int("log-max-backups", 0, "number of rotated log files to retain (.log.1 .. .log.N); overrides AGENT_BRIDGE_LOG_MAX_BACKUPS env var (default: 3)")
	showVersion := fs.Bool("version", false, "print version and exit")

	if err := fs.Parse(args); err != nil {
		return rootCLIOptions{}, err
	}

	return rootCLIOptions{
		configPath:     *configPath,
		force:          *force,
		observe:        *observe,
		observeChannel: *observeChannel,
		logMaxSize:     *logMaxSize,
		logMaxBackups:  *logMaxBackups,
		showVersion:    *showVersion,
		args:           fs.Args(),
	}, nil
}

func validateNoExtraTopLevelArgs(args []string) error {
	if len(args) == 0 {
		return nil
	}
	return fmt.Errorf("unknown top-level command: %s", args[0])
}

// sessionStorePath builds a unique filename from project name + work_dir.
// It checks for legacy session files (without the sessions/ subdirectory) in dataDir
// for backward compatibility; if found, uses that path. Otherwise uses dataDir/sessions/.
func sessionStorePath(dataDir, name, workDir string) string {
	var filename string
	if workDir == "" {
		filename = name + ".json"
	} else {
		abs, err := filepath.Abs(workDir)
		if err != nil {
			abs = workDir
		}
		h := sha256.Sum256([]byte(abs))
		short := hex.EncodeToString(h[:4])
		filename = fmt.Sprintf("%s_%s.json", name, short)
	}

	// Check legacy path in dataDir (without sessions/ subdirectory) for backward compatibility.
	// Also check for the older .sessions.json naming convention.
	for _, legacy := range []string{
		filepath.Join(dataDir, filename),
		filepath.Join(dataDir, strings.TrimSuffix(filename, ".json")+".sessions.json"),
	} {
		if _, err := os.Stat(legacy); err == nil {
			slog.Info("session: using legacy file in dataDir", "path", legacy)
			return legacy
		}
	}

	return filepath.Join(dataDir, "sessions", filename)
}

func projectStatePath(dataDir, projectName string) string {
	replacer := strings.NewReplacer(
		"\\", "_",
		"/", "_",
		":", "_",
		"*", "_",
		"?", "_",
		"\"", "_",
		"<", "_",
		">", "_",
		"|", "_",
	)
	name := strings.TrimSpace(projectName)
	name = replacer.Replace(name)
	if name == "" {
		name = "project"
	}
	return filepath.Join(dataDir, "projects", name+".state.json")
}

func applyProjectStateOverride(projectName string, agent core.Agent, configuredWorkDir string, store *core.ProjectStateStore) string {
	effectiveWorkDir := configuredWorkDir
	if store == nil {
		return effectiveWorkDir
	}

	switcher, ok := agent.(core.WorkDirSwitcher)
	if !ok {
		return effectiveWorkDir
	}

	override := store.WorkDirOverride()
	if override == "" {
		return effectiveWorkDir
	}
	if abs, err := filepath.Abs(override); err == nil {
		override = abs
	}

	info, err := os.Stat(override)
	if err != nil || !info.IsDir() {
		slog.Warn("project_state: ignoring invalid work_dir override", "project", projectName, "work_dir", override)
		return effectiveWorkDir
	}

	switcher.SetWorkDir(override)
	slog.Info("project_state: applied work_dir override", "project", projectName, "work_dir", override)
	return override
}

// resolveClaudeProjectDir returns the Claude Code project directory for a given
// work directory, or "" if it doesn't exist.
func resolveClaudeProjectDir(workDir string) string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	// Claude Code encodes paths by replacing os.PathSeparator with "-"
	// e.g. /home/leigh/workspace/agent-bridge -> -home-leigh-workspace-agent-bridge
	encoded := strings.ReplaceAll(workDir, string(os.PathSeparator), "-")
	dir := filepath.Join(homeDir, ".claude", "projects", encoded)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return ""
	}
	return dir
}

// resolveConfigPath determines which config file to use.
// Priority: explicit flag → ./config.toml → ~/.agent-bridge/config.toml
func resolveConfigPath(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if _, err := os.Stat("config.toml"); err == nil {
		return "config.toml"
	}
	if home, err := os.UserHomeDir(); err == nil {
		return filepath.Join(home, ".agent-bridge", "config.toml")
	}
	return "config.toml"
}

func bootstrapConfig(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	const tmpl = `# agent-bridge configuration
# See local README.md

[log]
level = "info"

[[projects]]
name = "my-project"

[projects.agent]
type = "claudecode"   # "claudecode", "codex"

[projects.agent.options]
work_dir = "/path/to/your/project"
mode = "default"
# model = "claude-sonnet-4-20250514"

# --- Choose at least one platform below ---

# Telegram
[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "your-telegram-bot-token"

# For Weixin and QQ platform examples
# see: config.example.toml
`
	return os.WriteFile(path, []byte(tmpl), 0o644)
}

func printUsage() {
	v := version
	if v == "" || v == "dev" {
		v = "dev"
	}

	fmt.Fprintf(os.Stderr, `agent-bridge %s

  Bridge your messaging platforms to local AI coding agents.
  Supports: Claude Code and Codex
  Platforms: Weixin, Telegram, QQ (OneBot)

Usage:
  agent-bridge [flags]
  agent-bridge <command> [args]

Flags:
  --config <path>    Path to config file (default: ./config.toml or ~/.agent-bridge/config.toml)
  --force            Kill any existing instance with the same config before starting
  --version          Print version and exit
  --help             Show this help message

Commands:
  daemon             Manage agent-bridge as a background service (systemd/launchd)
    install          Install and start the daemon service
    uninstall        Remove the daemon service
    start            Start the daemon
    stop             Stop the daemon
    restart          Restart the daemon
    status           Show daemon status
    logs             View daemon logs (-f to follow, -n N for last N lines)

  send               Send attachments to an active session
                     (--image|--file|--audio|--video <path>,
                      -m <caption> with image/file only, -p <project>, -s <session>)


  sessions           Browse session history
    list             List all sessions (pipe-friendly)
    show <id>        Show session messages (-n N for last N)

  agent-sid          Print the agent session ID for the current session

  weixin             Setup Weixin personal (ilink) via QR or token
    setup            QR login, or bind when --token is provided
    new              Force QR login
    bind             Bind existing ilink bot token

  config             Manage configuration
    example          Print a complete annotated config.toml example
    format           Format the config file (alias: fmt)
    path             Print the resolved config file path

  config-example     (deprecated: use 'config example' instead)

Examples:
  agent-bridge                          Start with default config
  agent-bridge --config /path/to.toml   Start with a specific config file
  agent-bridge daemon install           Install as a system service
  agent-bridge daemon logs -f           Follow daemon logs
  agent-bridge send --file report.pdf   Send a file to the active session
  agent-bridge weixin setup             Setup Weixin (ilink) with QR or --token
  agent-bridge config format            Format the config file
  agent-bridge config example > c.toml  Save example config to a file

`, v)
}

func setupLogger(level string, w io.Writer) {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	if w == nil {
		w = os.Stdout
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: logLevel,
	})))
}

// reloadConfig re-reads config.toml and applies hot-reloadable settings
// (display, commands) to the given engine.
func reloadConfig(configPath, projName string, engine *core.Engine) (*core.ConfigReloadResult, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("reload config: %w", err)
	}

	result := &core.ConfigReloadResult{}

	// Re-apply process-global hot-reloadable settings.
	if globalAPIServer != nil {
		globalAPIServer.SetMaxAttachmentSize(resolveMaxAttachmentSize(cfg))
	}

	// Find the matching project
	var proj *config.ProjectConfig
	for i := range cfg.Projects {
		if cfg.Projects[i].Name == projName {
			proj = &cfg.Projects[i]
			break
		}
	}
	if proj == nil {
		return nil, fmt.Errorf("project %q not found in config", projName)
	}

	// Reload display config (includes legacy quiet → display mapping)
	mode, tm, tool, tmlen, toollen, showCtx, showFooter, hideAgentFooter := config.EffectiveDisplay(cfg, proj)
	historyMaxLen := config.EffectiveHistoryMaxLen(cfg, proj)
	cleanup, collapse := config.EffectiveProgressDisplay(cfg, proj)
	engine.SetDisplayConfig(core.DisplayCfg{
		CleanupProgressOnComplete: cleanup,
		CollapseToolMessages:      collapse,
		Mode:                      mode,
		ThinkingMessages:          tm,
		ThinkingMaxLen:            tmlen,
		ToolMaxLen:                toollen,
		ToolMessages:              tool,
		HistoryMaxLen:             &historyMaxLen,
		HideAgentFooter:           hideAgentFooter,
	})
	result.DisplayUpdated = true

	// Wire show_context_indicator and reply_footer from display config
	engine.SetShowContextIndicator(showCtx)
	showWorkdir := true
	if proj.ShowWorkdirIndicator != nil {
		showWorkdir = *proj.ShowWorkdirIndicator
	}
	engine.SetShowWorkdirIndicator(showWorkdir)
	engine.SetReplyFooterEnabled(showFooter)

	// Reload auto-compress settings
	if proj.AutoCompress.Enabled != nil && *proj.AutoCompress.Enabled {
		minGap := 30 * time.Minute
		if proj.AutoCompress.MinGapMins != nil {
			minGap = time.Duration(*proj.AutoCompress.MinGapMins) * time.Minute
		}
		maxTokens := derefInt(proj.AutoCompress.MaxTokens)
		if maxTokens <= 0 {
			maxTokens = 12000
		}
		engine.SetAutoCompressConfig(true, maxTokens, minGap)
	} else {
		engine.SetAutoCompressConfig(false, 0, 0)
	}
	resetIdle, defaulted := resolveResetOnIdle(proj.ResetOnIdleMins)
	engine.SetResetOnIdle(resetIdle)
	if defaulted {
		slog.Info("project: reset_on_idle_mins not set, applying default — set reset_on_idle_mins = 0 to opt out, see docs/usage.md",
			"project", proj.Name, "default_minutes", defaultResetOnIdleMins)
	}
	if proj.AgentSessionIdleTimeoutMins != nil {
		mins := *proj.AgentSessionIdleTimeoutMins
		if mins <= 0 {
			engine.SetAgentSessionIdleTimeout(0)
		} else {
			engine.SetAgentSessionIdleTimeout(time.Duration(mins) * time.Minute)
		}
	} else {
		// A reload may remove this option after timers were scheduled; reset
		// explicitly so those stale idle-close timers cannot fire later.
		engine.SetAgentSessionIdleTimeout(0)
	}

	// Reload instant reply
	if cfg.InstantReply.Enabled != nil && *cfg.InstantReply.Enabled {
		engine.SetInstantReply(core.InstantReplyCfg{
			Enabled: true,
			Content: cfg.InstantReply.Content,
		})
	} else {
		engine.SetInstantReply(core.InstantReplyCfg{})
	}

	// Reload sender injection
	engine.SetInjectSender(proj.InjectSender != nil && *proj.InjectSender)

	// Reload attachment send-back switch
	engine.SetAttachmentSendEnabled(cfg.AttachmentSend != "off")

	// Reload filter_external_sessions
	engine.SetFilterExternalSessions(proj.FilterExternalSessions != nil && *proj.FilterExternalSessions)

	// Reload custom commands
	engine.ClearCommands("config")
	for _, c := range cfg.Commands {
		engine.AddCommand(c.Name, c.Description, c.Prompt, c.Exec, c.WorkDir, "config")
	}
	result.CommandsUpdated = len(cfg.Commands)

	// Reload aliases
	engine.ClearAliases()
	for _, a := range cfg.Aliases {
		engine.AddAlias(a.Name, a.Command)
	}

	// Reload banned words
	engine.SetBannedWords(cfg.BannedWords)

	// Reload disabled commands
	engine.SetDisabledCommands(proj.DisabledCommands)

	// Reload admin allowlist
	engine.SetAdminFrom(proj.AdminFrom)

	// Reload per-user role-based policies
	if proj.Users != nil {
		engine.SetUserRoles(buildUserRoleManager(proj.Users))
	} else {
		engine.SetUserRoles(nil)
	}

	slog.Info("config reloaded", "project", projName)
	return result, nil
}

func buildUserRoleManager(uc *config.UsersConfig) *core.UserRoleManager {
	var roles []core.RoleInput
	for name, rc := range uc.Roles {
		var rlCfg *core.RateLimitCfg
		if rc.RateLimit != nil {
			maxMsg, windowSecs := 20, 60
			if rc.RateLimit.MaxMessages != nil {
				maxMsg = *rc.RateLimit.MaxMessages
			}
			if rc.RateLimit.WindowSecs != nil {
				windowSecs = *rc.RateLimit.WindowSecs
			}
			rlCfg = &core.RateLimitCfg{
				MaxMessages: maxMsg,
				Window:      time.Duration(windowSecs) * time.Second,
			}
		}
		roles = append(roles, core.RoleInput{
			Name:             name,
			UserIDs:          rc.UserIDs,
			DisabledCommands: rc.DisabledCommands,
			RateLimit:        rlCfg,
		})
	}
	defaultRole := "member"
	if uc.DefaultRole != "" {
		defaultRole = uc.DefaultRole
	}
	urm := core.NewUserRoleManager()
	urm.Configure(defaultRole, roles)
	return urm
}

func buildAgentOptions(dataDir string, proj config.ProjectConfig) map[string]any {
	opts := make(map[string]any, len(proj.Agent.Options)+2)
	for k, v := range proj.Agent.Options {
		opts[k] = v
	}
	opts["cc_data_dir"] = dataDir
	opts["cc_project"] = proj.Name
	return opts
}

func startInitialRefresh(agent core.Agent) {
	if starter, ok := agent.(initialModelRefreshStarter); ok {
		starter.StartInitialModelRefresh()
	}
}

func derefInt(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
