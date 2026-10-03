package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
)

func TestEffectiveDisplayQuiet(t *testing.T) {
	tru, fal := true, false
	compact := DisplayModeCompact
	quiet := DisplayModeQuiet
	tests := []struct {
		name     string
		cfg      Config
		proj     ProjectConfig
		wantMode string
		wantTM   bool
		wantTool bool
	}{
		{
			name:     "defaults no quiet",
			cfg:      Config{},
			proj:     ProjectConfig{},
			wantMode: "full",
			wantTM:   true,
			wantTool: true,
		},
		{
			name:     "global quiet maps to quiet mode",
			cfg:      Config{Quiet: &tru},
			proj:     ProjectConfig{},
			wantMode: "quiet",
			wantTM:   false,
			wantTool: false,
		},
		{
			name:     "project quiet maps to quiet mode",
			cfg:      Config{},
			proj:     ProjectConfig{Quiet: &tru},
			wantMode: "quiet",
			wantTM:   false,
			wantTool: false,
		},
		{
			name: "explicit thinking_messages wins over quiet",
			cfg: Config{
				Quiet:   &tru,
				Display: DisplayConfig{ThinkingMessages: &tru},
			},
			proj:     ProjectConfig{},
			wantMode: "quiet",
			wantTM:   true,
			wantTool: false,
		},
		{
			name:     "project quiet false overrides global quiet",
			cfg:      Config{Quiet: &tru},
			proj:     ProjectConfig{Quiet: &fal},
			wantMode: "full",
			wantTM:   true,
			wantTool: true,
		},
		{
			name:     "explicit mode compact",
			cfg:      Config{Display: DisplayConfig{Mode: &compact}},
			proj:     ProjectConfig{},
			wantMode: "compact",
			wantTM:   false,
			wantTool: false,
		},
		{
			name:     "project mode overrides global mode",
			cfg:      Config{Display: DisplayConfig{Mode: &quiet}},
			proj:     ProjectConfig{Display: &DisplayConfig{Mode: &compact}},
			wantMode: "compact",
			wantTM:   false,
			wantTool: false,
		},
		{
			name:     "explicit mode wins over legacy quiet",
			cfg:      Config{Quiet: &tru, Display: DisplayConfig{Mode: &compact}},
			proj:     ProjectConfig{},
			wantMode: "compact",
			wantTM:   false,
			wantTool: false,
		},
		{
			name: "explicit mode quiet with thinking override",
			cfg: Config{
				Display: DisplayConfig{Mode: &quiet, ThinkingMessages: &tru},
			},
			proj:     ProjectConfig{},
			wantMode: "quiet",
			wantTM:   true,
			wantTool: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, tm, tool, _, _, _, _, _ := EffectiveDisplay(&tt.cfg, &tt.proj)
			if mode != tt.wantMode {
				t.Fatalf("Mode = %q, want %q", mode, tt.wantMode)
			}
			if tm != tt.wantTM {
				t.Fatalf("ThinkingMessages = %v, want %v", tm, tt.wantTM)
			}
			if tool != tt.wantTool {
				t.Fatalf("ToolMessages = %v, want %v", tool, tt.wantTool)
			}
		})
	}
}

func TestEffectiveDisplay_ProjectOverride(t *testing.T) {
	tru, fal := true, false
	maxA, maxB := 100, 200

	tests := []struct {
		name           string
		cfg            Config
		proj           ProjectConfig
		wantTM         bool
		wantTool       bool
		wantThinkLen   int
		wantToolMaxLen int
	}{
		{
			name: "project overrides global thinking_messages",
			cfg: Config{
				Display: DisplayConfig{ThinkingMessages: &tru, ToolMessages: &tru},
			},
			proj: ProjectConfig{
				Display: &DisplayConfig{ThinkingMessages: &fal},
			},
			wantTM:         false,
			wantTool:       true,
			wantThinkLen:   300,
			wantToolMaxLen: 500,
		},
		{
			name: "project unset falls back to global",
			cfg: Config{
				Display: DisplayConfig{ThinkingMessages: &fal, ToolMessages: &fal},
			},
			proj: ProjectConfig{
				Display: &DisplayConfig{},
			},
			wantTM:         false,
			wantTool:       false,
			wantThinkLen:   300,
			wantToolMaxLen: 500,
		},
		{
			name: "both unset falls back to default",
			cfg:  Config{},
			proj: ProjectConfig{
				Display: &DisplayConfig{},
			},
			wantTM:         true,
			wantTool:       true,
			wantThinkLen:   300,
			wantToolMaxLen: 500,
		},
		{
			name: "project overrides max-len fields",
			cfg: Config{
				Display: DisplayConfig{ThinkingMaxLen: &maxA, ToolMaxLen: &maxA},
			},
			proj: ProjectConfig{
				Display: &DisplayConfig{ThinkingMaxLen: &maxB, ToolMaxLen: &maxB},
			},
			wantTM:         true,
			wantTool:       true,
			wantThinkLen:   200,
			wantToolMaxLen: 200,
		},
		{
			name: "project quiet still respected when project display unset",
			cfg:  Config{Quiet: &tru},
			proj: ProjectConfig{
				Display: &DisplayConfig{},
			},
			wantTM:         false,
			wantTool:       false,
			wantThinkLen:   300,
			wantToolMaxLen: 500,
		},
		{
			name: "project display.thinking_messages true overrides project quiet",
			cfg:  Config{Quiet: &tru},
			proj: ProjectConfig{
				Display: &DisplayConfig{ThinkingMessages: &tru},
			},
			wantTM:         true,
			wantTool:       false,
			wantThinkLen:   300,
			wantToolMaxLen: 500,
		},
		{
			name: "nil project display behaves like before",
			cfg: Config{
				Display: DisplayConfig{ThinkingMessages: &fal},
			},
			proj:           ProjectConfig{},
			wantTM:         false,
			wantTool:       true,
			wantThinkLen:   300,
			wantToolMaxLen: 500,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, tm, tool, thinkLen, toolMaxLen, _, _, _ := EffectiveDisplay(&tt.cfg, &tt.proj)
			if tm != tt.wantTM {
				t.Errorf("ThinkingMessages = %v, want %v", tm, tt.wantTM)
			}
			if tool != tt.wantTool {
				t.Errorf("ToolMessages = %v, want %v", tool, tt.wantTool)
			}
			if thinkLen != tt.wantThinkLen {
				t.Errorf("ThinkingMaxLen = %d, want %d", thinkLen, tt.wantThinkLen)
			}
			if toolMaxLen != tt.wantToolMaxLen {
				t.Errorf("ToolMaxLen = %d, want %d", toolMaxLen, tt.wantToolMaxLen)
			}
		})
	}
}

func TestEffectiveHistoryMaxLen(t *testing.T) {
	globalLen, projectLen, unlimited := 800, 1200, 0

	tests := []struct {
		name string
		cfg  Config
		proj ProjectConfig
		want int
	}{
		{
			name: "default",
			cfg:  Config{},
			proj: ProjectConfig{},
			want: 1000,
		},
		{
			name: "global display",
			cfg: Config{
				Display: DisplayConfig{HistoryMaxLen: &globalLen},
			},
			proj: ProjectConfig{},
			want: 800,
		},
		{
			name: "project display overrides global",
			cfg: Config{
				Display: DisplayConfig{HistoryMaxLen: &globalLen},
			},
			proj: ProjectConfig{
				Display: &DisplayConfig{HistoryMaxLen: &projectLen},
			},
			want: 1200,
		},
		{
			name: "zero disables truncation",
			cfg: Config{
				Display: DisplayConfig{HistoryMaxLen: &unlimited},
			},
			proj: ProjectConfig{},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EffectiveHistoryMaxLen(&tt.cfg, &tt.proj); got != tt.want {
				t.Fatalf("EffectiveHistoryMaxLen() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEffectiveDisplayHideAgentFooter(t *testing.T) {
	tru := true
	fal := false

	tests := []struct {
		name string
		cfg  Config
		proj ProjectConfig
		want bool
	}{
		{
			name: "default false",
			cfg:  Config{},
			proj: ProjectConfig{},
			want: false,
		},
		{
			name: "global true",
			cfg:  Config{Display: DisplayConfig{HideAgentFooter: &tru}},
			proj: ProjectConfig{},
			want: true,
		},
		{
			name: "project overrides global",
			cfg:  Config{Display: DisplayConfig{HideAgentFooter: &tru}},
			proj: ProjectConfig{Display: &DisplayConfig{HideAgentFooter: &fal}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, _, _, _, got := EffectiveDisplay(&tt.cfg, &tt.proj)
			if got != tt.want {
				t.Fatalf("hideAgentFooter = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateProjectDisplayConfig(t *testing.T) {
	mode := "verbose"
	negativeHistoryMaxLen := -1

	tests := []struct {
		name    string
		display *DisplayConfig
		wantErr string
	}{
		{
			name:    "invalid project display mode",
			display: &DisplayConfig{Mode: &mode},
			wantErr: `projects[0].display.mode must be "full", "compact", or "quiet"`,
		},
		{
			name:    "invalid project history max len",
			display: &DisplayConfig{HistoryMaxLen: &negativeHistoryMaxLen},
			wantErr: `projects[0].display.history_max_len must be >= 0`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{Projects: []ProjectConfig{validProject("demo")}}
			cfg.Projects[0].Display = tt.display
			err := cfg.validate()
			if err == nil {
				t.Fatalf("validate() = nil, want %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate() = %q, want contains %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoad_DefaultsDataDir(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	cfgPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(cfgPath, []byte(baseConfigTOML), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	want := filepath.Join(dir, ".agent-bridge")
	if cfg.DataDir != want {
		t.Fatalf("Load() data_dir = %q, want %q", cfg.DataDir, want)
	}
}

func TestLoad_ResolvesEnvPlaceholders(t *testing.T) {

	root := t.TempDir()
	t.Setenv("AGENT_BRIDGE_ROOT", root)
	t.Setenv("TG_TOKEN", "tg-secret")
	t.Setenv("HOOK_TOKEN", "hook-secret")
	t.Setenv("OPENAI_API_KEY", "sk-test")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:7890")

	configPath := writeConfigFixture(t, `
 data_dir = "${AGENT_BRIDGE_ROOT}/state"

 [webhook]
 token = "${HOOK_TOKEN}"

 [[projects]]
 name = "demo"

 [projects.agent]
 type = "codex"

 [projects.agent.options]
 work_dir = "${AGENT_BRIDGE_ROOT}/repo"
 note = "prefix-${HOOK_TOKEN}-suffix"
 retries = 3


 [[projects.platforms]]
 type = "telegram"

 [projects.platforms.options]
 token = "${TG_TOKEN}"
 chat_id = 12345
 `)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if got, want := cfg.DataDir, filepath.Join(root, "state"); got != want {
		t.Fatalf("DataDir = %q, want %q", got, want)
	}
	if got := cfg.Webhook.Token; got != "hook-secret" {
		t.Fatalf("Webhook.Token = %q, want hook-secret", got)
	}
	if got := stringMapValue(cfg.Projects[0].Agent.Options, "work_dir"); got != filepath.Join(root, "repo") {
		t.Fatalf("work_dir = %q, want %q", got, filepath.Join(root, "repo"))
	}
	if got := stringMapValue(cfg.Projects[0].Agent.Options, "note"); got != "prefix-hook-secret-suffix" {
		t.Fatalf("note = %q, want prefix-hook-secret-suffix", got)
	}
	if got := stringMapValue(cfg.Projects[0].Platforms[0].Options, "token"); got != "tg-secret" {
		t.Fatalf("platform token = %q, want tg-secret", got)
	}
	if _, ok := cfg.Projects[0].Platforms[0].Options["chat_id"].(int64); !ok {
		t.Fatalf("chat_id type = %T, want int64", cfg.Projects[0].Platforms[0].Options["chat_id"])
	}
}

func TestLoad_MissingEnvPlaceholderBecomesEmptyString(t *testing.T) {

	configPath := writeConfigFixture(t, `
 [[projects]]
 name = "demo"

 [projects.agent]
 type = "codex"

 [projects.agent.options]
 work_dir = "/tmp/demo"
 retries = 5


 [[projects.platforms]]
 type = "telegram"

 [projects.platforms.options]
 token = "prefix-${MISSING_TOKEN}-suffix"
 `)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if got := stringMapValue(cfg.Projects[0].Platforms[0].Options, "token"); got != "prefix--suffix" {
		t.Fatalf("platform token = %q, want prefix--suffix", got)
	}
	if _, ok := cfg.Projects[0].Agent.Options["retries"].(int64); !ok {
		t.Fatalf("retries type = %T, want int64", cfg.Projects[0].Agent.Options["retries"])
	}
}

func TestListProjects(t *testing.T) {
	writeTestConfig(t, baseConfigTOML)

	names, err := ListProjects()
	if err != nil {
		t.Fatalf("ListProjects() error: %v", err)
	}
	if len(names) != 1 || names[0] != "demo" {
		t.Fatalf("ListProjects() = %#v, want [demo]", names)
	}
}

func TestSaveAgentModel(t *testing.T) {
	writeTestConfig(t, agentConfigTOML)

	if err := SaveAgentModel("demo", "gpt-5.4"); err != nil {
		t.Fatalf("SaveAgentModel() error: %v", err)
	}

	cfg := readTestConfig(t)
	if got, _ := cfg.Projects[0].Agent.Options["model"].(string); got != "gpt-5.4" {
		t.Fatalf("agent.options.model = %q, want gpt-5.4", got)
	}
	if got, _ := cfg.Projects[0].Agent.Options["mode"].(string); got != "default" {
		t.Fatalf("agent.options.mode = %q, want default", got)
	}
	if got, _ := cfg.Projects[0].Agent.Options["reasoning_effort"].(string); got != "high" {
		t.Fatalf("agent.options.reasoning_effort = %q, want high", got)
	}
}

const agentConfigWithCommentsTOML = `# This is my config file
# Very important - do not lose this!
custom_top = "keep_me"

[[projects]]
name = "demo"
work_dir = "/tmp/demo" # inline comment

[projects.agent]
type = "claudecode"

[projects.agent.options]
mode = "default"
reasoning_effort = "high"
custom_option = "still_here" # keep inline comment

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "test-token"
`

func TestSaveAgentModel_PreservesCommentsAndUnknownFields(t *testing.T) {
	writeTestConfig(t, agentConfigWithCommentsTOML)

	if err := SaveAgentModel("demo", "gpt-5.4"); err != nil {
		t.Fatalf("SaveAgentModel() error: %v", err)
	}

	content, err := os.ReadFile(ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(content)

	if !strings.Contains(text, "# This is my config file") {
		t.Fatalf("expected top comment to be preserved, got:\n%s", text)
	}
	if !strings.Contains(text, `custom_option = "still_here"`) {
		t.Fatalf("expected unknown options field to be preserved, got:\n%s", text)
	}
	if !strings.Contains(text, `reasoning_effort = "high"`) {
		t.Fatalf("expected reasoning_effort to be preserved, got:\n%s", text)
	}
	if !strings.Contains(text, `model = "gpt-5.4"`) {
		t.Fatalf("expected model to be set, got:\n%s", text)
	}
}

func TestSaveDisplayConfig_PreservesComments(t *testing.T) {
	configWithDisplay := agentConfigWithCommentsTOML + `
[display]
# display settings below
thinking_messages = true
custom_display = "keep" # also keep
`
	writeTestConfig(t, configWithDisplay)

	thinking := 200
	toolShow := false
	if err := SaveDisplayConfig(nil, nil, &thinking, nil, &toolShow); err != nil {
		t.Fatalf("SaveDisplayConfig() error: %v", err)
	}

	content, err := os.ReadFile(ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(content)

	if !strings.Contains(text, "# This is my config file") {
		t.Fatalf("expected top comment to be preserved, got:\n%s", text)
	}
	if !strings.Contains(text, "# display settings below") {
		t.Fatalf("expected display comment to be preserved, got:\n%s", text)
	}
	if !strings.Contains(text, `custom_display = "keep"`) {
		t.Fatalf("expected unknown display field to be preserved, got:\n%s", text)
	}
	if !strings.Contains(text, `thinking_max_len = 200`) {
		t.Fatalf("expected thinking_max_len to be set, got:\n%s", text)
	}
	if !strings.Contains(text, `tool_messages = false`) {
		t.Fatalf("expected tool_messages to be set, got:\n%s", text)
	}
}

func TestCommandConfig_AddAndRemove(t *testing.T) {
	writeTestConfig(t, baseConfigTOML)

	cmd := CommandConfig{Name: "review", Description: "code review", Prompt: "review {{args}}"}
	if err := AddCommand(cmd); err != nil {
		t.Fatalf("AddCommand() error: %v", err)
	}
	if err := AddCommand(cmd); err == nil {
		t.Fatal("AddCommand() duplicate command: expected error")
	}

	cfg := readTestConfig(t)
	if len(cfg.Commands) != 1 || cfg.Commands[0].Name != "review" {
		t.Fatalf("commands after add = %#v, want one review command", cfg.Commands)
	}

	if err := RemoveCommand("review"); err != nil {
		t.Fatalf("RemoveCommand() error: %v", err)
	}
	if err := RemoveCommand("review"); err == nil {
		t.Fatal("RemoveCommand() missing command: expected error")
	}
}

func TestAliasConfig_AddAndRemove(t *testing.T) {
	writeTestConfig(t, baseConfigTOML)

	if err := AddAlias(AliasConfig{Name: "帮助", Command: "/help"}); err != nil {
		t.Fatalf("AddAlias() error: %v", err)
	}
	if err := AddAlias(AliasConfig{Name: "帮助", Command: "/list"}); err != nil {
		t.Fatalf("AddAlias() update error: %v", err)
	}

	cfg := readTestConfig(t)
	if len(cfg.Aliases) != 1 || cfg.Aliases[0].Command != "/list" {
		t.Fatalf("aliases after update = %#v, want one updated alias", cfg.Aliases)
	}

	if err := RemoveAlias("帮助"); err != nil {
		t.Fatalf("RemoveAlias() error: %v", err)
	}
	if err := RemoveAlias("帮助"); err == nil {
		t.Fatal("RemoveAlias() missing alias: expected error")
	}
}

func TestDisplayConfig_Save(t *testing.T) {
	writeTestConfig(t, baseConfigTOML)

	thinking := 120
	tool := 240
	showTools := false
	if err := SaveDisplayConfig(nil, nil, &thinking, &tool, &showTools); err != nil {
		t.Fatalf("SaveDisplayConfig() error: %v", err)
	}

	cfg := readTestConfig(t)
	if cfg.Display.ThinkingMaxLen == nil || *cfg.Display.ThinkingMaxLen != 120 {
		t.Fatalf("ThinkingMaxLen = %#v, want 120", cfg.Display.ThinkingMaxLen)
	}
	if cfg.Display.ToolMaxLen == nil || *cfg.Display.ToolMaxLen != 240 {
		t.Fatalf("ToolMaxLen = %#v, want 240", cfg.Display.ToolMaxLen)
	}
	if cfg.Display.ToolMessages == nil || *cfg.Display.ToolMessages {
		t.Fatalf("ToolMessages = %#v, want false", cfg.Display.ToolMessages)
	}

	thinking = 360
	if err := SaveDisplayConfig(nil, nil, &thinking, nil, nil); err != nil {
		t.Fatalf("SaveDisplayConfig() second update error: %v", err)
	}

	cfg = readTestConfig(t)
	if cfg.Display.ThinkingMaxLen == nil || *cfg.Display.ThinkingMaxLen != 360 {
		t.Fatalf("ThinkingMaxLen after update = %#v, want 360", cfg.Display.ThinkingMaxLen)
	}
	if cfg.Display.ToolMaxLen == nil || *cfg.Display.ToolMaxLen != 240 {
		t.Fatalf("ToolMaxLen after nil update = %#v, want 240", cfg.Display.ToolMaxLen)
	}
	if cfg.Display.ToolMessages == nil || *cfg.Display.ToolMessages {
		t.Fatalf("ToolMessages after nil update = %#v, want false", cfg.Display.ToolMessages)
	}
}

const attachmentSendConfigFixture = `
attachment_send = "off"

[[projects]]
name = "alpha"

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/alpha"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"
`

func TestLoad_DefaultsAttachmentSendToOn(t *testing.T) {
	configPath := writeConfigFixture(t, telegramOnlyConfigFixture)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.AttachmentSend != "on" {
		t.Fatalf("cfg.AttachmentSend = %q, want %q", cfg.AttachmentSend, "on")
	}
}

func TestLoad_DefaultsAutoCompressDisabled(t *testing.T) {
	configPath := writeConfigFixture(t, telegramOnlyConfigFixture)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(cfg.Projects) == 0 {
		t.Fatalf("expected at least one project")
	}
	if cfg.Projects[0].AutoCompress.Enabled != nil {
		t.Fatalf("expected auto_compress.enabled to default to nil")
	}
}

func TestLoad_ParsesResetOnIdleMins(t *testing.T) {
	configPath := writeConfigFixture(t, projectWithResetOnIdleFixture)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Projects[0].ResetOnIdleMins == nil {
		t.Fatal("expected reset_on_idle_mins to be parsed")
	}
	if got := *cfg.Projects[0].ResetOnIdleMins; got != 60 {
		t.Fatalf("reset_on_idle_mins = %d, want 60", got)
	}
}

func TestLoad_RejectsNegativeResetOnIdleMins(t *testing.T) {
	configPath := writeConfigFixture(t, projectWithNegativeResetOnIdleFixture)

	_, err := Load(configPath)
	if err == nil {
		t.Fatal("expected error for negative reset_on_idle_mins")
	}
	if !strings.Contains(err.Error(), "reset_on_idle_mins") {
		t.Fatalf("error = %q, want reset_on_idle_mins validation", err.Error())
	}
}

func TestLoad_ParsesAgentSessionIdleTimeoutMins(t *testing.T) {
	configPath := writeConfigFixture(t, projectWithAgentSessionIdleTimeoutFixture)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Projects[0].AgentSessionIdleTimeoutMins == nil {
		t.Fatal("expected agent_session_idle_timeout_mins to be parsed")
	}
	if got := *cfg.Projects[0].AgentSessionIdleTimeoutMins; got != 45 {
		t.Fatalf("agent_session_idle_timeout_mins = %d, want 45", got)
	}
}

func TestLoad_RejectsNegativeAgentSessionIdleTimeoutMins(t *testing.T) {
	configPath := writeConfigFixture(t, projectWithNegativeAgentSessionIdleTimeoutFixture)

	_, err := Load(configPath)
	if err == nil {
		t.Fatal("expected error for negative agent_session_idle_timeout_mins")
	}
	if !strings.Contains(err.Error(), "agent_session_idle_timeout_mins") {
		t.Fatalf("error = %q, want agent_session_idle_timeout_mins validation", err.Error())
	}
}

func TestLoad_ParsesAttachmentSendOff(t *testing.T) {
	configPath := writeConfigFixture(t, attachmentSendConfigFixture)

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.AttachmentSend != "off" {
		t.Fatalf("cfg.AttachmentSend = %q, want %q", cfg.AttachmentSend, "off")
	}
}

func TestLoad_FilterExternalSessionsDefault(t *testing.T) {
	configPath := writeConfigFixture(t, attachmentSendConfigFixture)
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	proj := cfg.Projects[0]
	if proj.FilterExternalSessions != nil {
		t.Fatalf("FilterExternalSessions should be nil by default, got %v", *proj.FilterExternalSessions)
	}
}

func TestLoad_FilterExternalSessionsTrue(t *testing.T) {
	fixture := `
[[projects]]
name = "beta"
filter_external_sessions = true

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/beta"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "test"
`
	configPath := writeConfigFixture(t, fixture)
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	proj := cfg.Projects[0]
	if proj.FilterExternalSessions == nil || !*proj.FilterExternalSessions {
		t.Fatalf("FilterExternalSessions should be true, got %v", proj.FilterExternalSessions)
	}
}

func TestLoad_FilterExternalSessionsFalse(t *testing.T) {
	fixture := `
[[projects]]
name = "gamma"
filter_external_sessions = false

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/gamma"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "test"
`
	configPath := writeConfigFixture(t, fixture)
	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	proj := cfg.Projects[0]
	if proj.FilterExternalSessions == nil || *proj.FilterExternalSessions {
		t.Fatalf("FilterExternalSessions should be false, got %v", proj.FilterExternalSessions)
	}
}

func validProject(name string) ProjectConfig {
	return ProjectConfig{
		Name: name,
		Agent: AgentConfig{
			Type:    "claudecode",
			Options: map[string]any{"mode": "default"},
		},
		Platforms: []PlatformConfig{
			{Type: "telegram", Options: map[string]any{"token": "test-token"}},
		},
	}
}

func assertErrContains(t *testing.T, err error, want string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected error containing %q, got nil", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want substring %q", err.Error(), want)
	}
}

func writeTestConfig(t *testing.T, content string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	oldPath := ConfigPath
	ConfigPath = path
	t.Cleanup(func() {
		ConfigPath = oldPath
	})
}

func readTestConfig(t *testing.T) Config {
	t.Helper()

	data, err := os.ReadFile(ConfigPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	return cfg
}

func writeConfigFixture(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config fixture: %v", err)
	}
	return path
}

func patchConfigPath(t *testing.T, path string) {
	t.Helper()
	prev := ConfigPath
	ConfigPath = path
	t.Cleanup(func() {
		ConfigPath = prev
	})
}

func readConfigFixture(t *testing.T, path string) *Config {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config fixture: %v", err)
	}
	cfg := &Config{}
	if err := toml.Unmarshal(data, cfg); err != nil {
		t.Fatalf("parse config fixture: %v", err)
	}
	return cfg
}

func stringMapValue(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

const baseConfigTOML = `
[[projects]]
name = "demo"

[projects.agent]
type = "claudecode"

[projects.agent.options]
mode = "default"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "test-token"
`

const agentConfigTOML = `
[[projects]]
name = "demo"

[projects.agent]
type = "claudecode"

[projects.agent.options]
mode = "default"
reasoning_effort = "high"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "test-token"
`

const multiPlatformConfigFixture = `
[[projects]]
name = "alpha"

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/alpha"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"

[[projects.platforms]]
type = "weixin"

[projects.platforms.options]
token = "old_weixin_token"

[[projects.platforms]]
type = "qq"

[projects.platforms.options]
ws_url = "ws://127.0.0.1:3001"
allow_from = "ou_existing_owner"
`

const telegramOnlyConfigFixture = `
[[projects]]
name = "beta"

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/beta"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"
`

const projectWithResetOnIdleFixture = `
[[projects]]
name = "beta"
reset_on_idle_mins = 60

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/beta"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"
`

const projectWithNegativeResetOnIdleFixture = `
[[projects]]
name = "beta"
reset_on_idle_mins = -1

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/beta"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"
`

const projectWithAgentSessionIdleTimeoutFixture = `
[[projects]]
name = "beta"
agent_session_idle_timeout_mins = 45

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/beta"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"
`

const projectWithNegativeAgentSessionIdleTimeoutFixture = `
[[projects]]
name = "beta"
agent_session_idle_timeout_mins = -1

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/beta"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
bot_token = "token_xxx"
`

const projectWithRunAsUserFixture = `
[[projects]]
name = "sandboxed"
run_as_user = "partseeker-coder"
run_as_env = ["PGSSLROOTCERT", "PGSSLMODE"]

[projects.agent]
type = "claudecode"

[projects.agent.options]
work_dir = "/tmp/sandboxed"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "tg-token"
`

const projectWithRunAsUserRootFixture = `
[[projects]]
name = "bad"
run_as_user = "root"

[projects.agent]
type = "claudecode"

[projects.agent.options]
work_dir = "/tmp/bad"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "tg-token"
`

const projectWithRunAsUserInvalidFixture = `
[[projects]]
name = "bad"
run_as_user = "has space"

[projects.agent]
type = "claudecode"

[projects.agent.options]
work_dir = "/tmp/bad"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
token = "tg-token"
`

const weixinConfigFixture = `
[[projects]]
name = "alpha"

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/alpha"

[[projects.platforms]]
type = "weixin"

[projects.platforms.options]
token = "old_weixin_token"
base_url = "https://ilink.example"
`

const preserveFormatFixture = `# top comment should stay
custom_top = "keep_me"

[[projects]]
name = "alpha"

[projects.agent]
type = "codex"

[projects.agent.options]
work_dir = "/tmp/alpha"

[[projects.platforms]]
type = "telegram"

[projects.platforms.options]
app_id = "old_app" # keep inline comment
app_secret = "old_secret"
custom_option = "still_here"
`

// --- validateUsersConfig tests ---

func TestValidateUsersConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "nil users is valid",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users:     nil,
				}},
			},
			wantErr: "",
		},
		{
			name: "empty roles",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users:     &UsersConfig{Roles: map[string]RoleConfig{}},
				}},
			},
			wantErr: `no roles defined`,
		},
		{
			name: "empty user_ids in role",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users: &UsersConfig{
						Roles: map[string]RoleConfig{
							"admin": {UserIDs: []string{}},
						},
					},
				}},
			},
			wantErr: `empty user_ids`,
		},
		{
			name: "duplicate user in different roles",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users: &UsersConfig{
						Roles: map[string]RoleConfig{
							"admin":  {UserIDs: []string{"user1"}},
							"member": {UserIDs: []string{"user1"}},
						},
					},
				}},
			},
			wantErr: `appears in both role`,
		},
		{
			name: "wildcard in multiple roles",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users: &UsersConfig{
						Roles: map[string]RoleConfig{
							"admin":  {UserIDs: []string{"*"}},
							"member": {UserIDs: []string{"*"}},
						},
					},
				}},
			},
			wantErr: `wildcard`,
		},
		{
			name: "default_role not matching any role",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users: &UsersConfig{
						DefaultRole: "superadmin",
						Roles: map[string]RoleConfig{
							"admin": {UserIDs: []string{"u1"}},
						},
					},
				}},
			},
			wantErr: `default_role`,
		},
		{
			name: "valid users config",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users: &UsersConfig{
						DefaultRole: "member",
						Roles: map[string]RoleConfig{
							"admin":  {UserIDs: []string{"admin1"}},
							"member": {UserIDs: []string{"*"}},
						},
					},
				}},
			},
			wantErr: "",
		},
		{
			name: "valid with wildcard in one role only",
			cfg: Config{
				Projects: []ProjectConfig{{
					Name:      "p1",
					Agent:     AgentConfig{Type: "codex"},
					Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
					Users: &UsersConfig{
						Roles: map[string]RoleConfig{
							"admin":  {UserIDs: []string{"u1"}},
							"member": {UserIDs: []string{"*", "u2"}},
						},
					},
				}},
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUsersConfig("projects[0]", tt.cfg.Projects[0].Users)
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want substring %q", err.Error(), tt.wantErr)
				}
			}
		})
	}
}

// --- cloneStringMap tests ---

func TestCloneStringMap(t *testing.T) {
	// nil map
	if got := cloneStringMap(nil); got != nil {
		t.Errorf("cloneStringMap(nil) = %v, want nil", got)
	}

	// empty map
	empty := cloneStringMap(map[string]string{})
	if got := cloneStringMap(empty); got == nil || len(got) != 0 {
		t.Errorf("cloneStringMap(empty) = %v, want empty non-nil map", got)
	}

	// populated map
	orig := map[string]string{"key1": "val1", "key2": "val2"}
	cloned := cloneStringMap(orig)
	if len(cloned) != len(orig) {
		t.Errorf("length mismatch: got %d, want %d", len(cloned), len(orig))
	}
	for k, v := range orig {
		if cloned[k] != v {
			t.Errorf("cloneStringMap[%q] = %q, want %q", k, cloned[k], v)
		}
	}
	// verify it's a deep copy
	delete(cloned, "key1")
	if _, ok := orig["key1"]; !ok {
		t.Error("cloneStringMap returned same map reference, not a copy")
	}
}

// --- pickAgentTemplateForNewProject tests ---

func TestPickAgentTemplateForNewProject(t *testing.T) {
	baseProj := ProjectConfig{
		Name: "base",
		Agent: AgentConfig{
			Type:    "claudecode",
			Options: map[string]any{"mode": "yolo"},
		},
		Platforms: []PlatformConfig{{Type: "telegram", Options: map[string]any{"token": "x"}}},
	}

	t.Run("clone from existing project", func(t *testing.T) {
		cfg := &Config{Projects: []ProjectConfig{baseProj}}
		opts := newProjectAgentTemplate{CloneFromProject: "base"}
		got := pickAgentTemplateForNewProject(cfg, opts)
		if got.Type != "claudecode" {
			t.Errorf("Type = %q, want claudecode", got.Type)
		}
		if got.Options["mode"] != "yolo" {
			t.Errorf("Options not cloned correctly")
		}
	})

	t.Run("no clone but has projects", func(t *testing.T) {
		cfg := &Config{Projects: []ProjectConfig{baseProj}}
		opts := newProjectAgentTemplate{}
		got := pickAgentTemplateForNewProject(cfg, opts)
		if got.Type != "claudecode" {
			t.Errorf("Type = %q, want claudecode", got.Type)
		}
	})

	t.Run("no projects uses default codex", func(t *testing.T) {
		cfg := &Config{Projects: []ProjectConfig{}}
		opts := newProjectAgentTemplate{}
		got := pickAgentTemplateForNewProject(cfg, opts)
		if got.Type != "codex" {
			t.Errorf("Type = %q, want codex", got.Type)
		}
		if got.Options == nil {
			t.Error("Options should not be nil")
		}
	})

	t.Run("no projects with explicit agent type", func(t *testing.T) {
		cfg := &Config{Projects: []ProjectConfig{}}
		opts := newProjectAgentTemplate{AgentType: "claudecode"}
		got := pickAgentTemplateForNewProject(cfg, opts)
		if got.Type != "claudecode" {
			t.Errorf("Type = %q, want claudecode", got.Type)
		}
	})

	t.Run("explicit agent type overrides clone from first project", func(t *testing.T) {
		cfg := &Config{Projects: []ProjectConfig{baseProj}}
		opts := newProjectAgentTemplate{AgentType: "codex"}
		got := pickAgentTemplateForNewProject(cfg, opts)
		if got.Type != "codex" {
			t.Errorf("Type = %q, want codex (explicit AgentType should take priority over cloning first project)", got.Type)
		}
	})
}

// --- cloneAgentConfig tests ---

func TestCloneAgentConfig(t *testing.T) {
	in := AgentConfig{
		Type:    "codex",
		Options: map[string]any{"mode": "default"},
	}
	got := cloneAgentConfig(in)
	if got.Type != "codex" {
		t.Errorf("Type = %q, want codex", got.Type)
	}
	if got.Options["mode"] != "default" {
		t.Errorf("Options not cloned")
	}
	// Verify deep copy of Options
	got.Options["mode"] = "changed"
	if in.Options["mode"] == "changed" {
		t.Error("Options is same reference, not a deep copy")
	}
}

func TestEnsureProjectWithWeixinPlatform_CreatesMissingProject(t *testing.T) {
	configPath := writeConfigFixture(t, multiPlatformConfigFixture)
	patchConfigPath(t, configPath)

	result, err := EnsureProjectWithWeixinPlatform(EnsureProjectWithWeixinOptions{
		ProjectName: "gamma",
		WorkDir:     "/tmp/gamma",
	})
	if err != nil {
		t.Fatalf("EnsureProjectWithWeixinPlatform returned error: %v", err)
	}
	if !result.Created {
		t.Fatal("result.Created = false, want true")
	}
	if result.AddedPlatform {
		t.Fatal("result.AddedPlatform = true, want false")
	}

	cfg := readConfigFixture(t, configPath)
	if len(cfg.Projects) != 2 {
		t.Fatalf("len(cfg.Projects) = %d, want 2", len(cfg.Projects))
	}
	proj := cfg.Projects[1]
	if proj.Name != "gamma" {
		t.Fatalf("proj.Name = %q, want %q", proj.Name, "gamma")
	}
	if len(proj.Platforms) != 1 {
		t.Fatalf("len(proj.Platforms) = %d, want 1", len(proj.Platforms))
	}
	if proj.Platforms[0].Type != "weixin" {
		t.Fatalf("platform type = %q, want weixin", proj.Platforms[0].Type)
	}
}

func TestEnsureProjectWithWeixinPlatform_AddsPlatformWhenMissing(t *testing.T) {
	configPath := writeConfigFixture(t, telegramOnlyConfigFixture)
	patchConfigPath(t, configPath)

	result, err := EnsureProjectWithWeixinPlatform(EnsureProjectWithWeixinOptions{
		ProjectName: "beta",
	})
	if err != nil {
		t.Fatalf("EnsureProjectWithWeixinPlatform returned error: %v", err)
	}
	if result.Created {
		t.Fatal("result.Created = true, want false")
	}
	if !result.AddedPlatform {
		t.Fatal("result.AddedPlatform = false, want true")
	}

	cfg := readConfigFixture(t, configPath)
	proj := cfg.Projects[0]
	if len(proj.Platforms) != 2 {
		t.Fatalf("len(proj.Platforms) = %d, want 2", len(proj.Platforms))
	}
	if proj.Platforms[1].Type != "weixin" {
		t.Fatalf("platform type = %q, want weixin", proj.Platforms[1].Type)
	}
}

func TestSaveWeixinPlatformCredentials_UpdateToken(t *testing.T) {
	configPath := writeConfigFixture(t, weixinConfigFixture)
	patchConfigPath(t, configPath)

	_, err := SaveWeixinPlatformCredentials(WeixinCredentialUpdateOptions{
		ProjectName: "alpha",
		Token:       "new_weixin_token",
		BaseURL:     "https://ilinkai.weixin.qq.com",
	})
	if err != nil {
		t.Fatalf("SaveWeixinPlatformCredentials returned error: %v", err)
	}

	cfg := readConfigFixture(t, configPath)
	tok, _ := cfg.Projects[0].Platforms[0].Options["token"].(string)
	if tok != "new_weixin_token" {
		t.Fatalf("token = %q, want new_weixin_token", tok)
	}
	bu, _ := cfg.Projects[0].Platforms[0].Options["base_url"].(string)
	if bu != "https://ilinkai.weixin.qq.com" {
		t.Fatalf("base_url = %q", bu)
	}
}

func TestSaveWeixinPlatformCredentials_AppendsScannedUserToAllowFrom(t *testing.T) {
	configPath := writeConfigFixture(t, strings.Replace(weixinConfigFixture, `base_url = "https://ilink.example"`, "base_url = \"https://ilink.example\"\nallow_from = \"wx_user_1\"", 1))
	patchConfigPath(t, configPath)

	result, err := SaveWeixinPlatformCredentials(WeixinCredentialUpdateOptions{
		ProjectName:       "alpha",
		Token:             "new_weixin_token",
		ScannedUserID:     "wx_user_2",
		SetAllowFromEmpty: true,
	})
	if err != nil {
		t.Fatalf("SaveWeixinPlatformCredentials returned error: %v", err)
	}

	if result.AllowFrom != "wx_user_1,wx_user_2" {
		t.Fatalf("result.AllowFrom = %q, want %q", result.AllowFrom, "wx_user_1,wx_user_2")
	}

	cfg := readConfigFixture(t, configPath)
	if got := stringMapValue(cfg.Projects[0].Platforms[0].Options, "allow_from"); got != "wx_user_1,wx_user_2" {
		t.Fatalf("allow_from = %q, want %q", got, "wx_user_1,wx_user_2")
	}
}

func TestSaveWeixinPlatformCredentials_LeavesWildcardAllowFromUnchanged(t *testing.T) {
	configPath := writeConfigFixture(t, strings.Replace(weixinConfigFixture, `base_url = "https://ilink.example"`, "base_url = \"https://ilink.example\"\nallow_from = \"*\"", 1))
	patchConfigPath(t, configPath)

	result, err := SaveWeixinPlatformCredentials(WeixinCredentialUpdateOptions{
		ProjectName:       "alpha",
		Token:             "new_weixin_token",
		ScannedUserID:     "wx_user_2",
		SetAllowFromEmpty: true,
	})
	if err != nil {
		t.Fatalf("SaveWeixinPlatformCredentials returned error: %v", err)
	}

	if result.AllowFrom != "*" {
		t.Fatalf("result.AllowFrom = %q, want %q", result.AllowFrom, "*")
	}

	cfg := readConfigFixture(t, configPath)
	if got := stringMapValue(cfg.Projects[0].Platforms[0].Options, "allow_from"); got != "*" {
		t.Fatalf("allow_from = %q, want %q", got, "*")
	}
}

func TestSaveProjectSettings_ExtraFields(t *testing.T) {
	configPath := writeConfigFixture(t, multiPlatformConfigFixture)
	patchConfigPath(t, configPath)

	show := true
	hideWorkdir := false
	wd := "/tmp/patched"
	mode := "yolo"
	err := SaveProjectSettings("alpha", ProjectSettingsUpdate{
		WorkDir:              &wd,
		Mode:                 &mode,
		ShowContextIndicator: &show,
		ShowWorkdirIndicator: &hideWorkdir,
		PlatformAllowFrom:    map[string]string{"telegram": "u1", "Weixin": "u2"},
	})
	if err != nil {
		t.Fatalf("SaveProjectSettings: %v", err)
	}

	cfg := readConfigFixture(t, configPath)
	proj := cfg.Projects[0]
	if stringMapValue(proj.Agent.Options, "work_dir") != wd {
		t.Fatalf("work_dir = %q, want %q", stringMapValue(proj.Agent.Options, "work_dir"), wd)
	}
	if stringMapValue(proj.Agent.Options, "mode") != mode {
		t.Fatalf("mode = %q, want %q", stringMapValue(proj.Agent.Options, "mode"), mode)
	}
	if proj.ShowContextIndicator == nil || !*proj.ShowContextIndicator {
		t.Fatalf("ShowContextIndicator = %v, want true", proj.ShowContextIndicator)
	}
	if proj.ShowWorkdirIndicator == nil || *proj.ShowWorkdirIndicator {
		t.Fatalf("ShowWorkdirIndicator = %v, want false (per patch)", proj.ShowWorkdirIndicator)
	}
	if stringMapValue(proj.Platforms[0].Options, "allow_from") != "u1" {
		t.Fatalf("telegram allow_from = %q, want u1", stringMapValue(proj.Platforms[0].Options, "allow_from"))
	}
	if stringMapValue(proj.Platforms[1].Options, "allow_from") != "u2" {
		t.Fatalf("weixin allow_from = %q, want u2", stringMapValue(proj.Platforms[1].Options, "allow_from"))
	}
}

func TestGetProjectConfigDetails(t *testing.T) {
	configPath := writeConfigFixture(t, multiPlatformConfigFixture)
	patchConfigPath(t, configPath)

	details := GetProjectConfigDetails("alpha")
	if details == nil {
		t.Fatal("GetProjectConfigDetails returned nil")
	}
	if details["work_dir"] != "/tmp/alpha" {
		t.Fatalf("work_dir = %v", details["work_dir"])
	}
	pcs, ok := details["platform_configs"].([]map[string]any)
	if !ok || len(pcs) < 2 {
		t.Fatalf("platform_configs = %#v", details["platform_configs"])
	}
}

func TestAddPlatformToProject_NewProjectWithAgentTypeAndWorkDir(t *testing.T) {
	configPath := writeConfigFixture(t, multiPlatformConfigFixture)
	patchConfigPath(t, configPath)

	err := AddPlatformToProject("sigma", PlatformConfig{Type: "telegram", Options: map[string]any{"token": "x"}}, "/sigma", "claudecode")
	if err != nil {
		t.Fatalf("AddPlatformToProject: %v", err)
	}
	cfg := readConfigFixture(t, configPath)
	if len(cfg.Projects) != 2 {
		t.Fatalf("len(projects) = %d, want 2", len(cfg.Projects))
	}
	proj := cfg.Projects[1]
	if proj.Name != "sigma" {
		t.Fatalf("name = %q", proj.Name)
	}
	if proj.Agent.Type != "claudecode" {
		t.Fatalf("agent type = %q, want claudecode", proj.Agent.Type)
	}
	if stringMapValue(proj.Agent.Options, "work_dir") != "/sigma" {
		t.Fatalf("work_dir = %q", stringMapValue(proj.Agent.Options, "work_dir"))
	}
	if len(proj.Platforms) != 1 || proj.Platforms[0].Type != "telegram" {
		t.Fatalf("platforms = %#v", proj.Platforms)
	}
}

func TestAddPlatformToProject_NewProjectClonesAgentWhenAgentTypeEmpty(t *testing.T) {
	configPath := writeConfigFixture(t, multiPlatformConfigFixture)
	patchConfigPath(t, configPath)

	err := AddPlatformToProject("tau", PlatformConfig{Type: "telegram", Options: map[string]any{"token": "x"}}, "", "")
	if err != nil {
		t.Fatalf("AddPlatformToProject: %v", err)
	}
	cfg := readConfigFixture(t, configPath)
	proj := cfg.Projects[len(cfg.Projects)-1]
	if proj.Agent.Type != "codex" {
		t.Fatalf("agent type = %q, want codex (cloned)", proj.Agent.Type)
	}
	if stringMapValue(proj.Agent.Options, "work_dir") != "/tmp/alpha" {
		t.Fatalf("cloned work_dir = %q, want /tmp/alpha", stringMapValue(proj.Agent.Options, "work_dir"))
	}
}

func TestFormatTOML(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{
			name:  "collapse multiple blank lines",
			input: "a = 1\n\n\n\nb = 2\n",
			want:  "a = 1\n\nb = 2\n",
		},
		{
			name:  "blank line before section header",
			input: "a = 1\n[section]\nb = 2\n",
			want:  "a = 1\n\n[section]\nb = 2\n",
		},
		{
			name:  "strip trailing whitespace",
			input: "a = 1   \nb = 2\t\n",
			want:  "a = 1\nb = 2\n",
		},
		{
			name:  "remove empty section",
			input: "[empty]\n\n[real]\nk = 1\n",
			want:  "[real]\nk = 1\n",
		},
		{
			name:  "already formatted",
			input: "[section]\na = 1\n",
			want:  "[section]\na = 1\n",
		},
		{
			name:  "preserves comments",
			input: "# comment\na = 1\n\n[section]\n# inline\nb = 2\n",
			want:  "# comment\na = 1\n\n[section]\n# inline\nb = 2\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := formatTOML(tc.input)
			if got != tc.want {
				t.Errorf("formatTOML:\n  input: %q\n  got:   %q\n  want:  %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestFormatConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")

	messy := "data_dir = \"/tmp/ab\"   \n\n\n\n[[projects]]\nname = \"test\"\n\n\n[projects.agent]\ntype = \"codex\"\n\n[projects.agent.options]\n\n[[projects.platforms]]\ntype = \"telegram\"\n\n[projects.platforms.options]\ntoken = \"abc\"\n"
	os.WriteFile(path, []byte(messy), 0o644)

	if err := FormatConfigFile(path); err != nil {
		t.Fatalf("FormatConfigFile: %v", err)
	}

	data, _ := os.ReadFile(path)
	content := string(data)

	if strings.Contains(content, "   \n") {
		t.Error("trailing whitespace not stripped")
	}
	if strings.Contains(content, "\n\n\n") {
		t.Error("consecutive blank lines not collapsed")
	}

	cfg := &Config{}
	if _, err := toml.Decode(content, cfg); err != nil {
		t.Fatalf("formatted config is invalid TOML: %v", err)
	}
	if len(cfg.Projects) != 1 || cfg.Projects[0].Name != "test" {
		t.Error("formatting corrupted config content")
	}

	t.Run("no-op when already formatted", func(t *testing.T) {
		before, _ := os.ReadFile(path)
		if err := FormatConfigFile(path); err != nil {
			t.Fatalf("second FormatConfigFile: %v", err)
		}
		after, _ := os.ReadFile(path)
		if string(before) != string(after) {
			t.Error("idempotent format produced different output")
		}
	})

	t.Run("rejects invalid TOML", func(t *testing.T) {
		badPath := filepath.Join(dir, "bad.toml")
		os.WriteFile(badPath, []byte("[invalid\n"), 0o644)
		if err := FormatConfigFile(badPath); err == nil {
			t.Error("expected error for invalid TOML")
		}
	})
}
