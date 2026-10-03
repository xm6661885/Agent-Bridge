package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

// --- stubs for Engine tests ---

type stubAgent struct{}

func (a *stubAgent) Name() string { return "stub" }
func (a *stubAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	return &stubAgentSession{}, nil
}
func (a *stubAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) { return nil, nil }
func (a *stubAgent) Stop() error                                                { return nil }

type stubAgentSession struct{}

func (s *stubAgentSession) Send(_ string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	return nil
}
func (s *stubAgentSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *stubAgentSession) Events() <-chan Event                                 { return make(chan Event) }
func (s *stubAgentSession) CurrentSessionID() string                             { return "stub-session" }
func (s *stubAgentSession) Alive() bool                                          { return true }
func (s *stubAgentSession) Close() error                                         { return nil }

type recordingAgentSession struct {
	stubAgentSession
	lastID     string
	lastResult PermissionResult
	calls      int
}

func (s *recordingAgentSession) RespondPermission(id string, res PermissionResult) error {
	s.lastID = id
	s.lastResult = res
	s.calls++
	return nil
}

type stubPlatformEngine struct {
	n    string
	sent []string
	mu   sync.Mutex
}

func (p *stubPlatformEngine) Name() string               { return p.n }
func (p *stubPlatformEngine) Start(MessageHandler) error { return nil }
func (p *stubPlatformEngine) Reply(_ context.Context, _ any, content string) error {
	p.mu.Lock()
	p.sent = append(p.sent, content)
	p.mu.Unlock()
	return nil
}
func (p *stubPlatformEngine) Send(_ context.Context, _ any, content string) error {
	p.mu.Lock()
	p.sent = append(p.sent, content)
	p.mu.Unlock()
	return nil
}
func (p *stubPlatformEngine) Stop() error { return nil }

func (p *stubPlatformEngine) getSent() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]string, len(p.sent))
	copy(cp, p.sent)
	return cp
}

func (p *stubPlatformEngine) clearSent() {
	p.mu.Lock()
	p.sent = nil
	p.mu.Unlock()
}

type recallCheckingPlatform struct {
	stubPlatformEngine
	recalled bool
	checked  []any
}

func (p *recallCheckingPlatform) IsMessageRecalled(_ context.Context, replyCtx any) (bool, error) {
	p.mu.Lock()
	p.checked = append(p.checked, replyCtx)
	p.mu.Unlock()
	return p.recalled, nil
}

func (p *recallCheckingPlatform) checkedReplyCtxs() []any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]any, len(p.checked))
	copy(out, p.checked)
	return out
}

type stubReplyTargetPlatform struct {
	stubPlatformEngine
	reconstructSessionKey string
}

func (p *stubReplyTargetPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	p.reconstructSessionKey = sessionKey
	return "base-rctx", nil
}

func (p *stubReplyTargetPlatform) SendFile(_ context.Context, _ any, _ FileAttachment) error {
	return nil
}

type resultAgent struct {
	session AgentSession
}

func (a *resultAgent) Name() string { return "stub" }
func (a *resultAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	return a.session, nil
}
func (a *resultAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) { return nil, nil }
func (a *resultAgent) Stop() error                                                { return nil }

type sessionEnvRecordingAgent struct {
	stubAgent
	session AgentSession
	mu      sync.Mutex
	env     []string
}

func (a *sessionEnvRecordingAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	if a.session != nil {
		return a.session, nil
	}
	return &stubAgentSession{}, nil
}

func (a *sessionEnvRecordingAgent) SetSessionEnv(env []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.env = append([]string(nil), env...)
}

func (a *sessionEnvRecordingAgent) EnvValue(key string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	prefix := key + "="
	for _, entry := range a.env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

type resultAgentSession struct {
	mu          sync.Mutex
	events      chan Event
	result      string
	sendOnce    sync.Once
	sentPrompts []string
	images      []ImageAttachment
	files       []FileAttachment
}

type serialBlockingAgent struct {
	mu       sync.Mutex
	sessions []*serialBlockingSession
}

func (a *serialBlockingAgent) Name() string { return "serial-blocking" }
func (a *serialBlockingAgent) StartSession(_ context.Context, _ string) (AgentSession, error) {
	s := &serialBlockingSession{events: make(chan Event, 1), sends: make(chan string, 1)}
	a.mu.Lock()
	a.sessions = append(a.sessions, s)
	a.mu.Unlock()
	return s, nil
}
func (a *serialBlockingAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *serialBlockingAgent) Stop() error { return nil }
func (a *serialBlockingAgent) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.sessions)
}
func (a *serialBlockingAgent) session(i int) *serialBlockingSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.sessions[i]
}

type serialBlockingSession struct {
	events chan Event
	sends  chan string
}

func (s *serialBlockingSession) Send(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.sends <- prompt
	return nil
}
func (s *serialBlockingSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *serialBlockingSession) Events() <-chan Event                                 { return s.events }
func (s *serialBlockingSession) CurrentSessionID() string                             { return "serial-blocking" }
func (s *serialBlockingSession) Alive() bool                                          { return true }
func (s *serialBlockingSession) Close() error                                         { return nil }

func newResultAgentSession(result string) *resultAgentSession {
	return &resultAgentSession{
		events: make(chan Event, 1),
		result: result,
	}
}

func (s *resultAgentSession) Send(prompt string, _ string, images []ImageAttachment, files []FileAttachment) error {
	s.mu.Lock()
	s.sentPrompts = append(s.sentPrompts, prompt)
	s.mu.Unlock()
	s.images = append(s.images, images...)
	s.files = append(s.files, files...)
	s.sendOnce.Do(func() {
		s.events <- Event{Type: EventResult, Content: s.result, Done: true}
	})
	return nil
}

func (s *resultAgentSession) getSentPrompts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sentPrompts...)
}

func (s *resultAgentSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *resultAgentSession) Events() <-chan Event                                 { return s.events }
func (s *resultAgentSession) CurrentSessionID() string                             { return "result-session" }
func (s *resultAgentSession) Alive() bool                                          { return true }
func (s *resultAgentSession) Close() error                                         { return nil }

type stubLifecyclePlatform struct {
	stubPlatformEngine
	handler            PlatformLifecycleHandler
	registerCalls      int
	registeredCommands []BotCommandInfo
	cardNavSetCalls    int
	startCalls         int
	stopCalls          int
}

func (p *stubLifecyclePlatform) Start(MessageHandler) error {
	p.startCalls++
	return nil
}

func (p *stubLifecyclePlatform) Stop() error {
	p.stopCalls++
	return nil
}

func (p *stubLifecyclePlatform) SetLifecycleHandler(h PlatformLifecycleHandler) {
	p.handler = h
}

func (p *stubLifecyclePlatform) RegisterCommands(commands []BotCommandInfo) error {
	p.registerCalls++
	p.registeredCommands = append([]BotCommandInfo(nil), commands...)
	return nil
}

func (p *stubLifecyclePlatform) SetCardNavigationHandler(CardNavigationHandler) {
	p.cardNavSetCalls++
}

type blockingRegisterPlatform struct {
	stubLifecyclePlatform
	registerStarted chan struct{}
	allowRegister   chan struct{}
	stopCalled      chan struct{}
	registerOnce    sync.Once
	stopOnce        sync.Once
}

func newBlockingRegisterPlatform(name string) *blockingRegisterPlatform {
	return &blockingRegisterPlatform{
		stubLifecyclePlatform: stubLifecyclePlatform{
			stubPlatformEngine: stubPlatformEngine{n: name},
		},
		registerStarted: make(chan struct{}),
		allowRegister:   make(chan struct{}),
		stopCalled:      make(chan struct{}),
	}
}

func (p *blockingRegisterPlatform) RegisterCommands([]BotCommandInfo) error {
	p.registerOnce.Do(func() {
		close(p.registerStarted)
	})
	<-p.allowRegister
	p.registerCalls++
	return nil
}

func (p *blockingRegisterPlatform) Stop() error {
	p.stopCalls++
	p.stopOnce.Do(func() {
		close(p.stopCalled)
	})
	return nil
}

type stubMediaPlatform struct {
	stubPlatformEngine
	images []ImageAttachment
	files  []FileAttachment
}

func (p *stubMediaPlatform) SendImage(_ context.Context, _ any, img ImageAttachment) error {
	p.images = append(p.images, img)
	return nil
}

func (p *stubMediaPlatform) SendFile(_ context.Context, _ any, file FileAttachment) error {
	p.files = append(p.files, file)
	return nil
}

type stubInlineButtonPlatform struct {
	stubPlatformEngine
	buttonContent string
	buttonRows    [][]ButtonOption
}

func (p *stubInlineButtonPlatform) SendWithButtons(_ context.Context, _ any, content string, buttons [][]ButtonOption) error {
	p.buttonContent = content
	p.buttonRows = buttons
	return nil
}

type stubCardPlatform struct {
	stubPlatformEngine
	mu             sync.Mutex
	repliedCards   []*Card
	sentCards      []*Card
	refreshedCards []*Card
	cardErr        error
}

func (p *stubCardPlatform) ReplyCard(_ context.Context, _ any, card *Card) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cardErr != nil {
		return p.cardErr
	}
	p.repliedCards = append(p.repliedCards, card)
	return nil
}

func (p *stubCardPlatform) SendCard(_ context.Context, _ any, card *Card) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cardErr != nil {
		return p.cardErr
	}
	p.sentCards = append(p.sentCards, card)
	return nil
}

func (p *stubCardPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	return "reconstructed-ctx:" + sessionKey, nil
}

func (p *stubCardPlatform) RefreshCard(_ context.Context, _ string, card *Card) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cardErr != nil {
		return p.cardErr
	}
	p.refreshedCards = append(p.refreshedCards, card)
	return nil
}

func (p *stubCardPlatform) getRefreshedCards() []*Card {
	p.mu.Lock()
	defer p.mu.Unlock()
	dst := make([]*Card, len(p.refreshedCards))
	copy(dst, p.refreshedCards)
	return dst
}

type stubCompactProgressPlatform struct {
	stubPlatformEngine
	style          string
	supportPayload bool
	previewMu      sync.Mutex
	previewStarts  []string
	previewEdits   []string
}

func (p *stubCompactProgressPlatform) ProgressStyle() string {
	if p.style == "" {
		return "compact"
	}
	return p.style
}

func (p *stubCompactProgressPlatform) SupportsProgressCardPayload() bool {
	return p.supportPayload
}

func (p *stubCompactProgressPlatform) SendPreviewStart(_ context.Context, _ any, content string) (any, error) {
	p.previewMu.Lock()
	p.previewStarts = append(p.previewStarts, content)
	p.previewMu.Unlock()
	return "preview-handle", nil
}

func (p *stubCompactProgressPlatform) UpdateMessage(_ context.Context, _ any, content string) error {
	p.previewMu.Lock()
	p.previewEdits = append(p.previewEdits, content)
	p.previewMu.Unlock()
	return nil
}

func (p *stubCompactProgressPlatform) BuildRichCard(status CardStatus, title string, steps []ToolStep, markdown string, streaming bool, statusFooter string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "rich status=%s title=%s streaming=%t footer=%s\n", status, title, streaming, statusFooter)
	for _, step := range steps {
		fmt.Fprintf(&b, "step=%+v\n", step)
	}
	if markdown != "" {
		fmt.Fprintf(&b, "markdown=%s\n", markdown)
	}
	return b.String()
}

func (p *stubCompactProgressPlatform) getPreviewStarts() []string {
	p.previewMu.Lock()
	defer p.previewMu.Unlock()
	out := make([]string, len(p.previewStarts))
	copy(out, p.previewStarts)
	return out
}

func (p *stubCompactProgressPlatform) getPreviewEdits() []string {
	p.previewMu.Lock()
	defer p.previewMu.Unlock()
	out := make([]string, len(p.previewEdits))
	copy(out, p.previewEdits)
	return out
}

type stubDoneReactionPlatform struct {
	stubPlatformEngine
	doneMu    sync.Mutex
	doneCount int
	doneCtxs  []any
}

func (p *stubDoneReactionPlatform) AddDoneReaction(replyCtx any) {
	p.doneMu.Lock()
	defer p.doneMu.Unlock()
	p.doneCount++
	p.doneCtxs = append(p.doneCtxs, replyCtx)
}

func (p *stubDoneReactionPlatform) doneSnapshot() (int, []any) {
	p.doneMu.Lock()
	defer p.doneMu.Unlock()
	ctxs := make([]any, len(p.doneCtxs))
	copy(ctxs, p.doneCtxs)
	return p.doneCount, ctxs
}

type stubAskQuestionRichCardPlatform struct {
	stubCardPlatform
}

func (p *stubAskQuestionRichCardPlatform) BuildRichCard(status CardStatus, title string, steps []ToolStep, markdown string, streaming bool, statusFooter string) string {
	return "rich card"
}

type stubModelModeAgent struct {
	stubAgent
	model           string
	mode            string
	reasoningEffort string
	providers       []ProviderConfig
	active          string
}

type stubStrictModelAgent struct {
	stubModelModeAgent
	models []ModelOption
	calls  int
}

type stubLiveModeSession struct {
	stubAgentSession
	modes []string
}

func (s *stubLiveModeSession) SetLiveMode(mode string) bool {
	s.modes = append(s.modes, mode)
	return true
}

func (a *stubModelModeAgent) SetModel(model string) {
	a.model = model
}

func (a *stubModelModeAgent) GetModel() string {
	return a.model
}

func (a *stubModelModeAgent) AvailableModels(_ context.Context) []ModelOption {
	return []ModelOption{
		{Name: "gpt-4.1", Desc: "Balanced", Alias: "gpt"},
		{Name: "gpt-4.1-mini", Desc: "Fast"},
	}
}

func (a *stubStrictModelAgent) AvailableModels(_ context.Context) []ModelOption {
	a.calls++
	return append([]ModelOption(nil), a.models...)
}

func (a *stubModelModeAgent) SetProviders(providers []ProviderConfig) {
	a.providers = providers
}

func (a *stubModelModeAgent) GetActiveProvider() *ProviderConfig {
	for i := range a.providers {
		if a.providers[i].Name == a.active {
			return &a.providers[i]
		}
	}
	return nil
}

func (a *stubModelModeAgent) ListProviders() []ProviderConfig {
	result := make([]ProviderConfig, len(a.providers))
	copy(result, a.providers)
	return result
}

func (a *stubModelModeAgent) SetActiveProvider(name string) bool {
	if name == "" {
		a.active = ""
		return true
	}
	for _, prov := range a.providers {
		if prov.Name == name {
			a.active = name
			return true
		}
	}
	return false
}

func (a *stubModelModeAgent) SetMode(mode string) {
	a.mode = mode
}

func (a *stubModelModeAgent) GetMode() string {
	if a.mode == "" {
		return "default"
	}
	return a.mode
}

func (a *stubModelModeAgent) PermissionModes() []PermissionModeInfo {
	return []PermissionModeInfo{
		{Key: "default", Name: "Default", Desc: "Ask before risky actions"},
		{Key: "yolo", Name: "YOLO", Desc: "Skip confirmations"},
	}
}

func (a *stubModelModeAgent) SetReasoningEffort(effort string) {
	a.reasoningEffort = effort
}

func (a *stubModelModeAgent) GetReasoningEffort() string {
	return a.reasoningEffort
}

func (a *stubModelModeAgent) AvailableReasoningEfforts() []string {
	return []string{"low", "medium", "high", "xhigh"}
}

type namedStubModelModeAgent struct {
	stubModelModeAgent
	name string
}

func (a *namedStubModelModeAgent) Name() string {
	if a.name == "" {
		return "named-stub-model"
	}
	return a.name
}

type namedStubWorkspaceOptionAgent struct {
	namedStubModelModeAgent
	opts      map[string]any
	runAsUser string
	runAsEnv  []string
}

func (a *namedStubWorkspaceOptionAgent) WorkspaceAgentOptions() map[string]any {
	out := make(map[string]any, len(a.opts))
	for k, v := range a.opts {
		out[k] = v
	}
	return out
}

func (a *namedStubWorkspaceOptionAgent) GetRunAsUser() string { return a.runAsUser }

func (a *namedStubWorkspaceOptionAgent) GetRunAsEnv() []string {
	if len(a.runAsEnv) == 0 {
		return nil
	}
	out := make([]string, len(a.runAsEnv))
	copy(out, a.runAsEnv)
	return out
}

type stubWorkDirAgent struct {
	stubAgent
	workDir string
}

func (a *stubWorkDirAgent) SetWorkDir(dir string) {
	a.workDir = dir
}

func (a *stubWorkDirAgent) GetWorkDir() string {
	return a.workDir
}

type namedStubWorkDirAgent struct {
	stubWorkDirAgent
	name string
}

func (a *namedStubWorkDirAgent) Name() string {
	if a.name == "" {
		return "named-stub-workdir"
	}
	return a.name
}

type stubListAgent struct {
	stubAgent
	sessions []AgentSessionInfo
}

func (a *stubListAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	return a.sessions, nil
}

type stubDeleteAgent struct {
	stubListAgent
	deleted []string
	errByID map[string]error
}

func (a *stubDeleteAgent) DeleteSession(_ context.Context, sessionID string) error {
	if err := a.errByID[sessionID]; err != nil {
		return err
	}
	a.deleted = append(a.deleted, sessionID)
	return nil
}

// waitDeleteModePhase polls the delete-mode state for the given session key
// until it reaches the target phase or the timeout expires.
func waitDeleteModePhase(t *testing.T, e *Engine, sessionKey, targetPhase string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		dm := e.getDeleteModeState(sessionKey)
		if dm != nil && dm.phase == targetPhase {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for delete mode phase %q", targetPhase)
}

type stubProviderAgent struct {
	stubAgent
	providers []ProviderConfig
	active    string
}

func (a *stubProviderAgent) ListProviders() []ProviderConfig {
	return a.providers
}

func (a *stubProviderAgent) SetProviders(providers []ProviderConfig) {
	a.providers = providers
}

func (a *stubProviderAgent) GetActiveProvider() *ProviderConfig {
	for i := range a.providers {
		if a.providers[i].Name == a.active {
			return &a.providers[i]
		}
	}
	return nil
}

func (a *stubProviderAgent) SetActiveProvider(name string) bool {
	if name == "" {
		a.active = ""
		return true
	}
	for _, prov := range a.providers {
		if prov.Name == name {
			a.active = name
			return true
		}
	}
	return false
}

type stubUsageAgent struct {
	stubAgent
	report *UsageReport
	err    error
}

func (a *stubUsageAgent) GetUsage(_ context.Context) (*UsageReport, error) {
	return a.report, a.err
}

type stubReplyFooterAgent struct {
	stubModelModeAgent
	workDir string
	report  *UsageReport
	err     error
}

func (a *stubReplyFooterAgent) SetWorkDir(dir string) {
	a.workDir = dir
}

func (a *stubReplyFooterAgent) GetWorkDir() string {
	return a.workDir
}

func (a *stubReplyFooterAgent) GetUsage(_ context.Context) (*UsageReport, error) {
	return a.report, a.err
}

func newTestEngine() *Engine {
	return NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, "")
}

func TestReceiveMessage_SerialKeyQueuesIndependentSessions(t *testing.T) {
	agent := &serialBlockingAgent{}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.ReceiveMessage(p, &Message{SessionKey: "test:group:user-a", SerialKey: "test:group:1", Platform: "test", MessageID: "a", Content: "first"})
	deadline := time.After(time.Second)
	for agent.count() != 1 {
		select {
		case <-deadline:
			t.Fatalf("started sessions = %d, want 1", agent.count())
		case <-time.After(time.Millisecond):
		}
	}
	first := agent.session(0)
	select {
	case <-first.sends:
	case <-time.After(time.Second):
		t.Fatal("first message was not sent to agent")
	}

	e.ReceiveMessage(p, &Message{SessionKey: "test:group:user-b", SerialKey: "test:group:1", Platform: "test", MessageID: "b", Content: "second"})
	if got := p.getSent(); len(got) == 0 || got[len(got)-1] != e.i18n.T(MsgMessageQueued) {
		t.Fatalf("queued message notification = %#v, want %q", got, e.i18n.T(MsgMessageQueued))
	}
	if agent.count() != 1 {
		t.Fatalf("second independent session started before first finished: %d", agent.count())
	}

	first.events <- Event{Type: EventResult, Content: "done", Done: true}
	deadline = time.After(time.Second)
	for agent.count() != 2 {
		select {
		case <-deadline:
			t.Fatal("second message did not start after first completed")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-agent.session(1).sends:
	case <-time.After(time.Second):
		t.Fatal("second message was not sent to agent")
	}
}

// stubProactiveSendPlatform implements ReplyContextReconstruct for proactive
// SendToSessionWithAttachments when there is no interactive session.
type stubProactiveSendPlatform struct {
	stubMediaPlatform
	reconstructKey string
}

func (p *stubProactiveSendPlatform) ReconstructReplyCtx(sessionKey string) (any, error) {
	p.reconstructKey = sessionKey
	return "proactive-rctx", nil
}

func TestEngineStart_DefersAsyncPlatformReadyInitialization(t *testing.T) {
	p := &stubLifecyclePlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddCommand("help", "help", "", "", "", "test")

	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if p.handler == nil {
		t.Fatal("lifecycle handler not installed")
	}
	if p.registerCalls != 0 {
		t.Fatalf("registerCalls = %d, want 0 before ready", p.registerCalls)
	}
	if p.cardNavSetCalls != 0 {
		t.Fatalf("cardNavSetCalls = %d, want 0 before ready", p.cardNavSetCalls)
	}
}

func TestEngine_OnPlatformReady_IsIdempotentUntilUnavailable(t *testing.T) {
	p := &stubLifecyclePlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddCommand("help", "help", "", "", "", "test")

	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	e.OnPlatformReady(p)
	e.OnPlatformReady(p)

	if p.registerCalls != 1 {
		t.Fatalf("registerCalls = %d, want 1", p.registerCalls)
	}
	if p.cardNavSetCalls != 1 {
		t.Fatalf("cardNavSetCalls = %d, want 1", p.cardNavSetCalls)
	}

	e.OnPlatformUnavailable(p, errors.New("lost"))
	e.OnPlatformReady(p)

	if p.registerCalls != 2 {
		t.Fatalf("registerCalls after recover = %d, want 2", p.registerCalls)
	}
}

func TestEngine_OnPlatformUnavailable_IsIdempotent(t *testing.T) {
	p := &stubLifecyclePlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddCommand("help", "help", "", "", "", "test")

	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	e.OnPlatformReady(p)
	e.OnPlatformUnavailable(p, errors.New("lost"))
	e.OnPlatformUnavailable(p, errors.New("lost-again"))
	e.OnPlatformReady(p)

	if p.registerCalls != 2 {
		t.Fatalf("registerCalls after duplicate unavailable = %d, want 2", p.registerCalls)
	}
}

func TestEngine_LifecycleCallbacksIgnoredAfterStopBegins(t *testing.T) {
	p := &stubLifecyclePlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddCommand("help", "help", "", "", "", "test")

	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := e.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	e.OnPlatformReady(p)
	e.OnPlatformUnavailable(p, errors.New("late"))

	if p.registerCalls != 0 {
		t.Fatalf("registerCalls = %d, want 0 after stop", p.registerCalls)
	}
}

func TestEngine_StopDoesNotWaitForBlockedPlatformCapabilityInit(t *testing.T) {
	p := newBlockingRegisterPlatform("telegram")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddCommand("help", "help", "", "", "", "test")

	if err := e.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	readyDone := make(chan struct{})
	go func() {
		e.OnPlatformReady(p)
		close(readyDone)
	}()

	select {
	case <-p.registerStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("RegisterCommands was not called")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- e.Stop()
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Stop blocked on platform capability initialization")
	}

	select {
	case <-p.stopCalled:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("platform Stop was not called while RegisterCommands was blocked")
	}

	close(p.allowRegister)

	select {
	case <-readyDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("OnPlatformReady did not finish after RegisterCommands was released")
	}
}

func TestProcessInteractiveEvents_StripsAgentFooterWhenEnabled(t *testing.T) {
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		Mode:             "full",
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
		HideAgentFooter:  true,
	})

	sessionKey := "telegram:user-agent-footer"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-agent-footer")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-agent-footer",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventText, Content: "answer\n\n*claude-opus-4-8[1m] · out 788 · in 442 cw 0 cr 395.1k · ctx 40%*"}
	agentSession.events <- Event{Type: EventResult, Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-agent-footer", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	if sent[0] != "answer" {
		t.Fatalf("final reply = %q, want %q", sent[0], "answer")
	}
	if got := session.GetHistory(0); len(got) != 1 || got[0].Content != "answer" {
		t.Fatalf("history = %#v, want filtered answer", got)
	}
}

func TestProcessInteractiveEvents_KeepsAgentFooterByDefault(t *testing.T) {
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	sessionKey := "telegram:user-agent-footer-default"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-agent-footer-default")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-agent-footer-default",
	}
	e.interactiveStates[sessionKey] = state

	body := "answer\n\n*claude-opus-4-8[1m] · out 788 · in 442 cw 0 cr 395.1k · ctx 40%*"
	agentSession.events <- Event{Type: EventText, Content: body}
	agentSession.events <- Event{Type: EventResult, Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-agent-footer-default", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	if sent[0] != body {
		t.Fatalf("final reply = %q, want %q", sent[0], body)
	}
}

func TestStripAgentFooterLines(t *testing.T) {
	input := "answer\n\n*gpt-5.5 · xhigh · out 864 in 177.7k cr 175.5k · ctx 69%*"
	if got, want := stripAgentFooterLines(input), "answer"; got != want {
		t.Fatalf("stripAgentFooterLines() = %q, want %q", got, want)
	}

	prose := "The words out 10 in 20 ctx 30% can appear in prose."
	if got := stripAgentFooterLines(prose); got != prose {
		t.Fatalf("stripAgentFooterLines() stripped prose: %q", got)
	}
}

// TestProcessInteractiveEvents_NonTerminalResultContinuesTurn pins issue #481:
// when Claude Code emits a mid-turn compaction result (Done=false), the engine
// must NOT treat it as turn completion. Subsequent EventText (analogous to a
// post-compaction assistant chunk) must still be observed, and the final
// EventResult{Done:true} is the one that finalizes the turn
// (noteUserTurnCompleted called exactly once, fullResponse sent).
func TestProcessInteractiveEvents_NonTerminalResultContinuesTurn(t *testing.T) {
	p := &stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	sessionKey := "test:user1"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s1")
	state := &interactiveState{
		agentSession:                   agentSession,
		platform:                       p,
		replyCtx:                       "ctx-1",
		currentTurnUserMessageTimeMs:   100,
		lastCompletedUserMessageTimeMs: 0,
	}
	e.interactiveStates[sessionKey] = state

	// Mid-turn compaction event: agent emits type:"result" with Done=false
	// when it triggers automatic context compaction. Content is empty.
	agentSession.events <- Event{
		Type:        EventResult,
		Content:     "",
		Done:        false,
		InputTokens: 50000,
		Metadata:    map[string]any{"compaction_continue": true},
	}

	// Post-compaction assistant chunk: must still be observed by the engine
	// loop (not dropped by an early return). We don't depend on tool-rendering
	// state for the regression contract — the fact that the loop processes
	// this event proves it kept running past the compaction event.
	agentSession.events <- Event{Type: EventText, Content: "after-compact-"}

	// Final terminal result.
	finalText := "turn done after compaction"
	agentSession.events <- Event{
		Type:    EventResult,
		Content: finalText,
		Done:    true,
	}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m1", time.Now(), nil, nil, nil)

	// noteUserTurnCompleted must have been called exactly once on the
	// terminal result, advancing the watermark to the in-flight message time.
	state.mu.Lock()
	gotCompleted := state.lastCompletedUserMessageTimeMs
	state.mu.Unlock()
	if gotCompleted != 100 {
		t.Fatalf("lastCompletedUserMessageTimeMs = %d, want 100 (noteUserTurnCompleted should run exactly once on terminal result)", gotCompleted)
	}

	// The final text must have been sent to the platform. The compaction
	// event must NOT have produced an empty reply.
	sent := p.getSent()
	if len(sent) == 0 {
		t.Fatalf("no message sent; want at least the final reply %q", finalText)
	}
	if sent[len(sent)-1] != finalText {
		t.Fatalf("last sent = %q, want %q (compaction empty reply must not leak, final reply must arrive)", sent[len(sent)-1], finalText)
	}
	for i, msg := range sent {
		if msg == "" {
			t.Fatalf("sent[%d] is empty — compaction must not produce an empty message; all sent=%v", i, sent)
		}
	}
}

func TestProcessInteractiveEvents_AppendsReplyFooterWhenEnabled(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	workDir := filepath.Join(homeDir, "codes", "agent-bridge")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}

	agent := &stubReplyFooterAgent{
		stubModelModeAgent: stubModelModeAgent{
			model:           "gpt-5.4",
			reasoningEffort: "xhigh",
		},
		workDir: workDir,
		report: &UsageReport{
			Buckets: []UsageBucket{{
				Name: "Rate limit",
				Windows: []UsageWindow{{
					Name:          "Primary",
					UsedPercent:   0,
					WindowSeconds: 18000,
				}},
			}},
		},
	}
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetReplyFooterEnabled(true)

	sessionKey := "telegram:user-footer"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-footer")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-footer",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventResult, Content: "answer", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-footer", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	want := "answer\n\n*gpt-5.4 · xhigh · 100% left · " + compactReplyFooterPath(workDir) + "*"
	if sent[0] != want {
		t.Fatalf("final reply = %q, want %q", sent[0], want)
	}
}

func TestProcessInteractiveEvents_AppendsContextIndicatorInsideReplyFooter(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)

	workDir := filepath.Join(homeDir, "code", "TechStudio", "projects", "core", "agents", "ceo")
	agent := &stubReplyFooterAgent{
		stubModelModeAgent: stubModelModeAgent{model: "glm-5.1"},
		workDir:            workDir,
	}
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetReplyFooterEnabled(true)

	sessionKey := "telegram:user-footer-context"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-footer-context")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-footer-context",
		agent:        agent,
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventResult, Content: "answer", InputTokens: 28000, Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-footer-context", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	want := "answer\n\n*[ctx: ~14%] · glm-5.1 · " + compactReplyFooterPath(workDir) + "*"
	if sent[0] != want {
		t.Fatalf("final reply = %q, want %q", sent[0], want)
	}
}

func TestProcessInteractiveEvents_ToolSegmentsKeepFinalFooter(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)

	workDir := filepath.Join(homeDir, "code", "TechStudio", "projects", "core", "agents", "ceo")
	agent := &stubReplyFooterAgent{
		stubModelModeAgent: stubModelModeAgent{model: "glm-5.1"},
		workDir:            workDir,
	}
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetReplyFooterEnabled(true)
	e.SetDisplayConfig(DisplayCfg{ThinkingMessages: true, ThinkingMaxLen: 300, ToolMaxLen: 500, ToolMessages: true})

	sessionKey := "telegram:user-tool-footer"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-tool-footer")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-tool-footer",
		agent:        agent,
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventText, Content: "先检查一下。"}
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "pwd"}
	agentSession.events <- Event{Type: EventText, Content: "已处理完成。"}
	agentSession.events <- Event{Type: EventResult, Content: "已处理完成。", InputTokens: 28000, Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-tool-footer", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) == 0 {
		t.Fatal("sent = nil, want final reply")
	}
	final := sent[len(sent)-1]
	want := "已处理完成。\n\n*[ctx: ~14%] · glm-5.1 · " + compactReplyFooterPath(workDir) + "*"
	if final != want {
		t.Fatalf("final reply = %q, want %q\nall sent = %#v", final, want, sent)
	}
}

func TestProcessInteractiveEvents_DropsStandaloneEllipsisProgress(t *testing.T) {
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{ThinkingMessages: true, ThinkingMaxLen: 300, ToolMaxLen: 500, ToolMessages: true})

	sessionKey := "telegram:user-ellipsis"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-ellipsis")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-ellipsis",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventThinking, Content: "..."}
	agentSession.events <- Event{Type: EventText, Content: "..."}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-ellipsis", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 || sent[0] != "done" {
		t.Fatalf("sent = %#v, want only final answer without standalone ellipsis progress", sent)
	}
}

func TestProcessInteractiveEvents_AddsDoneReactionAfterNormalReply(t *testing.T) {
	p := &stubDoneReactionPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	sessionKey := "dingtalk:user-done"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-done")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-done",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-done", time.Now(), nil, nil, state.replyCtx)

	count, ctxs := p.doneSnapshot()
	if count != 1 {
		t.Fatalf("done reactions = %d, want 1", count)
	}
	if len(ctxs) != 1 || ctxs[0] != "ctx-done" {
		t.Fatalf("done reaction contexts = %#v, want [ctx-done]", ctxs)
	}
}

func TestProcessInteractiveEvents_DoesNotAppendReplyFooterWhenDisabled(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	agent := &stubReplyFooterAgent{
		stubModelModeAgent: stubModelModeAgent{
			model:           "gpt-5.4",
			reasoningEffort: "xhigh",
		},
		workDir: filepath.Join(homeDir, "codes", "agent-bridge"),
		report: &UsageReport{
			Buckets: []UsageBucket{{
				Name: "Rate limit",
				Windows: []UsageWindow{{
					Name:          "Primary",
					UsedPercent:   0,
					WindowSeconds: 18000,
				}},
			}},
		},
	}
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetReplyFooterEnabled(false)

	sessionKey := "telegram:user-footer-off"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-footer-off")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-footer-off",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventResult, Content: "answer", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-footer-off", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	if sent[0] != "answer" {
		t.Fatalf("final reply = %q, want plain answer without footer", sent[0])
	}
}

func TestProcessInteractiveEvents_ReplyFooterPrefersSessionRuntimeState(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	if err := os.MkdirAll(filepath.Join(homeDir, "codes", "agent-bridge"), 0o755); err != nil {
		t.Fatal(err)
	}

	agent := &stubReplyFooterAgent{
		stubModelModeAgent: stubModelModeAgent{
			model:           "agent-model",
			reasoningEffort: "medium",
		},
		workDir: filepath.Join(homeDir, "codes", "agent-default"),
		report: &UsageReport{
			Buckets: []UsageBucket{{
				Name: "Rate limit",
				Windows: []UsageWindow{{
					Name:          "Primary",
					UsedPercent:   80,
					WindowSeconds: 18000,
				}},
			}},
		},
	}
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetReplyFooterEnabled(true)

	sessionKey := "telegram:user-footer-runtime"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-footer-runtime")
	agentSession.model = "gpt-5.4"
	agentSession.reasoningEffort = "xhigh"
	sessionWorkDir := filepath.Join(homeDir, "codes", "agent-bridge")
	agentSession.workDir = sessionWorkDir
	agentSession.report = &UsageReport{
		Buckets: []UsageBucket{{
			Name: "Rate limit",
			Windows: []UsageWindow{{
				Name:          "Primary",
				UsedPercent:   0,
				WindowSeconds: 18000,
			}},
		}},
	}
	agentSession.contextUsage = &ContextUsage{
		UsedTokens:     181424,
		BaselineTokens: 12000,
		TotalTokens:    50821769,
		ContextWindow:  258400,
	}
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-footer-runtime",
		agent:        agent,
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventResult, Content: "answer", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-footer-runtime", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	want := "answer\n\n*gpt-5.4 · xhigh · 31% left · " + compactReplyFooterPath(sessionWorkDir) + "*"
	if sent[0] != want {
		t.Fatalf("final reply = %q, want %q", sent[0], want)
	}
}

// Regression: an agent that only exposes a workdir (no model/effort/usage)
// must not emit a footer at all. Previously this produced a footer like
// "*~*" when the agent was running in the user's home directory, which
// rendered as a bare "~" on Feishu/Weixin.
func TestProcessInteractiveEvents_SuppressesReplyFooterWhenOnlyWorkDir(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	agent := &stubWorkDirAgent{workDir: homeDir}
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetReplyFooterEnabled(true)

	sessionKey := "telegram:user-footer-workdir-only"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-footer-workdir-only")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-footer-workdir-only",
		agent:        agent,
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventResult, Content: "answer", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-footer-workdir-only", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	if sent[0] != "answer" {
		t.Fatalf("final reply = %q, want plain answer without footer", sent[0])
	}
}

func TestProcessInteractiveEvents_HiddenToolProgressKeepsPreviewOnFinalize(t *testing.T) {
	p := &mockKeepPreviewPlatform{}
	p.n = "feishu"
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{ThinkingMessages: true, ThinkingMaxLen: 300, ToolMaxLen: 500, ToolMessages: false})
	sessionKey := "test:user1"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s1")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-1",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventText, Content: "final response"}
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "echo hi"}
	agentSession.events <- Event{Type: EventResult, Content: "", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m1", time.Now(), nil, nil, nil)

	if got := p.getSent(); len(got) != 0 {
		t.Fatalf("sent text = %#v, want no plain-text fallback sends", got)
	}

	p.mu.Lock()
	deletedCount := len(p.deleted)
	previewMsgs := append([]string(nil), p.messages...)
	p.mu.Unlock()

	if deletedCount != 0 {
		t.Fatalf("deleted previews = %d, want 0", deletedCount)
	}
	if len(previewMsgs) == 0 || previewMsgs[len(previewMsgs)-1] != "update:final response" {
		t.Fatalf("preview messages = %#v, want in-place final update", previewMsgs)
	}
}

func TestProcessInteractiveEvents_ToolMessagesDisabledSuppressesToolProgressOnly(t *testing.T) {
	p := &stubPlatformEngine{n: "telegram"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{ThinkingMessages: true, ThinkingMaxLen: 300, ToolMaxLen: 500, ToolMessages: false})
	sessionKey := "telegram:user1"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s1")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-1",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventThinking, Content: "planning"}
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "echo hi"}
	agentSession.events <- Event{Type: EventToolResult, ToolName: "Bash", ToolResult: "hi"}
	agentSession.events <- Event{Type: EventText, Content: "done"}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m1", time.Now(), nil, nil, nil)

	sent := p.getSent()
	if len(sent) < 1 || len(sent) > 2 {
		t.Fatalf("sent = %#v, want final response with optional standalone thinking message", sent)
	}
	for _, msg := range sent {
		if strings.Contains(msg, "Bash") || strings.Contains(msg, "echo hi") || msg == "hi" {
			t.Fatalf("tool progress should stay hidden, got %q", msg)
		}
	}
	if len(sent) == 2 && !strings.Contains(sent[0], "planning") {
		t.Fatalf("thinking message = %q, want planning", sent[0])
	}
	if sent[len(sent)-1] != "done" {
		t.Fatalf("final message = %q, want done", sent[len(sent)-1])
	}
}

func TestProcessInteractiveEvents_CompactProgressCoalescesThinkingAndToolUse(t *testing.T) {
	p := &stubCompactProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	sessionKey := "feishu:user1"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s1")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-compact",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventThinking, Content: "Thinking about command"}
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "pwd"}
	agentSession.events <- Event{Type: EventText, Content: "done"}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m1", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 || sent[0] != "done" {
		t.Fatalf("sent = %#v, want only final assistant reply", sent)
	}

	starts := p.getPreviewStarts()
	if len(starts) != 1 {
		t.Fatalf("preview starts = %d, want 1", len(starts))
	}
	if !strings.Contains(starts[0], "Thinking") {
		t.Fatalf("start preview should contain thinking text, got %q", starts[0])
	}

	edits := p.getPreviewEdits()
	if len(edits) != 1 {
		t.Fatalf("preview edits = %d, want 1", len(edits))
	}
	if !strings.Contains(edits[0], "pwd") {
		t.Fatalf("updated preview should contain tool input, got %q", edits[0])
	}
}

func TestProcessInteractiveEvents_CardProgressUsesCardTemplate(t *testing.T) {
	p := &stubCompactProgressPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		style:              "card",
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	sessionKey := "feishu:user2"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s2")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-card",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventThinking, Content: "Plan first"}
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "echo hi"}
	agentSession.events <- Event{Type: EventText, Content: "done"}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m2", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 || sent[0] != "done" {
		t.Fatalf("sent = %#v, want only final assistant reply", sent)
	}

	starts := p.getPreviewStarts()
	if len(starts) != 1 {
		t.Fatalf("preview starts = %d, want 1", len(starts))
	}
	if !strings.Contains(starts[0], "**Progress**") {
		t.Fatalf("start preview should contain fallback progress title, got %q", starts[0])
	}
	if !strings.Contains(starts[0], "1.") {
		t.Fatalf("start preview should contain first item index, got %q", starts[0])
	}

	edits := p.getPreviewEdits()
	if len(edits) != 1 {
		t.Fatalf("preview edits = %d, want 1", len(edits))
	}
	if !strings.Contains(edits[0], "2.") {
		t.Fatalf("updated preview should contain second item index, got %q", edits[0])
	}
	if !strings.Contains(edits[0], "echo hi") {
		t.Fatalf("updated preview should contain tool command, got %q", edits[0])
	}
}

func TestProcessInteractiveEvents_FinalReplyUsesWorkspaceForReferenceRendering(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("TransformLocalReferences path handling assumes Unix separators")
	}
	p := &stubPlatformEngine{n: "feishu"}
	a := &namedStubModelModeAgent{name: "codex"}
	e := NewEngine("test", a, []Platform{p}, "")
	e.SetReferenceConfig(ReferenceRenderCfg{
		NormalizeAgents: []string{"codex"},
		RenderPlatforms: []string{"feishu"},
		DisplayPath:     "relative",
		MarkerStyle:     "emoji",
		EnclosureStyle:  "code",
	})

	sessionKey := "feishu:user-relative"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-relative")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-relative",
		workspaceDir: "/root/code",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{
		Type:    EventResult,
		Content: "/root/code/demo-repo/src/services/user_profile_service.ts:42",
		Done:    true,
	}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-relative", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	if got := sent[0]; got != "`demo-repo/src/services/user_profile_service.ts:42`" {
		t.Fatalf("final reply = %q, want workspace-relative rendered reference", got)
	}
}

func TestProcessInteractiveEvents_FinalReplyRemainsRawWhenReferencesDisabled(t *testing.T) {
	p := &stubPlatformEngine{n: "feishu"}
	a := &namedStubModelModeAgent{name: "codex"}
	e := NewEngine("test", a, []Platform{p}, "")
	e.SetReferenceConfig(ReferenceRenderCfg{
		NormalizeAgents: []string{},
		RenderPlatforms: []string{"feishu"},
		DisplayPath:     "relative",
		MarkerStyle:     "emoji",
		EnclosureStyle:  "code",
	})

	sessionKey := "feishu:user-relative-raw"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-relative-raw")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-relative-raw",
		workspaceDir: "/root/code/demo",
	}
	e.interactiveStates[sessionKey] = state

	raw := "Check [/root/code/demo/ui/recovery_contact_form.tsx](/root/code/demo/ui/recovery_contact_form.tsx) and /root/code/demo/ui/recovery_contact_form.tsx:11"
	agentSession.events <- Event{
		Type:    EventResult,
		Content: raw,
		Done:    true,
	}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-relative-raw", time.Now(), nil, nil, state.replyCtx)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %#v, want one final reply", sent)
	}
	if got := sent[0]; got != raw {
		t.Fatalf("final reply = %q, want raw unchanged content %q", got, raw)
	}
}

func TestProcessInteractiveEvents_CardProgressUsesStructuredPayloadWhenSupported(t *testing.T) {
	p := &stubCompactProgressPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		style:              "card",
		supportPayload:     true,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	sessionKey := "feishu:user3"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s3")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-card-structured",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventThinking, Content: "Plan first"}
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "echo hi"}
	agentSession.events <- Event{Type: EventText, Content: "done"}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m3", time.Now(), nil, nil, state.replyCtx)

	starts := p.getPreviewStarts()
	if len(starts) != 1 {
		t.Fatalf("preview starts = %d, want 1", len(starts))
	}
	if !strings.HasPrefix(starts[0], ProgressCardPayloadPrefix) {
		t.Fatalf("start preview should be structured payload, got %q", starts[0])
	}
	startPayload, ok := ParseProgressCardPayload(starts[0])
	if !ok {
		t.Fatalf("start preview should parse as structured payload, got %q", starts[0])
	}
	if len(startPayload.Items) != 1 {
		t.Fatalf("start payload items = %d, want 1", len(startPayload.Items))
	}
	if startPayload.Items[0].Kind != ProgressEntryThinking {
		t.Fatalf("start payload kind = %q, want %q", startPayload.Items[0].Kind, ProgressEntryThinking)
	}
	if startPayload.State != ProgressCardStateRunning {
		t.Fatalf("start payload state = %q, want %q", startPayload.State, ProgressCardStateRunning)
	}

	edits := p.getPreviewEdits()
	if len(edits) != 2 {
		t.Fatalf("preview edits = %d, want 2", len(edits))
	}
	updatePayload, ok := ParseProgressCardPayload(edits[0])
	if !ok {
		t.Fatalf("update preview should parse as structured payload, got %q", edits[0])
	}
	if len(updatePayload.Items) != 2 {
		t.Fatalf("update payload items = %d, want 2", len(updatePayload.Items))
	}
	if !strings.Contains(updatePayload.Items[1].Text, "echo hi") {
		t.Fatalf("second payload item should contain tool command, got %q", updatePayload.Items[1].Text)
	}

	finalPayload, ok := ParseProgressCardPayload(edits[1])
	if !ok {
		t.Fatalf("final preview should parse as structured payload, got %q", edits[1])
	}
	if finalPayload.State != ProgressCardStateCompleted {
		t.Fatalf("final payload state = %q, want %q", finalPayload.State, ProgressCardStateCompleted)
	}
}

// TestProcessInteractiveEvents_CardProgressTruncatesToolInputByToolMaxLen verifies
// that when progress_style=card is used (e.g. Feishu), a long tool input is
// truncated to display.ToolMaxLen in the structured card payload. The original
// event.ToolInput must remain unmutated.
func TestProcessInteractiveEvents_CardProgressTruncatesToolInputByToolMaxLen(t *testing.T) {
	p := &stubCompactProgressPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		style:              "card",
		supportPayload:     true,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       50,
		ToolMessages:     true,
		Mode:             "full",
	})
	sessionKey := "feishu:user-card-truncate"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-card-truncate")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-card-truncate",
	}
	e.interactiveStates[sessionKey] = state

	longInput := strings.Repeat("abcdefghij", 12) // 120 chars
	agentSession.events <- Event{Type: EventThinking, Content: "Plan"}
	toolEvt := Event{Type: EventToolUse, ToolName: "Bash", ToolInput: longInput}
	agentSession.events <- toolEvt
	agentSession.events <- Event{Type: EventText, Content: "done"}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-card-truncate", time.Now(), nil, nil, state.replyCtx)

	if toolEvt.ToolInput != longInput {
		t.Fatalf("event.ToolInput was mutated: got len=%d, want len=%d", len(toolEvt.ToolInput), len(longInput))
	}

	edits := p.getPreviewEdits()
	var toolUseText string
	for _, edit := range edits {
		payload, ok := ParseProgressCardPayload(edit)
		if !ok {
			continue
		}
		for _, item := range payload.Items {
			if item.Kind == ProgressEntryToolUse {
				toolUseText = item.Text
			}
		}
	}
	if toolUseText == "" {
		t.Fatalf("no ProgressEntryToolUse found in any card edit; edits=%v", edits)
	}
	if utf8.RuneCountInString(toolUseText) > 50+len("...") {
		t.Fatalf("card tool input not truncated: runeCount=%d, content=%q", utf8.RuneCountInString(toolUseText), toolUseText)
	}
	if !strings.HasSuffix(toolUseText, "...") {
		t.Fatalf("expected truncated tool input to end with '...', got %q", toolUseText)
	}
	if !strings.HasPrefix(toolUseText, "abcdefghij") {
		t.Fatalf("expected truncated tool input to start with original prefix, got %q", toolUseText)
	}
}

func TestProcessInteractiveEvents_RichCardShowsThinkingContent(t *testing.T) {
	p := &stubCompactProgressPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		style:              "card",
		supportPayload:     true,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
		Mode:             "full",
		CardMode:         "rich",
	})
	sessionKey := "feishu:user-rich-thinking"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-rich-thinking")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-rich-thinking",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventThinking, Content: "Inspecting event routing"}
	agentSession.events <- Event{Type: EventText, Content: "answer"}
	agentSession.events <- Event{Type: EventResult, Content: "answer", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-rich-thinking", time.Now(), nil, nil, state.replyCtx)

	starts := p.getPreviewStarts()
	if len(starts) != 1 {
		t.Fatalf("preview starts = %d, want 1", len(starts))
	}
	if !strings.Contains(starts[0], "Inspecting event routing") {
		t.Fatalf("rich card start should contain thinking content, got %q", starts[0])
	}
}

func TestProcessInteractiveEvents_RichCardCoalescesToolResult(t *testing.T) {
	p := &stubCompactProgressPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		style:              "card",
		supportPayload:     true,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
		Mode:             "full",
		CardMode:         "rich",
	})
	sessionKey := "feishu:user-rich-tool-result"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-rich-tool-result")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-rich-tool-result",
	}
	e.interactiveStates[sessionKey] = state

	code := 0
	success := true
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "echo hi"}
	agentSession.events <- Event{Type: EventToolResult, ToolName: "Bash", ToolResult: "hi", ToolStatus: "completed", ToolExitCode: &code, ToolSuccess: &success}
	agentSession.events <- Event{Type: EventText, Content: "done"}
	agentSession.events <- Event{Type: EventResult, Content: "done", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-rich-tool-result", time.Now(), nil, nil, state.replyCtx)

	starts := p.getPreviewStarts()
	if len(starts) != 1 {
		t.Fatalf("preview starts = %d, want only the rich card start and no separate progress card", len(starts))
	}
	rendered := strings.Join(append(starts, p.getPreviewEdits()...), "\n")
	for _, want := range []string{"echo hi", "completed", "hi"} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rich card should contain %q, got %q", want, rendered)
		}
	}
}

// stubRichCardSilentPlatform implements the full set of rich-card optional
// interfaces (RichCardSupporter, PreviewStarter, MessageUpdater,
// RichCardTextStreamer, PreviewCleaner) and tracks every call so tests can
// assert that NO_REPLY in rich card mode leaves zero footprint.
type stubRichCardSilentPlatform struct {
	stubPlatformEngine
	mu            sync.Mutex
	previewStarts []string
	streamTexts   []string
	updates       []string
	deleteCount   int
	nextHandleSeq int
}

func (p *stubRichCardSilentPlatform) BuildRichCard(status CardStatus, _ string, steps []ToolStep, markdown string, _ bool, _ string) string {
	return fmt.Sprintf("rich:status=%s steps=%d body=%q", status, len(steps), markdown)
}

func (p *stubRichCardSilentPlatform) SendPreviewStart(_ context.Context, _ any, content string) (any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.previewStarts = append(p.previewStarts, content)
	p.nextHandleSeq++
	return fmt.Sprintf("handle-%d", p.nextHandleSeq), nil
}

func (p *stubRichCardSilentPlatform) UpdateMessage(_ context.Context, _ any, content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates = append(p.updates, content)
	return nil
}

func (p *stubRichCardSilentPlatform) StreamRichCardText(_ context.Context, _ any, fullText string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.streamTexts = append(p.streamTexts, fullText)
	return nil
}

func (p *stubRichCardSilentPlatform) DeletePreviewMessage(_ context.Context, _ any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleteCount++
	return nil
}

func (p *stubRichCardSilentPlatform) snapshot() (starts, streams, updates []string, deletes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	starts = append(starts, p.previewStarts...)
	streams = append(streams, p.streamTexts...)
	updates = append(updates, p.updates...)
	deletes = p.deleteCount
	return
}

type stubRichCardResolverPlatform struct {
	*stubRichCardSilentPlatform
	resolverMu sync.Mutex
	calls      []bool
}

func (p *stubRichCardResolverPlatform) ResolveRichCardMarkdown(_ context.Context, markdown string, final bool) string {
	p.resolverMu.Lock()
	p.calls = append(p.calls, final)
	p.resolverMu.Unlock()
	return strings.ReplaceAll(markdown, "https://example.com/chart.png", "img_v3_chart")
}

func (p *stubRichCardResolverPlatform) resolverCallModes() []bool {
	p.resolverMu.Lock()
	defer p.resolverMu.Unlock()
	out := make([]bool, len(p.calls))
	copy(out, p.calls)
	return out
}

func TestProcessInteractiveEvents_RichCardResolvesMarkdownImages(t *testing.T) {
	p := &stubRichCardResolverPlatform{
		stubRichCardSilentPlatform: &stubRichCardSilentPlatform{
			stubPlatformEngine: stubPlatformEngine{n: "feishu"},
		},
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		Mode:             "full",
		CardMode:         "rich",
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
	})
	sessionKey := "feishu:user-rich-image-resolver"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-rich-image-resolver")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-rich-image-resolver",
	}
	e.interactiveStates[sessionKey] = state

	body := "see ![chart](https://example.com/chart.png)"
	agentSession.events <- Event{Type: EventText, Content: body}
	agentSession.events <- Event{Type: EventResult, Content: body, Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-rich-image-resolver", time.Now(), nil, nil, state.replyCtx)
	_, streams, updates, _ := p.snapshot()
	rendered := strings.Join(append(streams, updates...), "\n")
	if !strings.Contains(rendered, "![chart](img_v3_chart)") {
		t.Fatalf("rich card output should contain resolved image key, got %q", rendered)
	}
	if strings.Contains(rendered, "https://example.com/chart.png") {
		t.Fatalf("rich card output should not contain unresolved remote URL, got %q", rendered)
	}
	modes := p.resolverCallModes()
	if len(modes) == 0 {
		t.Fatalf("expected resolver to be called")
	}
	hasStreamingCall := false
	hasFinalCall := false
	for _, final := range modes {
		if final {
			hasFinalCall = true
		} else {
			hasStreamingCall = true
		}
	}
	if !hasStreamingCall || !hasFinalCall {
		t.Fatalf("resolver call final flags = %v, want both streaming=false and final=true calls", modes)
	}
}

// runRichCardSilentScenario exercises processInteractiveEvents in rich
// (Card 2.0) mode, sending the given EventText chunks followed by a terminal
// EventResult. Returns call counts so each test case can assert the no-trace
// invariant for the (chunk shape, final content) combination.
func runRichCardSilentScenario(t *testing.T, name string, chunks []string, finalContent string) (starts, streams, updates []string, deletes int) {
	t.Helper()
	p := &stubRichCardSilentPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		Mode:             "full",
		CardMode:         "rich",
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
	})
	sessionKey := "feishu:user-rich-silent-" + name
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-rich-silent-" + name)
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-rich-silent-" + name,
	}
	e.interactiveStates[sessionKey] = state

	for _, chunk := range chunks {
		agentSession.events <- Event{Type: EventText, Content: chunk}
	}
	agentSession.events <- Event{Type: EventResult, Content: finalContent, Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-rich-silent-"+name, time.Now(), nil, nil, state.replyCtx)
	return p.snapshot()
}

// TestProcessInteractiveEvents_RichCard_NoReplySingleChunk asserts that a
// single-chunk NO_REPLY response in rich card mode leaves zero trace: no
// preview card created, no streaming text update, no card deletion. Lark
// would otherwise render the Send-then-Delete lifecycle as a "撤回了一条消息"
// gray bar.
func TestProcessInteractiveEvents_RichCard_NoReplySingleChunk(t *testing.T) {
	starts, streams, updates, deletes := runRichCardSilentScenario(t, "single", []string{"NO_REPLY"}, "NO_REPLY")
	if len(starts) != 0 {
		t.Fatalf("expected no SendPreviewStart, got %d: %v", len(starts), starts)
	}
	if len(streams) != 0 {
		t.Fatalf("expected no StreamRichCardText, got %d: %v", len(streams), streams)
	}
	if len(updates) != 0 {
		t.Fatalf("expected no UpdateMessage, got %d: %v", len(updates), updates)
	}
	if deletes != 0 {
		t.Fatalf("expected no DeletePreviewMessage, got %d", deletes)
	}
}

// TestProcessInteractiveEvents_RichCard_NoReplyChunked asserts the same
// no-trace invariant when the agent emits NO_REPLY across two text chunks
// ("NO_R" + "EPLY"). The silentHold gate must hold across chunks until the
// segment proves it is no longer a NO_REPLY prefix (or stays one).
func TestProcessInteractiveEvents_RichCard_NoReplyChunked(t *testing.T) {
	starts, streams, updates, deletes := runRichCardSilentScenario(t, "chunked", []string{"NO_R", "EPLY"}, "NO_REPLY")
	if len(starts) != 0 {
		t.Fatalf("expected no SendPreviewStart, got %d: %v", len(starts), starts)
	}
	if len(streams) != 0 {
		t.Fatalf("expected no StreamRichCardText, got %d: %v", len(streams), streams)
	}
	if len(updates) != 0 {
		t.Fatalf("expected no UpdateMessage, got %d: %v", len(updates), updates)
	}
	if deletes != 0 {
		t.Fatalf("expected no DeletePreviewMessage, got %d", deletes)
	}
}

// TestProcessInteractiveEvents_RichCard_PrefixThenContent verifies that a
// stream which starts with a NO_REPLY prefix ("N") but continues into real
// content ("ote that...") releases the silentHold and lazily creates the
// preview card with the accumulated content already in the body. No recall.
func TestProcessInteractiveEvents_RichCard_PrefixThenContent(t *testing.T) {
	starts, _, _, deletes := runRichCardSilentScenario(t, "prefix-then-content", []string{"N", "ote that the answer is 42"}, "Note that the answer is 42")
	if len(starts) != 1 {
		t.Fatalf("expected exactly 1 SendPreviewStart (lazy create after release), got %d: %v", len(starts), starts)
	}
	if !strings.Contains(starts[0], "Note that the answer is 42") {
		t.Fatalf("lazy-created card should contain accumulated content, got %q", starts[0])
	}
	if deletes != 0 {
		t.Fatalf("expected no DeletePreviewMessage, got %d", deletes)
	}
}

// TestProcessInteractiveEvents_RichCard_TextThenNoReply_PreservesBody verifies
// that when the agent emits visible text and then a trailing NO_REPLY marker
// (engine sees EventResult.Content = "NO_REPLY", the dominant case for
// claudecode where Content is the final assistant block), the card finalizes
// with the pre-NO_REPLY text preserved instead of being blanked. Without this
// the silent path's finalize-to-Done would overwrite the already-streamed body
// with empty string, making the user's just-seen content "disappear".
func TestProcessInteractiveEvents_RichCard_TextThenNoReply_PreservesBody(t *testing.T) {
	p := &stubRichCardSilentPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		Mode:             "full",
		CardMode:         "rich",
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
	})
	sessionKey := "feishu:user-rich-text-then-noreply"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-rich-text-then-noreply")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-rich-text-then-noreply",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventText, Content: "Hello world"}
	agentSession.events <- Event{Type: EventText, Content: "\nNO_REPLY"}
	agentSession.events <- Event{Type: EventResult, Content: "NO_REPLY", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-rich-text-then-noreply", time.Now(), nil, nil, state.replyCtx)
	starts, _, updates, deletes := p.snapshot()

	if len(starts) == 0 {
		t.Fatalf("expected SendPreviewStart for the visible text chunk, got 0")
	}
	if deletes != 0 {
		t.Fatalf("expected no DeletePreviewMessage, got %d", deletes)
	}
	if len(updates) == 0 {
		t.Fatalf("expected at least one UpdateMessage (final Done finalize)")
	}
	last := updates[len(updates)-1]
	if !strings.Contains(last, "Hello world") {
		t.Fatalf("final card should preserve pre-NO_REPLY text, got %q", last)
	}
	if strings.Contains(last, "NO_REPLY") {
		t.Fatalf("final card should not contain NO_REPLY marker, got %q", last)
	}
	if !strings.Contains(last, "status=done") {
		t.Fatalf("final card should have status=done, got %q", last)
	}
}

// TestProcessInteractiveEvents_RichCard_ToolThenNoReply verifies that when a
// turn issues tool calls (creating the rich card with visible tool steps) and
// then resolves to NO_REPLY, the card is finalized to Done — not deleted.
// Deleting would leave a "撤回了一条消息" gray bar matched up with already-
// visible tool activity. This mirrors legacy + full mode where tool messages
// remain visible even when the final reply is silent.
func TestProcessInteractiveEvents_RichCard_ToolThenNoReply(t *testing.T) {
	p := &stubRichCardSilentPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "feishu"},
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		Mode:             "full",
		CardMode:         "rich",
		ThinkingMessages: true,
		ThinkingMaxLen:   300,
		ToolMaxLen:       500,
		ToolMessages:     true,
	})
	sessionKey := "feishu:user-rich-tool-then-noreply"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-rich-tool-then-noreply")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-rich-tool-then-noreply",
	}
	e.interactiveStates[sessionKey] = state

	code := 0
	success := true
	agentSession.events <- Event{Type: EventToolUse, ToolName: "Bash", ToolInput: "echo hi"}
	agentSession.events <- Event{Type: EventToolResult, ToolName: "Bash", ToolResult: "hi", ToolStatus: "completed", ToolExitCode: &code, ToolSuccess: &success}
	agentSession.events <- Event{Type: EventText, Content: "NO_REPLY"}
	agentSession.events <- Event{Type: EventResult, Content: "NO_REPLY", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-rich-tool-then-noreply", time.Now(), nil, nil, state.replyCtx)
	starts, streams, updates, deletes := p.snapshot()

	if len(starts) == 0 {
		t.Fatalf("expected SendPreviewStart for tool card, got 0")
	}
	if len(streams) != 0 {
		t.Fatalf("expected no StreamRichCardText (silentHold gates text path), got %d: %v", len(streams), streams)
	}
	if deletes != 0 {
		t.Fatalf("expected no DeletePreviewMessage (tool card finalized in place), got %d", deletes)
	}
	if len(updates) == 0 {
		t.Fatalf("expected at least one UpdateMessage (final Done finalize)")
	}
	last := updates[len(updates)-1]
	if !strings.Contains(last, "status=done") {
		t.Fatalf("final update should show status=done, got %q", last)
	}
	if strings.Contains(last, "NO_REPLY") {
		t.Fatalf("finalize should not include NO_REPLY in body, got %q", last)
	}
}

func countCardActionValues(card *Card, prefix string) int {
	count := 0
	for _, elem := range card.Elements {
		switch e := elem.(type) {
		case CardActions:
			for _, btn := range e.Buttons {
				if strings.HasPrefix(btn.Value, prefix) {
					count++
				}
			}
		case CardListItem:
			if strings.HasPrefix(e.BtnValue, prefix) {
				count++
			}
		}
	}
	return count
}

func waitForSentCard(t *testing.T, p *stubCardPlatform) *Card {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			p.mu.Lock()
			count := len(p.sentCards)
			p.mu.Unlock()
			t.Fatalf("timed out waiting for sent card, sentCards=%d", count)
		case <-ticker.C:
			p.mu.Lock()
			var card *Card
			if len(p.sentCards) > 0 {
				card = p.sentCards[0]
			}
			p.mu.Unlock()
			if card != nil {
				return card
			}
		}
	}
}

func waitForSentText(t *testing.T, p *stubPlatformEngine) string {
	t.Helper()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for sent text, sent=%#v", p.getSent())
		case <-ticker.C:
			sent := p.getSent()
			if len(sent) > 0 {
				return sent[0]
			}
		}
	}
}

func findCardAction(card *Card, value string) (CardButton, bool) {
	for _, elem := range card.Elements {
		switch e := elem.(type) {
		case CardActions:
			for _, btn := range e.Buttons {
				if btn.Value == value {
					return btn, true
				}
			}
		case CardListItem:
			if e.BtnValue == value {
				return CardButton{Text: e.BtnText, Type: e.BtnType, Value: e.BtnValue}, true
			}
		}
	}
	return CardButton{}, false
}

// --- alias tests ---

func TestEngine_Alias(t *testing.T) {
	e := newTestEngine()
	e.AddAlias("帮助", "/help")
	e.AddAlias("新建", "/new")

	got := e.resolveAlias("帮助")
	if got != "/help" {
		t.Errorf("resolveAlias('帮助') = %q, want /help", got)
	}

	got = e.resolveAlias("新建 my-session")
	if got != "/new my-session" {
		t.Errorf("resolveAlias('新建 my-session') = %q, want '/new my-session'", got)
	}

	got = e.resolveAlias("random text")
	if got != "random text" {
		t.Errorf("resolveAlias should not modify unmatched content, got %q", got)
	}
}

func TestEngine_ClearAliases(t *testing.T) {
	e := newTestEngine()
	e.AddAlias("帮助", "/help")
	e.ClearAliases()

	got := e.resolveAlias("帮助")
	if got != "帮助" {
		t.Errorf("after ClearAliases, should not resolve, got %q", got)
	}
}

// --- banned words tests ---

func TestEngine_BannedWords(t *testing.T) {
	e := newTestEngine()
	e.SetBannedWords([]string{"spam", "BadWord"})

	if w := e.matchBannedWord("this is spam content"); w != "spam" {
		t.Errorf("expected 'spam', got %q", w)
	}
	if w := e.matchBannedWord("CONTAINS BADWORD HERE"); w != "badword" {
		t.Errorf("expected case-insensitive match 'badword', got %q", w)
	}
	if w := e.matchBannedWord("clean message"); w != "" {
		t.Errorf("expected empty, got %q", w)
	}
}

func TestEngine_BannedWordsEmpty(t *testing.T) {
	e := newTestEngine()
	if w := e.matchBannedWord("anything"); w != "" {
		t.Errorf("no banned words set, should return empty, got %q", w)
	}
}

// --- disabled commands tests ---

func TestEngine_DisabledCommands(t *testing.T) {
	e := newTestEngine()
	e.SetDisabledCommands([]string{"delete", "restart"})

	if !e.disabledCmds["delete"] {
		t.Error("delete should be disabled")
	}
	if !e.disabledCmds["restart"] {
		t.Error("restart should be disabled")
	}
	if e.disabledCmds["help"] {
		t.Error("help should not be disabled")
	}
}

func TestEngine_DisabledCommandsWithSlash(t *testing.T) {
	e := newTestEngine()
	e.SetDisabledCommands([]string{"/delete"})

	if !e.disabledCmds["delete"] {
		t.Error("delete should be disabled even when prefixed with /")
	}
}

func TestResolveDisabledCmds_Wildcard(t *testing.T) {
	m := resolveDisabledCmds([]string{"*"})
	for _, bc := range builtinCommands {
		if !m[bc.id] {
			t.Errorf("wildcard should disable %q", bc.id)
		}
	}
}

func TestResolveDisabledCmds_Specific(t *testing.T) {
	m := resolveDisabledCmds([]string{"delete", "/restart", "Help"})
	if !m["delete"] {
		t.Error("delete should be disabled")
	}
	if !m["restart"] {
		t.Error("restart should be disabled (slash stripped)")
	}
	if !m["help"] {
		t.Error("help should be disabled (case insensitive)")
	}
	if m["shell"] {
		t.Error("shell should not be disabled")
	}
}

func TestResolveDisabledCmds_Empty(t *testing.T) {
	m1 := resolveDisabledCmds(nil)
	if len(m1) != 0 {
		t.Errorf("nil input should produce empty map, got %d entries", len(m1))
	}
	m2 := resolveDisabledCmds([]string{})
	if len(m2) != 0 {
		t.Errorf("empty input should produce empty map, got %d entries", len(m2))
	}
}

func TestEngine_DisabledCommandsWildcard(t *testing.T) {
	e := newTestEngine()
	e.SetDisabledCommands([]string{"*"})

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}

	e.handleCommand(p, msg, "/help")
	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "disabled") && !strings.Contains(p.sent[0], "禁用") {
		t.Errorf("expected disabled message, got: %s", p.sent[0])
	}
}

// --- admin_from tests ---

func TestEngine_AdminFrom_DenyByDefault(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/shell echo hi")

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "admin") {
		t.Errorf("expected admin required message, got: %s", p.sent[0])
	}
}

func TestEngine_AdminFrom_ExplicitUser(t *testing.T) {
	e := newTestEngine()
	e.SetAdminFrom("admin1,admin2")
	p := &stubPlatformEngine{n: "test"}

	if !e.isAdmin("admin1") {
		t.Error("admin1 should be admin")
	}
	if !e.isAdmin("admin2") {
		t.Error("admin2 should be admin")
	}
	if e.isAdmin("user3") {
		t.Error("user3 should not be admin")
	}

	// non-admin user tries /shell
	msg := &Message{SessionKey: "test:u3", UserID: "user3", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/shell echo hi")
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "admin") {
		t.Errorf("non-admin should be blocked from /shell, got: %v", p.sent)
	}
}

func TestEngine_AdminFrom_Wildcard(t *testing.T) {
	e := newTestEngine()
	e.SetAdminFrom("*")

	if !e.isAdmin("anyone") {
		t.Error("wildcard admin_from should allow any user")
	}
	if !e.isAdmin("12345") {
		t.Error("wildcard admin_from should allow any user ID")
	}
}

func TestEngine_AdminFrom_GatesRestart(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/restart")

	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "admin") {
		t.Errorf("non-admin should be blocked from /restart, got: %v", p.sent)
	}
}

func TestEngine_AdminFrom_AllowsNonPrivileged(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/help")

	if len(p.sent) == 0 {
		t.Fatal("expected /help to produce a reply")
	}
	if strings.Contains(p.sent[0], "requires admin") {
		t.Errorf("/help should not require admin, got: %s", p.sent[0])
	}
}

func TestEngine_AdminFrom_GatesCommandsAddExec(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/commands addexec mysh echo hello")

	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "admin") {
		t.Errorf("non-admin should be blocked from /commands addexec, got: %v", p.sent)
	}
}

func TestEngine_AdminFrom_GatesCustomExecCommand(t *testing.T) {
	e := newTestEngine()
	e.commands.Add("deploy", "", "", "echo deploying", "", "config")
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/deploy")

	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "admin") {
		t.Errorf("non-admin should be blocked from custom exec command, got: %v", p.sent)
	}
}

func TestEngine_AdminFrom_AdminCanRunShell(t *testing.T) {
	e := newTestEngine()
	e.SetAdminFrom("admin1")
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{SessionKey: "test:a1", UserID: "admin1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/shell echo hello")

	// Shell runs async in a goroutine; wait for it to complete.
	time.Sleep(500 * time.Millisecond)

	for _, s := range p.getSent() {
		if strings.Contains(s, "admin") {
			t.Errorf("admin user should not be blocked, got: %s", s)
		}
	}
}

// --- role-based ACL tests ---

func TestEngine_RoleBasedACL_AdminCanRunAll(t *testing.T) {
	e := newTestEngine()
	e.SetDisabledCommands([]string{"help", "list"}) // project-level disables

	urm := NewUserRoleManager()
	urm.Configure("member", []RoleInput{
		{Name: "admin", UserIDs: []string{"admin1"}, DisabledCommands: []string{}},
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{"*"}},
	})
	e.SetUserRoles(urm)

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:a1", UserID: "admin1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/help")

	// Admin role has disabled_commands=[], so /help should NOT be blocked
	for _, s := range p.sent {
		if strings.Contains(s, "disabled") || strings.Contains(s, "禁用") {
			t.Errorf("admin should not have /help disabled, got: %s", s)
		}
	}
}

func TestEngine_RoleBasedACL_MemberBlocked(t *testing.T) {
	e := newTestEngine()

	urm := NewUserRoleManager()
	urm.Configure("member", []RoleInput{
		{Name: "admin", UserIDs: []string{"admin1"}, DisabledCommands: []string{}},
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{"*"}},
	})
	e.SetUserRoles(urm)

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/help")

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "disabled") && !strings.Contains(p.sent[0], "禁用") {
		t.Errorf("member should have /help disabled, got: %s", p.sent[0])
	}
}

func TestEngine_RoleBasedACL_NoUserID_UsesDefaultRole(t *testing.T) {
	e := newTestEngine()
	e.SetDisabledCommands([]string{"help"}) // project-level disables /help

	// Default role "member" has wildcard with disabled_commands=["*"]
	urm := NewUserRoleManager()
	urm.Configure("member", []RoleInput{
		{Name: "admin", UserIDs: []string{"admin1"}, DisabledCommands: []string{}},
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{"*"}},
	})
	e.SetUserRoles(urm)

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:anon", UserID: "", ReplyCtx: "ctx"} // no UserID
	e.handleCommand(p, msg, "/help")

	// Empty UserID resolves to default/wildcard role, which disables all commands
	if len(p.sent) != 1 || (!strings.Contains(p.sent[0], "disabled") && !strings.Contains(p.sent[0], "禁用")) {
		t.Errorf("empty UserID should resolve to default role ACL, got: %v", p.sent)
	}
}

func TestEngine_RoleBasedACL_NoUsersConfig_Legacy(t *testing.T) {
	e := newTestEngine()
	e.SetDisabledCommands([]string{"help"})
	// No SetUserRoles — legacy mode

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/help")

	if len(p.sent) != 1 || (!strings.Contains(p.sent[0], "disabled") && !strings.Contains(p.sent[0], "禁用")) {
		t.Errorf("legacy mode should use project-level disabled_commands, got: %v", p.sent)
	}
}

func TestEngine_CustomCommand_DisabledByRole(t *testing.T) {
	e := newTestEngine()
	e.commands.Add("deploy", "deploy command", "deploy it", "", "", "test")

	urm := NewUserRoleManager()
	urm.Configure("member", []RoleInput{
		{Name: "admin", UserIDs: []string{"admin1"}, DisabledCommands: []string{}},
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{"deploy"}},
	})
	e.SetUserRoles(urm)

	// Member should be blocked from custom command
	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/deploy")

	if len(p.sent) != 1 || (!strings.Contains(p.sent[0], "disabled") && !strings.Contains(p.sent[0], "禁用")) {
		t.Errorf("custom command should be blocked for member, got: %v", p.sent)
	}

	// Admin should be allowed
	p2 := &stubPlatformEngine{n: "test"}
	msg2 := &Message{SessionKey: "test:a1", UserID: "admin1", ReplyCtx: "ctx"}
	e.handleCommand(p2, msg2, "/deploy")

	if len(p2.sent) > 0 && (strings.Contains(p2.sent[0], "disabled") || strings.Contains(p2.sent[0], "禁用")) {
		t.Errorf("custom command should be allowed for admin, got: %v", p2.sent)
	}
}

// --- role-based rate limit tests ---

func TestEngine_RateLimit_RoleSpecific(t *testing.T) {
	e := newTestEngine()

	urm := NewUserRoleManager()
	urm.Configure("member", []RoleInput{
		{Name: "admin", UserIDs: []string{"admin1"}, DisabledCommands: []string{},
			RateLimit: &RateLimitCfg{MaxMessages: 50, Window: time.Minute}},
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{},
			RateLimit: &RateLimitCfg{MaxMessages: 2, Window: time.Minute}},
	})
	e.SetUserRoles(urm)

	// Member should be limited after 2 messages
	msg := &Message{SessionKey: "test:u1", UserID: "user1"}
	if !e.checkRateLimit(msg) {
		t.Error("1st message should be allowed")
	}
	if !e.checkRateLimit(msg) {
		t.Error("2nd message should be allowed")
	}
	if e.checkRateLimit(msg) {
		t.Error("3rd message should be rate-limited")
	}

	// Admin should still be allowed
	adminMsg := &Message{SessionKey: "test:a1", UserID: "admin1"}
	if !e.checkRateLimit(adminMsg) {
		t.Error("admin should not be rate-limited")
	}
}

func TestEngine_RateLimit_NoUsersConfig_Legacy(t *testing.T) {
	e := newTestEngine()
	e.SetRateLimitCfg(RateLimitCfg{MaxMessages: 2, Window: time.Minute})

	msg := &Message{SessionKey: "test:session1", UserID: "user1"}
	if !e.checkRateLimit(msg) {
		t.Error("1st should be allowed")
	}
	if !e.checkRateLimit(msg) {
		t.Error("2nd should be allowed")
	}
	if e.checkRateLimit(msg) {
		t.Error("3rd should be rate-limited")
	}

	// Different session key should be independent (legacy keying)
	msg2 := &Message{SessionKey: "test:session2", UserID: "user1"}
	if !e.checkRateLimit(msg2) {
		t.Error("different session key should have independent bucket in legacy mode")
	}
}

func TestEngine_RateLimit_GlobalFallback(t *testing.T) {
	e := newTestEngine()
	e.SetRateLimitCfg(RateLimitCfg{MaxMessages: 2, Window: time.Minute})

	// User roles configured but role has no rate_limit
	urm := NewUserRoleManager()
	urm.Configure("member", []RoleInput{
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{}},
		// No RateLimit on this role
	})
	e.SetUserRoles(urm)

	msg := &Message{SessionKey: "test:s1", UserID: "user1"}
	if !e.checkRateLimit(msg) {
		t.Error("1st should be allowed")
	}
	if !e.checkRateLimit(msg) {
		t.Error("2nd should be allowed")
	}
	if e.checkRateLimit(msg) {
		t.Error("3rd should be rate-limited by global limiter")
	}

	// Same user, different session → should share limit (keyed by userID when users config active)
	msg2 := &Message{SessionKey: "test:s2", UserID: "user1"}
	if e.checkRateLimit(msg2) {
		t.Error("same user from different session should still be rate-limited")
	}
}

// --- permission prompt card tests ---

func TestSendPermissionPrompt_CardPlatform(t *testing.T) {
	e := newTestEngine()
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}

	e.sendPermissionPrompt(p, "ctx", "full prompt text", "write_file", "/tmp/test.txt")

	if len(p.sentCards) != 1 {
		t.Fatalf("expected 1 sent card, got %d", len(p.sentCards))
	}
	card := p.sentCards[0]
	if card.Header == nil || card.Header.Color != "orange" {
		t.Errorf("expected orange header, got %+v", card.Header)
	}
	if !card.HasButtons() {
		t.Error("expected card to have buttons")
	}
	buttons := card.CollectButtons()
	if len(buttons) < 2 {
		t.Fatalf("expected at least 2 button rows, got %d", len(buttons))
	}
	if buttons[0][0].Data != "perm:allow" {
		t.Errorf("expected first button data=perm:allow, got %s", buttons[0][0].Data)
	}
	if buttons[0][1].Data != "perm:deny" {
		t.Errorf("expected second button data=perm:deny, got %s", buttons[0][1].Data)
	}
	if buttons[1][0].Data != "perm:allow_all" {
		t.Errorf("expected third button data=perm:allow_all, got %s", buttons[1][0].Data)
	}
	if len(p.sent) != 0 {
		t.Errorf("plain text should not be sent when card is used, got %v", p.sent)
	}

	// Verify Extra fields carry i18n labels and body for card callback updates
	var allowBtn, denyBtn CardButton
	for _, elem := range card.Elements {
		if actions, ok := elem.(CardActions); ok {
			for _, btn := range actions.Buttons {
				switch btn.Value {
				case "perm:allow":
					allowBtn = btn
				case "perm:deny":
					denyBtn = btn
				}
			}
		}
	}
	if allowBtn.Extra == nil {
		t.Fatal("allow button should have Extra map")
	}
	if allowBtn.Extra["perm_color"] != "green" {
		t.Errorf("allow button perm_color should be green, got %s", allowBtn.Extra["perm_color"])
	}
	if allowBtn.Extra["perm_body"] == "" {
		t.Error("allow button perm_body should not be empty")
	}
	if !strings.Contains(allowBtn.Extra["perm_label"], "Allow") {
		t.Errorf("allow button perm_label should contain 'Allow', got %s", allowBtn.Extra["perm_label"])
	}
	if denyBtn.Extra["perm_color"] != "red" {
		t.Errorf("deny button perm_color should be red, got %s", denyBtn.Extra["perm_color"])
	}
}

func TestSendPermissionPrompt_InlineButtonPlatform(t *testing.T) {
	e := newTestEngine()
	p := &stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}}

	e.sendPermissionPrompt(p, "ctx", "full prompt text", "write_file", "/tmp/test.txt")

	if p.buttonContent != "full prompt text" {
		t.Errorf("expected button content to be prompt, got %s", p.buttonContent)
	}
	if len(p.buttonRows) < 2 {
		t.Fatalf("expected at least 2 button rows, got %d", len(p.buttonRows))
	}
	if p.buttonRows[0][0].Data != "perm:allow" {
		t.Errorf("expected perm:allow, got %s", p.buttonRows[0][0].Data)
	}
}

func TestSendPermissionPrompt_PlainPlatform(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "plain"}

	e.sendPermissionPrompt(p, "ctx", "full prompt text", "write_file", "/tmp/test.txt")

	if len(p.sent) != 1 || p.sent[0] != "full prompt text" {
		t.Errorf("expected plain text fallback, got %v", p.sent)
	}
}

func TestHandleMessage_AutoResetOnIdle_RotatesToNewSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("fresh reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetResetOnIdle(60 * time.Minute)

	key := "test:user1"
	old := e.sessions.GetOrCreateActive(key)
	old.AddHistory("user", "stale context")
	old.SetAgentSessionID("old-session", "stub")
	staleAt := time.Now().Add(-2 * time.Hour)
	old.mu.Lock()
	old.LastUserActivity = staleAt
	old.UpdatedAt = staleAt
	old.mu.Unlock()

	msg := &Message{
		SessionKey: key,
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello after idle",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	deadline := time.After(2 * time.Second)
	for {
		active := e.sessions.GetOrCreateActive(key)
		sent := p.getSent()
		if active.ID != old.ID && len(active.GetHistory(0)) >= 2 && len(sent) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for idle auto-reset, sent=%v active=%s old=%s", sent, active.ID, old.ID)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	active := e.sessions.GetOrCreateActive(key)
	if active.ID == old.ID {
		t.Fatal("expected a new active session after idle auto-reset")
	}
	if got := old.GetAgentSessionID(); got != "old-session" {
		t.Fatalf("old session agent id = %q, want old-session preserved", got)
	}
	if got := len(old.GetHistory(0)); got != 1 {
		t.Fatalf("old session history len = %d, want 1 preserved entry", got)
	}
	if got := old.GetUpdatedAt(); !got.Equal(staleAt) {
		t.Fatalf("old session updated_at = %v, want unchanged %v", got, staleAt)
	}

	history := active.GetHistory(0)
	if len(history) != 2 {
		t.Fatalf("new session history len = %d, want 2", len(history))
	}
	if history[0].Role != "user" || history[0].Content != "hello after idle" {
		t.Fatalf("unexpected first history entry: %#v", history[0])
	}
	if history[1].Role != "assistant" || history[1].Content != "fresh reply" {
		t.Fatalf("unexpected second history entry: %#v", history[1])
	}

	sent := p.getSent()
	if !strings.Contains(sent[0], "Session auto-reset") {
		t.Fatalf("first reply = %q, want auto-reset notice", sent[0])
	}
	if got := sent[len(sent)-1]; got != "fresh reply" {
		t.Fatalf("final reply = %q, want fresh reply", got)
	}
}

func TestHandleMessage_AutoResetOnIdle_DoesNotRotateFreshSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("normal reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetResetOnIdle(60 * time.Minute)

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	session.AddHistory("user", "recent context")
	session.SetAgentSessionID("existing-session", "stub")
	recentAt := time.Now().Add(-5 * time.Minute)
	session.mu.Lock()
	session.LastUserActivity = recentAt
	session.UpdatedAt = recentAt
	session.mu.Unlock()

	msg := &Message{
		SessionKey: key,
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "follow up",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	deadline := time.After(2 * time.Second)
	for {
		if len(session.GetHistory(0)) >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for normal turn, sent=%v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	active := e.sessions.GetOrCreateActive(key)
	if active.ID != session.ID {
		t.Fatalf("active session = %s, want unchanged %s", active.ID, session.ID)
	}
	sent := p.getSent()
	for _, line := range sent {
		if strings.Contains(line, "Session auto-reset") {
			t.Fatalf("unexpected auto-reset notice in replies: %v", sent)
		}
	}
}

func TestHandleMessage_AutoResetOnIdle_FiresWhenHeartbeatBumpedUpdatedAt(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("fresh reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetResetOnIdle(60 * time.Minute)

	key := "test:user1"
	old := e.sessions.GetOrCreateActive(key)
	old.AddHistory("user", "stale context")
	old.SetAgentSessionID("old-session", "stub")

	// Last user message was a long time ago — well past the idle threshold.
	staleAt := time.Now().Add(-2 * time.Hour)
	old.mu.Lock()
	old.LastUserActivity = staleAt
	old.mu.Unlock()

	// Simulate a heartbeat (or unsolicited agent response) finishing right
	// before this test's user message: Unlock() bumps UpdatedAt to now, but
	// LastUserActivity is intentionally NOT touched by those code paths.
	old.Unlock()

	if !old.GetUpdatedAt().After(staleAt) {
		t.Fatalf("expected Unlock to bump UpdatedAt, got %v vs %v", old.GetUpdatedAt(), staleAt)
	}
	if !old.GetLastUserActivity().Equal(staleAt) {
		t.Fatalf("expected LastUserActivity to remain at %v, got %v", staleAt, old.GetLastUserActivity())
	}

	msg := &Message{
		SessionKey: key,
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello after idle",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	deadline := time.After(2 * time.Second)
	for {
		active := e.sessions.GetOrCreateActive(key)
		sent := p.getSent()
		if active.ID != old.ID && len(active.GetHistory(0)) >= 2 && len(sent) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for idle auto-reset despite heartbeat-bumped UpdatedAt, sent=%v active=%s old=%s", sent, active.ID, old.ID)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	active := e.sessions.GetOrCreateActive(key)
	if active.ID == old.ID {
		t.Fatal("expected a new active session after idle auto-reset")
	}
	sent := p.getSent()
	if !strings.Contains(sent[0], "Session auto-reset") {
		t.Fatalf("first reply = %q, want auto-reset notice", sent[0])
	}
}

func TestHandleMessage_AutoResetOnIdle_DoesNotTriggerForSlashCommand(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetResetOnIdle(60 * time.Minute)

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	session.AddHistory("user", "stale context")
	session.SetAgentSessionID("old-session", "stub")
	staleAt := time.Now().Add(-2 * time.Hour)
	session.mu.Lock()
	session.LastUserActivity = staleAt
	session.UpdatedAt = staleAt
	session.mu.Unlock()

	msg := &Message{
		SessionKey: key,
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "/list",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	active := e.sessions.GetOrCreateActive(key)
	if active.ID != session.ID {
		t.Fatalf("active session = %s, want unchanged %s", active.ID, session.ID)
	}
	for _, line := range p.getSent() {
		if strings.Contains(line, "Session auto-reset") {
			t.Fatalf("unexpected auto-reset notice for slash command: %v", p.getSent())
		}
	}
}

func TestReplyWithCard_FallsBackToTextWhenPlatformHasNoCardSupport(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	card := NewCard().Title("Help", "blue").Markdown("Plain fallback").Build()

	e.replyWithCard(p, "ctx", card)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if got, want := p.sent[0], card.RenderText(); got != want {
		t.Fatalf("fallback text = %q, want %q", got, want)
	}
}

func TestReplyWithCard_UsesCardSenderWhenSupported(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "card"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	card := NewCard().Markdown("Interactive").Build()

	e.replyWithCard(p, "ctx", card)

	if len(p.repliedCards) != 1 {
		t.Fatalf("replied cards = %d, want 1", len(p.repliedCards))
	}
	if len(p.sent) != 0 {
		t.Fatalf("plain replies = %d, want 0", len(p.sent))
	}
}

func TestReply_DoesNotTransformLocalReferencesWhenEnabled(t *testing.T) {
	p := &stubPlatformEngine{n: "feishu"}
	a := &namedStubModelModeAgent{name: "codex"}
	e := NewEngine("test", a, []Platform{p}, "")
	e.SetBaseWorkDir("/root/code/demo")
	e.SetReferenceConfig(ReferenceRenderCfg{
		NormalizeAgents: []string{"codex"},
		RenderPlatforms: []string{"feishu"},
		DisplayPath:     "relative",
		MarkerStyle:     "emoji",
		EnclosureStyle:  "code",
	})

	e.reply(p, "ctx", "See /root/code/demo/src/app.ts:42")

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if got := p.sent[0]; got != "See /root/code/demo/src/app.ts:42" {
		t.Fatalf("reply content = %q, want raw path", got)
	}
}

func TestReplyWithCard_DoesNotTransformMarkdownOrFallback(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	a := &namedStubModelModeAgent{name: "codex"}
	e := NewEngine("test", a, []Platform{p}, "")
	e.SetBaseWorkDir("/root/code/demo")
	e.SetReferenceConfig(ReferenceRenderCfg{
		NormalizeAgents: []string{"codex"},
		RenderPlatforms: []string{"feishu"},
		DisplayPath:     "basename",
		MarkerStyle:     "ascii",
		EnclosureStyle:  "code",
	})
	card := NewCard().Markdown("Inspect /root/code/demo/src/app.ts:42").Build()

	e.replyWithCard(p, "ctx", card)

	if len(p.repliedCards) != 1 {
		t.Fatalf("replied cards = %d, want 1", len(p.repliedCards))
	}
	rendered := p.repliedCards[0]
	md, ok := rendered.Elements[0].(CardMarkdown)
	if !ok {
		t.Fatalf("first card element = %T, want CardMarkdown", rendered.Elements[0])
	}
	if md.Content != "Inspect /root/code/demo/src/app.ts:42" {
		t.Fatalf("card markdown = %q, want raw reference", md.Content)
	}
	if got := rendered.RenderText(); !strings.Contains(got, "/root/code/demo/src/app.ts:42") {
		t.Fatalf("fallback RenderText() = %q, want raw reference", got)
	}
}

func TestCmdList_UsesLegacyTextOnPlatformWithoutCardSupport(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	sessions := []AgentSessionInfo{{ID: "session-a", Summary: "First session", MessageCount: 3, ModifiedAt: time.Date(2026, 3, 11, 2, 0, 0, 0, time.UTC)}}
	e := NewEngine("test", &stubListAgent{sessions: sessions}, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "Sessions") {
		t.Fatalf("list text = %q, want legacy list title", p.sent[0])
	}
	if strings.Contains(p.sent[0], "[← 返回]") {
		t.Fatalf("list text = %q, should not be card fallback text", p.sent[0])
	}
}

func TestCmdCurrent_UsesLegacyTextOnPlatformWithoutCardSupport(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	session := e.sessions.GetOrCreateActive(msg.SessionKey)
	session.SetAgentSessionID("session-123", "test")
	e.sessions.SetSessionName("session-123", "Focus")
	session.History = append(session.History, HistoryEntry{Role: "user", Content: "hello", Timestamp: time.Now()})

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "Current session") {
		t.Fatalf("current text = %q, want legacy current session text", p.sent[0])
	}
	if !strings.Contains(p.sent[0], "Focus") {
		t.Fatalf("current text = %q, want session name 'Focus'", p.sent[0])
	}
	if strings.Contains(p.sent[0], "agent-bridge") {
		t.Fatalf("current text = %q, should not be card fallback title", p.sent[0])
	}
}

func TestCmdCurrent_ShowsAgentSummaryWhenNoCustomName(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-abc", Summary: "Fix the login bug", MessageCount: 5},
	}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	session := e.sessions.GetOrCreateActive(msg.SessionKey)
	session.SetAgentSessionID("session-abc", "test")

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "Fix the login bug") {
		t.Fatalf("current text = %q, want agent summary 'Fix the login bug'", p.sent[0])
	}
}

func TestCmdCurrent_ShowsUntitledWhenNoNameOrSummary(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-xyz", Summary: "", MessageCount: 0},
	}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	session := e.sessions.GetOrCreateActive(msg.SessionKey)
	session.SetAgentSessionID("session-xyz", "test")

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "(untitled)") {
		t.Fatalf("current text = %q, want '(untitled)' fallback", p.sent[0])
	}
}

func TestCmdCurrent_CustomNameOverridesSummary(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-override", Summary: "Agent summary", MessageCount: 3},
	}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	session := e.sessions.GetOrCreateActive(msg.SessionKey)
	session.SetAgentSessionID("session-override", "test")
	e.sessions.SetSessionName("session-override", "MyCustomName")

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "MyCustomName") {
		t.Fatalf("current text = %q, want custom name 'MyCustomName'", p.sent[0])
	}
	if strings.Contains(p.sent[0], "Agent summary") {
		t.Fatalf("current text = %q, should not contain agent summary when custom name set", p.sent[0])
	}
}

func TestCmdCurrent_NotStartedSessionShowsUntitled(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "(untitled)") {
		t.Fatalf("current text = %q, want '(untitled)' for not-started session", p.sent[0])
	}
}

type stubTitleAgent struct {
	stubAgent
	titles map[string]string
}

func (a *stubTitleAgent) GetSessionTitle(sessionID string) string {
	if a.titles == nil {
		return ""
	}
	return a.titles[sessionID]
}

func TestCmdCurrent_SessionTitleProviderFallback(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubTitleAgent{
		titles: map[string]string{
			"session-not-in-list": "Title from DB",
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	session := e.sessions.GetOrCreateActive(msg.SessionKey)
	session.SetAgentSessionID("session-not-in-list", "test")

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "Title from DB") {
		t.Fatalf("current text = %q, want 'Title from DB' from SessionTitleProvider", p.sent[0])
	}
}

func TestCmdCurrent_SessionTitleProviderNotUsedWhenListMatches(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubListAgentWithTitle{
		stubTitleAgent: stubTitleAgent{
			titles: map[string]string{
				"session-abc": "DB Title (should not appear)",
			},
		},
		sessions: []AgentSessionInfo{
			{ID: "session-abc", Summary: "List Summary", MessageCount: 5},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	session := e.sessions.GetOrCreateActive(msg.SessionKey)
	session.SetAgentSessionID("session-abc", "test")

	e.cmdCurrent(p, msg)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "List Summary") {
		t.Fatalf("current text = %q, want 'List Summary' from ListSessions", p.sent[0])
	}
}

type stubListAgentWithTitle struct {
	stubTitleAgent
	sessions []AgentSessionInfo
}

func (a *stubListAgentWithTitle) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	return a.sessions, nil
}

func TestCmdDelete_BatchCommaList(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
		{ID: "session-4", Summary: "Four"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, []string{"1,2,3"})

	if got, want := strings.Join(agent.deleted, ","), "session-1,session-2,session-3"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "Session deleted: One") || !strings.Contains(p.sent[0], "Session deleted: Three") {
		t.Fatalf("reply = %q, want combined delete summary", p.sent[0])
	}
}

func TestCmdDelete_BatchRange(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
		{ID: "session-4", Summary: "Four"},
		{ID: "session-5", Summary: "Five"},
		{ID: "session-6", Summary: "Six"},
		{ID: "session-7", Summary: "Seven"},
		{ID: "session-8", Summary: "Eight"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, []string{"3-7"})

	if got, want := strings.Join(agent.deleted, ","), "session-3,session-4,session-5,session-6,session-7"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
}

func TestCmdDelete_BatchMixedSyntax(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
		{ID: "session-4", Summary: "Four"},
		{ID: "session-5", Summary: "Five"},
		{ID: "session-6", Summary: "Six"},
		{ID: "session-7", Summary: "Seven"},
		{ID: "session-8", Summary: "Eight"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, []string{"1,3-5,8"})

	if got, want := strings.Join(agent.deleted, ","), "session-1,session-3,session-4,session-5,session-8"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
}

func TestCmdDelete_InvalidExplicitBatchSyntaxShowsUsage(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, []string{"1,3-a,8"})

	if len(agent.deleted) != 0 {
		t.Fatalf("deleted = %v, want none", agent.deleted)
	}
	if len(p.sent) != 1 || p.sent[0] != e.i18n.T(MsgDeleteUsage) {
		t.Fatalf("sent = %v, want usage", p.sent)
	}
}

func TestCmdDelete_WhitespaceSeparatedArgsAreRejected(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, []string{"1", "2", "3"})

	if len(agent.deleted) != 0 {
		t.Fatalf("deleted = %v, want none", agent.deleted)
	}
	if len(p.sent) != 1 || p.sent[0] != e.i18n.T(MsgDeleteUsage) {
		t.Fatalf("sent = %v, want usage", p.sent)
	}
}

func TestCmdDelete_SingleSessionPrefixStillWorks(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "abc123456789", Summary: "One"},
		{ID: "def987654321", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, []string{"abc123"})

	if got, want := strings.Join(agent.deleted, ","), "abc123456789"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
}

func TestCmdDelete_SyncsLocalSessionSnapshot(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	victim := e.sessions.NewSession("test:user2", "victim")
	victim.SetAgentSessionID("session-1", "stub")
	keep := e.sessions.NewSession("test:user3", "keep")
	keep.SetAgentSessionID("session-2", "stub")

	e.cmdDelete(p, msg, []string{"1"})

	if got, want := strings.Join(agent.deleted, ","), "session-1"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
	if got := e.sessions.FindByID(victim.ID); got != nil {
		t.Fatalf("victim session should be removed, got %+v", got)
	}
	if got := e.sessions.FindByID(keep.ID); got == nil {
		t.Fatal("keep session should remain")
	}
}

func TestCmdDelete_NoArgsOnCardPlatformShowsDeleteModeCard(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)

	if len(p.repliedCards) != 1 {
		t.Fatalf("replied cards = %d, want 1", len(p.repliedCards))
	}
	card := p.repliedCards[0]
	if got := countCardActionValues(card, "act:/delete-mode toggle "); got != 2 {
		t.Fatalf("toggle action count = %d, want 2", got)
	}
	if _, ok := findCardAction(card, "act:/delete-mode cancel"); !ok {
		t.Fatal("expected delete mode cancel action")
	}
}

func TestDeleteMode_ToggleSelectionReturnsUpdatedCard(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	card := e.handleCardNav("act:/delete-mode toggle session-2", msg.SessionKey)
	if card == nil {
		t.Fatal("expected card update after toggle")
	}
	if !strings.Contains(card.RenderText(), "1 selected") {
		t.Fatalf("card text = %q, want selected count", card.RenderText())
	}

	confirmCard := e.handleCardNav("act:/delete-mode confirm", msg.SessionKey)
	if confirmCard == nil {
		t.Fatal("expected confirmation card")
	}
	if !strings.Contains(confirmCard.RenderText(), "Two") {
		t.Fatalf("confirmation text = %q, want selected session", confirmCard.RenderText())
	}
}

func TestDeleteMode_ConfirmAndSubmitDeletesSelectedSessions(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	_ = e.handleCardNav("act:/delete-mode toggle session-1", msg.SessionKey)
	_ = e.handleCardNav("act:/delete-mode toggle session-3", msg.SessionKey)

	confirmCard := e.handleCardNav("act:/delete-mode confirm", msg.SessionKey)
	if confirmCard == nil {
		t.Fatal("expected confirmation card")
	}
	confirmText := confirmCard.RenderText()
	if !strings.Contains(confirmText, "One") || !strings.Contains(confirmText, "Three") {
		t.Fatalf("confirmation text = %q, want selected session names", confirmText)
	}

	resultCard := e.handleCardNav("act:/delete-mode submit", msg.SessionKey)
	if resultCard == nil {
		t.Fatal("expected deleting card after submit")
	}
	// Submit is now async; the returned card is a "deleting" indicator.
	// Wait for the background goroutine to complete and push the result card.
	waitDeleteModePhase(t, e, msg.SessionKey, "result")
	if got, want := strings.Join(agent.deleted, ","), "session-1,session-3"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
	refreshed := p.getRefreshedCards()
	if len(refreshed) == 0 {
		t.Fatal("expected refreshed result card via RefreshCard")
	}
	pushedCard := refreshed[len(refreshed)-1]
	if !strings.Contains(pushedCard.RenderText(), "Session deleted: One") {
		t.Fatalf("result text = %q, want delete result", pushedCard.RenderText())
	}
}

func TestDeleteMode_SubmitReportsMissingSelectedSessions(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	_ = e.handleCardNav("act:/delete-mode toggle session-1", msg.SessionKey)
	_ = e.handleCardNav("act:/delete-mode toggle session-3", msg.SessionKey)

	agent.sessions = []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}

	resultCard := e.handleCardNav("act:/delete-mode submit", msg.SessionKey)
	if resultCard == nil {
		t.Fatal("expected deleting card after submit")
	}
	// Wait for async deletion to complete.
	waitDeleteModePhase(t, e, msg.SessionKey, "result")
	refreshed := p.getRefreshedCards()
	if len(refreshed) == 0 {
		t.Fatal("expected refreshed result card via RefreshCard")
	}
	pushedCard := refreshed[len(refreshed)-1]
	resultText := pushedCard.RenderText()
	if !strings.Contains(resultText, "Session deleted: One") {
		t.Fatalf("result text = %q, want deleted session line", resultText)
	}
	if !strings.Contains(resultText, "Missing selected session") || !strings.Contains(resultText, "session-3") {
		t.Fatalf("result text = %q, want missing selected session to be reported", resultText)
	}
}

func TestDeleteMode_CancelReturnsListCard(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	card := e.handleCardNav("act:/delete-mode cancel", msg.SessionKey)
	if card == nil {
		t.Fatal("expected list card after cancel")
	}
	if got := countCardActionValues(card, "act:/switch "); got != 2 {
		t.Fatalf("switch action count = %d, want 2", got)
	}
}

func TestDeleteMode_ConfirmWithoutSelectionShowsHint(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	card := e.handleCardNav("act:/delete-mode confirm", msg.SessionKey)
	if card == nil {
		t.Fatal("expected delete mode card when confirming empty selection")
	}
	if !strings.Contains(card.RenderText(), "Select at least one session.") {
		t.Fatalf("card text = %q, want empty-selection hint", card.RenderText())
	}
}

func TestDeleteMode_PageNavigationPreservesSelection(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	sessions := make([]AgentSessionInfo, 0, 8)
	for i := 1; i <= 8; i++ {
		sessions = append(sessions, AgentSessionInfo{ID: fmt.Sprintf("session-%d", i), Summary: fmt.Sprintf("Session %d", i)})
	}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: sessions}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	_ = e.handleCardNav("act:/delete-mode toggle session-1", msg.SessionKey)
	pageTwo := e.handleCardNav("act:/delete-mode page 2", msg.SessionKey)
	if pageTwo == nil {
		t.Fatal("expected page 2 card")
	}
	if !strings.Contains(pageTwo.RenderText(), "1 selected") {
		t.Fatalf("page 2 text = %q, want preserved selected count", pageTwo.RenderText())
	}
	pageOne := e.handleCardNav("act:/delete-mode page 1", msg.SessionKey)
	if pageOne == nil {
		t.Fatal("expected page 1 card")
	}
	btn, ok := findCardAction(pageOne, "act:/delete-mode toggle session-1")
	if !ok {
		t.Fatal("expected toggle action for session-1")
	}
	if btn.Type != "primary" {
		t.Fatalf("selected button type = %q, want primary", btn.Type)
	}
}

func TestDeleteMode_SubmitBlocksActiveSession(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}
	e.sessions.GetOrCreateActive(msg.SessionKey).SetAgentSessionID("session-1", "test")

	e.cmdDelete(p, msg, nil)
	_ = e.handleCardNav("act:/delete-mode toggle session-1", msg.SessionKey)
	resultCard := e.handleCardNav("act:/delete-mode submit", msg.SessionKey)
	if resultCard == nil {
		t.Fatal("expected deleting card")
	}
	// Wait for async deletion to complete.
	waitDeleteModePhase(t, e, msg.SessionKey, "result")
	if len(agent.deleted) != 0 {
		t.Fatalf("deleted = %v, want none", agent.deleted)
	}
	if len(p.getRefreshedCards()) == 0 {
		t.Fatal("expected refreshed result card via RefreshCard")
	}
	pushedCard := p.getRefreshedCards()[len(p.getRefreshedCards())-1]
	if !strings.Contains(pushedCard.RenderText(), "Cannot delete the currently active session") {
		t.Fatalf("result text = %q, want active-session warning", pushedCard.RenderText())
	}
}

func TestDeleteMode_ActiveSessionMarkedWithArrowAndNotSelectable(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}
	// Register both sessions so they pass the owned-session filter.
	s1 := e.sessions.GetOrCreateActive(msg.SessionKey)
	s1.SetAgentSessionID("session-1", "test")
	s2 := e.sessions.NewSession(msg.SessionKey, "two")
	s2.SetAgentSessionID("session-2", "test")
	// Switch back to s1 as the active session.
	e.sessions.SwitchSession(msg.SessionKey, s1.ID)

	e.cmdDelete(p, msg, nil)
	if len(p.repliedCards) != 1 {
		t.Fatalf("replied cards = %d, want 1", len(p.repliedCards))
	}
	card := p.repliedCards[0]
	if _, ok := findCardAction(card, "act:/delete-mode toggle session-1"); ok {
		t.Fatal("active session should not be toggle-selectable")
	}
	if _, ok := findCardAction(card, "act:/delete-mode noop session-1"); !ok {
		t.Fatal("expected noop action for active session")
	}
	if got := countCardActionValues(card, "act:/delete-mode toggle "); got != 1 {
		t.Fatalf("toggle action count = %d, want 1", got)
	}
	if !strings.Contains(card.RenderText(), "(current) **1.**") {
		t.Fatalf("card text = %q, want arrow marker for active session", card.RenderText())
	}
}

func TestDeleteMode_FormSubmitShowsConfirmThenDeletes(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubDeleteAgent{stubListAgent: stubListAgent{sessions: []AgentSessionInfo{
		{ID: "session-1", Summary: "One"},
		{ID: "session-2", Summary: "Two"},
		{ID: "session-3", Summary: "Three"},
	}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "feishu:user1", ReplyCtx: "ctx"}

	e.cmdDelete(p, msg, nil)
	confirmCard := e.handleCardNav("act:/delete-mode form-submit session-1,session-3", msg.SessionKey)
	if confirmCard == nil {
		t.Fatal("expected confirm card after form-submit")
	}
	if len(agent.deleted) != 0 {
		t.Fatalf("deleted = %v, want none before confirm", agent.deleted)
	}
	confirmText := confirmCard.RenderText()
	if !strings.Contains(confirmText, "One") || !strings.Contains(confirmText, "Three") {
		t.Fatalf("confirm text = %q, want selected sessions", confirmText)
	}

	resultCard := e.handleCardNav("act:/delete-mode submit", msg.SessionKey)
	if resultCard == nil {
		t.Fatal("expected deleting card after submit")
	}
	// Wait for async deletion to complete.
	waitDeleteModePhase(t, e, msg.SessionKey, "result")
	if got, want := strings.Join(agent.deleted, ","), "session-1,session-3"; got != want {
		t.Fatalf("deleted = %q, want %q", got, want)
	}
	refreshed := p.getRefreshedCards()
	if len(refreshed) == 0 {
		t.Fatal("expected pushed result card via RefreshCard")
	}
	pushedCard := refreshed[len(refreshed)-1]
	if !strings.Contains(pushedCard.RenderText(), "Session deleted: One") {
		t.Fatalf("result text = %q, want delete result", pushedCard.RenderText())
	}
}

func TestExecuteCardActionStop_RemovesInteractiveState(t *testing.T) {
	e := newTestEngine()
	e.interactiveMu.Lock()
	e.interactiveStates["test:user1"] = &interactiveState{}
	e.interactiveMu.Unlock()

	e.executeCardAction("/stop", "", "test:user1")

	e.interactiveMu.Lock()
	state := e.interactiveStates["test:user1"]
	e.interactiveMu.Unlock()
	if state != nil {
		t.Fatal("expected interactive state to be removed")
	}
}

func TestCmdProvider_UsesLegacyTextOnPlatformWithoutCardSupport(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubProviderAgent{
		providers: []ProviderConfig{
			{Name: "openai", BaseURL: "https://api.openai.com", Model: "gpt-4.1"},
			{Name: "azure", BaseURL: "https://azure.example", Model: "gpt-4.1-mini"},
		},
		active: "openai",
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdProvider(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "Active provider") {
		t.Fatalf("provider text = %q, want current provider section", p.sent[0])
	}
	if !strings.Contains(p.sent[0], "openai") || !strings.Contains(p.sent[0], "azure") {
		t.Fatalf("provider text = %q, want provider list", p.sent[0])
	}
	if !strings.Contains(p.sent[0], "switch") {
		t.Fatalf("provider text = %q, want switch hint", p.sent[0])
	}
}

func TestCmdModel_UsesInlineButtonsOnButtonOnlyPlatform(t *testing.T) {
	p := &stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "inline-only"}}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdModel(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.buttonRows) == 0 {
		t.Fatal("expected /model to send inline buttons on button-only platform")
	}
	if got := p.buttonRows[0][0].Data; got != "cmd:/model switch 1" {
		t.Fatalf("first /model button = %q, want %q", got, "cmd:/model switch 1")
	}
}

func TestCmdModel_UpdatesActiveProviderModel(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{
		model: "gpt-4.1-mini",
		providers: []ProviderConfig{
			{
				Name:   "openai",
				Model:  "gpt-4.1-mini",
				Models: []ModelOption{{Name: "gpt-4.1", Alias: "gpt"}, {Name: "gpt-4.1-mini", Alias: "mini"}},
			},
		},
		active: "openai",
	}
	e := NewEngine("test", agent, []Platform{p}, "")
	var savedProvider, savedModel string
	e.SetProviderModelSaveFunc(func(providerName, model string) error {
		savedProvider = providerName
		savedModel = model
		return nil
	})
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("existing-session", "test")

	e.cmdModel(p, msg, []string{"switch", "gpt"})

	if agent.model != "gpt-4.1" {
		t.Fatalf("agent model = %q, want gpt-4.1", agent.model)
	}
	if got := agent.GetActiveProvider(); got == nil || got.Model != "gpt-4.1" {
		t.Fatalf("active provider model = %#v, want gpt-4.1", got)
	}
	if got := agent.GetModel(); got != "gpt-4.1" {
		t.Fatalf("GetModel() = %q, want gpt-4.1", got)
	}
	if savedProvider != "openai" || savedModel != "gpt-4.1" {
		t.Fatalf("saved provider/model = %q/%q, want openai/gpt-4.1", savedProvider, savedModel)
	}
	if active := e.sessions.GetOrCreateActive(msg.SessionKey); active.AgentSessionID != "existing-session" {
		t.Fatalf("session id = %q, want preserved after model switch", active.AgentSessionID)
	}
}

func TestCmdModel_DirectNameDoesNotNeedModelListMatch(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubStrictModelAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdModel(p, msg, []string{"switch", "custom/provider-model"})

	if agent.model != "custom/provider-model" {
		t.Fatalf("agent model = %q, want custom/provider-model", agent.model)
	}
	if agent.calls != 0 {
		t.Fatalf("AvailableModels calls = %d, want 0 for direct name switch", agent.calls)
	}
}

func TestCmdModel_AliasWithPunctuationStillResolves(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubStrictModelAgent{models: []ModelOption{{Name: "openai/gpt-4.1", Alias: "gpt-4.1"}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdModel(p, msg, []string{"switch", "gpt-4.1"})

	if agent.model != "openai/gpt-4.1" {
		t.Fatalf("agent model = %q, want openai/gpt-4.1", agent.model)
	}
	if agent.calls != 1 {
		t.Fatalf("AvailableModels calls = %d, want 1 for punctuated alias lookup", agent.calls)
	}
}

func TestCmdModel_AliasStillResolvesOnColdStart(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubStrictModelAgent{models: []ModelOption{{Name: "gpt-4.1", Alias: "gpt"}}}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdModel(p, msg, []string{"switch", "gpt"})

	if agent.model != "gpt-4.1" {
		t.Fatalf("agent model = %q, want gpt-4.1", agent.model)
	}
}

func TestCmdModel_LegacySyntaxStillWorks(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdModel(p, msg, []string{"gpt"})

	if agent.model != "gpt-4.1" {
		t.Fatalf("agent model = %q, want gpt-4.1", agent.model)
	}
}

func TestCmdModel_SavesModelWhenNoActiveProvider(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{
		model: "gpt-4.1-mini",
		providers: []ProviderConfig{
			{
				Name:   "openai",
				Model:  "gpt-4.1-mini",
				Models: []ModelOption{{Name: "gpt-4.1", Alias: "gpt"}, {Name: "gpt-4.1-mini", Alias: "mini"}},
			},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	var savedModel string
	e.SetModelSaveFunc(func(model string) error {
		savedModel = model
		return nil
	})

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	e.cmdModel(p, msg, []string{"switch", "gpt"})

	if agent.model != "gpt-4.1" {
		t.Fatalf("agent model = %q, want gpt-4.1", agent.model)
	}
	if savedModel != "gpt-4.1" {
		t.Fatalf("saved model = %q, want gpt-4.1", savedModel)
	}
}

func TestCmdModel_DoesNotClaimSuccessWhenModelSaveFails(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{
		model: "gpt-4.1-mini",
		providers: []ProviderConfig{
			{
				Name:   "openai",
				Model:  "gpt-4.1-mini",
				Models: []ModelOption{{Name: "gpt-4.1", Alias: "gpt"}, {Name: "gpt-4.1-mini", Alias: "mini"}},
			},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetModelSaveFunc(func(model string) error {
		return errors.New("disk full")
	})

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("existing-session", "test")
	s.AddHistory("user", "keep me")

	e.cmdModel(p, msg, []string{"switch", "gpt"})

	if agent.model != "gpt-4.1-mini" {
		t.Fatalf("agent model = %q, want unchanged gpt-4.1-mini", agent.model)
	}
	if active := e.sessions.GetOrCreateActive(msg.SessionKey); active.AgentSessionID != "existing-session" {
		t.Fatalf("session id = %q, want existing-session after failure", active.AgentSessionID)
	}
	if active := e.sessions.GetOrCreateActive(msg.SessionKey); len(active.History) != 1 {
		t.Fatalf("history length = %d, want 1 after failure", len(active.History))
	}
	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(sent))
	}
	if !strings.Contains(sent[0], "Failed to change model") {
		t.Fatalf("reply = %q, want model change failure message", sent[0])
	}
}

func TestCmdModel_KeepHistoryPreservesSessionID(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{
		model: "gpt-4.1-mini",
		providers: []ProviderConfig{
			{
				Name:   "openai",
				Model:  "gpt-4.1-mini",
				Models: []ModelOption{{Name: "gpt-4.1", Alias: "gpt"}, {Name: "gpt-4.1-mini", Alias: "mini"}},
			},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("existing-session-id", "test")
	s.AddHistory("user", "hello")

	e.cmdModel(p, msg, []string{"switch", "gpt"})

	if got := s.GetAgentSessionID(); got != "existing-session-id" {
		t.Fatalf("session id = %q, want existing-session-id (should be preserved)", got)
	}
	if got := len(s.GetHistory(0)); got != 1 {
		t.Fatalf("history len = %d, want 1 (original entry preserved)", got)
	}
}

func TestCmdDir_ShowsCurrentDirectory(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubWorkDirAgent{workDir: "/tmp/project-a"}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdDir(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "/tmp/project-a") {
		t.Fatalf("sent = %q, want current work dir", p.sent[0])
	}
}

func TestCmdDir_SwitchesDirectoryAndResetsSession(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	tempDir := t.TempDir()
	nextDir := filepath.Join(tempDir, "next")
	if err := os.Mkdir(nextDir, 0o755); err != nil {
		t.Fatalf("mkdir next dir: %v", err)
	}

	agent := &stubWorkDirAgent{workDir: tempDir}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("existing-session", "test")
	s.AddHistory("user", "hello")

	e.cmdDir(p, msg, []string{"next"})

	if agent.workDir != nextDir {
		t.Fatalf("workDir = %q, want %q", agent.workDir, nextDir)
	}
	if s.GetAgentSessionID() != "" {
		t.Fatalf("AgentSessionID = %q, want cleared", s.GetAgentSessionID())
	}
	if len(s.History) != 0 {
		t.Fatalf("history length = %d, want 0", len(s.History))
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], nextDir) {
		t.Fatalf("sent = %v, want directory changed message", p.sent)
	}
}

func TestCmdDir_RejectsMissingDirectory(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	tempDir := t.TempDir()
	missingDir := filepath.Join(tempDir, "missing")
	agent := &stubWorkDirAgent{workDir: tempDir}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdDir(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, []string{"missing"})

	if agent.workDir != tempDir {
		t.Fatalf("workDir = %q, want unchanged %q", agent.workDir, tempDir)
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], missingDir) {
		t.Fatalf("sent = %v, want invalid path message", p.sent)
	}
}

func TestCmdDir_AliasCdStillWorks(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	tempDir := t.TempDir()
	nextDir := filepath.Join(tempDir, "next")
	if err := os.Mkdir(nextDir, 0o755); err != nil {
		t.Fatalf("mkdir next dir: %v", err)
	}
	agent := &stubWorkDirAgent{workDir: tempDir}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetAdminFrom("admin1")

	e.handleCommand(p, &Message{SessionKey: "test:user1", UserID: "admin1", ReplyCtx: "ctx"}, "/cd next")

	if agent.workDir != nextDir {
		t.Fatalf("workDir = %q, want %q", agent.workDir, nextDir)
	}
}

func TestCmdDir_HelpShowsUsage(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubWorkDirAgent{workDir: "/tmp/project-a"}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdDir(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, []string{"help"})

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "/dir <path>") {
		t.Fatalf("sent = %q, want /dir usage", p.sent[0])
	}
}

func TestCmdDir_PersistsAbsoluteOverride(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	baseDir := t.TempDir()
	nextDir := filepath.Join(baseDir, "next")
	if err := os.Mkdir(nextDir, 0o755); err != nil {
		t.Fatalf("mkdir next dir: %v", err)
	}
	statePath := filepath.Join(t.TempDir(), "projects", "test.state.json")
	store := NewProjectStateStore(statePath)

	agent := &stubWorkDirAgent{workDir: baseDir}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetBaseWorkDir(baseDir)
	e.SetProjectStateStore(store)

	e.cmdDir(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, []string{"next"})

	reloaded := NewProjectStateStore(statePath)
	if got := reloaded.WorkDirOverride(); got != nextDir {
		t.Fatalf("WorkDirOverride() = %q, want %q", got, nextDir)
	}
}

func TestCmdDir_ResetRestoresBaseWorkDirAndClearsState(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	baseDir := t.TempDir()
	overrideDir := filepath.Join(baseDir, "override")
	if err := os.Mkdir(overrideDir, 0o755); err != nil {
		t.Fatalf("mkdir override dir: %v", err)
	}
	statePath := filepath.Join(t.TempDir(), "projects", "test.state.json")
	store := NewProjectStateStore(statePath)
	store.SetWorkDirOverride(overrideDir)
	store.Save()

	agent := &stubWorkDirAgent{workDir: overrideDir}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetBaseWorkDir(baseDir)
	e.SetProjectStateStore(store)
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("existing-session", "test")
	s.Name = "old"
	s.AddHistory("user", "hello")

	e.cmdDir(p, msg, []string{"reset"})

	if agent.workDir != baseDir {
		t.Fatalf("workDir = %q, want %q", agent.workDir, baseDir)
	}
	reloaded := NewProjectStateStore(statePath)
	if got := reloaded.WorkDirOverride(); got != "" {
		t.Fatalf("WorkDirOverride() = %q, want empty", got)
	}
	if s.GetAgentSessionID() != "" {
		t.Fatalf("AgentSessionID = %q, want cleared", s.GetAgentSessionID())
	}
	if s.Name != "old" {
		t.Fatalf("Name = %q, want unchanged", s.Name)
	}
	if len(s.History) != 0 {
		t.Fatalf("history length = %d, want 0", len(s.History))
	}
	if len(p.sent) != 1 || !strings.Contains(strings.ToLower(p.sent[0]), "default") {
		t.Fatalf("sent = %v, want reset success message", p.sent)
	}
}

func TestCmdDir_SwitchesByHistoryIndex(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	tempDir := t.TempDir()
	dir1 := filepath.Join(tempDir, "dir1")
	dir2 := filepath.Join(tempDir, "dir2")
	dir3 := filepath.Join(tempDir, "dir3")
	for _, d := range []string{dir1, dir2, dir3} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	dataDir := t.TempDir() // separate data dir for history
	agent := &stubWorkDirAgent{workDir: dir1}
	e := NewEngine("test", agent, []Platform{p}, dataDir)
	e.SetDirHistory(NewDirHistory(dataDir))

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	// Build history: dir1 -> dir2 -> dir3
	e.cmdDir(p, msg, []string{dir2})
	if agent.workDir != dir2 {
		t.Fatalf("after /dir dir2: workDir = %q, want %q", agent.workDir, dir2)
	}

	e.cmdDir(p, msg, []string{dir3})
	if agent.workDir != dir3 {
		t.Fatalf("after /dir dir3: workDir = %q, want %q", agent.workDir, dir3)
	}

	// Now history should be: [dir3, dir2, dir1] (dir1 might not be in history since it wasn't added initially)
	// Current dir is dir3
	// Index 2 should be dir2

	p.sent = nil
	e.cmdDir(p, msg, []string{"2"})

	// Should have switched to dir2
	if agent.workDir != dir2 {
		t.Fatalf("after /dir 2: workDir = %q, want %q", agent.workDir, dir2)
	}

	// Check the reply mentions dir2
	if len(p.sent) != 1 {
		t.Fatalf("sent = %d messages, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], dir2) {
		t.Fatalf("sent = %q, want message containing %q", p.sent[0], dir2)
	}
}

func TestCmdDir_DisplaysCorrectIndices(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	tempDir := t.TempDir()
	dir1 := filepath.Join(tempDir, "dir1")
	dir2 := filepath.Join(tempDir, "dir2")
	dir3 := filepath.Join(tempDir, "dir3")
	for _, d := range []string{dir1, dir2, dir3} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}

	dataDir := t.TempDir()
	agent := &stubWorkDirAgent{workDir: dir1}
	e := NewEngine("test", agent, []Platform{p}, dataDir)
	e.SetDirHistory(NewDirHistory(dataDir))

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	// Build history
	e.cmdDir(p, msg, []string{dir2})
	e.cmdDir(p, msg, []string{dir3})

	// Now current is dir3, history is [dir3, dir2]
	p.sent = nil
	e.cmdDir(p, msg, nil) // show current + history

	if len(p.sent) != 1 {
		t.Fatalf("sent = %d messages, want 1", len(p.sent))
	}

	// Verify the display shows:
	// - dir3 with (current) marker (current)
	// - dir2 with [ ] marker at index 2
	output := p.sent[0]

	// Check that dir3 is marked as current
	if !strings.Contains(output, "(current) 1. "+dir3) {
		t.Fatalf("output should contain '(current) 1. %s', got: %s", dir3, output)
	}

	// Check that dir2 is at index 2
	if !strings.Contains(output, "[ ] 2. "+dir2) {
		t.Fatalf("output should contain '[ ] 2. %s', got: %s", dir2, output)
	}
}

func TestCmdDir_ExpandsTilde(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Skip("cannot determine home dir:", err)
	}

	p := &stubPlatformEngine{n: "plain"}
	agent := &stubWorkDirAgent{workDir: homeDir}
	e := NewEngine("test", agent, []Platform{p}, t.TempDir())
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	tests := []struct {
		input   string
		wantDir string
	}{
		{"~", homeDir},
		{"~/", homeDir},
		{"~/Documents", filepath.Join(homeDir, "Documents")},
	}

	for _, tc := range tests {
		agent.workDir = homeDir
		// Ensure the target directory exists before switching
		if err := os.MkdirAll(tc.wantDir, 0o755); err != nil {
			t.Fatalf("MkdirAll %q: %v", tc.wantDir, err)
		}
		e.cmdDir(p, msg, []string{tc.input})
		if agent.workDir != tc.wantDir {
			t.Errorf("input %q: workDir = %q, want %q", tc.input, agent.workDir, tc.wantDir)
		}
	}
}

func TestEngine_AdminFrom_GatesDir(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	tempDir := t.TempDir()
	agent := &stubWorkDirAgent{workDir: tempDir}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:u1", UserID: "user1", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, "/dir .")

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	if !strings.Contains(strings.ToLower(p.sent[0]), "admin") {
		t.Fatalf("expected admin required message, got: %s", p.sent[0])
	}
	if agent.workDir != tempDir {
		t.Fatalf("workDir = %q, want unchanged %q", agent.workDir, tempDir)
	}
}

func TestCmdEffort_UsesInlineButtonsOnButtonOnlyPlatform(t *testing.T) {
	p := &stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "inline-only"}}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdEffort(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.buttonRows) == 0 {
		t.Fatal("expected /effort to send inline buttons on button-only platform")
	}
	if got := p.buttonRows[0][0].Data; got != "cmd:/effort 1" {
		t.Fatalf("first /effort button = %q, want %q", got, "cmd:/effort 1")
	}
	if got := p.buttonRows[0][0].Text; got != "low" {
		t.Fatalf("first /effort button text = %q, want low", got)
	}
}

func TestCmdEffort_SwitchesEffortAndPreservesSession(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("existing-session", "test")
	s.AddHistory("user", "hello")

	e.cmdEffort(p, msg, []string{"3"})

	if agent.reasoningEffort != "high" {
		t.Fatalf("reasoning effort = %q, want high", agent.reasoningEffort)
	}
	if s.GetAgentSessionID() != "existing-session" {
		t.Fatalf("AgentSessionID = %q, want existing-session", s.GetAgentSessionID())
	}
	if len(s.History) != 1 {
		t.Fatalf("history length = %d, want 1", len(s.History))
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "Reasoning effort switched to `high`") {
		t.Fatalf("sent = %v, want reasoning changed message", p.sent)
	}
}

func TestCmdEffort_RejectsMinimal(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	e.cmdEffort(p, msg, []string{"minimal"})

	if agent.reasoningEffort != "" {
		t.Fatalf("reasoning effort = %q, want unchanged empty", agent.reasoningEffort)
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "/effort <number>") || strings.Contains(p.sent[0], "minimal") {
		t.Fatalf("sent = %v, want usage without minimal", p.sent)
	}
}

// TestSwitchProvider_PersistsToSession verifies that `/provider switch <name>`
// records the choice on the Session so it survives a agent-bridge process
// restart. Without this, the agent_session_id keeps the conversation alive
// while the in-memory active provider reverts to default — see internal
// task t-20260614-qp7xnl.
func TestSwitchProvider_PersistsToSession(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubProviderAgent{
		providers: []ProviderConfig{{Name: "default-prov"}, {Name: "minimax"}},
		active:    "default-prov",
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("agent-sess-1", "test")

	e.cmdProvider(p, msg, []string{"switch", "minimax"})

	if got := s.GetActiveProvider(); got != "minimax" {
		t.Fatalf("session.ActiveProvider = %q, want %q", got, "minimax")
	}
	// switchProvider should also clear the agent_session_id (existing behavior).
	if got := s.GetAgentSessionID(); got != "" {
		t.Fatalf("session.AgentSessionID = %q, want cleared", got)
	}
}

// TestProviderClear_ClearsSessionActiveProvider verifies `/provider clear`
// also wipes the persisted choice so the next session starts with the
// agent's default provider again.
func TestProviderClear_ClearsSessionActiveProvider(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubProviderAgent{
		providers: []ProviderConfig{{Name: "default-prov"}, {Name: "minimax"}},
		active:    "minimax",
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}
	s := e.sessions.GetOrCreateActive(msg.SessionKey)
	s.SetActiveProvider("minimax")

	e.cmdProvider(p, msg, []string{"clear"})

	if got := s.GetActiveProvider(); got != "" {
		t.Fatalf("after clear: session.ActiveProvider = %q, want empty", got)
	}
}

// TestRestoreActiveProviderFromSession_AllPaths exercises every branch of the
// restore helper: agent without ProviderSwitcher, empty session value, agent
// already on the right provider (no-op), missing provider name (graceful
// warning), and the happy path that fixes t-20260614-qp7xnl.
func TestRestoreActiveProviderFromSession_AllPaths(t *testing.T) {
	t.Run("agent without ProviderSwitcher is a no-op", func(t *testing.T) {
		// stubAgent does not implement ProviderSwitcher.
		s := &Session{ID: "s1", ActiveProvider: "minimax"}
		// Must not panic.
		restoreActiveProviderFromSession(&stubAgent{}, s)
	})

	t.Run("empty session.ActiveProvider leaves agent untouched", func(t *testing.T) {
		agent := &stubProviderAgent{
			providers: []ProviderConfig{{Name: "a"}, {Name: "b"}},
			active:    "a",
		}
		s := &Session{ID: "s2"}
		restoreActiveProviderFromSession(agent, s)
		if agent.active != "a" {
			t.Fatalf("agent.active = %q, want %q (untouched)", agent.active, "a")
		}
	})

	t.Run("steady state: agent already on the right provider is a no-op", func(t *testing.T) {
		agent := &stubProviderAgent{
			providers: []ProviderConfig{{Name: "a"}, {Name: "b"}},
			active:    "b",
		}
		s := &Session{ID: "s3", ActiveProvider: "b"}
		restoreActiveProviderFromSession(agent, s)
		if agent.active != "b" {
			t.Fatalf("agent.active = %q, want %q", agent.active, "b")
		}
	})

	t.Run("missing provider name is a graceful warning, not a panic", func(t *testing.T) {
		agent := &stubProviderAgent{
			providers: []ProviderConfig{{Name: "a"}},
			active:    "a",
		}
		s := &Session{ID: "s4", ActiveProvider: "no-longer-exists"}
		restoreActiveProviderFromSession(agent, s)
		if agent.active != "a" {
			t.Fatalf("agent.active = %q, want %q (unchanged on unknown provider)", agent.active, "a")
		}
	})

	t.Run("post-restart restore: agent is rebound to the persisted provider", func(t *testing.T) {
		// Simulates the t-20260614-qp7xnl scenario: process restarted, so
		// in-memory activeIdx is at the default ("a"), but the session
		// recorded that the user previously switched to "minimax".
		agent := &stubProviderAgent{
			providers: []ProviderConfig{{Name: "a"}, {Name: "minimax"}},
			active:    "a",
		}
		s := &Session{ID: "s5", ActiveProvider: "minimax"}
		restoreActiveProviderFromSession(agent, s)
		if agent.active != "minimax" {
			t.Fatalf("agent.active = %q, want %q (restored from session)", agent.active, "minimax")
		}
	})
}

func TestCmdMode_UsesInlineButtonsOnButtonOnlyPlatform(t *testing.T) {
	p := &stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "inline-only"}}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.cmdMode(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.buttonRows) == 0 {
		t.Fatal("expected /mode to send inline buttons on button-only platform")
	}
	if got := p.buttonRows[0][0].Data; got != "cmd:/mode default" {
		t.Fatalf("first /mode button = %q, want %q", got, "cmd:/mode default")
	}
	if !strings.Contains(p.buttonContent, "Available: `default` / `yolo`") {
		t.Fatalf("button content = %q, want dynamic mode list", p.buttonContent)
	}
	if strings.Contains(p.buttonContent, "`edit`") {
		t.Fatalf("button content = %q, want no hardcoded mode list", p.buttonContent)
	}
}

func TestCmdMode_AppliesLiveModeWithoutReset(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	live := &stubLiveModeSession{}
	state := &interactiveState{agentSession: live, platform: p, replyCtx: "ctx"}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	session := e.sessions.GetOrCreateActive(key)
	session.SetAgentSessionID("existing-session", "stub")
	session.AddHistory("user", "hello")

	e.cmdMode(p, &Message{SessionKey: key, ReplyCtx: "ctx"}, []string{"yolo"})

	if len(live.modes) != 1 || live.modes[0] != "yolo" {
		t.Fatalf("live modes = %v, want [yolo]", live.modes)
	}
	if session.GetAgentSessionID() != "existing-session" {
		t.Fatalf("agent session id = %q, want existing-session", session.GetAgentSessionID())
	}
	if len(session.GetHistory(0)) != 1 {
		t.Fatalf("history len = %d, want 1", len(session.GetHistory(0)))
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "Current session updated immediately.") {
		t.Fatalf("sent = %v, want live mode update reply", p.sent)
	}
	if got := agent.GetMode(); got != "yolo" {
		t.Fatalf("agent mode = %q, want yolo", got)
	}
}

func TestCmdQuiet_TogglesDisplay(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{Mode: "full", ThinkingMessages: true, ToolMessages: true, ThinkingMaxLen: 300, ToolMaxLen: 500})
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}

	// 1st /quiet: full → quiet
	e.cmdQuiet(p, msg, nil)
	if e.display.Mode != "quiet" || e.display.ThinkingMessages || e.display.ToolMessages {
		t.Fatalf("after 1st /quiet: Mode=%q, TM=%v, Tool=%v, want quiet/false/false",
			e.display.Mode, e.display.ThinkingMessages, e.display.ToolMessages)
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "Quiet mode ON") {
		t.Fatalf("sent = %q, want quiet ON message", p.sent)
	}

	// 2nd /quiet: quiet → compact
	p.sent = nil
	e.cmdQuiet(p, msg, nil)
	if e.display.Mode != "compact" || e.display.ThinkingMessages || e.display.ToolMessages {
		t.Fatalf("after 2nd /quiet: Mode=%q, TM=%v, Tool=%v, want compact/false/false",
			e.display.Mode, e.display.ThinkingMessages, e.display.ToolMessages)
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "Compact mode") {
		t.Fatalf("sent = %q, want compact mode message", p.sent)
	}

	// 3rd /quiet: compact → full
	p.sent = nil
	e.cmdQuiet(p, msg, nil)
	if e.display.Mode != "full" || !e.display.ThinkingMessages || !e.display.ToolMessages {
		t.Fatalf("after 3rd /quiet: Mode=%q, TM=%v, Tool=%v, want full/true/true",
			e.display.Mode, e.display.ThinkingMessages, e.display.ToolMessages)
	}
	if len(p.sent) != 1 || !strings.Contains(p.sent[0], "Quiet mode OFF") {
		t.Fatalf("sent = %q, want quiet OFF message", p.sent)
	}

	// /quiet with explicit argument
	p.sent = nil
	e.cmdQuiet(p, msg, []string{"compact"})
	if e.display.Mode != "compact" {
		t.Fatalf("after /quiet compact: Mode=%q, want compact", e.display.Mode)
	}
}

func TestHandleMessage_ExtraContentPreservedThroughAlias(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &stubAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	e.aliasMu.Lock()
	e.aliases["hi"] = "hello world"
	e.aliasMu.Unlock()

	msg := &Message{
		SessionKey:   "test:user1",
		ReplyCtx:     "ctx",
		Content:      "hi",
		ExtraContent: "> quoted reply context",
		Platform:     "test",
		UserID:       "user1",
	}

	e.handleMessage(p, msg)

	if !strings.Contains(msg.Content, "> quoted reply context") {
		t.Fatalf("ExtraContent lost after alias resolution: msg.Content = %q", msg.Content)
	}
	if !strings.Contains(msg.Content, "hello world") {
		t.Fatalf("alias not resolved: msg.Content = %q", msg.Content)
	}
}

func TestHandleMessage_ExtraContentOnlyIsProcessed(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &stubAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{
		SessionKey:   "test:user1",
		ReplyCtx:     "ctx",
		Content:      "",
		ExtraContent: "> quoted reply context",
		Platform:     "test",
		UserID:       "user1",
		MessageID:    "m-extra-only",
	}

	e.handleMessage(p, msg)

	if msg.Content != "> quoted reply context" {
		t.Fatalf("Content = %q, want ExtraContent to become message content", msg.Content)
	}
}

// TestHandleMessage_PureAttachmentMessage_StagedNotSentToAgent verifies a
// text-less image/file message is staged (and acknowledged) without ever
// reaching the agent — the core-level fix for platforms (Telegram, WeChat
// Work) sending each photo in a multi-image message as its own core.Message.
func TestHandleMessage_PureAttachmentMessage_StagedNotSentToAgent(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "",
		Images:     []ImageAttachment{{MimeType: "image/png", Data: []byte("img1")}},
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %v, want exactly one staged-attachment acknowledgment", sent)
	}
	if sent[0] != e.i18n.T(MsgAttachmentStaged) {
		t.Fatalf("reply = %q, want MsgAttachmentStaged text", sent[0])
	}
	if len(agentSession.getSentPrompts()) != 0 {
		t.Fatalf("agent received a prompt for a pure-attachment message: %v", agentSession.getSentPrompts())
	}
}

// TestHandleMessage_StagedAttachment_MergedIntoNextTextMessage verifies a
// staged image is merged into the next text message from the same session
// and delivered to the agent as a single turn.
func TestHandleMessage_StagedAttachment_MergedIntoNextTextMessage(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")

	imgMsg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "",
		Images:     []ImageAttachment{{MimeType: "image/png", Data: []byte("img1")}},
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, imgMsg)

	textMsg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "describe this",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, textMsg)

	deadline := time.After(2 * time.Second)
	for {
		if len(agentSession.getSentPrompts()) >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for agent turn")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	if len(textMsg.Images) != 1 {
		t.Fatalf("textMsg.Images = %d, want the staged image merged in", len(textMsg.Images))
	}
}

// TestHandleMessage_PureAttachmentMessage_NotStagedWhenSessionBusy verifies
// that a pure-attachment message arriving while the agent is mid-turn is NOT
// staged — the staging (📎) behavior is reserved for an idle agent. A busy
// session routes the file through the normal busy dispatch instead
// (interrupt-and-retry when the agent supports cancellation, the busy queue
// otherwise), so it is inserted directly rather than silently held back.
func TestHandleMessage_PureAttachmentMessage_NotStagedWhenSessionBusy(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")

	session := e.sessions.GetOrCreateActive("test:user1")
	if !session.TryLock() {
		t.Fatalf("failed to lock session for busy simulation")
	}
	defer session.Unlock()

	msg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "",
		Images:     []ImageAttachment{{MimeType: "image/png", Data: []byte("img1")}},
		ReplyCtx:   "ctx",
		MessageID:  "file-msg-1",
	}
	e.handleMessage(p, msg)

	// Busy session must NOT acknowledge with the staging message.
	sent := p.getSent()
	for _, s := range sent {
		if s == e.i18n.T(MsgAttachmentStaged) {
			t.Fatalf("pure-attachment message staged while session busy: sent=%v", sent)
		}
	}

	// It must also not be held in staged attachments.
	e.interactiveMu.Lock()
	state := e.interactiveStates["test:user1"]
	e.interactiveMu.Unlock()
	if state == nil {
		// No interactiveState could be created if the placeholder was never
		// made; treat that as a pass only if nothing was sent either.
		if len(sent) == 0 {
			return
		}
		t.Fatalf("no interactiveState created for busy dispatch")
	}
	e.interactiveMu.Lock()
	staged := e.stagedAttachments["test:user1"]
	e.interactiveMu.Unlock()
	if staged != nil {
		t.Fatalf("attachments staged while session busy: %+v", staged)
	}
}

// TestHandleMessage_NewSessionClearsStagedAttachments verifies /new's
// cleanupInteractiveState path discards any staged attachments so a stale
// image from a previous conversation cannot leak into the next one.
func TestHandleMessage_NewSessionClearsStagedAttachments(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")

	imgMsg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "",
		Images:     []ImageAttachment{{MimeType: "image/png", Data: []byte("img1")}},
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, imgMsg)

	e.cmdNew(p, &Message{SessionKey: "test:user1", Platform: "test", ReplyCtx: "ctx"}, nil)

	textMsg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello again",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, textMsg)

	deadline := time.After(2 * time.Second)
	for {
		if len(agentSession.getSentPrompts()) >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for agent turn")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	if len(textMsg.Images) != 0 {
		t.Fatalf("textMsg.Images = %d, want 0 — staged attachment should have been cleared by /new", len(textMsg.Images))
	}
}

func TestHandleMessage_OnAcceptedOnlyRunsForAgentTurn(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	commandAccepted := false
	e.handleMessage(p, &Message{
		SessionKey: "test:command-user",
		ReplyCtx:   "ctx",
		Content:    "/help",
		Platform:   "test",
		UserID:     "command-user",
		OnAccepted: func() { commandAccepted = true },
	})
	if commandAccepted {
		t.Fatal("handled command consumed agent-bound context")
	}

	agentAccepted := false
	e.handleMessage(p, &Message{
		SessionKey: "test:agent-user",
		ReplyCtx:   "ctx",
		Content:    "hello",
		Platform:   "test",
		UserID:     "agent-user",
		OnAccepted: func() { agentAccepted = true },
	})
	if !agentAccepted {
		t.Fatal("agent-bound message did not run OnAccepted")
	}
}

func TestCmdDiff_RejectsDashTarget(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	msg := &Message{SessionKey: "test:user1", ReplyCtx: "ctx", UserID: "admin1"}
	e.SetAdminFrom("admin1")

	e.handleCommand(p, msg, "/diff --output=/tmp/evil")

	if len(p.sent) == 0 {
		t.Fatal("expected error reply for dash target")
	}
	if !strings.Contains(p.sent[0], "must not start with '-'") {
		t.Fatalf("sent = %q, want rejection of dash target", p.sent[0])
	}
}

func TestCmdCommands_UsesLegacyTextOnPlatformWithoutCardSupport(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddCommand("deploy", "Deploy app", "ship it", "", "", "config")

	e.cmdCommands(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "/deploy") {
		t.Fatalf("commands text = %q, want legacy command list", p.sent[0])
	}
	if strings.Contains(p.sent[0], "[← Back]") {
		t.Fatalf("commands text = %q, should not be card fallback text", p.sent[0])
	}
}

func TestCmdAlias_UsesLegacyTextOnPlatformWithoutCardSupport(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.AddAlias("ls", "/list")

	e.cmdAlias(p, &Message{SessionKey: "test:user1", ReplyCtx: "ctx"}, nil)

	if len(p.sent) != 1 {
		t.Fatalf("sent messages = %d, want 1", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "ls") || !strings.Contains(p.sent[0], "/list") {
		t.Fatalf("alias text = %q, want legacy alias list", p.sent[0])
	}
	if strings.Contains(p.sent[0], "[← Back]") {
		t.Fatalf("alias text = %q, should not be card fallback text", p.sent[0])
	}
}

func TestRenderListCard_MakesEveryVisibleSessionClickable(t *testing.T) {
	sessions := make([]AgentSessionInfo, 0, 7)
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 7; i++ {
		sessions = append(sessions, AgentSessionInfo{
			ID:           "agent-session-" + string(rune('A'+i)),
			Summary:      "Session summary",
			MessageCount: i + 1,
			ModifiedAt:   base.Add(time.Duration(i) * time.Minute),
		})
	}

	e := NewEngine("test", &stubListAgent{sessions: sessions}, []Platform{&stubPlatformEngine{n: "test"}}, "")
	// Register all agent sessions with the session manager so they pass the
	// owned-session filter (simulates agent-bridge having created each session).
	var internalIDs []string
	for i, s := range sessions {
		sess := e.sessions.NewSession("test:user1", "session-"+string(rune('A'+i)))
		sess.SetAgentSessionID(s.ID, "test")
		internalIDs = append(internalIDs, sess.ID)
	}
	// Switch active to the session mapped to sessions[5] (agent-session-F).
	e.sessions.SwitchSession("test:user1", internalIDs[5])

	card, err := e.renderListCard("test:user1", 1)
	if err != nil {
		t.Fatalf("renderListCard returned error: %v", err)
	}

	if got := countCardActionValues(card, "act:/switch "); got != len(sessions) {
		t.Fatalf("switch action count = %d, want %d", got, len(sessions))
	}

	btn, ok := findCardAction(card, "act:/switch 6")
	if !ok {
		t.Fatal("expected active session switch action to exist")
	}
	if btn.Type != "primary" {
		t.Fatalf("active session button type = %q, want primary", btn.Type)
	}
}

func TestRenderDirCard_HistoryRowsUseSelectActions(t *testing.T) {
	tempDir := t.TempDir()
	dir1 := filepath.Join(tempDir, "dir1")
	dir2 := filepath.Join(tempDir, "dir2")
	for _, d := range []string{dir1, dir2} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	dataDir := t.TempDir()
	agent := &stubWorkDirAgent{workDir: dir2}
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, dataDir)
	e.SetDirHistory(NewDirHistory(dataDir))
	e.dirHistory.Add("test", dir1)
	e.dirHistory.Add("test", dir2)

	card, err := e.renderDirCard("test:user1", 1)
	if err != nil {
		t.Fatalf("renderDirCard: %v", err)
	}
	if got := countCardActionValues(card, "act:/dir select "); got != 2 {
		t.Fatalf("dir select actions = %d, want 2", got)
	}
}

func TestHandleCardNav_DirSelectSwitchesWorkDir(t *testing.T) {
	temp := t.TempDir()
	d1 := filepath.Join(temp, "a")
	d2 := filepath.Join(temp, "b")
	d3 := filepath.Join(temp, "c")
	for _, d := range []string{d1, d2, d3} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
	}
	dataDir := t.TempDir()
	agent := &stubWorkDirAgent{workDir: d3}
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, dataDir)
	e.SetDirHistory(NewDirHistory(dataDir))
	e.dirHistory.Add("test", d1)
	e.dirHistory.Add("test", d2)
	e.dirHistory.Add("test", d3)

	sk := "test:user1"
	_ = e.handleCardNav("act:/dir select 2", sk)
	if agent.workDir != d2 {
		t.Fatalf("workDir = %q, want %q", agent.workDir, d2)
	}
	card := e.handleCardNav("nav:/dir 1", sk)
	if card == nil {
		t.Fatal("expected dir card after nav")
	}
}

func TestRenderHelpCard_DefaultsToSessionTab(t *testing.T) {
	e := NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, "")

	card := e.renderHelpCard()
	text := card.RenderText()

	if got := countCardActionValues(card, "nav:/help "); got != 4 {
		t.Fatalf("help tab action count = %d, want 4", got)
	}
	btn, ok := findCardAction(card, "nav:/help session")
	if !ok {
		t.Fatal("expected session help tab to exist")
	}
	if btn.Type != "primary" {
		t.Fatalf("session help tab type = %q, want primary", btn.Type)
	}
	if btn.Text != "Session Management" {
		t.Fatalf("session help tab text = %q, want full title", btn.Text)
	}
	if !strings.Contains(text, "**/new**") {
		t.Fatalf("default help text = %q, want session commands", text)
	}
	if strings.Contains(text, "**Session Management**") {
		t.Fatalf("default help text = %q, should not repeat tab title in body", text)
	}
	if strings.Contains(text, "**/model**") {
		t.Fatalf("default help text = %q, should not include agent commands", text)
	}
}

func TestHandleCardNav_HelpSwitchesTabs(t *testing.T) {
	e := NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, "")

	card := e.handleCardNav("nav:/help agent", "test:user1")
	if card == nil {
		t.Fatal("expected help nav card")
	}
	text := card.RenderText()

	if !strings.Contains(text, "**/model**") {
		t.Fatalf("agent help text = %q, want agent commands", text)
	}
	if strings.Contains(text, "**Agent Configuration**") {
		t.Fatalf("agent help text = %q, should not repeat tab title in body", text)
	}
	if strings.Contains(text, "**/new**") {
		t.Fatalf("agent help text = %q, should not include session commands", text)
	}
}

// --- AskUserQuestion tests ---

func testQuestions() []UserQuestion {
	return []UserQuestion{{
		Question: "Which database?",
		Header:   "Setup",
		Options: []UserQuestionOption{
			{Label: "PostgreSQL", Description: "Recommended for production"},
			{Label: "SQLite", Description: "Lightweight, file-based"},
			{Label: "MySQL", Description: "Popular open-source"},
		},
		MultiSelect: false,
	}}
}

func testMultiQuestions() []UserQuestion {
	return []UserQuestion{
		{
			Question: "Which database?",
			Header:   "Database",
			Options: []UserQuestionOption{
				{Label: "PostgreSQL"},
				{Label: "SQLite"},
			},
		},
		{
			Question: "Which framework?",
			Header:   "Framework",
			Options: []UserQuestionOption{
				{Label: "Gin"},
				{Label: "Echo"},
			},
		},
	}
}

func TestResolveAskQuestionAnswer_NumericIndex(t *testing.T) {
	e := newTestEngine()
	q := testQuestions()[0]
	got := e.resolveAskQuestionAnswer(q, "2")
	if got != "SQLite" {
		t.Errorf("expected SQLite, got %s", got)
	}
}

func TestResolveAskQuestionAnswer_ButtonCallback(t *testing.T) {
	e := newTestEngine()
	q := testQuestions()[0]
	got := e.resolveAskQuestionAnswer(q, "askq:0:1")
	if got != "PostgreSQL" {
		t.Errorf("expected PostgreSQL, got %s", got)
	}
}

func TestResolveAskQuestionAnswer_FreeText(t *testing.T) {
	e := newTestEngine()
	q := testQuestions()[0]
	got := e.resolveAskQuestionAnswer(q, "Redis")
	if got != "Redis" {
		t.Errorf("expected Redis, got %s", got)
	}
}

func TestResolveAskQuestionAnswer_MultiSelect(t *testing.T) {
	e := newTestEngine()
	q := testQuestions()[0]
	q.MultiSelect = true
	got := e.resolveAskQuestionAnswer(q, "1,3")
	if got != "PostgreSQL, MySQL" {
		t.Errorf("expected 'PostgreSQL, MySQL', got %s", got)
	}
}

func TestResolveAskQuestionAnswer_OutOfRange(t *testing.T) {
	e := newTestEngine()
	q := testQuestions()[0]
	got := e.resolveAskQuestionAnswer(q, "99")
	if got != "99" {
		t.Errorf("expected raw '99' for out-of-range, got %s", got)
	}
}

func TestBuildAskQuestionResponse(t *testing.T) {
	input := map[string]any{
		"questions": []any{map[string]any{"question": "Which?"}},
	}
	collected := map[int]string{0: "PostgreSQL", 1: "Gin"}
	result := buildAskQuestionResponse(input, testMultiQuestions(), collected)
	answers, ok := result["answers"].(map[string]any)
	if !ok {
		t.Fatal("expected answers map")
	}
	if answers["Which database?"] != "PostgreSQL" {
		t.Errorf("expected answer[Which database?]=PostgreSQL, got %v", answers["Which database?"])
	}
	if answers["Which framework?"] != "Gin" {
		t.Errorf("expected answer[Which framework?]=Gin, got %v", answers["Which framework?"])
	}
	if _, ok := result["questions"]; !ok {
		t.Error("expected original questions to be preserved")
	}
}

func TestSendAskQuestionPrompt_CardPlatform(t *testing.T) {
	e := newTestEngine()
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e.sendAskQuestionPrompt(p, "ctx", testQuestions(), 0)

	if len(p.sentCards) != 1 {
		t.Fatalf("expected 1 card, got %d", len(p.sentCards))
	}
	card := p.sentCards[0]
	if card.Header == nil || card.Header.Color != "blue" {
		t.Errorf("expected blue header, got %+v", card.Header)
	}
	askqCount := countCardActionValues(card, "askq:")
	if askqCount != 3 {
		t.Errorf("expected 3 askq buttons, got %d", askqCount)
	}
}

func TestSendAskQuestionPrompt_CardPlatform_MultiQuestion_ShowsIndex(t *testing.T) {
	e := newTestEngine()
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	qs := testMultiQuestions()
	e.sendAskQuestionPrompt(p, "ctx", qs, 0)

	if len(p.sentCards) != 1 {
		t.Fatalf("expected 1 card, got %d", len(p.sentCards))
	}
	card := p.sentCards[0]
	if !strings.Contains(card.Header.Title, "(1/2)") {
		t.Errorf("expected (1/2) in title, got %s", card.Header.Title)
	}
}

func TestSendAskQuestionPrompt_InlineButtonPlatform(t *testing.T) {
	e := newTestEngine()
	p := &stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}}
	e.sendAskQuestionPrompt(p, "ctx", testQuestions(), 0)

	if len(p.buttonRows) != 3 {
		t.Fatalf("expected 3 button rows, got %d", len(p.buttonRows))
	}
	if p.buttonRows[0][0].Data != "askq:0:1" {
		t.Errorf("expected askq:0:1, got %s", p.buttonRows[0][0].Data)
	}
}

func TestSendAskQuestionPrompt_PlainPlatform(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "plain"}
	e.sendAskQuestionPrompt(p, "ctx", testQuestions(), 0)

	if len(p.sent) != 1 {
		t.Fatal("expected 1 message")
	}
	msg := p.sent[0]
	if !strings.Contains(msg, "Which database?") {
		t.Errorf("expected question text, got %s", msg)
	}
	if !strings.Contains(msg, "1. **PostgreSQL**") {
		t.Errorf("expected numbered options, got %s", msg)
	}
}

func TestProcessInteractiveEvents_AskUserQuestionFromAgent_RendersRichCardPrompt(t *testing.T) {
	p := &stubAskQuestionRichCardPlatform{
		stubCardPlatform: stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}},
	}
	sess := newBlockingSendSession("codex-ask-card")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, "")
	e.SetDisplayConfig(DisplayCfg{
		Mode:             "full",
		CardMode:         "rich",
		ThinkingMessages: true,
		ThinkingMaxLen:   defaultThinkingMaxLen,
		ToolMaxLen:       defaultToolMaxLen,
		ToolMessages:     true,
	})

	key := "test:chat:user1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sess.Send("prompt", "", nil, nil)
	}()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "m-codex-ask-card", time.Now(), nil, sendDone, nil)
		close(done)
	}()

	select {
	case <-sess.sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not reach blocking wait")
	}

	sess.events <- Event{
		Type:         EventPermissionRequest,
		RequestID:    `"rui-card"`,
		ToolName:     "AskUserQuestion",
		ToolInput:    `{"questions":[{"id":"database","question":"Which database?"}]}`,
		ToolInputRaw: map[string]any{"questions": []any{map[string]any{"id": "database", "question": "Which database?"}}},
		Questions:    testQuestions(),
	}

	card := waitForSentCard(t, &p.stubCardPlatform)
	if card.Header == nil || card.Header.Color != "blue" {
		t.Fatalf("card header = %#v, want blue AskUserQuestion card", card.Header)
	}
	if countCardActionValues(card, "askq:") != 3 {
		t.Fatalf("askq button count = %d, want 3", countCardActionValues(card, "askq:"))
	}

	if !e.handlePendingPermission(p, &Message{
		SessionKey: key,
		UserID:     "user1",
		Content:    "askq:0:2",
		ReplyCtx:   "ctx",
	}, "askq:0:2", key) {
		t.Fatal("expected AskUserQuestion answer to resolve pending request")
	}

	close(sess.unblock)
	sess.events <- Event{Type: EventResult, Content: "ok", Done: true}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processInteractiveEvents did not complete")
	}
}

func TestProcessInteractiveEvents_AskUserQuestionFromAgent_RendersLegacyPrompt(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	sess := newBlockingSendSession("codex-ask-legacy")
	e := NewEngine("test", &controllableAgent{nextSession: sess}, []Platform{p}, "")

	key := "test:chat:user1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sess.Send("prompt", "", nil, nil)
	}()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "m-codex-ask-legacy", time.Now(), nil, sendDone, nil)
		close(done)
	}()

	select {
	case <-sess.sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not reach blocking wait")
	}

	sess.events <- Event{
		Type:         EventPermissionRequest,
		RequestID:    `"rui-legacy"`,
		ToolName:     "AskUserQuestion",
		ToolInput:    `{"questions":[{"id":"database","question":"Which database?"}]}`,
		ToolInputRaw: map[string]any{"questions": []any{map[string]any{"id": "database", "question": "Which database?"}}},
		Questions:    testQuestions(),
	}

	msg := waitForSentText(t, p)
	if !strings.Contains(msg, "Which database?") || !strings.Contains(msg, "1. **PostgreSQL**") {
		t.Fatalf("legacy AskUserQuestion prompt = %q, want question and numbered options", msg)
	}

	if !e.handlePendingPermission(p, &Message{
		SessionKey: key,
		UserID:     "user1",
		Content:    "2",
		ReplyCtx:   "ctx",
	}, "2", key) {
		t.Fatal("expected AskUserQuestion answer to resolve pending request")
	}

	close(sess.unblock)
	sess.events <- Event{Type: EventResult, Content: "ok", Done: true}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processInteractiveEvents did not complete")
	}
}

func TestHandlePendingPermission_AskUserQuestion_SingleQuestion(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID: "req-1",
			ToolName:  "AskUserQuestion",
			ToolInput: map[string]any{
				"questions": []any{map[string]any{"question": "Which?"}},
			},
			Questions: testQuestions(),
			Resolved:  make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates["test:chat:user1"] = state
	e.interactiveMu.Unlock()

	handled := e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "2",
		ReplyCtx:   "ctx",
	}, "2", "")

	if !handled {
		t.Fatal("expected handlePendingPermission to return true")
	}
	if rec.calls != 1 {
		t.Fatalf("expected 1 RespondPermission call, got %d", rec.calls)
	}
	answers, ok := rec.lastResult.UpdatedInput["answers"].(map[string]any)
	if !ok {
		t.Fatal("expected answers in updatedInput")
	}
	if answers["Which database?"] != "SQLite" {
		t.Errorf("expected answer=SQLite, got %v", answers["Which database?"])
	}

	state.mu.Lock()
	if state.pending != nil {
		t.Error("expected pending to be cleared after response")
	}
	state.mu.Unlock()
}

func TestHandlePendingPermission_AskUserQuestion_MultiQuestion_Sequential(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	qs := testMultiQuestions()
	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID: "req-1",
			ToolName:  "AskUserQuestion",
			ToolInput: map[string]any{"questions": []any{}},
			Questions: qs,
			Resolved:  make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates["test:chat:user1"] = state
	e.interactiveMu.Unlock()

	// Answer question 0 — should NOT resolve yet
	handled := e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "1",
		ReplyCtx:   "ctx",
	}, "1", "")
	if !handled {
		t.Fatal("expected handled=true for question 0")
	}
	if rec.calls != 0 {
		t.Fatalf("should not have called RespondPermission yet, got %d calls", rec.calls)
	}
	state.mu.Lock()
	if state.pending == nil {
		t.Fatal("pending should still exist (more questions)")
	}
	if state.pending.CurrentQuestion != 1 {
		t.Errorf("expected CurrentQuestion=1, got %d", state.pending.CurrentQuestion)
	}
	state.mu.Unlock()

	// Answer question 1 — should resolve
	handled = e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "2",
		ReplyCtx:   "ctx",
	}, "2", "")
	if !handled {
		t.Fatal("expected handled=true for question 1")
	}
	if rec.calls != 1 {
		t.Fatalf("expected 1 RespondPermission call, got %d", rec.calls)
	}
	answers, ok := rec.lastResult.UpdatedInput["answers"].(map[string]any)
	if !ok {
		t.Fatal("expected answers in updatedInput")
	}
	if answers["Which database?"] != "PostgreSQL" {
		t.Errorf("expected answer[Which database?]=PostgreSQL, got %v", answers["Which database?"])
	}
	if answers["Which framework?"] != "Echo" {
		t.Errorf("expected answer[Which framework?]=Echo, got %v", answers["Which framework?"])
	}

	state.mu.Lock()
	if state.pending != nil {
		t.Error("expected pending to be cleared after all questions answered")
	}
	state.mu.Unlock()
}

func TestHandlePendingPermission_AskUserQuestion_SkipsPermFlow(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID: "req-1",
			ToolName:  "AskUserQuestion",
			ToolInput: map[string]any{
				"questions": []any{map[string]any{"question": "Which?"}},
			},
			Questions: testQuestions(),
			Resolved:  make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates["test:chat:user1"] = state
	e.interactiveMu.Unlock()

	// "allow" should NOT be interpreted as permission allow; should be treated as free text answer
	handled := e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "allow",
		ReplyCtx:   "ctx",
	}, "allow", "")

	if !handled {
		t.Fatal("expected handled=true")
	}
	answers, ok := rec.lastResult.UpdatedInput["answers"].(map[string]any)
	if !ok {
		t.Fatal("expected answers in updatedInput")
	}
	if answers["Which database?"] != "allow" {
		t.Errorf("expected free text 'allow' as answer, got %v", answers["Which database?"])
	}
}

// TestHandlePendingPermission_ExtensionConfirm_AllowIsPermissionAllow is a
// regression test for the fcb1beae regression: extension_confirm events
// (forwarded by the pi session for ctx.ui.confirm, used by extensions like
// permission-gate to ask the user for tool-use permission) must be handled
// as a regular permission request. "allow" / "deny" / "allow all" must be
// interpreted as permission decisions, NOT as free-text answers to a
// AskUserQuestion-style Yes/No question.
//
// The bug: fcb1beae routed extension_confirm through the AskUserQuestion
// flow (populated Questions=[{Yes, No}] in forwardConfirm). That made the
// engine render a Yes/No question card on Feishu instead of an Allow/Deny
// permission card, breaking the UX for permission-gate and any other
// extension that uses ctx.ui.confirm for permission decisions.
func TestHandlePendingPermission_ExtensionConfirm_AllowIsPermissionAllow(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID: "pi_ext_cfm-1",
			ToolName:  "extension_confirm",
			ToolInput: map[string]any{
				"title":   "Command needs confirmation",
				"message": "rm -rf /tmp/foo",
				"method":  "confirm",
			},
			// No Questions field — extension_confirm must NOT carry one.
			// Questions: nil,
			Resolved: make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates["test:chat:user1"] = state
	e.interactiveMu.Unlock()

	// User clicks "allow" — must be treated as permission allow, not as a
	// free-text answer to a Yes/No question.
	if !e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "allow",
		ReplyCtx:   "ctx",
	}, "allow", "") {
		t.Fatal("expected handlePendingPermission to return true")
	}

	if rec.calls != 1 {
		t.Fatalf("RespondPermission calls = %d, want 1", rec.calls)
	}
	if rec.lastResult.Behavior != "allow" {
		t.Fatalf("Behavior = %q, want %q (extension_confirm must treat 'allow' as permission allow)", rec.lastResult.Behavior, "allow")
	}
	if _, ok := rec.lastResult.UpdatedInput["answers"]; ok {
		t.Errorf("UpdatedInput must not carry AskUserQuestion answers, got %v", rec.lastResult.UpdatedInput)
	}

	state.mu.Lock()
	if state.pending != nil {
		t.Error("expected pending to be cleared after 'allow'")
	}
	state.mu.Unlock()
}

// TestHandlePendingPermission_ExtensionConfirm_DenyIsPermissionDeny covers
// the deny half of the same regression: "deny" must produce Behavior="deny"
// and clear the pending state.
func TestHandlePendingPermission_ExtensionConfirm_DenyIsPermissionDeny(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID: "pi_ext_cfm-1",
			ToolName:  "extension_confirm",
			ToolInput: map[string]any{
				"title":   "Command needs confirmation",
				"message": "rm -rf /tmp/foo",
				"method":  "confirm",
			},
			Resolved: make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates["test:chat:user1"] = state
	e.interactiveMu.Unlock()

	if !e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "deny",
		ReplyCtx:   "ctx",
	}, "deny", "") {
		t.Fatal("expected handlePendingPermission to return true")
	}

	if rec.lastResult.Behavior != "deny" {
		t.Fatalf("Behavior = %q, want %q", rec.lastResult.Behavior, "deny")
	}
}

// TestHandlePendingPermission_ExtensionSelect_StillRoutedAsAskUserQuestion
// is the counterpart guard test: extension_select (used by the questionnaire
// extension in RPC mode) MUST still be routed through the AskUserQuestion
// flow, with its Questions field preserved. This test exists so that a future
// refactor of the AskUserQuestion detection doesn't accidentally pull
// extension_select back into the regular permission path and break the
// button-card UX for questionnaire / multi-choice prompts.
func TestHandlePendingPermission_ExtensionSelect_StillRoutedAsAskUserQuestion(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID: "pi_ext_sel-1",
			ToolName:  "extension_select",
			ToolInput: map[string]any{
				"title":   "Pick a color",
				"options": []any{"Red", "Green", "Blue"},
				"method":  "select",
			},
			Questions: []UserQuestion{{
				Question: "Pick a color",
				Header:   "Select",
				Options: []UserQuestionOption{
					{Label: "Red"},
					{Label: "Green"},
					{Label: "Blue"},
				},
				MultiSelect: false,
			}},
			Resolved: make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates["test:chat:user1"] = state
	e.interactiveMu.Unlock()

	// "allow" must be treated as a free-text answer to the question, NOT as
	// a permission allow. If this assertion fails, the engine has pulled
	// extension_select back into the regular permission path.
	if !e.handlePendingPermission(p, &Message{
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		Content:    "allow",
		ReplyCtx:   "ctx",
	}, "allow", "") {
		t.Fatal("expected handlePendingPermission to return true")
	}

	answers, ok := rec.lastResult.UpdatedInput["answers"].(map[string]any)
	if !ok {
		t.Fatalf("expected answers in updatedInput (AskUserQuestion flow), got %v", rec.lastResult.UpdatedInput)
	}
	if answers["Pick a color"] != "allow" {
		t.Errorf("expected answer 'allow' preserved as free text, got %v", answers["Pick a color"])
	}
	// The decisive signal that this is the AskUserQuestion path (not the
	// regular permission path) is the presence of an "answers" key in
	// UpdatedInput. extension_select must take this path; extension_confirm
	// (the regression) must NOT. If a future refactor ever strips
	// extension_select out of the AskUserQuestion list, this test catches it.
}

// TestHandlePendingPermission_CronFallback verifies that the fallback path
// in handlePendingPermission can locate a pending permission stored under a
// session composite key when the callback uses the
// plain sessionKey.

// ──────────────────────────────────────────────────────────────
// Session routing / cleanup CAS tests
// ──────────────────────────────────────────────────────────────

// controllableAgentSession is an AgentSession stub whose session ID, liveness,
// and events channel can be controlled by the test.
type controllableAgentSession struct {
	sessionID       string
	alive           bool
	events          chan Event
	closed          chan struct{} // closed when Close() is called
	model           string
	reasoningEffort string
	workDir         string
	report          *UsageReport
	contextUsage    *ContextUsage
	usageErr        error

	// Teardown controls, for tests that exercise the close/spawn race.
	closeDelay    time.Duration // how long Close() blocks before returning
	closeErr      error         // what Close() reports (e.g. "process still alive")
	closeFinished atomic.Bool   // set once Close() has returned
}

func newControllableSession(id string) *controllableAgentSession {
	return &controllableAgentSession{
		sessionID: id,
		alive:     true,
		events:    make(chan Event, 8),
		closed:    make(chan struct{}),
	}
}

func (s *controllableAgentSession) Send(_ string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	return nil
}
func (s *controllableAgentSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *controllableAgentSession) Events() <-chan Event                                 { return s.events }
func (s *controllableAgentSession) CurrentSessionID() string                             { return s.sessionID }
func (s *controllableAgentSession) GetModel() string                                     { return s.model }
func (s *controllableAgentSession) GetReasoningEffort() string                           { return s.reasoningEffort }
func (s *controllableAgentSession) GetWorkDir() string                                   { return s.workDir }
func (s *controllableAgentSession) GetUsage(_ context.Context) (*UsageReport, error) {
	if s.report == nil && s.usageErr == nil {
		return nil, fmt.Errorf("usage unavailable")
	}
	return s.report, s.usageErr
}
func (s *controllableAgentSession) GetContextUsage() *ContextUsage { return s.contextUsage }
func (s *controllableAgentSession) Alive() bool                    { return s.alive }
func (s *controllableAgentSession) Close() error {
	if s.closeDelay > 0 {
		time.Sleep(s.closeDelay)
	}
	s.alive = false
	close(s.events)
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	s.closeFinished.Store(true)
	return s.closeErr
}

func waitForInteractiveStateRemoved(t *testing.T, e *Engine, key string) {
	t.Helper()
	deadline := time.After(500 * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		e.interactiveMu.Lock()
		current := e.interactiveStates[key]
		e.interactiveMu.Unlock()
		if current == nil {
			return
		}
		select {
		case <-deadline:
			t.Fatal("expected idle timeout cleanup to remove interactive state")
		case <-ticker.C:
		}
	}
}

func waitForAgentSessionID(t *testing.T, session *Session, want string) {
	t.Helper()
	deadline := time.After(500 * time.Millisecond)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := session.GetAgentSessionID(); got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("agent session id = %q, want %q", session.GetAgentSessionID(), want)
		case <-ticker.C:
		}
	}
}

// controllableAgent lets tests control which session is returned by StartSession.
type controllableAgent struct {
	nextSession    AgentSession
	listFn         func() ([]AgentSessionInfo, error)
	startSessionFn func(ctx context.Context, sessionID string) (AgentSession, error)
}

func (a *controllableAgent) Name() string { return "controllable" }
func (a *controllableAgent) StartSession(ctx context.Context, sessionID string) (AgentSession, error) {
	if a.startSessionFn != nil {
		return a.startSessionFn(ctx, sessionID)
	}
	if a.nextSession != nil {
		return a.nextSession, nil
	}
	return newControllableSession("default"), nil
}
func (a *controllableAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	if a.listFn != nil {
		return a.listFn()
	}
	return nil, nil
}
func (a *controllableAgent) Stop() error { return nil }

// TestCleanupCAS_SkipsWhenStateReplaced verifies that cleanupInteractiveState
// with an expected state pointer is a no-op when the map entry has been replaced.
// This is the core of the /new race fix: old goroutine's cleanup must not delete
// a replacement state created by a new turn.
func TestCleanupCAS_SkipsWhenStateReplaced(t *testing.T) {
	e := newTestEngine()
	key := "test:user1"

	oldState := &interactiveState{agentSession: newControllableSession("old")}
	newState := &interactiveState{agentSession: newControllableSession("new")}

	// Place the NEW state in the map (simulating: /new already cleaned up and
	// a new turn created a replacement state).
	e.interactiveMu.Lock()
	e.interactiveStates[key] = newState
	e.interactiveMu.Unlock()

	// Old goroutine calls cleanup with the OLD state pointer — should be skipped.
	e.cleanupInteractiveState(key, oldState)

	e.interactiveMu.Lock()
	current := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if current != newState {
		t.Fatal("CAS cleanup deleted the replacement state — race not prevented")
	}
}

// TestCleanupCAS_DeletesWhenStateMatches verifies that cleanup proceeds normally
// when the expected state matches the current map entry.
func TestCleanupCAS_DeletesWhenStateMatches(t *testing.T) {
	e := newTestEngine()
	key := "test:user1"

	state := &interactiveState{agentSession: newControllableSession("s1")}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.cleanupInteractiveState(key, state)

	e.interactiveMu.Lock()
	current := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if current != nil {
		t.Fatal("expected state to be deleted when expected pointer matches")
	}
}

// TestCleanupCAS_UnconditionalWithoutExpected verifies that cleanup without an
// expected pointer always deletes (backward compat for command handlers).
func TestCleanupCAS_UnconditionalWithoutExpected(t *testing.T) {
	e := newTestEngine()
	key := "test:user1"

	state := &interactiveState{agentSession: newControllableSession("s1")}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// No expected pointer — unconditional cleanup (used by /new, /switch).
	e.cleanupInteractiveState(key)

	e.interactiveMu.Lock()
	current := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if current != nil {
		t.Fatal("expected unconditional cleanup to delete state")
	}
}

func TestAgentSessionIdleTimeout_ClosesIdleLiveSession(t *testing.T) {
	e := newTestEngine()
	e.SetAgentSessionIdleTimeout(20 * time.Millisecond)
	key := "test:user1"
	sess := newControllableSession("s1")
	state := &interactiveState{agentSession: sess, eventsNeedResync: false}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.scheduleAgentSessionIdleClose(key, state)

	select {
	case <-sess.closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("agent session was not closed after idle timeout")
	}

	waitForInteractiveStateRemoved(t, e, key)
}

func TestAgentSessionIdleTimeout_CancelPreventsClose(t *testing.T) {
	e := newTestEngine()
	e.SetAgentSessionIdleTimeout(20 * time.Millisecond)
	key := "test:user1"
	sess := newControllableSession("s1")
	state := &interactiveState{agentSession: sess, eventsNeedResync: false}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.scheduleAgentSessionIdleClose(key, state)
	e.cancelAgentSessionIdleClose(state)

	select {
	case <-sess.closed:
		t.Fatal("agent session closed even though idle close was cancelled")
	case <-time.After(80 * time.Millisecond):
	}

	e.interactiveMu.Lock()
	current := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if current != state {
		t.Fatal("expected interactive state to remain after cancelling idle close")
	}
}

func TestAgentSessionIdleTimeout_DisableCancelsScheduledClose(t *testing.T) {
	e := newTestEngine()
	e.SetAgentSessionIdleTimeout(20 * time.Millisecond)
	key := "test:user1"
	sess := newControllableSession("s1")
	state := &interactiveState{agentSession: sess, eventsNeedResync: false}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.scheduleAgentSessionIdleClose(key, state)
	e.SetAgentSessionIdleTimeout(0)

	select {
	case <-sess.closed:
		t.Fatal("agent session closed after idle timeout was disabled")
	case <-time.After(80 * time.Millisecond):
	}
}

func TestAgentSessionIdleTimeout_StaleTokenDoesNotCloseSession(t *testing.T) {
	e := newTestEngine()
	key := "test:user1"
	sess := newControllableSession("s1")
	state := &interactiveState{
		agentSession:           sess,
		eventsNeedResync:       false,
		agentSessionIdleToken:  2,
		agentSessionIdleCancel: func() {},
	}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.cleanupInteractiveStateForIdleToken(key, state, 1, 20*time.Millisecond)

	select {
	case <-sess.closed:
		t.Fatal("stale idle token closed the live agent session")
	case <-time.After(20 * time.Millisecond):
	}

	e.interactiveMu.Lock()
	current := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if current != state {
		t.Fatal("expected stale idle token to leave interactive state in place")
	}
}

func TestProcessInteractiveMessage_SchedulesAgentSessionIdleCloseAfterCleanTurn(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newResultAgentSession("done")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"))
	e.SetAgentSessionIdleTimeout(20 * time.Millisecond)
	session := e.sessions.GetOrCreateActive("test:chat:user1")

	go e.processInteractiveMessageWith(p, &Message{
		Platform:   "test",
		SessionKey: "test:chat:user1",
		UserID:     "user1",
		UserName:   "User One",
		Content:    "hello",
		ReplyCtx:   "ctx",
	}, session, agent, e.sessions, "test:chat:user1", "", "")

	waitForAgentSessionID(t, session, "result-session")
	waitForInteractiveStateRemoved(t, e, "test:chat:user1")
}

// TestCleanupCAS_ConcurrentUnconditionalCloseOnce verifies that two concurrent
// unconditional cleanups for the same key only Close() the agent session once.
func TestCleanupCAS_ConcurrentUnconditionalCloseOnce(t *testing.T) {
	e := newTestEngine()
	key := "test:user1"

	var closeCount atomic.Int32
	sess := newControllableSession("s1")
	origClose := sess.Close
	_ = origClose
	state := &interactiveState{agentSession: sess}

	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			e.cleanupInteractiveState(key)
		}()
	}
	wg.Wait()

	// The session's Close() should have been called at most once because
	// the first cleanup nil's out state.agentSession under the lock.
	select {
	case <-sess.closed:
		closeCount.Add(1)
	default:
	}
	if closeCount.Load() > 1 {
		t.Fatalf("expected at most 1 close, got %d", closeCount.Load())
	}

	e.interactiveMu.Lock()
	if e.interactiveStates[key] != nil {
		t.Fatal("expected state to be deleted after cleanup")
	}
	e.interactiveMu.Unlock()
}

// TestSessionMismatch_RecyclesStaleAgent verifies that getOrCreateInteractiveStateWith
// detects when the running agent session ID differs from the active Session's
// AgentSessionID and creates a fresh agent instead of reusing the stale one.
func TestSessionMismatch_RecyclesStaleAgent(t *testing.T) {
	newSess := newControllableSession("new-agent-id")
	agent := &controllableAgent{nextSession: newSess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"

	// Seed a live agent session with ID "old-agent-id".
	oldSess := newControllableSession("old-agent-id")
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{
		agentSession: oldSess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Unlock()

	// The active Session now wants a DIFFERENT agent session ID.
	session := &Session{AgentSessionID: "new-agent-id"}

	state := e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")

	if state.agentSession == oldSess {
		t.Fatal("expected stale agent session to be replaced")
	}
	if state.agentSession != newSess {
		t.Fatal("expected new agent session from StartSession")
	}

	// Old session should be closed asynchronously.
	select {
	case <-oldSess.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("old agent session was not closed after mismatch")
	}
}

// TestSessionClearedAfterNew_RecyclesAliveAgent verifies issue #238: after /new the
// Session's AgentSessionID is empty but an older Claude process may still be alive;
// it must be recycled instead of reused (which would keep prior --resume context).
func TestSessionClearedAfterNew_RecyclesAliveAgent(t *testing.T) {
	newSess := newControllableSession("fresh-id")
	agent := &controllableAgent{nextSession: newSess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")
	key := "test:user1"
	oldSess := newControllableSession("prior-claude-session")
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{
		agentSession: oldSess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Unlock()

	session := &Session{AgentSessionID: ""}

	state := e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")
	if state.agentSession == oldSess {
		t.Fatal("expected stale agent to be recycled when AgentSessionID was cleared")
	}
	if state.agentSession != newSess {
		t.Fatal("expected new agent session from StartSession")
	}
	select {
	case <-oldSess.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("old agent session was not closed after /new-style clear")
	}
}

// TestSessionMismatch_ReusesWhenIDsMatch verifies that getOrCreateInteractiveStateWith
// returns the existing state when agent session IDs match (no unnecessary recycling).
func TestSessionMismatch_ReusesWhenIDsMatch(t *testing.T) {
	agent := &controllableAgent{}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"

	existingSess := newControllableSession("matching-id")
	existingState := &interactiveState{
		agentSession: existingSess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = existingState
	e.interactiveMu.Unlock()

	session := &Session{AgentSessionID: "matching-id"}

	state := e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")
	if state != existingState {
		t.Fatal("expected existing state to be reused when session IDs match")
	}
}

// TestSessionIDWriteback_ImmediateAfterStartSession verifies that after
// StartSession, the agent's CurrentSessionID is immediately written back
// to the Session's AgentSessionID when it was previously empty.
func TestSessionIDWriteback_ImmediateAfterStartSession(t *testing.T) {
	sess := newControllableSession("agent-uuid-123")
	agent := &controllableAgent{nextSession: sess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	session := &Session{AgentSessionID: ""} // empty — no prior binding

	e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")

	got := session.GetAgentSessionID()

	if got != "agent-uuid-123" {
		t.Fatalf("AgentSessionID = %q, want %q — immediate writeback not working", got, "agent-uuid-123")
	}
}

// TestSessionIDWriteback_MapsSessionName verifies that when startOrResumeSession
// sets the AgentSessionID, it also maps the session's pending name via
// SetSessionName so that /list displays the custom name from /new.
func TestSessionIDWriteback_MapsSessionName(t *testing.T) {
	sess := newControllableSession("agent-uuid-456")
	agent := &controllableAgent{nextSession: sess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	session := e.sessions.NewSession(key, "我的自定义会话")

	e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")

	got := e.sessions.GetSessionName("agent-uuid-456")
	if got != "我的自定义会话" {
		t.Fatalf("GetSessionName = %q, want %q — name not mapped during startOrResumeSession", got, "我的自定义会话")
	}
}

// TestSessionIDWriteback_TracksLiveForkedID verifies that write-back follows
// the ID the live process actually reports. Claude forks a new session_id on
// every --resume, so even when the session already holds an ID, the stored ID
// must update to the live one — otherwise a later /stop or /model would resume
// a stale node and lose context.
func TestSessionIDWriteback_TracksLiveForkedID(t *testing.T) {
	sess := newControllableSession("new-uuid")
	agent := &controllableAgent{nextSession: sess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	session := &Session{AgentSessionID: "existing-uuid"}

	e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")

	got := session.GetAgentSessionID()

	if got != "new-uuid" {
		t.Fatalf("AgentSessionID = %q, want %q — writeback should track the live forked ID", got, "new-uuid")
	}
}

// TestInteractiveWriteBack_TracksForkedSessionID verifies that when the live
// agent process reports a session ID different from the one stored (Claude
// forks a new session_id on every --resume), the stored AgentSessionID is
// updated to follow the live process. Without this, a later /stop or /model
// would resume a stale node and lose context. Regression for the model-switch
// context-loss bug.
func TestInteractiveWriteBack_TracksForkedSessionID(t *testing.T) {
	// StartSession succeeds but the live process reports a forked ID.
	forkedSess := newControllableSession("forked-id")
	agent := &controllableAgent{nextSession: forkedSess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	// Session already holds the original (now stale) ID from a prior turn.
	session := &Session{AgentSessionID: "orig-id", AgentType: "controllable"}

	e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")

	if got := session.GetAgentSessionID(); got != "forked-id" {
		t.Fatalf("AgentSessionID = %q, want %q — must track the live forked ID", got, "forked-id")
	}
}

// TestInteractiveWriteBack_NamingBindsOnlyOnFirstAssignment verifies that the
// custom session name is bound only when the session first acquires an ID, not
// on every forked ID. Otherwise sessionNames would be polluted with a stale
// name on each --resume fork.
func TestInteractiveWriteBack_NamingBindsOnlyOnFirstAssignment(t *testing.T) {
	firstSess := newControllableSession("first-id")
	agent := &controllableAgent{nextSession: firstSess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	// Fresh session with a custom name but no ID yet.
	session := &Session{Name: "my-feature", AgentType: "controllable"}

	// First assignment: name should bind to first-id.
	e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")
	if got := e.sessions.GetSessionName("first-id"); got != "my-feature" {
		t.Fatalf("first-id name = %q, want %q on first assignment", got, "my-feature")
	}

	// Now simulate a forked ID on a subsequent start. The name must NOT be
	// rebound to the forked ID.
	forkedSess := newControllableSession("forked-id")
	agent.nextSession = forkedSess
	e.interactiveMu.Lock()
	delete(e.interactiveStates, key) // force a fresh start path
	e.interactiveMu.Unlock()

	e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")
	if got := session.GetAgentSessionID(); got != "forked-id" {
		t.Fatalf("AgentSessionID = %q, want %q after fork", got, "forked-id")
	}
	if got := e.sessions.GetSessionName("forked-id"); got != "" {
		t.Fatalf("forked-id name = %q, want empty — name must bind only on first assignment", got)
	}
}

// TestCmdStop_PreservesAgentSessionID verifies /stop tears down the live
// process but keeps the stored AgentSessionID so the next message can --resume.
func TestCmdStop_PreservesAgentSessionID(t *testing.T) {
	sess := newControllableSession("agent-1")
	agent := &controllableAgent{nextSession: sess}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"

	// Seed a live interactive state.
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Unlock()

	// Set the Session's AgentSessionID to match (simulates a normal turn).
	active := e.sessions.GetOrCreateActive(key)
	active.SetAgentSessionID("agent-1", "controllable")
	e.sessions.Save()

	// Simulate /stop: it uses interactiveKeyForSessionKey internally.
	msg := &Message{SessionKey: key, ReplyCtx: "ctx"}
	e.cmdStop(p, msg)

	// After /stop, AgentSessionID must be preserved so the next message can
	// --resume the conversation (matching the card-button stop path).
	got := active.GetAgentSessionID()
	if got != "agent-1" {
		t.Fatalf("AgentSessionID = %q, want %q preserved after /stop", got, "agent-1")
	}
}

// TestResumeFallback_ClearsStaleSessionID verifies that when agent.StartSession
// fails with a stale session ID and falls back to a fresh session, the stale
// AgentSessionID is cleared so CompareAndSetAgentSessionID can write the new ID
// (issue #830, matching the relay fallback at engine.go:12640).
func TestResumeFallback_ClearsStaleSessionID(t *testing.T) {
	freshSess := newControllableSession("fresh-id")
	agent := &controllableAgent{
		startSessionFn: func(_ context.Context, sessionID string) (AgentSession, error) {
			if sessionID != "" {
				return nil, errors.New("session not found")
			}
			return freshSess, nil
		},
	}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"

	// Session has a stale AgentSessionID from a previously killed agent.
	session := &Session{AgentSessionID: "stale-id", AgentType: "controllable"}

	state := e.getOrCreateInteractiveStateWith(key, p, "ctx", session, e.sessions, nil, "")

	// The new agent session should be the fresh one.
	if state.agentSession != freshSess {
		t.Fatal("expected fresh agent session from fallback")
	}

	// The stale ID should have been replaced with the new ID.
	got := session.GetAgentSessionID()
	if got != "fresh-id" {
		t.Fatalf("AgentSessionID = %q, want %q — stale ID should be replaced", got, "fresh-id")
	}
}

// TestStaleGoroutineCleanup_RaceSimulation simulates the full race scenario:
// old turn still processing → /new creates new Session → new turn starts →
// old turn exits and calls cleanup. Verifies the new state survives.
func TestStaleGoroutineCleanup_RaceSimulation(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	newSess := newControllableSession("new-agent")
	agent := &controllableAgent{nextSession: newSess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"

	// Step 1: Old turn created state S1 with old agent.
	oldSess := newControllableSession("old-agent")
	oldState := &interactiveState{
		agentSession: oldSess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = oldState
	e.interactiveMu.Unlock()

	// Step 2: /new runs — unconditional cleanup deletes S1.
	e.cleanupInteractiveState(key)

	// Step 3: New turn creates Session B and calls getOrCreateInteractiveStateWith.
	sessionB := &Session{AgentSessionID: ""}
	newState := e.getOrCreateInteractiveStateWith(key, p, "ctx", sessionB, e.sessions, nil, "")

	// Verify S2 is in the map.
	e.interactiveMu.Lock()
	current := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if current != newState {
		t.Fatal("new state not in map")
	}

	// Step 4: Old goroutine exits and calls cleanup with OLD state pointer.
	// This simulates processInteractiveEvents channelClosed path.
	e.cleanupInteractiveState(key, oldState)

	// Verify: new state must survive.
	e.interactiveMu.Lock()
	afterCleanup := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if afterCleanup != newState {
		t.Fatal("stale goroutine's cleanup deleted the replacement state — CAS not working")
	}
	if newState.agentSession.Alive() != true {
		t.Fatal("replacement agent session was killed by stale cleanup")
	}
}

// --- session resilience tests ---

// stubStartSessionAgent records StartSession calls and can fail on specific session IDs.
type stubStartSessionAgent struct {
	calls   []string
	failIDs map[string]error // session IDs that should fail
	mu      sync.Mutex
}

func (a *stubStartSessionAgent) Name() string { return "stub" }
func (a *stubStartSessionAgent) StartSession(_ context.Context, sessionID string) (AgentSession, error) {
	a.mu.Lock()
	a.calls = append(a.calls, sessionID)
	a.mu.Unlock()

	if err, ok := a.failIDs[sessionID]; ok {
		return nil, err
	}
	return &stubAgentSession{}, nil
}
func (a *stubStartSessionAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	return nil, nil
}
func (a *stubStartSessionAgent) Stop() error { return nil }

func TestResumeFailureFallbackToFreshSession(t *testing.T) {
	agent := &stubStartSessionAgent{
		failIDs: map[string]error{
			"old-session-id": fmt.Errorf("Prompt is too long"),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := &Engine{
		agent:             agent,
		sessions:          NewSessionManager(""),
		ctx:               ctx,
		i18n:              NewI18n(),
		interactiveStates: make(map[string]*interactiveState),
		display:           DisplayCfg{},
	}

	session := e.sessions.GetOrCreateActive("test:user1")
	session.SetAgentSessionID("old-session-id", "stub")

	p := &stubPlatformEngine{n: "test"}
	state := e.getOrCreateInteractiveStateWith("test:user1", p, "ctx", session, e.sessions, nil, "")

	if state.agentSession == nil {
		t.Fatal("expected agentSession to be non-nil after fallback")
	}

	agent.mu.Lock()
	calls := append([]string{}, agent.calls...)
	agent.mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("expected 2 StartSession calls, got %d: %v", len(calls), calls)
	}
	if calls[0] != "old-session-id" {
		t.Fatalf("first StartSession call = %q, want saved session id", calls[0])
	}
	if calls[1] != "" {
		t.Fatalf("second StartSession call = %q, want empty string", calls[1])
	}
}

func TestFreshSessionWithoutSavedSessionIDStartsFresh(t *testing.T) {
	agent := &stubStartSessionAgent{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := &Engine{
		agent:             agent,
		sessions:          NewSessionManager(""),
		ctx:               ctx,
		i18n:              NewI18n(),
		interactiveStates: make(map[string]*interactiveState),
		display:           DisplayCfg{},
	}
	session := e.sessions.GetOrCreateActive("test:user2")

	p := &stubPlatformEngine{n: "test"}
	state := e.getOrCreateInteractiveStateWith("test:user2", p, "ctx", session, e.sessions, nil, "")

	if state.agentSession == nil {
		t.Fatal("expected agentSession to be non-nil")
	}

	agent.mu.Lock()
	calls := append([]string{}, agent.calls...)
	agent.mu.Unlock()

	if len(calls) != 1 {
		t.Fatalf("expected 1 StartSession call, got %d: %v", len(calls), calls)
	}
	if calls[0] != "" {
		t.Fatalf("StartSession call = %q, want empty string (fresh session)", calls[0])
	}
}

func TestWorkspaceReconnectWithSavedSessionIDUsesExactResume(t *testing.T) {
	agent := &stubStartSessionAgent{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	e := &Engine{
		agent:             agent,
		sessions:          NewSessionManager(""),
		ctx:               ctx,
		i18n:              NewI18n(),
		interactiveStates: make(map[string]*interactiveState),
		display:           DisplayCfg{},
	}

	session := e.sessions.GetOrCreateActive("test:user3")
	session.SetAgentSessionID("saved-session-id", "stub")

	p := &stubPlatformEngine{n: "test"}
	state := e.getOrCreateInteractiveStateWith("test:user3", p, "ctx", session, e.sessions, nil, "")

	if state.agentSession == nil {
		t.Fatal("expected agentSession to be non-nil")
	}

	agent.mu.Lock()
	calls := append([]string{}, agent.calls...)
	agent.mu.Unlock()

	if len(calls) != 1 {
		t.Fatalf("expected 1 StartSession call, got %d: %v", len(calls), calls)
	}
	if calls[0] != "saved-session-id" {
		t.Fatalf("StartSession call = %q, want saved session id", calls[0])
	}
}

func TestParseSelfReportedCtx(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"here is my response\n[ctx: ~42%]", 42},
		{"no context here", 0},
		{"response\n[ctx: ~100%]", 100},
		{"response\n[ctx: ~5%]", 5},
		{"", 0},
	}
	for _, tt := range tests {
		got := parseSelfReportedCtx(tt.input)
		if got != tt.want {
			t.Errorf("parseSelfReportedCtx(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestDrainEventsClosedChannel(t *testing.T) {
	ch := make(chan Event, 2)
	ch <- Event{Type: EventToolUse, Content: "a"}
	ch <- Event{Type: EventToolUse, Content: "b"}
	close(ch)

	done := make(chan struct{})
	go func() {
		drainEvents(ch)
		close(done)
	}()

	select {
	case <-done:
		// ok — returned promptly
	case <-time.After(2 * time.Second):
		t.Fatal("drainEvents did not return on closed channel (infinite loop)")
	}
}

func TestDrainEventsOpenChannel(t *testing.T) {
	ch := make(chan Event, 3)
	ch <- Event{Type: EventToolUse, Content: "a"}
	ch <- Event{Type: EventToolUse, Content: "b"}

	done := make(chan struct{})
	go func() {
		drainEvents(ch)
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("drainEvents did not return on open channel with buffered events")
	}

	// Channel should now be empty.
	select {
	case <-ch:
		t.Fatal("expected channel to be drained")
	default:
	}
}

// --- Message queuing tests ---

// queuingAgentSession records Send calls and emits events via a controllable channel.
type queuingAgentSession struct {
	controllableAgentSession
	sendCalls []string
	sendMu    sync.Mutex
}

func newQueuingSession(id string) *queuingAgentSession {
	return &queuingAgentSession{
		controllableAgentSession: controllableAgentSession{
			sessionID: id,
			alive:     true,
			events:    make(chan Event, 16),
			closed:    make(chan struct{}),
		},
	}
}

func (s *queuingAgentSession) Send(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.sendMu.Lock()
	s.sendCalls = append(s.sendCalls, prompt)
	s.sendMu.Unlock()
	return nil
}

// blockingSendAgentSession blocks in Send until unblock is closed, mimicking agents
// whose Send does not return until the prompt turn completes (e.g. ACP session/prompt).
type blockingSendAgentSession struct {
	controllableAgentSession
	sendStarted chan struct{} // sent to when Send begins waiting on unblock
	unblock     chan struct{} // close to let Send return
}

func newBlockingSendSession(id string) *blockingSendAgentSession {
	return &blockingSendAgentSession{
		controllableAgentSession: controllableAgentSession{
			sessionID: id,
			alive:     true,
			events:    make(chan Event, 16),
			closed:    make(chan struct{}),
		},
		sendStarted: make(chan struct{}, 1),
		unblock:     make(chan struct{}),
	}
}

func (s *blockingSendAgentSession) Send(_ string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.sendStarted <- struct{}{}
	<-s.unblock
	return nil
}

// blockingCloseAgentSession blocks in Close until releaseClose is closed.
// It is used to verify that /stop detaches the session and stops forwarding
// events before the underlying agent process has fully exited.
type blockingCloseAgentSession struct {
	controllableAgentSession
	closeStarted chan struct{}
	releaseClose chan struct{}
}

func newBlockingCloseSession(id string) *blockingCloseAgentSession {
	return &blockingCloseAgentSession{
		controllableAgentSession: controllableAgentSession{
			sessionID: id,
			alive:     true,
			events:    make(chan Event, 16),
			closed:    make(chan struct{}),
		},
		closeStarted: make(chan struct{}, 1),
		releaseClose: make(chan struct{}),
	}
}

func (s *blockingCloseAgentSession) Close() error {
	s.alive = false
	select {
	case s.closeStarted <- struct{}{}:
	default:
	}
	<-s.releaseClose
	close(s.events)
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

// permSignalInlinePlatform wraps stubInlineButtonPlatform and signals when a
// SendWithButtons call includes perm:allow, so tests do not read buttonRows
// from another goroutine (race with the engine under -race).
type permSignalInlinePlatform struct {
	stubInlineButtonPlatform
	permAllowSent chan<- struct{}
}

func (p *permSignalInlinePlatform) SendWithButtons(ctx context.Context, replyCtx any, content string, buttons [][]ButtonOption) error {
	if err := p.stubInlineButtonPlatform.SendWithButtons(ctx, replyCtx, content, buttons); err != nil {
		return err
	}
	for _, row := range buttons {
		for _, b := range row {
			if b.Data == "perm:allow" {
				select {
				case p.permAllowSent <- struct{}{}:
				default:
				}
				return nil
			}
		}
	}
	return nil
}

// Regression: permission events must be handled while Send is still blocked.
// If the engine called Send synchronously before reading Events(), this would deadlock
// and never call sendPermissionPrompt.
func TestProcessInteractiveEvents_PermissionWhileSendBlocked(t *testing.T) {
	permAllowSent := make(chan struct{}, 1)
	p := &permSignalInlinePlatform{
		stubInlineButtonPlatform: stubInlineButtonPlatform{stubPlatformEngine: stubPlatformEngine{n: "telegram"}},
		permAllowSent:            permAllowSent,
	}
	sess := newBlockingSendSession("blk-perm")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sess.Send("prompt", "", nil, nil)
	}()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "m1", time.Now(), nil, sendDone, nil)
		close(done)
	}()

	select {
	case <-sess.sendStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not reach blocking wait")
	}

	sess.events <- Event{
		Type:         EventPermissionRequest,
		RequestID:    "req-blocked-send",
		ToolName:     "write_file",
		ToolInput:    "/tmp/x",
		ToolInputRaw: map[string]any{"path": "/tmp/x"},
	}

	select {
	case <-permAllowSent:
	case <-time.After(2 * time.Second):
		t.Fatal("permission inline buttons not sent while Send blocked")
	}

	if !e.handlePendingPermission(p, &Message{SessionKey: key, ReplyCtx: "ctx"}, "allow", "") {
		t.Fatal("expected handlePendingPermission to resolve pending request")
	}
	close(sess.unblock)

	sess.events <- Event{Type: EventResult, Content: "ok", Done: true}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processInteractiveEvents did not complete")
	}
}

func TestQueueMessageForBusySession_FIFODequeue(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("qs1")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"

	// Set up an interactive state as if a turn is in progress.
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx1",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Queue two messages while the session is "busy".
	msg1 := &Message{SessionKey: key, Content: "msg1", ReplyCtx: "ctx-msg1"}
	msg2 := &Message{SessionKey: key, Content: "msg2", ReplyCtx: "ctx-msg2"}

	ok1 := e.queueMessageForBusySession(p, msg1, key)
	ok2 := e.queueMessageForBusySession(p, msg2, key)

	if !ok1 || !ok2 {
		t.Fatal("expected both messages to be queued successfully")
	}

	// Since deferred-send, messages are NOT sent to agent stdin at queue
	// time — only metadata is stored. Verify no Send calls occurred.
	sess.sendMu.Lock()
	if len(sess.sendCalls) != 0 {
		t.Fatalf("sendCalls = %v, want [] (deferred send)", sess.sendCalls)
	}
	sess.sendMu.Unlock()

	// Verify pending messages queue has correct FIFO order.
	state.mu.Lock()
	if len(state.pendingMessages) != 2 {
		t.Fatalf("pendingMessages len = %d, want 2", len(state.pendingMessages))
	}
	if state.pendingMessages[0].content != "msg1" || state.pendingMessages[1].content != "msg2" {
		t.Fatalf("pendingMessages = [%s, %s], want [msg1, msg2]",
			state.pendingMessages[0].content, state.pendingMessages[1].content)
	}
	state.mu.Unlock()
}

// cancellableSession wraps controllableAgentSession and implements
// AgentSessionCanceller so tests can exercise the interrupt-on-new-message
// path in interruptAndRetryLock.
type cancellableSession struct {
	*controllableAgentSession
	cancelFn    func() error
	cancelCalls int
	cancelMu    sync.Mutex
	steerFn     func(string, string, []ImageAttachment, []FileAttachment) error
	steerCalls  int
	steerMu     sync.Mutex
}

func (s *cancellableSession) CancelTurn() error {
	s.cancelMu.Lock()
	s.cancelCalls++
	s.cancelMu.Unlock()
	if s.cancelFn != nil {
		return s.cancelFn()
	}
	return nil
}

func (s *cancellableSession) callCount() int {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	return s.cancelCalls
}

func (s *cancellableSession) Steer(prompt string, messageID string, images []ImageAttachment, files []FileAttachment) error {
	s.steerMu.Lock()
	s.steerCalls++
	s.steerMu.Unlock()
	if s.steerFn != nil {
		return s.steerFn(prompt, messageID, images, files)
	}
	return nil
}

func (s *cancellableSession) steerCallCount() int {
	s.steerMu.Lock()
	defer s.steerMu.Unlock()
	return s.steerCalls
}

func TestQueuedUserMessageStaleForDrainIgnoresOtherPendingMessages(t *testing.T) {
	e := &Engine{}
	state := &interactiveState{
		pendingMessages: []queuedMessage{
			{userMessageTimeMs: 3_000},
		},
	}
	if e.isQueuedUserMessageStaleForDrainLocked(state, 2_000) {
		t.Fatal("queued message was marked stale using another pending message watermark")
	}

	state.currentTurnUserMessageTimeMs = 3_000
	if !e.isQueuedUserMessageStaleForDrainLocked(state, 2_000) {
		t.Fatal("queued message older than the in-flight turn was not marked stale")
	}
}

func TestProcessInteractiveEvents_DrainsQueuedMessages(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("qs2")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)

	// Pre-populate the interactive state with one queued message.
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx-turn1",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-turn2", content: "queued-msg"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Simulate the agent completing turn 1 then turn 2.
	// Turn 2 events are pushed only after Send() is called for the queued
	// message, matching real-world timing where the agent doesn't produce
	// events for a turn until it receives the prompt on stdin.
	go func() {
		// Turn 1 result
		sess.events <- Event{Type: EventText, Content: "response1"}
		sess.events <- Event{Type: EventResult, Content: "response1", Done: true}
		// Wait for the queued message's Send() call before pushing turn 2 events.
		sess.sendMu.Lock()
		for len(sess.sendCalls) == 0 {
			sess.sendMu.Unlock()
			time.Sleep(5 * time.Millisecond)
			sess.sendMu.Lock()
		}
		sess.sendMu.Unlock()
		// Turn 2 result (for the queued message)
		sess.events <- Event{Type: EventText, Content: "response2"}
		sess.events <- Event{Type: EventResult, Content: "response2", Done: true}
	}()

	session.AddHistory("user", "initial-msg")

	sendDone := make(chan error, 1)
	sendDone <- nil

	// processInteractiveEvents should handle both turns.
	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "msg1", time.Now(), nil, sendDone, nil)
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(5 * time.Second):
		t.Fatal("processInteractiveEvents did not complete in time")
	}

	// Verify queue is empty after processing.
	state.mu.Lock()
	remaining := len(state.pendingMessages)
	state.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("pendingMessages after processing = %d, want 0", remaining)
	}

	// Verify both turns recorded in session history.
	history := session.GetHistory(100)
	var assistantMsgs []string
	for _, h := range history {
		if h.Role == "assistant" {
			assistantMsgs = append(assistantMsgs, h.Content)
		}
	}
	if len(assistantMsgs) != 2 {
		t.Fatalf("assistant history entries = %d, want 2", len(assistantMsgs))
	}

	// Verify the queued message was also added to history.
	var userMsgs []string
	for _, h := range history {
		if h.Role == "user" {
			userMsgs = append(userMsgs, h.Content)
		}
	}
	if len(userMsgs) < 2 {
		t.Fatalf("user history entries = %d, want >= 2", len(userMsgs))
	}
}

func TestProcessInteractiveEvents_DrainsQueuedMessagesFIFOWithCreateTimes(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("qs-fifo-times")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	state := &interactiveState{
		agentSession:                 sess,
		platform:                     p,
		replyCtx:                     "ctx-turn1",
		currentTurnUserMessageTimeMs: 1_000,
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-msg1", content: "msg1", userMessageTimeMs: 2_000},
			{platform: p, replyCtx: "ctx-msg2", content: "msg2", userMessageTimeMs: 3_000},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	waitSendCount := func(n int) bool {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			sess.sendMu.Lock()
			got := len(sess.sendCalls)
			sess.sendMu.Unlock()
			if got >= n {
				return true
			}
			time.Sleep(5 * time.Millisecond)
		}
		return false
	}

	go func() {
		sess.events <- Event{Type: EventResult, Content: "response0", Done: true}
		if waitSendCount(1) {
			sess.events <- Event{Type: EventResult, Content: "response1", Done: true}
		}
		if waitSendCount(2) {
			sess.events <- Event{Type: EventResult, Content: "response2", Done: true}
		}
	}()

	session.AddHistory("user", "initial-msg")
	sendDone := make(chan error, 1)
	sendDone <- nil

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "msg0", time.Now(), nil, sendDone, "ctx-turn1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processInteractiveEvents did not complete in time")
	}

	sess.sendMu.Lock()
	calls := append([]string(nil), sess.sendCalls...)
	sess.sendMu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("sendCalls len = %d, want 2; calls=%v", len(calls), calls)
	}
	if !strings.Contains(calls[0], "msg1") || !strings.Contains(calls[1], "msg2") {
		t.Fatalf("queued sends = %v, want FIFO msg1 then msg2", calls)
	}
}

// replyCtxRecordingPlatform records (replyCtx, content) for each Send/Reply
// so tests can assert which trigger context was used for which message.
type replyCtxRecordingPlatform struct {
	stubPlatformEngine
	mu     sync.Mutex
	events []replyCtxCall
}

type replyCtxCall struct {
	op       string
	replyCtx any
	content  string
}

func (p *replyCtxRecordingPlatform) Reply(_ context.Context, replyCtx any, content string) error {
	p.mu.Lock()
	p.events = append(p.events, replyCtxCall{op: "reply", replyCtx: replyCtx, content: content})
	p.mu.Unlock()
	return nil
}

func (p *replyCtxRecordingPlatform) Send(_ context.Context, replyCtx any, content string) error {
	p.mu.Lock()
	p.events = append(p.events, replyCtxCall{op: "send", replyCtx: replyCtx, content: content})
	p.mu.Unlock()
	return nil
}

func (p *replyCtxRecordingPlatform) recordedEvents() []replyCtxCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]replyCtxCall, len(p.events))
	copy(out, p.events)
	return out
}

// TestProcessInteractiveEvents_QueuedMessageUsesItsOwnReplyCtx verifies that
// when a queued message is dequeued mid-loop, subsequent Send/Reply calls use
// the queued message's reply context (not the original turn's). Without this,
// platforms that derive the parent message_id from replyCtx (e.g. feishu Reply
// API for the reply quote) would quote the wrong message.
func TestProcessInteractiveEvents_QueuedMessageUsesItsOwnReplyCtx(t *testing.T) {
	p := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	sess := newQueuingSession("qs-replyctx")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)

	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx-turn1",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-turn2", content: "queued-msg"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	go func() {
		// Turn 1 result — final reply should use ctx-turn1.
		sess.events <- Event{Type: EventResult, Content: "response1", Done: true}
		// Wait for the queued message's Send() before pushing turn 2.
		sess.sendMu.Lock()
		for len(sess.sendCalls) == 0 {
			sess.sendMu.Unlock()
			time.Sleep(5 * time.Millisecond)
			sess.sendMu.Lock()
		}
		sess.sendMu.Unlock()
		// Turn 2 result — final reply should use ctx-turn2.
		sess.events <- Event{Type: EventResult, Content: "response2", Done: true}
	}()

	session.AddHistory("user", "initial-msg")
	sendDone := make(chan error, 1)
	sendDone <- nil

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "msg1", time.Now(), nil, sendDone, "ctx-turn1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("processInteractiveEvents did not complete in time")
	}

	// Map each recorded send to the responsible turn by content match.
	for _, ev := range p.recordedEvents() {
		switch ev.content {
		case "response1":
			if ev.replyCtx != "ctx-turn1" {
				t.Errorf("turn1 reply used replyCtx=%v, want ctx-turn1", ev.replyCtx)
			}
		case "response2":
			if ev.replyCtx != "ctx-turn2" {
				t.Errorf("turn2 reply used replyCtx=%v, want ctx-turn2 (regression: msg2's reply quoted msg1)", ev.replyCtx)
			}
		}
	}
}

// TestIssue814_QueuedMessageAfterCleanEventResult_UsesOwnReplyCtx is a
// regression test for issue #814 ("Bot replies with the previous
// message's answer instead of the current one"). The reported symptom is
// that when the user sends message B immediately after message A, the
// bot's reply to B carries A's reply context (and therefore quotes
// A's bubble instead of B's), even though the response text is for B.
//
// The existing TestProcessInteractiveEvents_QueuedMessageUsesItsOwnReplyCtx
// pins the "queue drain INSIDE processInteractiveEvents" path. This
// new test pins the equivalent invariant for the OUTER drain path
// driven from ReceiveMessage / handleMessage, which is what real users
// hit: A's foreground turn is in processInteractiveEvents; B arrives
// via ReceiveMessage while the session is locked; A finishes; the
// foreground goroutine calls drainPendingMessages; B is processed
// inside that drain loop. If anything along that path leaks A's
// replyCtx into B's reply (or vice versa), the assertions at the
// bottom of this test will fail.
//
// Unlike the inner-drain test, this one goes through the full
// ReceiveMessage → handleMessage → processInteractiveMessageWith →
// processInteractiveEvents → drainPendingMessages pipeline so any
// state-handling bug at any layer surfaces.
func TestIssue814_QueuedMessageAfterCleanEventResult_UsesOwnReplyCtx(t *testing.T) {
	p := &replyCtxRecordingPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	sess := newQueuingSession("qs-814")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user-814"

	// Wait until B has been queued (state.pendingMessages grows), then
	// release A's turn. This guarantees the test exercises the exact
	// ordering the bug report describes: A is mid-flight, B arrives and
	// is queued behind A, A's response is sent, the drain loop picks
	// up B and runs its own turn. Without this signal, the agent's
	// events would be produced faster than the test goroutine can
	// dispatch B, and the test would degenerate into "two independent
	// sequential turns" — which is not the bug's path.
	turnAEmitted := make(chan struct{})

	// Producer goroutine: A's events are held back until the engine
	// shows B sitting in the queue; B's events are then produced after
	// A's turn is recorded as complete.
	go func() {
		// Turn 1 (A) — wait for Send call.
		sess.sendMu.Lock()
		for len(sess.sendCalls) < 1 {
			sess.sendMu.Unlock()
			time.Sleep(5 * time.Millisecond)
			sess.sendMu.Lock()
		}
		sess.sendMu.Unlock()

		// Wait until B has been queued behind A. Poll the engine's
		// interactive state directly; this is the same state that
		// handleMessage updated when it called queueMessageForBusySession.
		for {
			e.interactiveMu.Lock()
			st, ok := e.interactiveStates[key]
			e.interactiveMu.Unlock()
			if ok && st != nil {
				st.mu.Lock()
				n := len(st.pendingMessages)
				st.mu.Unlock()
				if n >= 1 {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
		}

		// A's events — emitted only after we are confident B is queued.
		sess.events <- Event{Type: EventText, Content: "response-A"}
		sess.events <- Event{Type: EventResult, Content: "response-A", Done: true}
		close(turnAEmitted)

		// Turn 2 (B) — only after the engine has called Send for B.
		sess.sendMu.Lock()
		for len(sess.sendCalls) < 2 {
			sess.sendMu.Unlock()
			time.Sleep(5 * time.Millisecond)
			sess.sendMu.Lock()
		}
		sess.sendMu.Unlock()
		sess.events <- Event{Type: EventText, Content: "response-B"}
		sess.events <- Event{Type: EventResult, Content: "response-B", Done: true}
	}()

	// A: must reach the foreground turn (not be queued behind a stale
	// state). We launch it first; the producer above gates A's events
	// on B being queued, so A's processing will block on the engine
	// event loop until the test sends B.
	e.ReceiveMessage(p, &Message{
		SessionKey: key,
		Platform:   "test",
		UserID:     "u-814",
		UserName:   "user814",
		MessageID:  "msg-A",
		Content:    "what is the answer to A?",
		ReplyCtx:   "ctx-A",
	})

	// Give A's foreground goroutine time to enter the event loop and
	// the session lock to settle, then dispatch B. B will arrive while
	// A is in the event loop awaiting events — exactly the timing the
	// bug report describes.
	time.Sleep(50 * time.Millisecond)
	e.ReceiveMessage(p, &Message{
		SessionKey: key,
		Platform:   "test",
		UserID:     "u-814",
		UserName:   "user814",
		MessageID:  "msg-B",
		Content:    "what is the answer to B?",
		ReplyCtx:   "ctx-B",
	})

	// Wait for both replies to be recorded. The producer gates B's
	// events on A being complete, so we will see A's reply first,
	// then B's.
	deadline := time.After(5 * time.Second)
	for {
		evs := p.recordedEvents()
		var sawA, sawB bool
		for _, ev := range evs {
			if ev.content == "response-A" {
				sawA = true
			}
			if ev.content == "response-B" {
				sawB = true
			}
		}
		if sawA && sawB {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for both replies; recorded=%v", p.recordedEvents())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// The invariant: each turn's response text must be delivered with
	// that turn's replyCtx. If a race leaks A's replyCtx into B's reply
	// (or vice versa) — the symptom in #814 — the assertions below
	// fire.
	for _, ev := range p.recordedEvents() {
		switch ev.content {
		case "response-A":
			if ev.replyCtx != "ctx-A" {
				t.Errorf("turn-A reply used replyCtx=%v, want ctx-A", ev.replyCtx)
			}
		case "response-B":
			if ev.replyCtx != "ctx-B" {
				t.Errorf("turn-B reply used replyCtx=%v, want ctx-B (regression for #814: msg-B's reply quoted msg-A)", ev.replyCtx)
			}
		}
	}
}

// TestDrainOrphanedQueue_UsesWorkspaceSessionManager verifies that
// drainOrphanedQueue saves session history through the passed sessions
// manager (workspace-specific) rather than e.sessions (global).
func TestDrainOrphanedQueue_UsesWorkspaceSessionManager(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("qs-orphan")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	// Create a separate "workspace" session manager that drainOrphanedQueue should use.
	wsSessionsPath := filepath.Join(t.TempDir(), "ws_sessions.json")
	wsSessions := NewSessionManager(wsSessionsPath)

	key := "ws1:test:user1"
	session := wsSessions.GetOrCreateActive("test:user1")
	if !session.TryLock() {
		t.Fatal("expected TryLock to succeed")
	}

	// Set up interactive state with a queued message.
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-q", content: "queued-orphan"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Push events so the drain completes.
	go func() {
		sess.sendMu.Lock()
		for len(sess.sendCalls) == 0 {
			sess.sendMu.Unlock()
			time.Sleep(5 * time.Millisecond)
			sess.sendMu.Lock()
		}
		sess.sendMu.Unlock()
		sess.events <- Event{Type: EventResult, Content: "orphan-response", Done: true}
	}()

	done := make(chan struct{})
	go func() {
		e.drainOrphanedQueue(session, wsSessions, key, agent, "")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainOrphanedQueue did not complete in time")
	}

	// The assistant response should be saved in the workspace session manager,
	// NOT in e.sessions (global).
	wsHistory := wsSessions.GetOrCreateActive("test:user1").GetHistory(0)
	var wsAssistant []string
	for _, h := range wsHistory {
		if h.Role == "assistant" {
			wsAssistant = append(wsAssistant, h.Content)
		}
	}
	if len(wsAssistant) == 0 {
		t.Fatal("expected assistant history in workspace session manager, got none")
	}

	// Verify e.sessions (global) does NOT have this history.
	globalSession := e.sessions.GetOrCreateActive("test:user1")
	globalHistory := globalSession.GetHistory(0)
	for _, h := range globalHistory {
		if h.Role == "assistant" && h.Content == "orphan-response" {
			t.Fatal("orphan response was saved to global e.sessions instead of workspace sessions")
		}
	}
}

// ── executeCardAction interactiveKey tests ───────────────────

func TestHandleCardNav_ModelSwitchesAndRefreshesCard(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubModelModeAgent{model: "old"}
	e := NewEngine("test", agent, []Platform{p}, "")

	sessionKey := "feishu:channel1:user1"
	card := e.handleCardNav("act:/model new-model", sessionKey)
	if card == nil {
		t.Fatal("expected immediate result card")
	}
	if text := card.RenderText(); !strings.Contains(text, "Model switched to `new-model`.") {
		t.Fatalf("result card = %q", text)
	}
	if agent.model != "new-model" {
		t.Fatalf("model = %q, want new-model", agent.model)
	}
	if refreshed := p.getRefreshedCards(); len(refreshed) != 0 {
		t.Fatalf("unexpected async refreshed cards: %d", len(refreshed))
	}
}

func TestHandleCardNav_ModelSwitchFailureRefreshesCard(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubModelModeAgent{model: "old"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.modelSaveFunc = func(string) error { return errors.New("save failed") }

	sessionKey := "feishu:channel1:user1"
	card := e.handleCardNav("act:/model broken-model", sessionKey)
	if card == nil {
		t.Fatal("expected immediate failure card")
	}
	if text := card.RenderText(); !strings.Contains(text, "Failed to switch model: save model: save failed") {
		t.Fatalf("failure card = %q", text)
	}
	if refreshed := p.getRefreshedCards(); len(refreshed) != 0 {
		t.Fatalf("unexpected async refreshed cards: %d", len(refreshed))
	}
}

func TestHandleCardNav_ModelResultBackReturnsModelCard(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{model: "gpt-5.4"}
	e := NewEngine("test", agent, []Platform{p}, "")

	sessionKey := "feishu:channel1:user1"
	result := e.renderModelSwitchResultCard("gpt-5.4", nil)
	buttons := result.CollectButtons()
	if len(buttons) != 1 || len(buttons[0]) != 1 {
		t.Fatalf("result buttons = %#v, want single back button", buttons)
	}
	if buttons[0][0].Data != "nav:/model" {
		t.Fatalf("back button value = %q, want nav:/model", buttons[0][0].Data)
	}

	card := e.handleCardNav(buttons[0][0].Data, sessionKey)
	if card == nil {
		t.Fatal("expected /model card")
	}
	text := card.RenderText()
	if !strings.Contains(text, "Current model: gpt-5.4") {
		t.Fatalf("model card text = %q", text)
	}
}

func TestExecuteCardAction_ModeCleansUpWithInteractiveKey(t *testing.T) {
	p := &stubPlatformEngine{n: "plain"}
	agent := &stubModelModeAgent{mode: "default"}
	e := NewEngine("test", agent, []Platform{p}, "")

	sessionKey := "feishu:channel1:user1"

	e.interactiveMu.Lock()
	e.interactiveStates[sessionKey] = &interactiveState{}
	e.interactiveMu.Unlock()

	e.executeCardAction("/mode", "yolo", sessionKey)

	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[sessionKey]
	e.interactiveMu.Unlock()
	if exists {
		t.Error("expected interactive state to be cleaned up after /mode")
	}
}

// ===========================================================================
// P0 Beta release tests
// ===========================================================================

// --- 1. Message queue overflow ---

func TestQueueMessageOverflow_DropsOldestAndReturnsfalse(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("qs-overflow")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:overflow-user"

	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Fill the queue to defaultMaxQueuedMessages (5).
	for i := 0; i < defaultMaxQueuedMessages; i++ {
		msg := &Message{SessionKey: key, Content: fmt.Sprintf("msg-%d", i), ReplyCtx: fmt.Sprintf("ctx-%d", i)}
		ok := e.queueMessageForBusySession(p, msg, key)
		if !ok {
			t.Fatalf("expected msg-%d to be queued, got false", i)
		}
	}

	state.mu.Lock()
	if len(state.pendingMessages) != defaultMaxQueuedMessages {
		t.Fatalf("queue depth = %d, want %d", len(state.pendingMessages), defaultMaxQueuedMessages)
	}
	state.mu.Unlock()

	// The 6th message should be handled (returns true) but not queued — MsgQueueFull sent.
	overflow := &Message{SessionKey: key, Content: "msg-overflow", ReplyCtx: "ctx-overflow"}
	ok := e.queueMessageForBusySession(p, overflow, key)
	if !ok {
		t.Fatal("expected 6th message to be handled (queue-full reply), got false")
	}

	// Queue should still have exactly defaultMaxQueuedMessages items (the original 5).
	state.mu.Lock()
	if len(state.pendingMessages) != defaultMaxQueuedMessages {
		t.Fatalf("queue depth after overflow = %d, want %d", len(state.pendingMessages), defaultMaxQueuedMessages)
	}
	// First message should still be msg-0 (FIFO preserved, no silent drop).
	if state.pendingMessages[0].content != "msg-0" {
		t.Fatalf("first queued = %q, want msg-0", state.pendingMessages[0].content)
	}
	state.mu.Unlock()

	// Platform should have received MsgMessageQueued for 5 accepted + MsgQueueFull for the overflow.
	sent := p.getSent()
	if len(sent) != defaultMaxQueuedMessages+1 {
		t.Fatalf("platform replies = %d, want %d (queued + queue-full)", len(sent), defaultMaxQueuedMessages+1)
	}
}

func TestQueueMessage_NoState_ReturnsFalse(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := newTestEngine()

	msg := &Message{SessionKey: "nonexistent:key", Content: "hello"}
	ok := e.queueMessageForBusySession(p, msg, "nonexistent:key")
	if ok {
		t.Fatal("expected false when no interactive state exists")
	}
}

func TestQueueMessage_DeadSession_ReturnsFalse(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("dead")
	sess.alive = false
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:dead-session"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: key, Content: "hello"}
	ok := e.queueMessageForBusySession(p, msg, key)
	if ok {
		t.Fatal("expected false for dead session")
	}
}

// TestQueueMessage_NilAgentSession_DuringStartup verifies that messages can be
// queued when the interactiveState exists but agentSession is nil (session is
// still starting up). This is the fix for issue #565.
func TestQueueMessage_NilAgentSession_DuringStartup(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := newTestEngine()

	key := "test:starting-session"
	// Simulate the placeholder state created by ensureInteractiveStateForQueueing
	state := &interactiveState{
		platform: p,
		replyCtx: "ctx",
		// agentSession is nil — session is starting up
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: key, Content: "queued during startup", ReplyCtx: "ctx-startup"}
	ok := e.queueMessageForBusySession(p, msg, key)
	if !ok {
		t.Fatal("expected true: messages should be queueable during session startup")
	}

	state.mu.Lock()
	if len(state.pendingMessages) != 1 {
		t.Fatalf("pendingMessages len = %d, want 1", len(state.pendingMessages))
	}
	if state.pendingMessages[0].content != "queued during startup" {
		t.Fatalf("queued content = %q, want %q", state.pendingMessages[0].content, "queued during startup")
	}
	state.mu.Unlock()
}

// TestProcessInteractiveMessageWith_NilAgentSession_NoPanic is a regression
// test for issue #1181. When a long-running agent turn is force-killed
// (e.g. by max_turn_time_mins) the cleanup path may leave an interactive
// state in the map with agentSession==nil. A subsequent message routed to
// that state must NOT panic with a nil-pointer deref at the old engine.go
// v1.3.2 line 2164 site (drainEvents(state.agentSession.Events())) —
// processInteractiveMessageWith should detect the nil state, send a
// user-visible failure reply, and return cleanly.
func TestProcessInteractiveMessageWith_NilAgentSession_NoPanic(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &controllableAgent{
		startSessionFn: func(_ context.Context, _ string) (AgentSession, error) {
			return nil, fmt.Errorf("simulated agent start failure")
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	sessionKey := "test:user-nil-after-abandon"
	session := e.sessions.GetOrCreateActive(sessionKey)
	if !session.TryLock() {
		t.Fatal("expected session lock")
	}

	// First call: agent.StartSession fails, leaving state.agentSession == nil.
	// The nil guard at engine.go:2851 must send a failure reply and return
	// without panicking. This branch protects against the v1.3.2 panic at
	// the old line 2164.
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("processInteractiveMessageWith panicked on nil agentSession: %v", r)
			}
			close(done)
		}()
		e.processInteractiveMessageWith(p, &Message{
			SessionKey: sessionKey,
			UserID:     "user-nil",
			Content:    "trigger nil guard",
			ReplyCtx:   "ctx-nil",
		}, session, e.agent, e.sessions, sessionKey, "", sessionKey)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processInteractiveMessageWith did not return after nil agentSession")
	}

	sent := p.getSent()
	if len(sent) == 0 {
		t.Fatal("expected a failure reply on the platform, got none")
	}
	if !strings.Contains(sent[0], e.i18n.T(MsgFailedToStartAgentSession)) {
		t.Fatalf("expected MsgFailedToStartAgentSession, got %q", sent[0])
	}

	// State must still be present (cleanup did NOT run) and agentSession must
	// still be nil — the user should be able to retry with a fresh message.
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	e.interactiveMu.Unlock()
	if !ok || state == nil {
		t.Fatal("expected interactive state to remain in the map for retry")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.agentSession != nil {
		t.Fatal("expected agentSession to remain nil after failed start")
	}
}

// --- 2. /compact flow ---

type stubCompressorAgent struct {
	stubAgent
	cmd string
}

func (a *stubCompressorAgent) CompressCommand() string { return a.cmd }

func TestCmdCompact_NoCompressor_RepliesNotSupported(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	msg := &Message{SessionKey: "test:user1", Content: "/compact", ReplyCtx: "ctx"}
	e.cmdCompact(p, msg)

	sent := p.getSent()
	if len(sent) == 0 {
		t.Fatal("expected a reply")
	}
	if !strings.Contains(sent[0], e.i18n.T(MsgCompressNotSupported)) {
		t.Fatalf("expected MsgCompressNotSupported, got %q", sent[0])
	}
}

func TestCmdCompact_NoSession_RepliesNoSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &stubCompressorAgent{cmd: "/compact"}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:user1", Content: "/compact", ReplyCtx: "ctx"}
	e.cmdCompact(p, msg)

	sent := p.getSent()
	if len(sent) == 0 {
		t.Fatal("expected a reply")
	}
	if !strings.Contains(sent[0], e.i18n.T(MsgCompressNoSession)) {
		t.Fatalf("expected MsgCompressNoSession, got %q", sent[0])
	}
}

func TestAutoCompress_TriggerAfterResult(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("auto-compress")
	agent := &stubCompressorAgent{cmd: "/compact"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetAutoCompressConfig(true, 4, 0) // tiny threshold

	key := "test:user1"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Seed history so estimate crosses threshold after assistant response.
	session := e.sessions.GetOrCreateActive(key)
	session.AddHistory("user", "hello world")

	// Simulate a full turn.
	go e.processInteractiveEvents(state, session, e.sessions, key, "msg1", time.Now(), func() {}, nil, nil)

	sess.events <- Event{Type: EventResult, Content: "response", Done: true}

	// The auto-compress should send /compact to the agent session.
	deadline := time.After(2 * time.Second)
	for {
		sess.sendMu.Lock()
		n := len(sess.sendCalls)
		sess.sendMu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for auto-compress send")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	sess.sendMu.Lock()
	last := sess.sendCalls[len(sess.sendCalls)-1]
	sess.sendMu.Unlock()
	if last != "/compact" {
		t.Fatalf("expected /compact auto-compress, got %q", last)
	}
}

func TestCmdCompact_SessionBusy_RepliesPreviousProcessing(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("compress-busy")
	agent := &stubCompressorAgent{cmd: "/compact"}
	agent.stubAgent = stubAgent{}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	// Lock the session to simulate busy.
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("expected TryLock to succeed")
	}

	msg := &Message{SessionKey: key, Content: "/compact", ReplyCtx: "ctx"}
	e.cmdCompact(p, msg)

	sent := p.getSent()
	found := false
	for _, s := range sent {
		if strings.Contains(s, e.i18n.T(MsgPreviousProcessing)) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected MsgPreviousProcessing reply, got %v", sent)
	}
	session.Unlock()
}

func TestCmdCompact_Success_SendsCompressDone(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("compress-ok")
	agent := &stubCompressorAgent{cmd: "/compact"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: key, Content: "/compact", ReplyCtx: "ctx"}
	e.cmdCompact(p, msg)

	// Wait for Send to be called (happens after drainEvents), then inject the result event.
	deadline := time.After(3 * time.Second)
	for {
		sess.sendMu.Lock()
		n := len(sess.sendCalls)
		sess.sendMu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for compress Send call")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	sess.events <- Event{Type: EventResult, Content: "", Done: true}

	for {
		sent := p.getSent()
		foundDone := false
		for _, s := range sent {
			if strings.Contains(s, e.i18n.T(MsgCompressDone)) {
				foundDone = true
			}
		}
		if foundDone {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for MsgCompressDone, sent = %v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestCmdCompact_WithText_SendsResult(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("compress-text")
	agent := &stubCompressorAgent{cmd: "/compact"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: key, Content: "/compact", ReplyCtx: "ctx"}
	e.cmdCompact(p, msg)

	// Wait for Send to be called (happens after drainEvents).
	deadline := time.After(3 * time.Second)
	for {
		sess.sendMu.Lock()
		n := len(sess.sendCalls)
		sess.sendMu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for compress Send call")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	sess.events <- Event{Type: EventText, Content: "Compressed to 50%"}
	sess.events <- Event{Type: EventResult, Content: "Compression complete", Done: true}

	for {
		sent := p.getSent()
		foundResult := false
		for _, s := range sent {
			if strings.Contains(s, "Compression complete") {
				foundResult = true
			}
		}
		if foundResult {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for compress result, sent = %v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestCmdCompact_DrainsQueueAfterSuccess(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newQueuingSession("compress-drain")
	agent := &stubCompressorAgent{cmd: "/compact"}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:user1"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
		pendingMessages: []queuedMessage{
			{platform: p, replyCtx: "ctx-q1", content: "queued-after-compress"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: key, Content: "/compact", ReplyCtx: "ctx"}
	e.cmdCompact(p, msg)

	// Complete compress.
	sess.events <- Event{Type: EventResult, Content: "", Done: true}

	// Wait for Send to be called (drain of queued message).
	deadline := time.After(3 * time.Second)
	for {
		sess.sendMu.Lock()
		n := len(sess.sendCalls)
		sess.sendMu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for queued message to be sent after compress")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Provide events for the drained turn so processInteractiveEvents completes.
	sess.events <- Event{Type: EventResult, Content: "drain-done", Done: true}

	// Verify the queued message was actually sent.
	time.Sleep(100 * time.Millisecond)
	sess.sendMu.Lock()
	calls := make([]string, len(sess.sendCalls))
	copy(calls, sess.sendCalls)
	sess.sendMu.Unlock()

	if len(calls) == 0 {
		t.Fatal("expected at least one Send call for the queued message")
	}
	found := false
	for _, c := range calls {
		if strings.Contains(c, "queued-after-compress") {
			found = true
		}
	}
	if !found {
		t.Fatalf("queued message not found in send calls: %v", calls)
	}
}

// --- 3. executeCardAction routing ---

func TestExecuteCardAction_StopCleansUp(t *testing.T) {
	sess := newControllableSession("stop-test")
	e := newTestEngine()
	key := "test:user1"

	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: sess}
	e.interactiveMu.Unlock()

	e.executeCardAction("/stop", "", key)

	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if exists {
		t.Error("expected interactive state to be removed after /stop")
	}
}

func TestExecuteCardAction_StopClearsInteractiveState(t *testing.T) {
	sess := newControllableSession("stop-quiet")
	e := newTestEngine()
	key := "test:user1"

	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: sess}
	e.interactiveMu.Unlock()

	e.executeCardAction("/stop", "", key)

	e.interactiveMu.Lock()
	state, exists := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if exists || state != nil {
		t.Fatal("expected interactive state to be removed after /stop")
	}
}

func TestCmdStop_ReturnsWhileCloseBlockedAndStopsEventLoop(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newBlockingCloseSession("stop-blocked")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)

	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "msg-1", time.Now(), nil, nil, "ctx")
		close(done)
	}()

	stopDone := make(chan struct{})
	go func() {
		e.cmdStop(p, &Message{SessionKey: key, ReplyCtx: "ctx"})
		close(stopDone)
	}()

	select {
	case <-sess.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected Close to start after /stop")
	}

	select {
	case <-stopDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("cmdStop blocked on Close")
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("event loop did not stop after /stop")
	}

	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if exists {
		t.Fatal("expected interactive state to be removed after /stop")
	}

	sess.events <- Event{Type: EventText, Content: "stale output"}
	sess.events <- Event{Type: EventResult, Content: "stale result", Done: true}
	time.Sleep(50 * time.Millisecond)

	sent := p.getSent()
	if len(sent) != 1 || sent[0] != e.i18n.T(MsgExecutionStopped) {
		t.Fatalf("sent messages = %v, want only execution stopped", sent)
	}

	close(sess.releaseClose)
	select {
	case <-sess.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after release")
	}
}

func TestHandleMessageRecallStopsCurrentMessageSilently(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newBlockingCloseSession("recall-active")
	defer close(sess.releaseClose)

	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx-active",
		currentMessageID: "msg-active",
		pendingMessages: []queuedMessage{
			{messageID: "msg-queued", platform: p, replyCtx: "ctx-queued", content: "queued"},
		},
	}
	e.interactiveMu.Unlock()

	e.ReceiveMessage(p, &Message{
		Platform:  "test",
		MessageID: "msg-active",
		Recalled:  true,
	})

	select {
	case <-sess.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected Close to start after recalling the active message")
	}

	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if exists {
		t.Fatal("expected interactive state to be removed after active message recall")
	}

	if sent := p.getSent(); len(sent) != 0 {
		t.Fatalf("sent messages = %v, want no user-visible stop reply for recall", sent)
	}
}

func TestHandleMessageRecallRemovesQueuedMessageSilently(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"
	state := &interactiveState{
		agentSession: newControllableSession("recall-queued"),
		platform:     p,
		replyCtx:     "ctx-active",
		pendingMessages: []queuedMessage{
			{messageID: "msg-1", platform: p, replyCtx: "ctx-1", content: "first"},
			{messageID: "msg-2", platform: p, replyCtx: "ctx-2", content: "second"},
			{messageID: "msg-3", platform: p, replyCtx: "ctx-3", content: "third"},
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.ReceiveMessage(p, &Message{
		Platform:  "test",
		MessageID: "msg-2",
		Recalled:  true,
	})

	state.mu.Lock()
	got := make([]string, len(state.pendingMessages))
	for i, queued := range state.pendingMessages {
		got[i] = queued.messageID
	}
	state.mu.Unlock()

	want := []string{"msg-1", "msg-3"}
	if len(got) != len(want) {
		t.Fatalf("pending message IDs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pending message IDs = %v, want %v", got, want)
		}
	}

	if sent := p.getSent(); len(sent) != 0 {
		t.Fatalf("sent messages = %v, want no user-visible queue removal reply for recall", sent)
	}
}

func TestHandleMessageBusyRecalledCurrentStopsAndProcessesNewMessage(t *testing.T) {
	p := &recallCheckingPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		recalled:           true,
	}
	newAgentSession := newResultAgentSession("new message processed")
	e := NewEngine("test", &resultAgent{session: newAgentSession}, []Platform{p}, "")
	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("expected to lock session for busy setup")
	}

	oldState := &interactiveState{
		agentSession:     newControllableSession("old-current"),
		platform:         p,
		replyCtx:         "old-reply-ctx",
		currentMessageID: "old-msg",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = oldState
	e.interactiveMu.Unlock()

	oldStopped := oldState.stopSignal()
	go func() {
		<-oldStopped
		session.Unlock()
	}()

	e.ReceiveMessage(p, &Message{
		SessionKey: key,
		Platform:   "test",
		MessageID:  "new-msg",
		Content:    "please handle this",
		ReplyCtx:   "new-reply-ctx",
	})

	sent := waitForPlatformSend(&p.stubPlatformEngine, 1, 3*time.Second)
	if len(sent) == 0 || sent[0] != "new message processed" {
		t.Fatalf("sent = %v, want new message processed", sent)
	}
	for _, line := range sent {
		if strings.Contains(line, e.i18n.T(MsgMessageQueued)) {
			t.Fatalf("unexpected queued reply after recalled active message: %v", sent)
		}
	}
	checked := p.checkedReplyCtxs()
	if len(checked) == 0 || checked[0] != "old-reply-ctx" {
		t.Fatalf("checked reply contexts = %v, want old-reply-ctx first", checked)
	}
	if len(newAgentSession.getSentPrompts()) != 1 || !strings.Contains(newAgentSession.getSentPrompts()[0], "please handle this") {
		t.Fatalf("new session prompts = %#v, want new message prompt", newAgentSession.getSentPrompts())
	}
}

// TestHandleMessage_BusySessionWithCanceller_InterruptsInsteadOfQueueing is
// an end-to-end regression test for the interrupt-on-new-message feature:
// when the currently busy AgentSession supports AgentSessionCanceller, a new
// incoming message should interrupt the in-flight turn and be processed
// immediately as a fresh turn, rather than going through
// queueMessageForBusySession (no MsgMessageQueued/MsgPreviousProcessing
// reply, and the new agent session actually receives the new prompt).
func TestHandleMessage_BusySessionWithCanceller_InterruptsInsteadOfQueueing(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("expected to lock session for busy setup")
	}

	steered := make(chan string, 1)
	busySess := &cancellableSession{
		controllableAgentSession: newControllableSession("old-current"),
		steerFn: func(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
			steered <- prompt
			return nil
		},
	}
	oldState := &interactiveState{
		agentSession: busySess,
		platform:     p,
		replyCtx:     "old-reply-ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = oldState
	e.interactiveMu.Unlock()

	e.ReceiveMessage(p, &Message{
		SessionKey: key,
		Platform:   "test",
		MessageID:  "new-msg",
		Content:    "please handle this",
		ReplyCtx:   "new-reply-ctx",
	})

	sent := waitForPlatformSend(p, 1, 3*time.Second)
	if len(sent) != 1 || sent[0] != e.i18n.T(MsgSteering) {
		t.Fatalf("sent = %v, want immediate steering acknowledgement", sent)
	}
	for _, line := range sent {
		if strings.Contains(line, e.i18n.T(MsgMessageQueued)) || strings.Contains(line, e.i18n.T(MsgPreviousProcessing)) {
			t.Fatalf("unexpected queue/busy reply after interrupt path: %v", sent)
		}
	}
	select {
	case prompt := <-steered:
		if prompt != "please handle this" {
			t.Fatalf("steered prompt = %q, want new message", prompt)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for direct steer")
	}
	if busySess.steerCallCount() != 1 {
		t.Fatalf("Steer called %d times, want 1", busySess.steerCallCount())
	}
}

// TestHandleMessage_PureAttachmentMessage_BusyCancellable_InsertedAsTurn is
// an end-to-end regression test for the stage-until-text change: a text-less
// file/image message arriving while a cancellable agent is mid-turn should be
// inserted directly as a new turn (interrupt + fresh Send carrying the
// attachment) rather than staged behind a "📎 Got it" acknowledgment. The
// staging path is reserved for an idle agent.
func TestHandleMessage_PureAttachmentMessage_BusyCancellable_InsertedAsTurn(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("expected to lock session for busy setup")
	}

	steeredImages := make(chan []ImageAttachment, 1)
	busySess := &cancellableSession{
		controllableAgentSession: newControllableSession("old-current"),
		steerFn: func(_ string, _ string, images []ImageAttachment, _ []FileAttachment) error {
			steeredImages <- images
			return nil
		},
	}
	oldState := &interactiveState{
		agentSession: busySess,
		platform:     p,
		replyCtx:     "old-reply-ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = oldState
	e.interactiveMu.Unlock()

	e.ReceiveMessage(p, &Message{
		SessionKey: key,
		Platform:   "test",
		MessageID:  "file-msg-1",
		Content:    "",
		Images:     []ImageAttachment{{MimeType: "image/png", Data: []byte("img1")}},
		ReplyCtx:   "new-reply-ctx",
	})

	sent := waitForPlatformSend(p, 1, 3*time.Second)
	if len(sent) != 1 || sent[0] != e.i18n.T(MsgSteering) {
		t.Fatalf("sent = %v, want immediate steering acknowledgement", sent)
	}
	for _, line := range sent {
		if strings.Contains(line, e.i18n.T(MsgAttachmentStaged)) ||
			strings.Contains(line, e.i18n.T(MsgMessageQueued)) ||
			strings.Contains(line, e.i18n.T(MsgPreviousProcessing)) {
			t.Fatalf("unexpected staged/queue/busy reply after insert path: %v", sent)
		}
	}
	select {
	case images := <-steeredImages:
		if len(images) != 1 || string(images[0].Data) != "img1" {
			t.Fatalf("steered images = %#v, want original image", images)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for direct attachment steer")
	}
}

func TestStopCurrentMessageIfRecalledThrottlesRepeatedFallbackChecks(t *testing.T) {
	p := &recallCheckingPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "test"},
		recalled:           false,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"
	state := &interactiveState{
		agentSession:     newControllableSession("current"),
		platform:         p,
		replyCtx:         "reply-ctx-1",
		currentMessageID: "msg-1",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	for range 3 {
		if e.stopCurrentMessageIfRecalled(key) {
			t.Fatal("stopCurrentMessageIfRecalled returned true for non-recalled message")
		}
	}
	checked := p.checkedReplyCtxs()
	if len(checked) != 1 || checked[0] != "reply-ctx-1" {
		t.Fatalf("checked reply contexts = %v, want exactly one check for reply-ctx-1", checked)
	}

	state.mu.Lock()
	state.replyCtx = "reply-ctx-2"
	state.currentMessageID = "msg-2"
	state.lastRecallProbeMessageID = ""
	state.lastRecallProbeAt = time.Time{}
	state.recallProbeInFlight = false
	state.mu.Unlock()

	if e.stopCurrentMessageIfRecalled(key) {
		t.Fatal("stopCurrentMessageIfRecalled returned true for second non-recalled message")
	}
	checked = p.checkedReplyCtxs()
	if len(checked) != 2 || checked[1] != "reply-ctx-2" {
		t.Fatalf("checked reply contexts = %v, want second check for new message", checked)
	}
}

func TestExecuteCardAction_NewCleansUpAndCreatesSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	key := "test:user1"

	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: newControllableSession("old")}
	e.interactiveMu.Unlock()

	e.executeCardAction("/new", "", key)

	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[key]
	e.interactiveMu.Unlock()

	if exists {
		t.Error("expected old interactive state to be cleaned up after /new")
	}
}

func TestExecuteCardAction_UnknownCommand_NoPanic(t *testing.T) {
	e := newTestEngine()
	// Should not panic for unrecognized commands.
	e.executeCardAction("/nonexistent", "args", "test:user1")
	e.executeCardAction("", "", "test:user1")
}

// --- 4. Multi-workspace command handlers use interactiveKey ---

func TestCmdStop_UsesInteractiveKeyForMultiWorkspace(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("ws-stop-test")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	wsDir := t.TempDir()
	rawKey := "feishu:ch1:user1"
	wsKey := wsDir + ":" + rawKey

	iKey := e.interactiveKeyForSessionKey(wsKey)
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = &interactiveState{agentSession: sess}
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: wsKey, Content: "/stop", ReplyCtx: "ctx"}
	e.cmdStop(p, msg)

	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[iKey]
	e.interactiveMu.Unlock()

	if exists {
		t.Error("expected interactive state to be cleaned up by /stop using interactiveKey")
	}
}

// ===========================================================================
// Beta pre-release tests: inject_sender, idle_timeout, /shell, /workspace,
//                         /switch
// ===========================================================================

// --- 1. inject_sender ---

func TestBuildSenderPrompt_Enabled(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	result := e.buildSenderPrompt("hello world", "user123", "Alice", "feishu", "feishu:channel42:user123", "")
	expected := "[agent-bridge sender_id=user123 sender_name=\"Alice\" platform=feishu chat_id=channel42]\nhello world"
	if result != expected {
		t.Fatalf("got %q, want %q", result, expected)
	}
}

func TestBuildSenderPrompt_Disabled(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(false)

	result := e.buildSenderPrompt("hello", "user1", "Alice", "feishu", "feishu:ch:user1", "")
	if result != "hello" {
		t.Fatalf("expected raw content when disabled, got %q", result)
	}
}

func TestBuildSenderPrompt_EmptyUserID(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	result := e.buildSenderPrompt("hello", "", "Bob", "telegram", "telegram:ch:user1", "")
	if result != "hello" {
		t.Fatalf("expected raw content when userID is empty, got %q", result)
	}
}

func TestBuildSenderPrompt_EmptyUserName(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	result := e.buildSenderPrompt("hello", "user1", "", "feishu", "feishu:ch:user1", "")
	expected := "[agent-bridge sender_id=user1 platform=feishu chat_id=ch]\nhello"
	if result != expected {
		t.Fatalf("got %q, want %q", result, expected)
	}
}

func TestBuildSenderPrompt_NameWithSpaces(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	result := e.buildSenderPrompt("hi", "U999", "Jim Tang", "slack", "slack:C012:U999", "")
	expected := "[agent-bridge sender_id=U999 sender_name=\"Jim Tang\" platform=slack chat_id=C012]\nhi"
	if result != expected {
		t.Fatalf("got %q, want %q", result, expected)
	}
}

func TestExtractChannelID(t *testing.T) {
	tests := []struct {
		key  string
		want string
	}{
		{"feishu:channel42:user1", "channel42"},
		{"telegram:group123:user2", "group123"},
		{"plain", ""},
		{"a:b", "b"},
		{"a:bb:c:d", "bb"},
		{"dingtalk:g:cidXXX:staff1", "cidXXX"},
		{"dingtalk:d:cidYYY:staff2", "cidYYY"},
		// 3-segment shared-session keys with single-char type tag — used by
		// dingtalk/qq/qqbot when share_session_in_channel is enabled.
		{"dingtalk:g:cidZZZ", "cidZZZ"},
		{"qq:g:12345", "12345"},
		{"qqbot:g:openid_abc", "openid_abc"},
	}
	for _, tt := range tests {
		got := extractChannelID(tt.key)
		if got != tt.want {
			t.Errorf("extractChannelID(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestBuildSenderPrompt_DifferentPlatforms(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	platforms := []struct {
		platform   string
		sessionKey string
		wantChat   string
	}{
		{"telegram", "telegram:group99:alice", "group99"},
		{"discord", "discord:server1:bob", "server1"},
		{"slack", "slack:C012345:carol", "C012345"},
	}
	for _, tc := range platforms {
		result := e.buildSenderPrompt("msg", "uid", "TestUser", tc.platform, tc.sessionKey, "")
		if !strings.Contains(result, "platform="+tc.platform) {
			t.Errorf("missing platform=%s in %q", tc.platform, result)
		}
		if !strings.Contains(result, "chat_id="+tc.wantChat) {
			t.Errorf("missing chat_id=%s in %q", tc.wantChat, result)
		}
	}
}

func TestBuildSenderPrompt_SanitizesSpecialChars(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	result := e.buildSenderPrompt("hi", "U1", "Evil\"Name\nInject", "slack", "slack:C1:U1", "")
	if strings.Contains(result, `"Name`) || strings.Contains(result, "\n"+`Inject`) {
		t.Fatalf("quotes/newlines should be sanitized, got %q", result)
	}
	if !strings.Contains(result, `sender_name="Evil'Name Inject"`) {
		t.Fatalf("expected sanitized name, got %q", result)
	}
}

func TestBuildSenderPrompt_ChannelKeyOverridesSessionKey(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	// When channelKey is provided, it should be used as chat_id instead of
	// extracting from sessionKey (which would give "g" for dingtalk).
	result := e.buildSenderPrompt("hello", "staff1", "Alice", "dingtalk", "dingtalk:g:cidXXX:staff1", "cidXXX")
	expected := "[agent-bridge sender_id=staff1 sender_name=\"Alice\" platform=dingtalk chat_id=cidXXX]\nhello"
	if result != expected {
		t.Fatalf("got %q, want %q", result, expected)
	}
}

func TestBuildSenderPrompt_FallbackWithoutChannelKey(t *testing.T) {
	e := newTestEngine()
	e.SetInjectSender(true)

	// When channelKey is empty, extractChannelID heuristic should detect
	// the 4-segment format and extract the correct channel.
	result := e.buildSenderPrompt("hello", "staff1", "Alice", "dingtalk", "dingtalk:g:cidXXX:staff1", "")
	expected := "[agent-bridge sender_id=staff1 sender_name=\"Alice\" platform=dingtalk chat_id=cidXXX]\nhello"
	if result != expected {
		t.Fatalf("got %q, want %q", result, expected)
	}
}

// --- 2. idle_timeout ---

func TestEventIdleTimeout_CleansUpSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("idle-test")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetEventIdleTimeout(100 * time.Millisecond)

	key := "test:idle-user"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	session := e.sessions.GetOrCreateActive(key)
	session.TryLock()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "", time.Now(), nil, nil, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processInteractiveEvents did not return after idle timeout")
	}

	sent := p.getSent()
	foundTimeout := false
	for _, s := range sent {
		if strings.Contains(s, "timed out") {
			foundTimeout = true
		}
	}
	if !foundTimeout {
		t.Fatalf("expected timeout error message, got %v", sent)
	}
}

func TestEventIdleTimeout_ResetOnEvent(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("idle-reset")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetEventIdleTimeout(200 * time.Millisecond)

	key := "test:idle-reset"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	session := e.sessions.GetOrCreateActive(key)
	session.TryLock()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "", time.Now(), nil, nil, nil)
		close(done)
	}()

	// Send a text event at 100ms (before the 200ms timeout), resetting the timer.
	time.Sleep(100 * time.Millisecond)
	sess.events <- Event{Type: EventText, Content: "thinking..."}

	// Then send the result at 150ms after the text event (within the reset 200ms window).
	time.Sleep(150 * time.Millisecond)
	sess.events <- Event{Type: EventResult, Content: "done", Done: true}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("processInteractiveEvents did not complete after events")
	}

	sent := p.getSent()
	foundTimeout := false
	for _, s := range sent {
		if strings.Contains(s, "timed out") {
			foundTimeout = true
		}
	}
	if foundTimeout {
		t.Error("should NOT have timed out — events should have reset the timer")
	}
}

func TestEventIdleTimeout_DisabledWhenZero(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("idle-zero")
	agent := &controllableAgent{nextSession: sess}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetEventIdleTimeout(0)

	key := "test:idle-zero"
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	session := e.sessions.GetOrCreateActive(key)
	session.TryLock()

	done := make(chan struct{})
	go func() {
		e.processInteractiveEvents(state, session, e.sessions, key, "", time.Now(), nil, nil, nil)
		close(done)
	}()

	// With timeout disabled, it should block until we send a result.
	time.Sleep(50 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("should not have returned yet — timeout is disabled and no events sent")
	default:
	}

	sess.events <- Event{Type: EventResult, Content: "ok", Done: true}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("did not return after result event")
	}
}

// --- 3. /shell command ---

func TestCmdShell_BlockedWithoutAdmin(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	msg := &Message{
		SessionKey: "test:ch:user1",
		Content:    "/shell ls -la",
		ReplyCtx:   "ctx",
		UserID:     "user1",
		Platform:   "test",
	}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundAdmin := false
	for _, s := range sent {
		if strings.Contains(s, e.i18n.T(MsgAdminRequired)[:10]) || strings.Contains(s, "admin") {
			foundAdmin = true
		}
	}
	if !foundAdmin {
		t.Fatalf("expected admin required reply, got %v", sent)
	}
}

func TestCmdShell_AllowedForAdmin(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetAdminFrom("admin-user")

	msg := &Message{
		SessionKey: "test:ch:admin-user",
		Content:    "/shell echo hello",
		ReplyCtx:   "ctx",
		UserID:     "admin-user",
		Platform:   "test",
	}
	e.handleCommand(p, msg, msg.Content)

	// Give the async goroutine time to complete.
	time.Sleep(500 * time.Millisecond)

	sent := p.getSent()
	foundAdmin := false
	for _, s := range sent {
		if strings.Contains(s, "admin") && strings.Contains(s, "privilege") {
			foundAdmin = true
		}
	}
	if foundAdmin {
		t.Fatalf("admin user should not be blocked, got %v", sent)
	}
}

func TestCmdShell_EmptyCommand_ShowsUsage(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetAdminFrom("admin")

	// Call cmdShell directly with empty command to test usage path.
	msg := &Message{
		SessionKey: "test:ch:admin",
		Content:    "/shell",
		ReplyCtx:   "ctx",
		UserID:     "admin",
		Platform:   "test",
	}
	e.cmdShell(p, msg, "/shell ")

	sent := p.getSent()
	foundUsage := false
	for _, s := range sent {
		if strings.Contains(s, "Usage") || strings.Contains(s, "/shell") {
			foundUsage = true
		}
	}
	if !foundUsage {
		t.Fatalf("expected usage message, got %v", sent)
	}
}

// --- truncateRunes tests ---

func TestTruncateRunes(t *testing.T) {
	t.Run("short string unchanged", func(t *testing.T) {
		if got := truncateRunes("hello", 10); got != "hello" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("exact limit unchanged", func(t *testing.T) {
		if got := truncateRunes("abcde", 5); got != "abcde" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("ascii truncation", func(t *testing.T) {
		got := truncateRunes("abcdefghij", 7)
		if got != "abcd..." {
			t.Errorf("got %q", got)
		}
	})
	t.Run("multi-byte truncation at rune boundary", func(t *testing.T) {
		input := strings.Repeat("中", 10)
		got := truncateRunes(input, 7)
		if !utf8.ValidString(got) {
			t.Errorf("produced invalid UTF-8: %q", got)
		}
		runes := []rune(got)
		if len(runes) != 7 {
			t.Errorf("expected 7 runes, got %d (%q)", len(runes), got)
		}
		if !strings.HasSuffix(got, "...") {
			t.Errorf("expected trailing ..., got %q", got)
		}
	})
	t.Run("exact max multi-byte unchanged", func(t *testing.T) {
		input := strings.Repeat("中", 5)
		got := truncateRunes(input, 5)
		if got != input {
			t.Errorf("expected no truncation, got %q", got)
		}
	})
	t.Run("max less than 4 clamped", func(t *testing.T) {
		// Should not panic when max < 4
		got := truncateRunes("abcdefgh", 2)
		runes := []rune(got)
		if len(runes) != 4 {
			t.Errorf("expected 4 runes (clamped), got %d (%q)", len(runes), got)
		}
	})
}

// --- runShellWithProgress tests ---

func TestRunShellWithProgress_BasicOutput(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	err := e.runShellWithProgress(p, "ctx", "echo hello", t.TempDir(), 5*time.Second, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if !strings.Contains(last, "hello") {
				t.Errorf("expected output to contain 'hello', got %q", last)
			}
			if strings.Contains(last, "✅") {
				t.Errorf("unexpected success emoji, got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shell response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunShellWithProgress_FailedCommand(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	err := e.runShellWithProgress(p, "ctx", "exit 42", t.TempDir(), 5*time.Second, 4000)
	if err == nil {
		t.Fatal("expected error for non-zero exit")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if strings.Contains(last, "❌") {
				t.Errorf("unexpected failure emoji, got %q", last)
			}
			if !strings.Contains(last, "42") {
				t.Errorf("expected exit code 42 in output, got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shell response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunShellWithProgress_Timeout(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	err := e.runShellWithProgress(p, "ctx", "sleep 30", t.TempDir(), 200*time.Millisecond, 4000)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected 'timed out' in error, got %q", err.Error())
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if !strings.Contains(last, "⚠️") && !strings.Contains(last, "timeout") {
				t.Errorf("expected timeout indicator, got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for timeout message")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunShellWithProgress_EmptyOutput(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	cmd := "true"
	if runtime.GOOS == "windows" {
		cmd = `cmd /c "exit /b 0"`
	}
	err := e.runShellWithProgress(p, "ctx", cmd, t.TempDir(), 5*time.Second, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if !strings.Contains(last, "(no output)") {
				t.Errorf("expected '(no output)', got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shell response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunShellWithProgress_StderrOutput(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	cmd := "echo err >&2"
	if runtime.GOOS == "windows" {
		cmd = `cmd /c "echo err >&2"`
	}
	err := e.runShellWithProgress(p, "ctx", cmd, t.TempDir(), 5*time.Second, 4000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if !strings.Contains(last, "err") {
				t.Errorf("expected stderr output 'err', got %q", last)
			}
			if strings.Contains(last, "✅") {
				t.Errorf("unexpected success emoji, got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shell response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunShellWithProgress_LongOutputTruncated(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	// Generate output longer than maxOutput
	cmd := "python3 -c 'print(\"x\" * 5000)'"
	timeout := 5 * time.Second
	if runtime.GOOS == "windows" {
		cmd = `Write-Host ('x' * 5000)`
		timeout = 15 * time.Second
	}
	err := e.runShellWithProgress(p, "ctx", cmd, t.TempDir(), timeout, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if !utf8.ValidString(last) {
				t.Errorf("output contains invalid UTF-8")
			}
			// Should be truncated — the code block content should end with "..."
			if !strings.Contains(last, "...") {
				t.Errorf("expected truncation marker '...', got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shell response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunShellWithProgress_NonexistentCommand(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	err := e.runShellWithProgress(p, "ctx", "nonexistent_command_xyz_12345", t.TempDir(), 5*time.Second, 4000)
	if err == nil {
		t.Fatal("expected error for nonexistent command")
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			last := sent[len(sent)-1]
			if strings.Contains(last, "❌") {
				t.Errorf("unexpected failure emoji, got %q", last)
			}
			if !strings.Contains(last, "failed to start") && !strings.Contains(last, "not found") && !strings.Contains(last, "executable file not found") && !strings.Contains(last, "CommandNotFoundException") {
				t.Errorf("expected start failure message, got %q", last)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shell response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- /diff command tests ---

func TestCmdDiff_BlockedWithoutAdmin(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	msg := &Message{
		SessionKey: "test:ch:user1",
		Content:    "/diff main",
		ReplyCtx:   "ctx",
		UserID:     "user1",
		Platform:   "test",
	}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundAdmin := false
	for _, s := range sent {
		if strings.Contains(s, "admin") || strings.Contains(s, e.i18n.T(MsgAdminRequired)[:10]) {
			foundAdmin = true
		}
	}
	if !foundAdmin {
		t.Fatalf("expected admin required reply, got %v", sent)
	}
}

func TestCmdDiff_EmptyDiff(t *testing.T) {
	// Create a temp git repo with no changes
	dir := t.TempDir()
	cmds := [][]string{
		{"git", "init"},
		{"git", "config", "user.email", "test@test.com"},
		{"git", "config", "user.name", "test"},
		{"git", "commit", "--allow-empty", "-m", "init"},
	}
	for _, args := range cmds {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("setup %v: %s %v", args, out, err)
		}
	}

	agent := &stubWorkDirAgent{workDir: dir}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetAdminFrom("admin")

	msg := &Message{
		SessionKey: "test:ch:admin",
		Content:    "/diff",
		ReplyCtx:   "ctx",
		UserID:     "admin",
		Platform:   "test",
	}
	e.cmdDiff(p, msg, "/diff")

	deadline := time.Now().Add(2 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			found := false
			for _, s := range sent {
				if strings.Contains(s, "diff") || strings.Contains(s, "clean") {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected empty diff message, got %v", sent)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for diff response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCmdDiff_PlainTextFallback(t *testing.T) {
	// Create a temp git repo with uncommitted changes
	dir := t.TempDir()
	cmds := [][]string{
		{"git", "init"},
		{"git", "config", "user.email", "test@test.com"},
		{"git", "config", "user.name", "test"},
	}
	for _, args := range cmds {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("setup %v: %s %v", args, out, err)
		}
	}
	// Create and commit a file, then modify it
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"git", "add", "test.txt"},
		{"git", "commit", "-m", "add test.txt"},
	} {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("setup %v: %s %v", args, out, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello\nworld\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Use stubPlatformEngine (no FileSender) → should fall back to plain text
	agent := &stubWorkDirAgent{workDir: dir}
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetAdminFrom("admin")

	msg := &Message{
		SessionKey: "test:ch:admin",
		Content:    "/diff",
		ReplyCtx:   "ctx",
		UserID:     "admin",
		Platform:   "test",
	}
	e.cmdDiff(p, msg, "/diff")

	deadline := time.Now().Add(2 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) > 0 {
			found := false
			for _, s := range sent {
				if strings.Contains(s, "```diff") && strings.Contains(s, "world") {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected plain text diff with ```diff block, got %v", sent)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for diff response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCmdDiff_FileSenderPath(t *testing.T) {
	// Create a temp git repo with uncommitted changes
	dir := t.TempDir()
	cmds := [][]string{
		{"git", "init"},
		{"git", "config", "user.email", "test@test.com"},
		{"git", "config", "user.name", "test"},
	}
	for _, args := range cmds {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("setup %v: %s %v", args, out, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"git", "add", "test.txt"},
		{"git", "commit", "-m", "add test.txt"},
	} {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("setup %v: %s %v", args, out, err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "test.txt"), []byte("changed\n"), 0644); err != nil {
		t.Fatal(err)
	}

	agent := &stubWorkDirAgent{workDir: dir}
	mp := &stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("test", agent, []Platform{mp}, "")
	e.SetAdminFrom("admin")

	msg := &Message{
		SessionKey: "test:ch:admin",
		Content:    "/diff",
		ReplyCtx:   "ctx",
		UserID:     "admin",
		Platform:   "test",
	}
	e.cmdDiff(mp, msg, "/diff")

	deadline := time.Now().Add(2 * time.Second)
	for {
		// If diff2html is installed, we get a file; otherwise plain text fallback
		files := mp.files
		sent := mp.getSent()
		if len(files) > 0 {
			f := files[0]
			if f.MimeType != "text/html" {
				t.Fatalf("expected text/html, got %s", f.MimeType)
			}
			if !strings.HasSuffix(f.FileName, ".html") {
				t.Fatalf("expected .html filename, got %s", f.FileName)
			}
			return
		}
		if len(sent) > 0 {
			// diff2html not installed → plain text fallback is also acceptable
			found := false
			for _, s := range sent {
				if strings.Contains(s, "```diff") {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected diff output (file or plain text), got %v", sent)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for diff response")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCmdShow_EmptyReference_ShowsUsage(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetAdminFrom("admin")

	msg := &Message{
		SessionKey: "test:ch:admin",
		Content:    "/show",
		ReplyCtx:   "ctx",
		UserID:     "admin",
		Platform:   "test",
	}
	e.cmdShow(p, msg, nil)

	sent := p.getSent()
	if len(sent) != 1 || !strings.Contains(sent[0], "/show") {
		t.Fatalf("sent = %v, want show usage", sent)
	}
}

func TestHandleCommand_ShowRequiresAdmin(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetAdminFrom("admin")

	msg := &Message{
		SessionKey: "test:ch:user1",
		Content:    "/show foo.txt",
		ReplyCtx:   "ctx",
		UserID:     "user1",
		Platform:   "test",
	}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	if len(sent) != 1 || !strings.Contains(strings.ToLower(sent[0]), "admin") {
		t.Fatalf("sent = %v, want admin required message", sent)
	}
}

func TestCmdShow_OutputRemainsRawWhenReferencesEnabled(t *testing.T) {
	p := &stubPlatformEngine{n: "feishu"}
	agent := &stubWorkDirAgent{workDir: t.TempDir()}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetAdminFrom("admin")
	e.references = normalizeReferenceRenderCfg(ReferenceRenderCfg{
		NormalizeAgents: []string{"all"},
		RenderPlatforms: []string{"all"},
		DisplayPath:     "relative",
		MarkerStyle:     "emoji",
		EnclosureStyle:  "code",
	})

	file := filepath.Join(agent.workDir, "svc", "handler.go")
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	rawLine := "/root/code/demo-repo/ui/recovery_contact_form.tsx:11"
	if err := os.WriteFile(file, []byte(rawLine+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	msg := &Message{
		SessionKey: "test:ch:admin",
		Content:    "/show svc/handler.go",
		ReplyCtx:   "ctx",
		UserID:     "admin",
		Platform:   "feishu",
	}
	e.cmdShow(p, msg, []string{"svc/handler.go"})

	sent := p.getSent()
	if len(sent) != 1 {
		t.Fatalf("sent = %v, want one response", sent)
	}
	if !strings.Contains(sent[0], rawLine) {
		t.Fatalf("output = %q, want raw code content preserved", sent[0])
	}
}

// --- 4. /workspace subcommands ---

// --- 5. /switch ---

type switchableAgent struct {
	stubAgent
	sessions []AgentSessionInfo
}

func (a *switchableAgent) ListSessions(_ context.Context) ([]AgentSessionInfo, error) {
	return a.sessions, nil
}

func TestCmdSwitch_NoArgs_ShowsUsage(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")

	msg := &Message{SessionKey: "test:ch:user1", Content: "/switch", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundUsage := false
	for _, s := range sent {
		if strings.Contains(s, "Usage") || strings.Contains(s, "/switch") {
			foundUsage = true
		}
	}
	if !foundUsage {
		t.Fatalf("expected usage reply, got %v", sent)
	}
}

func TestCmdSwitch_ByIndex_SetsSession(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &switchableAgent{
		sessions: []AgentSessionInfo{
			{ID: "sess-aaa", Summary: "First session", MessageCount: 5},
			{ID: "sess-bbb", Summary: "Second session", MessageCount: 3},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:ch:user1"

	// Pre-create an interactive state to verify cleanup.
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: newControllableSession("old")}
	e.interactiveMu.Unlock()

	msg := &Message{SessionKey: key, Content: "/switch 2", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundSwitch := false
	for _, s := range sent {
		if strings.Contains(s, "Second session") || strings.Contains(s, "sess-bbb") {
			foundSwitch = true
		}
	}
	if !foundSwitch {
		t.Fatalf("expected switch success reply referencing session 2, got %v", sent)
	}

	// Verify old interactive state was cleaned up.
	e.interactiveMu.Lock()
	_, exists := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if exists {
		t.Error("expected old interactive state to be cleaned up after /switch")
	}

	// Verify session was updated.
	session := e.sessions.GetOrCreateActive(key)
	if id := session.GetAgentSessionID(); id != "sess-bbb" {
		t.Errorf("expected session ID sess-bbb, got %q", id)
	}
}

func TestCmdSwitch_ByIDPrefix(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &switchableAgent{
		sessions: []AgentSessionInfo{
			{ID: "abc-123-def", Summary: "Target session"},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:ch:user1", Content: "/switch abc-123", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundSwitch := false
	for _, s := range sent {
		if strings.Contains(s, "Target session") || strings.Contains(s, "abc-123") {
			foundSwitch = true
		}
	}
	if !foundSwitch {
		t.Fatalf("expected switch by prefix to succeed, got %v", sent)
	}
}

func TestCmdSwitch_NoMatch(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &switchableAgent{
		sessions: []AgentSessionInfo{
			{ID: "sess-111", Summary: "Only session"},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{SessionKey: "test:ch:user1", Content: "/switch nonexistent", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundNoMatch := false
	for _, s := range sent {
		if strings.Contains(s, "nonexistent") {
			foundNoMatch = true
		}
	}
	if !foundNoMatch {
		t.Fatalf("expected no-match reply, got %v", sent)
	}
}

func TestCmdSwitch_ByName(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agent := &switchableAgent{
		sessions: []AgentSessionInfo{
			{ID: "sess-named-1", Summary: "Unnamed"},
			{ID: "sess-named-2", Summary: "My Feature"},
		},
	}
	e := NewEngine("test", agent, []Platform{p}, "")

	key := "test:ch:user1"
	// Set a custom name for the second session.
	e.sessions.SetSessionName("sess-named-2", "feature-branch")

	msg := &Message{SessionKey: key, Content: "/switch feature-branch", ReplyCtx: "ctx"}
	e.handleCommand(p, msg, msg.Content)

	sent := p.getSent()
	foundSwitch := false
	for _, s := range sent {
		if strings.Contains(s, "My Feature") || strings.Contains(s, "feature-branch") || strings.Contains(s, "sess-named-2") {
			foundSwitch = true
		}
	}
	if !foundSwitch {
		t.Fatalf("expected switch by name to succeed, got %v", sent)
	}
}

// ── /whoami tests ───────────────────────────────────────────

func TestCmdWhoami_ShowsUserID(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "telegram"}

	msg := &Message{
		SessionKey: "telegram:chat123:user456",
		Platform:   "telegram",
		UserID:     "user456",
		UserName:   "Alice",
		ReplyCtx:   "ctx",
		Content:    "/whoami",
	}
	e.handleCommand(p, msg, msg.Content)

	if len(p.sent) == 0 {
		t.Fatal("expected /whoami to produce a reply")
	}
	reply := p.sent[0]
	if !strings.Contains(reply, "user456") {
		t.Errorf("expected reply to contain user ID 'user456', got: %s", reply)
	}
	if !strings.Contains(reply, "Alice") {
		t.Errorf("expected reply to contain user name 'Alice', got: %s", reply)
	}
	if !strings.Contains(reply, "telegram") {
		t.Errorf("expected reply to contain platform 'telegram', got: %s", reply)
	}
	if !strings.Contains(reply, "chat123") {
		t.Errorf("expected reply to contain chat ID 'chat123', got: %s", reply)
	}
	if !strings.Contains(reply, "allow_from") {
		t.Errorf("expected reply to mention allow_from usage, got: %s", reply)
	}
}

func TestCmdWhoami_EmptyUserID(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{
		SessionKey: "test:ch1",
		Platform:   "test",
		UserID:     "",
		ReplyCtx:   "ctx",
		Content:    "/whoami",
	}
	e.handleCommand(p, msg, msg.Content)

	if len(p.sent) == 0 {
		t.Fatal("expected /whoami to produce a reply")
	}
	if !strings.Contains(p.sent[0], "(unknown)") {
		t.Errorf("expected '(unknown)' for empty UserID, got: %s", p.sent[0])
	}
}

func TestCmdWhoami_AliasMyID(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}

	msg := &Message{
		SessionKey: "test:ch1:u1",
		Platform:   "test",
		UserID:     "u1",
		ReplyCtx:   "ctx",
		Content:    "/myid",
	}
	e.handleCommand(p, msg, msg.Content)

	if len(p.sent) == 0 {
		t.Fatal("expected /myid alias to produce a reply")
	}
	if !strings.Contains(p.sent[0], "u1") {
		t.Errorf("expected reply to contain user ID, got: %s", p.sent[0])
	}
}

func TestCmdWhoami_CardPlatform(t *testing.T) {
	p := &stubCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	agent := &stubModelModeAgent{model: "gpt-4.1", mode: "default"}
	e := NewEngine("test", agent, []Platform{p}, "")

	msg := &Message{
		SessionKey: "feishu:chat999:ou_abc123",
		Platform:   "feishu",
		UserID:     "ou_abc123",
		UserName:   "张三",
		ReplyCtx:   "ctx",
		Content:    "/whoami",
	}
	e.handleCommand(p, msg, msg.Content)

	if len(p.repliedCards) == 0 && len(p.sentCards) == 0 {
		t.Fatal("expected /whoami to produce a card")
	}

	var card *Card
	if len(p.repliedCards) > 0 {
		card = p.repliedCards[0]
	} else {
		card = p.sentCards[0]
	}

	if card.Header == nil || card.Header.Title == "" {
		t.Fatal("expected card to have a header title")
	}

	text := card.RenderText()
	if !strings.Contains(text, "ou_abc123") {
		t.Errorf("expected card to contain user ID, got: %s", text)
	}
	if !strings.Contains(text, "张三") {
		t.Errorf("expected card to contain user name, got: %s", text)
	}
	if !strings.Contains(text, "feishu") {
		t.Errorf("expected card to contain platform, got: %s", text)
	}
	if !strings.Contains(text, "chat999") {
		t.Errorf("expected card to contain chat ID, got: %s", text)
	}
}

// ---------------------------------------------------------------------------
// Engine method coverage tests
// ---------------------------------------------------------------------------

func TestEngine_AddPlatform(t *testing.T) {
	agent := &stubAgent{}
	p1 := &stubPlatformEngine{n: "feishu"}
	p2 := &stubPlatformEngine{n: "telegram"}

	e := NewEngine("test", agent, []Platform{p1}, "")

	// Initially has 1 platform
	if len(e.platforms) != 1 {
		t.Fatalf("expected 1 platform, got %d", len(e.platforms))
	}

	// Add another platform
	e.AddPlatform(p2)

	if len(e.platforms) != 2 {
		t.Fatalf("expected 2 platforms, got %d", len(e.platforms))
	}

	if e.platforms[0].Name() != "feishu" {
		t.Errorf("expected first platform to be feishu, got %s", e.platforms[0].Name())
	}
	if e.platforms[1].Name() != "telegram" {
		t.Errorf("expected second platform to be telegram, got %s", e.platforms[1].Name())
	}
}

func TestEngine_GetAgent(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}

	e := NewEngine("test", agent, []Platform{p}, "")

	// GetAgent should return the agent
	got := e.GetAgent()
	if got == nil {
		t.Fatal("expected GetAgent to return agent, got nil")
	}
	if got.Name() != "stub" {
		t.Errorf("expected agent name 'stub', got %s", got.Name())
	}
}

func TestEngine_ClearCommands(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}

	e := NewEngine("test", agent, []Platform{p}, "")

	// Add commands from two sources
	e.AddCommand("cmd1", "desc1", "prompt1", "", "", "config")
	e.AddCommand("cmd2", "desc2", "prompt2", "", "", "agent")

	// Verify commands exist
	if _, ok := e.commands.Resolve("cmd1"); !ok {
		t.Fatal("expected cmd1 to exist")
	}

	// Clear commands from config source
	e.ClearCommands("config")

	// cmd1 should be gone, cmd2 should remain
	if _, ok := e.commands.Resolve("cmd1"); ok {
		t.Error("expected cmd1 to be cleared")
	}
	if _, ok := e.commands.Resolve("cmd2"); !ok {
		t.Error("expected cmd2 to remain after clearing config source")
	}
}

func TestEngine_SetAndGetAgent(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}

	e := NewEngine("test", agent, []Platform{p}, "")

	// Verify GetAgent returns correct agent
	got := e.GetAgent()
	if got.Name() != "stub" {
		t.Errorf("expected agent name 'stub', got %s", got.Name())
	}
}

func TestEngine_AddCommand(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}

	e := NewEngine("test", agent, []Platform{p}, "")

	// Add a command
	e.AddCommand("testcmd", "A test command", "This is a test {{args}}", "", "", "config")

	// Resolve should find it
	cmd, ok := e.commands.Resolve("testcmd")
	if !ok {
		t.Fatal("expected to resolve testcmd")
	}
	if cmd.Name != "testcmd" {
		t.Errorf("expected command name 'testcmd', got %s", cmd.Name)
	}
	if cmd.Description != "A test command" {
		t.Errorf("expected description 'A test command', got %s", cmd.Description)
	}
	if cmd.Prompt != "This is a test {{args}}" {
		t.Errorf("expected prompt 'This is a test {{args}}', got %s", cmd.Prompt)
	}
}

func TestEngine_AddAlias(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}

	e := NewEngine("test", agent, []Platform{p}, "")

	// Add an alias
	e.AddAlias("shortcut", "very-long-command")

	// Check alias was stored (via internal map)
	// We can verify this through command resolution if shortcut is used as a command
	e.AddCommand("very-long-command", "Long command", "prompt", "", "", "config")

	// The alias mechanism works through the alias map
	if len(e.aliases) != 1 {
		t.Fatalf("expected 1 alias, got %d", len(e.aliases))
	}
}

func TestEstimateTokensWithPendingAssistant(t *testing.T) {
	// Test with pending assistant message
	entries := []HistoryEntry{
		{Role: "user", Content: "Hello"},
	}
	got := estimateTokensWithPendingAssistant(entries, "Thinking...")
	if got <= 0 {
		t.Errorf("estimateTokensWithPendingAssistant([Hello], Thinking...) = %d, want > 0", got)
	}

	// Pending message should add to the count
	gotWithoutPending := estimateTokensWithPendingAssistant(entries, "")
	gotWithPending := estimateTokensWithPendingAssistant(entries, "Extra content here")
	if gotWithPending <= gotWithoutPending {
		t.Errorf("expected pending message to increase token count")
	}
}

func TestTruncateHistoryEntry(t *testing.T) {
	if got := truncateHistoryEntry("abcdef", 3); got != "abc..." {
		t.Fatalf("truncateHistoryEntry ascii = %q, want %q", got, "abc...")
	}
	if got := truncateHistoryEntry("你好世界", 2); got != "你好..." {
		t.Fatalf("truncateHistoryEntry unicode = %q, want %q", got, "你好...")
	}
	if got := truncateHistoryEntry("👨‍👩‍👧 中文", 2); got != "👨‍..." || !utf8.ValidString(got) {
		t.Fatalf("truncateHistoryEntry emoji = %q, want valid UTF-8 %q", got, "👨‍...")
	}
	if got := truncateHistoryEntry("abcdef", 0); got != "abcdef" {
		t.Fatalf("truncateHistoryEntry disabled = %q, want original", got)
	}
}

func TestEngineHistoryEntryMaxLen(t *testing.T) {
	e := NewEngine("test", &stubAgent{}, []Platform{&stubPlatformEngine{n: "test"}}, filepath.Join(t.TempDir(), "sessions.json"))
	if got := e.historyEntryMaxLen(); got != defaultHistoryMaxLen {
		t.Fatalf("default historyEntryMaxLen = %d, want %d", got, defaultHistoryMaxLen)
	}

	limit := 0
	e.SetDisplayConfig(DisplayCfg{HistoryMaxLen: &limit})
	if got := e.historyEntryMaxLen(); got != 0 {
		t.Fatalf("configured historyEntryMaxLen = %d, want 0", got)
	}
}

type recordingTTS struct {
	mu    sync.Mutex
	text  string
	opts  TTSSynthesisOpts
	calls int
}

func (t *recordingTTS) Synthesize(_ context.Context, text string, opts TTSSynthesisOpts) ([]byte, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.text = text
	t.opts = opts
	t.calls++
	return []byte("audio-bytes"), "mp3", nil
}

func (t *recordingTTS) snapshot() (string, TTSSynthesisOpts, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.text, t.opts, t.calls
}

type audioStubPlatform struct {
	stubPlatformEngine
	mu         sync.Mutex
	audio      []byte
	format     string
	audioCalls int
}

func (p *audioStubPlatform) SendAudio(_ context.Context, _ any, audio []byte, format string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.audio = append([]byte(nil), audio...)
	p.format = format
	p.audioCalls++
	return nil
}

func (p *audioStubPlatform) audioSnapshot() ([]byte, string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.audio...), p.format, p.audioCalls
}

func TestSynthesizedTTSReply_PropagatesSpeed(t *testing.T) {
	tts := &recordingTTS{}
	p := &audioStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e := NewEngine("assistant", &stubAgent{}, []Platform{p}, "")
	e.SetTTSConfig(&TTSCfg{Enabled: true, Voice: "voice-b", Speed: 1.06, TTS: tts})

	if err := e.synthesizeAndSendTTS(p, "ctx", "hello"); err != nil {
		t.Fatalf("synthesizeAndSendTTS() error = %v", err)
	}
	_, opts, calls := tts.snapshot()
	if calls != 1 {
		t.Fatalf("tts calls = %d, want 1", calls)
	}
	if opts.Speed != 1.06 {
		t.Fatalf("speed = %v, want 1.06", opts.Speed)
	}
}

func TestSynthesizedTTSReply_ErrorWhenTTSDisabled(t *testing.T) {
	p := &audioStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e := NewEngine("assistant", &stubAgent{}, []Platform{p}, "")
	e.SetTTSConfig(&TTSCfg{Enabled: false, TTS: &recordingTTS{}})

	err := e.synthesizeAndSendTTS(p, "ctx", "hello")
	if err == nil || !strings.Contains(err.Error(), "tts is not configured") {
		t.Fatalf("error = %v, want tts is not configured", err)
	}
}

func TestSynthesizedTTSReply_ErrorWhenProviderMissing(t *testing.T) {
	p := &audioStubPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}
	e := NewEngine("assistant", &stubAgent{}, []Platform{p}, "")
	e.SetTTSConfig(&TTSCfg{Enabled: true})

	err := e.synthesizeAndSendTTS(p, "ctx", "hello")
	if err == nil || !strings.Contains(err.Error(), "tts provider is not configured") {
		t.Fatalf("error = %v, want tts provider is not configured", err)
	}
}

func TestSynthesizedTTSReply_ErrorWhenPlatformCannotSendAudio(t *testing.T) {
	tts := &recordingTTS{}
	p := &stubPlatformEngine{n: "discord"}
	e := NewEngine("assistant", &stubAgent{}, []Platform{p}, "")
	e.SetTTSConfig(&TTSCfg{Enabled: true, TTS: tts})

	err := e.synthesizeAndSendTTS(p, "ctx", "hello")
	if err == nil || !strings.Contains(err.Error(), "platform discord does not support audio sending") {
		t.Fatalf("error = %v, want unsupported audio sender error", err)
	}
	_, _, calls := tts.snapshot()
	if calls != 0 {
		t.Fatalf("tts calls = %d, want 0", calls)
	}
}

// ---------------------------------------------------------------------------
// Engine setter method coverage tests
// ---------------------------------------------------------------------------

func TestEngine_SetterMethods(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{p}, "")

	// Test SetSpeechConfig
	e.SetSpeechConfig(SpeechCfg{Enabled: true})

	// Test SetTTSConfig
	e.SetTTSConfig(&TTSCfg{Voice: "voice-1"})

	// Test SetTTSSaveFunc (just verify it doesn't panic)
	e.SetTTSSaveFunc(func(text string) error {
		return nil
	})

	// Test SetProviderSaveFunc
	e.SetProviderSaveFunc(func(providerName string) error {
		return nil
	})

	// Test SetProviderAddSaveFunc
	e.SetProviderAddSaveFunc(func(cfg ProviderConfig) error {
		return nil
	})

	// Test SetProviderRemoveSaveFunc
	e.SetProviderRemoveSaveFunc(func(name string) error {
		return nil
	})

	// Test SetCommandSaveAddFunc
	e.SetCommandSaveAddFunc(func(name, desc, prompt, exec, workDir string) error {
		return nil
	})

	// Test SetCommandSaveDelFunc
	e.SetCommandSaveDelFunc(func(name string) error {
		return nil
	})

	// Test SetDisplaySaveFunc
	e.SetDisplaySaveFunc(func(mode *string, thinkingMessages *bool, thinkMax, toolMax *int, toolMessages *bool) error {
		return nil
	})

	// Test SetConfigReloadFunc
	e.SetConfigReloadFunc(func() (*ConfigReloadResult, error) {
		return nil, nil
	})

	// Test SetAliasSaveAddFunc
	e.SetAliasSaveAddFunc(func(alias, cmd string) error {
		return nil
	})

	// Test SetAliasSaveDelFunc
	e.SetAliasSaveDelFunc(func(alias string) error {
		return nil
	})

	// Test SetStreamPreviewCfg
	e.SetStreamPreviewCfg(StreamPreviewCfg{Enabled: true})

	// Verify setters didn't break core functionality
	if e.GetAgent() == nil {
		t.Error("GetAgent should still work after setters")
	}
}

func TestEngine_SetUserRoles(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{p}, "")

	mgr := NewUserRoleManager()
	mgr.Configure("member", []RoleInput{
		{Name: "admin", UserIDs: []string{"admin1"}, DisabledCommands: []string{}},
		{Name: "member", UserIDs: []string{"*"}, DisabledCommands: []string{}},
	})

	e.SetUserRoles(mgr)

	// Verify the manager was stored
	e.userRolesMu.RLock()
	stored := e.userRoles
	e.userRolesMu.RUnlock()
	if stored == nil {
		t.Error("userRoles manager should be set")
	}
	if stored != mgr {
		t.Error("stored manager should be the same as configured manager")
	}
}

func TestEngine_SetStreamPreviewCfg(t *testing.T) {
	agent := &stubAgent{}
	p := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{p}, "")

	cfg := StreamPreviewCfg{Enabled: true, IntervalMs: 1000, MinDeltaChars: 10}
	e.SetStreamPreviewCfg(cfg)

	if e.streamPreview.Enabled != true {
		t.Error("streamPreview.Enabled should be true")
	}
	if e.streamPreview.IntervalMs != 1000 {
		t.Error("streamPreview.IntervalMs mismatch")
	}
}

func TestEngine_AddPlatform_Multiple(t *testing.T) {
	agent := &stubAgent{}
	p1 := &stubPlatformEngine{n: "feishu"}
	e := NewEngine("test", agent, []Platform{p1}, "")

	p2 := &stubPlatformEngine{n: "telegram"}
	p3 := &stubPlatformEngine{n: "discord"}

	e.AddPlatform(p2)
	e.AddPlatform(p3)

	if len(e.platforms) != 3 {
		t.Fatalf("expected 3 platforms, got %d", len(e.platforms))
	}
}

func TestSetObserveConfig(t *testing.T) {
	e := NewEngine("test", &stubAgent{}, nil, "")
	e.SetObserveConfig("/tmp/test-project", "slack:C123:U456")
	if !e.observeEnabled {
		t.Fatal("observe should be enabled")
	}
	if e.observeProjectDir != "/tmp/test-project" {
		t.Fatalf("unexpected project dir: %s", e.observeProjectDir)
	}
}

func TestObserveStartsOnlyWithSlack(t *testing.T) {
	stub := &stubPlatformWithObserve{stubPlatform: stubPlatform{n: "slack"}}
	e := NewEngine("test", &stubAgent{}, []Platform{stub}, "")
	e.SetObserveConfig("/tmp/fake-project", "slack:C123:U456")

	target := e.findObserverTarget()
	if target == nil {
		t.Fatal("expected to find observer target for Slack")
	}
}

func TestObserveNoTargetWithoutSlack(t *testing.T) {
	stub := &stubPlatform{n: "telegram"}
	e := NewEngine("test", &stubAgent{}, []Platform{stub}, "")
	e.SetObserveConfig("/tmp/fake-project", "slack:C123:U456")

	target := e.findObserverTarget()
	if target != nil {
		t.Fatal("expected no observer target without Slack")
	}
}

type stubPlatformWithObserve struct {
	stubPlatform
}

func (s *stubPlatformWithObserve) SendObservation(_ context.Context, _, _ string) error {
	return nil
}

// --- Instant Reply tests ---

// stubStreamingCardPlatform simulates a platform that supports StreamingCardPlatform
// (e.g. DingTalk with AI Card configured), so instant reply should be skipped.
type stubStreamingCardPlatform struct {
	stubPlatformEngine
	cardCreated bool
	cardFail    bool // when true, CreateStreamingCard returns an error
}

func (p *stubStreamingCardPlatform) CreateStreamingCard(_ context.Context, _ any) (StreamingCard, error) {
	if p.cardFail {
		return nil, fmt.Errorf("stub: card_template_id not configured")
	}
	p.cardCreated = true
	return &stubStreamingCard{}, nil
}

// stubStreamingCard is a minimal StreamingCard for tests.
type stubStreamingCard struct{}

func (c *stubStreamingCard) Update(_ context.Context, _ string) error   { return nil }
func (c *stubStreamingCard) Finalize(_ context.Context, _ string) error { return nil }
func (c *stubStreamingCard) Failed() bool                               { return false }

func TestHandleMessage_InstantReply_SendsConfirmationWhenEnabled(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetInstantReply(InstantReplyCfg{Enabled: true, Content: "🤔 Thinking..."})

	msg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	// Wait for async processing to complete
	deadline := time.After(2 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for replies, got: %v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	sent := p.getSent()
	if sent[0] != "🤔 Thinking..." {
		t.Fatalf("first reply = %q, want instant reply '🤔 Thinking...'", sent[0])
	}
}

func TestHandleMessage_InstantReply_UsesDefaultI18nWhenContentEmpty(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetInstantReply(InstantReplyCfg{Enabled: true}) // Content empty → use MsgStarting

	msg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	deadline := time.After(2 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for replies, got: %v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	sent := p.getSent()
	if sent[0] != "Processing..." {
		t.Fatalf("first reply = %q, want i18n default 'Processing...'", sent[0])
	}
}

func TestHandleMessage_InstantReply_SkippedWhenDisabled(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	// InstantReply not set (default: disabled)

	msg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	deadline := time.After(2 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for replies, got: %v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	sent := p.getSent()
	// The only reply should be the agent result, no instant reply
	if len(sent) != 1 {
		t.Fatalf("sent messages = %d, want exactly 1 (no instant reply), got: %v", len(sent), sent)
	}
	if sent[0] != "agent reply" {
		t.Fatalf("first reply = %q, want 'agent reply'", sent[0])
	}
}

func TestHandleMessage_InstantReply_SkippedForStreamingCardPlatform(t *testing.T) {
	p := &stubStreamingCardPlatform{stubPlatformEngine: stubPlatformEngine{n: "dingtalk"}}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetInstantReply(InstantReplyCfg{Enabled: true, Content: "🤔 Thinking..."})

	msg := &Message{
		SessionKey: "dingtalk:user1",
		Platform:   "dingtalk",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	// When streaming card succeeds, the agent reply goes through streamCard.Finalize,
	// not p.Send. Wait briefly then verify no instant reply was sent via p.Send.
	time.Sleep(500 * time.Millisecond)

	sent := p.getSent()
	for _, s := range sent {
		if s == "🤔 Thinking..." {
			t.Fatalf("instant reply should be skipped for StreamingCardPlatform, but got: %v", sent)
		}
	}
}

func TestHandleMessage_InstantReply_SentWhenStreamingCardFails(t *testing.T) {
	p := &stubStreamingCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "dingtalk"},
		cardFail:           true,
	}
	agentSession := newResultAgentSession("agent reply")
	agent := &resultAgent{session: agentSession}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetInstantReply(InstantReplyCfg{Enabled: true, Content: "🤔 Thinking..."})

	msg := &Message{
		SessionKey: "dingtalk:user1",
		Platform:   "dingtalk",
		UserID:     "u1",
		UserName:   "user",
		Content:    "hello",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	deadline := time.After(2 * time.Second)
	for {
		sent := p.getSent()
		if len(sent) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for replies, got: %v", p.getSent())
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	sent := p.getSent()
	if sent[0] != "🤔 Thinking..." {
		t.Fatalf("first reply = %q, want instant reply when card creation fails", sent[0])
	}
}

func TestHandleMessage_InstantReply_SkippedForSlashCommands(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetInstantReply(InstantReplyCfg{Enabled: true, Content: "🤔 Thinking..."})

	msg := &Message{
		SessionKey: "test:user1",
		Platform:   "test",
		UserID:     "u1",
		UserName:   "user",
		Content:    "/help",
		ReplyCtx:   "ctx",
	}
	e.handleMessage(p, msg)

	// Give a short time for any async processing
	time.Sleep(200 * time.Millisecond)

	sent := p.getSent()
	for _, s := range sent {
		if s == "🤔 Thinking..." {
			t.Fatalf("instant reply should be skipped for slash commands, but got: %v", sent)
		}
	}
}

// ===========================================================================
// Unsolicited events tests
// ===========================================================================

// waitForPlatformSend polls until the platform has at least n messages or timeout.
func waitForPlatformSend(p *stubPlatformEngine, n int, timeout time.Duration) []string {
	deadline := time.After(timeout)
	for {
		sent := p.getSent()
		if len(sent) >= n {
			return sent
		}
		select {
		case <-deadline:
			return sent
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestUnsolicitedReader_RelaysEventResult verifies that the unsolicited reader
// goroutine relays EventResult content to the platform.
func TestUnsolicitedReader_RelaysEventResult(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("unsol-relay")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:ch1:u1")

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
	}
	iKey := "test:ch1:u1"
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = state
	e.interactiveMu.Unlock()

	e.startUnsolicitedReader(state, session, sessions, iKey, "")
	defer e.stopUnsolicitedReader(state)

	// Send only EventResult (no EventText) to ensure the reader uses EventResult.Content.
	sess.events <- Event{Type: EventResult, Content: "All 5 campaigns created successfully", Done: true}

	sent := waitForPlatformSend(p, 1, 5*time.Second)
	if len(sent) == 0 {
		t.Fatal("expected unsolicited reader to relay EventResult to platform, got nothing")
	}
	found := false
	for _, s := range sent {
		if strings.Contains(s, "5 campaigns created successfully") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected relayed content to contain '5 campaigns created successfully', got %v", sent)
	}

	// Verify eventsNeedResync is false after clean EventResult.
	state.mu.Lock()
	resync := state.eventsNeedResync
	state.mu.Unlock()
	if resync {
		t.Error("expected eventsNeedResync=false after clean unsolicited EventResult")
	}
}

// TestUnsolicitedReader_ResetsIdleCloseOnEventResult verifies the P1-C P1-1
// fix: an unsolicited EventResult must re-arm the per-session idle close
// timer. Before the fix, the idle timer was only scheduled at the end of
// the last foreground turn, so a long-running background turn (background
// task that takes longer than the idle timeout) would be killed mid-flight
// when the original timer fired.
func TestUnsolicitedReader_ResetsIdleCloseOnEventResult(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("unsol-idle-reset")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer func() { _ = e.Stop() }()
	e.SetAgentSessionIdleTimeout(50 * time.Millisecond)

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:ch_idle:u1")

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
	}
	iKey := "test:ch_idle:u1"
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = state
	e.interactiveMu.Unlock()

	// Pretend a previous foreground turn already scheduled an idle close
	// with token N and a known cancel function. The cancel must be
	// invoked by the unsolicited reader's EventResult path as part of
	// re-scheduling.
	var prevCancelCalled atomic.Int32
	state.mu.Lock()
	state.agentSessionIdleCancel = func() { prevCancelCalled.Add(1) }
	state.agentSessionIdleToken = 42
	state.mu.Unlock()

	e.startUnsolicitedReader(state, session, sessions, iKey, "")
	defer e.stopUnsolicitedReader(state)

	// Send an EventResult — this is the trigger for re-scheduling.
	sess.events <- Event{Type: EventResult, Content: "background step done", Done: true}

	// Wait for the reader to drain the EventResult and reach the schedule
	// call. The reader sends the relayed content asynchronously; we poll
	// until either the cancel fires or a generous timeout elapses.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if prevCancelCalled.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if prevCancelCalled.Load() == 0 {
		t.Fatal("expected EventResult path to call cancelAgentSessionIdleClose before re-scheduling")
	}

	// After re-scheduling, token must have advanced and a fresh cancel
	// function must be installed. A stale token would risk the cleanup
	// path closing the live agent session while the background turn is
	// still running.
	state.mu.Lock()
	newToken := state.agentSessionIdleToken
	newCancel := state.agentSessionIdleCancel
	state.mu.Unlock()
	if newToken == 42 {
		t.Errorf("expected agentSessionIdleToken to advance after re-schedule, still %d", newToken)
	}
	if newCancel == nil {
		t.Fatal("expected a fresh agentSessionIdleCancel to be installed after EventResult")
	}

	// Cancel the freshly installed timer so the test does not leak the
	// background goroutine until idle timeout fires.
	state.mu.Lock()
	state.agentSessionIdleCancel = nil
	state.agentSessionIdleToken = 0
	state.mu.Unlock()
}

// TestUnsolicitedReader_StopsOnCancel verifies that stopUnsolicitedReader
// cleanly stops the reader goroutine and waits for it to exit.
func TestUnsolicitedReader_StopsOnCancel(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("unsol-cancel")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:ch1:u1")

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
	}

	e.startUnsolicitedReader(state, session, sessions, "test:ch1:u1", "")

	// Capture the done channel before stop nils it.
	state.mu.Lock()
	doneCh := state.unsolicitedDone
	state.mu.Unlock()

	if doneCh == nil {
		t.Fatal("expected unsolicitedDone to be set after startUnsolicitedReader")
	}

	start := time.Now()
	e.stopUnsolicitedReader(state)
	elapsed := time.Since(start)

	if elapsed > 3*time.Second {
		t.Errorf("stopUnsolicitedReader took too long: %v", elapsed)
	}

	// Verify the goroutine actually exited by checking the done channel.
	select {
	case <-doneCh:
		// Good — goroutine exited.
	default:
		t.Error("expected unsolicited reader goroutine to have exited (done channel not closed)")
	}
}

// TestUnsolicitedReader_SetsResyncOnChannelClose verifies that when the agent
// process exits (events channel closed), eventsNeedResync is set to true.
func TestUnsolicitedReader_SetsResyncOnChannelClose(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("unsol-close")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:close:u1")

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
	}

	e.startUnsolicitedReader(state, session, sessions, "test:close:u1", "")

	state.mu.Lock()
	doneCh := state.unsolicitedDone
	state.mu.Unlock()

	// Close the events channel (simulates agent process exit).
	close(sess.events)

	// Wait for reader to detect the close.
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("unsolicited reader did not exit after channel close")
	}

	state.mu.Lock()
	resync := state.eventsNeedResync
	state.mu.Unlock()
	if !resync {
		t.Error("expected eventsNeedResync=true after channel close")
	}
}

// TestUnsolicitedReader_SetsResyncOnEventError verifies that EventError
// sets eventsNeedResync to true and relays the error.
func TestUnsolicitedReader_SetsResyncOnEventError(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("unsol-error")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:error:u1")

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
	}

	e.startUnsolicitedReader(state, session, sessions, "test:error:u1", "")

	state.mu.Lock()
	doneCh := state.unsolicitedDone
	state.mu.Unlock()

	// Send an error event.
	sess.events <- Event{Type: EventError, Error: errors.New("something broke")}

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("unsolicited reader did not exit after EventError")
	}

	state.mu.Lock()
	resync := state.eventsNeedResync
	state.mu.Unlock()
	if !resync {
		t.Error("expected eventsNeedResync=true after EventError")
	}

	// Verify error was relayed to platform.
	sent := p.getSent()
	found := false
	for _, s := range sent {
		if strings.Contains(s, "something broke") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected error to be relayed to platform, got %v", sent)
	}
}

// TestUnsolicitedReader_PermissionDeny verifies that unsolicited permission
// requests are denied when approveAll is false.
func TestUnsolicitedReader_PermissionDeny(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	// NOTE: constructed inline rather than copying a *controllableAgentSession,
	// because that struct now carries an atomic.Bool (closeFinished) and must
	// not be copied by value.
	permRecorder := &permRecordingSession{
		controllableAgentSession: controllableAgentSession{
			sessionID: "unsol-perm",
			alive:     true,
			events:    make(chan Event, 8),
			closed:    make(chan struct{}),
		},
	}

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:perm:u1")

	state := &interactiveState{
		agentSession:     permRecorder,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
		approveAll:       false,
	}

	e.startUnsolicitedReader(state, session, sessions, "test:perm:u1", "")

	// Send a permission request.
	permRecorder.events <- Event{
		Type:      EventPermissionRequest,
		RequestID: "req-1",
		ToolName:  "Bash",
	}

	// Wait for the response.
	deadline := time.After(5 * time.Second)
	for {
		permRecorder.mu.Lock()
		calls := permRecorder.permCalls
		permRecorder.mu.Unlock()
		if calls > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for permission response")
		case <-time.After(10 * time.Millisecond):
		}
	}

	permRecorder.mu.Lock()
	result := permRecorder.lastPermResult
	permRecorder.mu.Unlock()

	if result.Behavior != "deny" {
		t.Errorf("expected deny, got %q", result.Behavior)
	}

	e.stopUnsolicitedReader(state)
}

// permRecordingSession wraps controllableAgentSession and records permission responses.
type permRecordingSession struct {
	controllableAgentSession
	mu             sync.Mutex
	permCalls      int
	lastPermResult PermissionResult
}

func (s *permRecordingSession) RespondPermission(_ string, res PermissionResult) error {
	s.mu.Lock()
	s.permCalls++
	s.lastPermResult = res
	s.mu.Unlock()
	return nil
}

// TestEventsNeedResync_DefaultTrue verifies that new interactiveState
// constructors set eventsNeedResync to true.
func TestEventsNeedResync_DefaultTrue(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	e.ensureInteractiveStateForQueueing("key1", p, "ctx")

	e.interactiveMu.Lock()
	state := e.interactiveStates["key1"]
	e.interactiveMu.Unlock()

	if state == nil {
		t.Fatal("expected state to be created")
	}
	if !state.eventsNeedResync {
		t.Error("expected eventsNeedResync to be true for new state")
	}
}

// TestEventsNeedResync_ClearedOnCleanResult verifies that eventsNeedResync
// is cleared after a clean EventResult in processInteractiveEvents.
func TestEventsNeedResync_ClearedOnCleanResult(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("resync-clean")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:resync:u1")
	session.TryLock()

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: true,
	}

	// Send EventResult to trigger clean exit.
	go func() {
		sess.events <- Event{Type: EventResult, Content: "done", Done: true}
	}()

	sendDone := make(chan error, 1)
	sendDone <- nil
	e.processInteractiveEvents(state, session, sessions, "test:resync:u1", "", time.Now(), nil, sendDone, "ctx")

	state.mu.Lock()
	resync := state.eventsNeedResync
	state.mu.Unlock()

	if resync {
		t.Error("expected eventsNeedResync to be false after clean EventResult")
	}
}

// TestCleanupInteractiveState_StopsUnsolicitedReader verifies that cleanup
// stops the unsolicited reader goroutine and waits for it to exit.
func TestCleanupInteractiveState_StopsUnsolicitedReader(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	sess := newControllableSession("cleanup-unsol")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	defer e.Stop()

	sessions := e.sessions
	session := sessions.GetOrCreateActive("test:cleanup:u1")

	state := &interactiveState{
		agentSession:     sess,
		platform:         p,
		replyCtx:         "ctx",
		eventsNeedResync: false,
	}
	iKey := "test:cleanup:u1"
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = state
	e.interactiveMu.Unlock()

	e.startUnsolicitedReader(state, session, sessions, iKey, "")

	// Capture the done channel before cleanup nils it.
	state.mu.Lock()
	doneCh := state.unsolicitedDone
	state.mu.Unlock()

	// Cleanup should stop the reader and close the session.
	e.cleanupInteractiveState(iKey)

	// Verify the goroutine actually exited.
	select {
	case <-doneCh:
		// Good.
	case <-time.After(5 * time.Second):
		t.Fatal("unsolicited reader goroutine did not exit after cleanup")
	}
}

// TestReapIdle_DisabledWhenZeroTimeout verifies that ReapIdle returns nil
// when idleTimeout is zero.
func TestReapIdle_DisabledWhenZeroTimeout(t *testing.T) {
	pool := newWorkspacePool(0)
	ws := pool.GetOrCreate("/test/workspace")
	ws.Touch()
	// Even with an existing workspace, zero timeout disables reaping.
	reaped := pool.ReapIdle()
	if len(reaped) != 0 {
		t.Errorf("expected no reaping with zero timeout, got %v", reaped)
	}
}

func TestIsSilentReply(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"exact marker", "NO_REPLY", true},
		{"lowercase", "no_reply", true},
		{"mixed case", "No_Reply", true},
		{"leading/trailing spaces", "  NO_REPLY  ", true},
		{"surrounding newlines", "\nNO_REPLY\n", true},
		{"tabs around", "\tNO_REPLY\t", true},

		{"empty", "", false},
		{"whitespace only", "   ", false},
		{"mixed with content", "Hello NO_REPLY", false},
		{"marker with suffix", "NO_REPLY_EXTRA", false},
		{"marker with prefix", "X NO_REPLY", false},
		{"missing underscore", "NO REPLY", false},
		{"partial", "NO_REPL", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSilentReply(tc.in); got != tc.want {
				t.Errorf("isSilentReply(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestStripTrailingSilent(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		want   string
		wantOK bool
	}{
		{"trailing on new line", "Hello\nNO_REPLY", "Hello", true},
		{"trailing lowercase on new line", "Hello\nno_reply", "Hello", true},
		{"trailing after space", "Some reasoning here NO_REPLY", "Some reasoning here", true},
		{"multi-line then marker", "Line1\nLine2\nNO_REPLY", "Line1\nLine2", true},
		{"trailing with markdown emphasis", "Done. *NO_REPLY*", "Done. *NO_REPLY*", false},
		{"trailing preceded by asterisks", "Done.**NO_REPLY", "Done.", true},
		{"trailing with crlf", "Hello\r\nNO_REPLY", "Hello", true},
		{"marker followed by trailing whitespace", "Hello NO_REPLY   ", "Hello", true},

		{"no marker", "Hello world", "Hello world", false},
		{"marker not at end", "NO_REPLY then more", "NO_REPLY then more", false},
		{"marker with suffix token", "Hello NO_REPLY_EXTRA", "Hello NO_REPLY_EXTRA", false},
		{"marker touching prior letters", "somethingNO_REPLY", "somethingNO_REPLY", false},
		{"empty", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := stripTrailingSilent(tc.in)
			if ok != tc.wantOK {
				t.Errorf("stripTrailingSilent(%q) ok=%v, want %v", tc.in, ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("stripTrailingSilent(%q) got=%q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestCouldBeSilentPrefix(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"empty", "", true},
		{"whitespace only", "   ", true},
		{"one letter", "N", true},
		{"two letters", "NO", true},
		{"underscore partial", "NO_", true},
		{"five letters", "NO_RE", true},
		{"almost full", "NO_REPL", true},
		{"full marker", "NO_REPLY", true},
		{"lowercase partial", "no_r", true},
		{"mixed case partial", "No_Re", true},
		{"trimmed surrounding whitespace", "  NO_  ", true},

		{"non-N start", "Hello", false},
		{"one wrong letter", "X", false},
		{"longer than marker", "NO_REPLYX", false},
		{"similar but divergent", "NO-REPLY", false},
		{"partial then wrong", "NO_Q", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := couldBeSilentPrefix(tc.in); got != tc.want {
				t.Errorf("couldBeSilentPrefix(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Integration tests for /list visibility after /new and provider switches
// ---------------------------------------------------------------------------

// TestCmdList_AllSessionsVisibleAfterRepeatedNew verifies that /list shows ALL
// sessions after multiple /new cycles. This is the exact reproduction scenario
// reported by users: /new clears the active session's AgentSessionID, causing
// filterOwnedSessions to progressively hide older sessions.
func TestCmdList_AllSessionsVisibleAfterRepeatedNew(t *testing.T) {
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	agentSessions := make([]AgentSessionInfo, 5)
	for i := range agentSessions {
		agentSessions[i] = AgentSessionInfo{
			ID:           fmt.Sprintf("codex-thread-%d", i+1),
			Summary:      fmt.Sprintf("Session %d summary", i+1),
			MessageCount: (i + 1) * 2,
			ModifiedAt:   base.Add(time.Duration(i) * time.Hour),
		}
	}

	agent := &stubListAgent{sessions: agentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	for i, as := range agentSessions {
		if i > 0 {
			old := e.sessions.GetOrCreateActive(userKey)
			old.SetAgentSessionID("", "")
			old.ClearHistory()
			e.sessions.Save()
			e.sessions.NewSession(userKey, fmt.Sprintf("session-%d", i+1))
		}
		s := e.sessions.GetOrCreateActive(userKey)
		s.SetAgentSessionID(as.ID, "codex")
		e.sessions.Save()
	}

	p.sent = nil
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}
	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	for _, as := range agentSessions {
		if !strings.Contains(p.sent[0], as.Summary) {
			t.Errorf("/list output missing session %q:\n%s", as.ID, p.sent[0])
		}
	}
}

// TestCmdList_AllSessionsVisibleAfterResetAllSessions simulates a management
// API provider switch (resetAllSessions) followed by creating a new session.
// All previously tracked sessions must remain visible in /list.
func TestCmdList_AllSessionsVisibleAfterResetAllSessions(t *testing.T) {
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	agentSessions := make([]AgentSessionInfo, 4)
	for i := range agentSessions {
		agentSessions[i] = AgentSessionInfo{
			ID:           fmt.Sprintf("thread-%d", i+1),
			Summary:      fmt.Sprintf("Chat %d", i+1),
			MessageCount: 5,
			ModifiedAt:   base.Add(time.Duration(i) * time.Hour),
		}
	}

	agent := &stubListAgent{sessions: agentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	for _, as := range agentSessions[:3] {
		s := e.sessions.NewSession(userKey, "")
		s.SetAgentSessionID(as.ID, "codex")
	}
	e.sessions.Save()

	e.resetAllSessions()

	newS := e.sessions.NewSession(userKey, "fresh")
	newS.SetAgentSessionID(agentSessions[3].ID, "codex")
	e.sessions.Save()

	p.sent = nil
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}
	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	for _, as := range agentSessions {
		if !strings.Contains(p.sent[0], as.Summary) {
			t.Errorf("/list output missing session %q after resetAllSessions:\n%s", as.ID, p.sent[0])
		}
	}
}

// TestCmdList_SessionVisibleDuringAgentProcessing simulates the window where
// a new session has been created (/new) and a message sent, but the agent
// has not yet responded with a session ID. During this window, the active
// session has no AgentSessionID. Previously this caused filterOwnedSessions
// to either return all sessions (empty known set) or hide sessions (if other
// sessions also had cleared IDs). The fix ensures deterministic behavior.
func TestCmdList_SessionVisibleDuringAgentProcessing(t *testing.T) {
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	agentSessions := []AgentSessionInfo{
		{ID: "old-thread-1", Summary: "Old session 1", MessageCount: 10, ModifiedAt: base},
		{ID: "old-thread-2", Summary: "Old session 2", MessageCount: 8, ModifiedAt: base.Add(time.Hour)},
		{ID: "new-thread-3", Summary: "Processing...", MessageCount: 1, ModifiedAt: base.Add(2 * time.Hour)},
	}

	agent := &stubListAgent{sessions: agentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	s1 := e.sessions.GetOrCreateActive(userKey)
	s1.SetAgentSessionID("old-thread-1", "codex")
	e.sessions.Save()

	s1.SetAgentSessionID("", "")
	s2 := e.sessions.NewSession(userKey, "session-2")
	s2.SetAgentSessionID("old-thread-2", "codex")
	e.sessions.Save()

	s2.SetAgentSessionID("", "")
	e.sessions.NewSession(userKey, "processing")
	e.sessions.Save()

	p.sent = nil
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}
	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	reply := p.sent[0]
	if !strings.Contains(reply, "Old session 1") {
		t.Errorf("/list missing 'Old session 1' during processing:\n%s", reply)
	}
	if !strings.Contains(reply, "Old session 2") {
		t.Errorf("/list missing 'Old session 2' during processing:\n%s", reply)
	}
}

// TestRenderListCard_AllSessionsVisibleAfterRepeatedNew is the card-based
// variant of the /new regression test.
func TestRenderListCard_AllSessionsVisibleAfterRepeatedNew(t *testing.T) {
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	agentSessions := make([]AgentSessionInfo, 6)
	for i := range agentSessions {
		agentSessions[i] = AgentSessionInfo{
			ID:           fmt.Sprintf("thread-%d", i+1),
			Summary:      fmt.Sprintf("Session %d", i+1),
			MessageCount: 3,
			ModifiedAt:   base.Add(time.Duration(i) * time.Minute),
		}
	}

	agent := &stubListAgent{sessions: agentSessions}
	e := NewEngine("test", agent, []Platform{&stubPlatformEngine{n: "test"}}, "")
	userKey := "test:user1"

	for i, as := range agentSessions {
		if i > 0 {
			old := e.sessions.GetOrCreateActive(userKey)
			old.SetAgentSessionID("", "")
			old.ClearHistory()
			e.sessions.NewSession(userKey, fmt.Sprintf("s%d", i+1))
		}
		s := e.sessions.GetOrCreateActive(userKey)
		s.SetAgentSessionID(as.ID, "codex")
	}
	e.sessions.Save()

	card, err := e.renderListCard(userKey, 1)
	if err != nil {
		t.Fatalf("renderListCard error: %v", err)
	}

	switchActions := countCardActionValues(card, "act:/switch ")
	if switchActions != len(agentSessions) {
		t.Fatalf("card switch actions = %d, want %d (some sessions hidden by filter)",
			switchActions, len(agentSessions))
	}
}

// TestCmdList_ProviderSwitchThenNewDoesNotHideSessions simulates the full
// real-world scenario: user has sessions → switches provider → creates new
// sessions → all sessions (old and new) must remain visible.
func TestCmdList_ProviderSwitchThenNewDoesNotHideSessions(t *testing.T) {
	base := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	allAgentSessions := []AgentSessionInfo{
		{ID: "old-1", Summary: "Before switch 1", MessageCount: 5, ModifiedAt: base},
		{ID: "old-2", Summary: "Before switch 2", MessageCount: 3, ModifiedAt: base.Add(time.Hour)},
		{ID: "new-1", Summary: "After switch 1", MessageCount: 2, ModifiedAt: base.Add(2 * time.Hour)},
		{ID: "new-2", Summary: "After switch 2", MessageCount: 1, ModifiedAt: base.Add(3 * time.Hour)},
	}
	agent := &stubListAgent{sessions: allAgentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	for _, as := range allAgentSessions[:2] {
		s := e.sessions.NewSession(userKey, "")
		s.SetAgentSessionID(as.ID, "codex")
	}
	e.sessions.Save()

	e.resetAllSessions()

	for i, as := range allAgentSessions[2:] {
		if i > 0 {
			old := e.sessions.GetOrCreateActive(userKey)
			old.SetAgentSessionID("", "")
		}
		s := e.sessions.NewSession(userKey, "")
		s.SetAgentSessionID(as.ID, "codex")
	}
	e.sessions.Save()

	p.sent = nil
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}
	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	for _, as := range allAgentSessions {
		if !strings.Contains(p.sent[0], as.Summary) {
			t.Errorf("/list missing %q after provider switch + new:\n%s", as.Summary, p.sent[0])
		}
	}
}

// TestCmdList_RealWorldLegacyDataFullFlow is a precise reproduction of the
// user-reported bug using data shaped exactly like the real qa-release project:
//   - 15 internal sessions, 14 with lost AgentSessionIDs (old code damage)
//   - 1 active session (s15) with a valid AgentSessionID
//   - 37 codex sessions on disk
//
// Steps (matching user's exact reproduction):
//  1. /list → must show all 37 sessions (legacy data, no filtering)
//  2. /new "我的新会话" → create named session
//  3. send message (agent hasn't replied yet) → /list → must STILL show all sessions
//  4. agent replies with SessionID → /list → must show all sessions + new one
//  5. session name "我的新会话" must appear in the list
func TestCmdList_RealWorldLegacyDataFullFlow(t *testing.T) {
	dir := t.TempDir()
	sessPath := filepath.Join(dir, "sessions.json")

	// Write legacy session data (no past_id_tracking, simulates pre-fix data)
	legacyJSON := `{
		"sessions": {
			"s1":  {"id":"s1", "name":"default",    "agent_session_id":"", "history":null, "created_at":"2026-03-26T22:25:56Z", "updated_at":"2026-03-26T22:25:56Z"},
			"s2":  {"id":"s2", "name":"default",    "agent_session_id":"", "history":null, "created_at":"2026-04-18T09:02:57Z", "updated_at":"2026-04-18T09:02:57Z"},
			"s3":  {"id":"s3", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T09:03:07Z", "updated_at":"2026-04-18T09:03:07Z"},
			"s4":  {"id":"s4", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T09:07:15Z", "updated_at":"2026-04-18T09:07:15Z"},
			"s5":  {"id":"s5", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T11:14:14Z", "updated_at":"2026-04-18T11:14:14Z"},
			"s6":  {"id":"s6", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T11:39:15Z", "updated_at":"2026-04-18T11:39:15Z"},
			"s7":  {"id":"s7", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T11:42:27Z", "updated_at":"2026-04-18T11:42:27Z"},
			"s8":  {"id":"s8", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T12:01:02Z", "updated_at":"2026-04-18T12:01:22Z"},
			"s9":  {"id":"s9", "name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T12:06:31Z", "updated_at":"2026-04-18T12:08:37Z"},
			"s10": {"id":"s10","name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T12:18:55Z", "updated_at":"2026-04-18T12:18:55Z"},
			"s11": {"id":"s11","name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T14:07:03Z", "updated_at":"2026-04-18T14:07:47Z"},
			"s12": {"id":"s12","name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T14:07:59Z", "updated_at":"2026-04-18T14:18:49Z"},
			"s13": {"id":"s13","name":"",           "agent_session_id":"", "history":null, "created_at":"2026-04-18T15:50:39Z", "updated_at":"2026-04-20T21:44:37Z"},
			"s14": {"id":"s14","name":"今天",       "agent_session_id":"", "history":null, "created_at":"2026-04-20T21:44:58Z", "updated_at":"2026-04-20T21:44:58Z"},
			"s15": {"id":"s15","name":"新的会话",   "agent_session_id":"019dab28-1a0f-7f60-87ed-b4fda306ebef", "agent_type":"codex", "history":null, "created_at":"2026-04-20T21:50:14Z", "updated_at":"2026-04-20T21:50:14Z"}
		},
		"active_session": {"feishu:chat:user1":"s15"},
		"user_sessions":  {"feishu:chat:user1":["s2","s3","s4","s5","s6","s7","s8","s9","s10","s11","s12","s13","s14","s15"]},
		"counter": 15
	}`
	if err := os.WriteFile(sessPath, []byte(legacyJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	base := time.Date(2026, 4, 18, 9, 0, 0, 0, time.UTC)
	agentSessions := make([]AgentSessionInfo, 37)
	for i := range agentSessions {
		agentSessions[i] = AgentSessionInfo{
			ID:           fmt.Sprintf("codex-thread-%03d", i+1),
			Summary:      fmt.Sprintf("Codex session %d", i+1),
			MessageCount: 3,
			ModifiedAt:   base.Add(time.Duration(i) * 30 * time.Minute),
		}
	}
	// s15's actual codex session is at index 36 (most recent)
	agentSessions[36].ID = "019dab28-1a0f-7f60-87ed-b4fda306ebef"
	agentSessions[36].Summary = "陈奕迅最有名是那首歌"

	agent := &stubListAgent{sessions: agentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.sessions = NewSessionManager(sessPath) // load real data
	userKey := "feishu:chat:user1"
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}

	// ── Step 1: /list on startup ───────────────────────────────
	p.sent = nil
	e.cmdList(p, msg, nil)
	if len(p.sent) != 1 {
		t.Fatalf("step1: expected 1 reply, got %d", len(p.sent))
	}
	step1Count := strings.Count(p.sent[0], "msgs")
	if step1Count != 20 {
		t.Fatalf("step1: /list should show first page (20 sessions), got %d", step1Count)
	}

	// ── Step 2: /new "我的新会话" ──────────────────────────────
	e.cmdNew(p, msg, []string{"我的新会话"})

	// ── Step 3: send message, agent not yet replied → /list ────
	// (agent process started but hasn't returned SessionID yet)
	p.sent = nil
	e.cmdList(p, msg, nil)
	if len(p.sent) != 1 {
		t.Fatalf("step3: expected 1 reply, got %d", len(p.sent))
	}
	step3Count := strings.Count(p.sent[0], "msgs")
	if step3Count < 20 {
		t.Fatalf("step3: /list BEFORE agent reply should still show all sessions (page 1 = 20), got %d\nreply:\n%s",
			step3Count, p.sent[0])
	}

	// ── Step 4: agent replies → set SessionID → /list ──────────
	newSession := e.sessions.GetOrCreateActive(userKey)
	newThreadID := "codex-thread-new-038"
	newSession.CompareAndSetAgentSessionID(newThreadID, "codex")
	// Engine maps the pending name to the new agent session ID
	pendingName := newSession.GetName()
	if pendingName != "" && pendingName != "session" && pendingName != "default" {
		e.sessions.SetSessionName(newThreadID, pendingName)
	}
	e.sessions.Save()

	// Agent now reports this new session in ListSessions
	agent.sessions = append(agent.sessions, AgentSessionInfo{
		ID:           newThreadID,
		Summary:      "我的新消息内容",
		MessageCount: 2,
		ModifiedAt:   time.Now(),
	})

	p.sent = nil
	e.cmdList(p, msg, nil)
	if len(p.sent) != 1 {
		t.Fatalf("step4: expected 1 reply, got %d", len(p.sent))
	}
	step4Count := strings.Count(p.sent[0], "msgs")
	if step4Count < 20 {
		t.Fatalf("step4: /list AFTER agent reply should show all sessions (page 1 = 20), got %d\nreply:\n%s",
			step4Count, p.sent[0])
	}

	// ── Step 5: verify session name on page 2 ─────────────────
	// The newest session is at the end of the list; check page 2.
	p.sent = nil
	e.cmdList(p, msg, []string{"2"})
	if len(p.sent) != 1 {
		t.Fatalf("step5: expected 1 reply for page 2, got %d", len(p.sent))
	}
	// The new session should show "我的新会话" (the name from /new), not the message content
	if !strings.Contains(p.sent[0], "我的新会话") {
		t.Errorf("step5: /list page 2 should display session name '我的新会话' but it's missing:\n%s", p.sent[0])
	}
}

// TestCmdList_FilterExternalSessionsEnabled verifies that when
// filter_external_sessions is enabled, only agent-bridge-tracked sessions
// appear in /list.
func TestCmdList_FilterExternalSessionsEnabled(t *testing.T) {
	agentSessions := []AgentSessionInfo{
		{ID: "tracked-1", Summary: "Tracked 1", MessageCount: 5},
		{ID: "tracked-2", Summary: "Tracked 2", MessageCount: 3},
		{ID: "external-1", Summary: "External CLI session", MessageCount: 10},
	}

	agent := &stubListAgent{sessions: agentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetFilterExternalSessions(true)
	userKey := "test:user1"

	s1 := e.sessions.GetOrCreateActive(userKey)
	s1.SetAgentSessionID("tracked-1", "codex")
	e.sessions.Save()
	s1.SetAgentSessionID("", "")
	s2 := e.sessions.NewSession(userKey, "session2")
	s2.SetAgentSessionID("tracked-2", "codex")
	e.sessions.Save()

	p.sent = nil
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}
	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	reply := p.sent[0]
	if !strings.Contains(reply, "Tracked 1") {
		t.Errorf("filter enabled: should show tracked session 'Tracked 1':\n%s", reply)
	}
	if !strings.Contains(reply, "Tracked 2") {
		t.Errorf("filter enabled: should show tracked session 'Tracked 2':\n%s", reply)
	}
	if strings.Contains(reply, "External CLI session") {
		t.Errorf("filter enabled: should NOT show external session:\n%s", reply)
	}
}

// TestCmdList_DefaultShowsAllSessions verifies that with default config
// (filter_external_sessions=false), all sessions including external ones appear.
func TestCmdList_DefaultShowsAllSessions(t *testing.T) {
	agentSessions := []AgentSessionInfo{
		{ID: "tracked-1", Summary: "Tracked session", MessageCount: 5},
		{ID: "external-1", Summary: "External session", MessageCount: 10},
	}

	agent := &stubListAgent{sessions: agentSessions}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	s := e.sessions.GetOrCreateActive(userKey)
	s.SetAgentSessionID("tracked-1", "codex")
	e.sessions.Save()

	p.sent = nil
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}
	e.cmdList(p, msg, nil)

	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	reply := p.sent[0]
	if !strings.Contains(reply, "Tracked session") {
		t.Errorf("default mode: should show tracked session:\n%s", reply)
	}
	if !strings.Contains(reply, "External session") {
		t.Errorf("default mode: should show external session:\n%s", reply)
	}
}

// ---------------------------------------------------------------------------
// filter_external_sessions integration test suite
// Covers /list, /switch, /delete, renderListCard under both modes.
// ---------------------------------------------------------------------------

// setupFilterTestEngine creates a test Engine with 3 agent sessions, 2 tracked
// by agent-bridge and 1 external. Returns (engine, platform, userKey, agentSessions).
func setupFilterTestEngine(t *testing.T, filterEnabled bool) (*Engine, *stubPlatformEngine, string, []AgentSessionInfo) {
	t.Helper()
	agentSessions := []AgentSessionInfo{
		{ID: "tracked-1", Summary: "Tracked session 1", MessageCount: 5, ModifiedAt: time.Now().Add(-2 * time.Hour)},
		{ID: "tracked-2", Summary: "Tracked session 2", MessageCount: 3, ModifiedAt: time.Now().Add(-time.Hour)},
		{ID: "external-1", Summary: "External CLI session", MessageCount: 10, ModifiedAt: time.Now()},
	}
	agent := &stubDeleteAgent{
		stubListAgent: stubListAgent{sessions: agentSessions},
		errByID:       map[string]error{},
	}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	e.SetFilterExternalSessions(filterEnabled)
	userKey := "test:filter-user"

	s1 := e.sessions.GetOrCreateActive(userKey)
	s1.SetAgentSessionID("tracked-1", "codex")
	e.sessions.Save()
	s1.SetAgentSessionID("", "")
	s2 := e.sessions.NewSession(userKey, "session2")
	s2.SetAgentSessionID("tracked-2", "codex")
	e.sessions.Save()

	return e, p, userKey, agentSessions
}

func TestFilterExternalSessions_SwitchByIndex(t *testing.T) {
	t.Run("disabled: index 3 reaches external session", func(t *testing.T) {
		e, p, userKey, _ := setupFilterTestEngine(t, false)
		p.sent = nil
		e.cmdSwitch(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"3"})
		if len(p.sent) != 1 {
			t.Fatalf("expected 1 reply, got %d", len(p.sent))
		}
		if !strings.Contains(p.sent[0], "External CLI session") {
			t.Errorf("default mode: /switch 3 should reach external session:\n%s", p.sent[0])
		}
	})

	t.Run("enabled: index 3 out of range", func(t *testing.T) {
		e, p, userKey, _ := setupFilterTestEngine(t, true)
		p.sent = nil
		e.cmdSwitch(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"3"})
		if len(p.sent) != 1 {
			t.Fatalf("expected 1 reply, got %d", len(p.sent))
		}
		if strings.Contains(p.sent[0], "External CLI session") {
			t.Errorf("filter enabled: /switch 3 should NOT reach external session:\n%s", p.sent[0])
		}
	})
}

func TestFilterExternalSessions_SwitchByIDPrefix(t *testing.T) {
	t.Run("disabled: can switch to external by ID prefix", func(t *testing.T) {
		e, p, userKey, _ := setupFilterTestEngine(t, false)
		p.sent = nil
		e.cmdSwitch(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"external"})
		if len(p.sent) != 1 {
			t.Fatalf("expected 1 reply, got %d", len(p.sent))
		}
		if !strings.Contains(p.sent[0], "External CLI session") {
			t.Errorf("default mode: /switch external should find external session:\n%s", p.sent[0])
		}
	})

	t.Run("enabled: external ID prefix not found", func(t *testing.T) {
		e, p, userKey, _ := setupFilterTestEngine(t, true)
		p.sent = nil
		e.cmdSwitch(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"external"})
		if len(p.sent) != 1 {
			t.Fatalf("expected 1 reply, got %d", len(p.sent))
		}
		if strings.Contains(p.sent[0], "External CLI session") {
			t.Errorf("filter enabled: /switch external should NOT find external session:\n%s", p.sent[0])
		}
	})
}

func TestFilterExternalSessions_DeleteByIndex(t *testing.T) {
	t.Run("disabled: /delete 3 hits external session", func(t *testing.T) {
		e, p, userKey, _ := setupFilterTestEngine(t, false)
		p.sent = nil
		e.cmdDelete(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"3"})
		if len(p.sent) == 0 {
			t.Fatal("expected reply from /delete")
		}
		reply := strings.Join(p.sent, "\n")
		if !strings.Contains(reply, "external-1") && !strings.Contains(reply, "External CLI session") {
			t.Errorf("default mode: /delete 3 should target external session:\n%s", reply)
		}
	})

	t.Run("enabled: /delete 3 out of range", func(t *testing.T) {
		e, p, userKey, _ := setupFilterTestEngine(t, true)
		p.sent = nil
		e.cmdDelete(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"3"})
		if len(p.sent) == 0 {
			t.Fatal("expected reply from /delete")
		}
		reply := strings.Join(p.sent, "\n")
		if strings.Contains(reply, "external-1") || strings.Contains(reply, "External CLI session") {
			t.Errorf("filter enabled: /delete 3 should NOT target external session:\n%s", reply)
		}
	})
}

func TestFilterExternalSessions_RenderListCard(t *testing.T) {
	t.Run("disabled: card shows all sessions", func(t *testing.T) {
		e, _, userKey, agentSessions := setupFilterTestEngine(t, false)
		card, err := e.renderListCard(userKey, 1)
		if err != nil {
			t.Fatalf("renderListCard: %v", err)
		}
		switchActions := countCardActionValues(card, "act:/switch ")
		if switchActions != len(agentSessions) {
			t.Errorf("default mode: card should show %d sessions, got %d", len(agentSessions), switchActions)
		}
	})

	t.Run("enabled: card hides external sessions", func(t *testing.T) {
		e, _, userKey, _ := setupFilterTestEngine(t, true)
		card, err := e.renderListCard(userKey, 1)
		if err != nil {
			t.Fatalf("renderListCard: %v", err)
		}
		switchActions := countCardActionValues(card, "act:/switch ")
		if switchActions != 2 {
			t.Errorf("filter enabled: card should show 2 tracked sessions, got %d", switchActions)
		}
	})
}

func TestFilterExternalSessions_DynamicToggle(t *testing.T) {
	e, p, userKey, agentSessions := setupFilterTestEngine(t, false)
	msg := &Message{SessionKey: userKey, ReplyCtx: "ctx"}

	p.sent = nil
	e.cmdList(p, msg, nil)
	count1 := strings.Count(p.sent[0], "msgs")
	if count1 != len(agentSessions) {
		t.Fatalf("before toggle: expected %d sessions, got %d", len(agentSessions), count1)
	}

	e.SetFilterExternalSessions(true)

	p.sent = nil
	e.cmdList(p, msg, nil)
	count2 := strings.Count(p.sent[0], "msgs")
	if count2 != 2 {
		t.Fatalf("after enabling filter: expected 2 sessions, got %d\nreply:\n%s", count2, p.sent[0])
	}

	e.SetFilterExternalSessions(false)

	p.sent = nil
	e.cmdList(p, msg, nil)
	count3 := strings.Count(p.sent[0], "msgs")
	if count3 != len(agentSessions) {
		t.Fatalf("after disabling filter: expected %d sessions, got %d", len(agentSessions), count3)
	}
}

// codexLikeSession simulates real codex agent behavior:
// - CurrentSessionID() returns "" until Send() is called
// - Send() sets the thread ID and pushes an EventResult with the SessionID
type codexLikeSession struct {
	threadID  string
	events    chan Event
	alive     bool
	hasSentID bool
}

func newCodexLikeSession(threadID string) *codexLikeSession {
	return &codexLikeSession{
		threadID: threadID,
		events:   make(chan Event, 8),
		alive:    true,
	}
}

func (s *codexLikeSession) Send(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.hasSentID = true
	s.events <- Event{Type: EventText, Content: "Agent reply to: " + prompt}
	s.events <- Event{Type: EventResult, SessionID: s.threadID, Content: "Done", Done: true}
	return nil
}
func (s *codexLikeSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *codexLikeSession) Events() <-chan Event                                 { return s.events }
func (s *codexLikeSession) CurrentSessionID() string {
	if s.hasSentID {
		return s.threadID
	}
	return ""
}
func (s *codexLikeSession) Alive() bool  { return s.alive }
func (s *codexLikeSession) Close() error { s.alive = false; return nil }

// TestSessionName_CodexLikeFlow does an end-to-end test simulating real codex
// behavior: CurrentSessionID()="" initially, thread ID only available after Send().
// This is the exact bug: /new xxx → send message → agent replies with SessionID
// in EventResult → name "xxx" must appear in /list.
func TestSessionName_CodexLikeFlow(t *testing.T) {
	sess := newCodexLikeSession("codex-thread-new-001")
	listSessions := []AgentSessionInfo{
		{ID: "codex-thread-old", Summary: "Old session", MessageCount: 5, ModifiedAt: time.Now().Add(-time.Hour)},
	}
	agent := &controllableAgent{
		nextSession: sess,
		listFn: func() ([]AgentSessionInfo, error) {
			return listSessions, nil
		},
	}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	// Setup: create initial session with a known agent session ID
	initial := e.sessions.GetOrCreateActive(userKey)
	initial.SetAgentSessionID("codex-thread-old", "codex")
	e.sessions.Save()

	// Step 1: /new "我的新会话"
	e.cmdNew(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"我的新会话"})

	// Step 2: send a message (this triggers startOrResumeSession + processInteractiveEvents)
	e.ReceiveMessage(p, &Message{
		SessionKey: userKey,
		Content:    "请帮我做个功能",
		ReplyCtx:   "ctx2",
	})

	// Wait for the event loop to complete
	time.Sleep(200 * time.Millisecond)

	// Step 3: verify session name was mapped
	newSession := e.sessions.GetOrCreateActive(userKey)
	agentID := newSession.GetAgentSessionID()
	if agentID != "codex-thread-new-001" {
		t.Fatalf("AgentSessionID = %q, want %q", agentID, "codex-thread-new-001")
	}

	gotName := e.sessions.GetSessionName("codex-thread-new-001")
	if gotName != "我的新会话" {
		t.Fatalf("GetSessionName(%q) = %q, want %q", "codex-thread-new-001", gotName, "我的新会话")
	}

	// Step 4: verify /list displays the name
	listSessions = append(listSessions, AgentSessionInfo{
		ID:           "codex-thread-new-001",
		Summary:      "请帮我做个功能",
		MessageCount: 2,
		ModifiedAt:   time.Now(),
	})
	p.sent = nil
	e.cmdList(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, nil)
	if len(p.sent) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(p.sent))
	}
	if !strings.Contains(p.sent[0], "我的新会话") {
		t.Errorf("/list should show session name '我的新会话':\n%s", p.sent[0])
	}
}

// claudeCodeLikeSession simulates claudecode/gemini/cursor behavior:
// - CurrentSessionID() returns "" at creation
// - Send() emits an early EventText with SessionID (system/init event)
// - Then normal EventText without SessionID
// - Finally EventResult with SessionID
type claudeCodeLikeSession struct {
	threadID  string
	events    chan Event
	alive     bool
	hasSentID bool
}

func newClaudeCodeLikeSession(threadID string) *claudeCodeLikeSession {
	return &claudeCodeLikeSession{
		threadID: threadID,
		events:   make(chan Event, 8),
		alive:    true,
	}
}

func (s *claudeCodeLikeSession) Send(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.hasSentID = true
	// claudecode sends an early system event with SessionID (empty content)
	s.events <- Event{Type: EventText, Content: "", SessionID: s.threadID}
	// Normal streaming text (no SessionID)
	s.events <- Event{Type: EventText, Content: "Reply to: " + prompt}
	// Final result
	s.events <- Event{Type: EventResult, SessionID: s.threadID, Content: "Done", Done: true}
	return nil
}
func (s *claudeCodeLikeSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *claudeCodeLikeSession) Events() <-chan Event                                 { return s.events }
func (s *claudeCodeLikeSession) CurrentSessionID() string {
	if s.hasSentID {
		return s.threadID
	}
	return ""
}
func (s *claudeCodeLikeSession) Alive() bool  { return s.alive }
func (s *claudeCodeLikeSession) Close() error { s.alive = false; return nil }

// TestSessionName_ClaudeCodeLikeFlow tests the claudecode/gemini/cursor pattern:
// CurrentSessionID()="" initially, but an early EventText carries SessionID.
func TestSessionName_ClaudeCodeLikeFlow(t *testing.T) {
	sess := newClaudeCodeLikeSession("claude-session-001")
	agent := &controllableAgent{nextSession: sess}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	initial := e.sessions.GetOrCreateActive(userKey)
	initial.SetAgentSessionID("claude-session-old", "claudecode")
	e.sessions.Save()

	// /new with a custom name
	e.cmdNew(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"Claude任务"})

	// Send message
	e.ReceiveMessage(p, &Message{
		SessionKey: userKey,
		Content:    "帮我重构代码",
		ReplyCtx:   "ctx2",
	})
	time.Sleep(200 * time.Millisecond)

	// Verify session name mapped via EventText path
	gotName := e.sessions.GetSessionName("claude-session-001")
	if gotName != "Claude任务" {
		t.Fatalf("GetSessionName(%q) = %q, want %q — claudecode-like EventText name mapping failed",
			"claude-session-001", gotName, "Claude任务")
	}
}

// acpLikeSession simulates ACP behavior:
//   - CurrentSessionID() returns the thread ID immediately after creation
//     (ACP does handshake before returning from StartSession)
type acpLikeSession struct {
	threadID string
	events   chan Event
	alive    bool
}

func newACPLikeSession(threadID string) *acpLikeSession {
	return &acpLikeSession{
		threadID: threadID,
		events:   make(chan Event, 8),
		alive:    true,
	}
}

func (s *acpLikeSession) Send(prompt string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.events <- Event{Type: EventText, Content: "Reply", SessionID: s.threadID}
	s.events <- Event{Type: EventResult, SessionID: s.threadID, Content: "Done", Done: true}
	return nil
}
func (s *acpLikeSession) RespondPermission(_ string, _ PermissionResult) error { return nil }
func (s *acpLikeSession) Events() <-chan Event                                 { return s.events }
func (s *acpLikeSession) CurrentSessionID() string                             { return s.threadID }
func (s *acpLikeSession) Alive() bool                                          { return s.alive }
func (s *acpLikeSession) Close() error                                         { s.alive = false; return nil }

// TestSessionName_ACPLikeFlow tests ACP pattern: CurrentSessionID() is non-empty
// immediately at creation, so name mapping happens in startOrResumeSession.
func TestSessionName_ACPLikeFlow(t *testing.T) {
	sess := newACPLikeSession("acp-session-001")
	agent := &controllableAgent{nextSession: sess}
	p := &stubPlatformEngine{n: "plain"}
	e := NewEngine("test", agent, []Platform{p}, "")
	userKey := "test:user1"

	// /new with a custom name
	e.cmdNew(p, &Message{SessionKey: userKey, ReplyCtx: "ctx"}, []string{"ACP任务"})

	// Send message — startOrResumeSession should map the name immediately
	e.ReceiveMessage(p, &Message{
		SessionKey: userKey,
		Content:    "帮我部署",
		ReplyCtx:   "ctx2",
	})
	time.Sleep(200 * time.Millisecond)

	gotName := e.sessions.GetSessionName("acp-session-001")
	if gotName != "ACP任务" {
		t.Fatalf("GetSessionName(%q) = %q, want %q — ACP-like immediate ID name mapping failed",
			"acp-session-001", gotName, "ACP任务")
	}
}

func TestHandlePendingPermission_AskQuestion_EmptyContentRejected(t *testing.T) {
	// Regression test for #1086: empty or whitespace-only messages must NOT
	// be accepted as AskUserQuestion answers. Some platforms deliver read-receipts
	// or delivery notifications as empty messages within ~500ms; before this fix,
	// they resolved the question with empty answers immediately.
	e := newTestEngine()

	session := &recordingAgentSession{}
	pending := &pendingPermission{
		RequestID: "req-askq",
		Questions: testQuestions(),
		Answers:   map[int]string{},
		Resolved:  make(chan struct{}),
	}

	iKey := "ws:sk"
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = &interactiveState{
		agentSession: session,
		pending:      pending,
	}
	e.interactiveMu.Unlock()

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "sk", ReplyCtx: "ctx"}

	for _, emptyContent := range []string{"", "   ", "\t", "\n"} {
		if e.handlePendingPermission(p, msg, emptyContent, iKey) {
			t.Errorf("handlePendingPermission(%q) = true, want false (empty answer must be rejected)", emptyContent)
		}
		select {
		case <-pending.Resolved:
			t.Errorf("AskUserQuestion resolved with empty content %q", emptyContent)
		default:
		}
		if session.calls != 0 {
			t.Errorf("RespondPermission called with empty content %q", emptyContent)
		}
	}

	// A real answer should still work after the empty ones were rejected.
	if !e.handlePendingPermission(p, msg, "1", iKey) {
		t.Fatal("handlePendingPermission(\"1\") = false, want true")
	}
	if session.calls != 1 {
		t.Fatalf("RespondPermission calls = %d, want 1", session.calls)
	}
}

func TestMaybeAutoResetSessionOnIdle_UsesLastUserActivity(t *testing.T) {
	// Regression test for #1115 Bug 2: maybeAutoResetSessionOnIdle must use
	// LastUserActivity (only updated on real user messages) rather than
	// UpdatedAt (bumped by every session.Unlock including heartbeats).
	// Without the fix, automated activity (heartbeats, unsolicited agent output)
	// would continuously bump UpdatedAt and prevent idle reset from ever firing.
	e := newTestEngine()
	e.SetResetOnIdle(30 * time.Minute)

	sm := NewSessionManager(t.TempDir())
	session := sm.GetOrCreateActive("user:sk")
	// Simulate history so the session is eligible for reset.
	session.AddHistory("user", "hello")
	session.SetAgentSessionID("agent-id-1", "claudecode")
	session.TryLock()

	// Simulate that UpdatedAt is recent (heartbeat just updated it)
	// but LastUserActivity is old (last real user message was 35 minutes ago).
	old := time.Now().Add(-35 * time.Minute)
	session.mu.Lock()
	session.UpdatedAt = time.Now() // heartbeat bumped this just now
	session.LastUserActivity = old // last real user message was 35 min ago
	session.mu.Unlock()

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "sk", ReplyCtx: "ctx"}

	rotated := e.maybeAutoResetSessionOnIdle(p, msg, sm, "ws:sk", session)
	if rotated == nil {
		t.Fatal("expected idle reset to fire because LastUserActivity is 35min ago, but it did not")
	}
}

func TestMaybeAutoResetSessionOnIdle_NotFiredWhenUserActivityRecent(t *testing.T) {
	// Complementary test: when LastUserActivity is recent, the reset must NOT fire
	// even if UpdatedAt is also recent (normal case).
	e := newTestEngine()
	e.SetResetOnIdle(30 * time.Minute)

	sm := NewSessionManager(t.TempDir())
	session := sm.GetOrCreateActive("user:sk2")
	session.AddHistory("user", "hello")
	session.SetAgentSessionID("agent-id-2", "claudecode")
	session.TryLock()

	// LastUserActivity is only 5 minutes ago — should not idle-reset.
	session.mu.Lock()
	session.LastUserActivity = time.Now().Add(-5 * time.Minute)
	session.mu.Unlock()

	p := &stubPlatformEngine{n: "test"}
	msg := &Message{SessionKey: "sk2", ReplyCtx: "ctx"}

	rotated := e.maybeAutoResetSessionOnIdle(p, msg, sm, "ws:sk2", session)
	if rotated != nil {
		t.Fatal("expected no idle reset because LastUserActivity is only 5min ago")
	}
}

// TestHandlePendingPermission_StalePermissionCallback_Dropped verifies that
// permission-callback messages synthesized by inline-button / card-action paths
// (Telegram callback_query, Feishu card_action, QQBot interaction button, and
// the bridge web admin card_action) are silently dropped when there is no
// matching interactive state or pending request — instead of letting the
// literal "allow" / "deny" string reach the agent's prompt stream. Plain
// text "allow" / "deny" from a real user must continue to fall through
// (return false) so the caller can route them through the normal message
// handler. Regression test for #826.
func TestHandlePendingPermission_StalePermissionCallback_Dropped(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	t.Run("no interactive state — stale callback is dropped (returns true)", func(t *testing.T) {
		msg := &Message{
			SessionKey:           "ghost-session",
			Content:              "allow",
			IsPermissionResponse: true,
		}
		if !e.handlePendingPermission(p, msg, "allow", "ghost-ikey") {
			t.Fatal("handlePendingPermission returned false, want true (drop stale callback)")
		}
		if rec.calls != 0 {
			t.Fatalf("RespondPermission called %d times, want 0 (stale callback must not reach agent)", rec.calls)
		}
	})

	t.Run("no pending request — stale callback is dropped (returns true)", func(t *testing.T) {
		iKey := "ws:sk-stale-no-pending"
		e.interactiveMu.Lock()
		e.interactiveStates[iKey] = &interactiveState{
			agentSession: rec,
			pending:      nil,
		}
		e.interactiveMu.Unlock()

		msg := &Message{
			SessionKey:           "sk-stale-no-pending",
			Content:              "deny",
			IsPermissionResponse: true,
		}
		if !e.handlePendingPermission(p, msg, "deny", iKey) {
			t.Fatal("handlePendingPermission returned false, want true (drop stale callback)")
		}
		if rec.calls != 0 {
			t.Fatalf("RespondPermission called %d times, want 0 (stale callback must not reach agent)", rec.calls)
		}
	})

	t.Run("plain text 'allow' from real user falls through (returns false)", func(t *testing.T) {
		// No flag, no state → must return false so caller routes to normal handler.
		msg := &Message{
			SessionKey: "sk-plain",
			Content:    "allow",
		}
		if e.handlePendingPermission(p, msg, "allow", "sk-plain-ikey") {
			t.Fatal("handlePendingPermission returned true, want false (plain user message must fall through)")
		}
		if rec.calls != 0 {
			t.Fatalf("RespondPermission called %d times, want 0 (plain user message should not auto-resolve)", rec.calls)
		}
	})

	t.Run("matching pending request — callback still resolves", func(t *testing.T) {
		iKey := "ws:sk-fresh"
		pending := &pendingPermission{
			RequestID: "req-fresh",
			ToolName:  "Bash",
			ToolInput: map[string]any{"command": "ls"},
			Resolved:  make(chan struct{}),
		}
		e.interactiveMu.Lock()
		e.interactiveStates[iKey] = &interactiveState{
			agentSession: rec,
			pending:      pending,
		}
		e.interactiveMu.Unlock()

		msg := &Message{
			SessionKey:           "sk-fresh",
			Content:              "allow",
			IsPermissionResponse: true,
		}
		if !e.handlePendingPermission(p, msg, "allow", iKey) {
			t.Fatal("handlePendingPermission returned false, want true (matching pending must resolve)")
		}
		if rec.calls != 1 {
			t.Fatalf("RespondPermission calls = %d, want 1", rec.calls)
		}
		if rec.lastID != "req-fresh" {
			t.Fatalf("RespondPermission id = %q, want req-fresh", rec.lastID)
		}
		if rec.lastResult.Behavior != "allow" {
			t.Fatalf("RespondPermission behavior = %q, want allow", rec.lastResult.Behavior)
		}
	})
}

// ─── Permission keyword tokenization (t-20260614-ayc85z) ────────────────
// Group-chat platforms (wecom in particular) require the user to
// @mention the bot for the message to reach agent-bridge, so permission
// replies arrive as "@bot 允许" / "允许 @bot" / etc. rather than the
// bare keyword. The matchers must tolerate the surrounding mention
// without losing word-boundary discipline (e.g. must NOT match
// "禁止允许这种" — the keyword is embedded inside another CJK word).

func TestIsAllowResponse_WithLeadingMention(t *testing.T) {
	cases := []string{
		"@产品经理 允许",
		"@bot 允许",
		"@bot allow",
		"@bot ok",
		"@产品经理 同意",
	}
	for _, s := range cases {
		if !isAllowResponse(strings.ToLower(s)) {
			t.Errorf("isAllowResponse(%q) = false, want true", s)
		}
	}
}

func TestIsAllowResponse_WithTrailingMention(t *testing.T) {
	cases := []string{
		"允许 @产品经理",
		"allow @bot",
		"好的 @bot",
		"yes @bot",
	}
	for _, s := range cases {
		if !isAllowResponse(strings.ToLower(s)) {
			t.Errorf("isAllowResponse(%q) = false, want true", s)
		}
	}
}

func TestIsAllowResponse_WithMultipleMentions(t *testing.T) {
	cases := []string{
		"@a @b 允许",
		"@a allow @b",
		"hey @bot 好的, @user2",
	}
	for _, s := range cases {
		if !isAllowResponse(strings.ToLower(s)) {
			t.Errorf("isAllowResponse(%q) = false, want true", s)
		}
	}
}

// TestIsAllowResponse_NotInsideOtherWord locks the false-positive
// boundary: a keyword embedded inside a longer CJK string must NOT
// match. This is what distinguishes token-level matching from naive
// substring contains.
func TestIsAllowResponse_NotInsideOtherWord(t *testing.T) {
	cases := []string{
		"禁止允许这种",
		"不允许这样",                            // "不允许" has its own deny entry, but as part of "不允许这样" the user clearly is denying / negating, never allowing.
		"我不太允许这件事",                         // long sentence, no token equals "允许"
		"please don't allowall the things", // FieldsFunc keeps "allowall" intact, but it is the approveAll single-token form, not allow.
		"hello world",
		"",
	}
	for _, s := range cases {
		if isAllowResponse(strings.ToLower(s)) {
			t.Errorf("isAllowResponse(%q) = true, want false (no token equals an allow keyword)", s)
		}
	}
}

func TestIsDenyResponse_WithMention(t *testing.T) {
	cases := []string{
		"@产品经理 拒绝",
		"@bot deny",
		"拒绝 @bot",
		"@bot reject",
		"@bot 不允许",
		"@bot cancel",
	}
	for _, s := range cases {
		if !isDenyResponse(strings.ToLower(s)) {
			t.Errorf("isDenyResponse(%q) = false, want true", s)
		}
	}

	negatives := []string{
		"拒绝症患者",        // embedded — must not match
		"我们都不应该 hello", // unrelated
	}
	for _, s := range negatives {
		if isDenyResponse(strings.ToLower(s)) {
			t.Errorf("isDenyResponse(%q) = true, want false", s)
		}
	}
}

func TestIsApproveAllResponse_MultiWordWithMention(t *testing.T) {
	cases := []string{
		"@bot 允许所有",
		"@bot allow all",
		"allow all @bot",
		"@产品经理 允许全部",
		"hey please allow all things @bot", // sliding-window phrase match
		"全部允许",
	}
	for _, s := range cases {
		if !isApproveAllResponse(strings.ToLower(s)) {
			t.Errorf("isApproveAllResponse(%q) = false, want true", s)
		}
	}

	// Single allow keyword alone must not be approve-all.
	negatives := []string{
		"@bot 允许",
		"allow",
		"yes",
		"",
	}
	for _, s := range negatives {
		if isApproveAllResponse(strings.ToLower(s)) {
			t.Errorf("isApproveAllResponse(%q) = true, want false (single allow is not approve-all)", s)
		}
	}
}

// TestHandlePendingPermission_AllowWithMention is the integration
// regression for the wecom group bug: a real Bash permission request is
// pending, and the user replies with "@产品经理 允许" exactly as wecom
// delivers it. Before the fix this fell through to the "still waiting"
// branch and the agent never advanced.
func TestHandlePendingPermission_AllowWithMention(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	iKey := "wecom:group:user1"
	pending := &pendingPermission{
		RequestID: "req-bash-1",
		ToolName:  "Bash",
		ToolInput: map[string]any{"command": "cat /etc/passwd"},
		Resolved:  make(chan struct{}),
	}
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending:      pending,
	}
	e.interactiveMu.Unlock()

	msg := &Message{
		SessionKey: "wecom:group:user1",
		UserID:     "user1",
		Content:    "@产品经理 允许",
		ReplyCtx:   "ctx",
	}
	if !e.handlePendingPermission(p, msg, "@产品经理 允许", iKey) {
		t.Fatal("handlePendingPermission returned false, want true")
	}
	if rec.calls != 1 {
		t.Fatalf("RespondPermission calls = %d, want 1", rec.calls)
	}
	if rec.lastID != "req-bash-1" {
		t.Fatalf("RespondPermission id = %q, want req-bash-1", rec.lastID)
	}
	if rec.lastResult.Behavior != "allow" {
		t.Fatalf("RespondPermission behavior = %q, want allow", rec.lastResult.Behavior)
	}
}

// TestHandlePendingPermission_ApproveAllWithMention covers the same
// path for the approve-all phrase — must beat the per-token allow
// match because handlePendingPermission checks isApproveAllResponse
// first.
func TestHandlePendingPermission_ApproveAllWithMention(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}

	iKey := "wecom:group:user2"
	pending := &pendingPermission{
		RequestID: "req-bash-2",
		ToolName:  "Bash",
		ToolInput: map[string]any{"command": "rm -rf /tmp/x"},
		Resolved:  make(chan struct{}),
	}
	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending:      pending,
	}
	e.interactiveMu.Lock()
	e.interactiveStates[iKey] = state
	e.interactiveMu.Unlock()

	msg := &Message{
		SessionKey: "wecom:group:user2",
		UserID:     "user2",
		Content:    "@产品经理 允许所有",
		ReplyCtx:   "ctx",
	}
	if !e.handlePendingPermission(p, msg, "@产品经理 允许所有", iKey) {
		t.Fatal("handlePendingPermission returned false, want true")
	}
	if rec.calls != 1 {
		t.Fatalf("RespondPermission calls = %d, want 1", rec.calls)
	}
	if rec.lastResult.Behavior != "allow" {
		t.Fatalf("RespondPermission behavior = %q, want allow", rec.lastResult.Behavior)
	}
	state.mu.Lock()
	approveAll := state.approveAll
	state.mu.Unlock()
	if !approveAll {
		t.Fatal("state.approveAll = false, want true (approve-all must persist for follow-up tools)")
	}
}

// ─── Audio / Video routing (t-20260615-cqjbk1) ────────────────────────
// `agent-bridge send --audio` / `--video` must reach AudioSender /
// VideoSender — NOT SendFile. PR #1202 made the CLI flags exist but
// silently routed clips through SendFile, defeating the
// transcoding-and-render-as-native-bubble pipeline.

// audioVideoStubPlatform implements both AudioSender and VideoSender
// alongside the file-fallback path so we can assert the engine picks
// the dedicated method.
type audioVideoStubPlatform struct {
	stubMediaPlatform
	mu     sync.Mutex
	audios []audioCall
	videos []videoCall
}

type audioCall struct {
	data   []byte
	format string
}

type videoCall struct {
	data     []byte
	format   string
	fileName string
}

func (p *audioVideoStubPlatform) SendAudio(_ context.Context, _ any, audio []byte, format string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.audios = append(p.audios, audioCall{data: append([]byte(nil), audio...), format: format})
	return nil
}

func (p *audioVideoStubPlatform) SendVideo(_ context.Context, _ any, video []byte, format string, fileName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.videos = append(p.videos, videoCall{data: append([]byte(nil), video...), format: format, fileName: fileName})
	return nil
}

func TestSendAudiosToSession_RoutesToSendAudio_NotSendFile(t *testing.T) {
	p := &audioVideoStubPlatform{stubMediaPlatform: stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.interactiveStates["session-audio"] = &interactiveState{platform: p, replyCtx: "ctx"}

	err := e.SendAudiosToSession("session-audio", []FileAttachment{
		{MimeType: "audio/mpeg", Data: []byte("mp3-bytes"), FileName: "clip.mp3"},
		{MimeType: "audio/ogg", Data: []byte("opus-bytes"), FileName: "voice.opus"},
	})
	if err != nil {
		t.Fatalf("SendAudiosToSession returned error: %v", err)
	}
	if got := len(p.audios); got != 2 {
		t.Fatalf("AudioSender.SendAudio called %d times, want 2", got)
	}
	if p.audios[0].format != "mp3" {
		t.Errorf("audio[0].format = %q, want %q (filename ext wins)", p.audios[0].format, "mp3")
	}
	if p.audios[1].format != "opus" {
		t.Errorf("audio[1].format = %q, want %q", p.audios[1].format, "opus")
	}
	if string(p.audios[0].data) != "mp3-bytes" {
		t.Errorf("audio[0].data = %q, want %q", p.audios[0].data, "mp3-bytes")
	}
	// Crucial: must NOT have hit SendFile.
	if len(p.files) != 0 {
		t.Errorf("SendFile called %d times, want 0 (audio must not fall back when AudioSender exists)", len(p.files))
	}
}

func TestSendAudiosToSession_PlatformWithoutAudioSender_FallsBackToFile(t *testing.T) {
	// stubMediaPlatform implements ImageSender + FileSender but NOT AudioSender.
	p := &stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "no-audio"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.interactiveStates["session-fallback"] = &interactiveState{platform: p, replyCtx: "ctx"}

	err := e.SendAudiosToSession("session-fallback", []FileAttachment{
		{MimeType: "audio/mpeg", Data: []byte("mp3-bytes"), FileName: "clip.mp3"},
	})
	if err != nil {
		t.Fatalf("SendAudiosToSession returned error: %v", err)
	}
	if len(p.files) != 1 {
		t.Fatalf("expected fallback SendFile call, got %d", len(p.files))
	}
	if p.files[0].FileName != "clip.mp3" {
		t.Errorf("fallback file name = %q, want clip.mp3", p.files[0].FileName)
	}
}

func TestSendAudiosToSession_PlatformWithNeitherSender_Errors(t *testing.T) {
	p := &stubPlatformEngine{n: "text-only"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.interactiveStates["session-none"] = &interactiveState{platform: p, replyCtx: "ctx"}

	err := e.SendAudiosToSession("session-none", []FileAttachment{
		{MimeType: "audio/mpeg", Data: []byte("x"), FileName: "x.mp3"},
	})
	if err == nil {
		t.Fatal("expected error when platform has neither AudioSender nor FileSender")
	}
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("err = %v, want ErrNotSupported", err)
	}
}

func TestSendAudiosToSession_DisabledByConfig(t *testing.T) {
	p := &audioVideoStubPlatform{stubMediaPlatform: stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.SetAttachmentSendEnabled(false)
	e.interactiveStates["session-audio-off"] = &interactiveState{platform: p, replyCtx: "ctx"}

	err := e.SendAudiosToSession("session-audio-off", []FileAttachment{
		{MimeType: "audio/mpeg", Data: []byte("x"), FileName: "x.mp3"},
	})
	if !errors.Is(err, ErrAttachmentSendDisabled) {
		t.Fatalf("err = %v, want ErrAttachmentSendDisabled", err)
	}
	if len(p.audios) != 0 {
		t.Errorf("audios sent while disabled: %d, want 0", len(p.audios))
	}
}

func TestSendVideosToSession_RoutesToSendVideo_NotSendFile(t *testing.T) {
	p := &audioVideoStubPlatform{stubMediaPlatform: stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "feishu"}}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.interactiveStates["session-video"] = &interactiveState{platform: p, replyCtx: "ctx"}

	err := e.SendVideosToSession("session-video", []FileAttachment{
		{MimeType: "video/mp4", Data: []byte("mp4-bytes"), FileName: "demo.mp4"},
	})
	if err != nil {
		t.Fatalf("SendVideosToSession returned error: %v", err)
	}
	if len(p.videos) != 1 {
		t.Fatalf("VideoSender.SendVideo called %d times, want 1", len(p.videos))
	}
	if p.videos[0].format != "mp4" {
		t.Errorf("video[0].format = %q, want mp4", p.videos[0].format)
	}
	if p.videos[0].fileName != "demo.mp4" {
		t.Errorf("video[0].fileName = %q, want demo.mp4", p.videos[0].fileName)
	}
	if len(p.files) != 0 {
		t.Errorf("SendFile called %d times, want 0 (video must not fall back when VideoSender exists)", len(p.files))
	}
}

func TestSendVideosToSession_PlatformWithoutVideoSender_FallsBackToFile(t *testing.T) {
	p := &stubMediaPlatform{stubPlatformEngine: stubPlatformEngine{n: "no-video"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.interactiveStates["session-vfb"] = &interactiveState{platform: p, replyCtx: "ctx"}

	err := e.SendVideosToSession("session-vfb", []FileAttachment{
		{MimeType: "video/mp4", Data: []byte("mp4"), FileName: "demo.mp4"},
	})
	if err != nil {
		t.Fatalf("SendVideosToSession returned error: %v", err)
	}
	if len(p.files) != 1 {
		t.Fatalf("expected fallback SendFile call, got %d", len(p.files))
	}
}

func TestAudioFormatHint(t *testing.T) {
	cases := []struct {
		name string
		in   FileAttachment
		want string
	}{
		{"filename ext wins", FileAttachment{FileName: "voice.OPUS", MimeType: "application/octet-stream"}, "opus"},
		{"mime fallback", FileAttachment{FileName: "blob", MimeType: "audio/mpeg"}, "mpeg"},
		{"mime with codecs", FileAttachment{FileName: "blob", MimeType: "audio/ogg; codecs=opus"}, "ogg"},
		{"empty", FileAttachment{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := audioFormatHint(tc.in); got != tc.want {
				t.Errorf("audioFormatHint(%+v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// --- Regression: streaming-card silent reply must not leak the NO_REPLY marker ---

// recordingStreamCard captures the content passed to Finalize so tests can
// assert what was rendered into the card.
type recordingStreamCard struct {
	mu      sync.Mutex
	final   bool
	content string
}

func (c *recordingStreamCard) Update(_ context.Context, _ string) error { return nil }
func (c *recordingStreamCard) Finalize(_ context.Context, content string) error {
	c.mu.Lock()
	c.final = true
	c.content = content
	c.mu.Unlock()
	return nil
}
func (c *recordingStreamCard) Failed() bool { return false }
func (c *recordingStreamCard) finalized() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.final
}
func (c *recordingStreamCard) finalContent() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.content
}

// recordingStreamCardPlatform is a StreamingCardPlatform whose card records the
// finalized content for assertions.
type recordingStreamCardPlatform struct {
	stubPlatformEngine
	card *recordingStreamCard
}

func (p *recordingStreamCardPlatform) CreateStreamingCard(_ context.Context, _ any) (StreamingCard, error) {
	return p.card, nil
}

// TestProcessInteractiveEvents_StreamingCard_BareNoReply_Suppressed is a
// regression test for the bug where a silent (bare NO_REPLY) turn on a
// StreamingCardPlatform rendered the literal "NO_REPLY" marker into the card:
// the streamCard finalize branch ran before — and shadowed — the isSilent
// suppression branch, so buildCardContent received the raw NO_REPLY response.
// The card must finalize WITHOUT the marker on a silent turn.
func TestProcessInteractiveEvents_StreamingCard_BareNoReply_Suppressed(t *testing.T) {
	card := &recordingStreamCard{}
	p := &recordingStreamCardPlatform{
		stubPlatformEngine: stubPlatformEngine{n: "slack"},
		card:               card,
	}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	sessionKey := "slack:user-streamcard-bare-noreply"
	session := e.sessions.GetOrCreateActive(sessionKey)
	agentSession := newControllableSession("s-streamcard-bare-noreply")
	state := &interactiveState{
		agentSession: agentSession,
		platform:     p,
		replyCtx:     "ctx-streamcard-bare-noreply",
	}
	e.interactiveStates[sessionKey] = state

	agentSession.events <- Event{Type: EventText, Content: "NO_REPLY"}
	agentSession.events <- Event{Type: EventResult, Content: "NO_REPLY", Done: true}

	e.processInteractiveEvents(state, session, e.sessions, sessionKey, "m-streamcard-bare-noreply", time.Now(), nil, nil, state.replyCtx)

	if !card.finalized() {
		t.Fatalf("expected streaming card to be finalized on a silent turn")
	}
	if strings.Contains(card.finalContent(), "NO_REPLY") {
		t.Fatalf("silent reply leaked NO_REPLY into the streaming card: %q", card.finalContent())
	}
}
