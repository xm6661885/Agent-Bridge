package core

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"
)

const maxPlatformMessageLen = 4000
const defaultMaxQueuedMessages = 5 // default cap for queued messages per session

// defaultPendingRestartTimeout is how long the post-restart notify
// dispatcher waits for the target platform to reach ready before
// dropping the notify with a warning. 10s covers the typical 2-3s
// Telegram connect window with margin and is short enough that a stuck
// platform does not block startup logging indefinitely.
const defaultPendingRestartTimeout = 10 * time.Second

// previewText truncates s to maxRunes runes for safe inclusion in debug logs.
// Truncation uses runes (not bytes) so multi-byte characters render cleanly.
// Newlines are replaced with literal "\n" to keep each log entry on one line.
func previewText(s string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	r := []rune(s)
	truncated := false
	if len(r) > maxRunes {
		r = r[:maxRunes]
		truncated = true
	}
	out := strings.ReplaceAll(string(r), "\n", "\\n")
	if truncated {
		out += "...(truncated)"
	}
	return out
}

const (
	defaultThinkingMaxLen = 300
	defaultToolMaxLen     = 500
	defaultHistoryMaxLen  = 1000
)

// Slow-operation thresholds. Operations exceeding these durations produce a
// slog.Warn so operators can quickly pinpoint bottlenecks.
const (
	slowPlatformSend    = 2 * time.Second  // platform Reply / Send
	slowAgentStart      = 5 * time.Second  // agent.StartSession
	slowAgentClose      = 3 * time.Second  // agentSession.Close
	slowAgentSend       = 2 * time.Second  // agentSession.Send
	slowAgentFirstEvent = 15 * time.Second // time from send to first agent event
)

// closeTimeout bounds a single agent session teardown. It must cover the
// agent's own graceful shutdown sequence: stdin close → Stop hooks
// (claude-mem summary etc.) → SIGTERM → SIGKILL (with retries). Claude Code's
// Stop hooks can take up to 120s (claude-mem uses a sonnet summarizer), so the
// 150s budget covers the 120s graceful phase + 5s SIGTERM + ~16s of bounded
// SIGKILL retries/confirmation + buffer. The wait ends early if the process
// exits sooner — this is the ceiling, not the typical duration.
const closeTimeout = 150 * time.Second

const (
	replyFooterUsageTimeout  = 1500 * time.Millisecond
	replyFooterUsageCacheTTL = 30 * time.Second
)

const (
	messageRecallCheckTimeout  = 2 * time.Second
	messageRecallPollInterval  = 2 * time.Second
	messageRecallProbeCooldown = time.Minute
	recalledStopLockWait       = 2 * time.Second
)

// CurrentVersion is the semver tag (e.g. "v1.2.0-beta.1"), set by main.
var CurrentVersion string

// ErrAttachmentSendDisabled indicates that side-channel image/file delivery is disabled by config.
var ErrAttachmentSendDisabled = errors.New("attachment send is disabled by config")

// RestartRequest carries info needed to send a post-restart notification.
type RestartRequest struct {
	SessionKey string `json:"session_key"`
	Platform   string `json:"platform"`
}

type replyFooterUsageCache struct {
	text      string
	fetchedAt time.Time
}

// SaveRestartNotify persists restart info so the new process can send
// a "restart successful" message after startup.
func SaveRestartNotify(dataDir string, req RestartRequest) error {
	dir := filepath.Join(dataDir, "run")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		slog.Warn("SaveRestartNotify: mkdir failed", "dir", dir, "error", err)
	}
	data, _ := json.Marshal(req)
	return os.WriteFile(filepath.Join(dir, "restart_notify"), data, 0o644)
}

// ConsumeRestartNotify reads and deletes the restart notification file.
// Returns nil if no notification is pending.
func ConsumeRestartNotify(dataDir string) *RestartRequest {
	p := filepath.Join(dataDir, "run", "restart_notify")
	data, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	os.Remove(p)
	var req RestartRequest
	if json.Unmarshal(data, &req) != nil {
		return nil
	}
	return &req
}

// SendRestartNotification sends a "restart successful" message to the
// platform/session that initiated the restart. For async-recoverable
// platforms that may not be ready yet at startup, the call is queued
// and dispatched on the first OnPlatformReady for the matching platform
// (see issue #1383).
func (e *Engine) SendRestartNotification(platformName, sessionKey string) {
	req := &RestartRequest{Platform: platformName, SessionKey: sessionKey}
	e.SetPendingRestartNotify(req)
}

// SetPendingRestartNotify queues a restart notification for dispatch
// when the target platform becomes ready. Replaces any previously queued
// notify. If the target platform is already ready, the notify is
// dispatched on a background goroutine with retry on send failure.
func (e *Engine) SetPendingRestartNotify(req *RestartRequest) {
	if req == nil {
		return
	}
	e.pendingRestartMu.Lock()
	e.pendingRestartNotify = req
	e.pendingRestartFiredCh = make(chan struct{})
	firedCh := e.pendingRestartFiredCh
	e.pendingRestartMu.Unlock()

	// If the target platform is already ready, fire the dispatch on a
	// goroutine so the caller (main startup) is not blocked. If not yet
	// ready, OnPlatformReady will pick it up. A safety goroutine drops
	// the notify after a timeout if the platform never reaches ready.
	go e.runPendingRestartNotify(req, firedCh)
}

// ConsumePendingRestartNotify returns the currently queued notify (if any)
// and clears it. Used by tests.
func (e *Engine) ConsumePendingRestartNotify() *RestartRequest {
	e.pendingRestartMu.Lock()
	defer e.pendingRestartMu.Unlock()
	req := e.pendingRestartNotify
	e.pendingRestartNotify = nil
	return req
}

// SetPendingRestartTimeout overrides the safety timeout used by
// runPendingRestartNotify. Production code uses defaultPendingRestartTimeout
// (10s); tests use this to exercise the timeout path quickly.
func (e *Engine) SetPendingRestartTimeout(d time.Duration) {
	e.pendingRestartTimeout = d
}

// runPendingRestartNotify dispatches the notify for a platform that is
// already ready, with bounded retry on transient send failure. The fired
// channel is closed when the notify is fully resolved (success, exhausted
// retries, or platform dropped from engine). This is called from
// SetPendingRestartNotify on a background goroutine and from
// onPlatformReady (also on a goroutine).
func (e *Engine) runPendingRestartNotify(req *RestartRequest, firedCh chan struct{}) {
	defer close(firedCh)

	// Recover from any panic inside dispatch (ReconstructReplyCtx / Send) so a
	// platform-side panic cannot take down the whole agent-bridge process.
	// See #1686 P1-A. Without this defer, a panic in the restart-notify
	// goroutine kills the daemon because no higher-level recover exists.
	defer func() {
		if r := recover(); r != nil {
			const maxStackBytes = 8192
			stack := make([]byte, maxStackBytes)
			n := runtime.Stack(stack, false)
			slog.Error("restart notify panic",
				"platform", req.Platform,
				"session", req.SessionKey,
				"panic", r,
				"stack", string(stack[:n]))
		}
	}()

	// Wait briefly for the platform to reach ready if it's not already.
	// Upper bound: matches the typical Telegram 2-3 s connect window
	// with margin (see defaultPendingRestartTimeout), and short enough
	// that a stuck platform does not block startup logging forever.
	timeout := e.pendingRestartTimeout
	if timeout <= 0 {
		timeout = defaultPendingRestartTimeout
	}
	deadline := time.Now().Add(timeout)
	for {
		e.pendingRestartMu.Lock()
		stillQueued := e.pendingRestartNotify == req
		e.pendingRestartMu.Unlock()
		if !stillQueued {
			return
		}
		if e.lookupReadyPlatform(req.Platform) != nil {
			break
		}
		if time.Now().After(deadline) {
			slog.Warn("restart notify: target platform did not reach ready in time, dropping",
				"platform", req.Platform, "session", req.SessionKey, "timeout", timeout)
			e.clearPendingRestart(req)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	if err := e.dispatchRestartNotify(req); err != nil {
		slog.Warn("restart notify: dispatch failed after retries",
			"platform", req.Platform, "session", req.SessionKey, "error", err)
	}
	e.clearPendingRestart(req)
}

// lookupReadyPlatform returns the platform with the given name if it has
// reached ready state, otherwise nil.
func (e *Engine) lookupReadyPlatform(platformName string) Platform {
	e.platformLifecycleMu.Lock()
	defer e.platformLifecycleMu.Unlock()
	for p, ready := range e.platformReady {
		if ready && p.Name() == platformName {
			return p
		}
	}
	return nil
}

// clearPendingRestart removes the queued notify if it is still the same
// request (avoids clearing a newer notify that replaced this one).
func (e *Engine) clearPendingRestart(req *RestartRequest) {
	e.pendingRestartMu.Lock()
	if e.pendingRestartNotify == req {
		e.pendingRestartNotify = nil
	}
	e.pendingRestartMu.Unlock()
}

// dispatchRestartNotify sends the notify to the target platform with up
// to 3 attempts (initial + 2 retries) on transient failure. The
// platform must already be ready when this is called.
func (e *Engine) dispatchRestartNotify(req *RestartRequest) error {
	p := e.lookupReadyPlatform(req.Platform)
	if p == nil {
		return fmt.Errorf("platform %q not ready", req.Platform)
	}
	rc, ok := p.(ReplyContextReconstructor)
	if !ok {
		return fmt.Errorf("platform %q does not support ReconstructReplyCtx", req.Platform)
	}
	rctx, err := rc.ReconstructReplyCtx(req.SessionKey)
	if err != nil {
		return fmt.Errorf("reconstruct reply ctx: %w", err)
	}
	text := e.i18n.T(MsgRestartSuccess)
	if CurrentVersion != "" {
		text += fmt.Sprintf(" (%s)", CurrentVersion)
	}

	backoffs := []time.Duration{0, 500 * time.Millisecond, 1500 * time.Millisecond}
	var lastErr error
	for attempt, wait := range backoffs {
		if wait > 0 {
			time.Sleep(wait)
		}
		if err := e.waitOutgoing(p); err != nil {
			lastErr = fmt.Errorf("wait outgoing: %w", err)
			slog.Warn("restart notify: wait outgoing failed",
				"platform", req.Platform, "attempt", attempt+1, "error", err)
			continue
		}
		if err := p.Send(e.ctx, rctx, text); err != nil {
			lastErr = err
			slog.Warn("restart notify: send failed, will retry",
				"platform", req.Platform, "session", req.SessionKey,
				"attempt", attempt+1, "max_attempts", len(backoffs), "error", err)
			continue
		}
		if attempt > 0 {
			slog.Info("restart notify: send succeeded after retry",
				"platform", req.Platform, "session", req.SessionKey, "attempt", attempt+1)
		}
		return nil
	}
	return lastErr
}

// RestartCh is signaled when /restart is invoked. main listens on it
// to perform a graceful shutdown followed by syscall.Exec.
var RestartCh = make(chan RestartRequest, 1)

// DisplayCfg controls how intermediate messages are surfaced.
// A value of -1 means "use default", 0 means "no truncation".
type DisplayCfg struct {
	CleanupProgressOnComplete bool
	CollapseToolMessages      bool
	Mode                      string // "full" (default), "compact", or "quiet" — thinking/tool visibility
	CardMode                  string // "legacy" (default) or "rich" (Card 2.0 Feishu)
	ThinkingMessages          bool
	ThinkingMaxLen            int // max runes for thinking preview; 0 = no truncation
	ToolMaxLen                int // max runes for tool use preview; 0 = no truncation
	ToolMessages              bool
	HistoryMaxLen             *int // max runes for /history entries; nil = default, 0 = no truncation
	HideAgentFooter           bool // strip model/token footer lines emitted as agent text
}

// InstantReplyCfg controls the immediate confirmation reply sent when a message
// is received, before the agent starts processing.
type InstantReplyCfg struct {
	Enabled bool
	Content string // custom reply text; empty = use i18n MsgStarting default
}

// RateLimitCfg controls per-session message rate limiting.
type RateLimitCfg struct {
	MaxMessages int           // max messages per window; 0 = disabled
	Window      time.Duration // sliding window size
}

// Engine routes messages between platforms and the agent for a single project.
type Engine struct {
	name                  string
	agent                 Agent
	platforms             []Platform
	sessions              *SessionManager
	ctx                   context.Context
	cancel                context.CancelFunc
	i18n                  *I18n
	speech                SpeechCfg
	tts                   *TTSCfg
	display               DisplayCfg
	injectSender          bool
	attachmentSendEnabled bool
	startedAt             time.Time

	providerSaveFunc        func(providerName string) error
	providerAddSaveFunc     func(p ProviderConfig) error
	providerRemoveSaveFunc  func(name string) error
	providerModelSaveFunc   func(providerName, model string) error
	providerRefsSaveFunc    func(refs []string) error
	listGlobalProvidersFunc func(agentType string) ([]ProviderConfig, error)
	modelSaveFunc           func(model string) error

	ttsSaveFunc func(mode string) error

	commandSaveAddFunc func(name, description, prompt, exec, workDir string) error
	commandSaveDelFunc func(name string) error

	displaySaveFunc  func(mode *string, thinkingMessages *bool, thinkingMaxLen, toolMaxLen *int, toolMessages *bool) error
	configReloadFunc func() (*ConfigReloadResult, error)

	hooks *HookManager

	commands *CommandRegistry
	aliases  map[string]string // trigger → command (e.g. "帮助" → "/help")
	aliasMu  sync.RWMutex

	aliasSaveAddFunc func(name, command string) error
	aliasSaveDelFunc func(name string) error

	bannedWords []string
	bannedMu    sync.RWMutex

	disabledCmds map[string]bool
	adminFrom    string           // comma-separated user IDs for privileged commands; "*" = all allowed users; "" = deny
	userRoles    *UserRoleManager // nil = legacy mode (no per-user policies)
	userRolesMu  sync.RWMutex     // protects userRoles, disabledCmds, and adminFrom

	rateLimiter      *RateLimiter
	outgoingRL       *OutgoingRateLimiter
	streamPreview    StreamPreviewCfg
	instantReply     InstantReplyCfg
	references       ReferenceRenderCfg
	eventIdleTimeout time.Duration
	maxTurnTime      time.Duration // absolute wall-clock cap per turn (0 = disabled)
	// agentSessionIdleTimeoutNanos 在单轮正常结束后关闭空闲的 live agent 进程，
	// 同时保留已保存的 session ID，便于下次继续恢复。
	agentSessionIdleTimeoutNanos atomic.Int64
	agentSessionIdleSeq          atomic.Uint64
	maxQueuedMessages            int
	dirHistory                   *DirHistory
	baseWorkDir                  string
	projectState                 *ProjectStateStore

	// Auto-compress settings
	autoCompressEnabled   bool
	autoCompressMaxTokens int
	autoCompressMinGap    time.Duration
	resetOnIdle           time.Duration

	// Reply footer composition flags. The footer renders up to two lines:
	//   line 1 — model · [effort ·] out/in/cw/cr · ctx%   (gated by showContextIndicator)
	//   line 2 — workspace directory                       (gated by showWorkdirIndicator)
	// replyFooterEnabled is the master toggle: when false, no footer is emitted
	// regardless of the per-line flags.
	showContextIndicator bool
	showWorkdirIndicator bool
	replyFooterEnabled   bool

	// When true, /list etc. only show sessions tracked by agent-bridge,
	// hiding sessions created by direct CLI usage in the same work_dir.
	// Default false = show all sessions.
	filterExternalSessions bool

	// Shell configuration for /shell, hooks, webhook exec
	shell        string // shell binary path (e.g. "sh", "/bin/zsh")
	shellFlag    string // shell flag (e.g. "-c", "-Command", "/C")
	shellProfile string // prepended to every command (e.g. "source ~/.zshrc;")

	// Per-work_dir agents created by send --cwd
	workspacePool *workspacePool
	sendWorkDirMu sync.RWMutex
	sendWorkDirs  map[string]string // sessionKey → work_dir assigned by send --cwd

	// Terminal observation (--observe)
	observeEnabled    bool
	observeProjectDir string // ~/.claude/projects/{projectKey}
	observeSessionKey string // e.g. "slack:C123:U456" — target for forwarding
	observeCancel     context.CancelFunc

	// Interactive agent session management
	interactiveMu     sync.Mutex
	interactiveStates map[string]*interactiveState // key = sessionKey
	// Attachment staging belongs to the conversation, not the live agent process.
	stagedAttachments map[string]*stagedAttachments // guarded by interactiveMu

	// serialQueues provides FIFO processing across separate sessions when a
	// platform assigns Message.SerialKey. Core intentionally knows nothing
	// about the platform or the shape of the key.
	serialMu     sync.Mutex
	serialQueues map[string]*serialMessageQueue

	// Teardown tracking. A session's state is removed from interactiveStates
	// as soon as /stop is issued, but its OS process can live on for up to
	// closeTimeout while it drains stdin and runs Stop hooks. Without the two
	// maps below, a message arriving in that window spawns a second process
	// that resumes the *same* agent session ID — two live agents then share
	// one conversation lineage, each acting on it independently.
	closingMu       sync.Mutex
	closingSessions map[string]chan struct{} // sessionKey → closed when teardown finishes
	unsafeResume    map[string]bool          // sessionKey → next spawn must start fresh, not resume

	platformLifecycleMu sync.Mutex
	platformReady       map[Platform]bool
	stopping            bool
	replyFooterMu       sync.Mutex
	replyFooterUsage    replyFooterUsageCache

	// pendingRestartNotify is queued at startup if a /restart was consumed
	// from the run/restart_notify file. It is dispatched on the first
	// OnPlatformReady for the matching platform name, so async platforms
	// (Telegram, Weixin, Matrix, Discord) have a chance to actually connect
	// before the post-restart message is sent. See issue #1383.
	pendingRestartMu      sync.Mutex
	pendingRestartNotify  *RestartRequest
	pendingRestartFiredCh chan struct{} // closed when the notify is dispatched (success or exhausted)

	// pendingRestartTimeout is how long the dispatcher waits for the
	// target platform to reach ready before dropping the notify with a
	// warning. Default 10s; tests may override.
	pendingRestartTimeout time.Duration

	// /web command callbacks
	webSetupFunc  func() (port int, token string, needRestart bool, err error)
	webStatusFunc func() (url string)

	// Data directory for socket path injection
	dataDir string
}

// queuedMessage holds a message that arrived while the session was busy.
// The message is NOT sent to agent stdin at queue time; the event loop
// sends it after the current turn completes to avoid mid-turn interference.
type queuedMessage struct {
	messageID         string
	platform          Platform
	replyCtx          any
	content           string
	images            []ImageAttachment
	files             []FileAttachment
	fromVoice         bool
	userID            string
	userName          string // sender's display name for sender injection
	msgPlatform       string // platform name for sender injection
	msgSessionKey     string // session key for extracting chat ID
	channelKey        string // platform-provided channel identifier (preferred over sessionKey extraction)
	userMessageTimeMs int64  // Feishu create_time ms (optional); see Message.UserMessageTimeMs
}

// serialMessageQueue is a per-platform-defined FIFO. A message remains the
// queue head until its asynchronous agent turn has fully finished.
type serialMessageQueue struct {
	running bool
	pending []serialMessage
}

type serialMessage struct {
	platform Platform
	msg      *Message
}

// interactiveState tracks a running interactive agent session and its permission state.
type interactiveState struct {
	agentSession     AgentSession
	platform         Platform
	replyCtx         any
	currentMessageID string
	currentUserID    string // sender of the in-flight foreground turn
	// steerGen increments on every accepted steer message; the event loop
	// starts fresh progress/preview messages when it changes.
	steerGen                 atomic.Uint64
	lastRecallProbeMessageID string
	lastRecallProbeAt        time.Time
	recallProbeInFlight      bool
	workspaceDir             string
	agent                    Agent
	mu                       sync.Mutex
	stopCh                   chan struct{}
	stopped                  bool
	pending                  *pendingPermission
	pendingMessages          []queuedMessage // messages queued while session was busy
	approveAll               bool            // when true, auto-approve all permission requests for this session
	fromVoice                bool            // true if current turn originated from voice transcription
	sideText                 string
	deleteMode               *deleteModeState
	modelSwitch              *modelSwitchState
	pendingProviderAdd       *pendingProviderAddState
	lastAutoCompressAt       time.Time
	lastAutoCompressTokens   int

	// Unsolicited event reader: a background goroutine that consumes agent
	// events between user-initiated turns (e.g. background task completions).
	// Cancel unsolicitedCancel to stop the reader; wait on unsolicitedDone
	// to confirm it has exited before starting a new foreground turn.
	unsolicitedCancel context.CancelFunc // nil when no reader is running
	unsolicitedDone   chan struct{}      // closed when the reader goroutine exits

	// agentSessionIdleCancel 取消当前会话的 idle 关闭计时器。
	agentSessionIdleCancel context.CancelFunc
	agentSessionIdleToken  uint64

	// eventsNeedResync is true when buffered events should be drained before
	// the next turn (e.g. after an abnormal exit). Defaults to true (safe);
	// cleared to false only after a clean EventResult.
	eventsNeedResync bool

	// lastCompletedUserMessageTimeMs is the max platform user-message create time
	// (ms) for which an agent turn has finished with EventResult.
	lastCompletedUserMessageTimeMs int64
	// currentTurnUserMessageTimeMs is the UserMessageTimeMs for the in-flight
	// foreground turn (including a queued turn after EventResult).
	currentTurnUserMessageTimeMs int64
}

// stagedAttachments holds images/files staged from a pure-attachment message
// (no text) until the next text message from the same session arrives to
// merge them into. See handleMessage's stage-until-text logic.
type stagedAttachments struct {
	images []ImageAttachment
	files  []FileAttachment
}

// latestUserMessageWatermarkLocked returns the highest UserMessageTimeMs among
// completed turns, the in-flight turn, and queued messages for this session.
// state.mu must be held by the caller.
func latestUserMessageWatermarkLocked(state *interactiveState) int64 {
	if state == nil {
		return 0
	}
	wm := state.lastCompletedUserMessageTimeMs
	if state.currentTurnUserMessageTimeMs > wm {
		wm = state.currentTurnUserMessageTimeMs
	}
	for _, q := range state.pendingMessages {
		if q.userMessageTimeMs > wm {
			wm = q.userMessageTimeMs
		}
	}
	return wm
}

type userMessageWatermarkSnapshot struct {
	maxMs       int64
	completedMs int64
	inFlightMs  int64
	queuedMaxMs int64
}

// userMessageWatermarkSnapshotLocked captures the per-source watermark values.
// state.mu must be held by the caller.
func userMessageWatermarkSnapshotLocked(state *interactiveState) userMessageWatermarkSnapshot {
	if state == nil {
		return userMessageWatermarkSnapshot{}
	}
	snap := userMessageWatermarkSnapshot{
		completedMs: state.lastCompletedUserMessageTimeMs,
		inFlightMs:  state.currentTurnUserMessageTimeMs,
	}
	for _, q := range state.pendingMessages {
		if q.userMessageTimeMs > snap.queuedMaxMs {
			snap.queuedMaxMs = q.userMessageTimeMs
		}
	}
	snap.maxMs = latestUserMessageWatermarkLocked(state)
	return snap
}

// acceptReason describes which accepted message makes an older create_time stale.
func (s userMessageWatermarkSnapshot) acceptReason() string {
	if s.maxMs <= 0 {
		return "none"
	}
	// Prefer the most specific active state when multiple sources tie.
	if s.inFlightMs == s.maxMs && s.inFlightMs > 0 {
		return "processing"
	}
	if s.queuedMaxMs == s.maxMs && s.queuedMaxMs > 0 {
		return "queued"
	}
	if s.completedMs == s.maxMs && s.completedMs > 0 {
		return "completed"
	}
	return "unknown"
}

func (e *Engine) logStaleUserMessageDropped(action string, msg *Message, interactiveKey string, snap userMessageWatermarkSnapshot) {
	if msg == nil {
		return
	}
	slog.Info("stale user message dropped: received from platform but create_time older than accepted watermark",
		"action", action,
		"platform", msg.Platform,
		"session", msg.SessionKey,
		"interactive_key", interactiveKey,
		"msg_id", msg.MessageID,
		"msg_create_time_ms", msg.UserMessageTimeMs,
		"watermark_ms", snap.maxMs,
		"watermark_reason", snap.acceptReason(),
		"watermark_processing_ms", snap.inFlightMs,
		"watermark_queued_max_ms", snap.queuedMaxMs,
		"watermark_completed_ms", snap.completedMs,
	)
	slog.Debug("stale user message dropped detail",
		"action", action,
		"msg_id", msg.MessageID,
		"msg_create_time_ms", msg.UserMessageTimeMs,
		"watermark_ms", snap.maxMs,
		"watermark_reason", snap.acceptReason(),
		"content_len", len(msg.Content),
	)
}

type pendingProviderAddState struct {
	phase string // "other" = waiting for name api_key base_url [model]
}

type deleteModeState struct {
	page        int
	selectedIDs map[string]struct{}
	phase       string
	hint        string
	result      string
}

type modelSwitchState struct {
	phase  string
	target string
	result string
}

// pendingPermission represents a permission request waiting for user response.
type pendingPermission struct {
	RequestID       string
	ToolName        string
	ToolInput       map[string]any
	InputPreview    string
	Questions       []UserQuestion // non-nil for AskUserQuestion
	Answers         map[int]string // collected answers keyed by question index
	CurrentQuestion int            // index of the question currently being asked
	Plan            string         // non-empty for ExitPlanMode plan review
	OwnerUserID     string         // user whose turn raised the prompt; "" = anyone
	Resolved        chan struct{}  // closed when user responds
	resolveOnce     sync.Once

	// awaitingFeedback is set after "keep planning" so the next text reply is
	// sent as revision feedback. Guarded by interactiveState.mu.
	awaitingFeedback bool
}

func (s *interactiveState) stopSignal() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
		if s.stopped {
			close(s.stopCh)
		}
	}
	return s.stopCh
}

func (s *interactiveState) isStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopped
}

func (s *interactiveState) markStopped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
	}
	close(s.stopCh)
}

// resolve safely closes the Resolved channel exactly once.
func (pp *pendingPermission) resolve() {
	pp.resolveOnce.Do(func() { close(pp.Resolved) })
}

func NewEngine(name string, ag Agent, platforms []Platform, sessionStorePath string) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		name:                  name,
		agent:                 ag,
		platforms:             platforms,
		sessions:              NewSessionManager(sessionStorePath),
		ctx:                   ctx,
		cancel:                cancel,
		i18n:                  NewI18n(),
		attachmentSendEnabled: true,
		display:               DisplayCfg{Mode: "full", ThinkingMessages: true, ThinkingMaxLen: defaultThinkingMaxLen, ToolMaxLen: defaultToolMaxLen, ToolMessages: true, CardMode: "legacy"},
		commands:              NewCommandRegistry(),
		aliases:               make(map[string]string),
		interactiveStates:     make(map[string]*interactiveState),
		serialQueues:          make(map[string]*serialMessageQueue),
		closingSessions:       make(map[string]chan struct{}),
		unsafeResume:          make(map[string]bool),
		sendWorkDirs:          make(map[string]string),
		platformReady:         make(map[Platform]bool),
		startedAt:             time.Now(),
		streamPreview:         DefaultStreamPreviewCfg(),
		references:            DefaultReferenceRenderCfg(),
		eventIdleTimeout:      defaultEventIdleTimeout,
		maxQueuedMessages:     defaultMaxQueuedMessages,
		showContextIndicator:  true,
		showWorkdirIndicator:  true,
		shell:                 defaultShell(),
		shellFlag:             defaultShellFlag(),
		pendingRestartTimeout: defaultPendingRestartTimeout,
	}

	if ag != nil {
		e.sessions.InvalidateForAgent(ag.Name())
	}

	if cp, ok := ag.(CommandProvider); ok {
		e.commands.SetAgentDirs(cp.CommandDirs())
	}

	return e
}

// DefaultWorkspaceIdleTimeout is the default time a workspace can be idle
// before the reaper reclaims it.
const DefaultWorkspaceIdleTimeout = 15 * time.Minute

// SetHooks configures the lifecycle event hook manager.
func (e *Engine) SetHooks(hm *HookManager) {
	e.hooks = hm
}

// SetShell configures the shell binary, flag, and shell profile used for exec.
func (e *Engine) SetShell(shell, flag, shellProfile string) {
	e.shell = shell
	e.shellFlag = flag
	e.shellProfile = shellProfile
}

func (e *Engine) SetSpeechConfig(cfg SpeechCfg) {
	e.speech = cfg
}

// SetTTSConfig configures the text-to-speech subsystem.
func (e *Engine) SetTTSConfig(cfg *TTSCfg) {
	e.tts = cfg
}

// SetTTSSaveFunc registers a callback that persists TTS mode changes.
func (e *Engine) SetTTSSaveFunc(fn func(mode string) error) {
	e.ttsSaveFunc = fn
}

// SetDisplayConfig overrides the default truncation settings.
func (e *Engine) SetDisplayConfig(cfg DisplayCfg) {
	e.display = cfg
}

// SetInstantReply configures the immediate confirmation reply.
func (e *Engine) SetInstantReply(cfg InstantReplyCfg) {
	e.instantReply = cfg
}

// SetReferenceConfig configures local reference normalization/rendering.
func (e *Engine) SetReferenceConfig(cfg ReferenceRenderCfg) {
	e.references = normalizeReferenceRenderCfg(cfg)
}

// estimateTokensWithPendingAssistant is like estimateTokens but includes an assistant
// message not yet written to history (used at EventResult before AddHistory).
func estimateTokensWithPendingAssistant(entries []HistoryEntry, pendingAssistant string) int {
	// Heuristic: ~1 token per 4 characters in mixed English/Chinese.
	count := 0
	for _, h := range entries {
		count += len([]rune(h.Content))
	}
	if pendingAssistant != "" {
		count += len([]rune(pendingAssistant))
	}
	if count == 0 {
		return 0
	}
	return (count + 3) / 4
}

// SetAutoCompressConfig configures automatic context compression.
func (e *Engine) SetAutoCompressConfig(enabled bool, maxTokens int, minGap time.Duration) {
	e.autoCompressEnabled = enabled
	e.autoCompressMaxTokens = maxTokens
	if minGap <= 0 {
		minGap = 30 * time.Minute
	}
	e.autoCompressMinGap = minGap
}

// SetResetOnIdle configures automatic session rotation after prolonged inactivity.
// A zero or negative duration disables the behavior.
func (e *Engine) SetResetOnIdle(d time.Duration) {
	if d <= 0 {
		e.resetOnIdle = 0
		return
	}
	e.resetOnIdle = d
}

// SetAgentSessionIdleTimeout 配置单轮正常结束后的 live agent 空闲关闭时间。
// 小于等于 0 表示禁用。
func (e *Engine) SetAgentSessionIdleTimeout(d time.Duration) {
	if d <= 0 {
		e.agentSessionIdleTimeoutNanos.Store(0)
		e.cancelAllAgentSessionIdleCloses()
		return
	}
	e.agentSessionIdleTimeoutNanos.Store(int64(d))
}

func (e *Engine) cancelAllAgentSessionIdleCloses() {
	e.interactiveMu.Lock()
	states := make([]*interactiveState, 0, len(e.interactiveStates))
	for _, state := range e.interactiveStates {
		states = append(states, state)
	}
	e.interactiveMu.Unlock()
	for _, state := range states {
		e.cancelAgentSessionIdleClose(state)
	}
}

// SetShowContextIndicator controls whether the reply footer's first line
// (model / reasoning effort / token counts / context %) is rendered.
// Subordinate to SetReplyFooterEnabled — when the footer is disabled overall,
// this flag has no effect.
func (e *Engine) SetShowContextIndicator(show bool) {
	e.showContextIndicator = show
}

// SetShowWorkdirIndicator controls whether the reply footer's second line
// (workspace directory) is rendered. Subordinate to SetReplyFooterEnabled.
func (e *Engine) SetShowWorkdirIndicator(show bool) {
	e.showWorkdirIndicator = show
}

// SetReplyFooterEnabled is the master toggle for the per-turn reply footer.
// When false, no footer (statusline-style or single-line) is emitted, and the
// per-line flags (SetShowContextIndicator / SetShowWorkdirIndicator) become
// no-ops.
func (e *Engine) SetReplyFooterEnabled(show bool) {
	e.replyFooterEnabled = show
}

// SetFilterExternalSessions controls whether /list, /switch, /delete, etc.
// hide sessions created by direct CLI usage in the same work_dir.
// Default false = show all sessions from the agent.
func (e *Engine) SetFilterExternalSessions(v bool) {
	e.filterExternalSessions = v
}

func (e *Engine) SetWebSetupFunc(fn func() (int, string, bool, error)) { e.webSetupFunc = fn }
func (e *Engine) SetWebStatusFunc(fn func() string)                    { e.webStatusFunc = fn }

// SetInjectSender controls whether sender identity (platform and user ID) is
// prepended to each message before forwarding it to the agent. When enabled,
// the agent receives a preamble line like:
//
//	[agent-bridge sender_id=ou_abc123 platform=feishu]
//
// This allows the agent to identify who sent the message and adjust behavior
// accordingly (e.g. personal task views, role-based access control).
func (e *Engine) SetInjectSender(v bool) {
	e.injectSender = v
}

// SetAttachmentSendEnabled controls whether side-channel image/file delivery is allowed.
func (e *Engine) SetAttachmentSendEnabled(v bool) {
	e.attachmentSendEnabled = v
}

// SetObserveConfig enables terminal session observation.
// projectDir is the Claude Code project directory containing session JSONL files.
// sessionKey identifies the Slack channel to forward messages to.
func (e *Engine) SetObserveConfig(projectDir, sessionKey string) {
	e.observeEnabled = true
	e.observeProjectDir = projectDir
	e.observeSessionKey = sessionKey
}

// findObserverTarget returns the first platform that implements ObserverTarget,
// or nil if none do.
func (e *Engine) findObserverTarget() ObserverTarget {
	for _, p := range e.platforms {
		if ot, ok := p.(ObserverTarget); ok {
			return ot
		}
	}
	return nil
}

func (e *Engine) SetProviderSaveFunc(fn func(providerName string) error) {
	e.providerSaveFunc = fn
}

func (e *Engine) SetProviderAddSaveFunc(fn func(ProviderConfig) error) {
	e.providerAddSaveFunc = fn
}

func (e *Engine) SetProviderRemoveSaveFunc(fn func(string) error) {
	e.providerRemoveSaveFunc = fn
}

func (e *Engine) SetProviderModelSaveFunc(fn func(providerName, model string) error) {
	e.providerModelSaveFunc = fn
}

func (e *Engine) SetProviderRefsSaveFunc(fn func(refs []string) error) {
	e.providerRefsSaveFunc = fn
}

func (e *Engine) SetListGlobalProvidersFunc(fn func(agentType string) ([]ProviderConfig, error)) {
	e.listGlobalProvidersFunc = fn
}

func (e *Engine) SetModelSaveFunc(fn func(model string) error) {
	e.modelSaveFunc = fn
}

// AddPlatform appends a platform to the engine after construction.
// The platform is started and wired during the next Engine.Start call,
// or if the engine is already running, it is started immediately.
func (e *Engine) AddPlatform(p Platform) {
	e.platforms = append(e.platforms, p)
}

func (e *Engine) SetCommandSaveAddFunc(fn func(name, description, prompt, exec, workDir string) error) {
	e.commandSaveAddFunc = fn
}

func (e *Engine) SetCommandSaveDelFunc(fn func(name string) error) {
	e.commandSaveDelFunc = fn
}

func (e *Engine) SetDisplaySaveFunc(fn func(mode *string, thinkingMessages *bool, thinkingMaxLen, toolMaxLen *int, toolMessages *bool) error) {
	e.displaySaveFunc = fn
}

// ConfigReloadResult describes what was updated by a config reload.
type ConfigReloadResult struct {
	DisplayUpdated   bool
	ProvidersUpdated int
	CommandsUpdated  int
}

func (e *Engine) SetConfigReloadFunc(fn func() (*ConfigReloadResult, error)) {
	e.configReloadFunc = fn
}

// GetAgent returns the engine's agent (for type assertions like ProviderSwitcher).
func (e *Engine) GetAgent() Agent {
	return e.agent
}

// GetSessions returns the Engine's session manager (for testing).
func (e *Engine) GetSessions() *SessionManager {
	return e.sessions
}

// AddCommand registers a custom slash command.
func (e *Engine) AddCommand(name, description, prompt, exec, workDir, source string) {
	e.commands.Add(name, description, prompt, exec, workDir, source)
}

// ClearCommands removes all commands from the given source.
func (e *Engine) ClearCommands(source string) {
	e.commands.ClearSource(source)
}

// AddAlias registers a command alias.
func (e *Engine) AddAlias(name, command string) {
	e.aliasMu.Lock()
	defer e.aliasMu.Unlock()
	e.aliases[name] = command
}

func (e *Engine) SetAliasSaveAddFunc(fn func(name, command string) error) {
	e.aliasSaveAddFunc = fn
}

func (e *Engine) SetAliasSaveDelFunc(fn func(name string) error) {
	e.aliasSaveDelFunc = fn
}

// ClearAliases removes all aliases (for config reload).
func (e *Engine) ClearAliases() {
	e.aliasMu.Lock()
	defer e.aliasMu.Unlock()
	e.aliases = make(map[string]string)
}

// resolveDisabledCmds resolves a list of command names (including "*" wildcard)
// to a set of canonical command IDs.
func resolveDisabledCmds(cmds []string) map[string]bool {
	m := make(map[string]bool, len(cmds))
	for _, c := range cmds {
		c = strings.ToLower(strings.TrimPrefix(c, "/"))
		if c == "*" {
			for _, bc := range builtinCommands {
				m[bc.id] = true
			}
			return m
		}
		if id := matchPrefix(c, builtinCommands); id != "" {
			m[id] = true
		} else {
			m[c] = true
		}
	}
	return m
}

// GetDisabledCommands returns the list of disabled command IDs for this project.
func (e *Engine) GetDisabledCommands() []string {
	e.userRolesMu.RLock()
	defer e.userRolesMu.RUnlock()
	out := make([]string, 0, len(e.disabledCmds))
	for k := range e.disabledCmds {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SetDisabledCommands sets the list of command IDs that are disabled for this project.
func (e *Engine) SetDisabledCommands(cmds []string) {
	e.userRolesMu.Lock()
	defer e.userRolesMu.Unlock()
	e.disabledCmds = resolveDisabledCmds(cmds)
}

// SetUserRoles configures per-user role-based policies. Pass nil to disable.
func (e *Engine) SetUserRoles(urm *UserRoleManager) {
	e.userRolesMu.Lock()
	defer e.userRolesMu.Unlock()
	if e.userRoles != nil {
		e.userRoles.Stop()
	}
	e.userRoles = urm
}

// SetAdminFrom sets the admin allowlist for privileged commands.
// "*" means all users who pass allow_from are admins.
// Empty string means privileged commands are denied for everyone.
func (e *Engine) SetAdminFrom(adminFrom string) {
	e.userRolesMu.Lock()
	e.adminFrom = strings.TrimSpace(adminFrom)
	af := e.adminFrom
	shellDisabled := e.disabledCmds["shell"]
	e.userRolesMu.Unlock()
	if af == "" && !shellDisabled {
		slog.Warn("admin_from is not set — privileged commands (/shell, /show, /dir, /restart, /diff) are blocked. "+
			"Set admin_from in config to enable them, or use disabled_commands to hide them.",
			"project", e.name)
	}
}

// privilegedCommands are commands that require admin_from authorization.
var privilegedCommands = map[string]bool{
	"shell":   true,
	"show":    true,
	"dir":     true,
	"restart": true,
	"diff":    true,
}

// isPrivilegedCommandInvocation extends the privilegedCommands map to
// also gate specific destructive subcommands. Currently:
//
//   - /commands addexec ...    — registers a custom shell-exec command
//
// This effectively creates an admin-only command at runtime; if a
// non-admin can call addexec, they can install arbitrary shell commands
// for any future user to trigger. Sibling subcommands (list, add, del,
// etc.) remain non-privileged.
//
// Returns true when the cmdID itself is in privilegedCommands, or when
// the (cmdID, args[0]) pair matches the explicitly gated
// subcommands above.
func isPrivilegedCommandInvocation(cmdID string, args []string) bool {
	if privilegedCommands[cmdID] {
		return true
	}
	if len(args) == 0 {
		return false
	}
	sub := strings.ToLower(args[0])
	switch cmdID {
	case "commands":
		return matchSubCommand(sub, []string{
			"list", "add", "addexec", "del", "delete", "rm", "remove",
		}) == "addexec"
	default:
		return false
	}
}

// isAdmin checks whether the given user ID is authorized for privileged commands.
// Unlike AllowList, empty adminFrom means deny-all (fail-closed).
func (e *Engine) isAdmin(userID string) bool {
	e.userRolesMu.RLock()
	af := e.adminFrom
	e.userRolesMu.RUnlock()
	if af == "" {
		return false
	}
	if af == "*" {
		return true
	}
	for _, id := range strings.Split(af, ",") {
		if strings.EqualFold(strings.TrimSpace(id), userID) {
			return true
		}
	}
	return false
}

// SetBannedWords replaces the banned words list.
func (e *Engine) SetBannedWords(words []string) {
	e.bannedMu.Lock()
	defer e.bannedMu.Unlock()
	lower := make([]string, len(words))
	for i, w := range words {
		lower[i] = strings.ToLower(w)
	}
	e.bannedWords = lower
}

// SetRateLimitCfg configures per-session message rate limiting.
// It stops the previous rate limiter's background goroutine before replacing it.
func (e *Engine) SetRateLimitCfg(cfg RateLimitCfg) {
	if e.rateLimiter != nil {
		e.rateLimiter.Stop()
	}
	e.rateLimiter = NewRateLimiter(cfg.MaxMessages, cfg.Window)
}

// SetOutgoingRateLimitCfg configures per-platform outgoing message throttling.
func (e *Engine) SetOutgoingRateLimitCfg(defaults OutgoingRateLimitCfg, overrides map[string]OutgoingRateLimitCfg) {
	e.outgoingRL = NewOutgoingRateLimiter(defaults, overrides)
}

// checkRateLimit returns true if the message is allowed, false if rate-limited.
// It checks per-user role-based limits first, then falls back to the global limiter.
func (e *Engine) checkRateLimit(msg *Message) bool {
	e.userRolesMu.RLock()
	urm := e.userRoles
	e.userRolesMu.RUnlock()

	// Try role-specific rate limit first
	if urm != nil {
		// Use userID if available, else fall back to sessionKey for unidentified users.
		// NOTE: sessionKey fallback means anonymous users get separate buckets per
		// session, which is less strict than per-user limiting. Platforms should
		// provide UserID for effective rate limiting.
		rateKey := msg.UserID
		if rateKey == "" {
			rateKey = msg.SessionKey
			slog.Debug("rate limit: no UserID, falling back to sessionKey", "session_key", msg.SessionKey)
		}
		allowed, handled := urm.AllowRate(rateKey)
		if handled {
			return allowed
		}
		// Role has no rate_limit config — fall through to global, keyed by user
	}
	// Global rate limiter
	if e.rateLimiter == nil {
		return true
	}
	// When users config active: key by userID (per-user); otherwise sessionKey (legacy)
	key := msg.SessionKey
	if urm != nil && msg.UserID != "" {
		key = msg.UserID
	}
	return e.rateLimiter.Allow(key)
}

// SetStreamPreviewCfg configures the streaming preview behavior.
func (e *Engine) SetStreamPreviewCfg(cfg StreamPreviewCfg) {
	e.streamPreview = cfg
}

// SetMaxTurnTime sets an absolute wall-clock limit on how long a single agent turn
// may run. Unlike SetEventIdleTimeout (which resets on every event), this timer is
// not reset by tool-call activity. When it fires, the session is terminated and the
// user is notified. A value of 0 disables the limit (default).
//
// This is the primary mitigation for #1091: long-running bash commands that generate
// periodic tool events keep the idle timer alive indefinitely; the turn timer caps them.
func (e *Engine) SetMaxTurnTime(d time.Duration) {
	e.maxTurnTime = d
}

// SetEventIdleTimeout sets the maximum time to wait between consecutive agent events.
// 0 disables the timeout entirely.
func (e *Engine) SetEventIdleTimeout(d time.Duration) {
	e.eventIdleTimeout = d
}

// SetMaxQueuedMessages sets the per-session message queue depth.
// Values <= 0 are ignored.
func (e *Engine) SetMaxQueuedMessages(n int) {
	if n > 0 {
		e.maxQueuedMessages = n
	}
}

func (e *Engine) SetDirHistory(dh *DirHistory) {
	e.dirHistory = dh
}

func (e *Engine) SetBaseWorkDir(dir string) {
	e.baseWorkDir = dir
}

func (e *Engine) SetProjectStateStore(store *ProjectStateStore) {
	e.projectState = store
}

func (e *Engine) SetDataDir(dir string) {
	e.dataDir = dir
}

// RemoveCommand removes a custom command by name. Returns false if not found.
func (e *Engine) RemoveCommand(name string) bool {
	return e.commands.Remove(name)
}

func (e *Engine) ProjectName() string {
	return e.name
}

// AgentTypeName returns the agent type name (e.g. "claudecode", "codex").
func (e *Engine) AgentTypeName() string {
	if e.agent != nil {
		return e.agent.Name()
	}
	return ""
}

// ActiveSessionKeys returns the session keys of all active interactive sessions.
func (e *Engine) ActiveSessionKeys() []string {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	var keys []string
	for key, state := range e.interactiveStates {
		if state.platform != nil {
			keys = append(keys, key)
		}
	}
	return keys
}

func (e *Engine) Start() error {
	var startErrs []error
	readyCount := 0
	pendingCount := 0
	for _, p := range e.platforms {
		_, isAsync := p.(AsyncRecoverablePlatform)
		if async, ok := p.(AsyncRecoverablePlatform); ok {
			async.SetLifecycleHandler(e)
		}
		if err := p.Start(e.handleMessage); err != nil {
			slog.Warn("platform start failed", "project", e.name, "platform", p.Name(), "error", err)
			startErrs = append(startErrs, fmt.Errorf("[%s] start platform %s: %w", e.name, p.Name(), err))
			continue
		}
		if isAsync {
			pendingCount++
			slog.Info("platform recovery loop started", "project", e.name, "platform", p.Name())
			continue
		}
		e.onPlatformReady(p)
		readyCount++
	}

	// Log summary
	if len(startErrs) > 0 || pendingCount > 0 {
		slog.Warn("engine started with partial readiness",
			"project", e.name,
			"agent", e.agent.Name(),
			"ready", readyCount,
			"pending", pendingCount,
			"failed", len(startErrs))
	} else {
		slog.Info("engine started", "project", e.name, "agent", e.agent.Name(), "platforms", len(e.platforms))
	}

	// Only return error if ALL platforms failed
	if len(startErrs) == len(e.platforms) && len(e.platforms) > 0 {
		return startErrs[0] // Return first error
	}

	e.startObserver()
	return nil
}

func (e *Engine) Stop() error {
	e.platformLifecycleMu.Lock()
	e.stopping = true
	e.platformLifecycleMu.Unlock()

	// Cancel first so late lifecycle callbacks observe shutdown immediately.
	e.cancel()

	if e.observeCancel != nil {
		e.observeCancel()
	}

	// Stop platforms after cancellation so they can unwind against the closed context.
	var errs []error
	for _, p := range e.platforms {
		if err := p.Stop(); err != nil {
			errs = append(errs, fmt.Errorf("stop platform %s: %w", p.Name(), err))
		}
	}

	e.interactiveMu.Lock()
	states := make(map[string]*interactiveState, len(e.interactiveStates))
	for k, v := range e.interactiveStates {
		states[k] = v
		delete(e.interactiveStates, k)
	}
	e.stagedAttachments = nil
	e.interactiveMu.Unlock()

	for key, state := range states {
		if state.agentSession != nil {
			slog.Debug("engine.Stop: closing agent session", "session", key)
			state.agentSession.Close()
		}
	}

	if e.rateLimiter != nil {
		e.rateLimiter.Stop()
	}
	e.userRolesMu.Lock()
	if e.userRoles != nil {
		e.userRoles.Stop()
	}
	e.userRolesMu.Unlock()

	if err := e.agent.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stop agent %s: %w", e.agent.Name(), err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("engine stop errors: %v", errs)
	}
	return nil
}

// OnPlatformReady marks an async platform as ready and initializes platform-level
// capabilities once per ready cycle.
func (e *Engine) OnPlatformReady(p Platform) {
	e.onPlatformReady(p)
}

// OnPlatformUnavailable marks an async platform as unavailable.
func (e *Engine) OnPlatformUnavailable(p Platform, err error) {
	if !e.markPlatformUnavailable(p) {
		return
	}
	slog.Warn("platform unavailable", "project", e.name, "platform", p.Name(), "error", err)
}

// ReceiveMessage delivers a message from a platform to the engine.
// This is a public wrapper for use in integration tests and external callers.
func (e *Engine) ReceiveMessage(p Platform, msg *Message) {
	// A turn blocked on a permission/question/plan prompt holds the serial
	// slot until it is answered, so answers must bypass the queue.
	if msg != nil && msg.SerialKey != "" && !msg.IsPermissionResponse && !e.hasPendingInteraction(msg.SessionKey) {
		e.enqueueSerialMessage(p, msg)
		return
	}
	e.handleMessage(p, msg)
}

// enqueueSerialMessage starts one message at a time for a platform-provided
// serial key. The notification is deliberately sent at enqueue time so a
// group member gets immediate feedback rather than waiting for the preceding
// agent turn to complete.
func (e *Engine) enqueueSerialMessage(p Platform, msg *Message) {
	e.serialMu.Lock()
	q := e.serialQueues[msg.SerialKey]
	if q == nil {
		q = &serialMessageQueue{}
		e.serialQueues[msg.SerialKey] = q
	}
	queued := q.running || len(q.pending) > 0
	q.pending = append(q.pending, serialMessage{platform: p, msg: msg})
	e.serialMu.Unlock()

	if queued {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgMessageQueued))
	}
	e.startNextSerialMessage(msg.SerialKey)
}

func (e *Engine) startNextSerialMessage(key string) {
	e.serialMu.Lock()
	q := e.serialQueues[key]
	if q == nil || q.running || len(q.pending) == 0 {
		e.serialMu.Unlock()
		return
	}
	entry := q.pending[0]
	q.running = true
	entry.msg.serialDone = func() { e.finishSerialMessage(key, entry.msg) }
	e.serialMu.Unlock()

	go e.handleMessage(entry.platform, entry.msg)
}

func (e *Engine) finishSerialMessage(key string, msg *Message) {
	e.serialMu.Lock()
	q := e.serialQueues[key]
	if q == nil || len(q.pending) == 0 || q.pending[0].msg != msg {
		e.serialMu.Unlock()
		return
	}
	q.pending = q.pending[1:]
	q.running = false
	if len(q.pending) == 0 {
		delete(e.serialQueues, key)
	}
	e.serialMu.Unlock()
	e.startNextSerialMessage(key)
}

func (msg *Message) deferSerialCompletion() {
	if msg != nil && msg.serialDone != nil {
		msg.serialAsync = true
	}
}

func (msg *Message) finishSerialCompletion() {
	if msg == nil || msg.serialDone == nil {
		return
	}
	done := msg.serialDone
	msg.serialDone = nil
	done()
}

func (e *Engine) onPlatformReady(p Platform) {
	if !e.markPlatformReady(p) {
		return
	}
	slog.Info("platform ready", "project", e.name, "platform", p.Name())
	e.initPlatformCapabilities(p)
}

func (e *Engine) markPlatformReady(p Platform) bool {
	e.platformLifecycleMu.Lock()
	defer e.platformLifecycleMu.Unlock()

	if e.stopping || e.ctx.Err() != nil {
		return false
	}
	if e.platformReady[p] {
		return false
	}
	e.platformReady[p] = true
	return true
}

func (e *Engine) markPlatformUnavailable(p Platform) bool {
	e.platformLifecycleMu.Lock()
	defer e.platformLifecycleMu.Unlock()

	if e.stopping || e.ctx.Err() != nil {
		return false
	}
	if !e.platformReady[p] {
		return false
	}
	e.platformReady[p] = false
	return true
}

func (e *Engine) initPlatformCapabilities(p Platform) {
	if registrar, ok := p.(CommandRegistrar); ok {
		commands := e.GetAllCommands()
		if err := registrar.RegisterCommands(commands); err != nil {
			slog.Error("platform command registration failed", "project", e.name, "platform", p.Name(), "error", err)
		} else {
			slog.Debug("platform commands registered", "project", e.name, "platform", p.Name(), "count", len(commands))
		}
	}

	if nav, ok := p.(CardNavigable); ok {
		nav.SetCardNavigationHandler(e.handleCardNav)
	}
}

// matchBannedWord returns the first banned word found in content, or "".
func (e *Engine) matchBannedWord(content string) string {
	e.bannedMu.RLock()
	defer e.bannedMu.RUnlock()
	if len(e.bannedWords) == 0 {
		return ""
	}
	lower := strings.ToLower(content)
	for _, w := range e.bannedWords {
		if strings.Contains(lower, w) {
			return w
		}
	}
	return ""
}

// resolveAlias checks if the content (or its first word) matches an alias and replaces it.
func (e *Engine) resolveAlias(content string) string {
	e.aliasMu.RLock()
	defer e.aliasMu.RUnlock()

	if len(e.aliases) == 0 {
		return content
	}

	// Exact match on full content
	if cmd, ok := e.aliases[content]; ok {
		return cmd
	}

	// Match first word, append remaining args
	parts := strings.SplitN(content, " ", 2)
	if cmd, ok := e.aliases[parts[0]]; ok {
		if len(parts) > 1 {
			return cmd + " " + parts[1]
		}
		return cmd
	}
	return content
}

func (e *Engine) handleMessageRecall(p Platform, msg *Message) {
	messageID := strings.TrimSpace(msg.MessageID)
	if messageID == "" {
		slog.Debug("message recall ignored without message id", "platform", msg.Platform)
		return
	}

	if sessionKey, ok := e.findCurrentMessageSession(messageID); ok {
		if e.stopInteractiveSessionSilently(sessionKey) {
			slog.Info("active message recalled; session stopped",
				"platform", p.Name(),
				"msg_id", messageID,
				"session", sessionKey,
			)
			return
		}
	}

	if sessionKey, ok := e.removeQueuedMessageByID(messageID); ok {
		slog.Info("queued message recalled; removed from pending queue",
			"platform", p.Name(),
			"msg_id", messageID,
			"session", sessionKey,
		)
		return
	}

	slog.Debug("message recall ignored; no active or queued message matched",
		"platform", p.Name(),
		"msg_id", messageID,
	)
}

func (e *Engine) findCurrentMessageSession(messageID string) (string, bool) {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()

	for sessionKey, state := range e.interactiveStates {
		if state == nil {
			continue
		}
		state.mu.Lock()
		currentMessageID := state.currentMessageID
		state.mu.Unlock()
		if currentMessageID == messageID {
			return sessionKey, true
		}
	}
	return "", false
}

func (e *Engine) removeQueuedMessageByID(messageID string) (string, bool) {
	e.interactiveMu.Lock()
	states := make(map[string]*interactiveState, len(e.interactiveStates))
	for sessionKey, state := range e.interactiveStates {
		states[sessionKey] = state
	}
	e.interactiveMu.Unlock()

	for sessionKey, state := range states {
		if state == nil {
			continue
		}
		state.mu.Lock()
		pending := state.pendingMessages
		if len(pending) == 0 {
			state.mu.Unlock()
			continue
		}
		filtered := pending[:0]
		removed := false
		for _, queued := range pending {
			if queued.messageID == messageID {
				removed = true
				continue
			}
			filtered = append(filtered, queued)
		}
		if removed {
			state.pendingMessages = filtered
			state.mu.Unlock()
			return sessionKey, true
		}
		state.mu.Unlock()
	}
	return "", false
}

// isStaleUserMessageLocked reports whether timeMs is strictly older than the
// latest user message already accepted for this session (completed, in-flight,
// or queued). state.mu must be held by the caller. timeMs <= 0 is never stale.
func (e *Engine) isStaleUserMessageLocked(state *interactiveState, timeMs int64) bool {
	if timeMs <= 0 {
		return false
	}
	wm := latestUserMessageWatermarkLocked(state)
	return wm > 0 && timeMs < wm
}

// isQueuedUserMessageStaleForDrainLocked reports whether a queued message is
// older than an already processed or currently in-flight turn. It intentionally
// ignores other queued messages so a FIFO queue with increasing create_time
// does not drop earlier queued messages just because later queued messages
// have already been accepted.
func (e *Engine) isQueuedUserMessageStaleForDrainLocked(state *interactiveState, timeMs int64) bool {
	if timeMs <= 0 {
		return false
	}
	wm := state.lastCompletedUserMessageTimeMs
	if state.currentTurnUserMessageTimeMs > wm {
		wm = state.currentTurnUserMessageTimeMs
	}
	return wm > 0 && timeMs < wm
}

// noteUserTurnCompleted advances lastCompletedUserMessageTimeMs after an
// agent turn ends with EventResult.
func (e *Engine) noteUserTurnCompleted(state *interactiveState) {
	state.mu.Lock()
	t := state.currentTurnUserMessageTimeMs
	if t > 0 && t > state.lastCompletedUserMessageTimeMs {
		state.lastCompletedUserMessageTimeMs = t
	}
	state.mu.Unlock()
}

// noteUserMessageAccepted records that a user message was accepted for this
// session (direct processing). Queued messages update the watermark via
// pendingMessages instead. This closes the window before processInteractiveMessageWith
// starts where a redelivery could slip through while the session lock is held.
func (e *Engine) noteUserMessageAccepted(interactiveKey string, timeMs int64) {
	if timeMs <= 0 {
		return
	}
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if !ok || state == nil {
		return
	}
	state.mu.Lock()
	if timeMs > state.currentTurnUserMessageTimeMs {
		state.currentTurnUserMessageTimeMs = timeMs
	}
	state.mu.Unlock()
}

// discardStaleUserMessageIfNeeded returns true when the message is dropped
// because a newer user message is already in progress, queued, or completed
// for this interactive session (e.g. Feishu redelivery with a new message_id
// but an older create_time).
func (e *Engine) discardStaleUserMessageIfNeeded(interactiveKey string, msg *Message) bool {
	if msg == nil || msg.UserMessageTimeMs <= 0 {
		return false
	}
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if !ok || state == nil {
		return false
	}
	state.mu.Lock()
	stale := e.isStaleUserMessageLocked(state, msg.UserMessageTimeMs)
	snap := userMessageWatermarkSnapshotLocked(state)
	state.mu.Unlock()
	if !stale {
		return false
	}
	e.logStaleUserMessageDropped("reject_before_handle", msg, interactiveKey, snap)
	return true
}

func (e *Engine) stopCurrentMessageIfRecalled(sessionKey string) bool {
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	e.interactiveMu.Unlock()
	if !ok || state == nil {
		return false
	}

	state.mu.Lock()
	platform := state.platform
	replyCtx := state.replyCtx
	messageID := state.currentMessageID
	if platform == nil || replyCtx == nil || messageID == "" {
		state.mu.Unlock()
		return false
	}
	detector, ok := platform.(MessageRecallDetector)
	if !ok {
		state.mu.Unlock()
		return false
	}
	if state.recallProbeInFlight {
		state.mu.Unlock()
		slog.Debug("message recall fallback probe skipped; probe already in flight",
			"platform", platform.Name(),
			"msg_id", messageID,
			"session", sessionKey,
		)
		return false
	}
	now := time.Now()
	if state.lastRecallProbeMessageID == messageID && now.Sub(state.lastRecallProbeAt) < messageRecallProbeCooldown {
		nextProbeIn := messageRecallProbeCooldown - now.Sub(state.lastRecallProbeAt)
		state.mu.Unlock()
		slog.Debug("message recall fallback probe throttled",
			"platform", platform.Name(),
			"msg_id", messageID,
			"session", sessionKey,
			"next_probe_in", nextProbeIn,
		)
		return false
	}
	state.recallProbeInFlight = true
	state.lastRecallProbeMessageID = messageID
	state.lastRecallProbeAt = now
	state.mu.Unlock()

	defer func() {
		state.mu.Lock()
		if state.currentMessageID == messageID {
			state.recallProbeInFlight = false
		}
		state.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(e.ctx, messageRecallCheckTimeout)
	defer cancel()
	recalled, err := detector.IsMessageRecalled(ctx, replyCtx)
	if err != nil {
		slog.Debug("message recall fallback check failed",
			"platform", platform.Name(),
			"msg_id", messageID,
			"session", sessionKey,
			"error", err,
		)
		return false
	}
	if !recalled {
		return false
	}
	if e.stopInteractiveSessionSilently(sessionKey) {
		slog.Info("active message recalled by fallback probe; session stopped",
			"platform", platform.Name(),
			"msg_id", messageID,
			"session", sessionKey,
		)
		return true
	}
	return false
}

// steerBusySession injects a new user message into the in-flight turn. The
// acknowledgement is deliberately sent before the protocol round-trip so the
// platform handler returns immediately instead of falling back to the queue
// when an agent is slow to acknowledge steering.
func (e *Engine) steerBusySession(p Platform, msg *Message, session *Session, agent Agent, sessions *SessionManager, interactiveKey, workspaceDir string) bool {
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return false
	}

	state.mu.Lock()
	agentSession := state.agentSession
	state.mu.Unlock()
	if agentSession == nil || !agentSession.Alive() {
		return false
	}
	steerer, ok := agentSession.(AgentSessionSteerer)
	if !ok {
		return false
	}

	// A steer is accepted as a user message in the same way as a fresh turn,
	// but it must not take the busy session lock or create a second event loop.
	session.TouchUserActivity()
	e.noteUserMessageAccepted(interactiveKey, msg.UserMessageTimeMs)
	runMessageAccepted(msg)
	session.AddHistory("user", msg.Content)
	sessions.Save()
	e.reply(p, msg.ReplyCtx, e.i18n.T(MsgSteering))
	state.steerGen.Add(1)
	go func() {
		if err := steerer.Steer(msg.Content, msg.MessageID, msg.Images, msg.Files); err != nil {
			slog.Warn("steering: inject user message failed",
				"session_key", interactiveKey, "error", err)
		}
	}()
	return true
}

func (e *Engine) waitForSessionLock(session *Session, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if session.TryLock() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-e.ctx.Done():
			return false
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func (e *Engine) startMessageRecallMonitor(sessionKey string) context.CancelFunc {
	ctx, cancel := context.WithCancel(e.ctx)
	go func() {
		ticker := time.NewTicker(messageRecallPollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if e.stopCurrentMessageIfRecalled(sessionKey) {
					return
				}
			}
		}
	}()
	return cancel
}

func (e *Engine) handleMessage(p Platform, msg *Message) {
	// Synchronous paths (validation failures and ordinary commands) release a
	// serial queue slot on return. Async agent paths transfer that ownership to
	// processInteractiveMessageWith before their goroutine is started.
	defer func() {
		if msg != nil && !msg.serialAsync {
			msg.finishSerialCompletion()
		}
	}()
	if msg.Recalled {
		e.handleMessageRecall(p, msg)
		return
	}

	slog.Info("message received",
		"platform", msg.Platform, "msg_id", msg.MessageID,
		"session", msg.SessionKey, "user", msg.UserName,
		"content_len", len(msg.Content),
		"has_images", len(msg.Images) > 0, "has_audio", msg.Audio != nil, "has_files", len(msg.Files) > 0,
	)
	// DEBUG: full message content for in-depth debugging (release-gate testing).
	// Gated behind DEBUG level so production INFO logs don't leak user text.
	if slog.Default().Enabled(e.ctx, slog.LevelDebug) {
		slog.Debug("message content",
			"platform", msg.Platform, "msg_id", msg.MessageID,
			"session", msg.SessionKey, "user", msg.UserName,
			"content", previewText(msg.Content, 500),
		)
	}

	e.hooks.Emit(HookEvent{
		Event:      HookEventMessageReceived,
		SessionKey: msg.SessionKey,
		Platform:   msg.Platform,
		UserID:     msg.UserID,
		UserName:   msg.UserName,
		Content:    msg.Content,
	})

	// Voice message: transcribe to text first
	if msg.Audio != nil {
		// If STT is configured, use it for transcription (more accurate)
		if e.speech.Enabled && e.speech.STT != nil {
			e.handleVoiceMessage(p, msg)
			return
		}
		// Fallback: use platform-provided recognition text if available
		if msg.Content == "" {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgVoiceNotEnabled))
			return
		}
		// Use platform recognition with a hint, then continue processing
		slog.Info("using platform-provided voice recognition",
			"platform", msg.Platform, "content_len", len(msg.Content))
		if msg.FromVoice {
			// Use platform name as parameter for the message
			// Capitalize first letter for better presentation
			if platformName := msg.Platform; len(platformName) > 0 {
				// Safe capitalization that handles multi-word names
				r := []rune(platformName)
				if len(r) > 0 {
					r[0] = []rune(strings.ToUpper(string(r[0])))[0]
				}
				platformName = string(r)
				e.send(p, msg.ReplyCtx, e.i18n.Tf(MsgVoiceUsingPlatformRecognition, platformName))
			}
		}
		// Continue processing with the platform-provided text content
	}

	content := strings.TrimSpace(msg.Content)
	if content == "" && msg.ExtraContent == "" && len(msg.Images) == 0 && len(msg.Files) == 0 && msg.Location == nil {
		return
	}

	// Resolve aliases on user text BEFORE merging ExtraContent, so reply
	// quotes and platform context survive alias resolution (PR #420 fix).
	content = e.resolveAlias(content)
	if msg.ExtraContent != "" {
		if content == "" {
			msg.Content = msg.ExtraContent
		} else {
			msg.Content = msg.ExtraContent + "\n" + content
		}
	} else {
		msg.Content = content
	}

	// Rate limit check (per-user role-based, then global fallback)
	if !e.checkRateLimit(msg) {
		slog.Info("message rate limited",
			"session", msg.SessionKey, "user_id", msg.UserID, "user", msg.UserName)
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgRateLimited))
		return
	}

	// Banned words check (skip for slash commands and ! shell shortcut)
	if !strings.HasPrefix(content, "/") && !strings.HasPrefix(content, "!") {
		if word := e.matchBannedWord(content); word != "" {
			slog.Info("message blocked by banned word", "word", word, "user", msg.UserName)
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgBannedWordBlocked))
			return
		}
	}

	// send --cwd work_dir resolution
	var wsAgent Agent
	var wsSessions *SessionManager
	var resolvedWorkspace string
	if forcedWorkDir := e.sendWorkDirForSession(msg.SessionKey); forcedWorkDir != "" {
		e.bindSendWorkDir(msg.SessionKey, forcedWorkDir)
		var err error
		wsAgent, wsSessions, err = e.getOrCreateWorkspaceAgent(forcedWorkDir)
		if err != nil {
			slog.Error("failed to create send work_dir agent", "work_dir", forcedWorkDir, "err", err)
			e.reply(p, msg.ReplyCtx, fmt.Sprintf("Failed to initialize workspace: %v", err))
			return
		}
		resolvedWorkspace = forcedWorkDir
	}

	// Select session manager and agent based on workspace mode
	sessions := e.sessions
	agent := e.agent
	interactiveKey := msg.SessionKey
	if resolvedWorkspace != "" && wsSessions != nil {
		sessions = wsSessions
		agent = wsAgent
		interactiveKey = resolvedWorkspace + ":" + msg.SessionKey
	}

	if len(msg.Images) == 0 && strings.HasPrefix(content, "/") {
		if e.handleCommand(p, msg, content) {
			return
		}
		// Unrecognized slash command — fall through to agent as normal message
	}

	// Permission responses bypass the session lock.
	// Must be after workspace resolution so interactiveKey is correct.
	if e.handlePendingPermission(p, msg, content, interactiveKey) {
		return
	}

	// "!" prefix: treat as shell command (same as /shell)
	// Placed after permission handling so "!yes" doesn't hijack permission responses.
	if len(msg.Images) == 0 && strings.HasPrefix(content, "!") {
		shellCmd := strings.TrimSpace(content[1:])
		if shellCmd != "" {
			// Check disabled / admin just like handleCommand does for "shell"
			e.userRolesMu.RLock()
			disabledCmds := e.disabledCmds
			urm := e.userRoles
			e.userRolesMu.RUnlock()
			if urm != nil {
				if role := urm.ResolveRole(msg.UserID); role != nil {
					disabledCmds = role.DisabledCmds
				}
			}
			if disabledCmds["shell"] {
				e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandDisabled), "!"))
				return
			}
			if !e.isAdmin(msg.UserID) {
				e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAdminRequired), "!"))
				return
			}
			slog.Info("audit: command_executed",
				"user_id", msg.UserID, "platform", msg.Platform,
				"project", e.name, "command", "shell")
			e.cmdShell(p, msg, "/shell "+shellCmd)
			return
		}
	}

	// Pending provider add (card-driven multi-step flow)
	if e.handlePendingProviderAdd(p, msg, content, interactiveKey) {
		return
	}

	if e.discardStaleUserMessageIfNeeded(interactiveKey, msg) {
		return
	}

	// Feature: stage-until-text. A message carrying only images/files (no
	// text) is downloaded and staged, but NOT sent to the agent yet. It is
	// merged into the next text message from the same session. This must
	// run before the busy/idle dispatch below — staging never touches the
	// agent-busy lock, so multiple platform-level attachment messages
	// arriving in quick succession (e.g. Telegram album photos, each its
	// own core.Message) no longer race for the session lock.
	//
	// Staging is only for an idle agent: when the agent is actively running
	// a turn, a file/image is a real new input — it should be inserted
	// directly as a new turn (interrupt-and-retry for cancellable agents,
	// or the busy queue otherwise), not held back waiting for a text
	// message. The busy check below is advisory (session state can change
	// between here and TryLock); it only decides stage-vs-insert, never
	// takes the session lock, and a stale read simply means the attachments
	// get staged and merged into the next text message as today.
	if strings.TrimSpace(msg.Content) == "" && (len(msg.Images) > 0 || len(msg.Files) > 0) {
		if s := sessions.GetActive(msg.SessionKey); s == nil || !s.Busy() {
			e.stageAttachments(interactiveKey, p, msg.ReplyCtx, msg.Images, msg.Files)
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgAttachmentStaged))
			return
		}
	}
	if staged := e.takeStagedAttachments(interactiveKey); staged != nil {
		msg.Images = append(append([]ImageAttachment{}, staged.images...), msg.Images...)
		msg.Files = append(append([]FileAttachment{}, staged.files...), msg.Files...)
	}

	session := sessions.GetOrCreateActive(msg.SessionKey)
	sessions.UpdateUserMeta(msg.SessionKey, msg.UserName, msg.ChatName)
	// Ensure an interactiveState entry exists before taking the session lock.
	// Without this, concurrent messages can observe the session as busy during
	// startup but still find no state to queue into.
	e.ensureInteractiveStateForQueueing(interactiveKey, p, msg.ReplyCtx)
	if !session.TryLock() {
		if e.stopCurrentMessageIfRecalled(interactiveKey) {
			if e.waitForSessionLock(session, recalledStopLockWait) {
				goto sessionLocked
			}
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPreviousProcessing))
			return
		}

		// Cancellable agents receive a steering request immediately. The
		// follow-up is started in the background once the interrupted turn
		// releases its lock, rather than being downgraded to the normal queue
		// when cancellation is slow.
		if e.steerBusySession(p, msg, session, agent, sessions, interactiveKey, resolvedWorkspace) {
			return
		}

		// Session is busy — try to queue the message for the running turn
		// so the agent processes it immediately after the current turn ends.
		if e.queueMessageForBusySession(p, msg, interactiveKey) {
			// Race guard: the drain loop in processInteractiveMessageWith may
			// have just finished (session unlocked) between our TryLock failure
			// and the queue append. Re-try TryLock — if it succeeds, no one is
			// draining the queue so we must start a processor ourselves.
			if session.TryLock() {
				go e.drainOrphanedQueue(session, sessions, interactiveKey, agent, resolvedWorkspace)
			}
			return
		}
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPreviousProcessing))
		return
	}

sessionLocked:
	e.startLockedInteractiveMessage(p, msg, session, agent, sessions, interactiveKey, resolvedWorkspace)
}

// startLockedInteractiveMessage records and starts a turn for a session whose
// lock is already held. It is shared by the ordinary idle path and the async
// steering path.
func (e *Engine) startLockedInteractiveMessage(p Platform, msg *Message, session *Session, agent Agent, sessions *SessionManager, interactiveKey, resolvedWorkspace string) {
	if rotated := e.maybeAutoResetSessionOnIdle(p, msg, sessions, interactiveKey, session); rotated != nil {
		session = rotated
	}
	// Record that a real user message is being processed. This keeps
	// LastUserActivity separate from UpdatedAt (bumped by every Unlock), so
	// reset_on_idle_mins is not defeated by heartbeats or unsolicited agent
	// output running between user messages (#1115).
	session.TouchUserActivity()

	// Ensure an interactiveState entry exists before launching the async
	// processor so messages arriving during session startup can be queued
	// instead of dropped (issue #565). This is still needed after idle auto-
	// reset because cleanupInteractiveState may remove the early placeholder.
	e.ensureInteractiveStateForQueueing(interactiveKey, p, msg.ReplyCtx)
	e.noteUserMessageAccepted(interactiveKey, msg.UserMessageTimeMs)
	runMessageAccepted(msg)
	slog.Debug("user message accepted for processing",
		"platform", msg.Platform,
		"session", msg.SessionKey,
		"interactive_key", interactiveKey,
		"msg_id", msg.MessageID,
		"msg_create_time_ms", msg.UserMessageTimeMs,
	)

	slog.Info("processing message",
		"platform", msg.Platform,
		"user", msg.UserName,
		"session", session.ID,
	)

	msg.deferSerialCompletion()
	go e.processInteractiveMessageWith(p, msg, session, agent, sessions, interactiveKey, resolvedWorkspace, msg.SessionKey)
}

func runMessageAccepted(msg *Message) {
	if msg == nil || msg.OnAccepted == nil {
		return
	}
	callback := msg.OnAccepted
	msg.OnAccepted = nil
	callback()
}

func (e *Engine) maybeAutoResetSessionOnIdle(p Platform, msg *Message, sessions *SessionManager, interactiveKey string, session *Session) *Session {
	if e.resetOnIdle <= 0 || session == nil {
		return nil
	}

	hasBackend := session.GetAgentSessionID() != ""
	hasHistory := len(session.GetHistory(1)) > 0
	if !hasBackend && !hasHistory {
		return nil
	}

	// Prefer LastUserActivity for idle tracking: it is only updated on actual
	// user messages, so heartbeats and unsolicited agent output don't prevent
	// idle reset from firing (#1115). Fall back to UpdatedAt for sessions
	// created before this field was introduced.
	lastActive := session.GetLastUserActivity()
	if lastActive.IsZero() {
		lastActive = session.GetUpdatedAt()
	}
	// Issue #1731: when the user has explicitly chosen this session via
	// /switch (or any other intentional selection), the idle baseline is the
	// explicit-activation time, not the last message in the session — otherwise
	// the first message after /switch into a long-idle session would be
	// wrongly rotated away. ExplicitActivationTTL caps how long that
	// exemption can last so a long-abandoned session cannot permanently
	// occupy the active slot.
	if explicitAt := session.GetExplicitActivatedAt(); !explicitAt.IsZero() {
		switch {
		case lastActive.IsZero() || explicitAt.After(lastActive):
			lastActive = explicitAt
		case time.Since(explicitAt) > ExplicitActivationTTL:
			// Explicit activation has expired — fall back to the standard
			// idle baseline so the session can finally be rotated.
			// lastActive is already set above.
		}
	}
	if lastActive.IsZero() || time.Since(lastActive) < e.resetOnIdle {
		return nil
	}

	slog.Info("auto-resetting idle session",
		"session_key", msg.SessionKey,
		"session_id", session.ID,
		"idle_for", time.Since(lastActive),
		"threshold", e.resetOnIdle,
	)

	// Check if the old session has an agent process that needs graceful
	// shutdown. If so, tell the user we're wrapping up before blocking.
	e.interactiveMu.Lock()
	state, hasState := e.interactiveStates[interactiveKey]
	hasAgent := hasState && state != nil && state.agentSession != nil && state.agentSession.Alive()
	e.interactiveMu.Unlock()

	if hasAgent {
		// Notify the user before the potentially long close. The close
		// returns as soon as the process exits (usually seconds), but
		// Stop hooks can take up to 120s.
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgSessionClosingGraceful))
	}

	e.cleanupInteractiveState(interactiveKey)
	session.UnlockWithoutUpdate()

	newSession := sessions.NewSession(msg.SessionKey, "")
	if !newSession.TryLock() {
		slog.Error("failed to lock new session after idle auto-reset", "session_key", msg.SessionKey, "new_session", newSession.ID)
		return nil
	}

	e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgSessionAutoResetIdle, int(e.resetOnIdle/time.Minute)))
	return newSession
}

// queueMessageForBusySession queues a message for later delivery when the
// session is busy. The message is NOT sent to agent stdin at queue time;
// the event loop sends it after the current turn's EventResult is received.
// Returns true if the message was successfully queued, false otherwise.
func (e *Engine) queueMessageForBusySession(p Platform, msg *Message, interactiveKey string) bool {
	e.interactiveMu.Lock()
	state, hasState := e.interactiveStates[interactiveKey]
	if !hasState || state == nil {
		e.interactiveMu.Unlock()
		return false
	}
	// Keep interactiveMu until state.mu is held. Otherwise a starting session can
	// replace a placeholder state between lookup and queue append, losing the
	// queued message before adoptPendingFromPlaceholder sees it.
	state.mu.Lock()
	e.interactiveMu.Unlock()
	defer state.mu.Unlock()

	// Allow queueing when agentSession is nil (session is starting up,
	// issue #565). Only reject if the session was established and died.
	if state.agentSession != nil && !state.agentSession.Alive() {
		return false
	}

	// Only queue metadata — do NOT send to agent stdin yet.
	// The agent CLI may treat a mid-turn stdin message as part of the
	// current turn, causing the event loop to hang waiting for a second
	// EventResult that never arrives. Instead, the event loop sends the
	// message after the current turn's EventResult is received.
	if e.isStaleUserMessageLocked(state, msg.UserMessageTimeMs) {
		snap := userMessageWatermarkSnapshotLocked(state)
		e.logStaleUserMessageDropped("reject_before_queue", msg, interactiveKey, snap)
		return true
	}
	if len(state.pendingMessages) >= e.maxQueuedMessages {
		depth := len(state.pendingMessages)
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgQueueFull), depth))
		return true // handled: queue-full reply sent
	}
	state.pendingMessages = append(state.pendingMessages, queuedMessage{
		messageID:         msg.MessageID,
		platform:          p,
		replyCtx:          msg.ReplyCtx,
		content:           msg.Content,
		images:            msg.Images,
		files:             msg.Files,
		fromVoice:         msg.FromVoice,
		userID:            msg.UserID,
		userName:          msg.UserName,
		msgPlatform:       msg.Platform,
		msgSessionKey:     msg.SessionKey,
		channelKey:        msg.ChannelKey,
		userMessageTimeMs: msg.UserMessageTimeMs,
	})
	runMessageAccepted(msg)
	queueDepth := len(state.pendingMessages)

	slog.Debug("user message accepted into queue",
		"session", msg.SessionKey,
		"interactive_key", interactiveKey,
		"msg_id", msg.MessageID,
		"msg_create_time_ms", msg.UserMessageTimeMs,
		"queue_depth", queueDepth,
	)

	slog.Info("message queued for busy session",
		"session", msg.SessionKey,
		"user", msg.UserName,
		"queue_depth", queueDepth,
	)
	e.reply(p, msg.ReplyCtx, e.i18n.T(MsgMessageQueued))
	return true
}

// ensureInteractiveStateForQueueing creates a placeholder interactiveState
// entry if none exists. This allows messages arriving while the agent session
// is still starting up to be queued instead of dropped (issue #565).
// The placeholder has agentSession==nil; getOrCreateInteractiveStateWith will
// replace it with a fully initialized state once the agent process is spawned.
func (e *Engine) ensureInteractiveStateForQueueing(key string, p Platform, replyCtx any) {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	if _, ok := e.interactiveStates[key]; !ok {
		e.interactiveStates[key] = &interactiveState{
			platform:         p,
			replyCtx:         replyCtx,
			eventsNeedResync: true,
		}
	}
}

// stageAttachments appends images/files to the session's pending-attachment
// record, creating a placeholder interactiveState (if none exists yet) as
// needed. The attachments are held here — not sent to the agent — until a
// text message arrives for the same interactiveKey (see handleMessage).
func (e *Engine) stageAttachments(interactiveKey string, p Platform, replyCtx any, images []ImageAttachment, files []FileAttachment) {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	if e.stagedAttachments == nil {
		e.stagedAttachments = make(map[string]*stagedAttachments)
	}
	staged := e.stagedAttachments[interactiveKey]
	if staged == nil {
		staged = &stagedAttachments{}
		e.stagedAttachments[interactiveKey] = staged
	}
	staged.images = append(staged.images, images...)
	staged.files = append(staged.files, files...)
}

// takeStagedAttachments returns and clears any staged attachments for the
// session, or nil if none are staged.
func (e *Engine) takeStagedAttachments(interactiveKey string) *stagedAttachments {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	staged := e.stagedAttachments[interactiveKey]
	delete(e.stagedAttachments, interactiveKey)
	return staged
}

// drainOrphanedQueue is called when a message was queued but the drain loop
// has already exited. It processes all pending messages in the state, similar
// to the drain loop in processInteractiveMessageWith but as a standalone
// goroutine.
func (e *Engine) drainOrphanedQueue(session *Session, sessions *SessionManager, interactiveKey string, agent Agent, workspaceDir string) {
	unlocked := false
	defer func() {
		if !unlocked {
			session.Unlock()
		}
	}()

	e.interactiveMu.Lock()
	state, hasState := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()

	if !hasState || state == nil || state.agentSession == nil || !state.agentSession.Alive() {
		if hasState && state != nil {
			e.notifyDroppedQueuedMessages(state, fmt.Errorf("agent session ended"))
		}
		return
	}

	// Stop unsolicited reader before draining — drainPendingMessages reads
	// from Events() and we must not have concurrent readers.
	e.stopUnsolicitedReader(state)

	unlocked = e.drainPendingMessages(state, session, sessions, interactiveKey)

	// Restart unsolicited reader if the session is still alive and clean.
	state.mu.Lock()
	alive := state.agentSession != nil && state.agentSession.Alive() && !state.stopped && !state.eventsNeedResync
	state.mu.Unlock()
	if alive {
		e.startUnsolicitedReader(state, session, sessions, interactiveKey, workspaceDir)
	}
}

// ──────────────────────────────────────────────────────────────
// Voice message handling
// ──────────────────────────────────────────────────────────────

func (e *Engine) handleVoiceMessage(p Platform, msg *Message) {
	if !e.speech.Enabled || e.speech.STT == nil {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgVoiceNotEnabled))
		return
	}

	audio := msg.Audio
	if NeedsConversion(audio.Format) && !HasFFmpeg() {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgVoiceNoFFmpeg))
		return
	}

	slog.Info("transcribing voice message",
		"platform", msg.Platform, "user", msg.UserName,
		"format", audio.Format, "size", len(audio.Data),
	)
	e.send(p, msg.ReplyCtx, e.i18n.T(MsgVoiceTranscribing))

	text, err := TranscribeAudio(e.ctx, e.speech.STT, audio, e.speech.Language)
	if err != nil {
		slog.Error("speech transcription failed", "error", err)
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgVoiceTranscribeFailed), err))
		return
	}

	text = strings.TrimSpace(text)
	if text == "" {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgVoiceEmpty))
		return
	}

	slog.Info("voice transcribed", "text_len", len(text))
	e.send(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgVoiceTranscribed), text))

	// Replace audio with transcribed text and re-dispatch
	msg.Audio = nil
	msg.Content = text
	msg.FromVoice = true
	e.handleMessage(p, msg)
}

// ──────────────────────────────────────────────────────────────
// Permission handling
// ──────────────────────────────────────────────────────────────

func (e *Engine) handlePendingPermission(p Platform, msg *Message, content string, interactiveKey string) bool {
	iKey := interactiveKey
	if iKey == "" {
		iKey = e.interactiveKeyForSessionKey(msg.SessionKey)
	}

	state, pending := e.lookupPending(iKey)

	if state == nil || pending == nil {
		// Stale platform-callback permission click (e.g. user tapped an old
		// "Allow"/"Deny" button after the session was reset, the bot was
		// restarted, or the card message ID was redelivered). Drop silently so
		// the synthesized "allow"/"deny" payload is not forwarded to the
		// agent as user input (issue #826). Only applies to permission
		// callbacks — plain text "allow"/"deny" from a real user falls
		// through to the normal message handler below.
		if msg.IsPermissionResponse {
			slog.Debug("dropping stale permission callback (no interactive state)",
				"session", msg.SessionKey, "content", content)
			return true
		}
		return false
	}
	if isForeignResponder(pending, msg) {
		// Another group member: button clicks are rejected, typed messages
		// continue as ordinary (queued) input instead of hijacking the answer.
		if msg.IsPermissionResponse || strings.HasPrefix(strings.TrimSpace(content), "askq:") {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgInteractionNotOwner))
			return true
		}
		return false
	}
	if pending.Plan != "" {
		return e.handlePlanResponse(p, msg, state, pending, content)
	}
	// AskUserQuestion: interpret user response as an answer, not a permission decision
	if len(pending.Questions) > 0 {
		// Reject empty or whitespace-only content: some platforms echo delivery
		// receipts or read-notifications as zero-length messages, and they must
		// not be accepted as answers — otherwise the tool gets empty answers
		// within ~500ms before the user has a chance to respond (#1086).
		if strings.TrimSpace(content) == "" {
			return false
		}

		curIdx := pending.CurrentQuestion
		if qIdx, _, ok := parseAskqCallback(content); ok && qIdx != curIdx {
			return true // click on an earlier question's buttons
		}
		q := pending.Questions[curIdx]
		answer := e.resolveAskQuestionAnswer(q, content)

		if pending.Answers == nil {
			pending.Answers = make(map[int]string)
		}
		pending.Answers[curIdx] = answer

		// More questions remaining — advance to next and send new card
		if curIdx+1 < len(pending.Questions) {
			pending.CurrentQuestion = curIdx + 1
			e.reply(p, msg.ReplyCtx, fmt.Sprintf("%s: **%s**", q.Question, answer))
			e.sendAskQuestionPrompt(p, msg.ReplyCtx, pending.Questions, curIdx+1)
			return true
		}

		// All questions answered — build response and resolve
		updatedInput := buildAskQuestionResponse(pending.ToolInput, pending.Questions, pending.Answers)

		if err := state.agentSession.RespondPermission(pending.RequestID, PermissionResult{
			Behavior:     "allow",
			UpdatedInput: updatedInput,
		}); err != nil {
			slog.Error("failed to send AskUserQuestion response", "error", err)
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgError), err))
		} else {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf("%s: **%s**", q.Question, answer))
		}

		state.mu.Lock()
		state.pending = nil
		state.mu.Unlock()
		pending.resolve()
		return true
	}

	lower := strings.ToLower(strings.TrimSpace(content))
	// Numeric shortcuts shown in the text prompt.
	switch lower {
	case "1":
		lower = "allow"
	case "2":
		lower = "deny"
	case "3":
		lower = "allow all"
	}

	if isApproveAllResponse(lower) {
		state.mu.Lock()
		state.approveAll = true
		state.mu.Unlock()

		if err := state.agentSession.RespondPermission(pending.RequestID, PermissionResult{
			Behavior:     "allow",
			UpdatedInput: pending.ToolInput,
		}); err != nil {
			slog.Error("failed to send permission response", "error", err)
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgError), err))
		} else {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPermissionApproveAll))
		}
	} else if isAllowResponse(lower) {
		if err := state.agentSession.RespondPermission(pending.RequestID, PermissionResult{
			Behavior:     "allow",
			UpdatedInput: pending.ToolInput,
		}); err != nil {
			slog.Error("failed to send permission response", "error", err)
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgError), err))
		} else {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPermissionAllowed))
		}
	} else if isDenyResponse(lower) {
		denyMsg := "User denied this tool use."
		if len(splitPermissionTokens(lower)) > 1 && !msg.IsPermissionResponse {
			// "no, use rg instead" — pass the rest on as guidance.
			denyMsg = "The user denied this tool use and said: " + strings.TrimSpace(content)
		}
		if err := state.agentSession.RespondPermission(pending.RequestID, PermissionResult{
			Behavior: "deny",
			Message:  denyMsg,
		}); err != nil {
			slog.Error("failed to send deny response", "error", err)
		}
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPermissionDenied))
	} else {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPermissionHint))
		return true
	}

	state.mu.Lock()
	state.pending = nil
	state.mu.Unlock()
	pending.resolve()

	return true
}

// lookupPending returns the interactive state and its pending permission for
// the given key, or nil/nil if the state is absent or has no pending. Caller
// must NOT hold interactiveMu.
func (e *Engine) lookupPending(iKey string) (*interactiveState, *pendingPermission) {
	e.interactiveMu.Lock()
	state := e.interactiveStates[iKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return nil, nil
	}
	state.mu.Lock()
	pending := state.pending
	state.mu.Unlock()
	return state, pending
}

// resolveAskQuestionAnswer converts user input into answer text.
// It handles button callbacks ("askq:qIdx:optIdx"), numeric selections ("1", "1,3"), and free text.
func (e *Engine) resolveAskQuestionAnswer(q UserQuestion, input string) string {
	input = strings.TrimSpace(input)

	// Handle card button callback: "askq:qIdx:optIdx"
	if strings.HasPrefix(input, "askq:") {
		parts := strings.SplitN(input, ":", 3)
		if len(parts) == 3 {
			if idx, err := strconv.Atoi(parts[2]); err == nil && idx >= 1 && idx <= len(q.Options) {
				return q.Options[idx-1].Label
			}
			// Multi-select "askq:qIdx:1,3" from toggle buttons.
			if _, opts, ok := parseAskqCallback(input); ok {
				var labels []string
				for _, idx := range opts {
					if idx >= 1 && idx <= len(q.Options) {
						labels = append(labels, q.Options[idx-1].Label)
					}
				}
				if len(labels) > 0 {
					return strings.Join(labels, ", ")
				}
			}
		}
		// Legacy format "askq:N"
		if len(parts) == 2 {
			if idx, err := strconv.Atoi(parts[1]); err == nil && idx >= 1 && idx <= len(q.Options) {
				return q.Options[idx-1].Label
			}
		}
	}

	// Try numeric index(es)
	if q.MultiSelect {
		parts := strings.FieldsFunc(input, func(r rune) bool { return r == ',' || r == '，' || r == ' ' })
		var labels []string
		allNumeric := true
		for _, p := range parts {
			p = strings.TrimSpace(p)
			idx, err := strconv.Atoi(p)
			if err != nil || idx < 1 || idx > len(q.Options) {
				allNumeric = false
				break
			}
			labels = append(labels, q.Options[idx-1].Label)
		}
		if allNumeric && len(labels) > 0 {
			return strings.Join(labels, ", ")
		}
	} else {
		if idx, err := strconv.Atoi(input); err == nil && idx >= 1 && idx <= len(q.Options) {
			return q.Options[idx-1].Label
		}
	}

	return input
}

// buildAskQuestionResponse constructs the updatedInput for AskUserQuestion control_response.
func buildAskQuestionResponse(originalInput map[string]any, questions []UserQuestion, collected map[int]string) map[string]any {
	result := make(map[string]any)
	for k, v := range originalInput {
		result[k] = v
	}
	answers := make(map[string]any)
	for idx, ans := range collected {
		if idx >= 0 && idx < len(questions) {
			answers[questions[idx].Question] = ans
		}
	}
	result["answers"] = answers
	return result
}

// permissionTokenSeparator reports whether r is a token boundary for
// permission keyword matching. It splits on Unicode whitespace plus the
// punctuation users typically write around an @mention or as filler in
// a chat reply, including full-width / Chinese variants.
//
// `@` is intentionally a separator so `@bot 允许` tokenises to
// ["bot", "允许"] without the keyword swallowing the mention prefix.
//
// We deliberately do not split on every Unicode punctuation rune (e.g. `'`
// inside contractions) — only the ones that show up in real IM messages
// — so unrelated words stay intact.
func permissionTokenSeparator(r rune) bool {
	if unicode.IsSpace(r) {
		return true
	}
	switch r {
	case '@', '＠',
		',', '，',
		'.', '。',
		'!', '！',
		'?', '？',
		':', '：',
		';', '；',
		'(', ')', '（', '）',
		'[', ']', '【', '】',
		'"', '\'', '“', '”', '‘', '’',
		'、', '·':
		return true
	}
	return false
}

// splitPermissionTokens lower-cases s and tokenises it on
// permissionTokenSeparator boundaries. Returns nil for the empty input
// so callers can range over it without a length check.
func splitPermissionTokens(s string) []string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return nil
	}
	return strings.FieldsFunc(s, permissionTokenSeparator)
}

// matchPermissionKeyword reports whether s contains any of the
// single-token keywords in `keywords`, OR equals any of the
// multi-token phrases in `phrases` (matched by joining the tokens
// with a single ASCII space, which canonicalises full-width spacing
// and adjacent @mentions).
//
// Single-token keywords are matched per-token (strict equality on a
// token boundary) so that "@bot 允许", "允许 @bot", "好的 allow" all
// match while "禁止允许这种" does NOT (it tokenises to a single
// 4-character CJK word, no token equals "允许"). Multi-token phrases
// are matched as a sequence so "@bot 允许 所有" still hits
// "允许 所有" without falling back to per-token "允许" — this is what
// keeps `/approve all` distinct from `/allow`.
func matchPermissionKeyword(s string, phrases []string, keywords []string) bool {
	tokens := splitPermissionTokens(s)
	if len(tokens) == 0 {
		return false
	}
	if len(phrases) > 0 {
		joined := strings.Join(tokens, " ")
		for _, ph := range phrases {
			if joined == ph {
				return true
			}
			// Sliding window over tokens to allow extra surrounding
			// tokens like "@bot allow all please".
			pTokens := strings.Fields(ph)
			if len(pTokens) > 1 && len(pTokens) <= len(tokens) {
				for i := 0; i+len(pTokens) <= len(tokens); i++ {
					match := true
					for j, pt := range pTokens {
						if tokens[i+j] != pt {
							match = false
							break
						}
					}
					if match {
						return true
					}
				}
			}
		}
	}
	for _, tok := range tokens {
		for _, w := range keywords {
			if tok == w {
				return true
			}
		}
	}
	return false
}

// approveAllPhrases / approveAllSingleTokens / allowKeywords / denyKeywords
// are the recognised vocabularies for permission responses. Kept as
// package-level vars so the test suite can assert the exact lists.
var (
	approveAllPhrases = []string{
		"allow all", "approve all", "yes all",
	}
	approveAllSingleTokens = []string{
		"allowall",
		"允许所有", "允许全部", "全部允许", "所有允许", "都允许", "全部同意",
	}
	allowKeywords = []string{
		"allow", "yes", "y", "ok", "approve",
		"允许", "同意", "可以", "好", "好的", "是", "确认",
	}
	denyKeywords = []string{
		"deny", "no", "n", "reject", "cancel",
		"拒绝", "不允许", "不行", "不", "否", "取消",
	}
)

// isApproveAllResponse reports whether s expresses "allow all" intent.
// Multi-token phrases (e.g. "allow all") are matched first; single-token
// CJK forms (e.g. "允许所有") are matched per-token so a leading or
// trailing @mention does not break recognition.
func isApproveAllResponse(s string) bool {
	return matchPermissionKeyword(s, approveAllPhrases, approveAllSingleTokens)
}

// isAllowResponse reports whether s expresses an "allow" intent.
// Tokenised so that group-chat replies like "@产品经理 允许" still match
// — see internal task t-20260614-ayc85z.
func isAllowResponse(s string) bool {
	return matchPermissionKeyword(s, nil, allowKeywords)
}

// isDenyResponse reports whether s expresses a "deny" intent.
// Tokenised so that group-chat replies like "@产品经理 拒绝" still match.
func isDenyResponse(s string) bool {
	return matchPermissionKeyword(s, nil, denyKeywords)
}

// ──────────────────────────────────────────────────────────────
// Interactive agent processing
// ──────────────────────────────────────────────────────────────

func (e *Engine) processInteractiveMessage(p Platform, msg *Message, session *Session) {
	e.processInteractiveMessageWith(p, msg, session, e.agent, e.sessions, msg.SessionKey, "", "")
}

// processInteractiveMessageWith is the core interactive processing loop.
// It accepts an explicit agent, interactiveKey (for the interactiveStates map),
// and workspaceDir so that multi-workspace mode can route to per-workspace agents.
// ccSessionKey, when non-empty, is used for AGENT_BRIDGE_SESSION_KEY in the agent env; otherwise interactiveKey is used.
func (e *Engine) processInteractiveMessageWith(p Platform, msg *Message, session *Session, agent Agent, sessions *SessionManager, interactiveKey string, workspaceDir string, ccSessionKey string) {
	// session.Unlock() is NOT deferred here — it is called explicitly in
	// the drain loop below while holding state.mu to close the race window
	// between "queue is empty" and "session unlocked". A deferred fallback
	// ensures the lock is released on early-return paths.
	unlocked := false
	defer func() {
		if !unlocked {
			session.Unlock()
		}
		msg.finishSerialCompletion()
	}()

	if e.ctx.Err() != nil {
		return
	}

	turnStart := time.Now()

	session.AddHistory("user", msg.Content)
	// Persist user message immediately so crashes between user input and
	// assistant reply don't lose it (the assistant-side Save below depends
	// on the turn completing without a process crash).
	sessions.Save()

	// Use the agent override when available (multi-workspace mode)
	var agentOverride Agent
	if agent != e.agent {
		agentOverride = agent
	}
	state := e.getOrCreateInteractiveStateWith(interactiveKey, p, msg.ReplyCtx, session, sessions, agentOverride, ccSessionKey)

	// Set workspaceDir on the state for idle reaper identification
	if workspaceDir != "" {
		state.mu.Lock()
		state.workspaceDir = workspaceDir
		state.mu.Unlock()
	}

	// Update reply context for this turn
	state.mu.Lock()
	if state.currentMessageID != msg.MessageID {
		state.lastRecallProbeMessageID = ""
		state.lastRecallProbeAt = time.Time{}
		state.recallProbeInFlight = false
	}
	state.platform = p
	state.replyCtx = msg.ReplyCtx
	state.currentMessageID = msg.MessageID
	state.currentTurnUserMessageTimeMs = msg.UserMessageTimeMs
	state.mu.Unlock()
	stopRecallMonitor := e.startMessageRecallMonitor(interactiveKey)
	defer stopRecallMonitor()

	if state.agentSession == nil {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgFailedToStartAgentSession))
		return
	}
	e.cancelAgentSessionIdleClose(state)

	if workspaceDir != "" && e.workspacePool != nil {
		ws := e.workspacePool.GetOrCreate(workspaceDir)
		ws.BeginTurn()
		defer ws.EndTurn()
	}

	// Apply per-message permission mode override for this turn.
	// Defer restores only when SetLiveMode succeeds for the override.
	if msg.ModeOverride != "" {
		if switcher, ok := state.agentSession.(LiveModeSwitcher); ok {
			if switcher.SetLiveMode(msg.ModeOverride) {
				defer func() {
					defaultMode := "default"
					if ma, ok := e.agent.(interface{ GetMode() string }); ok {
						if m := ma.GetMode(); m != "" {
							defaultMode = m
						}
					}
					switcher.SetLiveMode(defaultMode)
				}()
			}
		}
	}

	// Start typing indicator if platform supports it.
	// Ownership is transferred to processInteractiveEvents which manages
	// stopping/restarting it across queued message turns.
	var stopTyping func()
	if ti, ok := p.(TypingIndicator); ok {
		stopTyping = ti.StartTyping(e.ctx, msg.ReplyCtx)
	}
	defer func() {
		// Stop typing if ownership was NOT transferred to processInteractiveEvents
		// (i.e. an early return before that call).
		if stopTyping != nil {
			stopTyping()
		}
	}()

	// Stop the unsolicited reader (if running) and hand off event channel
	// ownership to this foreground turn. Only drain events when the previous
	// turn ended abnormally (eventsNeedResync=true, the default).
	e.stopUnsolicitedReader(state)
	state.mu.Lock()
	needResync := state.eventsNeedResync
	state.mu.Unlock()
	if needResync {
		drainEvents(state.agentSession.Events())
	}

	promptContent := e.buildSenderPrompt(msg.Content, msg.UserID, msg.UserName, msg.Platform, msg.SessionKey, msg.ChannelKey)

	sendStart := time.Now()
	state.mu.Lock()
	state.currentMessageID = msg.MessageID
	state.currentUserID = msg.UserID
	state.fromVoice = msg.FromVoice
	state.sideText = ""
	as := state.agentSession // capture under lock to avoid race with cleanup
	state.mu.Unlock()

	// Run Send concurrently with processInteractiveEvents. Some agents block inside
	// Send until the prompt turn finishes (e.g. ACP session/prompt); they may emit
	// EventPermissionRequest while blocked — the event loop must run in parallel.
	sendDone := make(chan error, 1)
	go func() {
		if as == nil {
			sendDone <- fmt.Errorf("agent session became nil")
			return
		}
		sendDone <- as.Send(promptContent, msg.MessageID, msg.Images, msg.Files)
	}()

	e.processInteractiveEvents(state, session, sessions, interactiveKey, msg.MessageID, turnStart, stopTyping, sendDone, msg.ReplyCtx)
	if elapsed := time.Since(sendStart); elapsed >= slowAgentSend {
		slog.Warn("slow agent send", "elapsed", elapsed, "session", msg.SessionKey, "content_len", len(msg.Content))
	}
	stopTyping = nil // ownership transferred; prevent defer from double-stopping

	// Start unsolicited reader and arm the idle close timer BEFORE draining
	// queued messages. drainPendingMessages releases the session lock, and
	// without this ordering the next user message can race in, call
	// cancelAgentSessionIdleClose (a no-op since nothing was scheduled yet),
	// and then the late schedule below arms a timer that no subsequent cancel
	// will catch — closing the live session mid-turn. See #1686 P1-C P1-2.
	// The schedule's own state checks (agentSession nil, stopped, etc.) and
	// cleanupInteractiveStateForIdleToken's stale-token guard make it safe to
	// leave a scheduled timer running across drain.
	state.mu.Lock()
	alive := state.agentSession != nil && state.agentSession.Alive() && !state.stopped && !state.eventsNeedResync
	state.mu.Unlock()
	if alive {
		e.startUnsolicitedReader(state, session, sessions, interactiveKey, workspaceDir)
		e.scheduleAgentSessionIdleClose(interactiveKey, state)
	}

	// Guard against a narrow race: a message may have been queued between
	// processInteractiveEvents observing an empty queue and returning here
	// (session is still locked, so handleMessage's TryLock fails and routes
	// the message to queueMessageForBusySession). Drain any such orphans.
	if e.drainPendingMessages(state, session, sessions, interactiveKey) {
		unlocked = true
	}
}

// getOrCreateWorkspaceAgent returns (or creates) a per-workspace agent and session manager.
// workspace must be a normalized path (from resolveWorkspace or normalizeWorkspacePath).
func (e *Engine) getOrCreateWorkspaceAgent(workspace string) (Agent, *SessionManager, error) {
	e.interactiveMu.Lock()
	if e.workspacePool == nil {
		e.workspacePool = newWorkspacePool(DefaultWorkspaceIdleTimeout)
	}
	pool := e.workspacePool
	e.interactiveMu.Unlock()

	ws := pool.GetOrCreate(workspace)
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.agent != nil {
		return ws.agent, ws.sessions, nil
	}

	// Create a new agent instance with this workspace's work_dir
	opts := make(map[string]any)
	// Let the agent seed its own base options (e.g. tmux session name)
	if snapshotter, ok := e.agent.(WorkspaceAgentOptionSnapshotter); ok {
		for k, v := range snapshotter.WorkspaceAgentOptions() {
			opts[k] = v
		}
	}

	// workspace-specific overrides always win
	opts["work_dir"] = workspace

	// Copy model from original agent if possible
	if _, ok := opts["model"]; !ok {
		if ma, ok := e.agent.(interface{ GetModel() string }); ok {
			if m := ma.GetModel(); m != "" {
				opts["model"] = m
			}
		}
	}
	// Copy permission mode
	if _, ok := opts["mode"]; !ok {
		if ma, ok := e.agent.(interface{ GetMode() string }); ok {
			if m := ma.GetMode(); m != "" {
				opts["mode"] = m
			}
		}
	}

	agent, err := CreateAgent(e.agent.Name(), opts)
	if err != nil {
		return nil, nil, fmt.Errorf("create workspace agent for %s: %w", workspace, err)
	}

	// Wire providers if original agent has them
	if ps, ok := e.agent.(ProviderSwitcher); ok {
		if ps2, ok2 := agent.(ProviderSwitcher); ok2 {
			ps2.SetProviders(ps.ListProviders())
			if active := ps.GetActiveProvider(); active != nil && active.Name != "" {
				ps2.SetActiveProvider(active.Name)
			}
		}
	}

	// Create per-workspace session manager
	h := sha256.Sum256([]byte(workspace))
	sessionFile := filepath.Join(filepath.Dir(e.sessions.StorePath()),
		fmt.Sprintf("%s_ws_%s.json", e.name, hex.EncodeToString(h[:4])))
	sessions := NewSessionManager(sessionFile)

	ws.agent = agent
	ws.sessions = sessions
	return agent, sessions, nil
}

// getOrCreateInteractiveStateWith accepts an optional agent override for multi-workspace mode.
// adoptPendingFromPlaceholder copies pendingMessages from an existing placeholder
// state to newState so queued messages are not lost when the map entry is replaced.
// Must be called under interactiveMu.
func adoptPendingFromPlaceholder(existing, newState *interactiveState) {
	if existing == nil || existing == newState {
		return
	}
	existing.mu.Lock()
	if len(existing.pendingMessages) > 0 {
		newState.pendingMessages = existing.pendingMessages
		existing.pendingMessages = nil
	}
	existing.mu.Unlock()
}

// When agentOverride is non-nil it is used instead of e.agent to start the session.
// ccSessionKey, when non-empty, is used for AGENT_BRIDGE_SESSION_KEY env injection; otherwise sessionKey is used.
func (e *Engine) getOrCreateInteractiveStateWith(sessionKey string, p Platform, replyCtx any, session *Session, sessions *SessionManager, agentOverride Agent, ccSessionKey string) *interactiveState {
	// /stop removes the state from the map immediately but tears the process
	// down in the background. Wait that teardown out *before* taking the lock:
	// spawning now would start a second process on the same agent session ID
	// while the first one is still alive and writing to the same transcript.
	closeSettled := e.awaitSessionClose(sessionKey)

	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()

	state, ok := e.interactiveStates[sessionKey]
	if ok && state.agentSession != nil && state.agentSession.Alive() {
		// Verify the running agent session matches the current active session.
		// After /new or /switch the active session changes, but the old agent
		// process may still be alive. Reusing it would send messages to the
		// wrong conversation context.
		wantID := session.GetAgentSessionID()
		currentID := state.agentSession.CurrentSessionID()
		// Reuse only when the live process matches what the Session expects:
		// - IDs match (same Claude session), or
		// - the process has not reported an ID yet (startup; empty want is OK).
		// If wantID is empty (/new, cleared session) but the process already has
		// a concrete ID, reusing would keep --resume context — recycle (#238).
		needRecycle := currentID != "" && (wantID == "" || wantID != currentID)
		if !needRecycle {
			return state
		}
		// Tear down the stale agent so we start one that matches the Session below.
		slog.Info("interactive session mismatch, recycling",
			"session_key", sessionKey,
			"want_agent_session", wantID,
			"have_agent_session", currentID,
		)
		e.stopUnsolicitedReader(state)
		state.markStopped()
		// Close synchronously to prevent race condition where old agent
		// continues outputting while new agent starts (issue #327).
		e.closeAgentSessionWithTimeout(sessionKey, state.agentSession, state.platform, state.replyCtx)
		delete(e.interactiveStates, sessionKey)
		ok = false // prevent reading stale settings below
	}

	// Select the agent to use for this session
	agent := e.agent
	if agentOverride != nil {
		agent = agentOverride
	}

	ccKey := sessionKey
	if ccSessionKey != "" {
		ccKey = ccSessionKey
	}

	// Pass bridge context to the agent subprocess for explicit bridge commands.
	if inj, ok := agent.(SessionEnvInjector); ok {
		envVars := []string{
			"AGENT_BRIDGE_PROJECT=" + e.name,
			"AGENT_BRIDGE_SESSION_KEY=" + ccKey,
		}
		if e.dataDir != "" {
			envVars = append(envVars, "AGENT_BRIDGE_DATA_DIR="+e.dataDir)
		}
		if exePath, err := os.Executable(); err == nil {
			binDir := filepath.Dir(exePath)
			if curPath := os.Getenv("PATH"); curPath != "" {
				envVars = append(envVars, "PATH="+binDir+string(filepath.ListSeparator)+curPath)
			} else {
				envVars = append(envVars, "PATH="+binDir)
			}
		}
		inj.SetSessionEnv(envVars)
	}

	// Check if context is already canceled (e.g. during shutdown/restart)
	if e.ctx.Err() != nil {
		slog.Debug("skipping session start: context canceled", "session_key", sessionKey)
		newState := &interactiveState{platform: p, replyCtx: replyCtx, agent: agent, eventsNeedResync: true}
		adoptPendingFromPlaceholder(e.interactiveStates[sessionKey], newState)
		state = newState
		e.interactiveStates[sessionKey] = state
		return state
	}

	// Restore the agent's active provider from the session before starting a
	// new sub-process. The provider choice is persisted to disk by
	// `/provider switch`; without restoring it here, a agent-bridge process
	// restart silently drops the user's choice while keeping the resumed
	// agent_session_id, producing "model X does not exist" errors when
	// the model name is sent to the wrong base_url
	// (agent-bridge internal task t-20260614-qp7xnl).
	restoreActiveProviderFromSession(agent, session)

	// Resume only when we have a concrete saved agent session ID. If the session
	// is unbound, force a fresh start instead of attaching to whichever CLI
	// conversation happens to be "latest" in this workspace.
	startSessionID := session.GetAgentSessionID()
	// Cross-project session leakage guard (issue #599): if a session ID was
	// inherited from a different project's workspace (e.g. another
	// agent-bridge project that happens to share a Session row), the agent
	// can detect the mismatch and we should clear the ID rather than
	// resume a conversation that has nothing to do with this project.
	if startSessionID != "" {
		if validator, ok := agent.(SessionIDValidator); ok && !validator.ValidateSessionID(e.ctx, startSessionID) {
			slog.Warn("session ID does not belong to this project, clearing it",
				"session_key", sessionKey, "invalid_session_id", startSessionID)
			session.SetAgentSessionID("", agent.Name())
			sessions.Save()
			startSessionID = ""
		}
	}
	// Refuse to resume when the previous process could not be confirmed dead
	// (close reported a kill failure, timed out, or we gave up waiting for it).
	// Starting fresh loses this conversation's history, but resuming would put
	// two live agents on one transcript — both replying, both calling tools.
	if startSessionID != "" && (!closeSettled || e.consumeUnsafeResume(sessionKey)) {
		slog.Warn("previous agent process not confirmed dead, starting a fresh session instead of resuming",
			"session_key", sessionKey, "abandoned_session_id", startSessionID)
		session.SetAgentSessionID("", agent.Name())
		sessions.Save()
		startSessionID = ""
		e.send(p, replyCtx, e.i18n.T(MsgSessionResumeUnsafe))
	}

	isResume := startSessionID != ""
	startAt := time.Now()
	agentSession, err := agent.StartSession(e.ctx, startSessionID)
	startElapsed := time.Since(startAt)
	if err != nil {
		// If resume/continue failed, try a fresh session as fallback.
		if startSessionID != "" {
			slog.Error("session resume failed, falling back to fresh session",
				"session_key", sessionKey, "failed_session_id", startSessionID,
				"error", err, "elapsed", startElapsed)
			// Clear the stale session ID so CompareAndSetAgentSessionID can
			// write the new ID, matching the relay fallback at line 12640.
			session.SetAgentSessionID("", agent.Name())
			sessions.Save()
			startAt = time.Now()
			agentSession, err = agent.StartSession(e.ctx, "")
			startElapsed = time.Since(startAt)
			if err == nil {
				slog.Info("fresh session started after resume failure",
					"session_key", sessionKey, "elapsed", startElapsed)
			}
		}
		if err != nil {
			slog.Error("failed to start interactive session", "error", err, "elapsed", startElapsed)
			e.hooks.Emit(HookEvent{
				Event:      HookEventError,
				SessionKey: sessionKey,
				Platform:   p.Name(),
				Error:      fmt.Sprintf("failed to start session: %v", err),
			})
			newState := &interactiveState{platform: p, replyCtx: replyCtx, agent: agent, eventsNeedResync: true}
			adoptPendingFromPlaceholder(e.interactiveStates[sessionKey], newState)
			state = newState
			e.interactiveStates[sessionKey] = state
			return state
		}
	}
	if startElapsed >= slowAgentStart {
		slog.Warn("slow agent session start", "elapsed", startElapsed, "agent", agent.Name(), "session_id", startSessionID)
	}

	// Surface any startup warning (e.g. permission mode downgrade under root) to the IM user.
	if warner, ok := agentSession.(StartupWarner); ok {
		if msg := warner.StartupWarning(); msg != "" {
			e.send(p, replyCtx, msg)
		}
	}

	if newID := agentSession.CurrentSessionID(); newID != "" {
		// Track the latest session ID Claude reports. Each --resume forks a new
		// session_id, so the stored ID must follow the live process or a later
		// resume (after /stop or /model) would target a stale node and lose
		// context. Session naming binds only on first assignment to avoid
		// polluting sessionNames with every forked ID.
		wasEmpty := session.GetAgentSessionID() == ""
		if session.GetAgentSessionID() != newID {
			session.SetAgentSessionID(newID, agent.Name())
			if wasEmpty {
				pendingName := session.GetName()
				if pendingName != "" && pendingName != "session" && pendingName != "default" {
					sessions.SetSessionName(newID, pendingName)
				}
			}
			sessions.Save()
		}
	}

	newState := &interactiveState{
		agentSession:     agentSession,
		platform:         p,
		replyCtx:         replyCtx,
		agent:            agent,
		eventsNeedResync: true,
	}
	adoptPendingFromPlaceholder(e.interactiveStates[sessionKey], newState)
	state = newState
	e.interactiveStates[sessionKey] = state

	slog.Info("session spawned", "session_key", sessionKey, "agent_session", session.GetAgentSessionID(), "is_resume", isResume, "elapsed", startElapsed)

	e.hooks.Emit(HookEvent{
		Event:      HookEventSessionStarted,
		SessionKey: sessionKey,
		Platform:   p.Name(),
		Extra: map[string]any{
			"agent_session_id": session.GetAgentSessionID(),
			"is_resume":        isResume,
		},
	})

	return state
}

// cleanupInteractiveState removes the interactive state for the given session key
// and closes its agent session. When an expected state is provided, cleanup is
// skipped if the map entry has been replaced by a different state — this prevents
// a stale goroutine (still running after /new created a fresh Session object and
// a new turn started on it) from accidentally destroying the replacement state.
//
// IMPORTANT: The state is deleted from the map AFTER the agent session is closed
// to avoid race conditions where concurrent requests see an empty map while the
// agent session is still being shut down (which can take up to 130s for Stop hooks).
func (e *Engine) cleanupInteractiveState(sessionKey string, expected ...*interactiveState) {
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	if len(expected) > 0 && expected[0] != nil && state != expected[0] {
		// Another turn has already replaced the state — skip cleanup.
		e.interactiveMu.Unlock()
		return
	}
	// Capture the agent session and nil it out atomically to prevent a
	// concurrent cleanup (without expected) from closing the same session.
	var agentSession AgentSession
	var closePlatform Platform
	var closeReplyCtx any
	if ok && state != nil {
		state.mu.Lock()
		agentSession = state.agentSession
		state.agentSession = nil
		closePlatform = state.platform
		closeReplyCtx = state.replyCtx
		if state.agentSessionIdleCancel != nil {
			state.agentSessionIdleCancel()
			state.agentSessionIdleCancel = nil
		}
		state.mu.Unlock()
	}
	e.interactiveMu.Unlock()

	// Notify senders of any queued messages that will never be processed.
	if ok && state != nil {
		// Stop unsolicited reader before marking stopped to avoid goroutine leak.
		e.stopUnsolicitedReader(state)

		state.markStopped()

		// Resolve any pending permission so the reader goroutine (or event
		// loop) does not block on <-pending.Resolved forever.
		state.mu.Lock()
		pending := state.pending
		state.pending = nil
		state.mu.Unlock()
		if pending != nil {
			pending.resolve()
		}

		e.notifyDroppedQueuedMessages(state, fmt.Errorf("session reset"))
	}

	// Close the agent session BEFORE deleting from the map.
	// This prevents race conditions where /stop during cleanup sees
	// an empty map and reports "No execution in progress" while
	// the agent session Close() is still blocking (up to 130s).
	if agentSession != nil {
		e.closeAgentSessionWithTimeout(sessionKey, agentSession, closePlatform, closeReplyCtx)
	}

	// Now delete the state from the map after the session is closed.
	e.interactiveMu.Lock()
	// Re-check that the state hasn't been replaced during the close
	currentState, currentOk := e.interactiveStates[sessionKey]
	if currentOk && len(expected) > 0 && expected[0] != nil && currentState != expected[0] {
		// Another turn has replaced the state during our close — don't delete it.
		e.interactiveMu.Unlock()
		return
	}
	delete(e.interactiveStates, sessionKey)
	e.interactiveMu.Unlock()
}

func (e *Engine) cancelAgentSessionIdleClose(state *interactiveState) {
	if state == nil {
		return
	}
	state.mu.Lock()
	cancel := state.agentSessionIdleCancel
	state.agentSessionIdleCancel = nil
	state.agentSessionIdleToken = 0
	state.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (e *Engine) scheduleAgentSessionIdleClose(sessionKey string, state *interactiveState) {
	if state == nil {
		return
	}
	timeout := time.Duration(e.agentSessionIdleTimeoutNanos.Load())
	if timeout <= 0 {
		return
	}

	e.cancelAgentSessionIdleClose(state)
	ctx, cancel := context.WithCancel(e.ctx)
	token := e.agentSessionIdleSeq.Add(1)
	state.mu.Lock()
	if state.stopped ||
		state.agentSession == nil ||
		!state.agentSession.Alive() ||
		state.eventsNeedResync ||
		state.pending != nil ||
		len(state.pendingMessages) > 0 {
		state.mu.Unlock()
		cancel()
		return
	}
	state.agentSessionIdleCancel = cancel
	state.agentSessionIdleToken = token
	state.mu.Unlock()

	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		e.cleanupInteractiveStateForIdleToken(sessionKey, state, token, timeout)
	}()
}

func (e *Engine) cleanupInteractiveStateForIdleToken(sessionKey string, expected *interactiveState, token uint64, timeout time.Duration) {
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	if !ok || state == nil || state != expected {
		e.interactiveMu.Unlock()
		return
	}

	var agentSession AgentSession
	state.mu.Lock()
	if state.agentSessionIdleToken != token ||
		state.agentSession == nil ||
		!state.agentSession.Alive() ||
		state.stopped ||
		state.eventsNeedResync ||
		state.pending != nil ||
		len(state.pendingMessages) > 0 {
		state.mu.Unlock()
		e.interactiveMu.Unlock()
		return
	}
	agentSession = state.agentSession
	state.agentSession = nil
	state.agentSessionIdleCancel = nil
	state.agentSessionIdleToken = 0
	closePlatform := state.platform
	closeReplyCtx := state.replyCtx
	state.mu.Unlock()
	e.interactiveMu.Unlock()

	slog.Info("agent session idle timeout: closing live agent session",
		"session_key", sessionKey, "timeout", timeout)
	e.stopUnsolicitedReader(state)
	state.markStopped()

	state.mu.Lock()
	pending := state.pending
	state.pending = nil
	state.mu.Unlock()
	if pending != nil {
		pending.resolve()
	}
	e.notifyDroppedQueuedMessages(state, fmt.Errorf("session reset"))

	e.closeAgentSessionWithTimeout(sessionKey, agentSession, closePlatform, closeReplyCtx)

	e.interactiveMu.Lock()
	if currentState, currentOk := e.interactiveStates[sessionKey]; currentOk && currentState == expected {
		delete(e.interactiveStates, sessionKey)
	}
	e.interactiveMu.Unlock()
}

// beginSessionClose registers an in-flight teardown for sessionKey and returns
// the function that must be called once the teardown is over. Spawns for the
// same key wait on this registration (awaitSessionClose), so a new agent
// process is never started while the previous one may still be alive.
func (e *Engine) beginSessionClose(sessionKey string) func() {
	if sessionKey == "" {
		return func() {}
	}
	done := make(chan struct{})
	e.closingMu.Lock()
	if e.closingSessions == nil {
		e.closingSessions = make(map[string]chan struct{})
	}
	e.closingSessions[sessionKey] = done
	e.closingMu.Unlock()

	return func() {
		e.closingMu.Lock()
		// Only clear the registry if it still points at *this* teardown; an
		// overlapping close may have replaced it and is still running.
		if cur, ok := e.closingSessions[sessionKey]; ok && cur == done {
			delete(e.closingSessions, sessionKey)
		}
		e.closingMu.Unlock()
		close(done)
	}
}

// awaitSessionClose blocks until no teardown is in flight for sessionKey.
// It reports whether the wait ended cleanly; false means the previous process
// could not be confirmed gone, in which case the caller must not resume its
// agent session ID. Must NOT be called while holding interactiveMu — the
// teardown itself can take up to closeTimeout.
func (e *Engine) awaitSessionClose(sessionKey string) bool {
	if sessionKey == "" {
		return true
	}
	// Budget slightly above the teardown's own ceiling so the close path
	// always gets to resolve (and flag the session) before we give up.
	deadline := time.After(closeTimeout + 30*time.Second)
	start := time.Now()
	waited := false
	for {
		e.closingMu.Lock()
		done := e.closingSessions[sessionKey]
		e.closingMu.Unlock()
		if done == nil {
			if waited {
				slog.Info("previous session teardown finished, starting new session",
					"session_key", sessionKey, "waited", time.Since(start))
			}
			return true
		}
		if !waited {
			waited = true
			slog.Info("waiting for previous session teardown before spawning",
				"session_key", sessionKey)
		}
		select {
		case <-done:
			// Loop again: an overlapping teardown may have been registered.
		case <-deadline:
			slog.Error("timed out waiting for previous session teardown; refusing to resume its agent session",
				"session_key", sessionKey, "waited", time.Since(start))
			return false
		case <-e.ctx.Done():
			return false
		}
	}
}

// markUnsafeResume records that sessionKey's previous agent process could not
// be confirmed dead, so its agent session ID must not be resumed. Resuming it
// would attach a second live process to the same conversation — both would
// read and write the same transcript and act on it independently.
func (e *Engine) markUnsafeResume(sessionKey string) {
	if sessionKey == "" {
		return
	}
	e.closingMu.Lock()
	if e.unsafeResume == nil {
		e.unsafeResume = make(map[string]bool)
	}
	e.unsafeResume[sessionKey] = true
	e.closingMu.Unlock()
}

// consumeUnsafeResume reports (and clears) whether the next spawn for
// sessionKey must start a fresh agent session instead of resuming.
func (e *Engine) consumeUnsafeResume(sessionKey string) bool {
	if sessionKey == "" {
		return false
	}
	e.closingMu.Lock()
	defer e.closingMu.Unlock()
	if !e.unsafeResume[sessionKey] {
		return false
	}
	delete(e.unsafeResume, sessionKey)
	return true
}

func (e *Engine) closeAgentSessionAsync(sessionKey string, agentSession AgentSession, platform Platform, replyCtx any) {
	if agentSession == nil {
		return
	}
	// Register synchronously: a message arriving right after /stop must see
	// the teardown even if this goroutine has not been scheduled yet.
	finish := e.beginSessionClose(sessionKey)
	go func() {
		defer finish()
		e.closeAgentSession(sessionKey, agentSession, platform, replyCtx)
	}()
}

// closeAgentSessionWithTimeout tears down agentSession in the background and
// waits up to closeTimeout for it to finish. platform/replyCtx (the chat that
// owned the session, if known) are used to alert the user if the underlying
// OS process cannot be confirmed dead — either because Close() itself
// reports a kill failure, or because it does not return within the budget
// at all. In both cases the process may still be running with the session's
// original credentials, so silence here is worse than a noisy warning.
func (e *Engine) closeAgentSessionWithTimeout(sessionKey string, agentSession AgentSession, platform Platform, replyCtx any) {
	if agentSession == nil {
		return
	}
	finish := e.beginSessionClose(sessionKey)
	defer finish()
	e.closeAgentSession(sessionKey, agentSession, platform, replyCtx)
}

// closeAgentSession performs the teardown itself. Callers must have registered
// the in-flight close (beginSessionClose) first.
func (e *Engine) closeAgentSession(sessionKey string, agentSession AgentSession, platform Platform, replyCtx any) {
	if agentSession == nil {
		return
	}

	slog.Debug("cleanupInteractiveState: closing agent session", "session", sessionKey)
	closeStart := time.Now()

	done := make(chan struct{})
	var closeErr error
	go func() {
		closeErr = agentSession.Close()
		close(done)
	}()

	select {
	case <-done:
		if closeErr != nil {
			slog.Error("agent session close reported failure, process may still be running",
				"session", sessionKey, "error", closeErr)
			// The old process may still hold this conversation open — the
			// next turn must not resume into it.
			e.markUnsafeResume(sessionKey)
			e.notifySessionCloseFailure(sessionKey, platform, replyCtx, closeErr)
			return
		}
		if elapsed := time.Since(closeStart); elapsed >= slowAgentClose {
			slog.Warn("slow agent session close", "elapsed", elapsed, "session", sessionKey)
		}
	case <-time.After(closeTimeout):
		slog.Error("agent session close timed out, abandoning",
			"timeout", closeTimeout, "session", sessionKey)
		e.markUnsafeResume(sessionKey)
		e.notifySessionCloseFailure(sessionKey, platform, replyCtx,
			fmt.Errorf("did not respond within %s", closeTimeout))
	}
}

// notifySessionCloseFailure alerts the chat that owned a session when its
// underlying process could not be confirmed killed. Without this, a stuck
// process can keep running in the background — still holding the session's
// credentials — while the user believes /stop (or /new) already ended it.
func (e *Engine) notifySessionCloseFailure(sessionKey string, platform Platform, replyCtx any, cause error) {
	if platform == nil || replyCtx == nil {
		slog.Error("agent session close failed and no chat is available to alert",
			"session", sessionKey, "cause", cause)
		return
	}
	text := e.i18n.T(MsgSessionCloseFailed) + fmt.Sprintf(" (%v)", cause)
	if err := e.waitOutgoing(platform); err != nil {
		slog.Warn("notifySessionCloseFailure: wait outgoing failed", "session", sessionKey, "error", err)
	}
	if err := platform.Send(e.ctx, replyCtx, text); err != nil {
		slog.Error("notifySessionCloseFailure: send failed", "session", sessionKey, "error", err)
	}
}

const defaultEventIdleTimeout = 2 * time.Hour

// cardToolEntry stores a tool call record for card content rendering.
type cardToolEntry struct {
	Index int
	Name  string
	Input string
}

// buildCardContent constructs the full markdown for the streaming card.
func buildCardContent(thinking string, tools []cardToolEntry, answer string) string {
	var sb strings.Builder
	if thinking != "" {
		sb.WriteString("**Thinking**\n\n")
		sb.WriteString(thinking)
		sb.WriteString("\n\n---\n\n")
	}
	for _, t := range tools {
		sb.WriteString(fmt.Sprintf("**Tool #%d**: `%s`\n", t.Index, t.Name))
		if t.Input != "" {
			sb.WriteString(t.Input)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}
	if answer != "" {
		if len(tools) > 0 || thinking != "" {
			sb.WriteString("---\n\n")
		}
		sb.WriteString(answer)
	}
	return sb.String()
}

// unsolicitedReaderStopTimeout bounds how long stopUnsolicitedReader waits
// for the reader goroutine to exit. The reader is structured so its iterations
// are short (blocking adapter calls like RespondPermission are offloaded), so
// this timeout should almost always be non-binding. If it does fire, callers
// force a resync of the Events channel to preserve single-reader correctness.
const unsolicitedReaderStopTimeout = 5 * time.Second

// stopUnsolicitedReader cancels any running unsolicited reader goroutine and
// waits (bounded) for it to exit. If the reader does not exit in time, the
// caller is responsible for draining/resyncing the Events channel before a
// new foreground turn reads from it — we set eventsNeedResync here so that
// any downstream consumer drains before resuming. We do NOT wait unbounded:
// some callers hold interactiveMu, and a reader stuck in a blocking adapter
// call would stall unrelated sessions.
func (e *Engine) stopUnsolicitedReader(state *interactiveState) {
	state.mu.Lock()
	cancel := state.unsolicitedCancel
	done := state.unsolicitedDone
	state.unsolicitedCancel = nil
	state.unsolicitedDone = nil
	state.mu.Unlock()

	if cancel == nil {
		return
	}
	cancel()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(unsolicitedReaderStopTimeout):
		slog.Warn("unsolicited reader stop timed out; forcing resync",
			"timeout", unsolicitedReaderStopTimeout)
		// Force the next foreground turn to drain Events() defensively.
		// The old reader may still be alive; its ctx-double-check will drop
		// any event read after cancellation, so concurrent consumers cannot
		// silently steal foreground events.
		state.mu.Lock()
		state.eventsNeedResync = true
		state.mu.Unlock()
	}
}

// startUnsolicitedReader launches a background goroutine that consumes agent
// events produced between user-initiated turns (e.g. background task
// completions in Claude Code). Events are relayed to the platform immediately.
// The goroutine exits when its context is cancelled (by a new foreground turn
// or session cleanup) or when the Events channel is closed.
func (e *Engine) startUnsolicitedReader(state *interactiveState, session *Session, sessions *SessionManager, sessionKey string, workspaceDir string) {
	// Ensure no previous reader is still running.
	e.stopUnsolicitedReader(state)

	// Capture the agent session under lock. cleanupInteractiveState may nil
	// state.agentSession concurrently, so reading it inside the goroutine
	// without synchronisation is a data race.
	state.mu.Lock()
	agentSession := state.agentSession
	state.mu.Unlock()
	if agentSession == nil {
		return
	}

	ctx, cancel := context.WithCancel(e.ctx)
	done := make(chan struct{})

	state.mu.Lock()
	state.unsolicitedCancel = cancel
	state.unsolicitedDone = done
	state.mu.Unlock()

	go e.runUnsolicitedReader(ctx, cancel, done, state, agentSession, session, sessions, sessionKey, workspaceDir)
}

// runUnsolicitedReader is the goroutine body for the unsolicited event reader.
// agentSession is captured by the caller so we don't race with
// cleanupInteractiveState nilling state.agentSession.
func (e *Engine) runUnsolicitedReader(ctx context.Context, cancel context.CancelFunc, done chan struct{}, state *interactiveState, agentSession AgentSession, session *Session, sessions *SessionManager, sessionKey string, workspaceDir string) {
	defer close(done)
	defer cancel()

	events := agentSession.Events()

	var turnActive bool // true after first event, cleared on EventResult
	defer func() {
		if turnActive {
			if workspaceDir != "" && e.workspacePool != nil {
				if ws := e.workspacePool.Get(workspaceDir); ws != nil {
					ws.EndTurn()
				}
			}
		}
	}()

	var textParts []string
	var toolsUsed []string
	diagnostics := diagnosticMessages{engine: e}

	for {
		select {
		case <-ctx.Done():
			// Context cancelled (new foreground turn or cleanup). Don't set
			// eventsNeedResync — the caller (stopUnsolicitedReader) knows the
			// channel state is clean because it just took ownership.
			return

		case event, ok := <-events:
			if !ok {
				// Channel closed — agent process exited. Log any buffered
				// tool/text context so it isn't lost silently.
				if len(toolsUsed) > 0 || len(textParts) > 0 {
					slog.Warn("unsolicited reader: agent channel closed mid-turn",
						"session", sessionKey,
						"tools_used", toolsUsed,
						"text_fragments", len(textParts))
				}
				state.mu.Lock()
				state.eventsNeedResync = true
				state.mu.Unlock()
				return
			}

			// Go's select is non-deterministic when multiple cases are
			// ready, so even after ctx is cancelled we may still read one
			// last event from the channel. If ownership has been handed
			// off, drop the event rather than processing it — otherwise we
			// could relay (or worse, respond to) an event that belongs to
			// the incoming foreground turn. The caller has already set
			// eventsNeedResync on timeout, so any buffered events will be
			// drained before the foreground turn reads them.
			select {
			case <-ctx.Done():
				slog.Warn("unsolicited reader: event received after cancellation, dropping",
					"session", sessionKey, "event_type", event.Type)
				state.mu.Lock()
				state.eventsNeedResync = true
				state.mu.Unlock()
				return
			default:
			}

			// Mark workspace active on first event.
			if !turnActive {
				turnActive = true
				if workspaceDir != "" && e.workspacePool != nil {
					if ws := e.workspacePool.Get(workspaceDir); ws != nil {
						ws.BeginTurn()
					}
				}
				slog.Info("unsolicited events detected, relaying to platform",
					"session", sessionKey)
			}

			state.mu.Lock()
			p := state.platform
			replyCtx := state.replyCtx
			state.mu.Unlock()

			switch event.Type {
			case EventText:
				if event.Content != "" {
					textParts = append(textParts, event.Content)
				}

			case EventToolUse:
				// Record tool name so we can log or surface context if the
				// channel closes before a clean EventResult. Output is
				// delivered via EventResult; we intentionally do not relay
				// per-tool progress here (no active user turn to observe it).
				if event.ToolName != "" {
					toolsUsed = append(toolsUsed, event.ToolName)
				}
				slog.Debug("unsolicited tool use",
					"session", sessionKey,
					"tool", event.ToolName)

			case EventToolResult:
				slog.Debug("unsolicited tool result",
					"session", sessionKey,
					"status", event.ToolStatus)

			case EventStatus:
				if event.Content != "" {
					diagnostics.Send(p, replyCtx, event.Content, workspaceDir)
				}

			case EventResult:
				if !event.Done {
					continue
				}
				delivered := true
				fullResponse := event.Content
				if fullResponse == "" && len(textParts) > 0 {
					fullResponse = strings.Join(textParts, "")
				}

				if fullResponse != "" {
					for _, chunk := range SplitMessageCodeFenceAware(fullResponse, maxPlatformMessageLen) {
						if err := e.sendWithError(p, replyCtx, chunk); err != nil {
							delivered = false
						}
					}
				}
				diagnostics.Finish(delivered && event.Error == nil)

				// Safety note: concurrent writes to session.History by the
				// unsolicited reader and a foreground turn cannot overlap.
				// Session.AddHistory takes session.mu internally, and
				// stopUnsolicitedReader (called before any foreground turn
				// takes event-channel ownership) blocks until this goroutine
				// exits — so a foreground AddHistory is always ordered after
				// any unsolicited AddHistory.
				session.AddHistory("assistant", fullResponse)
				sessions.Save()

				// Reset for potential subsequent unsolicited turn.
				textParts = nil
				toolsUsed = nil
				turnActive = false
				if workspaceDir != "" && e.workspacePool != nil {
					if ws := e.workspacePool.Get(workspaceDir); ws != nil {
						ws.EndTurn()
					}
				}

				// Mark clean exit so next foreground turn preserves events.
				state.mu.Lock()
				state.eventsNeedResync = false
				state.mu.Unlock()

				// Reset the per-session idle close timer so a series of
				// background task completions does not get cut short by the
				// idle timeout armed at the end of the last foreground turn.
				// Without this, a long-running background turn (e.g. background task
				// that reports progress every minute) can be killed mid-flight
				// when the original idle timer fires. See #1686 P1-C P1-1.
				e.scheduleAgentSessionIdleClose(sessionKey, state)

				slog.Info("unsolicited turn complete",
					"session", sessionKey,
					"response_len", len(fullResponse))

			case EventPermissionRequest:
				// If approveAll (/yolo) is set, grant the request. Otherwise
				// deny — there is no active user turn to consult — and notify
				// the user on the platform so a silently blocked background
				// task is not invisible. RespondPermission may make a slow
				// adapter call, so we run it in a detached goroutine to keep
				// reader iterations fast (stopUnsolicitedReader relies on a
				// bounded wait for the reader to exit).
				state.mu.Lock()
				autoApprove := state.approveAll
				state.mu.Unlock()

				result := PermissionResult{Behavior: "deny", Message: "denied: no active user turn"}
				if autoApprove {
					result = PermissionResult{Behavior: "allow", UpdatedInput: event.ToolInputRaw}
				}
				reqID := event.RequestID
				respondCtx := ctx // capture current unsolicited reader context
				go func() {
					// Run in a goroutine to keep reader iterations fast, but honour
					// the reader's context so we don't call into a dead session after
					// stopUnsolicitedReader cancels the context.
					select {
					case <-respondCtx.Done():
						return
					default:
					}
					if err := agentSession.RespondPermission(reqID, result); err != nil {
						if respondCtx.Err() == nil {
							slog.Error("unsolicited: failed to respond permission", "error", err)
						}
					}
				}()
				if !autoApprove {
					toolName := event.ToolName
					if toolName == "" {
						toolName = "(unknown)"
					}
					e.send(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgBackgroundAutoDenied), toolName))
				}

			case EventError:
				if event.Error != nil {
					slog.Error("unsolicited agent error", "error", event.Error, "session", sessionKey)
					e.send(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError), event.Error))
				}
				state.mu.Lock()
				state.eventsNeedResync = true
				state.mu.Unlock()
				return
			}
		}
	}
}

type agentErrorHandler struct {
	contains string
	msgKey   MsgKey
}

var agentErrorHandlers = []agentErrorHandler{
	{"Session not found", MsgSessionNotFound},
}

func (e *Engine) processInteractiveEvents(state *interactiveState, session *Session, sessions *SessionManager, sessionKey string, msgID string, turnStart time.Time, stopTypingFn func(), sendDone <-chan error, replyCtx any) {
	if msgID != "" {
		state.mu.Lock()
		state.currentMessageID = msgID
		state.mu.Unlock()
	}

	var textParts []string
	diagnostics := diagnosticMessages{engine: e}
	var segmentStart int // index into textParts: text before this has been sent/displayed
	silentHold := false  // true while accumulated segment text could still resolve to a bare NO_REPLY marker
	toolCount := 0
	waitStart := time.Now()
	firstEventLogged := false
	var toolSteps []ToolStep
	var lastRichCardUpdate time.Time
	var lastRichCardLen int
	var cardMessageID any
	var partialText string
	triggerAutoCompress := false
	pendingSend := sendDone

	// stopTyping tracks the current turn's typing indicator so it can be
	// stopped when a queued message starts a new turn.
	stopTyping := stopTypingFn
	// doneReaction stores a function to add a "done" emoji after stopTyping.
	// Set during EventResult handling for multi-round quiet turns.
	var doneReaction func()
	defer func() {
		if stopTyping != nil {
			stopTyping()
		}
		if doneReaction != nil {
			doneReaction()
		}
	}()

	state.mu.Lock()
	workspaceDir := state.workspaceDir
	replyAgent := state.agent
	if replyAgent == nil {
		replyAgent = e.agent
	}
	workspaceRenderer := func(content string) string {
		return e.renderOutgoingContentForWorkspace(state.platform, content, workspaceDir)
	}
	sendWorkspace := func(p Platform, replyCtx any, content string) {
		e.sendForWorkspace(p, replyCtx, content, workspaceDir)
	}
	sendWorkspaceWithError := func(p Platform, replyCtx any, content string) error {
		return e.sendWithErrorForWorkspace(p, replyCtx, content, workspaceDir)
	}

	// Streaming card: aggregate entire turn into a single updatable card.
	var streamCard StreamingCard
	var cardToolCalls []cardToolEntry  // track tool calls for card content
	var cardThinkingText string        // latest thinking text
	var cardAnswerText strings.Builder // accumulated answer text

	if scp, ok := state.platform.(StreamingCardPlatform); ok && !e.separateProgressMessages() {
		if sc, err := scp.CreateStreamingCard(e.ctx, state.replyCtx); err != nil {
			slog.Warn("streaming card creation failed, falling back to normal messages", "error", err)
		} else {
			streamCard = sc
			slog.Info("streaming card created for turn", "session", sessionKey)
		}
	}
	sp := newStreamPreview(e.streamPreview, state.platform, state.replyCtx, e.ctx, workspaceRenderer)
	cp := e.newProgressWriter(state.platform, state.replyCtx, workspaceRenderer)
	progressWriters := []*compactProgressWriter{cp}
	cleanupProgress := func() {
		for _, writer := range progressWriters {
			writer.Cleanup()
		}
		progressWriters = nil
	}
	defer cleanupProgress()
	state.mu.Unlock()
	seenSteerGen := state.steerGen.Load()

	// Send instant confirmation reply if enabled and no streaming card is active.
	// Streaming cards provide their own "processing" indicator, so instant reply
	// is only needed when the platform doesn't support cards or card creation failed.
	if e.instantReply.Enabled && streamCard == nil {
		replyContent := e.instantReply.Content
		if replyContent == "" {
			replyContent = e.i18n.T(MsgStarting)
		}
		e.send(state.platform, state.replyCtx, replyContent)
	}

	// Idle timeout: resets on every received event (0 = disabled)
	var idleTimer *time.Timer
	var idleCh <-chan time.Time
	if e.eventIdleTimeout > 0 {
		idleTimer = time.NewTimer(e.eventIdleTimeout)
		defer idleTimer.Stop()
		idleCh = idleTimer.C
	}

	// Max turn time: absolute wall-clock cap that does NOT reset on events.
	// Prevents long-running tool calls from blocking the session forever (#1091).
	var turnDeadlineCh <-chan time.Time
	if e.maxTurnTime > 0 {
		turnDeadlineTimer := time.NewTimer(e.maxTurnTime)
		defer turnDeadlineTimer.Stop()
		turnDeadlineCh = turnDeadlineTimer.C
	}

	events := state.agentSession.Events()
	stopCh := state.stopSignal()
	for {
		var event Event
		var ok bool

		select {
		case <-stopCh:
			sp.discard()
			return
		case event, ok = <-events:
			if !ok {
				goto channelClosed
			}
		case err := <-pendingSend:
			pendingSend = nil
			if err != nil {
				slog.Error("failed to send prompt", "error", err, "session_key", sessionKey)
				sp.discard()
				if stopTyping != nil {
					stopTyping()
					stopTyping = nil
				}
				e.notifyDroppedQueuedMessages(state, err)
				if state.agentSession == nil || !state.agentSession.Alive() {
					e.cleanupInteractiveState(sessionKey, state)
				}
				state.mu.Lock()
				p := state.platform
				state.mu.Unlock()
				e.send(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError), err))
				return
			}
			continue
		case <-idleCh:
			slog.Error("agent session idle timeout: no events for too long, killing session",
				"session_key", sessionKey, "timeout", e.eventIdleTimeout, "elapsed", time.Since(turnStart))
			cp.Finalize(ProgressCardStateFailed)
			sp.discard()
			state.mu.Lock()
			state.eventsNeedResync = true
			p := state.platform
			state.mu.Unlock()
			e.send(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError), "agent session timed out (no response)"))
			e.cleanupInteractiveState(sessionKey, state)
			return
		case <-turnDeadlineCh:
			elapsed := time.Since(turnStart)
			slog.Warn("agent turn exceeded max_turn_time: sending stop signal, will force-kill if needed",
				"session_key", sessionKey, "max_turn_time", e.maxTurnTime, "elapsed", elapsed)
			cp.Finalize(ProgressCardStateFailed)
			sp.discard()
			state.mu.Lock()
			p := state.platform
			state.mu.Unlock()
			e.send(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError),
				fmt.Sprintf("agent turn exceeded maximum time (%v), stopping", e.maxTurnTime)))

			// Two-phase shutdown: first try a graceful stop so the agent can
			// write its final state before dying (preserves --resume ability).
			// If it doesn't exit within a short grace window, force-kill.
			state.markStopped()
			gracePeriod := 10 * time.Second
			graceTimer := time.NewTimer(gracePeriod)
		graceLoop:
			for {
				select {
				case evt, ok := <-state.agentSession.Events():
					if !ok || (ok && evt.Done) {
						// Agent exited cleanly; state is intact, resume will work.
						slog.Info("agent exited gracefully after max_turn_time stop signal",
							"session_key", sessionKey, "elapsed", time.Since(turnStart))
						graceTimer.Stop()
						state.mu.Lock()
						state.eventsNeedResync = false
						state.mu.Unlock()
						break graceLoop
					}
				case <-graceTimer.C:
					// Agent did not stop within grace period — force-kill.
					slog.Error("agent did not stop within grace period after max_turn_time; force-killing",
						"session_key", sessionKey, "grace_period", gracePeriod)
					graceTimer.Stop()
					state.mu.Lock()
					state.eventsNeedResync = true
					state.mu.Unlock()
					e.cleanupInteractiveState(sessionKey, state)
					return
				}
			}
			// Graceful exit path: cleanupInteractiveState closes the session,
			// but eventsNeedResync=false so the next --resume works correctly.
			e.cleanupInteractiveState(sessionKey, state)
			return
		case <-e.ctx.Done():
			state.mu.Lock()
			state.eventsNeedResync = true
			state.mu.Unlock()
			return
		}

		if state.isStopped() {
			sp.discard()
			state.mu.Lock()
			state.eventsNeedResync = true
			state.mu.Unlock()
			return
		}

		// Reset idle timer after receiving an event
		if idleTimer != nil {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(e.eventIdleTimeout)
		}

		if !firstEventLogged {
			firstEventLogged = true
			if elapsed := time.Since(waitStart); elapsed >= slowAgentFirstEvent {
				slog.Warn("slow agent first event", "elapsed", elapsed, "session", sessionKey, "event_type", event.Type)
			}
		}

		state.mu.Lock()
		p := state.platform
		state.mu.Unlock()

		// A steer message was sent (its acknowledgement now sits below the
		// current progress/preview messages): leave those as they are and
		// render further output into new messages under the steer.
		if g := state.steerGen.Load(); g != seenSteerGen {
			seenSteerGen = g
			if len(textParts) > segmentStart {
				if !sp.canPreview() {
					segment := strings.Join(textParts[segmentStart:], "")
					if segment != "" {
						for _, chunk := range SplitMessageCodeFenceAware(segment, maxPlatformMessageLen) {
							sendWorkspace(p, replyCtx, chunk)
						}
					}
				}
				segmentStart = len(textParts)
				silentHold = false
			}
			sp.freeze()
			sp.detachPreview()
			sp.unfreeze()
			cp.Finalize(ProgressCardStateCompleted)
			cp = e.newProgressWriter(p, replyCtx, workspaceRenderer)
			progressWriters = append(progressWriters, cp)
			cardMessageID = nil
			toolSteps = nil
			partialText = ""
			lastRichCardUpdate = time.Time{}
			lastRichCardLen = 0
		}

		// main codebase has no per-session quiet flag; pr309 referenced
		// sessionQuiet which we drop. e.display.ThinkingMessages /
		// ToolMessages handle user-level quiet in the fallback branches.
		richCardSupporter, hasRichCard := p.(RichCardSupporter)
		// Card 2.0 rich-card path is opt-in via [display] mode = "rich".
		// Default "legacy" keeps upstream behavior for all platforms.
		if e.display.CardMode != "rich" || e.separateProgressMessages() {
			hasRichCard = false
		}
		richMarkdownResolver, hasRichMarkdownResolver := p.(RichCardMarkdownResolver)
		resolveRichCardMarkdown := func(markdown string, final bool) string {
			if !hasRichMarkdownResolver || markdown == "" {
				return markdown
			}
			return richMarkdownResolver.ResolveRichCardMarkdown(e.ctx, markdown, final)
		}
		buildResolvedRichCard := func(status CardStatus, title string, steps []ToolStep, markdown string, streaming bool, statusFooter string) string {
			return richCardSupporter.BuildRichCard(status, title, steps, resolveRichCardMarkdown(markdown, !streaming), streaming, statusFooter)
		}

		switch event.Type {
		case EventThinking:
			if isEllipsisOnly(event.Content) {
				break
			}
			if hasRichCard {
				// When thinking messages are suppressed, skip card creation.
				if !e.display.ThinkingMessages {
					break
				}
				if thinking := strings.TrimSpace(truncateIf(event.Content, e.display.ThinkingMaxLen)); thinking != "" {
					toolSteps = append(toolSteps, ToolStep{
						Kind:    ToolStepKindThinking,
						Name:    "Thinking",
						Summary: thinking,
						Done:    true,
					})
				}
				if cardMessageID == nil {
					card := buildResolvedRichCard(CardStatusThinking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
					if starter, ok := p.(PreviewStarter); ok {
						handle, err := starter.SendPreviewStart(e.ctx, replyCtx, card)
						if err != nil {
							slog.Debug("rich card: failed to create initial thinking card", "platform", p.Name(), "error", err)
						} else {
							cardMessageID = handle
						}
					}
				} else if updater, ok := p.(MessageUpdater); ok {
					card := buildResolvedRichCard(CardStatusThinking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
					if err := updater.UpdateMessage(e.ctx, cardMessageID, card); err != nil {
						slog.Debug("rich card: failed to update thinking card", "platform", p.Name(), "error", err)
					}
				}
				break
			}
			// When thinking messages are hidden, behavior depends on display mode:
			//   quiet:   append separator to keep all text in one card
			//   compact: freeze+detach to split text into separate cards
			if !e.display.ThinkingMessages && len(textParts) > segmentStart {
				if e.display.Mode == "quiet" {
					if sp.canPreview() && sp.appendSeparator("\n\n") {
						textParts = append(textParts, "\n\n")
					}
				} else {
					if sp.canPreview() {
						sp.freeze()
						sp.detachPreview()
					} else {
						segment := strings.Join(textParts[segmentStart:], "")
						if segment != "" {
							for _, chunk := range SplitMessageCodeFenceAware(segment, maxPlatformMessageLen) {
								sendWorkspace(p, replyCtx, chunk)
							}
						}
					}
					segmentStart = len(textParts)
				}
				silentHold = false
			}
			if e.display.ThinkingMessages && event.Content != "" {
				// --- StreamingCard path ---
				if streamCard != nil && !streamCard.Failed() {
					cardThinkingText = truncateIf(event.Content, e.display.ThinkingMaxLen)
					_ = streamCard.Update(e.ctx, buildCardContent(cardThinkingText, cardToolCalls, cardAnswerText.String()))
					continue // skip original independent message sending
				}
				// --- Original path (fallback) ---
				// Flush accumulated text segment before thinking display
				previewActive := sp.canPreview()
				if len(textParts) > segmentStart {
					if !previewActive {
						segment := strings.Join(textParts[segmentStart:], "")
						if segment != "" {
							for _, chunk := range SplitMessageCodeFenceAware(segment, maxPlatformMessageLen) {
								sendWorkspace(p, replyCtx, chunk)
							}
						}
					}
					segmentStart = len(textParts)
					silentHold = false
				}
				sp.freeze()
				if previewActive {
					sp.detachPreview() // keep frozen preview visible as permanent message
				}
				preview := truncateIf(event.Content, e.display.ThinkingMaxLen)
				thinkingMsg := fmt.Sprintf(e.i18n.T(MsgThinking), preview)
				if !cp.AppendEvent(ProgressEntryThinking, preview, "", thinkingMsg) {
					sendWorkspace(p, replyCtx, thinkingMsg)
				}
			}

		case EventToolUse:
			toolCount++
			if hasRichCard {
				// When tool messages are suppressed, skip card updates on tool events.
				if !e.display.ToolMessages {
					break
				}
				toolSteps = append(toolSteps, ToolStep{
					Kind:    ToolStepKindTool,
					Name:    event.ToolName,
					Summary: truncateIf(event.ToolInput, e.display.ToolMaxLen),
				})
				if cardMessageID == nil {
					card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
					if starter, ok := p.(PreviewStarter); ok {
						handle, err := starter.SendPreviewStart(e.ctx, replyCtx, card)
						if err != nil {
							slog.Debug("rich card: failed to create initial tool card", "platform", p.Name(), "error", err)
						} else {
							cardMessageID = handle
						}
					}
				} else if updater, ok := p.(MessageUpdater); ok {
					card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
					if err := updater.UpdateMessage(e.ctx, cardMessageID, card); err != nil {
						slog.Debug("rich card: failed to update tool card", "platform", p.Name(), "error", err)
					}
				}
				break
			}
			// When tool messages are hidden, behavior depends on display mode:
			//   quiet:   append separator to keep all text in one card
			//   compact: freeze+detach to split text into separate cards
			if !e.display.ToolMessages && len(textParts) > segmentStart {
				if e.display.Mode == "quiet" {
					if sp.canPreview() && sp.appendSeparator("\n\n") {
						textParts = append(textParts, "\n\n")
					}
				} else {
					if sp.canPreview() {
						sp.freeze()
						sp.detachPreview()
					} else {
						segment := strings.Join(textParts[segmentStart:], "")
						if segment != "" {
							for _, chunk := range SplitMessageCodeFenceAware(segment, maxPlatformMessageLen) {
								sendWorkspace(p, replyCtx, chunk)
							}
						}
					}
					segmentStart = len(textParts)
				}
				silentHold = false
			}
			if e.display.ToolMessages {
				// --- StreamingCard path ---
				if streamCard != nil && !streamCard.Failed() {
					toolInput := event.ToolInput
					var formattedInput string
					if toolInput == "" {
						formattedInput = ""
					} else if strings.Contains(toolInput, "```") {
						formattedInput = toolInput
					} else if strings.Contains(toolInput, "\n") || utf8.RuneCountInString(toolInput) > 200 {
						lang := toolCodeLang(event.ToolName, toolInput)
						formattedInput = fmt.Sprintf("```%s\n%s\n```", lang, toolInput)
					} else {
						switch event.ToolName {
						case "shell", "run_shell_command", "Bash":
							formattedInput = fmt.Sprintf("```bash\n%s\n```", toolInput)
						default:
							formattedInput = fmt.Sprintf("`%s`", toolInput)
						}
					}
					cardToolCalls = append(cardToolCalls, cardToolEntry{
						Index: toolCount,
						Name:  event.ToolName,
						Input: formattedInput,
					})
					_ = streamCard.Update(e.ctx, buildCardContent(cardThinkingText, cardToolCalls, cardAnswerText.String()))
					continue // skip original independent message sending
				}
				// --- Original path (fallback) ---
				// Flush accumulated text segment before tool display
				previewActive := sp.canPreview()
				if len(textParts) > segmentStart {
					if !previewActive {
						segment := strings.Join(textParts[segmentStart:], "")
						if segment != "" {
							for _, chunk := range SplitMessageCodeFenceAware(segment, maxPlatformMessageLen) {
								sendWorkspace(p, replyCtx, chunk)
							}
						}
					}
					segmentStart = len(textParts)
					silentHold = false
				}
				sp.freeze()
				if previewActive {
					sp.detachPreview() // keep frozen preview visible as permanent message
				}
				toolInput := event.ToolInput
				var formattedInput string
				if toolInput == "" {
					formattedInput = ""
				} else if strings.Contains(toolInput, "```") {
					formattedInput = toolInput
				} else if strings.Contains(toolInput, "\n") || utf8.RuneCountInString(toolInput) > 200 {
					lang := toolCodeLang(event.ToolName, toolInput)
					formattedInput = fmt.Sprintf("```%s\n%s\n```", lang, toolInput)
				} else {
					switch event.ToolName {
					case "shell", "run_shell_command", "Bash":
						formattedInput = fmt.Sprintf("```bash\n%s\n```", toolInput)
					default:
						formattedInput = fmt.Sprintf("`%s`", toolInput)
					}
				}
				toolMsg := fmt.Sprintf(e.i18n.T(MsgTool), toolCount, event.ToolName, formattedInput)
				if e.display.CollapseToolMessages {
					toolMsg = toolActivityLabel(event.ToolName)
				}
				// Truncate the tool input that goes into the progress card payload so
				// tool_max_len applies uniformly to progress_style=card, matching
				// the rich-card path. event.ToolInput itself is left untouched.
				cardToolInput := truncateIf(toolInput, e.display.ToolMaxLen)
				if !cp.AppendEvent(ProgressEntryToolUse, cardToolInput, event.ToolName, toolMsg) {
					for _, chunk := range SplitMessageCodeFenceAware(toolMsg, maxPlatformMessageLen) {
						sendWorkspace(p, replyCtx, chunk)
					}
				}
			}

		case EventToolResult:
			if e.display.CollapseToolMessages {
				break
			}
			if e.display.ToolMessages {
				result := strings.TrimSpace(event.ToolResult)
				if result == "" {
					result = strings.TrimSpace(event.Content)
				}
				if result != "" {
					result = truncateIf(result, e.display.ToolMaxLen)
				}
				if result != "" || event.ToolStatus != "" || event.ToolExitCode != nil || event.ToolSuccess != nil {
					if hasRichCard {
						toolSteps = mergeRichToolResult(toolSteps, event, result, e.display.ToolMaxLen)
						if cardMessageID == nil {
							card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
							if starter, ok := p.(PreviewStarter); ok {
								handle, err := starter.SendPreviewStart(e.ctx, replyCtx, card)
								if err != nil {
									slog.Debug("rich card: failed to create tool-result card", "platform", p.Name(), "error", err)
								} else {
									cardMessageID = handle
								}
							}
						} else if updater, ok := p.(MessageUpdater); ok {
							card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
							if err := updater.UpdateMessage(e.ctx, cardMessageID, card); err != nil {
								slog.Debug("rich card: failed to update tool-result card", "platform", p.Name(), "error", err)
							}
						}
						break
					}
					resultMsg := e.formatToolResultEventFallback(event.ToolName, result, event.ToolStatus, event.ToolExitCode, event.ToolSuccess)
					entry := ProgressCardEntry{
						Kind:     ProgressEntryToolResult,
						Tool:     event.ToolName,
						Text:     result,
						Status:   event.ToolStatus,
						ExitCode: event.ToolExitCode,
						Success:  event.ToolSuccess,
					}
					if !cp.AppendStructured(entry, resultMsg) {
						if !SuppressStandaloneToolResultEvent(p) {
							e.sendRaw(p, replyCtx, resultMsg)
						}
					}
				}
			}

		case EventText:
			content := event.Content
			if e.display.HideAgentFooter {
				content = stripAgentFooterLines(content)
			}
			if content != "" && !isEllipsisOnly(content) {
				// Pre-compute silentHold transition including this chunk so the
				// rich-card path doesn't leak a preview that gets recalled at
				// end-of-stream when the text resolves to bare NO_REPLY (Lark
				// renders the recall as "撤回了一条消息"). Both rich and legacy
				// paths share this single transition; couldBeSilentPrefix is
				// monotonically decreasing as segments grow, so the transition
				// is held → released at most once per segment.
				peekSegment := strings.Join(textParts[segmentStart:], "") + content
				prevHold := silentHold
				silentHold = couldBeSilentPrefix(peekSegment)
				releasedNow := prevHold && !silentHold

				handledByStreamCard := false
				if streamCard != nil && !streamCard.Failed() {
					textParts = append(textParts, content) // always accumulate for history
					if !silentHold {
						if releasedNow {
							cardAnswerText.WriteString(peekSegment)
						} else {
							cardAnswerText.WriteString(content)
						}
						_ = streamCard.Update(e.ctx, buildCardContent(cardThinkingText, cardToolCalls, cardAnswerText.String()))
					}
					handledByStreamCard = true
				}
				if !handledByStreamCard {
					if len(textParts) == 0 {
						if hasRichCard {
							if cardMessageID == nil && !silentHold {
								card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
								if starter, ok := p.(PreviewStarter); ok {
									handle, err := starter.SendPreviewStart(e.ctx, replyCtx, card)
									if err != nil {
										slog.Debug("rich card: failed to create initial text card", "platform", p.Name(), "error", err)
									} else {
										cardMessageID = handle
									}
								}
							}
						} else if !silentHold {
							sp.setStatus(CardStatusWorking)
						}
					}
					textParts = append(textParts, content)
					partialText += content
					if hasRichCard {
						if !silentHold {
							// Lazy creation: if we held during the first text events and
							// only released this chunk, the initial-create branch above
							// won't fire (textParts is non-empty by now). Build the card
							// here using the accumulated partialText so the card emerges
							// with the post-prefix content already in body.
							if cardMessageID == nil {
								card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
								if starter, ok := p.(PreviewStarter); ok {
									handle, err := starter.SendPreviewStart(e.ctx, replyCtx, card)
									if err != nil {
										slog.Debug("rich card: failed to create deferred text card", "platform", p.Name(), "error", err)
									} else {
										cardMessageID = handle
									}
								}
							}
							// Throttle: cardkit-v1 streaming text path uses tighter limits (200ms / 20 chars)
							// for smoother typewriter UX; full-card Patch fallback keeps the original 1500ms / 30 chars.
							streamer, hasStreamer := p.(RichCardTextStreamer)
							throttleDur := 1500 * time.Millisecond
							throttleChars := 30
							if hasStreamer && cardMessageID != nil {
								throttleDur = 200 * time.Millisecond
								throttleChars = 20
							}
							if cardMessageID != nil && (time.Since(lastRichCardUpdate) > throttleDur || len(partialText)-lastRichCardLen > throttleChars) {
								// Prefer per-element streaming text update (cardkit-v1) when available;
								// it engages Lark's native typewriter rendering. Falls back to
								// full-card Patch on ErrNotSupported (handle without cardID) or any error.
								streamed := false
								if hasStreamer {
									streamBody := resolveRichCardMarkdown(partialText, false)
									if err := streamer.StreamRichCardText(e.ctx, cardMessageID, streamBody); err == nil {
										lastRichCardUpdate = time.Now()
										lastRichCardLen = len(partialText)
										streamed = true
									} else if !errors.Is(err, ErrNotSupported) {
										slog.Debug("rich card: streaming text update failed, falling back to full Patch", "platform", p.Name(), "error", err)
									}
								}
								if !streamed {
									card := buildResolvedRichCard(CardStatusWorking, "", toolSteps, partialText, true, e.composeRichStatusFooter(true, turnStart, e.agent, state.agentSession, state.workspaceDir))
									if updater, ok := p.(MessageUpdater); ok {
										if err := updater.UpdateMessage(e.ctx, cardMessageID, card); err == nil {
											lastRichCardUpdate = time.Now()
											lastRichCardLen = len(partialText)
										} else {
											slog.Debug("rich card: failed to update text card", "platform", p.Name(), "error", err)
										}
									}
								}
							}
						}
					} else {
						if !silentHold && sp.canPreview() {
							if releasedNow {
								sp.appendText(peekSegment) // flush all held chunks at once
							} else {
								sp.appendText(content)
							}
						}
					}
				}
			}
			if event.SessionID != "" {
				wasEmpty := session.GetAgentSessionID() == ""
				if session.GetAgentSessionID() != event.SessionID {
					session.SetAgentSessionID(event.SessionID, e.agent.Name())
					if wasEmpty {
						pendingName := session.GetName()
						if pendingName != "" && pendingName != "session" && pendingName != "default" {
							sessions.SetSessionName(event.SessionID, pendingName)
						}
					}
					sessions.Save()
				}
			}

		case EventPermissionRequest:
			// extension_select is a Pi extension UI request routed via the
			// AskUserQuestion rich-card path. The pi session adapter populates
			// event.Questions so it renders as a button card (same UX as Claude
			// Code's AskUserQuestion).
			//
			// extension_confirm is intentionally NOT in this list: extensions
			// use ctx.ui.confirm() to ask the user for permission on a tool
			// call (e.g. permission-gate on Bash), and the engine must render
			// it as a regular permission request (Allow/Deny) so the UX
			// matches other agents.
			isAskQuestion := (event.ToolName == "AskUserQuestion" ||
				event.ToolName == "extension_select") && len(event.Questions) > 0
			isPlanReview := event.ToolName == "ExitPlanMode"

			state.mu.Lock()
			autoApprove := state.approveAll
			turnOwner := state.currentUserID
			state.mu.Unlock()

			if autoApprove && !isAskQuestion && !isPlanReview {
				slog.Debug("auto-approving (approve-all)", "request_id", event.RequestID, "tool", event.ToolName)
				_ = state.agentSession.RespondPermission(event.RequestID, PermissionResult{
					Behavior:     "allow",
					UpdatedInput: event.ToolInputRaw,
				})
				continue
			}

			// Flush accumulated text segment before permission prompt
			previewActive := sp.canPreview()
			if len(textParts) > segmentStart {
				if !previewActive {
					segment := strings.Join(textParts[segmentStart:], "")
					if segment != "" {
						for _, chunk := range SplitMessageCodeFenceAware(segment, maxPlatformMessageLen) {
							sendWorkspace(p, replyCtx, chunk)
						}
					}
				}
				segmentStart = len(textParts)
				silentHold = false
			}
			sp.freeze()
			if previewActive {
				sp.detachPreview() // keep frozen preview visible as permanent message
			}

			slog.Info("permission request",
				"request_id", event.RequestID,
				"tool", event.ToolName,
			)

			pending := &pendingPermission{
				RequestID:    event.RequestID,
				ToolName:     event.ToolName,
				ToolInput:    event.ToolInputRaw,
				InputPreview: event.ToolInput,
				Questions:    event.Questions,
				OwnerUserID:  turnOwner,
				Resolved:     make(chan struct{}),
			}
			if isPlanReview {
				pending.Plan = event.ToolInput
				if strings.TrimSpace(pending.Plan) == "" {
					pending.Plan = "(empty plan)"
				}
			}
			state.mu.Lock()
			state.pending = pending
			state.mu.Unlock()

			if isAskQuestion {
				e.sendAskQuestionPrompt(p, replyCtx, event.Questions, 0)
			} else if isPlanReview {
				_, canSwitch := state.agentSession.(LiveModeSwitcher)
				e.sendPlanPrompt(p, replyCtx, pending.Plan, canSwitch)
			} else {
				permLimit := e.display.ToolMaxLen
				if permLimit > 0 {
					permLimit = permLimit * 8 / 5
				}
				toolInput := truncateIf(event.ToolInput, permLimit)
				prompt := fmt.Sprintf(e.i18n.T(MsgPermissionPrompt), event.ToolName, toolInput)
				e.sendPermissionPrompt(p, replyCtx, prompt, event.ToolName, toolInput)
			}

			// Stop idle timer while waiting for user permission response;
			// the user may take a long time to decide, and we don't want
			// the idle timeout to kill the session during that wait.
			if idleTimer != nil {
				idleTimer.Stop()
			}

			<-pending.Resolved
			slog.Info("permission resolved", "request_id", event.RequestID)

			// The stream preview was frozen+detached when this permission
			// request was emitted, so any subsequent EventText in this turn
			// would otherwise be buffered until EventResult and sent as a
			// single bulk message (regression fixed by
			// fix/stream-preview-after-permission). unfreeze() restores the
			// preview so the post-resolution output opens a fresh streaming
			// card and continues to update incrementally.
			sp.unfreeze()

			// Restart idle timer after permission is resolved
			if idleTimer != nil {
				idleTimer.Reset(e.eventIdleTimeout)
			}

		case EventResult:
			// Non-terminal result events (e.g. mid-turn compaction: Claude
			// Code's auto-context-compact emits type:"result" with
			// subtype:"compact"/"compaction" and Done=false) must not trigger
			// turn-completion side effects. Skip them so the outer loop
			// continues to read subsequent tool calls and assistant messages
			// for the same turn. See PR #1272 / issue #481.
			if !event.Done {
				slog.Debug("EventResult: non-terminal result event, continuing event loop",
					"session", session.ID,
					"input_tokens", event.InputTokens,
					"output_tokens", event.OutputTokens,
					"metadata", event.Metadata,
				)
				continue
			}
			cp.Finalize(ProgressCardStateCompleted)
			cleanupProgress()
			// Use state.agentSession.CurrentSessionID() instead of event.SessionID.
			// event.SessionID may be empty in some cases, causing the agent_session_id
			// to not be persisted to disk, breaking session resume on next startup.
			if state != nil && state.agentSession != nil {
				if currentID := state.agentSession.CurrentSessionID(); currentID != "" {
					wasEmpty := session.GetAgentSessionID() == ""
					if session.GetAgentSessionID() != currentID {
						session.SetAgentSessionID(currentID, e.agent.Name())
						if wasEmpty {
							pendingName := session.GetName()
							if pendingName != "" && pendingName != "session" && pendingName != "default" {
								sessions.SetSessionName(currentID, pendingName)
							}
						}
					}
					sessions.Save()
				}
			}

			// Mark clean exit so unsolicited reader preserves buffered events.
			state.mu.Lock()
			state.eventsNeedResync = false
			state.mu.Unlock()

			fullResponse := event.Content
			if e.display.HideAgentFooter {
				fullResponse = stripAgentFooterLines(fullResponse)
			}
			// When tool progress is hidden, segmentStart stays 0 and textParts
			// contains ALL text across tool boundaries. Prefer the full accumulated
			// text over event.Content which only contains the last assistant segment.
			if len(textParts) > 0 && segmentStart == 0 && !e.display.ToolMessages {
				fullResponse = strings.Join(textParts, "")
			} else if fullResponse == "" && len(textParts) > 0 {
				fullResponse = strings.Join(textParts, "")
			}
			if fullResponse == "" {
				fullResponse = e.i18n.T(MsgEmptyResponse)
			}

			// Strip any agent-self-reported "[ctx: ~XX%]" marker so it does not
			// leak into the delivered text. The on-screen ctx indicator is now
			// rendered exclusively in the reply footer.
			sdkPlausible := event.InputTokens >= 100
			selfPct := parseSelfReportedCtx(fullResponse)
			cleanResponse := ctxSelfReportRe.ReplaceAllString(fullResponse, "")
			cleanResponse = strings.TrimRight(cleanResponse, "\n ")
			baseResponse := cleanResponse

			contextEstimate := estimateTokensWithPendingAssistant(session.GetHistory(0), baseResponse)

			// Evaluate auto-compress trigger (token estimate on user+assistant text,
			// including this turn's assistant reply before it is appended to history).
			if e.autoCompressEnabled && e.autoCompressMaxTokens > 0 {
				estimate := contextEstimate
				now := time.Now()
				state.mu.Lock()
				last := state.lastAutoCompressAt
				state.mu.Unlock()
				if estimate >= e.autoCompressMaxTokens && (last.IsZero() || now.Sub(last) >= e.autoCompressMinGap) {
					triggerAutoCompress = true
					state.mu.Lock()
					state.lastAutoCompressTokens = estimate
					state.mu.Unlock()
				}
			}

			// Detect NO_REPLY marker on the base response (before indicators/footer are appended).
			// Three cases:
			//   1. bare marker (isSilentReply)               → fully silent
			//   2. trailing marker with non-empty reasoning  → strip marker, deliver reasoning
			//   3. trailing marker with empty strip result   → fully silent
			// History records the ORIGINAL baseResponse so the agent retains context of its own
			// decision; only the outbound platform text gets rewritten/suppressed.
			session.AddHistory("assistant", baseResponse)
			sessions.Save()

			isSilent := isSilentReply(baseResponse)
			if !isSilent {
				if stripped, ok := stripTrailingSilent(baseResponse); ok {
					if strings.TrimSpace(stripped) == "" {
						isSilent = true
					} else {
						baseResponse = stripped
						cleanResponse = stripped
					}
				}
			}

			if !isSilent {
				e.hooks.Emit(HookEvent{
					Event:      HookEventMessageSent,
					SessionKey: sessionKey,
					Platform:   p.Name(),
					Content:    baseResponse,
				})
			}

			// statusFooter holds the structured CCD-style footer separately so
			// platforms implementing StatusFooterSender / StatusFooterUpdater
			// can render it with smaller/dim styling. Other paths inline-append
			// it via appendReplyFooter as a fallback. The default
			// (non-CCD) reply footer keeps its existing inline behavior since
			// it's a single short line that does not benefit from a separate
			// card element. In rich mode, the inline-append fallback is
			// suppressed — the rich card renders an equivalent statusFooter
			// through BuildRichCard, so re-appending the legacy footer here
			// would double-print model/ctx/workdir into the card body.
			var statusFooter string
			var legacyStatusFooter string
			if !isSilent {
				footerContext := replyFooterContextText(replyFooterSessionContextUsage(state.agentSession), e.i18n)
				if e.showContextIndicator {
					if sdkPlausible {
						if text := contextIndicatorText(event.InputTokens); text != "" {
							footerContext = text
						}
					} else if selfPct > 0 {
						footerContext = fmt.Sprintf("[ctx: ~%d%%]", selfPct)
					}
				}
				if status := e.buildClaudeStatusLineFooter(replyAgent, state.agentSession, workspaceDir); status != "" {
					statusFooter = status
				} else if footer := e.buildReplyFooter(replyAgent, state.agentSession, workspaceDir, footerContext); footer != "" {
					statusFooter = footer
					legacyStatusFooter = footer
				}
			}
			fullResponse = cleanResponse

			turnDuration := time.Since(turnStart)
			slog.Info("turn complete",
				"session", session.ID,
				"agent_session", session.GetAgentSessionID(),
				"msg_id", msgID,
				"tools", toolCount,
				"response_len", len(fullResponse),
				"turn_duration", turnDuration,
				"input_tokens", event.InputTokens,
				"output_tokens", event.OutputTokens,
				"silent", isSilent,
			)
			// DEBUG: full assistant response for in-depth debugging.
			if slog.Default().Enabled(e.ctx, slog.LevelDebug) {
				slog.Debug("turn response",
					"session", session.ID,
					"agent_session", session.GetAgentSessionID(),
					"history_len", session.HistoryLen(),
					"response", previewText(fullResponse, 500),
				)
			}

			e.noteUserTurnCompleted(state)

			normalizedBaseResponse := strings.TrimSpace(baseResponse)
			state.mu.Lock()
			suppressDuplicate := normalizedBaseResponse != "" && normalizedBaseResponse == state.sideText
			state.sideText = ""
			state.mu.Unlock()

			replyStart := time.Now()

			// --- StreamingCard path ---
			if streamCard != nil && !streamCard.Failed() {
				sp.finish("", "") // cleanup preview (should be no-op if card was active)
				// Silent reply: never render the NO_REPLY marker into the card.
				// cardAnswerText holds only the text streamed BEFORE the marker
				// (empty for a bare NO_REPLY, since silentHold suppresses card
				// writes while the segment is still a NO_REPLY prefix). Finalize
				// with that instead of fullResponse so the card resolves to Done
				// without leaking the marker, and skip the fallback send that
				// would otherwise post the suppressed marker verbatim.
				cardBody := fullResponse
				if isSilent {
					cardBody = strings.TrimRight(cardAnswerText.String(), " \t\r\n")
				}
				finalContent := buildCardContent(cardThinkingText, cardToolCalls, cardBody)
				if err := streamCard.Finalize(e.ctx, finalContent); err != nil {
					slog.Error("streaming card finalize failed, sending fallback", "error", err)
					// Fallback: send the response as a normal message — but never
					// for a silent reply, which has no deliverable content.
					if !isSilent {
						for _, chunk := range SplitMessageCodeFenceAware(fullResponse, maxPlatformMessageLen) {
							if err := sendWorkspaceWithError(p, replyCtx, chunk); err != nil {
								return
							}
						}
					}
				}
				if isSilent {
					slog.Info("silent reply suppressed", "session", session.ID)
				}
			} else if isSilent {
				// Silent reply: drop any in-flight preview and skip all send paths.
				// sp.discard() clears previewMsgID so sp.needsDoneReaction() also returns false,
				// preventing a stray done_emoji push.
				sp.discard()
				// Rich mode: cardMessageID is tracked independently of sp.previewMsgID,
				// so sp.discard() doesn't reach it. Without explicit handling the rich
				// card would stay frozen in "Working" / "Thinking" header state forever
				// (no Done flip, no Patch).
				//
				// Determine the visible body to finalize the card with. partialText
				// accumulates every EventText chunk this turn, so it captures any
				// pre-NO_REPLY content the user already saw streaming (e.g. when the
				// agent wrote "Hello\nNO_REPLY"). Strip the trailing NO_REPLY marker
				// before rendering. If there is neither body nor tool history, the
				// card has nothing visible worth keeping; delete to avoid an
				// orphaned shell. Finalizing-in-place avoids the "撤回了一条消息"
				// gray bar that DeletePreviewMessage would leave in Lark.
				if hasRichCard && cardMessageID != nil {
					silentBody := partialText
					if stripped, ok := stripTrailingSilent(partialText); ok {
						silentBody = strings.TrimRight(stripped, " \t\r\n")
					}
					if silentBody != "" || len(toolSteps) > 0 {
						card := buildResolvedRichCard(CardStatusDone, "", toolSteps, silentBody, false, e.composeRichStatusFooter(false, turnStart, e.agent, state.agentSession, state.workspaceDir))
						if updater, ok := p.(MessageUpdater); ok {
							if err := updater.UpdateMessage(e.ctx, cardMessageID, card); err != nil {
								slog.Debug("rich card: failed to finalize card on silent reply", "platform", p.Name(), "error", err)
							}
						}
					} else {
						if cleaner, ok := p.(PreviewCleaner); ok {
							if err := cleaner.DeletePreviewMessage(e.ctx, cardMessageID); err != nil {
								slog.Debug("rich card: failed to delete card on silent reply", "platform", p.Name(), "error", err)
							}
						}
					}
					cardMessageID = nil
				}
				slog.Info("silent reply suppressed", "session", session.ID)
			} else if hasRichCard {
				parts := []string{fullResponse}
				if splitter, ok := p.(MarkdownTableSplitter); ok {
					parts = splitter.SplitMarkdownByTables(fullResponse, 5)
				}
				richStatusFooter := e.composeRichStatusFooter(false, turnStart, e.agent, state.agentSession, state.workspaceDir)
				if legacyStatusFooter != "" {
					richStatusFooter = formatElapsed(time.Since(turnStart), false) + "\n" + legacyStatusFooter
				}
				finalBody := resolveRichCardMarkdown(parts[0], true)
				finalCard := richCardSupporter.BuildRichCard(CardStatusDone, "", toolSteps, finalBody, false, richStatusFooter)
				if cardMessageID != nil {
					// Forced final flush via cardkit-v1 streaming text update before
					// flipping status to Done via full-card Patch. The throttle in the
					// EventText path may have skipped the last <200ms / <20 chars; this
					// catch-up keeps the typewriter rendering smooth all the way to the
					// end. ErrNotSupported (no cardID) and any error are silent — the
					// subsequent UpdateMessage will rewrite the body anyway.
					if streamer, ok := p.(RichCardTextStreamer); ok {
						if err := streamer.StreamRichCardText(e.ctx, cardMessageID, finalBody); err != nil && !errors.Is(err, ErrNotSupported) {
							slog.Debug("rich card: final streaming flush failed (proceeding to full Patch)", "platform", p.Name(), "error", err)
						}
					}
					if updater, ok := p.(MessageUpdater); ok {
						if err := updater.UpdateMessage(e.ctx, cardMessageID, finalCard); err != nil {
							slog.Debug("rich card: final update failed, falling back to send", "platform", p.Name(), "error", err)
							if err := p.Send(e.ctx, replyCtx, finalCard); err != nil {
								slog.Error("failed to send rich card reply", "error", err)
								return
							}
						}
					}
				} else {
					if err := p.Send(e.ctx, replyCtx, finalCard); err != nil {
						slog.Error("failed to send rich card reply", "error", err)
						return
					}
				}
				for _, overflow := range parts[1:] {
					overflowBody := resolveRichCardMarkdown(overflow, true)
					overflowCard := richCardSupporter.BuildRichCard(CardStatusDone, "", nil, overflowBody, false, richStatusFooter)
					if err := p.Send(e.ctx, replyCtx, overflowCard); err != nil {
						slog.Error("failed to send overflow rich card", "error", err)
						return
					}
				}
			} else if toolCount > 0 && segmentStart > 0 {
				// When tool calls happened and prior text was already surfaced in segments,
				// only send the unsent remainder. When tool progress is hidden, tool events don't surface
				// side-channel messages and segmentStart stays 0, so keep normal finalize flow.
				sp.discard()
				if segmentStart < len(textParts) {
					unsent := strings.Join(textParts[segmentStart:], "")
					if unsent != "" {
						if !sendChunksWithStatusFooter(e.ctx, p, replyCtx, unsent, statusFooter, sendWorkspaceWithError) {
							return
						}
					}
				}
			} else if suppressDuplicate {
				sp.discard()
				metaOnly := strings.TrimSpace(strings.TrimPrefix(fullResponse, baseResponse))
				if metaOnly != "" || statusFooter != "" {
					if !sendChunksWithStatusFooter(e.ctx, p, replyCtx, metaOnly, statusFooter, sendWorkspaceWithError) {
						return
					}
				}
				slog.Debug("EventResult: suppressed duplicate side-channel text", "response_len", len(fullResponse))
			} else if sp.finish(fullResponse, statusFooter) {
				slog.Debug("EventResult: finalized via stream preview", "response_len", len(fullResponse), "footer_len", len(statusFooter))
			} else {
				slog.Debug("EventResult: sending via p.Send (preview inactive or failed)", "response_len", len(fullResponse), "footer_len", len(statusFooter))
				if !sendChunksWithStatusFooter(e.ctx, p, replyCtx, fullResponse, statusFooter, sendWorkspaceWithError) {
					return
				}
			}

			diagnostics.Finish(event.Error == nil)

			if elapsed := time.Since(replyStart); elapsed >= slowPlatformSend {
				slog.Warn("slow final reply send", "platform", p.Name(), "elapsed", elapsed, "response_len", len(fullResponse))
			}

			// TTS: async voice reply if enabled (skipped for silent replies)
			if !isSilent && e.tts != nil && e.tts.Enabled && e.tts.TTS != nil {
				state.mu.Lock()
				fromVoice := state.fromVoice
				state.mu.Unlock()
				mode := e.tts.GetTTSMode()
				slog.Debug("tts: checking conditions", "mode", mode, "fromVoice", fromVoice, "will_send", mode == "always" || (mode == "voice_only" && fromVoice))
				if mode == "always" || (mode == "voice_only" && fromVoice) {
					go e.sendTTSReply(p, replyCtx, fullResponse)
				}
			} else {
				slog.Debug("tts: not enabled", "tts_nil", e.tts == nil, "enabled", e.tts != nil && e.tts.Enabled, "tts_obj_nil", e.tts == nil || e.tts.TTS == nil)
			}

			// Auto-compress after finishing a turn, before sending any queued messages.
			if triggerAutoCompress {
				compressor, ok := e.agent.(ContextCompressor)
				if ok && compressor.CompressCommand() != "" {
					if pendingSend != nil {
						if err := <-pendingSend; err != nil {
							slog.Debug("async send error before compress", "error", err)
						}
					}
					state.mu.Lock()
					state.lastAutoCompressAt = time.Now()
					tokenEst := state.lastAutoCompressTokens
					state.mu.Unlock()
					slog.Info("auto-compress: triggering", "session", sessionKey)

					// Notify user before compressing so they know the context is about to change.
					compressNotice := e.i18n.T(MsgCompressing)
					if tokenEst > 0 {
						compressNotice = fmt.Sprintf("%s (~%dk tokens)", compressNotice, tokenEst/1000)
					}
					e.send(state.platform, state.replyCtx, compressNotice)

					// Run compress inline while the session is still locked.
					e.runCompress(state, session, sessions, sessionKey, state.platform, state.replyCtx, true)
					return
				}
			}

			// Check for queued messages — if present, continue the event loop
			// for the next turn instead of returning.
			state.mu.Lock()
			droppedStale := 0
			for len(state.pendingMessages) > 0 && e.isQueuedUserMessageStaleForDrainLocked(state, state.pendingMessages[0].userMessageTimeMs) {
				state.pendingMessages = state.pendingMessages[1:]
				droppedStale++
			}
			if droppedStale > 0 {
				slog.Info("dropped stale queued user messages after completed turn",
					"session", sessionKey,
					"dropped", droppedStale,
				)
			}
			if len(state.pendingMessages) > 0 {
				queued := state.pendingMessages[0]
				state.pendingMessages = state.pendingMessages[1:]
				remainingQueue := len(state.pendingMessages)
				state.platform = queued.platform
				state.replyCtx = queued.replyCtx
				state.currentMessageID = queued.messageID
				state.fromVoice = queued.fromVoice
				state.currentTurnUserMessageTimeMs = queued.userMessageTimeMs
				state.mu.Unlock()

				// Stop the previous turn's typing indicator
				if stopTyping != nil {
					stopTyping()
					stopTyping = nil
				}
				// Start a new typing indicator for the queued message's context
				if ti, ok := queued.platform.(TypingIndicator); ok {
					stopTyping = ti.StartTyping(e.ctx, queued.replyCtx)
				}
				// Agent continues working — don't add done reaction for this turn.
				doneReaction = nil

				// Drain stale events before starting the next turn. Between
				// EventResult and Send(), the only buffered events would be
				// stale leftovers (e.g. a deferred EventError from cmd.Wait()).
				drainEvents(state.agentSession.Events())

				if pendingSend != nil {
					if err := <-pendingSend; err != nil {
						slog.Debug("async send error before queued turn", "error", err)
					}
				}

				queuedPrompt := e.buildSenderPrompt(queued.content, queued.userID, queued.userName, queued.msgPlatform, queued.msgSessionKey, queued.channelKey)

				state.mu.Lock()
				as := state.agentSession // capture under lock to avoid race with cleanup
				state.mu.Unlock()

				nextSend := make(chan error, 1)
				go func() {
					if as == nil {
						nextSend <- fmt.Errorf("agent session became nil")
						return
					}
					nextSend <- as.Send(queuedPrompt, queued.messageID, queued.images, queued.files)
				}()
				pendingSend = nextSend

				// Detect language now (deferred from queue time to avoid
				// flipping locale while the previous turn is still running).

				// Reset per-turn state for the next turn
				msgID = queued.messageID
				textParts = nil
				segmentStart = 0
				toolCount = 0
				turnStart = time.Now()
				firstEventLogged = false
				waitStart = time.Now()
				// Reassign the local replyCtx parameter to the queued message's
				// trigger context. state.replyCtx was updated above, but the
				// function-scope replyCtx is what gets passed to p.Send / p.Reply
				// further down — and platforms derive the parent message_id from
				// it for the reply quote. Without this reassignment, msg2's
				// reply would quote msg1's bubble.
				replyCtx = queued.replyCtx
				// Rich-mode per-turn state must reset too — otherwise EventText for
				// the queued message would StreamRichCardText against the previous
				// turn's cardID, overwriting that card's body with the new turn's
				// content. Same risk for partialText/toolSteps leaking across turns.
				cardMessageID = nil
				toolSteps = nil
				partialText = ""
				lastRichCardUpdate = time.Time{}
				lastRichCardLen = 0
				queuedRenderer := func(content string) string {
					return e.renderOutgoingContentForWorkspace(queued.platform, content, workspaceDir)
				}
				sp = newStreamPreview(e.streamPreview, queued.platform, queued.replyCtx, e.ctx, queuedRenderer)
				cp = e.newProgressWriter(queued.platform, queued.replyCtx, queuedRenderer)
				progressWriters = append(progressWriters, cp)

				// Reset streaming card state for the next turn
				streamCard = nil
				cardToolCalls = nil
				cardThinkingText = ""
				cardAnswerText.Reset()

				// Try to create a new streaming card for the queued turn
				if scp, ok := queued.platform.(StreamingCardPlatform); ok && !e.separateProgressMessages() {
					if sc, err := scp.CreateStreamingCard(e.ctx, queued.replyCtx); err != nil {
						slog.Warn("streaming card creation failed for queued turn", "error", err)
					} else {
						streamCard = sc
					}
				}

				// Send instant reply for queued turn if no streaming card is active.
				if e.instantReply.Enabled && streamCard == nil {
					replyContent := e.instantReply.Content
					if replyContent == "" {
						replyContent = e.i18n.T(MsgStarting)
					}
					e.send(queued.platform, queued.replyCtx, replyContent)
				}

				session.AddHistory("user", queued.content)
				// Persist queued user message immediately (mirror of the
				// initial AddHistory("user",...) save above).
				sessions.Save()

				if idleTimer != nil {
					if !idleTimer.Stop() {
						select {
						case <-idleTimer.C:
						default:
						}
					}
					idleTimer.Reset(e.eventIdleTimeout)
				}

				slog.Info("processing queued message",
					"session", sessionKey,
					"remaining_queue", remainingQueue,
				)
				continue
			}
			state.mu.Unlock()

			if pendingSend != nil {
				if err := <-pendingSend; err != nil {
					slog.Debug("async send error after EventResult", "error", err)
				}
			}

			// Add a "done" reaction after the final answer when supported. Skip
			// silent turns and rich card mode (the card itself shows done status).
			if !isSilent && !hasRichCard {
				if doneTI, ok := p.(TypingIndicatorDone); ok {
					doneReaction = func() { doneTI.AddDoneReaction(replyCtx) }
				}
			}

			return

		case EventStatus:
			if event.Content != "" {
				diagnostics.Send(p, replyCtx, event.Content, workspaceDir)
			}

		case EventError:
			cp.Finalize(ProgressCardStateFailed)
			sp.discard()
			state.mu.Lock()
			state.eventsNeedResync = true
			state.mu.Unlock()
			if hasRichCard && cardMessageID != nil {
				errCard := buildResolvedRichCard(CardStatusError, "", toolSteps, partialText, false, e.composeRichStatusFooter(false, turnStart, e.agent, state.agentSession, state.workspaceDir))
				if updater, ok := p.(MessageUpdater); ok {
					if err := updater.UpdateMessage(e.ctx, cardMessageID, errCard); err != nil {
						slog.Debug("rich card: failed to update error card", "platform", p.Name(), "error", err)
					}
				}
			}
			if event.Error != nil {
				errMsg := event.Error.Error()
				slog.Error("agent error", "error", event.Error)
				e.hooks.Emit(HookEvent{
					Event:      HookEventError,
					SessionKey: sessionKey,
					Platform:   p.Name(),
					Error:      event.Error.Error(),
				})
				userMsg := fmt.Sprintf(e.i18n.T(MsgError), errMsg)
				for _, h := range agentErrorHandlers {
					if strings.Contains(errMsg, h.contains) {
						userMsg = e.i18n.T(h.msgKey)
						break
					}
				}
				e.send(p, replyCtx, userMsg)
			}
			// Only drop queued messages if the agent session is dead.
			// Some agents (e.g. Codex) emit EventError for per-turn failures
			// while keeping the session alive for subsequent turns.
			if state.agentSession == nil || !state.agentSession.Alive() {
				e.notifyDroppedQueuedMessages(state, event.Error)
			}
			return
		}
	}

channelClosed:
	// Channel closed - process exited unexpectedly
	slog.Warn("agent process exited", "session_key", sessionKey)
	state.mu.Lock()
	state.eventsNeedResync = true
	state.mu.Unlock()
	e.notifyDroppedQueuedMessages(state, fmt.Errorf("agent process exited"))
	e.cleanupInteractiveState(sessionKey, state)

	if len(textParts) > 0 {
		state.mu.Lock()
		p := state.platform
		state.mu.Unlock()

		fullResponse := strings.Join(textParts, "")
		session.AddHistory("assistant", fullResponse)
		// Persist immediately — this path runs on abnormal channel close,
		// so deferring the save until the next foreground turn risks losing
		// the partial assistant response if the process exits next.
		sessions.Save()

		// Respect NO_REPLY even on abnormal exit so silent turns stay silent.
		if isSilentReply(fullResponse) {
			sp.discard()
			slog.Info("silent reply suppressed (channel closed)", "session", session.ID)
			return
		}
		if stripped, ok := stripTrailingSilent(fullResponse); ok {
			if strings.TrimSpace(stripped) == "" {
				sp.discard()
				return
			}
			fullResponse = stripped
		}

		e.hooks.Emit(HookEvent{
			Event:      HookEventMessageSent,
			SessionKey: sessionKey,
			Platform:   p.Name(),
			Content:    fullResponse,
		})

		if toolCount > 0 && segmentStart > 0 {
			sp.discard()
			if segmentStart < len(textParts) {
				unsent := strings.Join(textParts[segmentStart:], "")
				if unsent != "" {
					for _, chunk := range SplitMessageCodeFenceAware(unsent, maxPlatformMessageLen) {
						if err := sendWorkspaceWithError(p, replyCtx, chunk); err != nil {
							return
						}
					}
				}
			}
		} else if sp.finish(fullResponse, "") {
			slog.Debug("stream preview: finalized in-place (process exited)")
		} else {
			for _, chunk := range SplitMessageCodeFenceAware(fullResponse, maxPlatformMessageLen) {
				if err := sendWorkspaceWithError(p, replyCtx, chunk); err != nil {
					return
				}
			}
		}
	}
}

func mergeRichToolResult(steps []ToolStep, event Event, result string, maxLen int) []ToolStep {
	toolName := strings.TrimSpace(event.ToolName)
	if toolName == "" {
		toolName = "Tool"
	}

	idx := -1
	for i := len(steps) - 1; i >= 0; i-- {
		if steps[i].Kind == ToolStepKindThinking {
			continue
		}
		if strings.TrimSpace(steps[i].Name) == "" || strings.TrimSpace(steps[i].Name) == toolName {
			idx = i
			break
		}
	}
	if idx == -1 {
		summary := strings.TrimSpace(event.ToolInput)
		if summary != "" {
			summary = truncateIf(summary, maxLen)
		}
		steps = append(steps, ToolStep{
			Kind:    ToolStepKindTool,
			Name:    toolName,
			Summary: summary,
		})
		idx = len(steps) - 1
	}

	if strings.TrimSpace(steps[idx].Name) == "" {
		steps[idx].Name = toolName
	}
	if steps[idx].Kind == "" {
		steps[idx].Kind = ToolStepKindTool
	}
	if strings.TrimSpace(steps[idx].Summary) == "" && strings.TrimSpace(event.ToolInput) != "" {
		steps[idx].Summary = truncateIf(strings.TrimSpace(event.ToolInput), maxLen)
	}
	steps[idx].Result = result
	steps[idx].Status = strings.TrimSpace(event.ToolStatus)
	steps[idx].ExitCode = event.ToolExitCode
	steps[idx].Success = event.ToolSuccess
	steps[idx].Done = true
	return steps
}

// notifyDroppedQueuedMessages drains pendingMessages from the state and
// sends an error notification to each queued message's sender. Called when
// the event loop exits abnormally (EventError, channel closed) and queued
// messages can no longer be delivered to the agent.
func (e *Engine) notifyDroppedQueuedMessages(state *interactiveState, reason error) {
	state.mu.Lock()
	remaining := state.pendingMessages
	state.pendingMessages = nil
	state.mu.Unlock()
	for _, q := range remaining {
		e.send(q.platform, q.replyCtx, fmt.Sprintf(e.i18n.T(MsgError), reason))
	}
}

// drainPendingMessages processes all queued messages in the state's pendingMessages
// queue. It atomically unlocks the session when the queue is empty (while holding
// state.mu) to close the race window between "queue empty" and "session unlocked".
// Returns true if the session was unlocked by this call.
func (e *Engine) drainPendingMessages(state *interactiveState, session *Session, sessions *SessionManager, sessionKey string) bool {
	for {
		state.mu.Lock()
		if len(state.pendingMessages) == 0 {
			session.Unlock()
			state.mu.Unlock()
			return true
		}
		droppedStale := 0
		for len(state.pendingMessages) > 0 && e.isQueuedUserMessageStaleForDrainLocked(state, state.pendingMessages[0].userMessageTimeMs) {
			state.pendingMessages = state.pendingMessages[1:]
			droppedStale++
		}
		if droppedStale > 0 {
			slog.Info("dropped stale queued user messages while draining",
				"session", sessionKey,
				"dropped", droppedStale,
			)
		}
		if len(state.pendingMessages) == 0 {
			session.Unlock()
			state.mu.Unlock()
			return true
		}
		queued := state.pendingMessages[0]
		state.pendingMessages = state.pendingMessages[1:]
		state.platform = queued.platform
		state.replyCtx = queued.replyCtx
		state.currentMessageID = queued.messageID
		state.fromVoice = queued.fromVoice
		state.currentTurnUserMessageTimeMs = queued.userMessageTimeMs
		state.mu.Unlock()

		prompt := e.buildSenderPrompt(queued.content, queued.userID, queued.userName, queued.msgPlatform, queued.msgSessionKey, queued.channelKey)

		state.mu.Lock()
		as := state.agentSession // capture under lock to avoid race with cleanup (mirrors #1436)
		state.mu.Unlock()
		if as == nil || !as.Alive() {
			e.send(queued.platform, queued.replyCtx, fmt.Sprintf(e.i18n.T(MsgError), "agent session ended"))
			e.notifyDroppedQueuedMessages(state, fmt.Errorf("agent session ended"))
			return false
		}

		drainEvents(as.Events())

		session.AddHistory("user", queued.content)

		sendDone := make(chan error, 1)
		go func() {
			if as == nil {
				sendDone <- fmt.Errorf("agent session became nil")
				return
			}
			sendDone <- as.Send(prompt, queued.messageID, queued.images, queued.files)
		}()

		var stopTyping func()
		if ti, ok := queued.platform.(TypingIndicator); ok {
			stopTyping = ti.StartTyping(e.ctx, queued.replyCtx)
		}

		slog.Info("processing queued message", "session", sessionKey)
		state.mu.Lock()
		state.currentUserID = queued.userID
		state.mu.Unlock()
		e.processInteractiveEvents(state, session, sessions, sessionKey, queued.messageID, time.Now(), stopTyping, sendDone, queued.replyCtx)
	}
}

// ──────────────────────────────────────────────────────────────
// Command handling
// ──────────────────────────────────────────────────────────────

// builtinCommands maps canonical command names to their aliases/full names.
// The first entry is the canonical name used for prefix matching.
var builtinCommands = []struct {
	names []string
	id    string
}{
	{[]string{"new"}, "new"},
	{[]string{"list", "sessions"}, "list"},
	{[]string{"switch"}, "switch"},
	{[]string{"name", "rename"}, "name"},
	{[]string{"current"}, "current"},
	{[]string{"history"}, "history"},
	{[]string{"allow"}, "allow"},
	{[]string{"model"}, "model"},
	{[]string{"effort"}, "effort"},
	{[]string{"mode"}, "mode"},
	{[]string{"quiet"}, "quiet"},
	{[]string{"provider"}, "provider"},
	{[]string{"compact"}, "compact"},
	{[]string{"stop"}, "stop"},
	{[]string{"cancel"}, "cancel"},
	{[]string{"help"}, "help"},
	{[]string{"commands", "command", "cmd"}, "commands"},
	{[]string{"restart"}, "restart"},
	{[]string{"alias"}, "alias"},
	{[]string{"delete", "del", "rm"}, "delete"},
	{[]string{"search", "find"}, "search"},
	{[]string{"shell", "sh", "exec", "run"}, "shell"},
	{[]string{"show"}, "show"},
	{[]string{"dir", "cd", "chdir", "workdir"}, "dir"},
	{[]string{"tts"}, "tts"},
	{[]string{"whoami", "myid"}, "whoami"},
	{[]string{"diff"}, "diff"},
}

// matchPrefix finds a unique command matching the given prefix.
// Returns the command id or "" if no match / ambiguous.
func matchPrefix(prefix string, candidates []struct {
	names []string
	id    string
}) string {
	// Exact match first
	for _, c := range candidates {
		for _, n := range c.names {
			if prefix == n {
				return c.id
			}
		}
	}
	// Prefix match
	var matched string
	for _, c := range candidates {
		for _, n := range c.names {
			if strings.HasPrefix(n, prefix) {
				if matched != "" && matched != c.id {
					return "" // ambiguous
				}
				matched = c.id
				break
			}
		}
	}
	return matched
}

// matchSubCommand does prefix matching against a flat list of subcommand names.
func matchSubCommand(input string, candidates []string) string {
	for _, c := range candidates {
		if input == c {
			return c
		}
	}
	var matched string
	for _, c := range candidates {
		if strings.HasPrefix(c, input) {
			if matched != "" {
				return input // ambiguous → return raw input (will hit default)
			}
			matched = c
		}
	}
	if matched != "" {
		return matched
	}
	return input
}

// splitCommandArgs splits a command string into tokens, respecting single- and
// double-quoted groups so paths like "/workspace bind '/my path/foo'" work
// correctly (#1211). Quotes are stripped from the resulting tokens.
func splitCommandArgs(s string) []string {
	var tokens []string
	var cur strings.Builder
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\'' && !inDouble:
			inSingle = !inSingle
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case (c == ' ' || c == '\t') && !inSingle && !inDouble:
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

func (e *Engine) handleCommand(p Platform, msg *Message, raw string) bool {
	parts := splitCommandArgs(raw)
	cmd := strings.ToLower(strings.TrimPrefix(parts[0], "/"))
	args := parts[1:]

	cmdID := matchPrefix(cmd, builtinCommands)

	// Resolve effective disabled commands: role-based if available, else project-level
	e.userRolesMu.RLock()
	disabledCmds := e.disabledCmds
	urm := e.userRoles
	e.userRolesMu.RUnlock()
	if urm != nil {
		if role := urm.ResolveRole(msg.UserID); role != nil {
			disabledCmds = role.DisabledCmds
		}
	}

	if cmdID != "" && disabledCmds[cmdID] {
		slog.Info("audit: command_blocked",
			"user_id", msg.UserID, "platform", msg.Platform,
			"project", e.name, "command", cmdID, "reason", "disabled")
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandDisabled), "/"+cmdID))
		return true
	}

	if cmdID != "" && isPrivilegedCommandInvocation(cmdID, args) && !e.isAdmin(msg.UserID) {
		slog.Info("audit: command_blocked",
			"user_id", msg.UserID, "platform", msg.Platform,
			"project", e.name, "command", cmdID, "reason", "unauthorized")
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAdminRequired), "/"+cmdID))
		return true
	}

	if cmdID != "" {
		slog.Info("audit: command_executed",
			"user_id", msg.UserID, "platform", msg.Platform,
			"project", e.name, "command", cmdID)
	}

	switch cmdID {
	case "new":
		e.cmdNew(p, msg, args)
	case "list":
		e.cmdList(p, msg, args)
	case "switch":
		e.cmdSwitch(p, msg, args)
	case "name":
		e.cmdName(p, msg, args)
	case "current":
		e.cmdCurrent(p, msg)
	case "history":
		e.cmdHistory(p, msg, args)
	case "allow":
		e.cmdAllow(p, msg, args)
	case "model":
		e.cmdModel(p, msg, args)
	case "effort":
		e.cmdEffort(p, msg, args)
	case "mode":
		e.cmdMode(p, msg, args)
	case "quiet":
		e.cmdQuiet(p, msg, args)
	case "provider":
		e.cmdProvider(p, msg, args)
	case "compact":
		e.cmdCompact(p, msg)
	case "stop":
		e.cmdStop(p, msg)
	case "cancel":
		e.cmdCancel(p, msg)
	case "help":
		e.cmdHelp(p, msg)
	case "start":
		e.cmdStart(p, msg)
	case "commands":
		e.cmdCommands(p, msg, args)
	case "restart":
		e.cmdRestart(p, msg)
	case "alias":
		e.cmdAlias(p, msg, args)
	case "delete":
		e.cmdDelete(p, msg, args)
	case "search":
		e.cmdSearch(p, msg, args)
	case "shell":
		e.cmdShell(p, msg, raw)
	case "diff":
		e.cmdDiff(p, msg, raw)
	case "show":
		e.cmdShow(p, msg, args)
	case "dir":
		e.cmdDir(p, msg, args)
	case "tts":
		e.cmdTTS(p, msg, args)
	case "whoami":
		e.cmdWhoami(p, msg)
	default:
		if custom, ok := e.commands.Resolve(cmd); ok {
			if disabledCmds[strings.ToLower(custom.Name)] {
				slog.Info("audit: command_blocked",
					"user_id", msg.UserID, "platform", msg.Platform,
					"project", e.name, "command", custom.Name, "reason", "disabled")
				e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandDisabled), "/"+custom.Name))
				return true
			}
			slog.Info("audit: command_executed",
				"user_id", msg.UserID, "platform", msg.Platform,
				"project", e.name, "command", custom.Name, "type", "custom")
			e.executeCustomCommand(p, msg, custom, args)
			return true
		}
		// Not an agent-bridge command — pass it through to the agent silently.
		return false
	}
	return true
}

func (e *Engine) cmdNew(p Platform, msg *Message, args []string) {
	_, sessions, interactiveKey := e.commandContext(p, msg)

	slog.Info("cmdNew: cleaning up old session", "session_key", msg.SessionKey)
	e.takeStagedAttachments(interactiveKey)
	e.cleanupInteractiveState(interactiveKey)
	slog.Info("cmdNew: cleanup done, creating new session", "session_key", msg.SessionKey)

	// Clear old session's agent session ID so it cannot be resumed
	old := sessions.GetOrCreateActive(msg.SessionKey)
	old.SetAgentSessionID("", "")
	old.ClearHistory()
	sessions.Save()

	name := ""
	if len(args) > 0 {
		name = strings.Join(args, " ")
	}
	sessions.NewSession(msg.SessionKey, name)
	if name != "" {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgNewSessionCreatedName), name))
	} else {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNewSessionCreated))
	}
}

// applySessionFilter conditionally filters agent sessions based on the
// filter_external_sessions config. When disabled (default), all sessions are
// returned. When enabled, only sessions tracked by agent-bridge are shown.
func (e *Engine) applySessionFilter(sessions []AgentSessionInfo, sm *SessionManager) []AgentSessionInfo {
	if !e.filterExternalSessions {
		return sessions
	}
	return filterOwnedSessions(sessions, sm.KnownAgentSessionIDs())
}

// filterOwnedSessions removes agent sessions that are not tracked by agent-bridge's
// session manager. This prevents external CLI sessions in the same work_dir from
// appearing in /list, /switch, /delete, etc. If the session manager has no tracked
// agent sessions at all (e.g. first run), all sessions are returned unfiltered.
func filterOwnedSessions(sessions []AgentSessionInfo, known map[string]struct{}) []AgentSessionInfo {
	if len(known) == 0 {
		return sessions
	}
	filtered := make([]AgentSessionInfo, 0, len(sessions))
	for _, s := range sessions {
		if _, ok := known[s.ID]; ok {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

const listPageSize = 20

// dirCardPageSize is the max directory history rows per card page (Feishu / other card UIs).
const dirCardPageSize = 20

func (e *Engine) cmdList(p Platform, msg *Message, args []string) {
	agent, sessions, _ := e.commandContext(p, msg)

	if !supportsCards(p) {
		agentSessions, err := agent.ListSessions(e.ctx)
		if err != nil {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgListError), err))
			return
		}
		agentSessions = e.applySessionFilter(agentSessions, sessions)
		if len(agentSessions) == 0 {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgListEmpty))
			return
		}

		total := len(agentSessions)
		totalPages := (total + listPageSize - 1) / listPageSize

		page := 1
		if len(args) > 0 {
			if n, err := strconv.Atoi(args[0]); err == nil && n > 0 {
				page = n
			}
		}
		if page > totalPages {
			page = totalPages
		}

		start := (page - 1) * listPageSize
		end := start + listPageSize
		if end > total {
			end = total
		}

		agentName := agent.Name()
		activeSession := sessions.GetOrCreateActive(msg.SessionKey)
		activeAgentID := activeSession.GetAgentSessionID()

		var sb strings.Builder
		if totalPages > 1 {
			sb.WriteString(fmt.Sprintf(e.i18n.T(MsgListTitlePaged), agentName, total, page, totalPages))
		} else {
			sb.WriteString(fmt.Sprintf(e.i18n.T(MsgListTitle), agentName, total))
		}
		for i := start; i < end; i++ {
			s := agentSessions[i]
			marker := "[ ]"
			if s.ID == activeAgentID {
				marker = "(current)"
			}
			displayName := sessions.GetSessionName(s.ID)
			if displayName == "" {
				displayName = strings.ReplaceAll(s.Summary, "\n", " ")
				displayName = strings.Join(strings.Fields(displayName), " ")
				if displayName == "" {
					displayName = "(empty)"
				}
				if len([]rune(displayName)) > 40 {
					displayName = string([]rune(displayName)[:40]) + "…"
				}
			}
			sb.WriteString(fmt.Sprintf("%s **%d.** %s · **%d** msgs · %s\n",
				marker, i+1, displayName, s.MessageCount, s.ModifiedAt.Format("01-02 15:04")))
		}
		if totalPages > 1 {
			sb.WriteString(fmt.Sprintf(e.i18n.T(MsgListPageHint), page, totalPages))
		}
		sb.WriteString(e.i18n.T(MsgListSwitchHint))
		e.reply(p, msg.ReplyCtx, sb.String())
		return
	}

	page := 1
	if len(args) > 0 {
		if n, err := strconv.Atoi(args[0]); err == nil && n > 0 {
			page = n
		}
	}
	card, err := e.renderListCard(msg.SessionKey, page)
	if err != nil {
		e.reply(p, msg.ReplyCtx, err.Error())
		return
	}
	e.replyWithCard(p, msg.ReplyCtx, card)
}

func (e *Engine) cmdSwitch(p Platform, msg *Message, args []string) {
	if len(args) == 0 {
		e.reply(p, msg.ReplyCtx, "Usage: /switch <number | id_prefix | name>")
		return
	}
	query := strings.TrimSpace(strings.Join(args, " "))

	slog.Info("cmdSwitch: listing agent sessions", "session_key", msg.SessionKey)
	agent, sessions, interactiveKey := e.commandContext(p, msg)
	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgError, err))
		return
	}
	agentSessions = e.applySessionFilter(agentSessions, sessions)

	matched := e.matchSession(agentSessions, sessions, query)
	if matched == nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgSwitchNoMatch), query))
		return
	}

	slog.Info("cmdSwitch: cleaning up old session", "session_key", msg.SessionKey)
	e.takeStagedAttachments(interactiveKey)
	e.cleanupInteractiveState(interactiveKey)
	slog.Info("cmdSwitch: cleanup done", "session_key", msg.SessionKey)

	// NOTE: Do NOT call session.ClearHistory() on the returned Session.
	// When switching back to a known agent_session_id, SwitchToAgentSession
	// returns the *existing* Session object whose History reflects the
	// original conversation; wiping it makes /history return empty after a
	// /switch round-trip. When SwitchToAgentSession creates a fresh Session
	// (no prior match), History is already nil, so preserving is a no-op.
	_ = sessions.SwitchToAgentSession(msg.SessionKey, matched.ID, agent.Name(), matched.Summary)

	shortID := matched.ID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}
	displayName := sessions.GetSessionName(matched.ID)
	if displayName == "" {
		displayName = matched.Summary
	}
	e.reply(p, msg.ReplyCtx,
		e.i18n.Tf(MsgSwitchSuccess, displayName, shortID, matched.MessageCount))
}

// matchSession resolves a user query to an agent session. Priority:
//  1. Numeric index (1-based, matching /list output)
//  2. Exact custom name match (case-insensitive)
//  3. Session ID prefix match
//  4. Custom name prefix match (case-insensitive)
//  5. Summary substring match (case-insensitive)
func (e *Engine) matchSession(sessions []AgentSessionInfo, manager *SessionManager, query string) *AgentSessionInfo {
	if len(sessions) == 0 {
		return nil
	}

	// 1. Numeric index
	if idx, err := strconv.Atoi(query); err == nil && idx >= 1 && idx <= len(sessions) {
		return &sessions[idx-1]
	}

	queryLower := strings.ToLower(query)

	// 2. Exact custom name match
	for i := range sessions {
		name := manager.GetSessionName(sessions[i].ID)
		if name != "" && strings.ToLower(name) == queryLower {
			return &sessions[i]
		}
	}

	// 3. Session ID prefix match
	for i := range sessions {
		if strings.HasPrefix(sessions[i].ID, query) {
			return &sessions[i]
		}
	}

	// 4. Custom name prefix match
	for i := range sessions {
		name := manager.GetSessionName(sessions[i].ID)
		if name != "" && strings.HasPrefix(strings.ToLower(name), queryLower) {
			return &sessions[i]
		}
	}

	// 5. Summary substring match
	for i := range sessions {
		if sessions[i].Summary != "" && strings.Contains(strings.ToLower(sessions[i].Summary), queryLower) {
			return &sessions[i]
		}
	}

	return nil
}

func (e *Engine) commandWorkDir(agent Agent, msg *Message) string {
	if switcher, ok := agent.(WorkDirSwitcher); ok {
		if wd := strings.TrimSpace(switcher.GetWorkDir()); wd != "" {
			return normalizeWorkspacePath(wd)
		}
	}
	if wd, ok := agent.(interface{ GetWorkDir() string }); ok {
		if dir := strings.TrimSpace(wd.GetWorkDir()); dir != "" {
			return normalizeWorkspacePath(dir)
		}
	}
	if wd, ok := e.agent.(interface{ GetWorkDir() string }); ok {
		if dir := strings.TrimSpace(wd.GetWorkDir()); dir != "" {
			return normalizeWorkspacePath(dir)
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		return normalizeWorkspacePath(cwd)
	}
	return ""
}

func (e *Engine) buildReplyFooter(agent Agent, session AgentSession, workspaceDir string, contextLeft string) string {
	if !e.replyFooterEnabled || agent == nil {
		return ""
	}

	var parts []string
	hasStatus := false
	if e.showContextIndicator {
		contextLeft = strings.TrimSpace(contextLeft)
		contextFirst := strings.HasPrefix(contextLeft, "[ctx:")
		if contextFirst {
			parts = append(parts, contextLeft)
			hasStatus = true
		}
		if model := replyFooterModel(session, agent); model != "" {
			parts = append(parts, model)
			hasStatus = true
		}
		if effort := replyFooterReasoningEffort(session, agent); effort != "" {
			parts = append(parts, effort)
			hasStatus = true
		}
		if contextFirst {
			// Already added before model so "[ctx]" stays on the same footer line.
		} else if contextLeft != "" {
			parts = append(parts, contextLeft)
			hasStatus = true
		} else if usage := e.replyFooterUsageText(session, agent); usage != "" {
			parts = append(parts, usage)
			hasStatus = true
		}
	}
	if e.showWorkdirIndicator {
		if dir := replyFooterWorkDir(session, agent, workspaceDir); dir != "" {
			parts = append(parts, dir)
		}
	}
	// A workdir alone is not a useful status signal (see #701), so suppress
	// the entire footer unless at least one status segment from line 1 is
	// present.
	if !hasStatus {
		return ""
	}
	return strings.Join(parts, " · ")
}

// composeRichStatusFooter assembles the multi-line statusFooter passed to
// RichCardSupporter.BuildRichCard. Layout (skipping any empty line):
//
//	line 1: ⏱ <i18n elapsed>                                  (subject to e.replyFooterEnabled)
//	line 2: model · out N · in N cw N cr N · ctx N%           (subject to e.showContextIndicator)
//	line 3: <workdir>                                         (subject to e.showWorkdirIndicator)
//
// Returns "" when the master replyFooterEnabled toggle is off, or while the
// turn is still streaming (footer represents finalized turn metadata —
// token counts aren't yet settled and a live-updating elapsed line creates
// visual noise during streaming. Header status badge already signals "Working").
func (e *Engine) composeRichStatusFooter(streaming bool, turnStart time.Time, agent Agent, session AgentSession, workspaceDir string) string {
	if !e.replyFooterEnabled {
		return ""
	}
	if streaming {
		return ""
	}
	var lines []string

	// Line 1: elapsed timer (now always the "done" form since streaming branch returned above)
	lines = append(lines, formatElapsed(time.Since(turnStart), streaming))

	// Line 2: model + effort + token usage detail + ctx %
	if e.showContextIndicator {
		usage := replyFooterSessionContextUsage(session)
		model := replyFooterModel(session, agent)
		effort := replyFooterReasoningEffort(session, agent)
		if line := buildClaudeStatusLineFooter(model, effort, usage); line != "" {
			lines = append(lines, line)
		} else if fallback := e.replyFooterUsageText(session, agent); fallback != "" {
			// fallback for non-claudecode agents that still expose UsageReporter
			parts := []string{}
			if model != "" {
				parts = append(parts, model)
			}
			if effort != "" {
				parts = append(parts, effort)
			}
			parts = append(parts, fallback)
			lines = append(lines, strings.Join(parts, " · "))
		}
	}

	// Line 3: workdir
	if e.showWorkdirIndicator {
		if dir := replyFooterWorkDir(session, agent, workspaceDir); dir != "" {
			lines = append(lines, dir)
		}
	}

	return strings.Join(lines, "\n")
}

// buildClaudeStatusLineFooter renders the rich-card line-2 token-usage detail:
//
//	claude-opus-4-7[1m] · xhigh · out 168 · in 1 cw 971 cr 40.8k · ctx 4%
//
// Sections (each skipped when its data is missing):
//   - model: from session GetModel() / agent.Name()
//   - effort: reasoning_effort (Codex / Claude high/medium/low/xhigh/max)
//   - token counts: out (output) · in (new input) · cw (cache create) · cr (cache read)
//   - ctx %: UsedTokens / ContextWindow, capped at 100%
//
// Returns "" when usage is nil and no model is known.
func buildClaudeStatusLineFooter(model, effort string, usage *ContextUsage) string {
	var parts []string
	if model != "" {
		parts = append(parts, model)
	}
	if effort != "" {
		parts = append(parts, effort)
	}
	if usage != nil {
		var counts []string
		if usage.OutputTokens > 0 {
			counts = append(counts, fmt.Sprintf("out %s", formatStatusTokenCount(usage.OutputTokens)))
		}
		if usage.InputTokens > 0 {
			counts = append(counts, fmt.Sprintf("in %s", formatStatusTokenCount(usage.InputTokens)))
		}
		if usage.CacheCreationInputTokens > 0 {
			counts = append(counts, fmt.Sprintf("cw %s", formatStatusTokenCount(usage.CacheCreationInputTokens)))
		}
		if usage.CachedInputTokens > 0 {
			counts = append(counts, fmt.Sprintf("cr %s", formatStatusTokenCount(usage.CachedInputTokens)))
		}
		if len(counts) > 0 {
			parts = append(parts, strings.Join(counts, " "))
		}
		if usage.ContextWindow > 0 {
			used := usage.UsedTokens
			if used <= 0 && usage.TotalTokens > 0 {
				used = usage.TotalTokens
			}
			if used > 0 {
				pct := used * 100 / usage.ContextWindow
				if pct > 100 {
					pct = 100
				}
				parts = append(parts, fmt.Sprintf("ctx %d%%", pct))
			}
		}
	}
	return strings.Join(parts, " · ")
}

// formatStatusTokenCount renders an integer token count compactly.
//
//	< 1000      -> "168"
//	< 1_000_000 -> "40.8k"
//	else        -> "1.2M"
//
// Negative inputs clamp to zero (defensive against bad token deltas).
func formatStatusTokenCount(n int) string {
	if n < 0 {
		n = 0
	}
	switch {
	case n < 1000:
		return fmt.Sprintf("%d", n)
	case n < 1_000_000:
		return fmt.Sprintf("%.1fk", float64(n)/1000.0)
	default:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000.0)
	}
}

// formatElapsed renders a turn elapsed duration for the rich card status
// footer: "Running for 12.3s..." while streaming, "Elapsed 1m 23s" after.
func formatElapsed(d time.Duration, streaming bool) string {
	if d < 0 {
		d = 0
	}
	totalSec := int64(d / time.Second)
	var dur string
	switch {
	case d < time.Minute:
		dur = fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		dur = fmt.Sprintf("%dm %02ds", totalSec/60, totalSec%60)
	default:
		dur = fmt.Sprintf("%dh %02dm", totalSec/3600, (totalSec%3600)/60)
	}
	if streaming {
		return fmt.Sprintf("Running for %s...", dur)
	}
	return fmt.Sprintf("Elapsed %s", dur)
}

func replyFooterModel(session AgentSession, agent Agent) string {
	if session != nil {
		if getter, ok := session.(interface{ GetModel() string }); ok {
			if model := strings.TrimSpace(getter.GetModel()); model != "" {
				return model
			}
		}
	}
	if getter, ok := agent.(interface{ GetModel() string }); ok {
		return strings.TrimSpace(getter.GetModel())
	}
	return ""
}

func replyFooterReasoningEffort(session AgentSession, agent Agent) string {
	if session != nil {
		if getter, ok := session.(interface{ GetReasoningEffort() string }); ok {
			if effort := strings.TrimSpace(getter.GetReasoningEffort()); effort != "" {
				return effort
			}
		}
	}
	if getter, ok := agent.(interface{ GetReasoningEffort() string }); ok {
		return strings.TrimSpace(getter.GetReasoningEffort())
	}
	return ""
}

func (e *Engine) replyFooterUsageText(session AgentSession, agent Agent) string {
	ctx, cancel := context.WithTimeout(e.ctx, replyFooterUsageTimeout)
	defer cancel()

	if session != nil {
		if reporter, ok := session.(UsageReporter); ok {
			if report, err := reporter.GetUsage(ctx); err == nil {
				return formatReplyFooterUsage(report, e.i18n)
			}
		}
	}

	reporter, ok := agent.(UsageReporter)
	if !ok {
		return ""
	}

	e.replyFooterMu.Lock()
	cached := e.replyFooterUsage
	e.replyFooterMu.Unlock()
	if !cached.fetchedAt.IsZero() && time.Since(cached.fetchedAt) < replyFooterUsageCacheTTL {
		return cached.text
	}

	text := ""
	if report, err := reporter.GetUsage(ctx); err == nil {
		text = formatReplyFooterUsage(report, e.i18n)
	} else if !cached.fetchedAt.IsZero() {
		text = cached.text
	}

	e.replyFooterMu.Lock()
	e.replyFooterUsage = replyFooterUsageCache{text: text, fetchedAt: time.Now()}
	e.replyFooterMu.Unlock()
	return text
}

func formatReplyFooterUsage(report *UsageReport, i18n *I18n) string {
	if report == nil || i18n == nil {
		return ""
	}
	window, _ := selectUsageWindows(report)
	if window == nil {
		return ""
	}
	remaining := 100 - window.UsedPercent
	if remaining < 0 {
		remaining = 0
	}
	if remaining > 100 {
		remaining = 100
	}
	return i18n.Tf(MsgReplyFooterRemaining, remaining)
}

func replyFooterSessionContextUsage(session AgentSession) *ContextUsage {
	if session == nil {
		return nil
	}
	reporter, ok := session.(ContextUsageReporter)
	if !ok {
		return nil
	}
	return reporter.GetContextUsage()
}

func replyFooterContextText(usage *ContextUsage, i18n *I18n) string {
	if usage == nil || i18n == nil {
		return ""
	}
	if usage.ContextWindow <= 0 {
		return ""
	}

	usedTokens := usage.UsedTokens
	if usedTokens <= 0 {
		switch {
		case usage.TotalTokens > 0:
			usedTokens = usage.TotalTokens
		case usage.InputTokens > 0 || usage.OutputTokens > 0:
			usedTokens = usage.InputTokens + usage.OutputTokens
		default:
			return ""
		}
	}

	baseline := usage.BaselineTokens
	if baseline < 0 {
		baseline = 0
	}
	if usage.ContextWindow <= baseline {
		return i18n.Tf(MsgReplyFooterRemaining, 0)
	}

	effectiveWindow := usage.ContextWindow - baseline
	effectiveUsed := usedTokens - baseline
	if effectiveUsed < 0 {
		effectiveUsed = 0
	}
	remaining := effectiveWindow - effectiveUsed
	if remaining < 0 {
		remaining = 0
	}

	left := int(math.Round(float64(remaining) / float64(effectiveWindow) * 100))
	if left < 0 {
		left = 0
	}
	if left > 100 {
		left = 100
	}
	return i18n.Tf(MsgReplyFooterRemaining, left)
}

func replyFooterWorkDir(session AgentSession, agent Agent, workspaceDir string) string {
	dir := strings.TrimSpace(workspaceDir)
	if dir == "" {
		if session != nil {
			if wd, ok := session.(interface{ GetWorkDir() string }); ok {
				dir = strings.TrimSpace(wd.GetWorkDir())
			}
		}
	}
	if dir == "" {
		if switcher, ok := agent.(WorkDirSwitcher); ok {
			dir = strings.TrimSpace(switcher.GetWorkDir())
		}
	}
	if dir == "" {
		if wd, ok := agent.(interface{ GetWorkDir() string }); ok {
			dir = strings.TrimSpace(wd.GetWorkDir())
		}
	}
	if dir == "" {
		return ""
	}
	return compactReplyFooterPath(dir)
}

func compactReplyFooterPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	cleaned := filepath.Clean(path)
	normalized := normalizeWorkspacePath(cleaned)
	if home, err := os.UserHomeDir(); err == nil {
		homeCleaned := filepath.Clean(home)
		homeNormalized := normalizeWorkspacePath(homeCleaned)
		for _, candidate := range []struct {
			path string
			home string
		}{
			{cleaned, homeCleaned},
			{normalized, homeNormalized},
			{cleaned, homeNormalized},
			{normalized, homeCleaned},
		} {
			if display, ok := replyFooterHomeRelativePath(candidate.path, candidate.home); ok {
				return display
			}
		}
	}
	return filepath.ToSlash(normalized)
}

func replyFooterHomeRelativePath(path, home string) (string, bool) {
	if path == "" || home == "" {
		return "", false
	}
	if path == home {
		return "~", true
	}
	prefix := home + string(os.PathSeparator)
	if strings.HasPrefix(path, prefix) {
		return "~" + filepath.ToSlash(strings.TrimPrefix(path, home)), true
	}
	return "", false
}

// buildClaudeStatusLineFooter renders a CCD-statusline-style footer for the
// reply, composed of two lines:
//
//	line 1 (controlled by show_context_indicator): <model id> · [effort:X ·] out N · in N cw N cr N · ctx N%
//	line 2 (controlled by show_workdir_indicator): <workspace dir>
//
// Returns "" if reply_footer is disabled, or if the active session does not
// expose per-turn cache-token data (i.e. this is not claudecode or no result
// event has arrived yet) so callers fall back to the default footer.
func (e *Engine) buildClaudeStatusLineFooter(agent Agent, session AgentSession, workspaceDir string) string {
	if !e.replyFooterEnabled {
		return ""
	}
	usage := replyFooterSessionContextUsage(session)
	if usage == nil || usage.ContextWindow <= 0 {
		return ""
	}
	// Only emit the CCD-style footer when we have the cache-token signals
	// that CCD's statusline consumes. Other agents (codex, gemini) fall
	// through to the default footer.
	if usage.CachedInputTokens == 0 && usage.CacheCreationInputTokens == 0 {
		return ""
	}

	var line1 string
	if e.showContextIndicator {
		used := usage.UsedTokens
		if used <= 0 {
			used = usage.InputTokens + usage.CachedInputTokens + usage.CacheCreationInputTokens
		}
		pct := int(math.Round(float64(used) * 100 / float64(usage.ContextWindow)))
		if pct < 0 {
			pct = 0
		}
		if pct > 100 {
			pct = 100
		}

		// Compose:
		//   <model id> · [effort:X ·] out N · in N cw N cr N · ctx N%
		// `·` separates major segments; tokens-in tier (in/cw/cr) groups under
		// one segment because cw/cr are just cache-tiered variants of input.
		// Raw model id is preserved (e.g. "claude-opus-4-7[1m]") for diagnostic
		// clarity over a prettified display name.
		var line1Parts []string
		if model := strings.TrimSpace(replyFooterModel(session, agent)); model != "" {
			line1Parts = append(line1Parts, model)
		}
		if effort := strings.TrimSpace(replyFooterReasoningEffort(session, agent)); effort != "" {
			line1Parts = append(line1Parts, "effort:"+effort)
		}
		line1Parts = append(line1Parts, fmt.Sprintf("out %s", formatStatusTokenCount(usage.OutputTokens)))
		line1Parts = append(line1Parts, fmt.Sprintf("in %s cw %s cr %s",
			formatStatusTokenCount(usage.InputTokens),
			formatStatusTokenCount(usage.CacheCreationInputTokens),
			formatStatusTokenCount(usage.CachedInputTokens)))
		line1Parts = append(line1Parts, fmt.Sprintf("ctx %d%%", pct))
		line1 = strings.Join(line1Parts, " · ")
	}

	var line2 string
	if e.showWorkdirIndicator {
		line2 = replyFooterWorkDir(session, agent, workspaceDir)
	}

	switch {
	case line1 != "" && line2 != "":
		return line1 + "\n" + line2
	case line1 != "":
		return line1
	case line2 != "":
		return line2
	default:
		return ""
	}
}

// sendChunksWithStatusFooter splits body across maxPlatformMessageLen and sends
// each chunk via the supplied sendFn. The final chunk carries the structured
// statusFooter: platforms implementing StatusFooterSender render it as a
// small/dim block; otherwise the footer is appended inline via
// appendReplyFooter. Returns true on success, false if any send failed (in
// which case caller should bail). sendFn is the workspace-aware send closure
// (so the helper picks up workspace transforms like path remapping).
func sendChunksWithStatusFooter(ctx context.Context, p Platform, replyCtx any, body, statusFooter string, sendFn func(Platform, any, string) error) bool {
	chunks := SplitMessageCodeFenceAware(body, maxPlatformMessageLen)
	for i, chunk := range chunks {
		isLast := i == len(chunks)-1
		if isLast && statusFooter != "" {
			if sfs, ok := p.(StatusFooterSender); ok {
				if err := sfs.SendWithStatusFooter(ctx, replyCtx, chunk, statusFooter); err == nil {
					continue
				} else {
					slog.Warn("SendWithStatusFooter failed, falling back to inline footer", "error", err)
				}
			}
			chunk = appendReplyFooter(chunk, statusFooter)
		}
		if err := sendFn(p, replyCtx, chunk); err != nil {
			return false
		}
	}
	return true
}

func appendReplyFooter(content, footer string) string {
	if footer == "" {
		return content
	}
	content = strings.TrimRight(content, "\n")
	last30 := content
	if len(last30) > 30 {
		last30 = last30[len(last30)-30:]
	}
	slog.Debug("appendReplyFooter", "content_len", len(content), "footer", footer, "content_last30", last30)
	if content == "" {
		return "*" + footer + "*"
	}
	return content + "\n\n*" + footer + "*"
}

func (e *Engine) cmdShow(p Platform, msg *Message, args []string) {
	rawRef := strings.TrimSpace(strings.Join(args, " "))
	if rawRef == "" {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgShowUsage))
		return
	}

	agent, _, _ := e.commandContext(p, msg)
	workDir := e.commandWorkDir(agent, msg)
	req, err := buildReferenceViewRequest(rawRef, workDir)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgShowParseError, rawRef))
		return
	}
	content, err := renderReferenceView(req)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "path does not exist"):
			e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgShowNotFound, rawRef))
		case strings.Contains(err.Error(), "directory reference cannot carry a location"):
			e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgShowDirWithLocation, rawRef))
		default:
			e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgShowReadFailed, err))
		}
		return
	}
	e.reply(p, msg.ReplyCtx, content)
}

// quickFinishTimeout is how long to wait before assuming the command is long-running.
const quickFinishTimeout = 500 * time.Millisecond

// shellExecCommand builds an exec.Cmd for running command via the given shell.
// For PowerShell/pwsh, extra flags (-NoProfile, -ExecutionPolicy Bypass) are
// added automatically. If shellProfile is non-empty, it is prepended to the command
// with a newline separator (useful for sourcing shell profiles).
func shellExecCommand(ctx context.Context, shell, flag, shellProfile, command string) *exec.Cmd {
	if shellProfile != "" {
		command = shellProfile + "\n" + command
	}
	base := strings.ToLower(filepath.Base(shell))
	if strings.HasPrefix(base, "powershell") || strings.HasPrefix(base, "pwsh") {
		return exec.CommandContext(ctx, shell, "-NoProfile", "-ExecutionPolicy", "Bypass", flag, command)
	}
	return exec.CommandContext(ctx, shell, flag, command)
}

func defaultShell() string {
	if runtime.GOOS == "windows" {
		return "powershell.exe"
	}
	return "sh"
}

func defaultShellFlag() string {
	if runtime.GOOS == "windows" {
		return "-Command"
	}
	return "-c"
}

// runShellWithProgress executes a shell command with live progress feedback.
// Strategy: start the command, wait 500ms. If it finishes within that window,
// just send the result directly (no intermediate messages). If it's still running,
// send a progress message and keep updating until completion.
func (e *Engine) runShellWithProgress(p Platform, replyCtx any, command string, workDir string, timeout time.Duration, maxOutput int) error {
	cmdLabel := truncateStr(command, 60)

	ctx, cancel := context.WithTimeout(e.ctx, timeout)
	defer cancel()

	cmd := shellExecCommand(ctx, e.shell, e.shellFlag, e.shellProfile, command)
	cmd.Dir = workDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		errMsg := fmt.Sprintf("failed to start command: %v", err)
		e.reply(p, replyCtx, fmt.Sprintf("`%s`\n```\n%s\n```", cmdLabel, errMsg))
		return err
	}

	// Read stdout and stderr concurrently
	var mu sync.Mutex
	var buf bytes.Buffer
	doneCh := make(chan struct{})
	var cmdWaitErr error

	readPipe := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
		for scanner.Scan() {
			mu.Lock()
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			buf.WriteString(scanner.Text())
			mu.Unlock()
		}
	}

	go func() {
		// Pipes must be fully drained before cmd.Wait() per Go API contract.
		var pipeWg sync.WaitGroup
		pipeWg.Add(2)
		go func() { defer pipeWg.Done(); readPipe(stdout) }()
		go func() { defer pipeWg.Done(); readPipe(stderr) }()
		pipeWg.Wait()
		cmdWaitErr = cmd.Wait()
		close(doneCh)
	}()

	// Wait a bit to see if the command finishes quickly
	select {
	case <-doneCh:
		// Command finished within the quick window — send result directly
		return e.finishShellCmd(p, replyCtx, cmd, &mu, &buf, cmdLabel, maxOutput, false, cmdWaitErr)
	case <-ctx.Done():
		// Timeout before even the quick window elapsed (very short timeout)
		killAndWait(cmd, doneCh)
		mu.Lock()
		output := buf.String()
		mu.Unlock()
		e.send(p, replyCtx, e.formatShellTimeout(cmdLabel, output, maxOutput))
		return fmt.Errorf("command timed out after %s", timeout)
	case <-time.After(quickFinishTimeout):
		// Still running — fall through to progress mode
	}

	// Command is long-running. Try to send a progress message.
	var previewHandle any
	var useUpdate bool
	if _, ok := p.(MessageUpdater); ok {
		if starter, ok := p.(PreviewStarter); ok {
			mu.Lock()
			output := buf.String()
			mu.Unlock()
			progressMsg := e.formatShellProgress(cmdLabel, output, maxOutput)
			handle, err := starter.SendPreviewStart(e.ctx, replyCtx, progressMsg)
			if err == nil && handle != nil {
				previewHandle = handle
				useUpdate = true
			}
		}
	}

	if !useUpdate {
		// Platform doesn't support in-place updates — send a status message
		e.send(p, replyCtx, fmt.Sprintf("`%s`", cmdLabel))
	}

	// Periodic updates (only for platforms that support UpdateMessage)
	updateDone := make(chan struct{})
	if useUpdate {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					mu.Lock()
					output := buf.String()
					mu.Unlock()
					msg := e.formatShellProgress(cmdLabel, output, maxOutput)
					_ = updaterFor(p).UpdateMessage(e.ctx, previewHandle, msg)
				case <-updateDone:
					return
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Wait for completion or timeout
	select {
	case <-doneCh:
		close(updateDone)
		return e.finishShellCmd(p, replyCtx, cmd, &mu, &buf, cmdLabel, maxOutput, useUpdate, previewHandle, cmdWaitErr)
	case <-ctx.Done():
		close(updateDone)
		killAndWait(cmd, doneCh)
		mu.Lock()
		output := buf.String()
		mu.Unlock()
		timeoutMsg := e.formatShellTimeout(cmdLabel, output, maxOutput)
		if useUpdate {
			_ = updaterFor(p).UpdateMessage(e.ctx, previewHandle, timeoutMsg)
		} else {
			e.send(p, replyCtx, timeoutMsg)
		}
		return fmt.Errorf("command timed out after %s", timeout)
	}
}

func truncateRunes(s string, max int) string {
	if max < 4 {
		max = 4
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max-3]) + "..."
}

func (e *Engine) finishShellCmd(p Platform, replyCtx any, cmd *exec.Cmd, mu *sync.Mutex, buf *bytes.Buffer, cmdLabel string, maxOutput int, opts ...any) error {
	var waitErr error
	// Extract waitErr from opts if provided as the last error argument.
	for _, o := range opts {
		if err, ok := o.(error); ok {
			waitErr = err
		}
	}

	mu.Lock()
	output := buf.String()
	mu.Unlock()

	exitCode := cmd.ProcessState.ExitCode()
	exitOK := exitCode == 0

	display := strings.TrimSpace(output)
	if display == "" && exitOK {
		display = "(no output)"
	}
	display = truncateRunes(display, maxOutput)

	var finalMsg string
	if exitOK {
		finalMsg = fmt.Sprintf("`%s`\n```\n%s\n```", cmdLabel, display)
	} else {
		// Prefer the wait error message when we have no captured output,
		// since it often contains the actual failure reason.
		if display == "" && waitErr != nil {
			display = truncateRunes(waitErr.Error(), maxOutput)
		}
		finalMsg = fmt.Sprintf("`%s` (exit code %d)\n```\n%s\n```", cmdLabel, exitCode, display)
	}

	// opts: [useUpdate bool, previewHandle any]
	if len(opts) >= 2 {
		if useUpdate, ok := opts[0].(bool); ok && useUpdate {
			if handle := opts[1]; handle != nil {
				_ = updaterFor(p).UpdateMessage(e.ctx, handle, finalMsg)
				if !exitOK {
					return fmt.Errorf("exit code %d", exitCode)
				}
				return nil
			}
		}
	}

	// No in-place update available, or command finished quickly — send final reply
	e.reply(p, replyCtx, finalMsg)
	if !exitOK {
		return fmt.Errorf("exit code %d", exitCode)
	}
	return nil
}

func (e *Engine) formatShellProgress(cmdLabel, output string, maxOutput int) string {
	display := truncateRunes(output, maxOutput)
	return fmt.Sprintf("`%s`\n```\n%s\n```", cmdLabel, display)
}

func (e *Engine) formatShellTimeout(cmdLabel, output string, maxOutput int) string {
	display := truncateRunes(output, maxOutput)
	return fmt.Sprintf("`%s` (timeout)\n```\n%s\n```", cmdLabel, display)
}

func killAndWait(cmd *exec.Cmd, doneCh <-chan struct{}) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	<-doneCh
}

func updaterFor(p Platform) MessageUpdater {
	return p.(MessageUpdater)
}

func (e *Engine) cmdShell(p Platform, msg *Message, raw string) {
	// Strip the command prefix ("/shell ", "/sh ", "/exec ", "/run ")
	shellCmd := raw
	for _, prefix := range []string{"/shell ", "/sh ", "/exec ", "/run "} {
		if strings.HasPrefix(strings.ToLower(raw), prefix) {
			shellCmd = raw[len(prefix):]
			break
		}
	}
	shellCmd = strings.TrimSpace(shellCmd)

	if shellCmd == "" {
		e.reply(p, msg.ReplyCtx, "Usage: /shell [--timeout <seconds>] <command>\nExample: /shell ls -la\nExample: /shell --timeout 300 npm install")
		return
	}

	// Parse optional --timeout at the beginning of the command.
	// Placed before the actual command so no CLI tool's own --timeout can conflict.
	// Supported: /shell --timeout 300 npm install, ! --timeout 300 npm install
	timeout := 60 * time.Second
	if strings.HasPrefix(shellCmd, "--timeout ") {
		rest := shellCmd[len("--timeout "):]
		if idx := strings.IndexByte(rest, ' '); idx > 0 {
			if secs, err := strconv.Atoi(rest[:idx]); err == nil && secs > 0 {
				timeout = time.Duration(secs) * time.Second
				shellCmd = strings.TrimSpace(rest[idx:])
			}
		}
	}

	agent, _, _ := e.commandContext(p, msg)
	workDir := e.commandWorkDir(agent, msg)
	if workDir == "" {
		workDir, _ = os.Getwd()
	}

	go func() { _ = e.runShellWithProgress(p, msg.ReplyCtx, shellCmd, workDir, timeout, 4000) }()
}

func (e *Engine) cmdDiff(p Platform, msg *Message, raw string) {
	// Parse optional target: /diff [target]
	diffTarget := ""
	if strings.HasPrefix(strings.ToLower(raw), "/diff ") {
		diffTarget = strings.TrimSpace(raw[6:])
	}

	if strings.HasPrefix(diffTarget, "-") {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgError), "diff target must not start with '-'"))
		return
	}

	// Resolve working directory (same pattern as cmdShell)
	var workDir string
	{
		if wd, ok := e.agent.(interface{ GetWorkDir() string }); ok {
			workDir = wd.GetWorkDir()
		}
	}
	if workDir == "" {
		workDir, _ = os.Getwd()
	}

	go func() {
		ctx, cancel := context.WithTimeout(e.ctx, 60*time.Second)
		defer cancel()

		// Get current branch name and short commit ID
		branchCmd := exec.CommandContext(ctx, "git", "rev-parse", "--abbrev-ref", "HEAD")
		branchCmd.Dir = workDir
		branchOut, _ := branchCmd.Output()
		currentBranch := strings.TrimSpace(string(branchOut))
		if currentBranch == "" {
			currentBranch = "unknown"
		}

		commitCmd := exec.CommandContext(ctx, "git", "rev-parse", "--short", "HEAD")
		commitCmd.Dir = workDir
		commitOut, _ := commitCmd.Output()
		commitID := strings.TrimSpace(string(commitOut))
		if commitID == "" {
			commitID = "0000000"
		}

		gitArgs := []string{"diff"}
		if diffTarget != "" {
			gitArgs = append(gitArgs, "--", diffTarget)
		}
		gitCmd := exec.CommandContext(ctx, "git", gitArgs...)
		gitCmd.Dir = workDir
		diffOutput, err := gitCmd.Output()

		if ctx.Err() == context.DeadlineExceeded {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandTimeout), "git diff"))
			return
		}
		if err != nil && len(diffOutput) == 0 {
			e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgError, err))
			return
		}

		target := diffTarget
		if target == "" {
			target = "HEAD"
		}
		if len(strings.TrimSpace(string(diffOutput))) == 0 {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgDiffEmpty), target))
			return
		}

		// Try diff2html + FileSender
		if fileSender, ok := p.(FileSender); ok {
			title := fmt.Sprintf("%s vs %s", currentBranch, target)
			htmlData, err := e.diff2html(ctx, diffOutput, workDir, title)
			if err == nil {
				fileName := fmt.Sprintf("%s-%s.html", currentBranch, commitID)
				if err := e.waitOutgoing(p); err != nil {
					slog.Warn("outgoing rate limit", "platform", p.Name(), "error", err)
				}
				if err := fileSender.SendFile(e.ctx, msg.ReplyCtx, FileAttachment{
					MimeType: "text/html", Data: htmlData, FileName: fileName,
				}); err == nil {
					return
				}
			}
			if errors.Is(err, exec.ErrNotFound) {
				e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDiffNoDiff2HTML))
			}
		}

		// Fallback: plain text diff
		result := strings.TrimSpace(string(diffOutput))
		if runes := []rune(result); len(runes) > 4000 {
			result = string(runes[:3997]) + "..."
		}
		e.reply(p, msg.ReplyCtx, "```diff\n"+result+"\n```")
	}()
}

func (e *Engine) diff2html(ctx context.Context, diff []byte, workDir, title string) ([]byte, error) {
	if _, err := exec.LookPath("diff2html"); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "diff2html", "-i", "stdin", "-o", "stdout", "--title", title)
	cmd.Dir = workDir
	cmd.Stdin = bytes.NewReader(diff)
	return cmd.Output()
}

// dirApply applies /dir mutations (same semantics as cmdDir). sessionKey is used for GetOrCreateActive.
// On failure returns a non-empty errMsg; on success returns ("", successMsg) for plain-text replies.
func (e *Engine) dirApply(agent Agent, sessions *SessionManager, interactiveKey, sessionKey string, args []string) (errMsg, successMsg string) {
	switcher, ok := agent.(WorkDirSwitcher)
	if !ok {
		return e.i18n.T(MsgDirNotSupported), ""
	}
	currentDir := switcher.GetWorkDir()

	if len(args) == 1 {
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "reset":
			baseDir := strings.TrimSpace(e.baseWorkDir)
			if baseDir == "" {
				baseDir = currentDir
			}
			if baseDir == "" {
				baseDir, _ = os.Getwd()
			}
			if absDir, err := filepath.Abs(baseDir); err == nil {
				baseDir = absDir
			}

			switcher.SetWorkDir(baseDir)
			e.cleanupInteractiveState(interactiveKey)

			s := sessions.GetOrCreateActive(sessionKey)
			s.SetAgentSessionID("", "")
			s.ClearHistory()
			sessions.Save()

			if e.projectState != nil {
				e.projectState.ClearWorkDirOverride()
				e.projectState.Save()
			}
			if e.dirHistory != nil {
				e.dirHistory.Add(e.name, baseDir)
			}

			return "", e.i18n.Tf(MsgDirReset, baseDir)
		}
	}

	arg := strings.Join(args, " ")
	var newDir string

	if idx, err := strconv.Atoi(strings.TrimSpace(arg)); err == nil && idx > 0 {
		if e.dirHistory != nil {
			newDir = e.dirHistory.Get(e.name, idx)
			if newDir == "" {
				return e.i18n.Tf(MsgDirInvalidIndex, idx), ""
			}
		} else {
			return e.i18n.T(MsgDirNoHistory), ""
		}
	} else if arg == "-" {
		if e.dirHistory != nil {
			newDir = e.dirHistory.Previous(e.name)
			if newDir == "" {
				return e.i18n.T(MsgDirNoPrevious), ""
			}
		} else {
			return e.i18n.T(MsgDirNoHistory), ""
		}
	} else {
		newDir = filepath.Clean(arg)
		if strings.HasPrefix(newDir, "~") {
			if homeDir, err := os.UserHomeDir(); err == nil {
				newDir = filepath.Join(homeDir, strings.TrimPrefix(newDir, "~"))
			}
		} else if !filepath.IsAbs(newDir) {
			baseDir := currentDir
			if baseDir == "" {
				baseDir, _ = os.Getwd()
			}
			newDir = filepath.Join(baseDir, newDir)
		}
	}
	if absDir, err := filepath.Abs(newDir); err == nil {
		newDir = absDir
	}

	info, err := os.Stat(newDir)
	if err != nil || !info.IsDir() {
		return e.i18n.Tf(MsgDirInvalidPath, newDir), ""
	}

	switcher.SetWorkDir(newDir)
	e.cleanupInteractiveState(interactiveKey)

	s := sessions.GetOrCreateActive(sessionKey)
	s.SetAgentSessionID("", "")
	s.ClearHistory()
	sessions.Save()

	if e.dirHistory != nil {
		e.dirHistory.Add(e.name, newDir)
	}
	if e.projectState != nil {
		e.projectState.SetWorkDirOverride(newDir)
		e.projectState.Save()
	}

	return "", e.i18n.Tf(MsgDirChanged, newDir)
}

func (e *Engine) cmdDir(p Platform, msg *Message, args []string) {
	agent, sessions, interactiveKey := e.commandContext(p, msg)
	switcher, ok := agent.(WorkDirSwitcher)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDirNotSupported))
		return
	}

	currentDir := switcher.GetWorkDir()

	if len(args) == 0 {
		if supportsCards(p) {
			e.replyWithCard(p, msg.ReplyCtx, e.renderDirCardSafe(msg.SessionKey, 1))
			return
		}
		var sb strings.Builder
		sb.WriteString(e.i18n.Tf(MsgDirCurrent, currentDir))

		if e.dirHistory != nil {
			history := e.dirHistory.List(e.name)
			if len(history) > 0 {
				sb.WriteString("\n\n")
				sb.WriteString(e.i18n.T(MsgDirHistoryTitle))
				for i, dir := range history {
					marker := "[ ]"
					if dir == currentDir {
						marker = "(current)"
					}
					sb.WriteString(fmt.Sprintf("\n  %s %d. %s", marker, i+1, dir))
				}
				sb.WriteString("\n\n")
				sb.WriteString(e.i18n.T(MsgDirHistoryHint))
			}
		}
		e.reply(p, msg.ReplyCtx, sb.String())
		return
	}

	if len(args) == 1 {
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "help", "-h", "--help":
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDirUsage))
			return
		}
	}

	errMsg, successMsg := e.dirApply(agent, sessions, interactiveKey, msg.SessionKey, args)
	if errMsg != "" {
		e.reply(p, msg.ReplyCtx, errMsg)
		return
	}
	if supportsCards(p) {
		e.replyWithCard(p, msg.ReplyCtx, e.renderDirCardSafe(msg.SessionKey, 1))
		return
	}
	e.reply(p, msg.ReplyCtx, successMsg)
}

// cmdSearch searches sessions by name or message content.
// Usage: /search <keyword>
func (e *Engine) cmdSearch(p Platform, msg *Message, args []string) {
	if len(args) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgSearchUsage))
		return
	}

	keyword := strings.ToLower(strings.Join(args, " "))

	// Get all agent sessions
	agent, sessions, _ := e.commandContext(p, msg)
	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgSearchError), err))
		return
	}
	agentSessions = e.applySessionFilter(agentSessions, sessions)

	type searchResult struct {
		id           string
		name         string
		summary      string
		matchType    string // "name" or "message"
		messageCount int
	}

	var results []searchResult

	for _, s := range agentSessions {
		// Check session name (custom name or summary)
		customName := sessions.GetSessionName(s.ID)
		displayName := customName
		if displayName == "" {
			displayName = s.Summary
		}

		// Match by name/summary
		if strings.Contains(strings.ToLower(displayName), keyword) {
			results = append(results, searchResult{
				id:           s.ID,
				name:         displayName,
				summary:      s.Summary,
				matchType:    "name",
				messageCount: s.MessageCount,
			})
			continue
		}

		// Match by session ID prefix
		if strings.HasPrefix(strings.ToLower(s.ID), keyword) {
			results = append(results, searchResult{
				id:           s.ID,
				name:         displayName,
				summary:      s.Summary,
				matchType:    "id",
				messageCount: s.MessageCount,
			})
			continue
		}
	}

	if len(results) == 0 {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgSearchNoResult), keyword))
		return
	}

	// Build result message
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(e.i18n.T(MsgSearchResult), len(results), keyword))

	for i, r := range results {
		shortID := r.id
		if len(shortID) > 12 {
			shortID = shortID[:12]
		}
		sb.WriteString(fmt.Sprintf("\n%d. [%s] %s", i+1, shortID, r.name))
	}

	sb.WriteString("\n\n" + e.i18n.T(MsgSearchHint))

	e.reply(p, msg.ReplyCtx, sb.String())
}

func (e *Engine) cmdName(p Platform, msg *Message, args []string) {
	if len(args) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNameUsage))
		return
	}

	agent, sessions, _ := e.commandContext(p, msg)

	// Check if first arg is a number → naming a specific session by list index
	var targetID string
	var name string

	if idx, err := strconv.Atoi(args[0]); err == nil && idx >= 1 {
		// /name <number> <name...>
		if len(args) < 2 {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNameUsage))
			return
		}
		agentSessions, err := agent.ListSessions(e.ctx)
		if err != nil {
			e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgError, err))
			return
		}
		agentSessions = e.applySessionFilter(agentSessions, sessions)
		if idx > len(agentSessions) {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgSwitchNoSession), idx))
			return
		}
		targetID = agentSessions[idx-1].ID
		name = strings.Join(args[1:], " ")
	} else {
		// /name <name...> → current session
		session := sessions.GetOrCreateActive(msg.SessionKey)
		targetID = session.GetAgentSessionID()
		if targetID == "" {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNameNoSession))
			return
		}
		name = strings.Join(args, " ")
	}

	name = strings.TrimSpace(name)
	if name == "" {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNameUsage))
		return
	}

	sessions.SetSessionName(targetID, name)

	shortID := targetID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}
	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgNameSet), name, shortID))
}

func (e *Engine) cmdCurrent(p Platform, msg *Message) {
	if !supportsCards(p) {
		agent, sessions, _ := e.commandContext(p, msg)
		s := sessions.GetOrCreateActive(msg.SessionKey)
		agentID := s.GetAgentSessionID()
		if agentID == "" {
			agentID = e.i18n.T(MsgSessionNotStarted)
		}
		displayName := e.currentSessionDisplayName(agent, sessions, agentID)
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCurrentSession), displayName, agentID, len(s.History)))
		return
	}

	e.replyWithCard(p, msg.ReplyCtx, e.renderCurrentCard(msg.SessionKey))
}

func selectUsageWindows(report *UsageReport) (*UsageWindow, *UsageWindow) {
	for _, bucket := range report.Buckets {
		if len(bucket.Windows) == 0 {
			continue
		}
		var primary, secondary *UsageWindow
		for i := range bucket.Windows {
			window := &bucket.Windows[i]
			switch window.WindowSeconds {
			case 18000:
				primary = window
			case 604800:
				if secondary == nil {
					secondary = window
				}
			}
		}
		if primary == nil && len(bucket.Windows) > 0 {
			// Fall back to the first window when no 5-hour quota is reported
			// (e.g. Codex accounts whose first bucket only exposes a 7-day
			// window). Skip it if the slot is already occupied by the 7-day
			// window so the same block is not rendered twice.
			if secondary != &bucket.Windows[0] {
				primary = &bucket.Windows[0]
			}
		}
		if secondary == nil && len(bucket.Windows) > 1 && &bucket.Windows[1] != primary {
			secondary = &bucket.Windows[1]
		}
		if primary != nil || secondary != nil {
			return primary, secondary
		}
	}
	return nil, nil
}

func (e *Engine) cardBackButton() CardButton {
	return DefaultBtn(e.i18n.T(MsgCardBack), "nav:/help")
}

func (e *Engine) modelCardBackButton() CardButton {
	return DefaultBtn(e.i18n.T(MsgCardBack), "nav:/model")
}

func (e *Engine) cardPrevButton(action string) CardButton {
	return DefaultBtn(e.i18n.T(MsgCardPrev), action)
}

func (e *Engine) cardNextButton(action string) CardButton {
	return DefaultBtn(e.i18n.T(MsgCardNext), action)
}

// simpleCard builds a card with a title, markdown body and a single Back button.
// Used to reduce repetition across render functions that share this pattern.
func (e *Engine) simpleCard(title, color, content string) *Card {
	return NewCard().Title(title, color).Markdown(content).Buttons(e.cardBackButton()).Build()
}

// renderListCardSafe wraps renderListCard and returns an error card on failure.
func (e *Engine) renderListCardSafe(sessionKey string, page int) *Card {
	card, err := e.renderListCard(sessionKey, page)
	if err != nil {
		agent, _ := e.sessionContextForKey(sessionKey)
		return e.simpleCard(e.i18n.Tf(MsgCardTitleSessions, agent.Name(), 0), "red", err.Error())
	}
	return card
}

// renderDirCardSafe wraps renderDirCard and returns an error card on failure.
func (e *Engine) renderDirCardSafe(sessionKey string, page int) *Card {
	card, err := e.renderDirCard(sessionKey, page)
	if err != nil {
		return e.simpleCard(e.i18n.T(MsgDirCardTitle), "red", err.Error())
	}
	return card
}

func (e *Engine) cmdHistory(p Platform, msg *Message, args []string) {
	if len(args) == 0 && supportsCards(p) {
		e.replyWithCard(p, msg.ReplyCtx, e.renderHistoryCard(msg.SessionKey))
		return
	}
	if len(args) == 0 {
		args = []string{"10"}
	}

	agent, sessions, _ := e.commandContext(p, msg)
	s := sessions.GetOrCreateActive(msg.SessionKey)
	n := 10
	if v, err := strconv.Atoi(args[0]); err == nil && v > 0 {
		n = v
	}

	entries := s.GetHistory(n)
	agentSID := s.GetAgentSessionID()
	if len(entries) == 0 && agentSID != "" {
		if hp, ok := agent.(HistoryProvider); ok {
			if agentEntries, err := hp.GetSessionHistory(e.ctx, agentSID, n); err == nil {
				entries = agentEntries
			}
		}
	}

	if len(entries) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgHistoryEmpty))
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("History (last %d):\n\n", len(entries)))
	maxLen := e.historyEntryMaxLen()
	for _, h := range entries {
		icon := "User"
		if h.Role == "assistant" {
			icon = "Assistant"
		}
		content := truncateHistoryEntry(h.Content, maxLen)
		sb.WriteString(fmt.Sprintf("%s [%s]\n%s\n\n", icon, h.Timestamp.Format("15:04:05"), content))
	}
	e.reply(p, msg.ReplyCtx, sb.String())
}

func (e *Engine) historyEntryMaxLen() int {
	if e.display.HistoryMaxLen != nil {
		return *e.display.HistoryMaxLen
	}
	return defaultHistoryMaxLen
}

func truncateHistoryEntry(content string, maxLen int) string {
	if maxLen <= 0 || utf8.RuneCountInString(content) <= maxLen {
		return content
	}
	runes := []rune(content)
	return string(runes[:maxLen]) + "..."
}

func (e *Engine) cmdHelp(p Platform, msg *Message) {
	if !supportsCards(p) {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgHelp))
		return
	}
	e.replyWithCard(p, msg.ReplyCtx, e.renderHelpCard())
}

// cmdStart handles the `/start` slash command.
//
// On Telegram, `/start` is a protocol convention sent by the client when a
// user first opens a bot (or taps the Start button). Without a native
// handler, the message previously fell through to the default branch and
// got forwarded verbatim to the agent — and Claude Code's CLI interprets a
// leading "/" as a slash-command request, replying "Unknown command:
// /start. Did you mean /stats?" instead of greeting the user.
//
// Replying with a localized welcome that names the project keeps the
// behavior consistent with every other Telegram bot framework, and is a
// no-op improvement on platforms where /start has no special meaning.
func (e *Engine) cmdStart(p Platform, msg *Message) {
	name := e.name
	if name == "" {
		name = e.agent.Name()
	}
	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgWelcome), name))
}

const defaultHelpGroup = "session"

type helpCardItem struct {
	command string
	action  string
}

type helpCardGroup struct {
	key      string
	titleKey MsgKey
	items    []helpCardItem
}

func helpCardGroups() []helpCardGroup {
	return []helpCardGroup{
		{
			key:      "session",
			titleKey: MsgHelpSessionSection,
			items: []helpCardItem{
				{command: "/new", action: "act:/new"},
				{command: "/cancel", action: "cmd:/cancel"},
				{command: "/list", action: "nav:/list"},
				{command: "/current", action: "nav:/current"},
				{command: "/switch", action: "nav:/list"},
				{command: "/search", action: "cmd:/search"},
				{command: "/history", action: "nav:/history"},
				{command: "/delete", action: "cmd:/delete"},
				{command: "/name", action: "cmd:/name"},
			},
		},
		{
			key:      "agent",
			titleKey: MsgHelpAgentSection,
			items: []helpCardItem{
				{command: "/model", action: "nav:/model"},
				{command: "/effort", action: "nav:/effort"},
				{command: "/mode", action: "nav:/mode"},
				{command: "/provider", action: "nav:/provider"},
				{command: "/allow", action: "cmd:/allow"},
				{command: "/quiet", action: "cmd:/quiet"},
				{command: "/tts", action: "cmd:/tts"},
			},
		},
		{
			key:      "tools",
			titleKey: MsgHelpToolsSection,
			items: []helpCardItem{
				{command: "/shell", action: "cmd:/shell"},
				{command: "/show", action: "cmd:/show"},
				{command: "/commands", action: "nav:/commands"},
				{command: "/alias", action: "nav:/alias"},
				{command: "/compact", action: "cmd:/compact"},
				{command: "/stop", action: "act:/stop"},
			},
		},
		{
			key:      "system",
			titleKey: MsgHelpSystemSection,
			items: []helpCardItem{
				{command: "/dir", action: "nav:/dir"},
				{command: "/restart", action: "cmd:/restart"},
			},
		},
	}
}

func (e *Engine) renderHelpCard() *Card {
	return e.renderHelpGroupCard(defaultHelpGroup)
}

// splitHelpTabRows splits tab buttons into rows. Card-based platforms
// get 2 buttons per row for better layout; others get all in one row.
func splitHelpTabRows(useMultiRow bool, tabs []CardButton) [][]CardButton {
	if useMultiRow {
		rows := make([][]CardButton, 0, (len(tabs)+1)/2)
		for i := 0; i < len(tabs); i += 2 {
			end := i + 2
			if end > len(tabs) {
				end = len(tabs)
			}
			rows = append(rows, tabs[i:end])
		}
		return rows
	}
	return [][]CardButton{tabs}
}

func (e *Engine) renderHelpGroupCard(groupKey string) *Card {
	sectionTitle := func(key MsgKey) string {
		section := e.i18n.T(key)
		if idx := strings.IndexByte(section, '\n'); idx >= 0 {
			return section[:idx]
		}
		return section
	}
	tabLabel := func(key MsgKey) string {
		return strings.Trim(sectionTitle(key), "* ")
	}
	commandText := func(command string) string {
		return "**" + command + "**  " + e.i18n.T(MsgKey(strings.TrimPrefix(command, "/")))
	}

	groups := helpCardGroups()
	current := groups[0]
	normalizedGroup := strings.ToLower(strings.TrimSpace(groupKey))
	for _, group := range groups {
		if group.key == normalizedGroup {
			current = group
			break
		}
	}

	cb := NewCard().Title(e.i18n.T(MsgHelpTitle), "blue")
	var tabs []CardButton
	for _, group := range groups {
		btnType := "default"
		if group.key == current.key {
			btnType = "primary"
		}
		tabs = append(tabs, Btn(tabLabel(group.titleKey), btnType, "nav:/help "+group.key))
	}
	for _, row := range splitHelpTabRows(true, tabs) {
		cb.ButtonsEqual(row...)
	}
	for _, item := range current.items {
		cb.ListItem(commandText(item.command), "(current)", item.action)
	}
	cb.Note(e.i18n.T(MsgHelpTip))
	return cb.Build()
}

// GetAllCommands returns all available commands for bot menu registration.
// It includes built-in commands (with localized descriptions) and custom commands.
func (e *Engine) GetAllCommands() []BotCommandInfo {
	var commands []BotCommandInfo

	e.userRolesMu.RLock()
	disabledCmds := e.disabledCmds
	e.userRolesMu.RUnlock()

	// Collect built-in  commands (use primary name, first in names list)
	seenCmds := make(map[string]bool)
	for _, c := range builtinCommands {
		if len(c.names) == 0 {
			continue
		}
		// Use id as primary
		primaryName := c.id
		if seenCmds[primaryName] {
			continue
		}
		seenCmds[primaryName] = true

		// Skip disabled commands
		if disabledCmds[c.id] {
			continue
		}

		commands = append(commands, BotCommandInfo{
			Command:     primaryName,
			Description: e.i18n.T(MsgKey(primaryName)),
		})
	}

	// Collect custom commands from CommandRegistry
	for _, c := range e.commands.ListAll() {
		if seenCmds[strings.ToLower(c.Name)] {
			continue
		}
		seenCmds[strings.ToLower(c.Name)] = true

		desc := c.Description
		if desc == "" {
			desc = "Custom command"
		}

		commands = append(commands, BotCommandInfo{
			Command:     c.Name,
			Description: desc,
		})
	}

	return commands
}

func (e *Engine) cmdModel(p Platform, msg *Message, args []string) {
	agent, sessions, interactiveKey := e.commandContext(p, msg)

	switcher, ok := agent.(ModelSwitcher)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgModelNotSupported))
		return
	}

	if len(args) == 0 {
		if !supportsCards(p) {
			fetchCtx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
			defer cancel()
			models := switcher.AvailableModels(fetchCtx)

			var sb strings.Builder
			current := switcher.GetModel()
			if current == "" {
				sb.WriteString(e.i18n.T(MsgModelDefault))
			} else {
				sb.WriteString(e.i18n.Tf(MsgModelCurrent, current))
				sb.WriteString("\n")
			}
			sb.WriteString("\n")
			sb.WriteString(e.i18n.T(MsgModelListTitle))
			var buttons [][]ButtonOption
			var row []ButtonOption
			for i, m := range models {
				marker := "  "
				if m.Name == current {
					marker = "> "
				}
				var line string
				if m.Alias != "" {
					line = fmt.Sprintf("%s%d. %s - %s\n", marker, i+1, m.Alias, m.Name)
				} else {
					desc := m.Desc
					if desc != "" {
						desc = " — " + desc
					}
					line = fmt.Sprintf("%s%d. %s%s\n", marker, i+1, m.Name, desc)
				}
				sb.WriteString(line)

				label := m.Name
				if m.Alias != "" {
					label = m.Alias
				}
				if m.Name == current {
					label = "(current) " + label
				}
				row = append(row, ButtonOption{Text: label, Data: fmt.Sprintf("cmd:/model switch %d", i+1)})
				if len(row) >= 3 {
					buttons = append(buttons, row)
					row = nil
				}
			}
			if len(row) > 0 {
				buttons = append(buttons, row)
			}
			sb.WriteString("\n")
			sb.WriteString(e.i18n.T(MsgModelUsage))
			e.replyWithButtons(p, msg.ReplyCtx, sb.String(), buttons)
			return
		}
		e.replyWithCard(p, msg.ReplyCtx, e.renderModelCard(msg.SessionKey))
		return
	}

	targetInput, ok := parseModelSwitchArgs(args)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgModelUsage))
		return
	}

	target := strings.TrimSpace(targetInput)
	if modelSwitchNeedsLookup(target) {
		fetchCtx, cancel := context.WithTimeout(e.ctx, 10*time.Second)
		defer cancel()
		models := switcher.AvailableModels(fetchCtx)
		target = resolveModelSwitchTarget(target, models)
	}

	target, err := e.switchModelOnAgent(agent, target, agent == e.agent)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgModelChangeFailed, err))
		return
	}
	e.cleanupInteractiveState(interactiveKey)

	// Keep the existing agent session ID so the next StartSession uses
	// --resume <id> --model <new>, which lets the CLI agent restore context
	// natively without replaying history (no extra token cost).
	sessions.Save()

	e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgModelChanged, target))
}

// resolveModelAlias resolves a user-supplied string to a model name.
// It first checks for an exact alias match, then falls back to the original value
// (which may be a direct model name).
func resolveModelAlias(models []ModelOption, input string) string {
	for _, m := range models {
		if m.Alias != "" && strings.EqualFold(m.Alias, input) {
			return m.Name
		}
	}
	return input
}

func resolveModelSwitchTarget(input string, models []ModelOption) string {
	input = strings.TrimSpace(input)
	if idx, err := strconv.Atoi(input); err == nil && idx >= 1 && idx <= len(models) {
		return models[idx-1].Name
	}
	if resolved := resolveModelAlias(models, input); resolved != input {
		return resolved
	}
	for _, m := range models {
		if strings.EqualFold(m.Name, input) {
			return m.Name
		}
	}
	return input
}

func modelSwitchNeedsLookup(input string) bool {
	input = strings.TrimSpace(input)
	if input == "" {
		return false
	}
	if _, err := strconv.Atoi(input); err == nil {
		return true
	}
	return !strings.Contains(input, "/")
}

func parseModelSwitchArgs(args []string) (string, bool) {
	if len(args) == 0 {
		return "", false
	}
	if len(args) == 1 {
		if strings.EqualFold(strings.TrimSpace(args[0]), "switch") {
			return "", false
		}
		return args[0], true
	}
	if strings.EqualFold(strings.TrimSpace(args[0]), "switch") && len(args) >= 2 {
		return strings.TrimSpace(args[1]), true
	}
	return "", false
}

// switchModel applies a runtime model selection to the global engine agent and
// persists the change so reloads keep the selected default.
func (e *Engine) switchModel(target string) (string, error) {
	return e.switchModelOnAgent(e.agent, target, true)
}

// switchModelOnAgent applies a runtime model selection to the provided agent.
// When persistConfig is true, config-backed model/provider changes are saved so
// reloads keep the new default. Workspace-scoped runtime switches pass false.
func (e *Engine) switchModelOnAgent(agent Agent, target string, persistConfig bool) (string, error) {
	switcher, ok := agent.(ModelSwitcher)
	if !ok {
		return target, nil
	}

	providerSwitcher, ok := agent.(ProviderSwitcher)
	if !ok {
		if persistConfig && e.modelSaveFunc != nil {
			if err := e.modelSaveFunc(target); err != nil {
				return "", fmt.Errorf("save model: %w", err)
			}
		}
		switcher.SetModel(target)
		return target, nil
	}
	active := providerSwitcher.GetActiveProvider()
	if active == nil {
		if persistConfig && e.modelSaveFunc != nil {
			if err := e.modelSaveFunc(target); err != nil {
				return "", fmt.Errorf("save model: %w", err)
			}
		}
		switcher.SetModel(target)
		return target, nil
	}

	providers := providerSwitcher.ListProviders()
	updated, found := SetProviderModel(providers, active.Name, target)
	if !found {
		switcher.SetModel(target)
		return target, nil
	}
	if !persistConfig {
		switcher.SetModel(target)
		return target, nil
	}
	if persistConfig && e.providerModelSaveFunc != nil {
		if err := e.providerModelSaveFunc(active.Name, target); err != nil {
			return "", fmt.Errorf("save provider model %q: %w", active.Name, err)
		}
	}
	providerSwitcher.SetProviders(updated)
	switcher.SetModel(target)
	providerSwitcher.SetActiveProvider(active.Name)
	return target, nil
}

func (e *Engine) cmdEffort(p Platform, msg *Message, args []string) {
	agent, sessions, _ := e.commandContext(p, msg)

	switcher, ok := agent.(ReasoningEffortSwitcher)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgReasoningNotSupported))
		return
	}

	if len(args) == 0 {
		if !supportsCards(p) {
			efforts := switcher.AvailableReasoningEfforts()

			var sb strings.Builder
			current := switcher.GetReasoningEffort()
			if current == "" {
				sb.WriteString(e.i18n.T(MsgReasoningDefault))
			} else {
				sb.WriteString(e.i18n.Tf(MsgReasoningCurrent, current))
				sb.WriteString("\n")
			}
			sb.WriteString("\n")
			sb.WriteString(e.i18n.T(MsgReasoningListTitle))
			var buttons [][]ButtonOption
			var row []ButtonOption
			for i, effort := range efforts {
				marker := "  "
				if effort == current {
					marker = "> "
				}
				sb.WriteString(fmt.Sprintf("%s%d. %s\n", marker, i+1, effort))

				label := effort
				if effort == current {
					label = "(current) " + label
				}
				row = append(row, ButtonOption{Text: label, Data: fmt.Sprintf("cmd:/effort %d", i+1)})
				if len(row) >= 3 {
					buttons = append(buttons, row)
					row = nil
				}
			}
			if len(row) > 0 {
				buttons = append(buttons, row)
			}
			sb.WriteString("\n")
			sb.WriteString(e.i18n.T(MsgReasoningUsage))
			e.replyWithButtons(p, msg.ReplyCtx, sb.String(), buttons)
			return
		}
		e.replyWithCard(p, msg.ReplyCtx, e.renderEffortCard())
		return
	}

	efforts := switcher.AvailableReasoningEfforts()
	target := strings.ToLower(strings.TrimSpace(args[0]))
	if idx, err := strconv.Atoi(target); err == nil && idx >= 1 && idx <= len(efforts) {
		target = efforts[idx-1]
	}

	valid := false
	for _, effort := range efforts {
		if effort == target {
			valid = true
			break
		}
	}
	if !valid {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgReasoningUsage))
		return
	}

	switcher.SetReasoningEffort(target)
	e.cleanupInteractiveState(e.interactiveKeyForSessionKey(msg.SessionKey))

	// Resume the same conversation with the new effort on the next spawn.
	sessions.Save()

	e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgReasoningChanged, target))
}

func (e *Engine) cmdMode(p Platform, msg *Message, args []string) {
	agent, _, _ := e.commandContext(p, msg)

	switcher, ok := agent.(ModeSwitcher)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgModeNotSupported))
		return
	}

	if len(args) == 0 {
		if !supportsCards(p) {
			current := switcher.GetMode()
			modes := switcher.PermissionModes()
			var sb strings.Builder
			for _, m := range modes {
				suffix := ""
				if m.Key == current {
					suffix = " (current)"
				}
				sb.WriteString(fmt.Sprintf("**%s**%s — %s\n", m.Name, suffix, m.Desc))
			}
			sb.WriteString(e.modeUsageText(modes))

			var buttons [][]ButtonOption
			var row []ButtonOption
			for _, m := range modes {
				label := m.Name
				row = append(row, ButtonOption{Text: label, Data: "cmd:/mode " + m.Key})
				if len(row) >= 2 {
					buttons = append(buttons, row)
					row = nil
				}
			}
			if len(row) > 0 {
				buttons = append(buttons, row)
			}
			e.replyWithButtons(p, msg.ReplyCtx, sb.String(), buttons)
			return
		}
		e.replyWithCard(p, msg.ReplyCtx, e.renderModeCard())
		return
	}

	target := strings.ToLower(args[0])
	switcher.SetMode(target)
	newMode := switcher.GetMode()
	appliedLive := e.applyLiveModeChange(msg.SessionKey, newMode)

	if !appliedLive {
		e.cleanupInteractiveState(e.interactiveKeyForSessionKey(msg.SessionKey))
	}

	modes := switcher.PermissionModes()
	displayName := newMode
	for _, m := range modes {
		if m.Key == newMode {
			displayName = m.Name
			break
		}
	}
	reply := fmt.Sprintf(e.i18n.T(MsgModeChanged), displayName)
	if appliedLive {
		reply += "\n\n(Current session updated immediately.)"
	}
	e.reply(p, msg.ReplyCtx, reply)
}

func (e *Engine) modeUsageText(modes []PermissionModeInfo) string {
	keys := make([]string, 0, len(modes))
	for _, mode := range modes {
		keys = append(keys, "`"+mode.Key+"`")
	}
	return e.i18n.Tf(MsgModeUsage, strings.Join(keys, " / "))
}

func (e *Engine) applyLiveModeChange(sessionKey, mode string) bool {
	iKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[iKey]
	e.interactiveMu.Unlock()
	if !ok || state == nil || state.agentSession == nil || !state.agentSession.Alive() {
		return false
	}
	switcher, ok := state.agentSession.(LiveModeSwitcher)
	if !ok {
		return false
	}
	return switcher.SetLiveMode(mode)
}

func (e *Engine) cmdQuiet(p Platform, msg *Message, args []string) {
	// /quiet [full|compact|quiet]
	// Without argument: cycle full → quiet → compact → full.
	// With argument: set mode directly.
	var newMode string
	if len(args) > 0 {
		switch strings.ToLower(args[0]) {
		case "full", "compact", "quiet":
			newMode = strings.ToLower(args[0])
		default:
			e.reply(p, msg.ReplyCtx, "Usage: /quiet [full|compact|quiet]")
			return
		}
	} else {
		switch e.display.Mode {
		case "full", "":
			newMode = "quiet"
		case "quiet":
			newMode = "compact"
		default: // "compact" or unknown
			newMode = "full"
		}
	}

	e.display.Mode = newMode
	switch newMode {
	case "compact", "quiet":
		e.display.ThinkingMessages = false
		e.display.ToolMessages = false
	default:
		e.display.ThinkingMessages = true
		e.display.ToolMessages = true
	}

	if e.displaySaveFunc != nil {
		tm := e.display.ThinkingMessages
		tool := e.display.ToolMessages
		if err := e.displaySaveFunc(&newMode, &tm, nil, nil, &tool); err != nil {
			slog.Error("failed to persist display config after /quiet", "error", err)
		}
	}

	switch newMode {
	case "quiet":
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgQuietOn))
	case "compact":
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDisplayModeCompact))
	default:
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgQuietOff))
	}
}

func (e *Engine) cmdTTS(p Platform, msg *Message, args []string) {
	if e.tts == nil || !e.tts.Enabled || e.tts.TTS == nil {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgTTSNotEnabled))
		return
	}
	if len(args) == 0 {
		providerStr := e.tts.Provider
		if providerStr == "" {
			providerStr = "unknown"
		}
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgTTSStatus), e.tts.GetTTSMode(), providerStr))
		return
	}
	switch args[0] {
	case "always", "voice_only":
		mode := args[0]
		e.tts.SetTTSMode(mode)
		if e.ttsSaveFunc != nil {
			if err := e.ttsSaveFunc(mode); err != nil {
				slog.Warn("tts: failed to persist mode", "error", err)
			}
		}
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgTTSSwitched), mode))
	default:
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgTTSUsage))
	}
}

func (e *Engine) cmdStop(p Platform, msg *Message) {
	// /stop only tears down the live agent process; it preserves the stored
	// AgentSessionID so the next message can --resume the conversation. This
	// matches the card-button stop path (see executeCardAction "/stop"). The
	// session-tracking write-back keeps AgentSessionID pointing at Claude's
	// latest forked session, so resuming after /stop is safe and no longer
	// triggers the recycling loop from issue #830.
	iKey := e.interactiveKeyForSessionKey(msg.SessionKey)
	if !e.stopInteractiveSession(iKey, p, msg.ReplyCtx) {
		// Fallback: try suffix scan in case interactiveKeyForSessionKey
		// resolved a different key than the one used to store the state
		// (e.g. workspace binding lookup inconsistency).
		if found := e.findInteractiveKeyForSession(msg.SessionKey); found != "" && found != iKey {
			if e.stopInteractiveSession(found, p, msg.ReplyCtx) {
				e.reply(p, msg.ReplyCtx, e.i18n.T(MsgExecutionStopped))
				return
			}
		}
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNoExecution))
		return
	}
	e.reply(p, msg.ReplyCtx, e.i18n.T(MsgExecutionStopped))
}

// cmdCancel stops the current execution and starts a fresh session.
// Unlike /stop which only halts execution, /cancel also resets the session
// so the user can immediately continue with new instructions.
func (e *Engine) cmdCancel(p Platform, msg *Message) {
	_, sessions, interactiveKey := e.commandContext(p, msg)

	slog.Info("cmdCancel: stopping execution and creating new session", "session_key", msg.SessionKey)

	// Stop the current execution (like /stop)
	stopped := e.stopInteractiveSession(interactiveKey, p, msg.ReplyCtx)
	if !stopped {
		// No execution in progress, but still create a new session
		slog.Debug("cmdCancel: no execution to stop, proceeding with new session", "session_key", msg.SessionKey)
	}

	// Clear old session's agent session ID so it cannot be resumed
	old := sessions.GetOrCreateActive(msg.SessionKey)
	old.SetAgentSessionID("", "")
	old.ClearHistory()
	sessions.Save()

	// Create a new session (like /new)
	sessions.NewSession(msg.SessionKey, "")

	e.reply(p, msg.ReplyCtx, e.i18n.T(MsgSessionCancelled))
}

func (e *Engine) stopInteractiveSession(sessionKey string, quietPlatform Platform, quietReplyCtx any) bool {
	return e.stopInteractiveSessionWithOptions(sessionKey, true)
}

func (e *Engine) stopInteractiveSessionSilently(sessionKey string) bool {
	return e.stopInteractiveSessionWithOptions(sessionKey, false)
}

func (e *Engine) stopInteractiveSessionWithOptions(sessionKey string, notifyQueued bool) bool {
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	if !ok || state == nil {
		e.interactiveMu.Unlock()
		return false
	}

	// Stop unsolicited reader before touching state to avoid races.
	e.stopUnsolicitedReader(state)

	state.mu.Lock()
	pending := state.pending
	state.pending = nil
	agentSession := state.agentSession
	closePlatform := state.platform
	closeReplyCtx := state.replyCtx
	state.mu.Unlock()

	// If the agent session supports graceful turn cancellation (e.g. ACP),
	// send a cancel notification and keep the session alive for the next
	// user message, rather than killing the process and destroying state.
	if canceller, ok := agentSession.(AgentSessionCanceller); ok && agentSession != nil {
		// Keep the state in the map so the next message reuses this session.
		// Don't markStopped — the session is still usable.
		// Don't delete from interactiveStates — keep it alive.
		e.interactiveMu.Unlock()

		if pending != nil {
			pending.resolve()
		}
		if notifyQueued {
			e.notifyDroppedQueuedMessages(state, fmt.Errorf("session cancelled"))
		} else {
			state.mu.Lock()
			state.pendingMessages = nil
			state.mu.Unlock()
		}

		// Mark eventsNeedResync so the next turn drains stale events from
		// the cancelled turn before processing fresh input.
		state.mu.Lock()
		state.eventsNeedResync = true
		state.mu.Unlock()

		cancelErr := canceller.CancelTurn()
		if cancelErr != nil {
			slog.Warn("agent session CancelTurn failed, falling back to Close",
				"session_key", sessionKey, "error", cancelErr)
			// Fall through to normal cleanup below.
			goto normalCleanup
		}

		slog.Info("agent session turn cancelled, session kept alive",
			"session_key", sessionKey)

		e.hooks.Emit(HookEvent{
			Event:      HookEventSessionEnded,
			SessionKey: sessionKey,
		})

		return true
	}

normalCleanup:
	state.markStopped()
	delete(e.interactiveStates, sessionKey)
	e.interactiveMu.Unlock()

	if pending != nil {
		pending.resolve()
	}
	if notifyQueued {
		e.notifyDroppedQueuedMessages(state, fmt.Errorf("session reset"))
	} else {
		state.mu.Lock()
		state.pendingMessages = nil
		state.mu.Unlock()
	}
	e.closeAgentSessionAsync(sessionKey, agentSession, closePlatform, closeReplyCtx)

	e.hooks.Emit(HookEvent{
		Event:      HookEventSessionEnded,
		SessionKey: sessionKey,
	})

	return true
}

func (e *Engine) cmdCompact(p Platform, msg *Message) {
	agent, sessions, _ := e.commandContext(p, msg)

	compressor, ok := agent.(ContextCompressor)
	if !ok || compressor.CompressCommand() == "" {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCompressNotSupported))
		return
	}

	iKey := e.interactiveKeyForSessionKey(msg.SessionKey)
	e.interactiveMu.Lock()
	state, hasState := e.interactiveStates[iKey]
	e.interactiveMu.Unlock()

	if !hasState || state == nil {
		// Fallback: suffix scan for multi-workspace key mismatch
		if found := e.findInteractiveKeyForSession(msg.SessionKey); found != "" && found != iKey {
			e.interactiveMu.Lock()
			state, hasState = e.interactiveStates[found]
			e.interactiveMu.Unlock()
		}
	}

	if !hasState || state == nil || state.agentSession == nil || !state.agentSession.Alive() {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCompressNoSession))
		return
	}

	session := sessions.GetOrCreateActive(msg.SessionKey)
	if !session.TryLock() {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPreviousProcessing))
		return
	}

	e.send(p, msg.ReplyCtx, e.i18n.T(MsgCompressing))

	go e.runCompress(state, session, sessions, iKey, p, msg.ReplyCtx, false)
}

// runCompress sends the agent's compress command and handles results.
// If autoTriggered is true, suppress user-visible "compressing" and completion messages.
func (e *Engine) runCompress(state *interactiveState, session *Session, sessions *SessionManager, iKey string, p Platform, replyCtx any, auto bool) {
	// session.Unlock() is called inside drainQueuedMessagesAfterCompress
	// while holding state.mu to close the race window. Deferred fallback
	// ensures the lock is released on early-return paths.
	compressUnlocked := false
	defer func() {
		if !compressUnlocked {
			session.Unlock()
		}
	}()

	// Stop unsolicited reader before taking event channel ownership.
	e.stopUnsolicitedReader(state)

	state.mu.Lock()
	state.platform = p
	state.replyCtx = replyCtx
	state.mu.Unlock()

	drainEvents(state.agentSession.Events())

	compressor, ok := e.agent.(ContextCompressor)
	if !ok || compressor.CompressCommand() == "" {
		if !auto {
			e.reply(p, replyCtx, e.i18n.T(MsgCompressNotSupported))
		}
		return
	}

	cmd := compressor.CompressCommand()
	if err := state.agentSession.Send(cmd, "", nil, nil); err != nil {
		if !auto {
			e.reply(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError), err))
		}
		if !state.agentSession.Alive() {
			e.cleanupInteractiveState(iKey)
		}
		return
	}

	e.processCompressEvents(state, session, sessions, iKey, p, replyCtx, &compressUnlocked, auto)
}

// processCompressEvents drains agent events after a compress command.
// Unlike processInteractiveEvents it does NOT record history and treats
// an empty result as success rather than "(empty response)".
func (e *Engine) processCompressEvents(state *interactiveState, session *Session, sessions *SessionManager, sessionKey string, p Platform, replyCtx any, unlocked *bool, auto bool) {

	var textParts []string
	diagnostics := diagnosticMessages{engine: e}
	events := state.agentSession.Events()
	stopCh := state.stopSignal()

	var idleTimer *time.Timer
	var idleCh <-chan time.Time
	if e.eventIdleTimeout > 0 {
		idleTimer = time.NewTimer(e.eventIdleTimeout)
		defer idleTimer.Stop()
		idleCh = idleTimer.C
	}

	for {
		var event Event
		var ok bool

		select {
		case <-stopCh:
			return
		case event, ok = <-events:
			if !ok {
				e.cleanupInteractiveState(sessionKey, state)
				if !auto {
					if len(textParts) > 0 {
						e.send(p, replyCtx, strings.Join(textParts, ""))
					} else {
						e.reply(p, replyCtx, e.i18n.T(MsgCompressDone))
					}
				}
				e.notifyDroppedQueuedMessages(state, fmt.Errorf("agent process exited during compress"))
				return
			}
		case <-idleCh:
			if !auto {
				e.send(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError), "compress timed out"))
			}
			e.cleanupInteractiveState(sessionKey, state)
			e.notifyDroppedQueuedMessages(state, fmt.Errorf("compress timed out"))
			return
		case <-e.ctx.Done():
			return
		}

		if state.isStopped() {
			return
		}

		if idleTimer != nil {
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(e.eventIdleTimeout)
		}

		switch event.Type {
		case EventText:
			if !auto && event.Content != "" {
				textParts = append(textParts, event.Content)
			}
		case EventToolResult:
			if !auto && !e.display.CollapseToolMessages {
				out := strings.TrimSpace(event.Content)
				if out == "" {
					out = strings.TrimSpace(event.ToolResult)
				}
				if out == "" {
					break
				}
				tn := strings.TrimSpace(event.ToolName)
				if tn == "" {
					tn = "tool"
				}
				textParts = append(textParts, fmt.Sprintf(e.i18n.T(MsgToolResult), tn, out)+"\n")
			}
		case EventResult:
			if !event.Done {
				continue
			}
			delivered := true
			result := event.Content
			if result == "" && len(textParts) > 0 {
				result = strings.Join(textParts, "")
			}
			if !auto {
				if result != "" {
					delivered = e.sendWithError(p, replyCtx, result) == nil
				} else {
					delivered = e.sendWithError(p, replyCtx, e.i18n.T(MsgCompressDone)) == nil
				}
			}
			diagnostics.Finish(delivered && event.Error == nil)

			// After compress succeeds, process any queued messages instead of dropping them.
			e.drainQueuedMessagesAfterCompress(state, session, sessions, sessionKey, unlocked)
			return
		case EventStatus:
			if event.Content != "" {
				diagnostics.Send(p, replyCtx, event.Content, state.workspaceDir)
			}
		case EventError:
			if !auto && event.Error != nil {
				e.reply(p, replyCtx, fmt.Sprintf(e.i18n.T(MsgError), event.Error))
			}
			// Only drop queued messages if the agent is dead; some agents
			// emit per-turn EventError while staying alive.
			if !state.agentSession.Alive() {
				e.notifyDroppedQueuedMessages(state, event.Error)
			} else {
				// Agent survived — try to process queued messages.
				e.drainQueuedMessagesAfterCompress(state, session, sessions, sessionKey, unlocked)
			}
			return
		case EventPermissionRequest:
			_ = state.agentSession.RespondPermission(event.RequestID, PermissionResult{
				Behavior:     "allow",
				UpdatedInput: event.ToolInputRaw,
			})
		}
	}
}

// drainQueuedMessagesAfterCompress processes any messages that were queued
// during a /compact operation. It sends each one to the agent and runs the
// full interactive event loop for it.
func (e *Engine) drainQueuedMessagesAfterCompress(state *interactiveState, session *Session, sessions *SessionManager, sessionKey string, unlocked *bool) {
	if e.drainPendingMessages(state, session, sessions, sessionKey) {
		*unlocked = true
	}
}

func (e *Engine) cmdAllow(p Platform, msg *Message, args []string) {
	agent, _, _ := e.commandContext(p, msg)

	if len(args) == 0 {
		if auth, ok := agent.(ToolAuthorizer); ok {
			tools := auth.GetAllowedTools()
			if len(tools) == 0 {
				e.reply(p, msg.ReplyCtx, e.i18n.T(MsgNoToolsAllowed))
			} else {
				e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCurrentTools), strings.Join(tools, ", ")))
			}
		} else {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgToolAuthNotSupported))
		}
		return
	}

	toolName := strings.TrimSpace(args[0])
	if auth, ok := agent.(ToolAuthorizer); ok {
		if err := auth.AddAllowedTools(toolName); err != nil {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgToolAllowFailed), err))
			return
		}
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgToolAllowedNew), toolName))
	} else {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgToolAuthNotSupported))
	}
}

func (e *Engine) cmdProvider(p Platform, msg *Message, args []string) {
	agent, sessions, _ := e.commandContext(p, msg)

	switcher, ok := agent.(ProviderSwitcher)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderNotSupported))
		return
	}

	if len(args) == 0 {
		if supportsCards(p) {
			e.replyWithCard(p, msg.ReplyCtx, e.renderProviderCard())
			return
		}

		current := switcher.GetActiveProvider()
		providers := switcher.ListProviders()
		if current == nil && len(providers) == 0 {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderNone))
			return
		}

		var sb strings.Builder
		if current != nil {
			sb.WriteString(fmt.Sprintf(e.i18n.T(MsgProviderCurrent), current.Name))
			sb.WriteString("\n\n")
		}
		sb.WriteString(e.i18n.T(MsgProviderListTitle))
		for _, prov := range providers {
			marker := "  "
			if current != nil && prov.Name == current.Name {
				marker = "(current) "
			}
			detail := prov.Name
			if prov.BaseURL != "" {
				detail += " (" + prov.BaseURL + ")"
			}
			if prov.Model != "" {
				detail += " [" + prov.Model + "]"
			}
			sb.WriteString(fmt.Sprintf("%s%s\n", marker, detail))
		}
		sb.WriteString("\n" + e.i18n.T(MsgProviderSwitchHint))
		e.reply(p, msg.ReplyCtx, sb.String())
		return
	}

	sub := matchSubCommand(strings.ToLower(args[0]), []string{
		"list", "add", "remove", "switch", "current", "clear", "reset", "none",
	})
	switch sub {
	case "list":
		providers := switcher.ListProviders()
		if len(providers) == 0 {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderListEmpty))
			return
		}
		current := switcher.GetActiveProvider()
		var sb strings.Builder
		sb.WriteString(e.i18n.T(MsgProviderListTitle))
		for _, prov := range providers {
			marker := "  "
			if current != nil && prov.Name == current.Name {
				marker = "(current) "
			}
			detail := prov.Name
			if prov.BaseURL != "" {
				detail += " (" + prov.BaseURL + ")"
			}
			if prov.Model != "" {
				detail += " [" + prov.Model + "]"
			}
			sb.WriteString(fmt.Sprintf("%s%s\n", marker, detail))
		}
		sb.WriteString("\n" + e.i18n.T(MsgProviderSwitchHint))
		e.reply(p, msg.ReplyCtx, sb.String())

	case "add":
		e.cmdProviderAdd(p, msg, switcher, args[1:])

	case "remove", "rm", "delete":
		e.cmdProviderRemove(p, msg, switcher, args[1:])

	case "switch":
		if len(args) < 2 {
			e.reply(p, msg.ReplyCtx, "Usage: /provider switch <name>")
			return
		}
		e.switchProvider(p, msg, sessions, switcher, args[1])

	case "current":
		current := switcher.GetActiveProvider()
		if current == nil {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderNone))
			return
		}
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderCurrent), current.Name))

	case "clear", "reset", "none":
		switcher.SetActiveProvider("")
		e.cleanupInteractiveState(e.interactiveKeyForSessionKey(msg.SessionKey))
		{
			s := sessions.GetOrCreateActive(msg.SessionKey)
			s.SetAgentSessionID("", "")
			s.ClearHistory()
			s.SetActiveProvider("")
			sessions.Save()
		}
		// Only persist to global config when operating on the global agent;
		// in workspace mode the provider state lives on the per-workspace agent.
		if sessions == e.sessions && e.providerSaveFunc != nil {
			if err := e.providerSaveFunc(""); err != nil {
				slog.Error("failed to save provider", "error", err)
			}
		}
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderCleared))

	default:
		e.switchProvider(p, msg, sessions, switcher, args[0])
	}
}

func (e *Engine) cmdProviderAdd(p Platform, msg *Message, switcher ProviderSwitcher, args []string) {
	if len(args) == 0 {
		if supportsCards(p) {
			e.replyWithCard(p, msg.ReplyCtx, e.renderProviderAddCard(msg.SessionKey))
			return
		}
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderAddUsage))
		return
	}

	var prov ProviderConfig

	// Join args back; detect JSON (starts with '{') vs positional
	raw := strings.Join(args, " ")
	raw = strings.TrimSpace(raw)

	if strings.HasPrefix(raw, "{") {
		// JSON format: /provider add {"name":"example","api_key":"sk-xxx",...}
		var jp struct {
			Name    string            `json:"name"`
			APIKey  string            `json:"api_key"`
			BaseURL string            `json:"base_url"`
			Model   string            `json:"model"`
			Env     map[string]string `json:"env"`
		}
		if err := json.Unmarshal([]byte(raw), &jp); err != nil {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderAddFailed), "invalid JSON: "+err.Error()))
			return
		}
		if jp.Name == "" {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderAddFailed), "\"name\" is required"))
			return
		}
		prov = ProviderConfig{Name: jp.Name, APIKey: jp.APIKey, BaseURL: jp.BaseURL, Model: jp.Model, Env: jp.Env}
	} else {
		// Positional: /provider add <name> <api_key> [base_url] [model]
		if len(args) < 2 {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderAddUsage))
			return
		}
		prov.Name = args[0]
		prov.APIKey = args[1]
		if len(args) > 2 {
			prov.BaseURL = args[2]
		}
		if len(args) > 3 {
			prov.Model = args[3]
		}
	}

	// Check for duplicates
	for _, existing := range switcher.ListProviders() {
		if existing.Name == prov.Name {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderAddFailed), fmt.Sprintf("provider %q already exists", prov.Name)))
			return
		}
	}

	// Add to runtime
	updated := append(switcher.ListProviders(), prov)
	switcher.SetProviders(updated)

	// Persist to config
	if e.providerAddSaveFunc != nil {
		if err := e.providerAddSaveFunc(prov); err != nil {
			slog.Error("failed to persist provider", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderAdded), prov.Name, prov.Name))
}

func (e *Engine) cmdProviderRemove(p Platform, msg *Message, switcher ProviderSwitcher, args []string) {
	if len(args) == 0 {
		e.reply(p, msg.ReplyCtx, "Usage: /provider remove <name>")
		return
	}
	name := args[0]

	providers := switcher.ListProviders()
	found := false
	var remaining []ProviderConfig
	for _, prov := range providers {
		if prov.Name == name {
			found = true
		} else {
			remaining = append(remaining, prov)
		}
	}

	if !found {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderNotFound), name))
		return
	}

	// If removing the active provider, clear it
	active := switcher.GetActiveProvider()
	switcher.SetProviders(remaining)
	if active != nil && active.Name == name {
		// No active provider after removal
		slog.Info("removed active provider, clearing selection", "name", name)
	}

	// Persist
	if e.providerRemoveSaveFunc != nil {
		if err := e.providerRemoveSaveFunc(name); err != nil {
			slog.Error("failed to persist provider removal", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderRemoved), name))
}

// resetAllSessions resets the agent session ID and clears history for all
// active sessions. Used when the provider changes via the management API
// (where there is no single session key context).
func (e *Engine) resetAllSessions() {
	for _, s := range e.sessions.AllSessions() {
		s.SetAgentSessionID("", "")
		s.ClearHistory()
	}
	e.sessions.Save()
}

func (e *Engine) switchProvider(p Platform, msg *Message, sessions *SessionManager, switcher ProviderSwitcher, name string) {
	if !switcher.SetActiveProvider(name) {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderNotFound), name))
		return
	}
	e.cleanupInteractiveState(e.interactiveKeyForSessionKey(msg.SessionKey))

	s := sessions.GetOrCreateActive(msg.SessionKey)
	s.SetAgentSessionID("", "")
	s.ClearHistory()
	// Persist the provider choice so that a subsequent --resume after a
	// agent-bridge process restart can re-bind the agent's activeIdx; without
	// this the agent reverts to its default provider while the saved
	// agent_session_id keeps the conversation going, producing "model X
	// does not exist" errors against the wrong base_url. See agent-bridge
	// internal task t-20260614-qp7xnl.
	s.SetActiveProvider(name)
	sessions.Save()

	// Only persist to global config when operating on the global agent;
	// in workspace mode the provider state lives on the per-workspace agent.
	if sessions == e.sessions && e.providerSaveFunc != nil {
		if err := e.providerSaveFunc(name); err != nil {
			slog.Error("failed to save provider", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderSwitched), name))
}

// handlePendingProviderAdd checks for a pending provider add state (from the
// card-driven add flow) and completes the add if the user sends the required input.
func (e *Engine) handlePendingProviderAdd(p Platform, msg *Message, content string, interactiveKey string) bool {
	if strings.HasPrefix(content, "/") {
		return false
	}
	if interactiveKey == "" {
		interactiveKey = e.interactiveKeyForSessionKey(msg.SessionKey)
	}
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return false
	}
	state.mu.Lock()
	pa := state.pendingProviderAdd
	if pa == nil {
		state.mu.Unlock()
		return false
	}
	paCopy := *pa
	state.pendingProviderAdd = nil
	state.mu.Unlock()

	switcher, ok := e.agent.(ProviderSwitcher)
	if !ok {
		return false
	}

	var prov ProviderConfig
	switch paCopy.phase {
	case "other":
		fields := strings.Fields(content)
		if len(fields) < 2 {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgProviderAddUsage))
			return true
		}
		prov.Name = fields[0]
		prov.APIKey = fields[1]
		if len(fields) > 2 {
			prov.BaseURL = fields[2]
		}
		if len(fields) > 3 {
			prov.Model = fields[3]
		}
	default:
		return false
	}

	for _, existing := range switcher.ListProviders() {
		if existing.Name == prov.Name {
			e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderAddFailed), fmt.Sprintf("provider %q already exists", prov.Name)))
			return true
		}
	}

	updated := append(switcher.ListProviders(), prov)
	switcher.SetProviders(updated)
	if e.providerAddSaveFunc != nil {
		if err := e.providerAddSaveFunc(prov); err != nil {
			slog.Error("failed to persist provider", "error", err)
		}
	}
	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgProviderAdded), prov.Name, prov.Name))
	return true
}

// setPendingProviderAdd stores a pending provider add state for the card-driven flow.
func (e *Engine) setPendingProviderAdd(sessionKey string, pa *pendingProviderAddState) {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[interactiveKey]
	if !ok {
		state = &interactiveState{}
		e.interactiveStates[interactiveKey] = state
	}
	e.interactiveMu.Unlock()
	state.mu.Lock()
	state.pendingProviderAdd = pa
	state.mu.Unlock()
}

// getPendingProviderAdd retrieves pending provider add state without removing it.
func (e *Engine) getPendingProviderAdd(sessionKey string) *pendingProviderAddState {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.pendingProviderAdd == nil {
		return nil
	}
	cp := *state.pendingProviderAdd
	return &cp
}

// ──────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────

// SendToSession sends a message to an active session from an external caller (API/CLI).
// If sessionKey is empty, it picks the first active session.
func (e *Engine) SendToSession(sessionKey, message string) error {
	return e.SendToSessionWithAttachments(sessionKey, message, nil, nil)
}

// SendOptions controls optional behavior for external send callers.
type SendOptions struct {
	WorkDir string
}

func (e *Engine) SendToSessionWithAttachments(sessionKey, message string, images []ImageAttachment, files []FileAttachment) error {
	return e.SendToSessionWithOptions(sessionKey, message, images, files, SendOptions{})
}

func (e *Engine) SendToSessionWithOptions(sessionKey, message string, images []ImageAttachment, files []FileAttachment, opts SendOptions) error {
	if strings.TrimSpace(opts.WorkDir) != "" {
		workDir, useWorkDirSession, err := e.resolveSendWorkDir(opts.WorkDir)
		if err != nil {
			return err
		}
		if useWorkDirSession {
			return e.SendToSessionInWorkDir(sessionKey, message, images, files, workDir)
		}
	}

	state, p, replyCtx, err := e.resolveOutboundSessionTarget(sessionKey, len(images) > 0 || len(files) > 0)
	if err != nil {
		return err
	}

	if message == "" && len(images) == 0 && len(files) == 0 {
		return fmt.Errorf("message or attachment is required")
	}
	if (len(images) > 0 || len(files) > 0) && !e.attachmentSendEnabled {
		return ErrAttachmentSendDisabled
	}

	var imageSender ImageSender
	if len(images) > 0 {
		var ok bool
		imageSender, ok = p.(ImageSender)
		if !ok {
			return fmt.Errorf("platform %s: %w", p.Name(), ErrNotSupported)
		}
	}

	var fileSender FileSender
	if len(files) > 0 {
		var ok bool
		fileSender, ok = p.(FileSender)
		if !ok {
			return fmt.Errorf("platform %s: %w", p.Name(), ErrNotSupported)
		}
	}

	if message != "" {
		if err := e.waitOutgoing(p); err != nil {
			return err
		}
		if err := p.Send(e.ctx, replyCtx, message); err != nil {
			return err
		}
		if state != nil {
			state.mu.Lock()
			state.sideText = strings.TrimSpace(message)
			state.mu.Unlock()
		}
	}
	for _, img := range images {
		if err := e.waitOutgoing(p); err != nil {
			return err
		}
		if err := imageSender.SendImage(e.ctx, replyCtx, img); err != nil {
			return err
		}
	}
	for _, file := range files {
		if err := e.waitOutgoing(p); err != nil {
			return err
		}
		if err := fileSender.SendFile(e.ctx, replyCtx, file); err != nil {
			return err
		}
	}
	return nil
}

type sendTarget struct {
	state      *interactiveState
	platform   Platform
	replyCtx   any
	sessionKey string
}

func (e *Engine) SendToSessionInWorkDir(sessionKey, message string, images []ImageAttachment, files []FileAttachment, workDir string) error {
	if message == "" && len(images) == 0 && len(files) == 0 {
		return fmt.Errorf("message or attachment is required")
	}
	if (len(images) > 0 || len(files) > 0) && !e.attachmentSendEnabled {
		return ErrAttachmentSendDisabled
	}

	target, err := e.resolveSendTarget(sessionKey, len(images) > 0 || len(files) > 0)
	if err != nil {
		return err
	}
	if target.platform == nil {
		return fmt.Errorf("no active session found (key=%q)", sessionKey)
	}

	_, sessions, err := e.getOrCreateWorkspaceAgent(workDir)
	if err != nil {
		return err
	}
	session := sessions.NewSession(target.sessionKey, "send")
	if message != "" {
		session.AddHistory("assistant", message)
	}
	sessions.Save()
	e.bindSendWorkDir(target.sessionKey, workDir)

	var imageSender ImageSender
	if len(images) > 0 {
		var ok bool
		imageSender, ok = target.platform.(ImageSender)
		if !ok {
			return fmt.Errorf("platform %s: %w", target.platform.Name(), ErrNotSupported)
		}
	}

	var fileSender FileSender
	if len(files) > 0 {
		var ok bool
		fileSender, ok = target.platform.(FileSender)
		if !ok {
			return fmt.Errorf("platform %s: %w", target.platform.Name(), ErrNotSupported)
		}
	}

	if message != "" {
		if err := e.waitOutgoing(target.platform); err != nil {
			return err
		}
		if err := target.platform.Send(e.ctx, target.replyCtx, message); err != nil {
			return err
		}
		if target.state != nil {
			target.state.mu.Lock()
			target.state.sideText = strings.TrimSpace(message)
			target.state.mu.Unlock()
		}
	}
	for _, img := range images {
		if err := e.waitOutgoing(target.platform); err != nil {
			return err
		}
		if err := imageSender.SendImage(e.ctx, target.replyCtx, img); err != nil {
			return err
		}
	}
	for _, file := range files {
		if err := e.waitOutgoing(target.platform); err != nil {
			return err
		}
		if err := fileSender.SendFile(e.ctx, target.replyCtx, file); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) resolveSendTarget(sessionKey string, attachments bool) (sendTarget, error) {
	e.interactiveMu.Lock()

	resolvedSessionKey := sessionKey
	var state *interactiveState
	if sessionKey != "" {
		state = e.interactiveStates[sessionKey]
	} else if len(e.interactiveStates) == 1 {
		for key, s := range e.interactiveStates {
			resolvedSessionKey = key
			state = s
			break
		}
	} else if len(e.interactiveStates) > 1 && attachments {
		e.interactiveMu.Unlock()
		return sendTarget{}, fmt.Errorf("multiple active sessions; must specify --session to send attachments")
	} else {
		for key, s := range e.interactiveStates {
			resolvedSessionKey = key
			state = s
			break
		}
	}
	e.interactiveMu.Unlock()

	var p Platform
	var replyCtx any
	if state != nil {
		state.mu.Lock()
		p = state.platform
		replyCtx = state.replyCtx
		state.mu.Unlock()
	}

	if p == nil && resolvedSessionKey != "" {
		strippedKey := resolvedSessionKey
		platformName := ""
		if idx := strings.Index(strippedKey, ":"); idx > 0 {
			platformName = strippedKey[:idx]
		}
		var targetPlatform Platform
		for _, candidate := range e.platforms {
			if candidate.Name() == platformName {
				targetPlatform = candidate
				break
			}
		}
		if targetPlatform == nil {
			for _, candidate := range e.platforms {
				needle := ":" + candidate.Name() + ":"
				if idx := strings.Index(strippedKey, needle); idx >= 0 {
					targetPlatform = candidate
					strippedKey = strippedKey[idx+1:]
					break
				}
			}
		}
		if targetPlatform != nil {
			rc, ok := targetPlatform.(ReplyContextReconstructor)
			if !ok {
				return sendTarget{}, fmt.Errorf("platform %q does not support proactive messaging", targetPlatform.Name())
			}
			reconstructed, err := rc.ReconstructReplyCtx(strippedKey)
			if err != nil {
				return sendTarget{}, fmt.Errorf("reconstruct reply context: %w", err)
			}
			p = targetPlatform
			replyCtx = reconstructed
			resolvedSessionKey = strippedKey
		}
	}

	return sendTarget{
		state:      state,
		platform:   p,
		replyCtx:   replyCtx,
		sessionKey: resolvedSessionKey,
	}, nil
}

func (e *Engine) resolveSendWorkDir(workDir string) (string, bool, error) {
	current := e.currentSendWorkDir()
	target, err := normalizeSendWorkDir(workDir, current)
	if err != nil {
		return "", false, err
	}
	info, err := os.Stat(target)
	if err != nil {
		return "", false, fmt.Errorf("work_dir %q: %w", target, err)
	}
	if !info.IsDir() {
		return "", false, fmt.Errorf("work_dir %q is not a directory", target)
	}

	if current != "" {
		if normalizedCurrent, err := normalizeSendWorkDir(current, ""); err == nil && normalizedCurrent == target {
			return target, false, nil
		}
	}
	return target, true, nil
}

func (e *Engine) currentSendWorkDir() string {
	if e.agent != nil {
		if wd, ok := e.agent.(interface{ GetWorkDir() string }); ok {
			if dir := strings.TrimSpace(wd.GetWorkDir()); dir != "" {
				return dir
			}
		}
	}
	if strings.TrimSpace(e.baseWorkDir) != "" {
		return strings.TrimSpace(e.baseWorkDir)
	}
	if cwd, err := os.Getwd(); err == nil {
		return cwd
	}
	return ""
}

func normalizeSendWorkDir(workDir, base string) (string, error) {
	dir := strings.TrimSpace(workDir)
	if dir == "" {
		return "", fmt.Errorf("work_dir is required")
	}
	if dir == "~" || strings.HasPrefix(dir, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolve home directory: %w", err)
		}
		if dir == "~" {
			dir = home
		} else {
			dir = filepath.Join(home, strings.TrimPrefix(dir, "~/"))
		}
	}
	if !filepath.IsAbs(dir) {
		if strings.TrimSpace(base) == "" {
			var err error
			base, err = os.Getwd()
			if err != nil {
				return "", fmt.Errorf("resolve current directory: %w", err)
			}
		}
		dir = filepath.Join(base, dir)
	}
	abs, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		return "", err
	}
	return abs, nil
}

// SendTTSToSession synthesizes and sends a voice message to an active session.
// It is used by the local API/CLI so agents can call `agent-bridge send --tts`.
func (e *Engine) SendTTSToSession(sessionKey, text string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("tts text is required")
	}
	_, p, replyCtx, err := e.resolveOutboundSessionTarget(sessionKey, false)
	if err != nil {
		return err
	}
	return e.synthesizeAndSendTTS(p, replyCtx, text)
}

// SendAudiosToSession routes outbound audio attachments to the
// platform's AudioSender (native voice bubble + transcoding) when
// supported, falling back to FileSender otherwise. Used by
// `agent-bridge send --audio`. Mirrors SendToSessionWithAttachments for
// audio-typed clips so they don't get dispatched as generic files.
func (e *Engine) SendAudiosToSession(sessionKey string, audios []FileAttachment) error {
	if len(audios) == 0 {
		return nil
	}
	_, p, replyCtx, err := e.resolveOutboundSessionTarget(sessionKey, true)
	if err != nil {
		return err
	}
	if !e.attachmentSendEnabled {
		return ErrAttachmentSendDisabled
	}

	audioSender, audioOK := p.(AudioSender)
	fileSender, fileOK := p.(FileSender)
	if !audioOK && !fileOK {
		return fmt.Errorf("platform %s: %w", p.Name(), ErrNotSupported)
	}

	for _, a := range audios {
		if err := e.waitOutgoing(p); err != nil {
			return err
		}
		format := audioFormatHint(a)
		if audioOK {
			if err := audioSender.SendAudio(e.ctx, replyCtx, a.Data, format); err != nil {
				return err
			}
			continue
		}
		// Fallback: platform has no AudioSender. Send as a plain file so
		// the user still receives the clip — but warn so operators know
		// the native voice bubble was unavailable.
		slog.Warn("send: platform has no AudioSender, falling back to SendFile",
			"platform", p.Name(), "file_name", a.FileName, "format", format)
		if err := fileSender.SendFile(e.ctx, replyCtx, a); err != nil {
			return err
		}
	}
	return nil
}

// SendVideosToSession routes outbound video attachments to the
// platform's VideoSender (native video bubble) when supported, falling
// back to FileSender otherwise. Used by `agent-bridge send --video`.
func (e *Engine) SendVideosToSession(sessionKey string, videos []FileAttachment) error {
	if len(videos) == 0 {
		return nil
	}
	_, p, replyCtx, err := e.resolveOutboundSessionTarget(sessionKey, true)
	if err != nil {
		return err
	}
	if !e.attachmentSendEnabled {
		return ErrAttachmentSendDisabled
	}

	videoSender, videoOK := p.(VideoSender)
	fileSender, fileOK := p.(FileSender)
	if !videoOK && !fileOK {
		return fmt.Errorf("platform %s: %w", p.Name(), ErrNotSupported)
	}

	for _, v := range videos {
		if err := e.waitOutgoing(p); err != nil {
			return err
		}
		format := videoFormatHint(v)
		if videoOK {
			if err := videoSender.SendVideo(e.ctx, replyCtx, v.Data, format, v.FileName); err != nil {
				return err
			}
			continue
		}
		slog.Warn("send: platform has no VideoSender, falling back to SendFile",
			"platform", p.Name(), "file_name", v.FileName, "format", format)
		if err := fileSender.SendFile(e.ctx, replyCtx, v); err != nil {
			return err
		}
	}
	return nil
}

// audioFormatHint extracts the short format hint (e.g. "mp3", "opus")
// from a FileAttachment. Filename extension wins over MIME type because
// the CLI-uploaded name is more reliable than detected MIME (raw
// audio bytes often sniff to application/octet-stream).
func audioFormatHint(a FileAttachment) string {
	if ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(a.FileName), ".")); ext != "" {
		return ext
	}
	if a.MimeType != "" {
		// "audio/mpeg" -> "mpeg"; "audio/ogg; codecs=opus" -> "ogg"
		mt := strings.ToLower(a.MimeType)
		if i := strings.Index(mt, ";"); i >= 0 {
			mt = mt[:i]
		}
		if i := strings.Index(mt, "/"); i >= 0 {
			return strings.TrimSpace(mt[i+1:])
		}
	}
	return ""
}

// videoFormatHint mirrors audioFormatHint for video clips.
func videoFormatHint(v FileAttachment) string {
	return audioFormatHint(v)
}

func (e *Engine) resolveOutboundSessionTarget(sessionKey string, hasAttachments bool) (*interactiveState, Platform, any, error) {
	e.interactiveMu.Lock()

	var state *interactiveState
	if sessionKey != "" {
		state = e.interactiveStates[sessionKey]
	} else if len(e.interactiveStates) == 1 {
		// Single session: use it when no sessionKey is provided (backward compatible)
		for _, s := range e.interactiveStates {
			state = s
			break
		}
	} else if len(e.interactiveStates) > 1 && hasAttachments {
		// Multiple sessions with attachments but no explicit sessionKey: ambiguous
		e.interactiveMu.Unlock()
		return nil, nil, nil, fmt.Errorf("multiple active sessions; must specify --session to send attachments")
	} else {
		// Multiple sessions but text-only: pick the first (legacy behavior)
		for _, s := range e.interactiveStates {
			state = s
			break
		}
	}
	e.interactiveMu.Unlock()

	var p Platform
	var replyCtx any
	if state != nil {
		state.mu.Lock()
		p = state.platform
		replyCtx = state.replyCtx
		state.mu.Unlock()
	}

	if p == nil && sessionKey != "" {
		strippedKey := sessionKey
		platformName := ""
		if idx := strings.Index(strippedKey, ":"); idx > 0 {
			platformName = strippedKey[:idx]
		}
		var targetPlatform Platform
		for _, candidate := range e.platforms {
			if candidate.Name() == platformName {
				targetPlatform = candidate
				break
			}
		}
		// Fallback: multi-workspace mode may prefix the session key with the
		// workspace path (same heuristic as interactive messages).
		if targetPlatform == nil {
			for _, candidate := range e.platforms {
				needle := ":" + candidate.Name() + ":"
				if idx := strings.Index(strippedKey, needle); idx >= 0 {
					targetPlatform = candidate
					strippedKey = strippedKey[idx+1:]
					break
				}
			}
		}
		if targetPlatform != nil {
			rc, ok := targetPlatform.(ReplyContextReconstructor)
			if !ok {
				return nil, nil, nil, fmt.Errorf("platform %q does not support proactive messaging", targetPlatform.Name())
			}
			reconstructed, err := rc.ReconstructReplyCtx(strippedKey)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("reconstruct reply context: %w", err)
			}
			p = targetPlatform
			replyCtx = reconstructed
		}
	}

	if p == nil {
		return nil, nil, nil, fmt.Errorf("no active session found (key=%q)", sessionKey)
	}
	return state, p, replyCtx, nil
}

// sendPermissionPrompt sends a permission prompt with interactive buttons when
// the platform supports them. Fallback chain: InlineButtonSender → CardSender → plain text.
func (e *Engine) sendPermissionPrompt(p Platform, replyCtx any, prompt, toolName, toolInput string) {
	e.hooks.Emit(HookEvent{
		Event:    HookEventPermissionRequested,
		Platform: p.Name(),
		Content:  prompt,
		Extra:    map[string]any{"tool_name": toolName},
	})

	// Try inline buttons first (Telegram)
	if bs, ok := p.(InlineButtonSender); ok {
		buttons := [][]ButtonOption{
			{
				{Text: e.i18n.T(MsgPermBtnAllow), Data: "perm:allow"},
				{Text: e.i18n.T(MsgPermBtnDeny), Data: "perm:deny"},
			},
			{
				{Text: e.i18n.T(MsgPermBtnAllowAll), Data: "perm:allow_all"},
			},
		}
		if err := e.waitOutgoing(p); err != nil {
			slog.Warn("sendPermissionPrompt: outgoing wait cancelled", "platform", p.Name(), "error", err)
			return
		}
		if err := bs.SendWithButtons(e.ctx, replyCtx, prompt, buttons); err == nil {
			return
		} else {
			slog.Warn("sendPermissionPrompt: inline buttons failed, falling back", "error", err)
		}
	}

	// Try card with buttons (Feishu/Lark)
	if supportsCards(p) {
		body := fmt.Sprintf(e.i18n.T(MsgPermCardBody), toolName, toolInput)
		extra := func(label, color string) map[string]string {
			return map[string]string{
				"perm_label": label,
				"perm_color": color,
				"perm_body":  body,
			}
		}
		allowBtn := CardButton{Text: e.i18n.T(MsgPermBtnAllow), Type: "primary", Value: "perm:allow",
			Extra: extra(""+e.i18n.T(MsgPermBtnAllow), "green")}
		denyBtn := CardButton{Text: e.i18n.T(MsgPermBtnDeny), Type: "danger", Value: "perm:deny",
			Extra: extra(""+e.i18n.T(MsgPermBtnDeny), "red")}
		allowAllBtn := CardButton{Text: e.i18n.T(MsgPermBtnAllowAll), Type: "default", Value: "perm:allow_all",
			Extra: extra(""+e.i18n.T(MsgPermBtnAllowAll), "green")}

		card := NewCard().
			Title(e.i18n.T(MsgPermCardTitle), "orange").
			Markdown(body).
			ButtonsEqual(allowBtn, denyBtn).
			Buttons(allowAllBtn).
			Note(e.i18n.T(MsgPermCardNote)).
			Build()
		e.sendWithCard(p, replyCtx, card)
		return
	}

	e.send(p, replyCtx, prompt+e.interactionHint(p, replyCtx))
}

// sendAskQuestionPrompt renders one question (by index) from the AskUserQuestion list.
// qIdx is the 0-based index of the question to display.
func (e *Engine) sendAskQuestionPrompt(p Platform, replyCtx any, questions []UserQuestion, qIdx int) {
	if qIdx >= len(questions) {
		return
	}
	q := questions[qIdx]
	total := len(questions)

	titleSuffix := ""
	if total > 1 {
		titleSuffix = fmt.Sprintf(" (%d/%d)", qIdx+1, total)
	}

	// Try card (Feishu/Lark)
	if supportsCards(p) {
		cb := NewCard().Title(e.i18n.T(MsgAskQuestionTitle)+titleSuffix, "blue")
		body := "**" + q.Question + "**"
		if q.MultiSelect {
			// For multiSelect, buttons would resolve on the first click and prevent
			// selecting multiple options. Render options as a numbered text list
			// instead, and instruct the user to reply with comma-separated numbers.
			body += e.i18n.T(MsgAskQuestionMulti) + "\n\n"
			for i, opt := range q.Options {
				body += fmt.Sprintf("%d. **%s**", i+1, opt.Label)
				if opt.Description != "" {
					body += " — " + opt.Description
				}
				body += "\n"
			}
			cb.Markdown(body)
			cb.Note(e.i18n.T(MsgAskQuestionNoteMulti))
		} else {
			cb.Markdown(body)
			for i, opt := range q.Options {
				desc := opt.Label
				if opt.Description != "" {
					desc += " — " + opt.Description
				}
				answerData := fmt.Sprintf("askq:%d:%d", qIdx, i+1)
				cb.ListItemBtnExtra(desc, opt.Label, "default", answerData, map[string]string{
					"askq_label":    opt.Label,
					"askq_question": q.Question,
				})
			}
			cb.Note(e.i18n.T(MsgAskQuestionNote))
		}
		e.sendWithCard(p, replyCtx, cb.Build())
		return
	}

	// Try inline buttons (Telegram)
	if bs, ok := p.(InlineButtonSender); ok {
		var textBuf strings.Builder
		textBuf.WriteString("*")
		textBuf.WriteString(q.Question)
		textBuf.WriteString("*")
		textBuf.WriteString(titleSuffix)
		if q.MultiSelect {
			textBuf.WriteString(e.i18n.T(MsgAskQuestionMulti))
		}
		hasDesc := false
		for _, opt := range q.Options {
			if opt.Description != "" {
				hasDesc = true
				break
			}
		}
		if hasDesc {
			textBuf.WriteString("\n")
			for i, opt := range q.Options {
				textBuf.WriteString(fmt.Sprintf("\n*%d. %s*", i+1, opt.Label))
				if opt.Description != "" {
					textBuf.WriteString(" — ")
					textBuf.WriteString(opt.Description)
				}
			}
			textBuf.WriteString("\n")
		}
		var rows [][]ButtonOption
		ms, _ := p.(MultiSelectButtonSender)
		if q.MultiSelect && ms != nil && ms.SupportsMultiSelectButtons() {
			for i, opt := range q.Options {
				rows = append(rows, []ButtonOption{{Text: "[ ] " + opt.Label, Data: fmt.Sprintf("askqt:%d:%d", qIdx, i+1)}})
			}
			rows = append(rows, []ButtonOption{{Text: e.i18n.T(MsgAskQuestionDone), Data: fmt.Sprintf("askqd:%d", qIdx)}})
			textBuf.WriteString("\n\n")
			textBuf.WriteString(e.i18n.T(MsgAskQuestionNoteToggle))
		} else {
			for i, opt := range q.Options {
				rows = append(rows, []ButtonOption{{Text: opt.Label, Data: fmt.Sprintf("askq:%d:%d", qIdx, i+1)}})
			}
			textBuf.WriteString("\n\n")
			textBuf.WriteString(e.i18n.T(MsgAskQuestionNoteOther))
		}
		textBuf.WriteString(e.interactionHint(p, replyCtx))
		if err := e.waitOutgoing(p); err != nil {
			slog.Warn("sendAskQuestionPrompt: outgoing wait cancelled", "platform", p.Name(), "error", err)
			return
		}
		if err := bs.SendWithButtons(e.ctx, replyCtx, textBuf.String(), rows); err == nil {
			return
		}
	}

	// Plain text fallback
	var sb strings.Builder
	sb.WriteString("**")
	sb.WriteString(q.Question)
	sb.WriteString("**")
	sb.WriteString(titleSuffix)
	if q.MultiSelect {
		sb.WriteString(e.i18n.T(MsgAskQuestionMulti))
	}
	sb.WriteString("\n\n")
	for i, opt := range q.Options {
		sb.WriteString(fmt.Sprintf("%d. **%s**", i+1, opt.Label))
		if opt.Description != "" {
			sb.WriteString(" — ")
			sb.WriteString(opt.Description)
		}
		sb.WriteString("\n")
	}
	note := e.i18n.T(MsgAskQuestionReplySingle)
	if q.MultiSelect {
		note = e.i18n.T(MsgAskQuestionNoteMulti)
	}
	if len(q.Options) == 0 {
		note = e.i18n.T(MsgAskQuestionReplyFree)
	}
	sb.WriteString(fmt.Sprintf("\n%s", note))
	sb.WriteString(e.interactionHint(p, replyCtx))
	e.send(p, replyCtx, sb.String())
}

// waitOutgoing blocks on the per-platform outgoing rate limiter when enabled.
func (e *Engine) waitOutgoing(p Platform) error {
	if e.outgoingRL == nil {
		return nil
	}
	return e.outgoingRL.Wait(e.ctx, p.Name())
}

func (e *Engine) renderOutgoingContentForWorkspace(p Platform, content, workspaceDir string) string {
	if strings.TrimSpace(content) == "" {
		return content
	}
	return TransformLocalReferences(content, e.references, e.agent.Name(), p.Name(), workspaceDir)
}

func (e *Engine) sendWithErrorForWorkspace(p Platform, replyCtx any, content, workspaceDir string) error {
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return err
	}
	content = e.renderOutgoingContentForWorkspace(p, content, workspaceDir)
	return e.sendAlreadyRenderedWithError(p, replyCtx, content)
}

func (e *Engine) sendForWorkspace(p Platform, replyCtx any, content, workspaceDir string) {
	_ = e.sendWithErrorForWorkspace(p, replyCtx, content, workspaceDir)
}

func (e *Engine) renderCardForPlatform(p Platform, card *Card) *Card {
	return e.renderCardForPlatformWorkspace(p, card, "")
}

func (e *Engine) renderCardForPlatformWorkspace(p Platform, card *Card, workspaceDir string) *Card {
	if card == nil {
		return nil
	}
	out := &Card{}
	if card.Header != nil {
		h := *card.Header
		out.Header = &h
	}
	out.Elements = make([]CardElement, 0, len(card.Elements))
	for _, elem := range card.Elements {
		switch v := elem.(type) {
		case CardMarkdown:
			content := v.Content
			if workspaceDir != "" {
				content = e.renderOutgoingContentForWorkspace(p, v.Content, workspaceDir)
			}
			out.Elements = append(out.Elements, CardMarkdown{Content: content})
		case CardNote:
			text := v.Text
			if workspaceDir != "" {
				text = e.renderOutgoingContentForWorkspace(p, v.Text, workspaceDir)
			}
			out.Elements = append(out.Elements, CardNote{Text: text, Tag: v.Tag})
		case CardListItem:
			text := v.Text
			if workspaceDir != "" {
				text = e.renderOutgoingContentForWorkspace(p, v.Text, workspaceDir)
			}
			out.Elements = append(out.Elements, CardListItem{
				Text:     text,
				BtnText:  v.BtnText,
				BtnType:  v.BtnType,
				BtnValue: v.BtnValue,
				Extra:    v.Extra,
			})
		default:
			out.Elements = append(out.Elements, elem)
		}
	}
	return out
}

// sendWithError applies outgoing rate limiting and p.Send. It logs wait
// cancellation and platform failures, and returns a non-nil error on either.
func (e *Engine) sendWithError(p Platform, replyCtx any, content string) error {
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return err
	}
	return e.sendAlreadyRenderedWithError(p, replyCtx, content)
}

func (e *Engine) sendAlreadyRenderedWithError(p Platform, replyCtx any, content string) error {
	start := time.Now()
	if err := p.Send(e.ctx, replyCtx, content); err != nil {
		// Check for context_token missing error (common for Weixin platform)
		if strings.Contains(err.Error(), "missing context_token") {
			slog.Error("platform send failed: context_token missing",
				"platform", p.Name(),
				"error", err,
				"content_len", len(content),
				"hint", "user needs to send a message to the bot first so a context_token can be captured")
		} else {
			slog.Error("platform send failed", "platform", p.Name(), "error", err, "content_len", len(content))
		}
		return err
	}
	if elapsed := time.Since(start); elapsed >= slowPlatformSend {
		slog.Warn("slow platform send", "platform", p.Name(), "elapsed", elapsed, "content_len", len(content))
	}
	return nil
}

// send wraps p.Send with error logging, slow-operation warnings, and outgoing rate limiting.
func (e *Engine) send(p Platform, replyCtx any, content string) {
	_ = e.sendWithError(p, replyCtx, content)
}

// sendRaw sends content without local-reference rendering. This is used for raw
// tool outputs, where preserving the original text is preferable to applying the
// agent-facing reference display transform.
func (e *Engine) sendRaw(p Platform, replyCtx any, content string) {
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return
	}
	_ = e.sendAlreadyRenderedWithError(p, replyCtx, content)
}

// drainEvents discards any buffered events from the channel.
// Called before a new turn to prevent stale events from a previous turn's
// agent process from being mistaken for the new turn's response.
func drainEvents(ch <-chan Event) {
	drained := 0
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				// Channel is closed; stop immediately to avoid an infinite loop.
				return
			}
			drained++
		default:
			if drained > 0 {
				slog.Warn("drained stale events from previous turn", "count", drained)
			}
			return
		}
	}
}

// replyWithError applies outgoing rate limiting and p.Reply.
func (e *Engine) replyWithError(p Platform, replyCtx any, content string) error {
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return err
	}
	start := time.Now()
	if err := p.Reply(e.ctx, replyCtx, content); err != nil {
		slog.Error("platform reply failed", "platform", p.Name(), "error", err, "content_len", len(content))
		return err
	}
	if elapsed := time.Since(start); elapsed >= slowPlatformSend {
		slog.Warn("slow platform reply", "platform", p.Name(), "elapsed", elapsed, "content_len", len(content))
	}
	return nil
}

// reply wraps p.Reply with error logging, slow-operation warnings, and outgoing rate limiting.
func (e *Engine) reply(p Platform, replyCtx any, content string) {
	_ = e.replyWithError(p, replyCtx, content)
}

// replyWithButtons sends a reply with inline buttons if the platform supports it,
// otherwise falls back to plain text reply.
func (e *Engine) replyWithButtons(p Platform, replyCtx any, content string, buttons [][]ButtonOption) {
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return
	}
	if bs, ok := p.(InlineButtonSender); ok {
		if err := bs.SendWithButtons(e.ctx, replyCtx, content, buttons); err == nil {
			return
		}
	}
	e.reply(p, replyCtx, content)
}

func supportsCards(p Platform) bool {
	_, ok := p.(CardSender)
	return ok
}

// replyWithCard sends a structured card via CardSender.
// For platforms without card support, renders as plain text (no intermediate fallback).
func (e *Engine) replyWithCard(p Platform, replyCtx any, card *Card) {
	if card == nil {
		slog.Error("replyWithCard: nil card", "platform", p.Name())
		return
	}
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return
	}
	if cs, ok := p.(CardSender); ok {
		rendered := e.renderCardForPlatform(p, card)
		if err := cs.ReplyCard(e.ctx, replyCtx, rendered); err != nil {
			slog.Error("card reply failed", "platform", p.Name(), "error", err)
		}
		return
	}
	e.reply(p, replyCtx, e.renderCardForPlatform(p, card).RenderText())
}

// sendWithCard sends a card as a new message (not a reply).
func (e *Engine) sendWithCard(p Platform, replyCtx any, card *Card) {
	if card == nil {
		slog.Error("sendWithCard: nil card", "platform", p.Name())
		return
	}
	if err := e.waitOutgoing(p); err != nil {
		slog.Warn("outgoing rate limit: context cancelled", "platform", p.Name(), "error", err)
		return
	}
	if cs, ok := p.(CardSender); ok {
		rendered := e.renderCardForPlatform(p, card)
		if err := cs.SendCard(e.ctx, replyCtx, rendered); err != nil {
			slog.Error("card send failed", "platform", p.Name(), "error", err)
		}
		return
	}
	e.send(p, replyCtx, e.renderCardForPlatform(p, card).RenderText())
}

// ──────────────────────────────────────────────────────────────
// Card navigation (in-place card updates)
// ──────────────────────────────────────────────────────────────

// handleCardNav is called by platforms that support in-place card updates.
// It routes nav: and act: prefixed actions to the appropriate render function.
func (e *Engine) handleCardNav(action string, sessionKey string) *Card {
	var prefix, body string
	if i := strings.Index(action, ":"); i >= 0 {
		prefix = action[:i]
		body = action[i+1:]
	} else {
		return nil
	}

	cmd, args := body, ""
	if i := strings.IndexByte(body, ' '); i >= 0 {
		cmd = body[:i]
		args = strings.TrimSpace(body[i+1:])
	}

	if prefix == "act" && cmd == "/model" {
		return e.handleModelCardAction(args, sessionKey)
	}

	if prefix == "act" {
		e.executeCardAction(cmd, args, sessionKey)
	}

	switch cmd {
	case "/help":
		return e.renderHelpGroupCard(args)
	case "/model":
		return e.renderModelCard(sessionKey)
	case "/effort":
		return e.renderEffortCard()
	case "/mode":
		return e.renderModeCard()
	case "/list":
		page := 1
		if args != "" {
			if n, err := strconv.Atoi(args); err == nil && n > 0 {
				page = n
			}
		}
		return e.renderListCardSafe(sessionKey, page)
	case "/dir":
		page := 1
		if args != "" {
			if n, err := strconv.Atoi(args); err == nil && n > 0 {
				page = n
			}
		}
		return e.renderDirCardSafe(sessionKey, page)
	case "/current":
		return e.renderCurrentCard(sessionKey)
	case "/history":
		return e.renderHistoryCard(sessionKey)
	case "/provider":
		return e.renderProviderCard()
	case "/provider/add-other", "/provider/add-cancel":
		return e.renderProviderAddCard(sessionKey)
	case "/commands":
		return e.renderCommandsCard()
	case "/alias":
		return e.renderAliasCard()
	case "/whoami":
		return e.renderWhoamiCard(&Message{
			SessionKey: sessionKey,
			UserID:     extractUserID(sessionKey),
			Platform:   extractPlatformName(sessionKey),
		})
	case "/new":
		return e.renderCurrentCard(sessionKey)
	case "/switch":
		return e.renderListCardSafe(sessionKey, 1)
	case "/delete-mode":
		if strings.HasPrefix(args, "cancel") {
			return e.renderListCardSafe(sessionKey, 1)
		}
		return e.renderDeleteModeCard(sessionKey)
	case "/stop":
		return e.renderCurrentCard(sessionKey)
	}
	return nil
}

func (e *Engine) handleModelCardAction(args, sessionKey string) *Card {
	agent, sessions := e.sessionContextForKey(sessionKey)
	switcher, ok := agent.(ModelSwitcher)
	if !ok {
		return e.simpleCard(e.i18n.T(MsgCardTitleModel), "indigo", e.i18n.T(MsgModelNotSupported))
	}

	target, ok := parseModelSwitchArgs(strings.Fields(args))
	if !ok {
		return e.renderModelCard(sessionKey)
	}
	target = strings.TrimSpace(target)
	if modelSwitchNeedsLookup(target) {
		fetchCtx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
		models := switcher.AvailableModels(fetchCtx)
		target = resolveModelSwitchTarget(target, models)
		cancel()
	}

	resolved, err := e.switchModelOnAgent(agent, target, agent == e.agent)
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	if err == nil {
	}
	e.cleanupInteractiveState(interactiveKey)
	if err == nil {
		sessions.Save()
	}

	return e.renderModelSwitchResultCard(resolved, err)
}

// executeCardAction performs the side-effect for act: prefixed actions
// (e.g. switching model/mode/lang) before the card is re-rendered.
func (e *Engine) executeCardAction(cmd, args, sessionKey string) {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)

	switch cmd {
	case "/model":
		if args == "" {
			return
		}
		agent, sessions := e.sessionContextForKey(sessionKey)
		switcher, ok := agent.(ModelSwitcher)
		if !ok {
			return
		}
		fetchCtx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
		target, ok := parseModelSwitchArgs(strings.Fields(args))
		if !ok {
			cancel()
			return
		}
		target = strings.TrimSpace(target)
		if modelSwitchNeedsLookup(target) {
			models := switcher.AvailableModels(fetchCtx)
			target = resolveModelSwitchTarget(target, models)
		}
		cancel()
		e.cleanupInteractiveState(interactiveKey)
		e.interactiveMu.Lock()
		state := e.interactiveStates[interactiveKey]
		if state == nil {
			state = &interactiveState{}
			e.interactiveStates[interactiveKey] = state
		}
		e.interactiveMu.Unlock()
		state.mu.Lock()
		state.modelSwitch = &modelSwitchState{phase: "switching", target: target}
		state.mu.Unlock()
		go e.performModelSwitchAsync(sessionKey, state, agent, sessions, target)

	case "/effort":
		if args == "" {
			return
		}
		switcher, ok := e.agent.(ReasoningEffortSwitcher)
		if !ok {
			return
		}
		efforts := switcher.AvailableReasoningEfforts()
		target := strings.ToLower(strings.TrimSpace(args))
		if idx, err := strconv.Atoi(target); err == nil && idx >= 1 && idx <= len(efforts) {
			target = efforts[idx-1]
		}
		for _, effort := range efforts {
			if effort == target {
				switcher.SetReasoningEffort(target)
				e.cleanupInteractiveState(interactiveKey)
				s := e.sessions.GetOrCreateActive(sessionKey)
				s.SetAgentSessionID("", "")
				s.ClearHistory()
				e.sessions.Save()
				return
			}
		}

	case "/mode":
		if args == "" {
			return
		}
		switcher, ok := e.agent.(ModeSwitcher)
		if !ok {
			return
		}
		newMode := strings.ToLower(args)
		switcher.SetMode(newMode)
		if e.applyLiveModeChange(sessionKey, switcher.GetMode()) {
			e.cleanupInteractiveState(interactiveKey)
			return
		}
		e.cleanupInteractiveState(interactiveKey)
		// Mode change requires a new session to take effect
		s := e.sessions.GetOrCreateActive(sessionKey)
		s.SetAgentSessionID("", "")
		s.ClearHistory()
		e.sessions.Save()

	case "/provider":
		if args == "" {
			return
		}
		switcher, ok := e.agent.(ProviderSwitcher)
		if !ok {
			return
		}
		provName := args
		if provName == "clear" {
			provName = ""
		}
		if switcher.SetActiveProvider(provName) {
			e.cleanupInteractiveState(interactiveKey)
			s := e.sessions.GetOrCreateActive(sessionKey)
			s.SetAgentSessionID("", "")
			s.ClearHistory()
			e.sessions.Save()
			if e.providerSaveFunc != nil {
				_ = e.providerSaveFunc(provName)
			}
		}

	case "/provider/add-other":
		e.setPendingProviderAdd(sessionKey, &pendingProviderAddState{
			phase: "other",
		})

	case "/provider/add-cancel":
		e.setPendingProviderAdd(sessionKey, nil)

	case "/provider/link":
		e.executeProviderLink(sessionKey, args)

	case "/new":
		_, sessions := e.sessionContextForKey(sessionKey)
		e.cleanupInteractiveState(interactiveKey)
		sessions.NewSession(sessionKey, "")

	case "/delete-mode":
		e.executeDeleteModeAction(sessionKey, args)

	case "/switch":
		if args == "" {
			return
		}
		agent, sessions := e.sessionContextForKey(sessionKey)
		agentSessions, err := agent.ListSessions(e.ctx)
		if err != nil || len(agentSessions) == 0 {
			return
		}
		agentSessions = e.applySessionFilter(agentSessions, sessions)
		matched := e.matchSession(agentSessions, sessions, args)
		if matched == nil {
			return
		}
		e.cleanupInteractiveState(interactiveKey)
		session := sessions.SwitchToAgentSession(sessionKey, matched.ID, agent.Name(), matched.Summary)
		session.ClearHistory()

	case "/dir":
		fields := strings.Fields(args)
		if len(fields) == 0 {
			return
		}
		agent, sessions := e.sessionContextForKey(sessionKey)
		ik := e.interactiveKeyForSessionKey(sessionKey)
		var applyArgs []string
		switch fields[0] {
		case "select":
			if len(fields) < 2 {
				return
			}
			applyArgs = []string{fields[1]}
		case "reset":
			applyArgs = []string{"reset"}
		case "prev":
			applyArgs = []string{"-"}
		default:
			return
		}
		errMsg, _ := e.dirApply(agent, sessions, ik, sessionKey, applyArgs)
		if errMsg != "" {
			slog.Debug("dir card action failed", "message", errMsg)
		}

	case "/stop":
		e.stopInteractiveSession(interactiveKey, nil, nil)

	}
}

func (e *Engine) getOrCreateDeleteModeState(sessionKey string, p Platform, replyCtx any) *deleteModeState {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[interactiveKey]
	if !ok || state == nil {
		state = &interactiveState{platform: p, replyCtx: replyCtx, eventsNeedResync: true}
		e.interactiveStates[interactiveKey] = state
	} else {
		state.platform = p
		state.replyCtx = replyCtx
	}
	e.interactiveMu.Unlock()

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteMode == nil {
		state.deleteMode = &deleteModeState{}
	}
	dm := state.deleteMode
	dm.page = 1
	dm.phase = "select"
	dm.hint = ""
	dm.result = ""
	dm.selectedIDs = make(map[string]struct{})
	return dm
}

func (e *Engine) getDeleteModeState(sessionKey string) *deleteModeState {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteMode == nil {
		return nil
	}
	cp := &deleteModeState{
		page:        state.deleteMode.page,
		selectedIDs: make(map[string]struct{}, len(state.deleteMode.selectedIDs)),
		phase:       state.deleteMode.phase,
		hint:        state.deleteMode.hint,
		result:      state.deleteMode.result,
	}
	for id := range state.deleteMode.selectedIDs {
		cp.selectedIDs[id] = struct{}{}
	}
	return cp
}

func (e *Engine) getModelSwitchState(sessionKey string) *modelSwitchState {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.modelSwitch == nil {
		return nil
	}
	cp := *state.modelSwitch
	return &cp
}

func (e *Engine) renderDeleteModeCard(sessionKey string) *Card {
	agent, sessions := e.sessionContextForKey(sessionKey)
	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		return e.simpleCard(e.i18n.T(MsgDeleteModeTitle), "red", err.Error())
	}
	agentSessions = e.applySessionFilter(agentSessions, sessions)
	dm := e.getDeleteModeState(sessionKey)
	if dm == nil {
		return e.simpleCard(e.i18n.T(MsgDeleteModeTitle), "red", e.i18n.T(MsgDeleteUsage))
	}
	switch dm.phase {
	case "confirm":
		return e.renderDeleteModeConfirmCard(sessions, dm, agentSessions)
	case "result":
		return e.renderDeleteModeResultCard(dm)
	case "deleting":
		return e.renderDeleteModeDeletingCard(dm)
	default:
		return e.renderDeleteModeSelectCard(sessionKey, sessions, dm, agentSessions)
	}
}

func (e *Engine) renderDeleteModeSelectCard(sessionKey string, sessions *SessionManager, dm *deleteModeState, agentSessions []AgentSessionInfo) *Card {
	if len(agentSessions) == 0 {
		return e.simpleCard(e.i18n.T(MsgDeleteModeTitle), "red", e.i18n.T(MsgListEmpty))
	}
	total := len(agentSessions)
	totalPages := (total + listPageSize - 1) / listPageSize
	page := dm.page
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * listPageSize
	end := start + listPageSize
	if end > total {
		end = total
	}

	cb := NewCard().Title(e.i18n.T(MsgDeleteModeTitle), "carmine")
	activeAgentID := sessions.GetOrCreateActive(sessionKey).GetAgentSessionID()
	selectedCount := 0
	for i := start; i < end; i++ {
		s := agentSessions[i]
		isActive := activeAgentID == s.ID
		isSelected := false
		if !isActive {
			_, isSelected = dm.selectedIDs[s.ID]
		}
		marker := "[ ]"
		if isActive {
			marker = "(current)"
		} else if isSelected {
			marker = "[x]"
			selectedCount++
		}
		btnText := e.i18n.T(MsgDeleteModeSelect)
		btnType := "default"
		action := fmt.Sprintf("act:/delete-mode toggle %s", s.ID)
		if isActive {
			btnText = e.i18n.T(MsgCardTitleCurrentSession)
			btnType = "primary"
			action = fmt.Sprintf("act:/delete-mode noop %s", s.ID)
		} else if isSelected {
			btnText = e.i18n.T(MsgDeleteModeSelected)
			btnType = "primary"
		}
		cb.ListItemBtn(
			e.i18n.Tf(MsgListItem, marker, i+1, e.deleteSessionDisplayName(sessions, &s), s.MessageCount, s.ModifiedAt.Format("01-02 15:04")),
			btnText,
			btnType,
			action,
		)
	}
	cb.TaggedNote("delete-mode-selected-count", e.i18n.Tf(MsgDeleteModeSelectedCount, selectedCount))
	if dm.hint != "" {
		cb.Note(dm.hint)
	}
	cb.Buttons(
		DangerBtn(e.i18n.T(MsgDeleteModeDeleteSelected), "act:/delete-mode confirm"),
		DefaultBtn(e.i18n.T(MsgDeleteModeCancel), "act:/delete-mode cancel"),
	)

	var navBtns []CardButton
	if page > 1 {
		navBtns = append(navBtns, DefaultBtn(e.i18n.T(MsgCardPrev), fmt.Sprintf("act:/delete-mode page %d", page-1)))
	}
	if page < totalPages {
		navBtns = append(navBtns, DefaultBtn(e.i18n.T(MsgCardNext), fmt.Sprintf("act:/delete-mode page %d", page+1)))
	}
	if len(navBtns) > 0 {
		cb.Buttons(navBtns...)
	}
	return cb.Build()
}

func (e *Engine) renderDeleteModeConfirmCard(sessions *SessionManager, dm *deleteModeState, agentSessions []AgentSessionInfo) *Card {
	selectedNames := e.deleteModeSelectionNames(sessions, dm, agentSessions)
	body := strings.Join(selectedNames, "\n")
	if body == "" {
		body = e.i18n.T(MsgDeleteModeEmptySelection)
	}
	return NewCard().
		Title(e.i18n.T(MsgDeleteModeConfirmTitle), "carmine").
		Markdown(body).
		Buttons(
			DangerBtn(e.i18n.T(MsgDeleteModeConfirmButton), "act:/delete-mode submit"),
			DefaultBtn(e.i18n.T(MsgDeleteModeBackButton), "act:/delete-mode back"),
		).
		Build()
}

func (e *Engine) renderDeleteModeResultCard(dm *deleteModeState) *Card {
	return NewCard().
		Title(e.i18n.T(MsgDeleteModeResultTitle), "turquoise").
		Markdown(dm.result).
		Buttons(DefaultBtn(e.i18n.T(MsgCardBack), "nav:/list 1")).
		Build()
}

func (e *Engine) renderDeleteModeDeletingCard(dm *deleteModeState) *Card {
	return NewCard().
		Title(e.i18n.T(MsgDeleteModeDeletingTitle), "orange").
		Markdown(dm.hint).
		Build()
}

// performDeleteModeAsync runs the actual session deletions in a background
// goroutine so that the card callback can return immediately with a "deleting"
// indicator. Once all deletions finish it updates the interactive state and
// pushes a result card to the originating platform.
func (e *Engine) performDeleteModeAsync(sessionKey string, selectedIDs map[string]struct{}) {
	lines := e.submitDeleteModeSelection(sessionKey, selectedIDs)
	result := strings.Join(lines, "\n")

	// Update the interactive state to "result" phase.
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state != nil {
		state.mu.Lock()
		if state.deleteMode != nil {
			state.deleteMode.result = result
			state.deleteMode.hint = ""
			state.deleteMode.phase = "result"
		}
		state.mu.Unlock()
	}

	// Push the result card to the platform proactively.
	e.pushDeleteModeResultCard(sessionKey)
}

// pushDeleteModeResultCard resolves the platform from the session key and
// refreshes the "deleting" card in-place with the final result. Falls back to
// sending a new card if the platform does not support in-place card refresh.
func (e *Engine) pushDeleteModeResultCard(sessionKey string) {
	dm := e.getDeleteModeState(sessionKey)
	if dm == nil {
		return
	}
	card := e.renderDeleteModeResultCard(dm)

	platformName := extractPlatformName(sessionKey)
	var targetPlatform Platform
	for _, p := range e.platforms {
		if p.Name() == platformName {
			targetPlatform = p
			break
		}
	}
	if targetPlatform == nil {
		slog.Warn("delete mode: platform not found for result card", "sessionKey", sessionKey)
		return
	}

	// Prefer in-place card refresh (updates the "deleting" card to show results).
	if refresher, ok := targetPlatform.(CardRefresher); ok {
		if err := refresher.RefreshCard(e.ctx, sessionKey, card); err != nil {
			slog.Warn("delete mode: refresh card failed, falling back to new message", "error", err)
		} else {
			return
		}
	}

	// Fallback: send a new card message.
	rc, ok := targetPlatform.(ReplyContextReconstructor)
	if !ok {
		slog.Warn("delete mode: platform does not support proactive messaging", "platform", platformName)
		return
	}
	rctx, err := rc.ReconstructReplyCtx(sessionKey)
	if err != nil {
		slog.Error("delete mode: reconstruct reply ctx failed", "error", err)
		return
	}
	e.sendWithCard(targetPlatform, rctx, card)
}

func (e *Engine) performModelSwitchAsync(sessionKey string, state *interactiveState, agent Agent, sessions *SessionManager, target string) {
	resolved, err := e.switchModelOnAgent(agent, target, agent == e.agent)
	if err == nil {
		sessions.Save()
	}

	resultCard := e.renderModelSwitchResultCard(resolved, err)
	if state != nil {
		state.mu.Lock()
		if state.modelSwitch != nil {
			state.modelSwitch.phase = "result"
			state.modelSwitch.target = resolved
			if err != nil {
				state.modelSwitch.result = e.i18n.Tf(MsgModelCardSwitchFailed, err)
			} else {
				state.modelSwitch.result = e.i18n.Tf(MsgModelCardSwitched, resolved)
			}
		}
		state.mu.Unlock()
	}
	e.pushModelSwitchResultCard(sessionKey, resultCard)
	e.cleanupInteractiveState(e.interactiveKeyForSessionKey(sessionKey), state)
}

func (e *Engine) pushModelSwitchResultCard(sessionKey string, card *Card) {
	platformName := extractPlatformName(sessionKey)
	var targetPlatform Platform
	for _, p := range e.platforms {
		if p.Name() == platformName {
			targetPlatform = p
			break
		}
	}
	if targetPlatform == nil {
		slog.Warn("model switch: platform not found for result card", "sessionKey", sessionKey)
		return
	}

	if refresher, ok := targetPlatform.(CardRefresher); ok {
		if err := refresher.RefreshCard(e.ctx, sessionKey, card); err != nil {
			slog.Warn("model switch: refresh card failed, falling back to new message", "error", err)
		} else {
			return
		}
	}

	rc, ok := targetPlatform.(ReplyContextReconstructor)
	if !ok {
		slog.Warn("model switch: platform does not support proactive messaging", "platform", platformName)
		return
	}
	rctx, err := rc.ReconstructReplyCtx(sessionKey)
	if err != nil {
		slog.Error("model switch: reconstruct reply ctx failed", "error", err)
		return
	}
	e.sendWithCard(targetPlatform, rctx, card)
}

func (e *Engine) deleteModeSelectionNames(sessions *SessionManager, dm *deleteModeState, agentSessions []AgentSessionInfo) []string {
	names := make([]string, 0, len(dm.selectedIDs))
	for i := range agentSessions {
		if _, ok := dm.selectedIDs[agentSessions[i].ID]; ok {
			names = append(names, "- "+e.deleteSessionDisplayName(sessions, &agentSessions[i]))
		}
	}
	return names
}

func (e *Engine) executeDeleteModeAction(sessionKey, args string) {
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return
	}

	fields := strings.Fields(args)
	if len(fields) == 0 {
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleteMode == nil {
		return
	}

	dm := state.deleteMode
	switch fields[0] {
	case "toggle":
		if len(fields) < 2 {
			return
		}
		id := fields[1]
		if _, ok := dm.selectedIDs[id]; ok {
			delete(dm.selectedIDs, id)
		} else {
			dm.selectedIDs[id] = struct{}{}
		}
		dm.phase = "select"
		dm.hint = ""
	case "page":
		if len(fields) < 2 {
			return
		}
		if n, err := strconv.Atoi(fields[1]); err == nil && n > 0 {
			dm.page = n
		}
		dm.phase = "select"
	case "confirm":
		if len(dm.selectedIDs) == 0 {
			dm.phase = "select"
			dm.hint = e.i18n.T(MsgDeleteModeEmptySelection)
			return
		}
		dm.phase = "confirm"
		dm.hint = ""
	case "back":
		dm.phase = "select"
	case "submit":
		// Capture selected IDs and switch to "deleting" phase immediately
		// so the card callback can return a loading card without blocking.
		ids := make(map[string]struct{}, len(dm.selectedIDs))
		for id := range dm.selectedIDs {
			ids[id] = struct{}{}
		}
		dm.selectedIDs = make(map[string]struct{})
		dm.phase = "deleting"
		dm.hint = e.i18n.Tf(MsgDeleteModeDeletingBody, len(ids))
		go e.performDeleteModeAsync(sessionKey, ids)
	case "form-submit":
		dm.selectedIDs = parseDeleteModeSelectedIDs(fields[1:])
		if len(dm.selectedIDs) == 0 {
			dm.phase = "select"
			dm.hint = e.i18n.T(MsgDeleteModeEmptySelection)
			return
		}
		dm.phase = "confirm"
		dm.hint = ""
	case "cancel":
		state.deleteMode = nil
	}
}

func parseDeleteModeSelectedIDs(args []string) map[string]struct{} {
	ids := make(map[string]struct{})
	for _, arg := range args {
		for _, id := range strings.Split(arg, ",") {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			ids[id] = struct{}{}
		}
	}
	return ids
}

func (e *Engine) submitDeleteModeSelection(sessionKey string, selectedIDs map[string]struct{}) []string {
	agent, sessions := e.sessionContextForKey(sessionKey)
	deleter, ok := agent.(SessionDeleter)
	if !ok {
		return []string{e.i18n.T(MsgDeleteNotSupported)}
	}
	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		return []string{e.i18n.Tf(MsgError, err)}
	}
	agentSessions = e.applySessionFilter(agentSessions, sessions)
	seen := make(map[string]struct{}, len(agentSessions))
	lines := make([]string, 0, len(selectedIDs))
	for i := range agentSessions {
		seen[agentSessions[i].ID] = struct{}{}
		if _, ok := selectedIDs[agentSessions[i].ID]; !ok {
			continue
		}
		if line := e.deleteSingleSessionReply(&Message{SessionKey: sessionKey}, deleter, &agentSessions[i]); line != "" {
			lines = append(lines, line)
		}
	}
	missingIDs := make([]string, 0)
	for id := range selectedIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		missingIDs = append(missingIDs, id)
	}
	sort.Strings(missingIDs)
	for _, id := range missingIDs {
		lines = append(lines, fmt.Sprintf(e.i18n.T(MsgDeleteModeMissingSession), id))
	}
	if len(lines) == 0 {
		lines = append(lines, e.i18n.T(MsgDeleteModeEmptySelection))
	}
	return lines
}

func (e *Engine) renderModelCard(sessionKey string) *Card {
	if ms := e.getModelSwitchState(sessionKey); ms != nil && ms.phase == "switching" {
		return e.renderModelSwitchingCard(ms.target)
	}

	agent := e.agent
	if sessionKey != "" {
		agent, _ = e.sessionContextForKey(sessionKey)
	}

	switcher, ok := agent.(ModelSwitcher)
	if !ok {
		return e.simpleCard(e.i18n.T(MsgCardTitleModel), "indigo", e.i18n.T(MsgModelNotSupported))
	}

	fetchCtx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
	defer cancel()
	models := switcher.AvailableModels(fetchCtx)
	current := switcher.GetModel()

	var sb strings.Builder
	if current == "" {
		sb.WriteString(e.i18n.T(MsgModelDefault))
	} else {
		sb.WriteString(e.i18n.Tf(MsgModelCurrent, current))
	}

	var opts []CardSelectOption
	initVal := ""
	for i, m := range models {
		label := m.Name
		if m.Alias != "" {
			label = m.Alias + " - " + m.Name
		} else if m.Desc != "" {
			label += " — " + m.Desc
		}
		val := fmt.Sprintf("act:/model switch %d", i+1)
		opts = append(opts, CardSelectOption{Text: label, Value: val})
		if m.Name == current {
			initVal = val
		}
	}

	cb := NewCard().Title(e.i18n.T(MsgCardTitleModel), "indigo").
		Markdown(sb.String()).
		Select(e.i18n.T(MsgModelSelectPlaceholder), opts, initVal).
		Buttons(e.cardBackButton())
	cb.Note(e.i18n.T(MsgModelUsage))
	return cb.Build()
}

func (e *Engine) renderModelSwitchingCard(target string) *Card {
	return NewCard().
		Title(e.i18n.T(MsgCardTitleModel), "orange").
		Markdown(e.i18n.Tf(MsgModelCardSwitching, target)).
		Build()
}

func (e *Engine) renderModelSwitchResultCard(target string, err error) *Card {
	if err != nil {
		return NewCard().
			Title(e.i18n.T(MsgCardTitleModel), "red").
			Markdown(e.i18n.Tf(MsgModelCardSwitchFailed, err)).
			Buttons(e.modelCardBackButton()).
			Build()
	}
	return NewCard().
		Title(e.i18n.T(MsgCardTitleModel), "green").
		Markdown(e.i18n.Tf(MsgModelCardSwitched, target)).
		Buttons(e.modelCardBackButton()).
		Build()
}

func (e *Engine) renderEffortCard() *Card {
	switcher, ok := e.agent.(ReasoningEffortSwitcher)
	if !ok {
		return e.simpleCard(e.i18n.T(MsgCardTitleReasoning), "orange", e.i18n.T(MsgReasoningNotSupported))
	}

	efforts := switcher.AvailableReasoningEfforts()
	current := switcher.GetReasoningEffort()

	var sb strings.Builder
	if current == "" {
		sb.WriteString(e.i18n.T(MsgReasoningDefault))
	} else {
		sb.WriteString(e.i18n.Tf(MsgReasoningCurrent, current))
	}

	var opts []CardSelectOption
	initVal := ""
	for i, effort := range efforts {
		val := fmt.Sprintf("act:/effort %d", i+1)
		opts = append(opts, CardSelectOption{Text: effort, Value: val})
		if effort == current {
			initVal = val
		}
	}

	cb := NewCard().Title(e.i18n.T(MsgCardTitleReasoning), "orange").
		Markdown(sb.String()).
		Select(e.i18n.T(MsgReasoningSelectPlaceholder), opts, initVal).
		Buttons(e.cardBackButton())
	cb.Note(e.i18n.T(MsgReasoningUsage))
	return cb.Build()
}

func (e *Engine) renderModeCard() *Card {
	switcher, ok := e.agent.(ModeSwitcher)
	if !ok {
		return e.simpleCard(e.i18n.T(MsgCardTitleMode), "violet", e.i18n.T(MsgModeNotSupported))
	}

	current := switcher.GetMode()
	modes := switcher.PermissionModes()

	var sb strings.Builder
	for _, m := range modes {
		marker := "[ ]"
		if m.Key == current {
			marker = "(current)"
		}
		sb.WriteString(fmt.Sprintf("%s **%s** — %s\n", marker, m.Name, m.Desc))
	}

	var opts []CardSelectOption
	initVal := ""
	for _, m := range modes {
		label := m.Name
		val := "act:/mode " + m.Key
		opts = append(opts, CardSelectOption{Text: label, Value: val})
		if m.Key == current {
			initVal = val
		}
	}

	cb := NewCard().Title(e.i18n.T(MsgCardTitleMode), "violet").
		Markdown(sb.String()).
		Select(e.i18n.T(MsgModeSelectPlaceholder), opts, initVal).
		Buttons(e.cardBackButton())
	cb.Note(e.modeUsageText(modes))
	return cb.Build()
}

func (e *Engine) renderListCard(sessionKey string, page int) (*Card, error) {
	agent, sessions := e.sessionContextForKey(sessionKey)
	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		return nil, fmt.Errorf(e.i18n.T(MsgListError), err)
	}
	agentSessions = e.applySessionFilter(agentSessions, sessions)
	if len(agentSessions) == 0 {
		return e.simpleCard(e.i18n.Tf(MsgCardTitleSessions, agent.Name(), 0), "turquoise", e.i18n.T(MsgListEmpty)), nil
	}

	total := len(agentSessions)
	totalPages := (total + listPageSize - 1) / listPageSize
	if page > totalPages {
		page = totalPages
	}

	start := (page - 1) * listPageSize
	end := start + listPageSize
	if end > total {
		end = total
	}

	agentName := agent.Name()
	activeSession := sessions.GetOrCreateActive(sessionKey)
	activeAgentID := activeSession.GetAgentSessionID()

	var titleStr string
	if totalPages > 1 {
		titleStr = e.i18n.Tf(MsgCardTitleSessionsPaged, agentName, total, page, totalPages)
	} else {
		titleStr = e.i18n.Tf(MsgCardTitleSessions, agentName, total)
	}

	cb := NewCard().Title(titleStr, "turquoise")
	for i := start; i < end; i++ {
		s := agentSessions[i]
		marker := "[ ]"
		if s.ID == activeAgentID {
			marker = "(current)"
		}
		displayName := sessions.GetSessionName(s.ID)
		if displayName == "" {
			displayName = strings.ReplaceAll(s.Summary, "\n", " ")
			displayName = strings.Join(strings.Fields(displayName), " ")
			if displayName == "" {
				displayName = e.i18n.T(MsgListEmptySummary)
			}
			if len([]rune(displayName)) > 40 {
				displayName = string([]rune(displayName)[:40]) + "…"
			}
		}
		btnType := "default"
		if s.ID == activeAgentID {
			btnType = "primary"
		}
		cb.ListItemBtn(
			e.i18n.Tf(MsgListItem, marker, i+1, displayName, s.MessageCount, s.ModifiedAt.Format("01-02 15:04")),
			fmt.Sprintf("#%d", i+1),
			btnType,
			fmt.Sprintf("act:/switch %d", i+1),
		)
	}

	var navBtns []CardButton
	if page > 1 {
		navBtns = append(navBtns, e.cardPrevButton(fmt.Sprintf("nav:/list %d", page-1)))
	}
	navBtns = append(navBtns, e.cardBackButton())
	if page < totalPages {
		navBtns = append(navBtns, e.cardNextButton(fmt.Sprintf("nav:/list %d", page+1)))
	}
	cb.Buttons(navBtns...)

	if totalPages > 1 {
		cb.Note(fmt.Sprintf(e.i18n.T(MsgListPageHint), page, totalPages))
	}

	return cb.Build(), nil
}

// dirCardTruncPath shortens absolute paths for card list rows.
func dirCardTruncPath(absPath string) string {
	r := []rune(absPath)
	if len(r) <= 56 {
		return absPath
	}
	return string(r[:53]) + "…"
}

func (e *Engine) renderDirCard(sessionKey string, page int) (*Card, error) {
	agent, _ := e.sessionContextForKey(sessionKey)
	switcher, ok := agent.(WorkDirSwitcher)
	if !ok {
		return nil, fmt.Errorf("%s", e.i18n.T(MsgDirNotSupported))
	}
	currentDir := switcher.GetWorkDir()
	var history []string
	if e.dirHistory != nil {
		history = e.dirHistory.List(e.name)
	}
	total := len(history)
	totalPages := 1
	if total > 0 {
		totalPages = (total + dirCardPageSize - 1) / dirCardPageSize
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}
	start := (page - 1) * dirCardPageSize
	end := start + dirCardPageSize
	if end > total {
		end = total
	}

	cb := NewCard().Title(e.i18n.T(MsgDirCardTitle), "turquoise")
	cb.Markdown(e.i18n.Tf(MsgDirCurrent, currentDir))
	if total == 0 {
		cb.Note(e.i18n.T(MsgDirCardEmptyHistory))
	} else {
		cb.Divider()
		for i := start; i < end; i++ {
			dir := history[i]
			marker := "[ ]"
			if dir == currentDir {
				marker = "(current)"
			}
			btnType := "default"
			if dir == currentDir {
				btnType = "primary"
			}
			displayPath := dirCardTruncPath(dir)
			cb.ListItemBtn(
				fmt.Sprintf("%s **%d.** `%s`", marker, i+1, displayPath),
				fmt.Sprintf("#%d", i+1),
				btnType,
				fmt.Sprintf("act:/dir select %d", i+1),
			)
		}
	}

	var actionRow []CardButton
	if e.dirHistory != nil && len(history) >= 2 {
		actionRow = append(actionRow, DefaultBtn(e.i18n.T(MsgDirCardPrev), "act:/dir prev"))
	}
	actionRow = append(actionRow, DefaultBtn(e.i18n.T(MsgDirCardReset), "act:/dir reset"))
	cb.Buttons(actionRow...)

	var navBtns []CardButton
	if totalPages > 1 && page > 1 {
		navBtns = append(navBtns, e.cardPrevButton(fmt.Sprintf("nav:/dir %d", page-1)))
	}
	navBtns = append(navBtns, e.cardBackButton())
	if totalPages > 1 && page < totalPages {
		navBtns = append(navBtns, e.cardNextButton(fmt.Sprintf("nav:/dir %d", page+1)))
	}
	cb.Buttons(navBtns...)

	if totalPages > 1 {
		cb.Note(fmt.Sprintf(e.i18n.T(MsgDirCardPageHint), page, totalPages))
	}

	return cb.Build(), nil
}

// ──────────────────────────────────────────────────────────────
// Navigable sub-cards (for in-place card updates)
// ──────────────────────────────────────────────────────────────

func (e *Engine) currentSessionDisplayName(agent Agent, sessions *SessionManager, agentID string) string {
	if agentID == "" || agentID == e.i18n.T(MsgSessionNotStarted) {
		return e.i18n.T(MsgUntitled)
	}
	displayName := sessions.GetSessionName(agentID)
	if displayName != "" {
		return displayName
	}
	agentSessions, err := agent.ListSessions(e.ctx)
	if err == nil {
		for _, as := range agentSessions {
			if as.ID == agentID {
				displayName = strings.ReplaceAll(as.Summary, "\n", " ")
				displayName = strings.Join(strings.Fields(displayName), " ")
				break
			}
		}
	}
	if displayName == "" {
		if tp, ok := agent.(SessionTitleProvider); ok {
			displayName = tp.GetSessionTitle(agentID)
		}
	}
	if displayName == "" {
		return e.i18n.T(MsgUntitled)
	}
	return displayName
}

func (e *Engine) renderCurrentCard(sessionKey string) *Card {
	agent, sessions := e.sessionContextForKey(sessionKey)
	s := sessions.GetOrCreateActive(sessionKey)
	agentID := s.GetAgentSessionID()
	if agentID == "" {
		agentID = e.i18n.T(MsgSessionNotStarted)
	}
	displayName := e.currentSessionDisplayName(agent, sessions, agentID)
	content := fmt.Sprintf(e.i18n.T(MsgCurrentSession), displayName, agentID, len(s.History))
	return NewCard().
		Title(e.i18n.T(MsgCardTitleCurrentSession), "turquoise").
		Markdown(content).
		Buttons(e.cardBackButton()).
		Build()
}

func (e *Engine) renderHistoryCard(sessionKey string) *Card {
	agent, sessions := e.sessionContextForKey(sessionKey)
	s := sessions.GetOrCreateActive(sessionKey)
	entries := s.GetHistory(10)

	agentSID := s.GetAgentSessionID()
	if len(entries) == 0 && agentSID != "" {
		if hp, ok := agent.(HistoryProvider); ok {
			if agentEntries, err := hp.GetSessionHistory(e.ctx, agentSID, 10); err == nil {
				entries = agentEntries
			}
		}
	}

	if len(entries) == 0 {
		return e.simpleCard(e.i18n.T(MsgCardTitleHistory), "turquoise", e.i18n.T(MsgHistoryEmpty))
	}

	var sb strings.Builder
	maxLen := e.historyEntryMaxLen()
	for _, h := range entries {
		icon := "User"
		if h.Role == "assistant" {
			icon = "Assistant"
		}
		content := truncateHistoryEntry(h.Content, maxLen)
		sb.WriteString(fmt.Sprintf("%s [%s]\n%s\n\n", icon, h.Timestamp.Format("15:04:05"), content))
	}

	return NewCard().
		Title(e.i18n.Tf(MsgCardTitleHistoryLast, len(entries)), "turquoise").
		Markdown(sb.String()).
		Buttons(e.cardBackButton()).
		Build()
}

func (e *Engine) renderProviderCard() *Card {
	switcher, ok := e.agent.(ProviderSwitcher)
	if !ok {
		return e.simpleCard(e.i18n.T(MsgCardTitleProvider), "indigo", e.i18n.T(MsgProviderNotSupported))
	}

	current := switcher.GetActiveProvider()
	providers := switcher.ListProviders()

	if current == nil && len(providers) == 0 {
		cb := NewCard().Title(e.i18n.T(MsgCardTitleProvider), "indigo").
			Markdown(e.i18n.T(MsgProviderNone))
		cb.Buttons(PrimaryBtn(""+e.i18n.T(MsgCardTitleProviderAdd), "nav:/provider/add"), e.cardBackButton())
		return cb.Build()
	}

	var body strings.Builder
	if current != nil {
		body.WriteString(fmt.Sprintf(e.i18n.T(MsgProviderCurrent), current.Name))
		body.WriteString("\n\n")
	}

	cb := NewCard().Title(e.i18n.T(MsgCardTitleProvider), "indigo").Markdown(body.String())
	if len(providers) > 0 {
		var opts []CardSelectOption
		initVal := ""
		if current != nil {
			opts = append(opts, CardSelectOption{
				Text:  e.i18n.T(MsgProviderClearOption),
				Value: "act:/provider clear",
			})
		}
		for _, prov := range providers {
			label := prov.Name
			if prov.BaseURL != "" {
				label += " (" + prov.BaseURL + ")"
			}
			val := "act:/provider " + prov.Name
			opts = append(opts, CardSelectOption{Text: label, Value: val})
			if current != nil && prov.Name == current.Name {
				initVal = val
			}
		}
		cb.Select(e.i18n.T(MsgProviderSelectPlaceholder), opts, initVal)
	}
	cb.Buttons(PrimaryBtn(""+e.i18n.T(MsgCardTitleProviderAdd), "nav:/provider/add"), e.cardBackButton())
	return cb.Build()
}

func (e *Engine) renderProviderAddCard(sessionKey string) *Card {
	if pa := e.getPendingProviderAdd(sessionKey); pa != nil {
		switch pa.phase {
		case "other":
			cb := NewCard().Title(e.i18n.T(MsgCardTitleProviderAdd), "indigo").
				Markdown(e.i18n.T(MsgProviderAddUsage))
			cb.Buttons(DefaultBtn(e.i18n.T(MsgCardBack), "act:/provider/add-cancel"))
			return cb.Build()
		}
	}

	agentType := e.agent.Name()

	cb := NewCard().Title(e.i18n.T(MsgCardTitleProviderAdd), "indigo").
		Markdown(e.i18n.T(MsgProviderAddPickHint))

	// Show linkable global providers not yet in this project
	if e.listGlobalProvidersFunc != nil {
		globals, gErr := e.listGlobalProvidersFunc(agentType)
		if gErr == nil && len(globals) > 0 {
			var existing map[string]bool
			if sw, ok := e.agent.(ProviderSwitcher); ok {
				existing = make(map[string]bool)
				for _, p := range sw.ListProviders() {
					existing[p.Name] = true
				}
			}
			var linkable []ProviderConfig
			for _, g := range globals {
				if existing[g.Name] {
					continue
				}
				linkable = append(linkable, g)
			}
			if len(linkable) > 0 {
				cb.Divider()
				cb.Markdown(e.i18n.T(MsgProviderLinkGlobal))
				for _, g := range linkable {
					label := g.Name
					if g.Model != "" {
						label += " · " + g.Model
					}
					cb.ListItem(label, g.Name, "act:/provider/link "+g.Name)
				}
			}
		}
	}

	cb.Divider()
	cb.Buttons(
		DefaultBtn(""+e.i18n.T(MsgProviderAddOther), "act:/provider/add-other"),
		DefaultBtn(e.i18n.T(MsgCardBack), "nav:/provider"),
	)
	return cb.Build()
}

func (e *Engine) executeProviderLink(sessionKey, name string) {
	name = strings.TrimSpace(name)
	if name == "" || e.listGlobalProvidersFunc == nil {
		return
	}
	agentType := e.agent.Name()
	globals, err := e.listGlobalProvidersFunc(agentType)
	if err != nil {
		slog.Warn("provider link: list global providers", "error", err)
		return
	}
	var target *ProviderConfig
	for i := range globals {
		if globals[i].Name == name {
			target = &globals[i]
			break
		}
	}
	if target == nil {
		slog.Warn("provider link: global provider not found or incompatible agent type", "name", name, "agentType", agentType)
		return
	}

	sw, ok := e.agent.(ProviderSwitcher)
	if !ok {
		return
	}
	for _, p := range sw.ListProviders() {
		if p.Name == name {
			return // already linked
		}
	}
	updated := append(sw.ListProviders(), *target)
	sw.SetProviders(updated)

	// Save the updated provider_refs
	if e.providerRefsSaveFunc != nil {
		refs := make([]string, 0, len(updated))
		for _, p := range updated {
			refs = append(refs, p.Name)
		}
		if err := e.providerRefsSaveFunc(refs); err != nil {
			slog.Error("provider link: save refs", "error", err)
		}
	}
}

func (e *Engine) renderCommandsCard() *Card {
	cmds := e.commands.ListAll()
	if len(cmds) == 0 {
		return e.simpleCard(e.i18n.T(MsgCardTitleCommands), "purple", e.i18n.T(MsgCommandsEmpty))
	}

	var sb strings.Builder
	sb.WriteString(e.i18n.Tf(MsgCommandsTitle, len(cmds)))
	for _, c := range cmds {
		tag := ""
		if c.Source == "agent" {
			tag = e.i18n.T(MsgCommandsTagAgent)
		} else if c.Exec != "" {
			tag = e.i18n.T(MsgCommandsTagShell)
		}
		desc := c.Description
		if desc == "" {
			if c.Exec != "" {
				desc = "$ " + truncateStr(c.Exec, 60)
			} else {
				desc = truncateStr(c.Prompt, 60)
			}
		}
		sb.WriteString(fmt.Sprintf("/%s%s — %s\n", c.Name, tag, desc))
	}

	return NewCard().Title(e.i18n.T(MsgCardTitleCommands), "purple").
		Markdown(sb.String()).
		Note(e.i18n.T(MsgCommandsHint)).
		Buttons(e.cardBackButton()).
		Build()
}

func (e *Engine) renderAliasCard() *Card {
	e.aliasMu.RLock()
	defer e.aliasMu.RUnlock()

	if len(e.aliases) == 0 {
		return e.simpleCard(e.i18n.T(MsgCardTitleAlias), "purple", e.i18n.T(MsgAliasEmpty))
	}

	names := make([]string, 0, len(e.aliases))
	for n := range e.aliases {
		names = append(names, n)
	}
	sort.Strings(names)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(e.i18n.T(MsgAliasListHeader), len(e.aliases)))
	sb.WriteString("\n")
	for _, n := range names {
		sb.WriteString(fmt.Sprintf("`%s` → `%s`\n", n, e.aliases[n]))
	}

	return NewCard().Title(e.i18n.T(MsgCardTitleAlias), "purple").
		Markdown(sb.String()).
		Buttons(e.cardBackButton()).
		Build()
}

// ──────────────────────────────────────────────────────────────
// Custom commands
// ──────────────────────────────────────────────────────────────

func (e *Engine) executeCustomCommand(p Platform, msg *Message, cmd *CustomCommand, args []string) {
	if cmd.Exec != "" && !e.isAdmin(msg.UserID) {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAdminRequired), "/"+cmd.Name))
		return
	}
	// If this is an exec command, run shell command directly
	if cmd.Exec != "" {
		go e.executeShellCommand(p, msg, cmd, args)
		return
	}

	// Otherwise, use prompt template
	prompt := ExpandPrompt(cmd.Prompt, args)

	// Resolve workspace-aware agent in multi-workspace mode. Without this the
	// custom command always runs against the global e.agent (with the
	// project-level work_dir), bypassing any per-channel binding written by
	// /workspace bind.
	agent, sessions, interactiveKey, workspaceDir := e.commandContextWithWorkspace(p, msg)

	session := sessions.GetOrCreateActive(interactiveKey)
	if !session.TryLock() {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPreviousProcessing))
		return
	}

	slog.Info("executing custom command",
		"command", cmd.Name,
		"source", cmd.Source,
		"user", msg.UserName,
		"workspace", workspaceDir,
	)

	msg.Content = prompt
	go e.processInteractiveMessageWith(p, msg, session, agent, sessions, interactiveKey, workspaceDir, msg.SessionKey)
}

// executeShellCommand runs a shell command and sends the output to the user.
func (e *Engine) executeShellCommand(p Platform, msg *Message, cmd *CustomCommand, args []string) {
	slog.Info("executing shell command",
		"command", cmd.Name,
		"exec", cmd.Exec,
		"user", msg.UserName,
	)

	// Expand placeholders in exec command
	execCmd := ExpandPrompt(cmd.Exec, args)

	// Determine working directory
	workDir := cmd.WorkDir
	if workDir == "" {
		// Default to agent's work_dir if available
		if e.agent != nil {
			if agentOpts, ok := e.agent.(interface{ GetWorkDir() string }); ok {
				workDir = agentOpts.GetWorkDir()
			}
		}
	}
	if workDir == "" {
		workDir, _ = os.Getwd()
	}

	_ = e.runShellWithProgress(p, msg.ReplyCtx, execCmd, workDir, 60*time.Second, 4000)
}

func (e *Engine) cmdCommands(p Platform, msg *Message, args []string) {
	if len(args) == 0 {
		if !supportsCards(p) {
			e.cmdCommandsList(p, msg)
			return
		}
		e.replyWithCard(p, msg.ReplyCtx, e.renderCommandsCard())
		return
	}

	sub := matchSubCommand(strings.ToLower(args[0]), []string{
		"list", "add", "addexec", "del", "delete", "rm", "remove",
	})
	switch sub {
	case "list":
		e.cmdCommandsList(p, msg)
	case "add":
		e.cmdCommandsAdd(p, msg, args[1:])
	case "addexec":
		e.cmdCommandsAddExec(p, msg, args[1:])
	case "del", "delete", "rm", "remove":
		e.cmdCommandsDel(p, msg, args[1:])
	default:
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsUsage))
	}
}

func (e *Engine) cmdCommandsList(p Platform, msg *Message) {
	cmds := e.commands.ListAll()
	if len(cmds) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsEmpty))
		return
	}

	var sb strings.Builder
	sb.WriteString(e.i18n.Tf(MsgCommandsTitle, len(cmds)))

	for _, c := range cmds {
		// Tag
		tag := ""
		if c.Source == "agent" {
			tag = " [agent]"
		} else if c.Exec != "" {
			tag = " [shell]"
		}
		sb.WriteString(fmt.Sprintf("/%s%s\n", c.Name, tag))

		// Description or fallback
		desc := c.Description
		if desc == "" {
			if c.Exec != "" {
				desc = "$ " + truncateStr(c.Exec, 60)
			} else {
				desc = truncateStr(c.Prompt, 60)
			}
		}
		sb.WriteString(fmt.Sprintf("  %s\n\n", desc))
	}

	sb.WriteString(e.i18n.T(MsgCommandsHint))
	e.reply(p, msg.ReplyCtx, sb.String())
}

func (e *Engine) cmdCommandsAdd(p Platform, msg *Message, args []string) {
	// /commands add <name> <prompt...>
	if len(args) < 2 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsAddUsage))
		return
	}

	name := strings.ToLower(args[0])
	prompt := strings.Join(args[1:], " ")

	if _, exists := e.commands.Resolve(name); exists {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandsAddExists), name, name))
		return
	}

	e.commands.Add(name, "", prompt, "", "", "config")

	if e.commandSaveAddFunc != nil {
		if err := e.commandSaveAddFunc(name, "", prompt, "", ""); err != nil {
			slog.Error("failed to persist command", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandsAdded), name, truncateStr(prompt, 80)))
}

func (e *Engine) cmdCommandsAddExec(p Platform, msg *Message, args []string) {
	if !e.isAdmin(msg.UserID) {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAdminRequired), "/commands addexec"))
		return
	}
	// /commands addexec <name> <shell command...>
	// /commands addexec --work-dir <dir> <name> <shell command...>
	if len(args) < 2 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsAddExecUsage))
		return
	}

	// Parse --work-dir flag
	workDir := ""
	i := 0
	if args[0] == "--work-dir" && len(args) >= 3 {
		workDir = args[1]
		i = 2
	}

	if i >= len(args) {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsAddExecUsage))
		return
	}

	name := strings.ToLower(args[i])
	execCmd := ""
	if i+1 < len(args) {
		execCmd = strings.Join(args[i+1:], " ")
	}

	if execCmd == "" {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsAddExecUsage))
		return
	}

	if _, exists := e.commands.Resolve(name); exists {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandsAddExists), name, name))
		return
	}

	e.commands.Add(name, "", "", execCmd, workDir, "config")

	if e.commandSaveAddFunc != nil {
		if err := e.commandSaveAddFunc(name, "", "", execCmd, workDir); err != nil {
			slog.Error("failed to persist command", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandsExecAdded), name, truncateStr(execCmd, 80)))
}

func (e *Engine) cmdCommandsDel(p Platform, msg *Message, args []string) {
	if len(args) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgCommandsDelUsage))
		return
	}
	name := strings.ToLower(args[0])

	if !e.commands.Remove(name) {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandsNotFound), name))
		return
	}

	if e.commandSaveDelFunc != nil {
		if err := e.commandSaveDelFunc(name); err != nil {
			slog.Error("failed to persist command removal", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgCommandsDeleted), name))
}

// ── /whoami command ─────────────────────────────────────────

func (e *Engine) cmdWhoami(p Platform, msg *Message) {
	if supportsCards(p) {
		e.replyWithCard(p, msg.ReplyCtx, e.renderWhoamiCard(msg))
		return
	}
	e.reply(p, msg.ReplyCtx, e.formatWhoamiText(msg))
}

func (e *Engine) formatWhoamiText(msg *Message) string {
	var sb strings.Builder
	sb.WriteString(e.i18n.T(MsgWhoamiTitle))
	sb.WriteString("\n")

	if msg.UserID != "" {
		sb.WriteString(fmt.Sprintf("User ID: `%s`\n", msg.UserID))
	} else {
		sb.WriteString("User ID: (unknown)\n")
	}
	if msg.UserName != "" {
		sb.WriteString(fmt.Sprintf("Name: %s\n", msg.UserName))
	}
	if msg.Platform != "" {
		sb.WriteString(fmt.Sprintf("Platform: %s\n", msg.Platform))
	}

	chatID := effectiveChannelID(msg)
	if chatID != "" {
		sb.WriteString(fmt.Sprintf("Chat ID: `%s`\n", chatID))
	}
	sb.WriteString(fmt.Sprintf("Session Key: `%s`\n", msg.SessionKey))

	sb.WriteString("\n")
	sb.WriteString(e.i18n.T(MsgWhoamiUsage))
	return sb.String()
}

func (e *Engine) renderWhoamiCard(msg *Message) *Card {
	userID := msg.UserID
	if userID == "" {
		userID = "(unknown)"
	}

	var body strings.Builder
	body.WriteString(fmt.Sprintf("**User ID:**  `%s`\n", userID))
	if msg.UserName != "" {
		body.WriteString(fmt.Sprintf("**%s:**  %s\n", e.i18n.T(MsgWhoamiName), msg.UserName))
	}
	if msg.Platform != "" {
		body.WriteString(fmt.Sprintf("**%s:**  %s\n", e.i18n.T(MsgWhoamiPlatform), msg.Platform))
	}
	chatID := effectiveChannelID(msg)
	if chatID != "" {
		body.WriteString(fmt.Sprintf("**Chat ID:**  `%s`\n", chatID))
	}
	body.WriteString(fmt.Sprintf("**Session Key:**  `%s`\n", msg.SessionKey))

	return NewCard().
		Title(e.i18n.T(MsgWhoamiCardTitle), "blue").
		Markdown(body.String()).
		Divider().
		Note(e.i18n.T(MsgWhoamiUsage)).
		Buttons(e.cardBackButton()).
		Build()
}

func (e *Engine) cmdRestart(p Platform, msg *Message) {
	e.reply(p, msg.ReplyCtx, e.i18n.T(MsgRestarting))
	select {
	case RestartCh <- RestartRequest{
		SessionKey: msg.SessionKey,
		Platform:   p.Name(),
	}:
	default:
	}
}

func (e *Engine) cmdAlias(p Platform, msg *Message, args []string) {
	if len(args) == 0 {
		if !supportsCards(p) {
			e.cmdAliasList(p, msg)
			return
		}
		e.replyWithCard(p, msg.ReplyCtx, e.renderAliasCard())
		return
	}

	sub := matchSubCommand(strings.ToLower(args[0]), []string{"list", "add", "del", "delete", "remove"})
	switch sub {
	case "list":
		e.cmdAliasList(p, msg)
	case "add":
		e.cmdAliasAdd(p, msg, args[1:])
	case "del", "delete", "remove":
		e.cmdAliasDel(p, msg, args[1:])
	default:
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgAliasUsage))
	}
}

func (e *Engine) cmdAliasList(p Platform, msg *Message) {
	e.aliasMu.RLock()
	defer e.aliasMu.RUnlock()

	if len(e.aliases) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgAliasEmpty))
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(e.i18n.T(MsgAliasListHeader), len(e.aliases)))
	sb.WriteString("\n")

	names := make([]string, 0, len(e.aliases))
	for n := range e.aliases {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, n := range names {
		sb.WriteString(fmt.Sprintf("  %s → %s\n", n, e.aliases[n]))
	}
	e.reply(p, msg.ReplyCtx, strings.TrimRight(sb.String(), "\n"))
}

func (e *Engine) cmdAliasAdd(p Platform, msg *Message, args []string) {
	if len(args) < 2 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgAliasUsage))
		return
	}
	name := args[0]
	command := strings.Join(args[1:], " ")
	if !strings.HasPrefix(command, "/") {
		command = "/" + command
	}

	e.aliasMu.Lock()
	e.aliases[name] = command
	e.aliasMu.Unlock()

	if e.aliasSaveAddFunc != nil {
		if err := e.aliasSaveAddFunc(name, command); err != nil {
			slog.Error("alias: save failed", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAliasAdded), name, command))
}

func (e *Engine) cmdAliasDel(p Platform, msg *Message, args []string) {
	if len(args) < 1 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgAliasUsage))
		return
	}
	name := args[0]

	e.aliasMu.Lock()
	_, exists := e.aliases[name]
	if exists {
		delete(e.aliases, name)
	}
	e.aliasMu.Unlock()

	if !exists {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAliasNotFound), name))
		return
	}

	if e.aliasSaveDelFunc != nil {
		if err := e.aliasSaveDelFunc(name); err != nil {
			slog.Error("alias: save failed", "error", err)
		}
	}

	e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgAliasDeleted), name))
}

func (e *Engine) cmdDelete(p Platform, msg *Message, args []string) {
	agent, sessions, _ := e.commandContext(p, msg)
	deleter, ok := agent.(SessionDeleter)
	if !ok {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDeleteNotSupported))
		return
	}

	if len(args) == 0 {
		if supportsCards(p) {
			_ = e.getOrCreateDeleteModeState(msg.SessionKey, p, msg.ReplyCtx)
			e.replyWithCard(p, msg.ReplyCtx, e.renderDeleteModeCard(msg.SessionKey))
			return
		}
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDeleteUsage))
		return
	}
	if len(args) > 1 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDeleteUsage))
		return
	}

	agentSessions, err := agent.ListSessions(e.ctx)
	if err != nil {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgError, err))
		return
	}
	agentSessions = e.applySessionFilter(agentSessions, sessions)

	prefix := strings.TrimSpace(args[0])
	if isExplicitDeleteBatchArg(prefix) {
		indices, err := parseDeleteBatchIndices(prefix, len(agentSessions))
		if err != nil {
			e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDeleteUsage))
			return
		}
		e.cmdDeleteBatch(p, msg, deleter, agentSessions, indices)
		return
	}
	var matched *AgentSessionInfo

	if idx, err := strconv.Atoi(prefix); err == nil && idx >= 1 && idx <= len(agentSessions) {
		matched = &agentSessions[idx-1]
	} else {
		for i := range agentSessions {
			if strings.HasPrefix(agentSessions[i].ID, prefix) {
				matched = &agentSessions[i]
				break
			}
		}
	}

	if matched == nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgSwitchNoMatch), prefix))
		return
	}

	e.deleteSingleSession(p, msg, deleter, matched)
}

func isExplicitDeleteBatchArg(arg string) bool {
	if strings.Contains(arg, ",") {
		return true
	}
	if !strings.Contains(arg, "-") {
		return false
	}
	for _, r := range arg {
		if (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

func parseDeleteBatchIndices(spec string, max int) ([]int, error) {
	parts := strings.Split(spec, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty batch spec")
	}
	seen := make(map[int]struct{}, len(parts))
	indices := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty batch item")
		}

		if strings.Contains(part, "-") {
			bounds := strings.Split(part, "-")
			if len(bounds) != 2 || bounds[0] == "" || bounds[1] == "" {
				return nil, fmt.Errorf("invalid range %q", part)
			}
			start, err := strconv.Atoi(bounds[0])
			if err != nil {
				return nil, err
			}
			end, err := strconv.Atoi(bounds[1])
			if err != nil {
				return nil, err
			}
			if start < 1 || end < 1 || start > end || end > max {
				return nil, fmt.Errorf("range %q out of bounds", part)
			}
			for idx := start; idx <= end; idx++ {
				if _, ok := seen[idx]; ok {
					continue
				}
				seen[idx] = struct{}{}
				indices = append(indices, idx)
			}
			continue
		}

		idx, err := strconv.Atoi(part)
		if err != nil {
			return nil, err
		}
		if idx < 1 || idx > max {
			return nil, fmt.Errorf("index %d out of bounds", idx)
		}
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		indices = append(indices, idx)
	}

	return indices, nil
}

func (e *Engine) cmdDeleteBatch(p Platform, msg *Message, deleter SessionDeleter, sessions []AgentSessionInfo, indices []int) {
	lines := make([]string, 0, len(indices))
	for _, idx := range indices {
		matched := &sessions[idx-1]
		if line := e.deleteSingleSessionReply(msg, deleter, matched); line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) == 0 {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgDeleteUsage))
		return
	}
	e.reply(p, msg.ReplyCtx, strings.Join(lines, "\n"))
}

func (e *Engine) deleteSingleSession(p Platform, msg *Message, deleter SessionDeleter, matched *AgentSessionInfo) {
	e.reply(p, msg.ReplyCtx, e.deleteSingleSessionReply(msg, deleter, matched))
}

func (e *Engine) deleteSingleSessionReply(msg *Message, deleter SessionDeleter, matched *AgentSessionInfo) string {
	if matched == nil {
		return ""
	}

	// Prevent deleting the currently active session
	_, sessions := e.sessionContextForKey(msg.SessionKey)
	activeSession := sessions.GetOrCreateActive(msg.SessionKey)
	if activeSession.GetAgentSessionID() == matched.ID {
		return e.i18n.T(MsgDeleteActiveDenied)
	}

	displayName := e.deleteSessionDisplayName(sessions, matched)

	if err := deleter.DeleteSession(e.ctx, matched.ID); err != nil {
		return e.i18n.Tf(MsgFailedToDeleteSession, displayName, err)
	}

	// Keep local session snapshot aligned with agent-side deletion.
	sessions.DeleteByAgentSessionID(matched.ID)
	sessions.SetSessionName(matched.ID, "")
	return fmt.Sprintf(e.i18n.T(MsgDeleteSuccess), displayName)
}

func (e *Engine) deleteSessionDisplayName(sessions *SessionManager, matched *AgentSessionInfo) string {
	displayName := sessions.GetSessionName(matched.ID)
	if displayName == "" {
		displayName = matched.Summary
	}
	if displayName == "" {
		shortID := matched.ID
		if len(shortID) > 12 {
			shortID = shortID[:12]
		}
		displayName = shortID
	}
	return displayName
}

// toolCodeLang picks the code block language hint for tool display.
func toolCodeLang(toolName, input string) string {
	switch toolName {
	case "shell", "run_shell_command", "Bash":
		return "bash"
	case "write_file", "WriteFile", "replace", "ReplaceInFile":
		if strings.Contains(input, "\n- ") || strings.Contains(input, "\n+ ") {
			return "diff"
		}
	}
	// Fallback: detect diff-like content
	if strings.Contains(input, "\n- ") && strings.Contains(input, "\n+ ") {
		return "diff"
	}
	return ""
}

func (e *Engine) formatToolResultEventFallback(toolName, result, status string, exitCode *int, success *bool) string {
	statusLabel := e.i18n.T(MsgToolResultFmtStatus)
	exitLabel := e.i18n.T(MsgToolResultFmtExit)
	noOutput := e.i18n.T(MsgToolResultFmtNoOutput)
	dot := "Pending"
	if success != nil {
		if *success {
			dot = "Succeeded"
		} else {
			dot = "Failed"
		}
	}
	var lines []string
	first := "Tool result"
	if strings.TrimSpace(toolName) != "" {
		first += " " + strings.TrimSpace(toolName)
	}
	lines = append(lines, first)
	if strings.TrimSpace(status) != "" || success != nil {
		s := strings.TrimSpace(status)
		if s == "" {
			if success != nil && *success {
				s = e.i18n.T(MsgToolResultFmtOk)
			} else if success != nil && !*success {
				s = e.i18n.T(MsgToolResultFmtFailed)
			}
		}
		lines = append(lines, fmt.Sprintf("%s %s: %s", dot, statusLabel, s))
	}
	if exitCode != nil {
		lines = append(lines, fmt.Sprintf("%s: %d", exitLabel, *exitCode))
	}
	if strings.TrimSpace(result) != "" {
		lines = append(lines, "```text\n"+strings.TrimSpace(result)+"\n```")
	} else {
		lines = append(lines, "_"+noOutput+"_")
	}
	return strings.Join(lines, "\n")
}

// truncateIf truncates s to maxLen runes. 0 means no truncation.
func truncateIf(s string, maxLen int) string {
	if maxLen <= 0 {
		return s
	}
	if utf8.RuneCountInString(s) <= maxLen {
		return s
	}
	return string([]rune(s)[:maxLen]) + "..."
}

// sendTTSReply synthesizes fullResponse text and sends audio to the platform.
// Called asynchronously after EventResult; text reply is always sent first.
func (e *Engine) sendTTSReply(p Platform, replyCtx any, text string) {
	slog.Debug("tts: sendTTSReply called", "platform", p.Name(), "text_len", len(text))
	if err := e.synthesizeAndSendTTS(p, replyCtx, text); err != nil {
		slog.Error("tts: voice reply failed", "platform", p.Name(), "error", err)
	}
}

func (e *Engine) synthesizeAndSendTTS(p Platform, replyCtx any, text string) error {
	if e.tts == nil || !e.tts.Enabled {
		return fmt.Errorf("tts is not configured")
	}
	if e.tts.TTS == nil {
		return fmt.Errorf("tts provider is not configured")
	}
	if e.tts.MaxTextLen > 0 && utf8.RuneCountInString(text) > e.tts.MaxTextLen {
		return fmt.Errorf("text exceeds max_text_len (%d > %d)", utf8.RuneCountInString(text), e.tts.MaxTextLen)
	}
	as, ok := p.(AudioSender)
	if !ok {
		return fmt.Errorf("platform %s does not support audio sending", p.Name())
	}
	slog.Info("tts: starting synthesis", "voice", e.tts.Voice, "speed", e.tts.Speed, "text_len", len(text))
	opts := TTSSynthesisOpts{
		Voice:        e.tts.Voice,
		LanguageType: e.tts.LanguageType,
		Speed:        e.tts.Speed,
	}
	audioData, format, err := e.tts.TTS.Synthesize(e.ctx, StripMarkdown(text), opts)
	if err != nil {
		return fmt.Errorf("synthesize: %w", err)
	}
	slog.Info("tts: synthesis successful", "format", format, "audio_size", len(audioData))
	if err := as.SendAudio(e.ctx, replyCtx, audioData, format); err != nil {
		return fmt.Errorf("send audio: %w", err)
	}
	slog.Info("tts: audio sent successfully", "platform", p.Name())
	return nil
}

// buildSenderPrompt prepends a sender identity header to content when
// injectSender is enabled and userID is non-empty. When userName is available
// it is included as sender_name so the agent can identify who sent the message
// by display name (useful in shared channel sessions with multiple users).
func (e *Engine) buildSenderPrompt(content, userID, userName, platform, sessionKey, channelKey string) string {
	if !e.injectSender || userID == "" {
		return content
	}
	chatID := channelKey
	if chatID == "" {
		chatID = extractChannelID(sessionKey)
	}
	if userName != "" {
		safeName := strings.NewReplacer(`"`, `'`, "\n", " ", "\r", "").Replace(userName)
		return fmt.Sprintf("[agent-bridge sender_id=%s sender_name=\"%s\" platform=%s chat_id=%s]\n%s", userID, safeName, platform, chatID, content)
	}
	return fmt.Sprintf("[agent-bridge sender_id=%s platform=%s chat_id=%s]\n%s", userID, platform, chatID, content)
}

func extractChannelID(sessionKey string) string {
	// Format: "platform:channelID:userID" or "platform:channelID"
	// Some platforms encode a short type tag as an extra segment, e.g.
	// "platform:t:channelID:userID" or "platform:t:channelID" (shared session)
	// where t is a single-char tag. When parts[1] is a single char and at
	// least one more segment follows, treat parts[2] as the real channel ID.
	parts := strings.SplitN(sessionKey, ":", 4)
	if len(parts) >= 3 && len(parts[1]) == 1 {
		return parts[2]
	}
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

func extractUserID(sessionKey string) string {
	// Format: "platform:channelID:userID" or "platform:type:channelID:userID"
	// When parts[1] is a single-char type tag, the user ID is in parts[3]
	// (4-segment form). The 3-segment form with a type tag (shared session,
	// e.g. "dingtalk:g:cid") has no per-user ID, so return "".
	parts := strings.SplitN(sessionKey, ":", 5)
	if len(parts) >= 3 && len(parts[1]) == 1 {
		if len(parts) >= 4 {
			return parts[3]
		}
		return ""
	}
	if len(parts) >= 3 {
		return parts[2]
	}
	return ""
}

func extractPlatformName(sessionKey string) string {
	if i := strings.IndexByte(sessionKey, ':'); i >= 0 {
		return sessionKey[:i]
	}
	return sessionKey
}

// effectiveChannelID returns the channel identifier from a Message.
// It prefers the platform-provided ChannelKey (e.g. "chatID:threadID" for forum topics)
// and falls back to parsing the session key.
func effectiveChannelID(msg *Message) string {
	if msg.ChannelKey != "" {
		return msg.ChannelKey
	}
	return extractChannelID(msg.SessionKey)
}

// commandContext resolves the agent, session manager, and interactive key for a command.
func (e *Engine) commandContext(p Platform, msg *Message) (Agent, *SessionManager, string) {
	return e.agent, e.sessions, msg.SessionKey
}

// commandContextWithWorkspace is like commandContext but additionally returns
// the workspace path forwarded to processInteractiveMessageWith ("" here).
func (e *Engine) commandContextWithWorkspace(p Platform, msg *Message) (Agent, *SessionManager, string, string) {
	return e.agent, e.sessions, msg.SessionKey, ""
}

// sessionContextForKey resolves the agent and session manager for a sessionKey.
// It uses existing workspace bindings and falls back to global context if unresolved.
func (e *Engine) sessionContextForKey(sessionKey string) (Agent, *SessionManager) {
	if workspace := e.sendWorkDirForSession(sessionKey); workspace != "" {
		if wsAgent, wsSessions, err := e.getOrCreateWorkspaceAgent(workspace); err == nil {
			return wsAgent, wsSessions
		}
	}
	return e.agent, e.sessions
}

func (e *Engine) bindSendWorkDir(sessionKey, workDir string) {
	if sessionKey == "" || workDir == "" {
		return
	}
	keys := []string{sessionKey}
	if participantKey := directParticipantKeyForSession(sessionKey); participantKey != "" {
		keys = append(keys, participantKey)
	}
	e.sendWorkDirMu.Lock()
	defer e.sendWorkDirMu.Unlock()
	if e.sendWorkDirs == nil {
		e.sendWorkDirs = make(map[string]string)
	}
	for _, key := range keys {
		if key != "" {
			e.sendWorkDirs[key] = workDir
		}
	}
}

func (e *Engine) sendWorkDirForSession(sessionKey string) string {
	if sessionKey == "" {
		return ""
	}
	participantKey := directParticipantKeyForSession(sessionKey)
	e.sendWorkDirMu.RLock()
	defer e.sendWorkDirMu.RUnlock()
	if workDir := e.sendWorkDirs[sessionKey]; workDir != "" {
		return workDir
	}
	if participantKey != "" {
		return e.sendWorkDirs[participantKey]
	}
	return ""
}

func directParticipantKeyForSession(sessionKey string) string {
	platformName := extractPlatformName(sessionKey)
	if platformName == "" {
		return ""
	}
	parts := strings.SplitN(sessionKey, ":", 5)
	if len(parts) < 4 || parts[1] != "d" || strings.TrimSpace(parts[3]) == "" {
		return ""
	}
	return platformName + ":direct-user:" + strings.TrimSpace(parts[3])
}

// interactiveKeyForSessionKey returns the interactive state key for a sessionKey.
// In multi-workspace mode, it prefixes with the bound workspace path when available.
func (e *Engine) interactiveKeyForSessionKey(sessionKey string) string {
	if workspace := e.sendWorkDirForSession(sessionKey); workspace != "" {
		return workspace + ":" + sessionKey
	}
	return sessionKey
}

// findInteractiveKeyForSession scans the live interactiveStates map for an
// interactive key that matches sessionKey, either as the key itself or as
// the trailing "<workspace>:<sessionKey>" segment. Returns "" when no live
// state references this sessionKey. Acquires e.interactiveMu internally;
// callers that already hold the lock must use findInteractiveKeyInStatesLocked.
//
// The scan is bounded by the number of in-flight interactive sessions
// (typically <10), so the linear cost is negligible compared to even one
// binding lookup. Avoiding a parallel sessionKey→interactiveKey map keeps
// the engine's state surface single-source-of-truth.
func (e *Engine) findInteractiveKeyForSession(sessionKey string) string {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	return findInteractiveKeyInStatesLocked(e.interactiveStates, sessionKey)
}

// findInteractiveKeyInStatesLocked is the lock-free body of the scan; it
// assumes the caller holds e.interactiveMu.
//
// Precedence is exact match first, then suffix scan. The exact path matters
// because Go map iteration order is randomized: if both `sessionKey` and
// `<workspace>:<sessionKey>` are live (e.g. a raw placeholder created before
// multi-workspace was enabled coexisting with a workspace-prefixed turn),
// a pure scan could non-deterministically return either, sending /stop or
// pending-permission handling at the wrong state.
func findInteractiveKeyInStatesLocked(states map[string]*interactiveState, sessionKey string) string {
	if sessionKey == "" {
		return ""
	}
	if _, ok := states[sessionKey]; ok {
		return sessionKey
	}
	suffix := ":" + sessionKey
	for k := range states {
		if strings.HasSuffix(k, suffix) {
			return k
		}
	}
	return ""
}

// ── Context usage indicator ──────────────────────────────────

const modelContextWindow = 200_000 // generic fallback window for heuristic context estimates

func contextIndicatorText(inputTokens int) string {
	if inputTokens <= 0 {
		return ""
	}
	pct := inputTokens * 100 / modelContextWindow
	if pct > 100 {
		pct = 100
	}
	return fmt.Sprintf("[ctx: ~%d%%]", pct)
}

// ctxSelfReportRe matches agent self-reported context lines like "[ctx: ~42%]".
// Used to strip such markers from delivered text — the ctx indicator is now
// rendered exclusively in the reply footer.
var ctxSelfReportRe = regexp.MustCompile(`(?m)\n?\[ctx: ~\d+%\]`)

// agentFooterLineRe matches standalone agent-emitted status footer lines such as:
// *claude-opus-4-8[1m] · out 788 · in 442 cw 0 cr 395.1k · ctx 40%*
// The line must contain all three metrics so ordinary prose is left untouched.
var agentFooterLineRe = regexp.MustCompile(`^[ \t]*\*?[A-Za-z0-9][^\n]*\s+·\s+out\s+[0-9][0-9A-Za-z.]*\b[^\n]*\bin\s+[0-9][0-9A-Za-z.]*\b[^\n]*\bctx\s+[0-9]+(?:\.[0-9]+)?%[^\n]*\*?[ \t]*$`)

func stripAgentFooterLines(text string) string {
	if text == "" {
		return text
	}
	lines := strings.Split(text, "\n")
	kept := lines[:0]
	removed := false
	for _, line := range lines {
		if agentFooterLineRe.MatchString(line) {
			removed = true
			continue
		}
		kept = append(kept, line)
	}
	if !removed {
		return text
	}
	return strings.TrimRight(strings.Join(kept, "\n"), "\n ")
}

// silentReplyRe matches a bare NO_REPLY marker (case-insensitive, optional surrounding whitespace).
// When the agent emits exactly this as its full response, the platform send is suppressed
// so the agent stays silent in group chats where a reply would be noise.
var silentReplyRe = regexp.MustCompile(`(?i)^\s*NO_REPLY\s*$`)

// silentReplyTrailingRe matches a trailing NO_REPLY marker preceded by whitespace or
// markdown emphasis (`*`). Lets agents that narrate their reasoning before the marker
// still suppress the marker from the delivered text (mirroring OpenClaw's stripSilentToken).
var silentReplyTrailingRe = regexp.MustCompile(`(?i)(?:^|\s+|\*+)NO_REPLY\s*$`)

// isSilentReply reports whether text is exactly a NO_REPLY marker.
func isSilentReply(text string) bool {
	return silentReplyRe.MatchString(text)
}

// stripTrailingSilent removes a trailing NO_REPLY marker and returns the stripped text
// along with whether a strip occurred. Caller must first check isSilentReply for the
// bare-marker case; this helper assumes mixed content.
func stripTrailingSilent(text string) (string, bool) {
	stripped := silentReplyTrailingRe.ReplaceAllString(text, "")
	if stripped == text {
		return text, false
	}
	return strings.TrimRight(stripped, " \t\r\n"), true
}

// couldBeSilentPrefix reports whether the trimmed text is still a case-insensitive
// prefix of "NO_REPLY". Used during streaming to hold the preview until we know
// whether the response will resolve to a pure NO_REPLY marker.
func couldBeSilentPrefix(text string) bool {
	t := strings.TrimSpace(text)
	if t == "" {
		return true
	}
	return strings.HasPrefix("NO_REPLY", strings.ToUpper(t))
}

func isEllipsisOnly(text string) bool {
	t := strings.TrimSpace(text)
	return t == "..." || t == "…"
}

// parseSelfReportedCtx extracts the percentage from a self-reported "[ctx: ~XX%]" line.
func parseSelfReportedCtx(s string) int {
	m := ctxSelfReportRe.FindString(s)
	if m == "" {
		return 0
	}
	start := strings.Index(m, "~") + 1
	end := strings.Index(m, "%")
	if start <= 0 || end <= start {
		return 0
	}
	v, _ := strconv.Atoi(m[start:end])
	return v
}

// restoreActiveProviderFromSession syncs the agent's active provider to the
// one persisted in the session, but only when the choice survived a
// agent-bridge process restart (i.e. the in-memory active provider is not
// already the desired one). It is a no-op when:
//   - the agent does not implement ProviderSwitcher,
//   - the session never recorded a provider choice (`/provider switch` was
//     never called for this conversation), or
//   - the agent already has the correct provider active (steady-state path
//     within a single process lifetime).
//
// The empty-session-value case is intentionally a no-op rather than
// `SetActiveProvider("")`: clearing the agent here would clobber a
// project-level default for sessions that predate this field.
func restoreActiveProviderFromSession(agent Agent, session *Session) {
	if agent == nil || session == nil {
		return
	}
	want := session.GetActiveProvider()
	if want == "" {
		return
	}
	ps, ok := agent.(ProviderSwitcher)
	if !ok {
		return
	}
	if cur := ps.GetActiveProvider(); cur != nil && cur.Name == want {
		return
	}
	if !ps.SetActiveProvider(want) {
		slog.Warn("session.active_provider no longer registered; leaving agent default",
			"session_id", session.ID, "wanted_provider", want)
		return
	}
	slog.Info("restored active provider from session",
		"session_id", session.ID, "provider", want)
}
