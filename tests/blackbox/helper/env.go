// Package helper provides test environment setup for blackbox tests.
//
// BlackboxEnv wraps a real agent-bridge Engine, a real Agent (Claude Code,
// Codex, etc.), and a MockPlatform. Tests inject messages and assert on what
// the platform receives — exactly what a real user would see.
package helper

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-bridge/core"
	bbplatform "agent-bridge/tests/blackbox/platform"
)

const (
	// DefaultReplyTimeout is generous because real agents (Claude Code, etc.)
	// take 10-60 seconds to respond. Tests using the default timeout accept
	// that slowness is the price of real testing.
	DefaultReplyTimeout = 120 * time.Second

	// DefaultUser and DefaultChat are used by Send / helpers that don't need
	// multi-user scenarios.
	DefaultUser = "user1"
	DefaultChat = "chat1"
)

// Env is a fully wired blackbox test environment.
// Always create via NewEnv — never construct directly.
type Env struct {
	T        *testing.T
	Platform *bbplatform.MockPlatform
	Engine   *core.Engine
	WorkDir  string
	DataDir  string

	// agentType is stored for diagnostic messages only.
	agentType string
}

// NewEnv creates a blackbox test environment with a real agent.
//
// agentType selects the agent: "claudecode", "codex"
// The test is skipped (not failed) when:
//   - the agent CLI binary is not in PATH
//   - the required API credentials are missing from the environment
//
// A successful NewEnv call registers a t.Cleanup that stops the engine.
func NewEnv(t *testing.T, agentType string) *Env {
	return NewEnvWithSetup(t, agentType, nil)
}

// NewEnvWithSetup is like NewEnv but accepts an optional setup function that
// is called after the engine is created but BEFORE engine.Start(). Use this
// to configure engine options (display, banned words, disabled commands, etc.)
// that must be set before the engine begins processing messages.
//
// Example:
//
//	env := helper.NewEnvWithSetup(t, "claudecode", func(e *core.Engine) {
//	    e.SetShowContextIndicator(false)
//	    e.SetBannedWords([]string{"secret"})
//	})
func NewEnvWithSetup(t *testing.T, agentType string, setup func(*core.Engine)) *Env {
	t.Helper()

	requireAgent(t, agentType)

	workDir := t.TempDir()
	dataDir := t.TempDir()

	opts := map[string]any{
		"work_dir": workDir,
	}

	agent, err := core.CreateAgent(agentType, opts)
	if err != nil {
		t.Skipf("blackbox skip: cannot create %s agent: %v", agentType, err)
	}

	mp := bbplatform.New(agentType + "-mock")

	sessPath := filepath.Join(dataDir, "sessions.json")
	engine := core.NewEngine("blackbox-"+t.Name(), agent, []core.Platform{mp}, sessPath)

	if setup != nil {
		setup(engine)
	}

	if err := engine.Start(); err != nil {
		t.Fatalf("blackbox: engine.Start failed: %v", err)
	}

	t.Cleanup(func() {
		engine.Stop()
		agent.Stop()
	})

	return &Env{
		T:         t,
		Platform:  mp,
		Engine:    engine,
		WorkDir:   workDir,
		DataDir:   dataDir,
		agentType: agentType,
	}
}

// ── Message helpers ───────────────────────────────────────────────────────────

// Send injects a message from DefaultUser in DefaultChat and waits up to
// DefaultReplyTimeout for the first reply. Fails the test on timeout.
func (e *Env) Send(content string) *bbplatform.SentMessage {
	e.T.Helper()
	return e.SendAs(DefaultUser, DefaultChat, content, DefaultReplyTimeout)
}

// SendWithTimeout is like Send but uses a custom timeout.
func (e *Env) SendWithTimeout(content string, timeout time.Duration) *bbplatform.SentMessage {
	e.T.Helper()
	return e.SendAs(DefaultUser, DefaultChat, content, timeout)
}

// SendAs injects a message from a specific user/chat pair and waits for a
// reply. Use this for multi-user isolation tests.
func (e *Env) SendAs(userID, chatID, content string, timeout time.Duration) *bbplatform.SentMessage {
	e.T.Helper()
	before := e.Platform.MessageCount()
	e.Platform.InjectMessage(userID, chatID, content)
	reply := e.Platform.WaitForReply(before, timeout)
	if reply == nil {
		e.T.Fatalf(
			"blackbox timeout (%v): no reply to %q from %s/%s\nagent=%s\nall messages so far:\n%s",
			timeout, content, userID, chatID, e.agentType,
			e.Platform.AllText(),
		)
	}
	return reply
}

// SendComplete injects a message and waits for the full agent turn to finish
// (i.e., the message stream is stable for idlePeriod). Returns all messages
// sent during the turn. Use this instead of Send when you need the turn to be
// fully done before sending the next message (multi-turn context tests).
func (e *Env) SendComplete(content string) []*bbplatform.SentMessage {
	e.T.Helper()
	return e.SendCompleteAs(DefaultUser, DefaultChat, content, 5*time.Second, DefaultReplyTimeout)
}

// SendCompleteAs is like SendComplete but for a specific user/chat.
func (e *Env) SendCompleteAs(userID, chatID, content string, idlePeriod, timeout time.Duration) []*bbplatform.SentMessage {
	e.T.Helper()
	before := e.Platform.MessageCount()
	e.Platform.InjectMessage(userID, chatID, content)
	msgs := e.Platform.WaitForTurnComplete(before, idlePeriod, timeout)
	if msgs == nil {
		e.T.Fatalf(
			"blackbox timeout (%v): no complete turn for %q from %s/%s\nagent=%s\nall messages so far:\n%s",
			timeout, content, userID, chatID, e.agentType,
			e.Platform.AllText(),
		)
	}
	return msgs
}

// LastText returns the text of the last message in a turn's message slice.
// Convenient for context retention assertions.
func LastText(msgs []*bbplatform.SentMessage) string {
	if len(msgs) == 0 {
		return ""
	}
	return msgs[len(msgs)-1].Text()
}

// AnyText returns all texts joined, for searching across multi-message turns.
func AnyText(msgs []*bbplatform.SentMessage) string {
	parts := make([]string, len(msgs))
	for i, m := range msgs {
		parts[i] = m.Text()
	}
	return strings.Join(parts, " ")
}

// SendNoWait injects a message but does not wait for a reply.
// Use for /stop scenarios where you send a command mid-processing.
func (e *Env) SendNoWait(content string) {
	e.Platform.InjectMessage(DefaultUser, DefaultChat, content)
}

// SendNoWaitAs injects from a specific user/chat without waiting.
func (e *Env) SendNoWaitAs(userID, chatID, content string) {
	e.Platform.InjectMessage(userID, chatID, content)
}

// ExpectReply waits for the next reply (after the current message count).
// Fails the test if nothing arrives before timeout.
func (e *Env) ExpectReply(timeout time.Duration) *bbplatform.SentMessage {
	e.T.Helper()
	before := e.Platform.MessageCount()
	reply := e.Platform.WaitForReply(before, timeout)
	if reply == nil {
		e.T.Fatalf(
			"blackbox timeout (%v): expected a reply\nagent=%s\nall messages so far:\n%s",
			timeout, e.agentType, e.Platform.AllText(),
		)
	}
	return reply
}

// WaitForMessageContaining waits for any message (from startIdx) containing
// substr (case-insensitive). Fails on timeout.
func (e *Env) WaitForMessageContaining(startIdx int, substr string, timeout time.Duration) *bbplatform.SentMessage {
	e.T.Helper()
	msg := e.Platform.WaitForMessageContaining(startIdx, substr, timeout)
	if msg == nil {
		e.T.Fatalf(
			"blackbox timeout (%v): no message containing %q\nagent=%s\nall messages:\n%s",
			timeout, substr, e.agentType, e.Platform.AllText(),
		)
	}
	return msg
}

// SessionKey returns the session key for the default user/chat, matching the
// format agent-bridge uses internally: "<platform>:<chat>:<user>".
func (e *Env) SessionKey() string {
	return fmt.Sprintf("%s:%s:%s", e.Platform.Name(), DefaultChat, DefaultUser)
}

// SessionKeyFor returns the session key for a specific user/chat.
func (e *Env) SessionKeyFor(userID, chatID string) string {
	return fmt.Sprintf("%s:%s:%s", e.Platform.Name(), chatID, userID)
}

// ── Skip guards ───────────────────────────────────────────────────────────────

// requireAgent skips (never fails) if the agent binary or API credentials are
// unavailable.  The distinction between skip and fail is intentional: missing
// credentials mean the test simply cannot run in this environment; a logical
// assertion failure inside the test is a real failure.
func requireAgent(t *testing.T, agentType string) {
	t.Helper()

	bin := agentBinName(agentType)
	if bin == "" {
		t.Skipf("blackbox skip: unknown agent type %q", agentType)
	}
	if _, err := exec.LookPath(bin); err != nil {
		t.Skipf("blackbox skip: %s binary %q not in PATH", agentType, bin)
	}
	switch agentType {
	case "claudecode":
		if os.Getenv("ANTHROPIC_API_KEY") == "" {
			t.Skipf("blackbox skip: ANTHROPIC_API_KEY not set")
		}
	case "codex":
		if os.Getenv("OPENAI_API_KEY") == "" {
			t.Skipf("blackbox skip: OPENAI_API_KEY not set")
		}
	}
}

func agentBinName(agentType string) string {
	switch agentType {
	case "claudecode":
		return "claude"
	case "codex":
		return "codex"
	default:
		return agentType
	}
}
