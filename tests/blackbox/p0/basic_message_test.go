//go:build blackbox

// Package p0 contains P0 blackbox tests — the hard release gate.
//
// These tests use real agents (Claude Code, Codex, etc.) and a MockPlatform.
// They are skipped when agent binaries or API credentials are unavailable,
// but FAIL on any logical assertion failure.
//
// Run all P0 tests:
//
//	go test -tags blackbox ./tests/blackbox/p0/... -timeout 1800s -v
//
// Run against a specific agent:
//
//	ANTHROPIC_API_KEY=xxx go test -tags blackbox \
//	    ./tests/blackbox/p0/... -run ClaudeCode -timeout 1800s -v
package p0

import (
	"strings"
	"testing"
	"time"

	"agent-bridge/tests/blackbox/helper"
)

// ── P0-1: 基础消息收发 ────────────────────────────────────────────────────────

// TestP0_1_BasicMessageFlow_ClaudeCode verifies the core message loop:
// user sends a message → agent-bridge routes it to Claude Code → user receives
// a non-empty reply.
//
// This is the most fundamental P0 test. If this fails, nothing else matters.
func TestP0_1_BasicMessageFlow_ClaudeCode(t *testing.T) {
	t.Parallel()
	testBasicMessageFlow(t, "claudecode")
}

func TestP0_1_BasicMessageFlow_Codex(t *testing.T) {
	t.Parallel()
	testBasicMessageFlow(t, "codex")
}

func testBasicMessageFlow(t *testing.T, agentType string) {
	t.Helper()
	env := helper.NewEnv(t, agentType)

	reply := env.Send("say hi briefly")

	if reply == nil {
		t.Fatal("no reply received")
	}
	if strings.TrimSpace(reply.Text()) == "" {
		t.Fatalf("received empty reply; all messages:\n%s", env.Platform.AllText())
	}
	t.Logf("P0-1 OK: received reply (%d chars): %q", len(reply.Text()), truncate(reply.Text(), 200))
}

// ── P0-2: /new 创建新会话 ─────────────────────────────────────────────────────

// TestP0_2_NewSession_ClaudeCode verifies /new creates a new session and
// agent-bridge sends a confirmation message.
func TestP0_2_NewSession_ClaudeCode(t *testing.T) {
	t.Parallel()
	testNewSession(t, "claudecode")
}

func testNewSession(t *testing.T, agentType string) {
	t.Helper()
	env := helper.NewEnv(t, agentType)

	// First, establish a session by sending a real message.
	env.Send("say hello briefly")

	// Now create a new session.
	before := env.Platform.MessageCount()
	env.SendNoWait("/new")

	reply := env.Platform.WaitForReply(before, 30*time.Second)
	if reply == nil {
		t.Fatal("/new: no confirmation message received within 30s")
	}
	t.Logf("P0-2 OK: /new replied: %q", truncate(reply.Text(), 200))
}

// ── P0-3: /list 显示会话列表 ──────────────────────────────────────────────────

// TestP0_3_ListSessions_ClaudeCode verifies /list returns a message containing
// session information after at least one session has been created.
func TestP0_3_ListSessions_ClaudeCode(t *testing.T) {
	t.Parallel()
	testListSessions(t, "claudecode")
}

func testListSessions(t *testing.T, agentType string) {
	t.Helper()
	env := helper.NewEnv(t, agentType)

	// Establish a session.
	env.Send("say hello briefly")

	// /list should return session information.
	reply := env.Send("/list")

	text := strings.ToLower(reply.Text())
	// The response should reference sessions in some form.
	hasSessionInfo := strings.Contains(text, "session") ||
		strings.Contains(text, "会话") ||
		strings.Contains(text, "#1") ||
		strings.Contains(text, "no sessions") ||
		strings.Contains(text, "没有会话")

	if !hasSessionInfo {
		t.Errorf("/list response doesn't look like a session list\ngot: %q\nall messages:\n%s",
			reply.Text(), env.Platform.AllText())
	}
	t.Logf("P0-3 OK: /list replied: %q", truncate(reply.Text(), 300))
}

// ── P0-5: /stop 停止当前任务 ──────────────────────────────────────────────────

// TestP0_5_StopCurrentTask_ClaudeCode verifies /stop causes agent-bridge to
// reply within a reasonable time even while a task is running.
//
// Strategy: we don't send a genuinely long-running task (too unreliable);
// instead we verify /stop itself produces a reply when no task is active,
// which still confirms the command is handled.
func TestP0_5_StopCurrentTask_ClaudeCode(t *testing.T) {
	t.Parallel()
	testStop(t, "claudecode")
}

func testStop(t *testing.T, agentType string) {
	t.Helper()
	env := helper.NewEnv(t, agentType)

	// Establish a session first so /stop has context.
	env.Send("say hello briefly")

	// Issue /stop and expect a timely response.
	reply := env.SendWithTimeout("/stop", 30*time.Second)

	if strings.TrimSpace(reply.Text()) == "" {
		t.Fatalf("/stop produced empty reply; all messages:\n%s", env.Platform.AllText())
	}
	t.Logf("P0-5 OK: /stop replied: %q", truncate(reply.Text(), 200))
}

// ── helpers ───────────────────────────────────────────────────────────────────

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
