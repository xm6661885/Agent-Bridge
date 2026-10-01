package claudecode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"agent-bridge/core"
)

func TestHandleResultParsesUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &claudeSession{
		events: make(chan core.Event, 8),
		ctx:    ctx,
	}
	cs.sessionID.Store("test-session")
	cs.alive.Store(true)

	raw := map[string]any{
		"type":       "result",
		"result":     "done",
		"session_id": "test-session",
		"usage": map[string]any{
			"input_tokens":  float64(150000),
			"output_tokens": float64(2000),
		},
	}

	cs.handleResult(raw)

	evt := <-cs.events
	if evt.InputTokens != 150000 {
		t.Errorf("InputTokens = %d, want 150000", evt.InputTokens)
	}
	if evt.OutputTokens != 2000 {
		t.Errorf("OutputTokens = %d, want 2000", evt.OutputTokens)
	}
	if !evt.Done {
		t.Errorf("regular result event Done = false, want true")
	}
}

// TestHandleResultCompactionSubtypeIsNotTerminal is a regression test for
// issue #481: Claude Code's mid-turn context compaction emits a
// `type:"result"` event with `subtype:"compact"` (newer CLI) or
// `subtype:"compaction"` (older CLI). The engine must keep the turn
// running, so the emitted EventResult must have Done=false.
func TestHandleResultCompactionSubtypeIsNotTerminal(t *testing.T) {
	cases := []string{"compact", "compaction"}
	for _, subtype := range cases {
		t.Run(subtype, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cs := &claudeSession{
				events: make(chan core.Event, 4),
				ctx:    ctx,
			}
			cs.sessionID.Store("test-session")
			cs.alive.Store(true)

			cs.handleResult(map[string]any{
				"type":       "result",
				"subtype":    subtype,
				"isCompact":  true,
				"session_id": "test-session",
			})

			select {
			case evt := <-cs.events:
				if evt.Type != core.EventResult {
					t.Fatalf("event type = %q, want %q", evt.Type, core.EventResult)
				}
				if evt.Done {
					t.Errorf("compaction result Done = true, want false (turn must continue)")
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for EventResult")
			}
		})
	}
}

// TestIsCompactionResult covers both accepted subtype spellings and the
// negative case (regular result with no subtype).
func TestIsCompactionResult(t *testing.T) {
	cases := []struct {
		name string
		raw  map[string]any
		want bool
	}{
		{"nil_subtype", map[string]any{"type": "result"}, false},
		{"empty_subtype", map[string]any{"type": "result", "subtype": ""}, false},
		{"success_subtype", map[string]any{"type": "result", "subtype": "success"}, false},
		{"compact_subtype", map[string]any{"type": "result", "subtype": "compact"}, true},
		{"compaction_subtype", map[string]any{"type": "result", "subtype": "compaction"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCompactionResult(tc.raw); got != tc.want {
				t.Errorf("isCompactionResult(%v) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestHandleAssistantCapturesPerSubCallUsage verifies the split-source policy
// for the runtime ContextUsage snapshot used by the reply footer:
//
//   - input/cache values come from the LAST assistant event (per-sub-call),
//     so ctx % reflects the prompt size of the final inference call rather
//     than a sum that exceeds the context window.
//   - output_tokens comes from the result event (turn aggregate), since
//     stream-json assistant events carry a placeholder output_tokens=1
//     (the real per-call output count never appears in the live stream).
func TestHandleAssistantCapturesPerSubCallUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &claudeSession{
		events: make(chan core.Event, 8),
		ctx:    ctx,
	}
	cs.sessionID.Store("test-session")
	cs.alive.Store(true)
	cs.activeModel.Store("claude-opus-4-7[1m]") // 1M context window

	// Sub-call #1: small prompt, ~100k tokens of cached prefix.
	// Stream-json carries placeholder output_tokens=1 on assistant events.
	cs.handleAssistant(map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{},
			"usage": map[string]any{
				"input_tokens":                float64(50),
				"output_tokens":               float64(1), // placeholder, ignored
				"cache_creation_input_tokens": float64(0),
				"cache_read_input_tokens":     float64(100_000),
			},
		},
	})
	// Drain any events emitted (none here since content is empty).
	for len(cs.events) > 0 {
		<-cs.events
	}

	// Sub-call #2 (final): same cached prefix grown to ~500k.
	cs.handleAssistant(map[string]any{
		"type": "assistant",
		"message": map[string]any{
			"content": []any{},
			"usage": map[string]any{
				"input_tokens":                float64(80),
				"output_tokens":               float64(1), // placeholder, ignored
				"cache_creation_input_tokens": float64(2_000),
				"cache_read_input_tokens":     float64(500_000),
			},
		},
	})

	// Result event: input/cache fields are aggregated (cache_read summed
	// across many sub-calls — would clamp ctx % to 100% if used). The
	// output_tokens here IS authoritative — the real total tokens
	// generated by the model this turn.
	cs.handleResult(map[string]any{
		"type":       "result",
		"result":     "done",
		"session_id": "test-session",
		"usage": map[string]any{
			"input_tokens":                float64(130),
			"output_tokens":               float64(648), // real turn total
			"cache_creation_input_tokens": float64(2_000),
			"cache_read_input_tokens":     float64(8_000_000), // summed, would inflate ctx
		},
	})

	usage := cs.GetContextUsage()
	if usage == nil {
		t.Fatal("GetContextUsage returned nil; expected per-sub-call snapshot")
	}
	// Input/cache: must match LAST assistant event (sub-call #2), not the
	// aggregated result.
	if usage.InputTokens != 80 {
		t.Errorf("InputTokens = %d, want 80 (last assistant)", usage.InputTokens)
	}
	if usage.CachedInputTokens != 500_000 {
		t.Errorf("CachedInputTokens = %d, want 500_000 (last assistant); 8M would indicate aggregated leak",
			usage.CachedInputTokens)
	}
	if usage.CacheCreationInputTokens != 2_000 {
		t.Errorf("CacheCreationInputTokens = %d, want 2_000", usage.CacheCreationInputTokens)
	}
	if usage.UsedTokens != 80+2_000+500_000 {
		t.Errorf("UsedTokens = %d, want %d", usage.UsedTokens, 80+2_000+500_000)
	}
	if usage.ContextWindow != 1_000_000 {
		t.Errorf("ContextWindow = %d, want 1_000_000 (opus-4-7[1m])", usage.ContextWindow)
	}
	// Output: must come from the result event, not from an assistant
	// placeholder. 648 is the real turn-total; 1 would indicate the
	// placeholder leaked through.
	if usage.OutputTokens != 648 {
		t.Errorf("OutputTokens = %d, want 648 (result aggregate)", usage.OutputTokens)
	}
	if usage.TotalTokens != usage.UsedTokens+648 {
		t.Errorf("TotalTokens = %d, want UsedTokens+648 = %d", usage.TotalTokens, usage.UsedTokens+648)
	}
	// Sanity: ctx % should be reasonable (~50%), NOT clamped at 100%.
	pct := float64(usage.UsedTokens) * 100 / float64(usage.ContextWindow)
	if pct > 90 {
		t.Errorf("ctx %% = %.1f, expected ~50%% — aggregated cache_read leaked through", pct)
	}
}

func TestHandleResultNoUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cs := &claudeSession{
		events: make(chan core.Event, 8),
		ctx:    ctx,
	}
	cs.sessionID.Store("test-session")
	cs.alive.Store(true)

	raw := map[string]any{
		"type":   "result",
		"result": "done",
	}

	cs.handleResult(raw)

	evt := <-cs.events
	if evt.InputTokens != 0 {
		t.Errorf("InputTokens = %d, want 0", evt.InputTokens)
	}
	if evt.OutputTokens != 0 {
		t.Errorf("OutputTokens = %d, want 0", evt.OutputTokens)
	}
}

func TestReadLoop_ChildHoldsStdoutPipe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	t.Cleanup(func() {
		_ = pw.Close()
	})

	writeDone := make(chan error, 1)
	go func() {
		_, err := io.WriteString(pw, `{"type":"system","session_id":"test-pipe"}`+"\n")
		writeDone <- err
	}()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	cs := &claudeSession{
		cmd:    cmd,
		events: make(chan core.Event, 64),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	cs.alive.Store(true)
	go cs.readLoop(pr, &stderrBuf)

	timeout := time.After(5 * time.Second)
	gotEvent := false
	for {
		select {
		case err := <-writeDone:
			if err != nil {
				t.Fatal(err)
			}
			writeDone = nil
		case evt, ok := <-cs.events:
			if !ok {
				if !gotEvent {
					t.Fatal("events closed but system event lost")
				}
				return
			}
			if evt.SessionID == "test-pipe" {
				gotEvent = true
			}
		case <-timeout:
			t.Fatal("HANG: events not closed within 5s - readLoop stuck in scanner.Scan()")
		}
	}
}

func TestReadLoop_CtxCancelClosesChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pr, pw := io.Pipe()
	t.Cleanup(func() {
		_ = pw.Close()
	})

	// "err-then-sleep" emits stderr before sleeping so that ctx cancel
	// produces a non-empty stderrBuf in readLoop's defer — exercising the
	// `case <-cs.ctx.Done()` select branch in finishReadLoop.
	cmd := helperCommand(ctx, "err-then-sleep")
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	cs := &claudeSession{
		cmd:    cmd,
		events: make(chan core.Event, 64),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	cs.alive.Store(true)
	go cs.readLoop(pr, &stderrBuf)

	time.Sleep(200 * time.Millisecond)
	cancel()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-cs.events:
			if !ok {
				goto closed
			}
		case <-timeout:
			t.Fatal("HANG: events not closed within 5s after ctx cancel")
		}
	}
closed:
	select {
	case <-cs.done:
	case <-timeout:
		t.Fatal("HANG: done not closed within 5s after ctx cancel")
	}
}

// nopCloseBuffer wraps bytes.Buffer with a no-op Close so it can serve as an
// io.WriteCloser stand-in for a claudeSession's stdin in tests.
type nopCloseBuffer struct {
	bytes.Buffer
}

func (nopCloseBuffer) Close() error { return nil }

// TestClaudeSessionCancelTurn_WritesInterruptAndWaitsForResult verifies
// CancelTurn writes the stream-json control_request/interrupt frame and
// unblocks as soon as the in-flight turn's terminal EventResult is observed
// via handleResult — without racing the engine's own read of cs.events.
func TestClaudeSessionCancelTurn_WritesInterruptAndWaitsForResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &nopCloseBuffer{}
	cs := &claudeSession{
		events: make(chan core.Event, 8),
		ctx:    ctx,
		stdin:  stdin,
	}
	cs.alive.Store(true)

	// Simulate Send() arming the turn for an in-flight prompt.
	cs.turnDoneMu.Lock()
	cs.turnDoneCh = make(chan struct{})
	cs.turnDoneMu.Unlock()

	done := make(chan error, 1)
	go func() {
		done <- cs.CancelTurn()
	}()

	// Give CancelTurn a moment to write the interrupt request before we
	// simulate the interrupted turn's own result event landing.
	deadline := time.After(2 * time.Second)
	for stdin.Len() == 0 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for interrupt control_request to be written")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	var req map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdin.Bytes()), &req); err != nil {
		t.Fatalf("interrupt request not valid JSON: %v (raw=%q)", err, stdin.String())
	}
	if req["type"] != "control_request" {
		t.Errorf("type = %v, want control_request", req["type"])
	}
	inner, _ := req["request"].(map[string]any)
	if inner == nil || inner["subtype"] != "interrupt" {
		t.Errorf("request.subtype = %v, want interrupt", inner)
	}
	if _, ok := req["request_id"].(string); !ok {
		t.Errorf("request_id missing or not a string: %v", req["request_id"])
	}

	// Simulate the interrupted turn's own terminal result event landing.
	cs.handleResult(map[string]any{"type": "result", "result": "partial output"})
	// Drain it exactly as the engine's processInteractiveEvents would, so
	// CancelTurn's turnDoneCh-based wait (not this channel) is what unblocks it.
	<-cs.events

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CancelTurn returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("CancelTurn did not return after simulated turn completion")
	}
}

// TestClaudeSessionCancelTurn_NoTurnInFlight verifies CancelTurn is a no-op
// (returns nil immediately, writes nothing) when no turn is armed.
func TestClaudeSessionCancelTurn_NoTurnInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdin := &nopCloseBuffer{}
	cs := &claudeSession{
		events: make(chan core.Event, 8),
		ctx:    ctx,
		stdin:  stdin,
	}
	cs.alive.Store(true)

	if err := cs.CancelTurn(); err != nil {
		t.Fatalf("CancelTurn() = %v, want nil when no turn in flight", err)
	}
	if stdin.Len() != 0 {
		t.Errorf("CancelTurn wrote to stdin with no turn in flight: %q", stdin.String())
	}
}

func TestClaudeSessionSteerWritesUserMessageWithoutReplacingActiveTurn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdin := &nopCloseBuffer{}
	cs := &claudeSession{ctx: ctx, stdin: stdin}
	cs.alive.Store(true)
	activeTurn := make(chan struct{})
	cs.turnDoneMu.Lock()
	cs.turnDoneCh = activeTurn
	cs.turnDoneMu.Unlock()

	if err := cs.Steer("prioritize tests", "msg-1", nil, nil); err != nil {
		t.Fatalf("Steer returned error: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stdin.Bytes()), &got); err != nil {
		t.Fatalf("steer message not valid JSON: %v", err)
	}
	if got["type"] != "user" {
		t.Fatalf("type = %v, want user", got["type"])
	}
	cs.turnDoneMu.Lock()
	defer cs.turnDoneMu.Unlock()
	if cs.turnDoneCh != activeTurn {
		t.Fatal("Steer replaced the active turn completion channel")
	}
}

func TestClaudeSessionClose_IdempotentNoPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := helperCommand(ctx, "stdin-eof-exit")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	cs := &claudeSession{
		cmd:                 cmd,
		stdin:               stdin,
		ctx:                 ctx,
		cancel:              cancel,
		done:                done,
		gracefulStopTimeout: 200 * time.Millisecond,
	}
	cs.alive.Store(true)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Close panicked: %v", r)
		}
	}()

	if err := cs.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestShellJoinArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"empty", nil, ""},
		{"single_plain", []string{"--verbose"}, "--verbose"},
		{"multiple_plain", []string{"--verbose", "--model", "opus"}, "--verbose --model opus"},
		{"arg_with_space", []string{"--prompt", "hello world"}, "--prompt 'hello world'"},
		{"arg_with_tab", []string{"a\tb"}, "'a\tb'"},
		{"arg_with_newline", []string{"line1\nline2"}, "'line1\nline2'"},
		{"arg_with_single_quote", []string{"it's"}, "'it'\\''s'"},
		{"arg_with_double_quote", []string{`say "hi"`}, `'say "hi"'`},
		{"arg_with_backslash", []string{`path\to`}, `'path\to'`},
		{"mixed", []string{"--flag", "has space", "plain", "it's here"}, "--flag 'has space' plain 'it'\\''s here'"},
		{"empty_string_arg", []string{""}, ""},
		{"long_prompt", []string{"--append-system-prompt", "You are a helpful assistant.\nBe concise."}, "--append-system-prompt 'You are a helpful assistant.\nBe concise.'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shellJoinArgs(tt.args)
			if got != tt.want {
				t.Errorf("shellJoinArgs(%v)\n  got  = %q\n  want = %q", tt.args, got, tt.want)
			}
		})
	}
}

// TestWriteTempAppendPromptFile_UniquePerCall covers the 1% edge case:
// when the prompt includes per-session pieces (platform formatting or
// user append) two concurrent spawns must each get their own file so
// they cannot overwrite each other's content before claude reads it.
func TestWriteTempAppendPromptFile_UniquePerCall(t *testing.T) {
	// dir is auto-cleaned by t.TempDir(), so per-file Remove is unnecessary.
	dir := t.TempDir()
	a, err := writeTempAppendPromptFile(dir, "session A")
	if err != nil {
		t.Fatalf("write A: %v", err)
	}
	b, err := writeTempAppendPromptFile(dir, "session B")
	if err != nil {
		t.Fatalf("write B: %v", err)
	}
	if a == b {
		t.Fatalf("two writeTempAppendPromptFile calls returned the same path %q "+
			"— concurrent customised sessions would overwrite each other", a)
	}

	// Files must contain their own content (no cross-talk).
	gotA, _ := os.ReadFile(a)
	gotB, _ := os.ReadFile(b)
	if string(gotA) != "session A" || string(gotB) != "session B" {
		t.Errorf("cross-talk: A=%q B=%q", string(gotA), string(gotB))
	}
}

// TestWriteTempAppendPromptFile_ReadableByOtherUser guards the
// run_as_user regression from issue #1429. os.CreateTemp defaults to
// 0600 owned by the agent-bridge process user; when the agent is
// spawned as a different OS user (via run_as_user), a 0600 root-owned
// file is unreadable and the agent exits with EACCES before reading
// any prompt at all. The fix is to chmod 0o644 immediately after
// write.
//
// We assert the contract two ways: (1) the on-disk mode is 0o644 —
// any reader path bit is set, no execute bits, no setuid/sticky; and
// (2) a non-owner stat-open succeeds in O_RDONLY, which is the same
// access path the spawned agent uses when it calls os.Open on the
// file path passed via --append-system-prompt-file.
func TestWriteTempAppendPromptFile_ReadableByOtherUser(t *testing.T) {
	dir := t.TempDir()
	path, err := writeTempAppendPromptFile(dir, "session X")
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	want := os.FileMode(0o644)
	if info.Mode().Perm() != want {
		t.Fatalf("per-spawn prompt file mode = %o, want %o — run_as_user target user would get EACCES (#1429)",
			info.Mode().Perm(), want)
	}

	// Non-owner open simulates the spawned agent's read path. On root
	// the kernel bypasses the mode bits, so this only fails for a
	// truly 0o000 file. We still assert it to make the regression
	// observable on systems where the test runs as a non-root user
	// (CI matrix, dev laptops).
	if _, err := os.OpenFile(path, os.O_RDONLY, 0); err != nil {
		t.Fatalf("open O_RDONLY as a non-owner: %v — file is unreadable even for an unprivileged reader", err)
	}
}

func makeFiller(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return string(b)
}

// TestHandleUserEmitsToolResult is a regression test for the bug where
// claudeSession.handleUser silently dropped tool_result content blocks
// (only logging when is_error=true) instead of emitting EventToolResult.
// Without this event, engine never sees tool output and the Feishu/Slack/
// Discord progress card never renders tool results — only the final
// assistant text reaches the user.
//
// Cases covered:
//   - string content (plain text result)
//   - array content (Anthropic SDK multi-block: [{type:"text", text:"..."}])
//   - is_error=true (exit code 1, success=false)
func TestHandleUserEmitsToolResult(t *testing.T) {
	cases := []struct {
		name        string
		raw         map[string]any
		wantResult  string
		wantCode    int
		wantSuccess bool
	}{
		{
			name: "string content",
			raw: map[string]any{
				"type": "user",
				"message": map[string]any{
					"content": []any{
						map[string]any{
							"type":        "tool_result",
							"tool_use_id": "toolu_abc",
							"is_error":    false,
							"content":     "command output here",
						},
					},
				},
			},
			wantResult:  "command output here",
			wantCode:    0,
			wantSuccess: true,
		},
		{
			name: "array content",
			raw: map[string]any{
				"type": "user",
				"message": map[string]any{
					"content": []any{
						map[string]any{
							"type":        "tool_result",
							"tool_use_id": "toolu_def",
							"is_error":    false,
							"content": []any{
								map[string]any{"type": "text", "text": "line one"},
								map[string]any{"type": "text", "text": "line two"},
							},
						},
					},
				},
			},
			wantResult:  "line one\nline two",
			wantCode:    0,
			wantSuccess: true,
		},
		{
			name: "error result",
			raw: map[string]any{
				"type": "user",
				"message": map[string]any{
					"content": []any{
						map[string]any{
							"type":        "tool_result",
							"tool_use_id": "toolu_err",
							"is_error":    true,
							"content":     "boom",
						},
					},
				},
			},
			wantResult:  "boom",
			wantCode:    1,
			wantSuccess: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cs := &claudeSession{
				events: make(chan core.Event, 4),
				ctx:    ctx,
			}
			cs.alive.Store(true)

			cs.handleUser(tc.raw)

			select {
			case evt := <-cs.events:
				if evt.Type != core.EventToolResult {
					t.Fatalf("event type = %q, want %q", evt.Type, core.EventToolResult)
				}
				if evt.ToolResult != tc.wantResult {
					t.Errorf("ToolResult = %q, want %q", evt.ToolResult, tc.wantResult)
				}
				if evt.ToolExitCode == nil || *evt.ToolExitCode != tc.wantCode {
					got := -1
					if evt.ToolExitCode != nil {
						got = *evt.ToolExitCode
					}
					t.Errorf("ToolExitCode = %d, want %d", got, tc.wantCode)
				}
				if evt.ToolSuccess == nil || *evt.ToolSuccess != tc.wantSuccess {
					got := false
					if evt.ToolSuccess != nil {
						got = *evt.ToolSuccess
					}
					t.Errorf("ToolSuccess = %v, want %v", got, tc.wantSuccess)
				}
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for EventToolResult — handleUser dropped the tool_result")
			}
		})
	}
}

func helperCommand(ctx context.Context, mode string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestHelperProcess", "--", mode)
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	return cmd
}

// TestHelperProcess lets this test binary act as a tiny external command for
// cases that need a process with controlled lifetime semantics.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	mode := "unknown"
	for i := len(os.Args) - 1; i >= 0; i-- {
		if os.Args[i] == "--" && i+1 < len(os.Args) {
			mode = os.Args[i+1]
			break
		}
	}
	if mode == "unknown" && len(os.Args) > 1 {
		// Backward-compatible fallback for tests that don't pass "--":
		// TestHelperProcess_StdinEofExit and friends invoke the helper
		// directly with the mode as the last arg.
		mode = os.Args[len(os.Args)-1]
	}
	switch mode {
	case "sleep":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "err-then-sleep":
		_, _ = os.Stderr.WriteString("helper: starting up\n")
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "stdin-eof-exit":
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	case "claude-stdin-echo":
		// Issue #1736 regression harness: act as a Claude Code stub that
		//   1. asserts --replay-user-messages is NOT in argv (its presence
		//      would make the CLI exit after the first message and break
		//      `/compact`, `/clear`, `/resume`).
		//   2. emits a system + result event on startup so agent-bridge
		//      recognises the session as live.
		//   3. reads stdin line-by-line, treating each line as a new user
		//      turn, and replies with a `type:"result"` event containing
		//      the line's text — proving the process stayed alive across
		//      turns instead of exiting after the first one.
		//   4. exits cleanly only when stdin closes (which is exactly how
		//      agent-bridge's Close() and the #1338 idle reaper will end
		//      the session).
		for _, a := range os.Args {
			if a == "--replay-user-messages" {
				_, _ = os.Stderr.WriteString("HELPER: --replay-user-messages unexpectedly present in argv\n")
				os.Exit(3)
			}
		}
		// Args layout reminder: the helper is invoked via
		//   test_bin -test.run=TestHelperProcess -- claude-stdin-echo <innerArgs...> <outerArgs...>
		// so the mode token (the value after "--") is the second-to-last
		// element, not the last one. Walk from the end backwards until we
		// hit it. Falling back to os.Args[1] is harmless if "--" is
		// missing (the earlier helperCommand usage relies on it).
		mode := "unknown"
		for i := len(os.Args) - 1; i >= 0; i-- {
			if os.Args[i] == "--" && i+1 < len(os.Args) {
				mode = os.Args[i+1]
				break
			}
		}
		_, _ = os.Stderr.WriteString("HELPER start mode=" + mode + " argvLen=" + itoa(len(os.Args)) + "\n")
		if mode != "claude-stdin-echo" {
			_, _ = os.Stderr.WriteString("HELPER: unexpected mode token; exiting\n")
			os.Exit(2)
		}
		_, _ = os.Stdout.WriteString(`{"type":"system","subtype":"init","session_id":"helper-keepalive"}` + "\n")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Buffer(make([]byte, 0, 64*1024), 1*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			// agent-bridge's Send() wraps every user message in the
			// envelope `{"type":"user","message":{"role":"user","content":"<text>"}}`.
			// Unwrap it so the echoed `result` field carries the human-
			// readable text and the regression test can assert ordering
			// against its expected slice without false negatives caused by
			// JSON quoting.
			turnText := line
			var envelope struct {
				Type    string `json:"type"`
				Message struct {
					Role    string `json:"role"`
					Content any    `json:"content"`
				} `json:"message"`
			}
			if err := json.Unmarshal([]byte(line), &envelope); err == nil && envelope.Type == "user" {
				switch c := envelope.Message.Content.(type) {
				case string:
					turnText = c
				case []any:
					var parts []string
					for _, item := range c {
						m, ok := item.(map[string]any)
						if !ok {
							continue
						}
						if t, ok := m["text"].(string); ok {
							parts = append(parts, t)
						}
					}
					turnText = strings.Join(parts, "\n")
				}
			}
			// Echo every turn back as a result event so the test
			// can assert the live process saw each one. Use json.Marshal
			// to keep escaping correct for any character class.
			payload, _ := json.Marshal(map[string]any{
				"type":    "result",
				"result":  "echo: " + turnText,
				"isError": false,
			})
			_, _ = os.Stdout.Write(payload)
			_, _ = os.Stdout.Write([]byte("\n"))
		}
		os.Exit(0)
	default:
		os.Exit(2)
	}
}

// TestNewClaudeSession_NoReplayFlagKeepsProcessAlive is the regression
// test for issue #1736. Before the fix, newClaudeSession passed
// --replay-user-messages to Claude Code, which drained stdin and exited
// after each turn. That made /compact (and any other in-session slash
// command) unreachable. The fix removes the flag; Claude Code then
// keeps reading stdin until either Close() or agent_session_idle_timeout_mins
// (#1338) reaps it.
//
// We verify both halves:
//
//	(a) the helper process (standing in for Claude Code) reports a
//	    missing --replay-user-messages in argv via a non-zero exit,
//	(b) the session can drive 100+ messages through the live process
//	    with /compact / /clear / /resume interleaved and the process
//	    echoes every one of them back, proving keep-alive works.
func TestNewClaudeSession_NoReplayFlagKeepsProcessAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build the helper command directly so the test does not depend on
	// any particular agent/session.go code path that would mask the flag.
	helperPath, err := exec.LookPath(os.Args[0])
	if err != nil {
		t.Fatalf("cannot locate test binary: %v", err)
	}
	bin := helperPath
	helperArgs := []string{
		"-test.run=TestHelperProcess",
		"--", "claude-stdin-echo",
	}
	helpEnv := append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")

	// Sanity: the helper itself asserts --replay-user-messages is not
	// present in argv. Run it once and verify the helper exits 0.
	probe := exec.CommandContext(ctx, bin, helperArgs...)
	probe.Env = helpEnv
	var probeStderr bytes.Buffer
	probe.Stderr = &probeStderr
	probe.Stdin = strings.NewReader("")
	if err := probe.Run(); err != nil {
		t.Fatalf("helper probe failed: %v\nstderr=%s", err, probeStderr.String())
	}

	workDir := t.TempDir()

	// Spawn newClaudeSession directly so we exercise the exact code path
	// that decides innerArgs/outerArgs. We bypass newClaudeSession's
	// permission-mode / system-prompt / plugin-dir machinery — they are
	// orthogonal to the flag under test.
	spawnOpts := core.SpawnOptions{}
	// The helper is the same Go test binary running TestHelperProcess in
	// helper mode; without GO_WANT_HELPER_PROCESS=1 the child would just
	// execute the parent test suite and exit immediately, never reaching
	// the helper branch.
	cs, err := newClaudeSession(
		ctx,
		workDir,
		bin,
		helperArgs,                           // cliExtraArgs (forwarded into allArgs)
		"",                                   // cmdArgsFlag (no wrapper bundling)
		"",                                   // model
		"",                                   // effort
		"",                                   // sessionID (truly fresh)
		"default",                            // mode
		"",                                   // systemPrompt
		"",                                   // appendSystemPrompt
		nil,                                  // allowedTools
		nil,                                  // disallowedTools
		nil,                                  // pluginDirs
		[]string{"GO_WANT_HELPER_PROCESS=1"}, // extraEnv: route child into helper branch
		false,                                // disableVerbose
		spawnOpts,
		0,  // maxContextTokens
		"", // ccDataDir
	)
	if err != nil {
		t.Fatalf("newClaudeSession: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// Wait for the system event so we know the helper is alive.
	// claudeSession.handleSystem emits EventText carrying the session id
	// when a `type:"system"` event with a session_id field arrives.
	startDeadline := time.After(5 * time.Second)
	for cs.CurrentSessionID() != "helper-keepalive" {
		select {
		case <-cs.Events():
			// drain
		case <-startDeadline:
			t.Fatalf("session id never set; helper did not emit a system event (alive=%v)", cs.Alive())
			return
		}
	}

	// Drive 100 normal messages + the three slash commands interleaved.
	// Issue #1736 expected behaviour: every one of these reaches the live
	// process and the process echoes it back. With --replay-user-messages
	// the process would have exited after the first message and the
	// remaining sends would fail with "session process is not running".
	plan := []string{}
	for i := 0; i < 100; i++ {
		plan = append(plan, "regular message "+itoa(i))
	}
	// Interleave the slash commands in the middle so the regression is
	// sensitive to keep-alive surviving a normal-message burst as well.
	plan = append(plan, "/compact", "/clear", "/resume")

	gotResults := 0
	for i, msg := range plan {
		if err := cs.Send(msg, "msg-"+itoa(i), nil, nil); err != nil {
			t.Fatalf("Send #%d (%q) failed: %v", i, msg, err)
		}
		if !cs.Alive() {
			t.Fatalf("session died after message #%d (%q) — the helper exited when it should have stayed alive", i, msg)
		}
	}

	// Drain echoed results until we have at least len(plan) of them or
	// time out. The helper echoes one `result` event per line written to
	// stdin.
	deadline := time.After(15 * time.Second)
	for gotResults < len(plan) {
		select {
		case ev, ok := <-cs.Events():
			if !ok {
				t.Fatalf("event channel closed after %d results; helper exited early (alive=%v)", gotResults, cs.Alive())
			}
			if ev.Type == core.EventResult {
				if !strings.HasPrefix(ev.Content, "echo: ") {
					t.Fatalf("result #%d = %q, want echo prefix", gotResults, ev.Content)
				}
				want := plan[gotResults]
				if ev.Content != "echo: "+want {
					t.Fatalf("result #%d = %q, want %q (order must be preserved across turns)", gotResults, ev.Content, "echo: "+want)
				}
				gotResults++
			}
		case <-deadline:
			t.Fatalf("only received %d/%d echoes within 15s", gotResults, len(plan))
		}
	}

	// Final assertion: the helper is still alive after 100+ messages.
	// Close() will send stdin EOF and the helper will exit cleanly.
	if !cs.Alive() {
		t.Fatal("helper died before Close() — keep-alive contract violated")
	}
}

// itoa avoids strconv import noise in the test body.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// TestClaudeSession_InteractiveToolsNotAutoDecided verifies that bypass and
// dontAsk modes still forward AskUserQuestion / ExitPlanMode to the user
// instead of answering them with an empty allow or a deny.
func TestClaudeSession_InteractiveToolsNotAutoDecided(t *testing.T) {
	for _, mode := range []string{"bypassPermissions", "dontAsk"} {
		for _, tool := range []string{"AskUserQuestion", "ExitPlanMode"} {
			ctx, cancel := context.WithCancel(context.Background())
			stdin := &nopCloseBuffer{}
			hooks := &ccPermissionHookRunner{}
			hooks.once.Do(func() {}) // no settings: hooks never decide
			cs := &claudeSession{events: make(chan core.Event, 1), ctx: ctx, stdin: stdin, ccHooks: hooks}
			cs.alive.Store(true)
			cs.setPermissionMode(mode)

			input := map[string]any{"plan": "the plan", "questions": []any{map[string]any{"question": "Q?"}}}
			cs.handleControlRequest(map[string]any{
				"request_id": "r1",
				"request":    map[string]any{"subtype": "can_use_tool", "tool_name": tool, "input": input},
			})
			if stdin.Len() != 0 {
				t.Fatalf("%s/%s: auto-responded %q", mode, tool, stdin.String())
			}
			select {
			case evt := <-cs.events:
				if evt.Type != core.EventPermissionRequest || evt.ToolName != tool {
					t.Fatalf("%s/%s: unexpected event %+v", mode, tool, evt)
				}
				if tool == "ExitPlanMode" && evt.ToolInput != "the plan" {
					t.Fatalf("plan text not surfaced: %q", evt.ToolInput)
				}
			default:
				t.Fatalf("%s/%s: no permission event forwarded", mode, tool)
			}
			cancel()
		}
	}
}

// TestClaudeSession_RespondPermissionSetMode verifies updatedPermissions are
// sent and the session's own mode flags follow a setMode update.
func TestClaudeSession_RespondPermissionSetMode(t *testing.T) {
	stdin := &nopCloseBuffer{}
	cs := &claudeSession{stdin: stdin}
	cs.alive.Store(true)
	cs.setPermissionMode("plan")

	err := cs.RespondPermission("r1", core.PermissionResult{
		Behavior:           "allow",
		UpdatedInput:       map[string]any{"plan": "p"},
		UpdatedPermissions: []map[string]any{{"type": "setMode", "mode": "acceptEdits", "destination": "session"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdin.String(), `"updatedPermissions":[{"destination":"session","mode":"acceptEdits","type":"setMode"}]`) {
		t.Fatalf("missing updatedPermissions: %s", stdin.String())
	}
	if cs.permissionModeValue() != "acceptEdits" || !cs.acceptEditsOnly.Load() {
		t.Fatalf("mode not mirrored: %q", cs.permissionModeValue())
	}
}
