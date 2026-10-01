package claudecode

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agent-bridge/core"
)

func TestClaudeDiagnostics_ApiRetryAndNetworkAreNonTerminal(t *testing.T) {
	cs := &claudeSession{ctx: context.Background(), events: make(chan core.Event, 8)}
	cs.handleReadLoopLine(`{"type":"system","subtype":"api_retry","attempt":1,"max_retries":10,"retry_delay_ms":10000,"error_status":503,"error":"server_error"}`)
	cs.handleReadLoopLine(`{"type":"system","subtype":"status","status":"waiting_for_network"}`)
	cs.handleReadLoopLine(`{"type":"assistant","error":"api_error","message":{"content":[{"type":"text","text":"API Error: retrying in 10s"}]}}`)
	cs.handleReadLoopLine(`{"type":"assistant","message":{"content":[{"type":"text","text":"recovered answer"}]}}`)
	cs.handleReadLoopLine(`{"type":"result","result":"recovered answer"}`)
	for i := 0; i < 3; i++ {
		event := <-cs.events
		if event.Type != core.EventStatus || event.Done {
			t.Fatalf("retry ended turn: %+v", event)
		}
		if i == 0 && (!strings.Contains(event.Content, "10 s") || !strings.Contains(event.Content, "503")) {
			t.Fatalf("retry details missing: %q", event.Content)
		}
	}
	if event := <-cs.events; event.Type != core.EventText || event.Content != "recovered answer" {
		t.Fatalf("lost recovered output: %+v", event)
	}
	if event := <-cs.events; event.Type != core.EventResult || !event.Done {
		t.Fatalf("lost terminal result: %+v", event)
	}
}

func TestClaudeDiagnostics_StderrArrivesBeforeProcessCompletion(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-claude")
	script := "#!/bin/sh\nprintf 'API Error: retrying in 10s\\n' >&2\nsleep 0.1\nprintf '%s\\n' '{\"type\":\"result\",\"result\":\"recovered\"}'\n"
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := newClaudeSession(ctx, dir, bin, nil, "", "", "", "", "default", "", "", nil, nil, nil, nil, false, core.SpawnOptions{}, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	select {
	case event := <-cs.Events():
		if event.Type != core.EventStatus || !strings.Contains(event.Content, "retrying") {
			t.Fatalf("first event = %+v, want live diagnostic", event)
		}
	case <-ctx.Done():
		t.Fatal("no live stderr diagnostic")
	}
	select {
	case event := <-cs.Events():
		if event.Type != core.EventResult || !event.Done || event.Content != "recovered" {
			t.Fatalf("retry interrupted result: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal("no recovered result")
	}
}

func TestClaudeDiagnosticWriter_StreamsSplitCRAndUnterminatedLines(t *testing.T) {
	var messages []string
	w := &claudeDiagnosticWriter{emit: func(s string) { messages = append(messages, s) }}
	w.Write([]byte("ordinary debug log\nAPI Er"))
	w.Write([]byte("ror: retrying in 10s\r\n"))
	w.Write([]byte("\x1b[33mWaiting for network\x1b[0m"))
	if len(messages) != 2 || messages[0] != "API Error: retrying in 10s" || messages[1] != "Waiting for network" {
		t.Fatalf("diagnostics = %#v", messages)
	}
	w.Write([]byte("\r\n"))
	if len(messages) != 2 {
		t.Fatalf("duplicated unterminated diagnostic: %#v", messages)
	}
	w.Write([]byte(strings.Repeat("x", 40000)))
	if len(w.String()) > 32*1024 {
		t.Fatal("stderr retention is unbounded")
	}
}

func TestClaudeDiagnostics_FinalErrorsRemainTerminalErrors(t *testing.T) {
	for _, raw := range []map[string]any{
		{"is_error": true, "result": "API Error: exhausted retries"},
		{"subtype": "error_during_execution", "errors": []any{"network unavailable"}},
		{"subtype": "error_max_turns", "result": "partial answer", "errors": []any{"turn limit reached"}},
	} {
		cs := &claudeSession{ctx: context.Background(), events: make(chan core.Event, 1)}
		cs.handleResult(raw)
		evt := <-cs.events
		if evt.Type != core.EventError || !evt.Done || evt.Error == nil {
			t.Fatalf("final error treated as success: %+v", evt)
		}
		if result, ok := raw["result"].(string); ok && !strings.Contains(evt.Error.Error(), result) {
			t.Fatal("lost partial result/error detail")
		}
	}
}
