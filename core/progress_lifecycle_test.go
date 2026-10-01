package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type lifecycleProgressPlatform struct {
	stubPlatformEngine
	style       string
	frames      []string
	messages    map[int]string
	deleted     []string
	deletedWhen []string
	failFinal   bool
}

func (p *lifecycleProgressPlatform) ProgressStyle() string { return p.style }
func (p *lifecycleProgressPlatform) SendPreviewStart(_ context.Context, _ any, content string) (any, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.messages == nil {
		p.messages = make(map[int]string)
	}
	id := len(p.messages) + 1
	p.messages[id] = content
	p.frames = append(p.frames, content)
	return id, nil
}
func (p *lifecycleProgressPlatform) UpdateMessage(_ context.Context, handle any, content string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages[handle.(int)] = content
	p.frames = append(p.frames, content)
	return nil
}
func (p *lifecycleProgressPlatform) DeletePreviewMessage(_ context.Context, handle any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted = append(p.deleted, p.messages[handle.(int)])
	p.deletedWhen = append(p.deletedWhen, strings.Join(append(append([]string{}, p.frames...), p.sent...), "\n"))
	return nil
}

func (p *lifecycleProgressPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	if p.failFinal && strings.Contains(content, "final answer") {
		return errors.New("delivery failed")
	}
	return p.stubPlatformEngine.Send(ctx, replyCtx, content)
}

func TestProgressLifecycle_PreservesAssistantTextAndCleansRecoveredDiagnostics(t *testing.T) {
	for _, style := range []string{"legacy", "compact", "latest", "card"} {
		for _, cleanup := range []bool{false, true} {
			for _, collapse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/cleanup=%v/collapse=%v", style, cleanup, collapse), func(t *testing.T) {
					p := &lifecycleProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, style: style}
					e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
					e.display.CleanupProgressOnComplete, e.display.CollapseToolMessages = cleanup, collapse
					e.SetReplyFooterEnabled(false)
					session := e.sessions.GetOrCreateActive("test:user")
					as := newControllableSession("progress-session")
					state := &interactiveState{agentSession: as, platform: p, replyCtx: "trigger"}
					e.interactiveStates["test:user"] = state
					for _, event := range []Event{
						{Type: EventThinking, Content: "thinking detail"},
						{Type: EventText, Content: "intermediate answer"},
						{Type: EventToolUse, ToolName: "Bash", ToolInput: "private-tool-input"},
						{Type: EventToolResult, ToolName: "Bash", ToolResult: "private-tool-result"},
						{Type: EventStatus, Content: "API error; retrying in 10 s"},
						{Type: EventText, Content: "final answer"},
						{Type: EventResult, Done: true},
					} {
						as.events <- event
					}
					e.processInteractiveEvents(state, session, e.sessions, "test:user", "m1", time.Now(), nil, nil, state.replyCtx)
					p.mu.Lock()
					defer p.mu.Unlock()
					all := strings.Join(append(append([]string{}, p.frames...), p.sent...), "\n")
					for _, expected := range []string{"intermediate answer", "final answer", "retrying in 10 s"} {
						if !strings.Contains(all, expected) {
							t.Fatalf("missing %q in %q", expected, all)
						}
						for _, deleted := range p.deleted {
							if expected != "retrying in 10 s" && strings.Contains(deleted, expected) {
								t.Fatalf("deleted assistant/diagnostic message %q", deleted)
							}
						}
					}
					if strings.Contains(strings.Join(p.deleted, "\n"), "retrying in 10 s") != cleanup {
						t.Fatalf("diagnostic cleanup = %#v, enabled=%v", p.deleted, cleanup)
					}
					for i, deleted := range p.deleted {
						if strings.Contains(deleted, "retrying") && !strings.Contains(p.deletedWhen[i], "final answer") {
							t.Fatal("diagnostic deleted before final delivery")
						}
					}
					if cleanup && len(p.deleted) == 0 {
						t.Fatal("progress was not deleted")
					}
					if !cleanup && len(p.deleted) != 0 {
						t.Fatal("cleanup disabled but messages deleted")
					}
					if collapse {
						if !strings.Contains(all, "Running command") {
							t.Fatalf("missing simple activity label: %q", all)
						}
						if strings.Contains(all, "private-tool-input") || strings.Contains(all, "private-tool-result") {
							t.Fatalf("collapsed progress leaked tool details: %q", all)
						}
					}
				})
			}
		}
	}
}

func TestStagedAttachments_SurviveConfigurationCommands(t *testing.T) {
	for _, command := range []string{"/model gpt-4.1", "/effort high", "/mode yolo", "/provider use second"} {
		t.Run(command, func(t *testing.T) {
			p := &stubPlatformEngine{n: "test"}
			a := &stubModelModeAgent{providers: []ProviderConfig{{Name: "second"}}}
			e := NewEngine("test", a, []Platform{p}, "")
			e.handleMessage(p, &Message{SessionKey: "test:user", ReplyCtx: "ctx", Images: []ImageAttachment{{Data: []byte("image")}}, Files: []FileAttachment{{FileName: "file.txt", Data: []byte("file")}}})
			// Simulate a live process being recycled by the configuration command.
			e.interactiveStates["test:user"] = &interactiveState{agentSession: &stubAgentSession{}, platform: p, replyCtx: "ctx"}
			e.handleMessage(p, &Message{SessionKey: "test:user", ReplyCtx: "ctx", Content: command})
			staged := e.takeStagedAttachments("test:user")
			if staged == nil || len(staged.images) != 1 || len(staged.files) != 1 {
				t.Fatalf("command lost staged attachments: %+v", staged)
			}
			if e.takeStagedAttachments("test:user") != nil {
				t.Fatal("attachments must be consumed only once")
			}
		})
	}
}

type queuedProgressSession struct{ *controllableAgentSession }

func (s *queuedProgressSession) Send(_ string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	for _, event := range []Event{
		{Type: EventToolUse, ToolName: "Read", ToolInput: "second-private-input"},
		{Type: EventStatus, Content: "second network wait"},
		{Type: EventText, Content: "second answer"},
		{Type: EventResult, Done: true},
	} {
		s.events <- event
	}
	return nil
}

func TestProgressLifecycle_QueuedTurnUsesItsOwnPlatformHandles(t *testing.T) {
	p1 := &lifecycleProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "first"}, style: "legacy"}
	p2 := &lifecycleProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "second"}, style: "legacy"}
	e := NewEngine("test", &stubAgent{}, []Platform{p1, p2}, "")
	e.display.CleanupProgressOnComplete, e.display.CollapseToolMessages = true, true
	e.SetReplyFooterEnabled(false)
	session := e.sessions.GetOrCreateActive("test:user")
	as := &queuedProgressSession{newControllableSession("queued-session")}
	state := &interactiveState{agentSession: as, platform: p1, replyCtx: "first-trigger", pendingMessages: []queuedMessage{{platform: p2, replyCtx: "second-trigger", content: "second prompt"}}}
	e.interactiveStates["test:user"] = state
	for _, event := range []Event{
		{Type: EventToolUse, ToolName: "Bash", ToolInput: "first-private-input"},
		{Type: EventStatus, Content: "first API retry"},
		{Type: EventText, Content: "first answer"},
		{Type: EventResult, Done: true},
	} {
		as.events <- event
	}
	e.processInteractiveEvents(state, session, e.sessions, "test:user", "m1", time.Now(), nil, nil, state.replyCtx)
	for _, tc := range []struct {
		p                *lifecycleProgressPlatform
		activity, answer string
	}{{p1, "Running command", "first answer"}, {p2, "Reading files", "second answer"}} {
		tc.p.mu.Lock()
		if len(tc.p.deleted) != 2 || !strings.Contains(tc.p.deleted[0], tc.activity) {
			t.Errorf("%s cleanup = %#v", tc.p.Name(), tc.p.deleted)
		}
		all := strings.Join(append(append([]string{}, tc.p.frames...), tc.p.sent...), "\n")
		if !strings.Contains(all, tc.answer) {
			t.Errorf("%s lost answer: %q", tc.p.Name(), all)
		}
		tc.p.mu.Unlock()
	}
}

type closedLifecycleSession struct{ *controllableAgentSession }

func (s *closedLifecycleSession) Close() error { return nil }

func TestProgressLifecycle_PreservesDiagnosticsWhenTurnOrDeliveryFails(t *testing.T) {
	for _, outcome := range []string{"agent-error", "result-error", "send-error", "channel-closed"} {
		t.Run(outcome, func(t *testing.T) {
			p := &lifecycleProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, style: "legacy", failFinal: outcome == "send-error"}
			e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
			e.display.CleanupProgressOnComplete = true
			e.SetReplyFooterEnabled(false)
			session := e.sessions.GetOrCreateActive("test:user")
			as := newControllableSession("failed-session")
			state := &interactiveState{agentSession: as, platform: p, replyCtx: "trigger"}
			e.interactiveStates["test:user"] = state
			as.events <- Event{Type: EventStatus, Content: "API error; retrying in 10 s"}
			switch outcome {
			case "agent-error":
				as.events <- Event{Type: EventError, Error: errors.New("final API error")}
			case "result-error":
				as.events <- Event{Type: EventResult, Done: true, Content: "final API error", Error: errors.New("failed")}
			case "send-error":
				as.events <- Event{Type: EventResult, Done: true, Content: "final answer"}
			case "channel-closed":
				close(as.events)
				state.agentSession = &closedLifecycleSession{as}
			}
			e.processInteractiveEvents(state, session, e.sessions, "test:user", "m1", time.Now(), nil, nil, state.replyCtx)
			p.mu.Lock()
			defer p.mu.Unlock()
			if len(p.deleted) != 0 {
				t.Fatalf("failed turn deleted diagnostics: %#v", p.deleted)
			}
			if !strings.Contains(strings.Join(p.frames, "\n"), "retrying in 10 s") {
				t.Fatal("diagnostics were not surfaced live")
			}
			if outcome == "agent-error" && !strings.Contains(strings.Join(p.sent, "\n"), "final API error") {
				t.Fatal("final error lost")
			}
		})
	}
}

func TestProgressLifecycle_PreservesEmojiInAssistantContent(t *testing.T) {
	p := &lifecycleProgressPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, style: "legacy"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "")
	e.display.CleanupProgressOnComplete = true
	e.SetReplyFooterEnabled(false)
	session := e.sessions.GetOrCreateActive("test:user")
	as := newControllableSession("emoji-session")
	state := &interactiveState{agentSession: as, platform: p, replyCtx: "trigger"}
	e.interactiveStates["test:user"] = state
	as.events <- Event{Type: EventResult, Done: true, Content: "User-requested output: ✅"}
	e.processInteractiveEvents(state, session, e.sessions, "test:user", "m1", time.Now(), nil, nil, state.replyCtx)
	if !strings.Contains(strings.Join(p.getSent(), "\n"), "✅") {
		t.Fatal("modified model content")
	}
}
