package core

import (
	"strings"
	"testing"
)

func newPlanPendingState(e *Engine, key string, p Platform, rec AgentSession) *interactiveState {
	state := &interactiveState{
		agentSession: rec,
		platform:     p,
		replyCtx:     "ctx",
		pending: &pendingPermission{
			RequestID:   "req-plan",
			ToolName:    "ExitPlanMode",
			ToolInput:   map[string]any{"plan": "1. do it"},
			Plan:        "1. do it",
			OwnerUserID: "owner",
			Resolved:    make(chan struct{}),
		},
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()
	return state
}

func planMsg(user, content string) *Message {
	return &Message{SessionKey: "test:chat", UserID: user, Content: content, ReplyCtx: "ctx"}
}

func TestParseAskqCallback(t *testing.T) {
	q, opts, ok := parseAskqCallback("askq:1:2,4")
	if !ok || q != 1 || len(opts) != 2 || opts[0] != 2 || opts[1] != 4 {
		t.Fatalf("got q=%d opts=%v ok=%v", q, opts, ok)
	}
	if _, _, ok := parseAskqCallback("askq:1"); ok {
		t.Fatal("legacy two-part form should not parse")
	}
	if _, _, ok := parseAskqCallback("hello"); ok {
		t.Fatal("free text should not parse")
	}
}

func TestParsePlanChoice(t *testing.T) {
	cases := map[string]int{
		"askq:0:1":                    planChoiceAutoEdits,
		"2":                           planChoiceReview,
		" 3 ":                         planChoiceRevise,
		"ok":                          planChoiceReview,
		"allow all":                   planChoiceAutoEdits,
		"no":                          planChoiceRevise,
		"no, use sqlite instead of x": 0,
		"改用内存缓存":                      0,
		"4":                           planChoiceBypass,
		"5":                           0,
	}
	for in, want := range cases {
		if got := parsePlanChoice(in); got != want {
			t.Errorf("parsePlanChoice(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestPlanReview_ApproveAutoEditsSetsMode(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}
	state := newPlanPendingState(e, "test:chat", p, rec)

	if !e.handlePendingPermission(p, planMsg("owner", "1"), "1", "") {
		t.Fatal("expected handled")
	}
	if rec.calls != 1 || rec.lastResult.Behavior != "allow" {
		t.Fatalf("expected one allow, got calls=%d result=%+v", rec.calls, rec.lastResult)
	}
	if len(rec.lastResult.UpdatedPermissions) != 1 || rec.lastResult.UpdatedPermissions[0]["mode"] != "acceptEdits" {
		t.Fatalf("expected setMode acceptEdits, got %+v", rec.lastResult.UpdatedPermissions)
	}
	if state.pending != nil {
		t.Fatal("pending should be cleared")
	}
}

func TestPlanReview_KeepPlanningThenFeedback(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}
	state := newPlanPendingState(e, "test:chat", p, rec)

	if !e.handlePendingPermission(p, planMsg("owner", "3"), "3", "") {
		t.Fatal("expected handled")
	}
	if rec.calls != 0 || state.pending == nil {
		t.Fatalf("keep planning should wait for feedback, calls=%d", rec.calls)
	}
	if !e.handlePendingPermission(p, planMsg("owner", "split step 2"), "split step 2", "") {
		t.Fatal("expected handled")
	}
	if rec.calls != 1 || rec.lastResult.Behavior != "deny" || !strings.Contains(rec.lastResult.Message, "split step 2") {
		t.Fatalf("expected deny with feedback, got %+v", rec.lastResult)
	}
}

func TestPlanReview_DirectFeedback(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}
	newPlanPendingState(e, "test:chat", p, rec)

	text := "no, use sqlite instead of postgres"
	e.handlePendingPermission(p, planMsg("owner", text), text, "")
	if rec.lastResult.Behavior != "deny" || !strings.Contains(rec.lastResult.Message, text) {
		t.Fatalf("expected deny carrying feedback, got %+v", rec.lastResult)
	}
}

func TestPendingPrompt_ForeignUserCannotAnswer(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}
	state := newPlanPendingState(e, "test:chat", p, rec)

	// Typed text from another member is not consumed.
	if e.handlePendingPermission(p, planMsg("other", "1"), "1", "") {
		t.Fatal("foreign text should fall through")
	}
	// A button click from another member is rejected.
	click := planMsg("other", "askq:0:1")
	click.IsPermissionResponse = true
	if !e.handlePendingPermission(p, click, "askq:0:1", "") {
		t.Fatal("foreign click should be consumed")
	}
	if rec.calls != 0 || state.pending == nil {
		t.Fatal("foreign responder must not resolve the prompt")
	}
}

func TestHandlePendingPermission_NumericShortcutsAndDenyReason(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}
	mk := func() {
		e.interactiveMu.Lock()
		e.interactiveStates["test:chat"] = &interactiveState{
			agentSession: rec, platform: p, replyCtx: "ctx",
			pending: &pendingPermission{RequestID: "r", ToolName: "Bash", Resolved: make(chan struct{})},
		}
		e.interactiveMu.Unlock()
	}

	mk()
	e.handlePendingPermission(p, planMsg("u", "1"), "1", "")
	if rec.lastResult.Behavior != "allow" {
		t.Fatalf("1 should allow, got %+v", rec.lastResult)
	}
	mk()
	e.handlePendingPermission(p, planMsg("u", "deny, use rg"), "deny, use rg", "")
	if rec.lastResult.Behavior != "deny" || !strings.Contains(rec.lastResult.Message, "use rg") {
		t.Fatalf("deny reason should be forwarded, got %+v", rec.lastResult)
	}
}

func TestResolveAskQuestionAnswer_MultiSelectCallback(t *testing.T) {
	e := newTestEngine()
	q := UserQuestion{Question: "Pick", MultiSelect: true, Options: []UserQuestionOption{{Label: "A"}, {Label: "B"}, {Label: "C"}}}
	if got := e.resolveAskQuestionAnswer(q, "askq:0:1,3"); got != "A, C" {
		t.Fatalf("got %q", got)
	}
}

func TestHasPendingInteraction_WorkspacePrefixedKey(t *testing.T) {
	e := newTestEngine()
	e.interactiveMu.Lock()
	e.interactiveStates["/ws/a:qq:g:1"] = &interactiveState{pending: &pendingPermission{Resolved: make(chan struct{})}}
	e.interactiveStates["qq:g:2"] = &interactiveState{}
	e.interactiveMu.Unlock()

	if !e.hasPendingInteraction("qq:g:1") {
		t.Fatal("expected pending for workspace-prefixed key")
	}
	if e.hasPendingInteraction("qq:g:2") {
		t.Fatal("no pending expected")
	}
}

type hintPlatform struct{ stubPlatformEngine }

func (h *hintPlatform) InteractionReplyHint(any) string { return "mention me" }

func TestSendPlanPrompt_PlainIncludesMenuAndHint(t *testing.T) {
	e := newTestEngine()
	p := &hintPlatform{stubPlatformEngine{n: "plain"}}
	e.sendPlanPrompt(p, "ctx", "Step one\nStep two", true)

	all := strings.Join(p.sent, "\n")
	for _, want := range []string{"Step two", "1. Approve, auto-accept edits", "3. Keep planning", "4. Approve, bypass all permissions", "mention me"} {
		if !strings.Contains(all, want) {
			t.Errorf("plan prompt missing %q:\n%s", want, all)
		}
	}
}

// liveModeRecordingSession records RespondPermission and SetLiveMode calls.
type liveModeRecordingSession struct {
	recordingAgentSession
	liveModes []string
}

func (s *liveModeRecordingSession) SetLiveMode(mode string) bool {
	s.liveModes = append(s.liveModes, mode)
	return true
}

func TestPlanReview_ApproveBypass(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &liveModeRecordingSession{}
	newPlanPendingState(e, "test:chat", p, rec)

	if !e.handlePendingPermission(p, planMsg("owner", "4"), "4", "") {
		t.Fatal("expected handled")
	}
	if rec.lastResult.Behavior != "allow" || rec.lastResult.UpdatedPermissions[0]["mode"] != "acceptEdits" {
		t.Fatalf("expected allow leaving plan mode via acceptEdits, got %+v", rec.lastResult)
	}
	if len(rec.liveModes) != 1 || rec.liveModes[0] != "bypassPermissions" {
		t.Fatalf("expected live switch to bypassPermissions, got %v", rec.liveModes)
	}
}

func TestPlanReview_BypassUnavailableKeepsPending(t *testing.T) {
	e := newTestEngine()
	p := &stubPlatformEngine{n: "test"}
	rec := &recordingAgentSession{}
	state := newPlanPendingState(e, "test:chat", p, rec)

	e.handlePendingPermission(p, planMsg("owner", "4"), "4", "")
	if rec.calls != 0 || state.pending == nil {
		t.Fatal("bypass on a non-switchable session must not resolve the plan")
	}
}
