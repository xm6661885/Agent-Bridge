package core

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
)

// Interactive agent prompts (AskUserQuestion, ExitPlanMode plan review and
// tool permission requests) share one reply protocol so that platforms
// without buttons (QQ OneBot, Weixin) work the same way as Telegram/Feishu:
//
//   - buttons send "askq:<question>:<option>" (or "perm:*"), which the engine
//     treats exactly like the typed number;
//   - a typed number picks an option, "1,3" picks several for multi-select;
//   - any other text is a free-form answer (questions) or revision feedback
//     (plans);
//   - /stop cancels the whole turn.
//
// Only the user who started the turn may answer; messages from other group
// members fall through to normal handling (queued as the next message).

// Plan review choices, matching Claude Code's own ExitPlanMode dialog.
const (
	planChoiceAutoEdits = 1 // approve, auto-accept edits
	planChoiceReview    = 2 // approve, confirm each edit
	planChoiceRevise    = 3 // keep planning
	planChoiceBypass    = 4 // approve, bypass all permissions (live-mode agents only)
)

// InteractionReplyHinter is an optional platform interface. Platforms whose
// group chats only deliver messages that mention the bot return a short hint
// that is appended to prompts expecting a typed reply. Return "" when no hint
// is needed for this conversation.
type InteractionReplyHinter interface {
	InteractionReplyHint(replyCtx any) string
}

// MultiSelectButtonSender is an optional marker for InlineButtonSender
// platforms that handle "askqt:<q>:<opt>" (toggle, handled locally) and
// "askqd:<q>" (done, forwarded as "askq:<q>:<opt>,<opt>...") callbacks, so a
// multi-select question can be answered by tapping several buttons.
type MultiSelectButtonSender interface {
	SupportsMultiSelectButtons() bool
}

func (e *Engine) interactionHint(p Platform, replyCtx any) string {
	if h, ok := p.(InteractionReplyHinter); ok {
		if hint := strings.TrimSpace(h.InteractionReplyHint(replyCtx)); hint != "" {
			return "\n\n_" + hint + "_"
		}
	}
	return ""
}

// isForeignResponder reports whether msg comes from someone other than the
// user whose turn raised the pending prompt.
func isForeignResponder(pending *pendingPermission, msg *Message) bool {
	if pending.OwnerUserID == "" || msg.UserID == "" || msg.UserID == "web-admin" {
		return false
	}
	return msg.UserID != pending.OwnerUserID
}

// parseAskqCallback parses "askq:<qIdx>:<opts>" button data. opts is either
// one 1-based option index or a comma-separated list for multi-select.
func parseAskqCallback(s string) (qIdx int, opts []int, ok bool) {
	parts := strings.SplitN(strings.TrimSpace(s), ":", 3)
	if len(parts) != 3 || parts[0] != "askq" {
		return 0, nil, false
	}
	q, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, nil, false
	}
	for _, f := range strings.Split(parts[2], ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil {
			return 0, nil, false
		}
		opts = append(opts, n)
	}
	return q, opts, len(opts) > 0
}

// hasPendingInteraction reports whether any interactive state for sessionKey
// (including workspace-prefixed keys) is waiting for a user answer. Used to
// let answers skip a platform serial queue that is blocked by the very turn
// waiting on them.
func (e *Engine) hasPendingInteraction(sessionKey string) bool {
	if sessionKey == "" {
		return false
	}
	var states []*interactiveState
	e.interactiveMu.Lock()
	for k, st := range e.interactiveStates {
		if st != nil && (k == sessionKey || strings.HasSuffix(k, ":"+sessionKey)) {
			states = append(states, st)
		}
	}
	e.interactiveMu.Unlock()
	for _, st := range states {
		st.mu.Lock()
		pending := st.pending != nil
		st.mu.Unlock()
		if pending {
			return true
		}
	}
	return false
}

// sendPlanPrompt renders an ExitPlanMode plan for review. allowBypass adds
// the "bypass all permissions" choice for sessions that can switch mode live.
func (e *Engine) sendPlanPrompt(p Platform, replyCtx any, plan string, allowBypass bool) {
	plan = strings.TrimSpace(plan)
	options := []string{
		e.i18n.T(MsgPlanOptAutoEdits),
		e.i18n.T(MsgPlanOptReview),
		e.i18n.T(MsgPlanOptRevise),
	}
	if allowBypass {
		options = append(options, e.i18n.T(MsgPlanOptBypass))
	}

	e.hooks.Emit(HookEvent{
		Event:    HookEventPermissionRequested,
		Platform: p.Name(),
		Content:  plan,
		Extra:    map[string]any{"tool_name": "ExitPlanMode"},
	})

	if supportsCards(p) {
		cb := NewCard().Title(e.i18n.T(MsgPlanTitle), "blue").Markdown(plan)
		for i, opt := range options {
			cb.ListItemBtnExtra(opt, opt, "default", fmt.Sprintf("askq:0:%d", i+1), map[string]string{
				"askq_label":    opt,
				"askq_question": e.i18n.T(MsgPlanTitle),
			})
		}
		cb.Note(e.i18n.T(MsgPlanNote))
		e.sendWithCard(p, replyCtx, cb.Build())
		return
	}

	header := "**" + e.i18n.T(MsgPlanTitle) + "**\n\n"
	hint := e.interactionHint(p, replyCtx)

	if bs, ok := p.(InlineButtonSender); ok {
		var rows [][]ButtonOption
		for i, opt := range options {
			rows = append(rows, []ButtonOption{{Text: opt, Data: fmt.Sprintf("askq:0:%d", i+1)}})
		}
		if err := e.waitOutgoing(p); err != nil {
			slog.Warn("sendPlanPrompt: outgoing wait cancelled", "platform", p.Name(), "error", err)
			return
		}
		body := header + plan + "\n\n" + e.i18n.T(MsgPlanNote) + hint
		if err := bs.SendWithButtons(e.ctx, replyCtx, body, rows); err == nil {
			return
		} else {
			slog.Warn("sendPlanPrompt: inline buttons failed, falling back", "error", err)
		}
	}

	// Plain text: the plan itself (split if long), then the reply menu.
	for _, chunk := range SplitMessageCodeFenceAware(header+plan, maxPlatformMessageLen) {
		e.send(p, replyCtx, chunk)
	}
	var sb strings.Builder
	sb.WriteString(e.i18n.T(MsgPlanReplyWith))
	sb.WriteString("\n")
	for i, opt := range options {
		fmt.Fprintf(&sb, "%d. %s\n", i+1, opt)
	}
	sb.WriteString("\n")
	sb.WriteString(e.i18n.T(MsgPlanNote))
	sb.WriteString(hint)
	e.send(p, replyCtx, sb.String())
}

// parsePlanChoice maps a reply to a plan choice. Returns 0 when the reply is
// free text (revision feedback).
func parsePlanChoice(content string) int {
	if q, opts, ok := parseAskqCallback(content); ok && q == 0 && len(opts) == 1 {
		return opts[0]
	}
	trimmed := strings.TrimSpace(content)
	if n, err := strconv.Atoi(trimmed); err == nil && n >= planChoiceAutoEdits && n <= planChoiceBypass {
		return n
	}
	// Bare yes/no words; longer sentences are treated as feedback so that
	// "no, use sqlite instead" is not reduced to a plain rejection.
	if len(splitPermissionTokens(trimmed)) <= 2 {
		lower := strings.ToLower(trimmed)
		switch {
		case isApproveAllResponse(lower):
			return planChoiceAutoEdits
		case isAllowResponse(lower):
			return planChoiceReview
		case isDenyResponse(lower):
			return planChoiceRevise
		}
	}
	return 0
}

// handlePlanResponse processes a reply to a pending ExitPlanMode review.
func (e *Engine) handlePlanResponse(p Platform, msg *Message, state *interactiveState, pending *pendingPermission, content string) bool {
	if strings.TrimSpace(content) == "" {
		return false
	}
	choice := parsePlanChoice(content)
	if strings.HasPrefix(strings.TrimSpace(content), "askq:") && (choice < planChoiceAutoEdits || choice > planChoiceBypass) {
		return true // malformed or stale button
	}
	switcher, canSwitch := state.agentSession.(LiveModeSwitcher)
	if choice == planChoiceBypass && !canSwitch {
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPlanBypassUnavailable))
		return true
	}

	state.mu.Lock()
	awaitingFeedback := pending.awaitingFeedback
	state.mu.Unlock()

	var result PermissionResult
	var ack string
	switch {
	case choice == planChoiceAutoEdits || choice == planChoiceReview || choice == planChoiceBypass:
		mode := "default"
		ack = e.i18n.T(MsgPlanApprovedReview)
		switch choice {
		case planChoiceAutoEdits:
			mode = "acceptEdits"
			ack = e.i18n.T(MsgPlanApprovedAutoEdits)
		case planChoiceBypass:
			// Claude only accepts setMode bypassPermissions when it was
			// launched with it, so leave plan mode via acceptEdits and let
			// the bridge session auto-approve everything else (below).
			mode = "acceptEdits"
			ack = e.i18n.T(MsgPlanApprovedBypass)
		}
		result = PermissionResult{
			Behavior:     "allow",
			UpdatedInput: pending.ToolInput,
			UpdatedPermissions: []map[string]any{
				{"type": "setMode", "mode": mode, "destination": "session"},
			},
		}
	case choice == planChoiceRevise && !awaitingFeedback:
		state.mu.Lock()
		pending.awaitingFeedback = true
		state.mu.Unlock()
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgPlanAskFeedback)+e.interactionHint(p, msg.ReplyCtx))
		return true
	case choice == planChoiceRevise || strings.EqualFold(strings.TrimSpace(content), "skip"):
		result = PermissionResult{
			Behavior: "deny",
			Message:  "The user rejected the plan and wants to keep planning. Refine the plan, asking clarifying questions if needed, then present it again with ExitPlanMode.",
		}
		ack = e.i18n.T(MsgPlanKeepPlanning)
	default:
		result = PermissionResult{
			Behavior: "deny",
			Message:  "The user rejected the plan with this feedback:\n\n" + strings.TrimSpace(content) + "\n\nRevise the plan accordingly and present it again with ExitPlanMode.",
		}
		ack = e.i18n.T(MsgPlanFeedbackSent)
	}

	if err := state.agentSession.RespondPermission(pending.RequestID, result); err != nil {
		slog.Error("failed to send plan review response", "error", err)
		e.reply(p, msg.ReplyCtx, fmt.Sprintf(e.i18n.T(MsgError), err))
	} else {
		if choice == planChoiceBypass && !switcher.SetLiveMode("bypassPermissions") {
			slog.Warn("plan review: live switch to bypassPermissions refused")
			ack = e.i18n.T(MsgPlanApprovedAutoEdits)
		}
		e.reply(p, msg.ReplyCtx, ack)
	}

	state.mu.Lock()
	state.pending = nil
	state.mu.Unlock()
	pending.resolve()
	return true
}
