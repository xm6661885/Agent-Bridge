package core

import (
	"context"
	"log/slog"
)

type diagnosticMessage struct {
	platform Platform
	handle   any
}

// Diagnostic messages belong to one turn. Retain them on failure, cancellation,
// or failed answer delivery; delete them only after a successful final reply.
type diagnosticMessages struct {
	engine   *Engine
	messages []diagnosticMessage
}

func (d *diagnosticMessages) Send(p Platform, replyCtx any, content, workspaceDir string) {
	e := d.engine
	starter, canStart := p.(PreviewStarter)
	_, canDelete := p.(PreviewCleaner)
	if !e.display.CleanupProgressOnComplete || !canStart || !canDelete {
		e.sendForWorkspace(p, replyCtx, content, workspaceDir)
		return
	}
	if err := e.waitOutgoing(p); err != nil {
		return
	}
	content = e.renderOutgoingContentForWorkspace(p, content, workspaceDir)
	ctx, cancel := context.WithTimeout(e.ctx, compactProgressAPITimeout)
	handle, err := starter.SendPreviewStart(ctx, replyCtx, content)
	cancel()
	if err != nil || handle == nil {
		// A normal message remains visible if this channel cannot create a
		// deletable message. Never register final answers in this tracker.
		_ = e.sendAlreadyRenderedWithError(p, replyCtx, content)
		return
	}
	d.messages = append(d.messages, diagnosticMessage{platform: p, handle: handle})
}

func (d *diagnosticMessages) Finish(success bool) {
	messages := d.messages
	d.messages = nil
	if !success {
		return
	}
	for _, msg := range messages {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(d.engine.ctx), compactProgressAPITimeout)
		err := msg.platform.(PreviewCleaner).DeletePreviewMessage(ctx, msg.handle)
		cancel()
		if err != nil {
			slog.Warn("diagnostic cleanup failed", "platform", msg.platform.Name(), "error", err)
		}
	}
}
