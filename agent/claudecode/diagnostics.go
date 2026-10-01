package claudecode

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"agent-bridge/core"
)

var claudeANSI = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

func cleanClaudeDiagnostic(s string) string {
	return strings.TrimSpace(claudeANSI.ReplaceAllString(s, ""))
}

func isClaudeDiagnostic(s string) bool {
	s = strings.ToLower(cleanClaudeDiagnostic(s))
	for _, phrase := range []string{"api error", "retrying", "retry in", "waiting for network", "waiting network", "network error", "connection error", "reconnecting", "unable to connect", "request timed out"} {
		if strings.Contains(s, phrase) {
			return true
		}
	}
	return false
}

// claudeDiagnosticWriter retains bounded stderr for terminal failures and emits
// complete live diagnostics, including CLI progress lines terminated by CR.
type claudeDiagnosticWriter struct {
	mu       sync.Mutex
	tail     string
	pending  string
	reported string
	emit     func(string)
}

func (w *claudeDiagnosticWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.tail += string(p)
	if len(w.tail) > 32*1024 {
		w.tail = w.tail[len(w.tail)-32*1024:]
	}
	w.pending += string(p)
	var messages []string
	for {
		i := strings.IndexAny(w.pending, "\r\n")
		if i < 0 {
			break
		}
		line := cleanClaudeDiagnostic(w.pending[:i])
		w.pending = w.pending[i+1:]
		if isClaudeDiagnostic(line) && line != w.reported {
			messages = append(messages, line)
		}
		w.reported = ""
	}
	// Some CLI versions write their waiting status without a newline.
	if line := cleanClaudeDiagnostic(w.pending); isClaudeDiagnostic(line) && line != w.reported {
		messages = append(messages, line)
		w.reported = line
	}
	if len(w.pending) > 4096 {
		w.pending = w.pending[len(w.pending)-4096:]
	}
	emit := w.emit
	w.mu.Unlock()
	if emit != nil {
		for _, message := range messages {
			emit(message)
		}
	}
	return len(p), nil
}

func (w *claudeDiagnosticWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return cleanClaudeDiagnostic(w.tail)
}

func (cs *claudeSession) emitStatus(content string) {
	if content == "" {
		return
	}
	select {
	case cs.events <- core.Event{Type: core.EventStatus, Content: content}:
	case <-cs.ctx.Done():
	}
}

func claudeSystemDiagnostic(raw map[string]any) string {
	subtype, _ := raw["subtype"].(string)
	if subtype == "api_retry" {
		message := "API error"
		if status, ok := raw["error_status"].(float64); ok {
			message += fmt.Sprintf(" (HTTP %d)", int(status))
		}
		if detail, ok := raw["error"].(string); ok && detail != "" {
			message += ": " + detail
		}
		if delay, ok := raw["retry_delay_ms"].(float64); ok {
			message += fmt.Sprintf("; retrying in %g s", delay/1000)
		} else {
			message += "; retrying"
		}
		if attempt, ok := raw["attempt"].(float64); ok {
			message += fmt.Sprintf(" (attempt %d", int(attempt))
			if max, ok := raw["max_retries"].(float64); ok {
				message += fmt.Sprintf("/%d", int(max))
			}
			message += ")"
		}
		return message
	}
	if subtype == "status" || subtype == "error" || subtype == "network_status" {
		for _, key := range []string{"message", "status", "error"} {
			if value, ok := raw[key].(string); ok {
				value = strings.ReplaceAll(value, "_", " ")
				if isClaudeDiagnostic(value) {
					return cleanClaudeDiagnostic(value)
				}
			}
		}
	}
	return ""
}
