package core

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// DefaultMaxAttachmentSize is the default per-attachment size limit (50 MiB)
// applied by the /send API and `agent-bridge send` when max_attachment_size_mb
// is unset. Exported so cmd/agent-bridge can resolve the same default.
const DefaultMaxAttachmentSize int64 = 50 << 20

// APIServer exposes a local Unix socket API for external messaging tools
// to send messages to active sessions.
type APIServer struct {
	socketPath string
	listener   net.Listener
	server     *http.Server
	mux        *http.ServeMux
	engines    map[string]*Engine // project name → engine
	// maxAttachmentBytes caps the raw size of a single attachment accepted by
	// /send; the request body limit in handleSend is derived from it (base64
	// expansion + envelope). Defaults to DefaultMaxAttachmentSize.
	maxAttachmentBytes int64
	mu                 sync.RWMutex
}

// SendRequest is the JSON body for POST /send.
//
// Audios and Videos are kept separate from Files so the engine can
// dispatch them to AudioSender / VideoSender (native voice / video
// bubble) instead of FileSender (generic file download). The fields
// reuse FileAttachment as the wire format because audio/video clips
// are byte blobs with a name + mime — the dedicated typing happens at
// the dispatch layer in engine.go. See agent-bridge internal task
// t-20260615-cqjbk1.
type SendRequest struct {
	Project    string            `json:"project"`
	SessionKey string            `json:"session_key"`
	Message    string            `json:"message,omitempty"` // caption; only sent together with images/files
	WorkDir    string            `json:"work_dir,omitempty"`
	CWD        string            `json:"cwd,omitempty"`
	TTSText    string            `json:"tts_text,omitempty"`
	Images     []ImageAttachment `json:"images,omitempty"`
	Files      []FileAttachment  `json:"files,omitempty"`
	Audios     []FileAttachment  `json:"audios,omitempty"`
	Videos     []FileAttachment  `json:"videos,omitempty"`
}

// NewAPIServer creates an API server on a Unix socket.
func NewAPIServer(dataDir string) (*APIServer, error) {
	sockDir := filepath.Join(dataDir, "run")
	if err := os.MkdirAll(sockDir, 0o755); err != nil {
		return nil, fmt.Errorf("create run dir: %w", err)
	}
	sockPath := filepath.Join(sockDir, "api.sock")

	// Remove stale socket
	os.Remove(sockPath)

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen unix socket: %w", err)
	}
	if err := os.Chmod(sockPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}

	s := &APIServer{
		socketPath:         sockPath,
		listener:           listener,
		mux:                http.NewServeMux(),
		engines:            make(map[string]*Engine),
		maxAttachmentBytes: DefaultMaxAttachmentSize,
	}
	s.mux.HandleFunc("/send", s.handleSend)
	s.mux.HandleFunc("/sessions", s.handleSessions)

	return s, nil
}

func (s *APIServer) RegisterEngine(name string, e *Engine) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engines[name] = e
}

func (s *APIServer) SetMaxAttachmentSize(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if bytes > 0 {
		s.maxAttachmentBytes = bytes
	}
}

// sendBodyEnvelope is the slack added on top of the base64-expanded attachment
// limit when sizing the /send request body: it covers the JSON envelope (field
// names, message text, metadata) and a few sub-limit attachments.
const sendBodyEnvelope int64 = 8 << 20 // 8 MiB

// sendBodyLimit returns the maximum accepted /send request body size in bytes.
// It is derived from the per-attachment limit to accommodate base64 expansion
// (~4/3) plus envelope slack, falling back to DefaultMaxAttachmentSize when no
// limit has been set (e.g. APIServer zero value in tests). Callers in hot paths
// (handleSend) run concurrently with SetMaxAttachmentSize, so the read is
// guarded by s.mu.
func (s *APIServer) sendBodyLimit() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	limit := s.maxAttachmentBytes
	if limit <= 0 {
		limit = DefaultMaxAttachmentSize
	}
	return limit*4/3 + sendBodyEnvelope
}

func (s *APIServer) Start() {
	s.server = &http.Server{Handler: s.mux}
	go func() {
		if err := s.server.Serve(s.listener); err != nil && err != http.ErrServerClosed {
			slog.Error("api server error", "error", err)
		}
	}()
	slog.Info("api server started", "socket", s.socketPath)
}

func (s *APIServer) Stop() {
	if s.server != nil {
		if err := s.server.Close(); err != nil && err != http.ErrServerClosed {
			slog.Debug("api server close failed", "error", err)
		}
	}
	if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
		slog.Debug("api server remove socket failed", "error", err)
	}
}

func apiJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("api server: write JSON failed", "error", err)
	}
}

func (s *APIServer) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	// Attachments travel base64-encoded inside the JSON body (~4/3 expansion)
	// plus the request envelope, so size the reader to fit one max-size
	// attachment with overhead to spare. The previous hard-coded 52 MB cap was
	// smaller than a single 50 MB attachment after base64 encoding and would
	// reject valid sends; deriving it from maxAttachmentBytes keeps the body
	// limit in step with the configured attachment limit.
	var req SendRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, s.sendBodyLimit())).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.TTSText) == "" && len(req.Images) == 0 && len(req.Files) == 0 && len(req.Audios) == 0 && len(req.Videos) == 0 {
		http.Error(w, "tts_text or attachment is required", http.StatusBadRequest)
		return
	}
	if req.Message != "" && len(req.Images) == 0 && len(req.Files) == 0 {
		http.Error(w, "message must accompany an image or file attachment; plain text sends are not supported", http.StatusBadRequest)
		return
	}

	s.mu.RLock()
	var engine *Engine
	var ok bool
	if req.Project != "" {
		engine, ok = s.engines[req.Project]
	} else if len(s.engines) == 1 {
		// No project specified and only one engine: use it by default.
		// Do NOT silently fall back when a non-empty project name is unknown —
		// that misroutes the message to the wrong engine. Mirrors the resolve
		// pattern in webhook.go.
		for _, e := range s.engines {
			engine = e
			ok = true
		}
	}
	s.mu.RUnlock()

	if !ok {
		if req.Project == "" {
			http.Error(w, "project is required (multiple projects configured)", http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("project %q not found", req.Project), http.StatusNotFound)
		return
	}

	workDir := req.WorkDir
	if workDir == "" {
		workDir = req.CWD
	}
	if len(req.Images) > 0 || len(req.Files) > 0 {
		if err := engine.SendToSessionWithOptions(req.SessionKey, req.Message, req.Images, req.Files, SendOptions{WorkDir: workDir}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if len(req.Audios) > 0 {
		if err := engine.SendAudiosToSession(req.SessionKey, req.Audios); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if len(req.Videos) > 0 {
		if err := engine.SendVideosToSession(req.SessionKey, req.Videos); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if strings.TrimSpace(req.TTSText) != "" {
		if err := engine.SendTTSToSession(req.SessionKey, req.TTSText); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	apiJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *APIServer) handleSessions(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	type sessionInfo struct {
		Project    string `json:"project"`
		SessionKey string `json:"session_key"`
		Platform   string `json:"platform"`
	}

	var result []sessionInfo
	for name, e := range s.engines {
		e.interactiveMu.Lock()
		for key, state := range e.interactiveStates {
			if state.platform != nil {
				result = append(result, sessionInfo{
					Project:    name,
					SessionKey: key,
					Platform:   state.platform.Name(),
				})
			}
		}
		e.interactiveMu.Unlock()
	}

	apiJSON(w, http.StatusOK, result)
}
