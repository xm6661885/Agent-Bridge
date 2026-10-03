package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	weixinDefaultAPIURL = "https://ilinkai.weixin.qq.com"
)

// ── Request types for setup save callbacks ──────────────────

type WeixinSetupSaveRequest struct {
	ProjectName string `json:"project"`
	Token       string `json:"token"`
	BaseURL     string `json:"base_url"`
	IlinkBotID  string `json:"ilink_bot_id"`
	IlinkUserID string `json:"ilink_user_id"`
	WorkDir     string `json:"work_dir"`
	AgentType   string `json:"agent_type"`
}

// ── Weixin (ilink) QR Setup ─────────────────────────────────

func (m *ManagementServer) handleSetupWeixinBegin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		mgmtError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req struct {
		APIURL string `json:"api_url"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	apiBase := weixinDefaultAPIURL
	if req.APIURL != "" {
		apiBase = strings.TrimRight(req.APIURL, "/")
	}

	u, err := url.Parse(apiBase + "/")
	if err != nil {
		mgmtError(w, http.StatusBadRequest, "invalid api_url")
		return
	}
	u = u.JoinPath("ilink", "bot", "get_bot_qrcode")
	q := u.Query()
	q.Set("bot_type", "3")
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		mgmtError(w, http.StatusInternalServerError, err.Error())
		return
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		mgmtError(w, http.StatusBadGateway, "weixin get_bot_qrcode: "+err.Error())
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		mgmtError(w, http.StatusBadGateway, fmt.Sprintf("weixin get_bot_qrcode: http %d", resp.StatusCode))
		return
	}

	var qrResp struct {
		QRCode           string `json:"qrcode"`
		QRCodeImgContent string `json:"qrcode_img_content"`
	}
	if err := json.Unmarshal(body, &qrResp); err != nil {
		mgmtError(w, http.StatusBadGateway, "weixin decode: "+err.Error())
		return
	}
	if qrResp.QRCodeImgContent == "" {
		slog.Warn("weixin begin: empty qrcode_img_content", "raw_body", string(body))
		mgmtError(w, http.StatusBadGateway, "weixin: empty qrcode_img_content")
		return
	}

	slog.Info("weixin begin: QR generated",
		"qr_key", qrResp.QRCode,
		"qr_url_len", len(qrResp.QRCodeImgContent),
		"qr_url_prefix", truncateStr(strings.TrimSpace(qrResp.QRCodeImgContent), 80),
	)

	mgmtJSON(w, http.StatusOK, map[string]any{
		"qr_key": qrResp.QRCode,
		"qr_url": strings.TrimSpace(qrResp.QRCodeImgContent),
	})
}

func (m *ManagementServer) handleSetupWeixinPoll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		mgmtError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}

	var req struct {
		QRKey  string `json:"qr_key"`
		APIURL string `json:"api_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mgmtError(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if req.QRKey == "" {
		mgmtError(w, http.StatusBadRequest, "qr_key required")
		return
	}

	apiBase := weixinDefaultAPIURL
	if req.APIURL != "" {
		apiBase = strings.TrimRight(req.APIURL, "/")
	}

	// Validate api_url before dereferencing — url.Parse returns a nil URL for
	// inputs like "://" or "%zz", and the previous `u, _ := url.Parse(...)`
	// then crashed the management server with a nil-pointer panic on the
	// next u.JoinPath call. handleSetupWeixinBegin already validates the
	// same field; mirror that here.
	u, err := url.Parse(apiBase + "/")
	if err != nil {
		mgmtError(w, http.StatusBadRequest, "invalid api_url")
		return
	}
	u = u.JoinPath("ilink", "bot", "get_qrcode_status")
	q := u.Query()
	q.Set("qrcode", req.QRKey)
	u.RawQuery = q.Encode()

	ctx, cancel := context.WithTimeout(r.Context(), 37*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		mgmtError(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpReq.Header.Set("iLink-App-ClientVersion", "1")

	slog.Info("weixin poll: calling ilink", "url", u.String(), "qr_key", req.QRKey)

	client := &http.Client{Timeout: 40 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		slog.Warn("weixin poll: request error", "error", err)
		mgmtJSON(w, http.StatusOK, map[string]any{"status": "wait"})
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	slog.Info("weixin poll: ilink raw response",
		"http_status", resp.StatusCode,
		"body", truncateStr(string(body), 500),
	)

	if resp.StatusCode != http.StatusOK {
		mgmtError(w, http.StatusBadGateway, fmt.Sprintf("weixin poll: http %d", resp.StatusCode))
		return
	}

	var status struct {
		Status      string `json:"status"`
		BotToken    string `json:"bot_token"`
		IlinkBotID  string `json:"ilink_bot_id"`
		BaseURL     string `json:"baseurl"`
		IlinkUserID string `json:"ilink_user_id"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		slog.Warn("weixin poll: JSON decode failed", "error", err, "body", truncateStr(string(body), 300))
		mgmtError(w, http.StatusBadGateway, "weixin decode: "+err.Error())
		return
	}

	slog.Info("weixin poll: parsed status", "status", status.Status)

	result := map[string]any{"status": status.Status}
	if status.Status == "" {
		slog.Warn("weixin poll: empty status field, raw body", "body", truncateStr(string(body), 500))
		result["status"] = "wait"
	}

	if status.Status == "confirmed" {
		result["bot_token"] = strings.TrimSpace(status.BotToken)
		result["ilink_bot_id"] = strings.TrimSpace(status.IlinkBotID)
		result["base_url"] = strings.TrimSpace(status.BaseURL)
		result["ilink_user_id"] = strings.TrimSpace(status.IlinkUserID)
	}

	mgmtJSON(w, http.StatusOK, result)
}

func (m *ManagementServer) handleSetupWeixinSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		mgmtError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var req WeixinSetupSaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mgmtError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.ProjectName == "" || req.Token == "" {
		mgmtError(w, http.StatusBadRequest, "project and token required")
		return
	}
	workDir, err := validateProjectWorkDir(req.WorkDir)
	if err != nil {
		mgmtError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.WorkDir = workDir
	if m.setupWeixinSave == nil {
		mgmtError(w, http.StatusServiceUnavailable, "weixin setup save not configured")
		return
	}
	if err := m.setupWeixinSave(req); err != nil {
		mgmtError(w, http.StatusInternalServerError, "save: "+err.Error())
		return
	}
	mgmtJSON(w, http.StatusOK, map[string]any{
		"message":          fmt.Sprintf("weixin platform configured for project %q", req.ProjectName),
		"restart_required": true,
	})
}

// ── Generic platform add (manual config) ─────────────────────

type AddPlatformRequest struct {
	Type      string         `json:"type"`
	Options   map[string]any `json:"options"`
	WorkDir   string         `json:"work_dir"`
	AgentType string         `json:"agent_type"`
}

func (m *ManagementServer) handleProjectAddPlatform(w http.ResponseWriter, r *http.Request, projectName string) {
	if r.Method != http.MethodPost {
		mgmtError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var req AddPlatformRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		mgmtError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if req.Type == "" {
		mgmtError(w, http.StatusBadRequest, "type is required")
		return
	}
	workDir, err := validateProjectWorkDir(req.WorkDir)
	if err != nil {
		mgmtError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.WorkDir = workDir
	if m.addPlatformToProject == nil {
		mgmtError(w, http.StatusServiceUnavailable, "config persistence not available")
		return
	}
	if err := m.addPlatformToProject(projectName, req.Type, req.Options, req.WorkDir, req.AgentType); err != nil {
		mgmtError(w, http.StatusInternalServerError, "save config: "+err.Error())
		return
	}
	mgmtJSON(w, http.StatusCreated, map[string]any{
		"message":          fmt.Sprintf("platform %q added to project %q", req.Type, projectName),
		"restart_required": true,
	})
}

func validateProjectWorkDir(workDir string) (string, error) {
	trimmed := strings.TrimSpace(workDir)
	if trimmed == "" {
		return "", nil
	}

	info, err := os.Stat(trimmed)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("work_dir does not exist: %s", trimmed)
		}
		return "", fmt.Errorf("work_dir is not accessible: %s: %w", trimmed, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("work_dir is not a directory: %s", trimmed)
	}
	return trimmed, nil
}
