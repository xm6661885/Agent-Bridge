package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

type deadlineAwareModelAgent struct {
	stubModelModeAgent
	mu          sync.Mutex
	hasDeadline bool
}

func (a *deadlineAwareModelAgent) AvailableModels(ctx context.Context) []ModelOption {
	a.mu.Lock()
	_, ok := ctx.Deadline()
	a.hasDeadline = ok
	a.mu.Unlock()
	return []ModelOption{{Name: "gpt-4.1"}}
}

func (a *deadlineAwareModelAgent) sawDeadline() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hasDeadline
}

// testManagementServer creates a ManagementServer with a test engine and returns an httptest.Server.
func testManagementServer(t *testing.T, token string) (*ManagementServer, *httptest.Server, *Engine) {
	t.Helper()

	agent := &stubAgent{}
	sm := NewSessionManager("")
	e := NewEngine("test-project", agent, nil, "")
	e.sessions = sm

	mgmt := NewManagementServer(0, token, nil)
	mgmt.RegisterEngine("test-project", e)

	mux := http.NewServeMux()
	prefix := "/api/v1"
	mux.HandleFunc(prefix+"/status", mgmt.wrap(mgmt.handleStatus))
	mux.HandleFunc(prefix+"/restart", mgmt.wrap(mgmt.handleRestart))
	mux.HandleFunc(prefix+"/reload", mgmt.wrap(mgmt.handleReload))
	mux.HandleFunc(prefix+"/config", mgmt.wrap(mgmt.handleConfig))
	mux.HandleFunc(prefix+"/settings", mgmt.wrap(mgmt.handleGlobalSettings))
	mux.HandleFunc(prefix+"/agents", mgmt.wrap(mgmt.handleAgents))
	mux.HandleFunc(prefix+"/projects", mgmt.wrap(mgmt.handleProjects))
	mux.HandleFunc(prefix+"/projects/", mgmt.wrap(mgmt.handleProjectRoutes))
	mux.HandleFunc(prefix+"/bridge/adapters", mgmt.wrap(mgmt.handleBridgeAdapters))

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return mgmt, ts, e
}

type mgmtResponse struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error string          `json:"error,omitempty"`
}

func mgmtGet(t *testing.T, url, token string) mgmtResponse {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	return r
}

func mgmtPost(t *testing.T, url, token string, body any) mgmtResponse {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode POST body: %v", err)
		}
	}
	req, _ := http.NewRequest("POST", url, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode POST response: %v", err)
	}
	return r
}

func mgmtPostHandler(t *testing.T, handler http.HandlerFunc, path string, body any) (mgmtResponse, int) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode POST body: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)

	var r mgmtResponse
	if err := json.NewDecoder(w.Body).Decode(&r); err != nil {
		t.Fatalf("decode POST handler response: %v", err)
	}
	return r, w.Code
}

func mgmtPatch(t *testing.T, url, token string, body any) mgmtResponse {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode PATCH body: %v", err)
		}
	}
	req, _ := http.NewRequest("PATCH", url, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PATCH %s: %v", url, err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode PATCH response: %v", err)
	}
	return r
}

func mgmtDelete(t *testing.T, url, token string) mgmtResponse {
	t.Helper()
	req, _ := http.NewRequest("DELETE", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", url, err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode DELETE response: %v", err)
	}
	return r
}

func TestMgmt_AuthRequired(t *testing.T) {
	_, ts, _ := testManagementServer(t, "secret-token")

	r := mgmtGet(t, ts.URL+"/api/v1/status", "")
	if r.OK {
		t.Fatal("expected auth failure without token")
	}
	if !strings.Contains(r.Error, "unauthorized") {
		t.Fatalf("expected unauthorized error, got: %s", r.Error)
	}

	r = mgmtGet(t, ts.URL+"/api/v1/status", "wrong-token")
	if r.OK {
		t.Fatal("expected auth failure with wrong token")
	}

	r = mgmtGet(t, ts.URL+"/api/v1/status", "secret-token")
	if !r.OK {
		t.Fatalf("expected success with correct token, got error: %s", r.Error)
	}
}

func TestMgmt_AuthQueryParam(t *testing.T) {
	_, ts, _ := testManagementServer(t, "qp-token")

	r := mgmtGet(t, ts.URL+"/api/v1/status?token=qp-token", "")
	if !r.OK {
		t.Fatalf("expected success with query param token, got: %s", r.Error)
	}
}

func TestMgmt_NoAuthRequired(t *testing.T) {
	_, ts, _ := testManagementServer(t, "")

	r := mgmtGet(t, ts.URL+"/api/v1/status", "")
	if !r.OK {
		t.Fatalf("expected success without token when no token configured, got: %s", r.Error)
	}
}

func TestMgmt_Status(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/status", "tok")
	if !r.OK {
		t.Fatalf("status failed: %s", r.Error)
	}

	var data map[string]any
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal status data: %v", err)
	}
	if data["projects_count"] != float64(1) {
		t.Fatalf("expected 1 project, got %v", data["projects_count"])
	}
}

func TestMgmt_StatusIncludesBridgeToken(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")
	mgmt.SetBridgeServer(NewBridgeServer(9810, "bridge-secret", "/bridge/ws", nil))

	r := mgmtGet(t, ts.URL+"/api/v1/status", "tok")
	if !r.OK {
		t.Fatalf("status failed: %s", r.Error)
	}

	var data struct {
		Bridge struct {
			Enabled bool   `json:"enabled"`
			Port    int    `json:"port"`
			Path    string `json:"path"`
			Token   string `json:"token"`
		} `json:"bridge"`
	}
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal status data: %v", err)
	}
	if !data.Bridge.Enabled {
		t.Fatal("expected bridge to be enabled")
	}
	if data.Bridge.Token != "bridge-secret" {
		t.Fatalf("expected bridge token, got %q", data.Bridge.Token)
	}
}

func TestMgmt_Projects(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/projects", "tok")
	if !r.OK {
		t.Fatalf("projects failed: %s", r.Error)
	}

	var data struct {
		Projects []map[string]any `json:"projects"`
	}
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal projects data: %v", err)
	}
	if len(data.Projects) != 1 {
		t.Fatalf("expected 1 project, got %d", len(data.Projects))
	}
	if data.Projects[0]["name"] != "test-project" {
		t.Fatalf("expected test-project, got %v", data.Projects[0]["name"])
	}
}

func TestMgmt_ProjectDetail(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project", "tok")
	if !r.OK {
		t.Fatalf("project detail failed: %s", r.Error)
	}

	var data map[string]any
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal project detail: %v", err)
	}
	if data["name"] != "test-project" {
		t.Fatalf("expected test-project, got %v", data["name"])
	}

	r = mgmtGet(t, ts.URL+"/api/v1/projects/nonexistent", "tok")
	if r.OK {
		t.Fatal("expected 404 for nonexistent project")
	}
}

func TestMgmt_ProjectPatch(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project", "tok", map[string]any{
		"reply_footer": true,
	})
	if !r.OK {
		t.Fatalf("patch failed: %s", r.Error)
	}
}

func TestMgmt_Sessions(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")

	e.sessions.GetOrCreateActive("user1")

	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/sessions", "tok")
	if !r.OK {
		t.Fatalf("sessions list failed: %s", r.Error)
	}

	// Create a session via API
	r = mgmtPost(t, ts.URL+"/api/v1/projects/test-project/sessions", "tok", map[string]string{
		"session_key": "user2",
		"name":        "work",
	})
	if !r.OK {
		t.Fatalf("create session failed: %s", r.Error)
	}
}

func TestMgmt_SessionDetail(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")

	s := e.sessions.GetOrCreateActive("user1")
	s.AddHistory("user", "hello")
	s.AddHistory("assistant", "hi there")

	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/sessions/"+s.ID, "tok")
	if !r.OK {
		t.Fatalf("session detail failed: %s", r.Error)
	}

	var data struct {
		History []map[string]any `json:"history"`
	}
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal session detail: %v", err)
	}
	if len(data.History) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(data.History))
	}
}

// TestMgmt_SessionsConcurrentNameWriteAndList pins the bug where the
// POST /sessions handler wrote s.Name = body.Name directly without
// holding s.mu, while the GET handler reads s.Name through s.mu.Lock().
// Run with -race to detect the data race; with the production fix
// the test stays clean.
func TestMgmt_SessionsConcurrentNameWriteAndList(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")

	// Pre-create the session so both handlers operate on the same instance.
	e.sessions.GetOrCreateActive("user1")

	listURL := ts.URL + "/api/v1/projects/test-project/sessions"
	postURL := listURL

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "a"
			if i%2 == 0 {
				name = "b"
			}
			_ = mgmtPost(t, postURL, "tok", map[string]string{
				"session_key": "user1",
				"name":        name,
			})
		}(i)
	}
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mgmtGet(t, listURL, "tok")
		}()
	}
	wg.Wait()
}

func TestMgmt_SessionDelete(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")

	s := e.sessions.GetOrCreateActive("user1")
	sid := s.ID

	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project/sessions/"+sid, "tok")
	if !r.OK {
		t.Fatalf("delete session failed: %s", r.Error)
	}

	r = mgmtGet(t, ts.URL+"/api/v1/projects/test-project/sessions/"+sid, "tok")
	if r.OK {
		t.Fatal("expected 404 after deletion")
	}
}

func TestMgmt_Config(t *testing.T) {
	srv, ts, _ := testManagementServer(t, "tok")

	// Write a temp TOML file and point the server at it
	tmp := t.TempDir()
	cfgPath := tmp + "/config.toml"
	if err := os.WriteFile(cfgPath, []byte("[display]\ntitle = \"test\"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	srv.SetConfigFilePath(cfgPath)

	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/config", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "title") {
		t.Fatalf("expected TOML content, got: %s", body)
	}
}

func TestMgmt_Reload(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")

	reloaded := false
	e.configReloadFunc = func() (*ConfigReloadResult, error) {
		reloaded = true
		return &ConfigReloadResult{}, nil
	}

	r := mgmtPost(t, ts.URL+"/api/v1/reload", "tok", nil)
	if !r.OK {
		t.Fatalf("reload failed: %s", r.Error)
	}
	if !reloaded {
		t.Fatal("expected config reload to be triggered")
	}
}

func TestMgmt_BridgeAdapters(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/bridge/adapters", "tok")
	if !r.OK {
		t.Fatalf("bridge adapters failed: %s", r.Error)
	}
}

func TestMgmt_CORS(t *testing.T) {
	mgmt := NewManagementServer(0, "", []string{"http://localhost:3000"})
	mgmt.RegisterEngine("p", NewEngine("p", &stubAgent{}, nil, ""))

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", mgmt.wrap(mgmt.handleStatus))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	req, _ := http.NewRequest("OPTIONS", ts.URL+"/api/v1/status", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 for OPTIONS, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatalf("expected CORS origin header, got %q", resp.Header.Get("Access-Control-Allow-Origin"))
	}
}

func TestMgmt_BridgeWebSocketPathProxiesToBridgeServer(t *testing.T) {
	mgmt := NewManagementServer(0, "", []string{"*"})
	mgmt.RegisterEngine("p", NewEngine("p", &stubAgent{}, nil, ""))
	mgmt.SetBridgeServer(NewBridgeServer(9810, "bridge-secret", "/bridge/ws", []string{"*"}))

	mux := http.NewServeMux()
	ts := httptest.NewServer(mgmt.buildHandler(mux))
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/bridge/ws?token=bridge-secret", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected websocket upgrade, got %d", resp.StatusCode)
	}
}

func TestMgmt_BridgeWebSocketPathWorksWhenBridgeServerSetAfterHandlerBuild(t *testing.T) {
	mgmt := NewManagementServer(0, "", []string{"*"})
	mgmt.RegisterEngine("p", NewEngine("p", &stubAgent{}, nil, ""))

	mux := http.NewServeMux()
	ts := httptest.NewServer(mgmt.buildHandler(mux))
	defer ts.Close()

	mgmt.SetBridgeServer(NewBridgeServer(9810, "bridge-secret", "/bridge/ws", []string{"*"}))

	req, _ := http.NewRequest("GET", ts.URL+"/bridge/ws?token=bridge-secret", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected websocket upgrade after late bridge setup, got %d", resp.StatusCode)
	}
}

func TestMgmt_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/status", nil)
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	var r mgmtResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode method not allowed response: %v", err)
	}
	resp.Body.Close()
}

func TestMgmt_ProjectModel_SavesModel(t *testing.T) {
	agent := &stubModelModeAgent{
		model: "gpt-4.1-mini",
	}
	e := NewEngine("test-project", agent, nil, "")
	var savedModel string
	e.SetModelSaveFunc(func(model string) error {
		savedModel = model
		return nil
	})

	mgmt := NewManagementServer(0, "tok", nil)
	mgmt.RegisterEngine("test-project", e)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/projects/", mgmt.wrap(mgmt.handleProjectRoutes))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/model", "tok", map[string]string{"model": "gpt-4.1"})
	if !r.OK {
		t.Fatalf("update model failed: %s", r.Error)
	}

	if got := agent.GetModel(); got != "gpt-4.1" {
		t.Fatalf("GetModel() = %q, want gpt-4.1", got)
	}
	if savedModel != "gpt-4.1" {
		t.Fatalf("saved model = %q, want gpt-4.1", savedModel)
	}
}

func TestMgmt_ProjectModel_ReturnsErrorWhenModelSaveFails(t *testing.T) {
	agent := &stubModelModeAgent{
		model: "gpt-4.1-mini",
	}
	e := NewEngine("test-project", agent, nil, "")
	e.SetModelSaveFunc(func(model string) error {
		return errors.New("disk full")
	})

	mgmt := NewManagementServer(0, "tok", nil)
	mgmt.RegisterEngine("test-project", e)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/projects/", mgmt.wrap(mgmt.handleProjectRoutes))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/model", "tok", map[string]string{"model": "gpt-4.1"})
	if r.OK {
		t.Fatal("update model unexpectedly succeeded")
	}
	if !strings.Contains(r.Error, "disk full") {
		t.Fatalf("error = %q, want save failure", r.Error)
	}
	if got := agent.GetModel(); got != "gpt-4.1-mini" {
		t.Fatalf("GetModel() = %q, want unchanged gpt-4.1-mini", got)
	}
}

func TestMgmt_ProjectModels_UsesTimeoutContext(t *testing.T) {
	agent := &deadlineAwareModelAgent{}
	e := NewEngine("test-project", agent, nil, "")

	mgmt := NewManagementServer(0, "tok", nil)
	mgmt.RegisterEngine("test-project", e)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/projects/", mgmt.wrap(mgmt.handleProjectRoutes))
	ts := httptest.NewServer(mux)
	defer ts.Close()

	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/models", "tok")
	if !r.OK {
		t.Fatalf("project models failed: %s", r.Error)
	}
	if !agent.sawDeadline() {
		t.Fatal("AvailableModels context has no deadline; want timeout-bounded context")
	}
}

func TestMgmt_AddPlatformToNewProject_DoesNotRequireEngine(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")

	var savedProject, savedPlatType string
	mgmt.SetAddPlatformToProject(func(proj, platType string, opts map[string]any, workDir, agentType string) error {
		savedProject = proj
		savedPlatType = platType
		return nil
	})

	// "brand-new-project" has no engine registered — this must NOT return 404.
	r := mgmtPost(t, ts.URL+"/api/v1/projects/brand-new-project/add-platform", "tok", map[string]any{
		"type":    "dingtalk",
		"options": map[string]any{"client_id": "abc", "client_secret": "def"},
	})
	if !r.OK {
		t.Fatalf("add-platform to new project failed: %s — should not require a running engine", r.Error)
	}
	if savedProject != "brand-new-project" {
		t.Fatalf("saved project = %q, want brand-new-project", savedProject)
	}
	if savedPlatType != "dingtalk" {
		t.Fatalf("saved platform type = %q, want dingtalk", savedPlatType)
	}
}

func TestMgmt_AddPlatformToNewProject_RejectsMissingWorkDir(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")

	called := false
	mgmt.SetAddPlatformToProject(func(proj, platType string, opts map[string]any, workDir, agentType string) error {
		called = true
		return nil
	})

	missing := filepath.Join(t.TempDir(), "missing")
	r := mgmtPost(t, ts.URL+"/api/v1/projects/brand-new-project/add-platform", "tok", map[string]any{
		"type":     "dingtalk",
		"options":  map[string]any{"client_id": "abc", "client_secret": "def"},
		"work_dir": missing,
	})
	if r.OK {
		t.Fatal("expected missing work_dir to be rejected")
	}
	if !strings.Contains(r.Error, "work_dir does not exist") {
		t.Fatalf("error = %q, want work_dir does not exist", r.Error)
	}
	if called {
		t.Fatal("addPlatformToProject should not be called when work_dir is invalid")
	}
}

func TestMgmt_SetupSave_RejectsMissingWorkDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")

	t.Run("weixin", func(t *testing.T) {
		mgmt := NewManagementServer(0, "", nil)
		called := false
		mgmt.SetSetupWeixinSave(func(req WeixinSetupSaveRequest) error {
			called = true
			return nil
		})

		r, code := mgmtPostHandler(t, mgmt.handleSetupWeixinSave, "/api/v1/setup/weixin/save", map[string]any{
			"project":  "demo",
			"token":    "token",
			"work_dir": missing,
		})
		if r.OK || code != http.StatusBadRequest {
			t.Fatalf("response ok=%v status=%d error=%q, want 400", r.OK, code, r.Error)
		}
		if !strings.Contains(r.Error, "work_dir does not exist") {
			t.Fatalf("error = %q, want work_dir does not exist", r.Error)
		}
		if called {
			t.Fatal("setupWeixinSave should not be called when work_dir is invalid")
		}
	})
}

func TestValidateProjectWorkDir(t *testing.T) {
	dir := t.TempDir()
	got, err := validateProjectWorkDir("  " + dir + "  ")
	if err != nil {
		t.Fatalf("validate existing dir: %v", err)
	}
	if got != dir {
		t.Fatalf("trimmed work_dir = %q, want %q", got, dir)
	}

	got, err = validateProjectWorkDir("  ")
	if err != nil {
		t.Fatalf("empty work_dir should be accepted: %v", err)
	}
	if got != "" {
		t.Fatalf("empty work_dir = %q, want empty", got)
	}

	file := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(file, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	if _, err := validateProjectWorkDir(file); err == nil || !strings.Contains(err.Error(), "work_dir is not a directory") {
		t.Fatalf("file work_dir error = %v, want not a directory", err)
	}
}

func TestMgmt_OtherRoutesStillRequireEngine(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/projects/nonexistent/sessions", "tok")
	if r.OK {
		t.Fatal("expected 404 for sessions on nonexistent project")
	}
	if !strings.Contains(r.Error, "project not found") {
		t.Fatalf("error = %q, want project not found", r.Error)
	}
}

func mgmtPut(t *testing.T, url, token string, body any) mgmtResponse {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode PUT body: %v", err)
		}
	}
	req, _ := http.NewRequest("PUT", url, &buf)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", url, err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Fatalf("decode PUT response: %v", err)
	}
	return r
}

// ── Restart ──

func TestMgmt_Restart(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtPost(t, ts.URL+"/api/v1/restart", "tok", nil)
	if !r.OK {
		t.Fatalf("restart failed: %s", r.Error)
	}
	select {
	case req := <-RestartCh:
		_ = req
	default:
	}
}

func TestMgmt_Restart_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/restart", "tok")
	if r.OK {
		t.Fatal("expected GET on restart to fail")
	}
}

// ── Agents ──

func TestMgmt_Agents(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/agents", "tok")
	if !r.OK {
		t.Fatalf("agents failed: %s", r.Error)
	}
	var data struct {
		Agents    []string `json:"agents"`
		Platforms []string `json:"platforms"`
	}
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal agents: %v", err)
	}
}

func TestMgmt_Agents_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/agents", "tok", nil)
	if r.OK {
		t.Fatal("expected POST on agents to fail")
	}
}

// ── Global Settings ──

func TestMgmt_GlobalSettings_Get(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")
	mgmt.SetGetGlobalSettings(func() map[string]any {
		return map[string]any{"auto_start": true}
	})

	r := mgmtGet(t, ts.URL+"/api/v1/settings", "tok")
	if !r.OK {
		t.Fatalf("get settings failed: %s", r.Error)
	}
	var data map[string]any
	if err := json.Unmarshal(r.Data, &data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if data["auto_start"] != true {
		t.Fatalf("auto_start = %v, want true", data["auto_start"])
	}
}

func TestMgmt_GlobalSettings_GetNotAvailable(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/settings", "tok")
	if r.OK {
		t.Fatal("expected error when getGlobalSettings is nil")
	}
}

func TestMgmt_GlobalSettings_Patch(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")

	saved := map[string]any{}
	mgmt.SetGetGlobalSettings(func() map[string]any { return saved })
	mgmt.SetSaveGlobalSettings(func(updates map[string]any) error {
		for k, v := range updates {
			saved[k] = v
		}
		return nil
	})

	r := mgmtPatch(t, ts.URL+"/api/v1/settings", "tok", map[string]any{"auto_start": false})
	if !r.OK {
		t.Fatalf("patch settings failed: %s", r.Error)
	}
	if saved["auto_start"] != false {
		t.Fatalf("saved auto_start = %v, want false", saved["auto_start"])
	}
}

func TestMgmt_GlobalSettings_PatchSaveError(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")
	mgmt.SetSaveGlobalSettings(func(updates map[string]any) error {
		return errors.New("write failed")
	})
	r := mgmtPatch(t, ts.URL+"/api/v1/settings", "tok", map[string]any{"x": 1})
	if r.OK {
		t.Fatal("expected save error")
	}
	if !strings.Contains(r.Error, "write failed") {
		t.Fatalf("error = %q, want write failed", r.Error)
	}
}

// ── Project Send ──

func TestMgmt_ProjectSend_EmptyMessage(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/send", "tok", map[string]string{
		"session_key": "user1",
		"message":     "",
	})
	if r.OK {
		t.Fatal("expected error for empty message")
	}
	if !strings.Contains(r.Error, "message is required") {
		t.Fatalf("error = %q, want message required", r.Error)
	}
}

func TestMgmt_ProjectSend_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/send", "tok")
	if r.OK {
		t.Fatal("expected GET on send to fail")
	}
}

// ── Project Users ──

func TestMgmt_ProjectUsers_Get(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/users", "tok")
	if !r.OK {
		t.Fatalf("get users failed: %s", r.Error)
	}
}

func TestMgmt_ProjectUsers_PatchInvalidJSON(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	req, _ := http.NewRequest("PATCH", ts.URL+"/api/v1/projects/test-project/users", strings.NewReader("{bad"))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.OK {
		t.Fatal("expected error for invalid JSON")
	}
}

// ── Project Delete ──

func TestMgmt_ProjectDelete_NotConfigured(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project", "tok")
	if r.OK {
		t.Fatal("expected error when removeProject is nil")
	}
	if !strings.Contains(r.Error, "not configured") {
		t.Fatalf("error = %q", r.Error)
	}
}

// ── Cron PATCH (update job) ──

// ── Project routes: unknown sub-path ──

func TestMgmt_ProjectRoutes_UnknownSubpath(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/unknown-route", "tok")
	if r.OK {
		t.Fatal("expected 404 for unknown sub-path")
	}
}

// ── Session create missing session_key ──

func TestMgmt_SessionCreate_MissingKey(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/sessions", "tok", map[string]string{
		"name": "work",
	})
	if r.OK {
		t.Fatal("expected error for missing session_key")
	}
}

// ── Reload failure ──

func TestMgmt_Reload_Failure(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")
	e.configReloadFunc = func() (*ConfigReloadResult, error) {
		return nil, errors.New("parse error")
	}
	r := mgmtPost(t, ts.URL+"/api/v1/reload", "tok", nil)
	if r.OK {
		t.Fatal("expected reload failure")
	}
	if !strings.Contains(r.Error, "parse error") {
		t.Fatalf("error = %q, want parse error", r.Error)
	}
}

// ── Config PUT (save) ──

func TestMgmt_Config_Save(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	tmp := t.TempDir()
	cfgPath := tmp + "/config.toml"
	os.WriteFile(cfgPath, []byte("[display]\ntitle = \"old\"\n"), 0644)

	req, _ := http.NewRequest("PUT", ts.URL+"/api/v1/config", strings.NewReader("# new config\n"))
	req.Header.Set("Authorization", "Bearer tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// Without SetConfigFilePath, save should fail
	if resp.StatusCode == 200 {
		t.Fatal("expected error without config file path set")
	}
}

// ────────────────────────────────────────────────────────────────
// Edge cases & boundary tests below
// ────────────────────────────────────────────────────────────────

// ── Restart edge cases ──

func TestMgmt_Restart_AlreadyInProgress(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	// Fill the buffered channel (cap=1) so the next restart is rejected.
	RestartCh <- RestartRequest{}

	r := mgmtPost(t, ts.URL+"/api/v1/restart", "tok", nil)
	if r.OK {
		t.Fatal("expected conflict when restart channel is full")
	}
	if !strings.Contains(r.Error, "already in progress") {
		t.Fatalf("error = %q, want 'already in progress'", r.Error)
	}

	// Drain so other tests aren't affected.
	<-RestartCh
}

func TestMgmt_Restart_WithSessionKey(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtPost(t, ts.URL+"/api/v1/restart", "tok", map[string]string{
		"session_key": "user1",
		"platform":    "telegram",
	})
	if !r.OK {
		t.Fatalf("restart with session_key failed: %s", r.Error)
	}
	req := <-RestartCh
	if req.SessionKey != "user1" || req.Platform != "telegram" {
		t.Fatalf("restart request = %+v, want session_key=user1 platform=telegram", req)
	}
}

// ── Config edge cases ──

func TestMgmt_Config_NoPathSet(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/config", "tok")
	if r.OK {
		t.Fatal("expected error when configFilePath is empty")
	}
	if !strings.Contains(r.Error, "not set") {
		t.Fatalf("error = %q", r.Error)
	}
}

func TestMgmt_Config_FileNotFound(t *testing.T) {
	srv, ts, _ := testManagementServer(t, "tok")
	srv.SetConfigFilePath("/nonexistent/path/config.toml")

	r := mgmtGet(t, ts.URL+"/api/v1/config", "tok")
	if r.OK {
		t.Fatal("expected error for missing config file")
	}
}

func TestMgmt_Config_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/config", "tok", nil)
	if r.OK {
		t.Fatal("expected POST on config to fail")
	}
}

// ── Settings edge cases ──

func TestMgmt_GlobalSettings_PatchInvalidJSON(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")
	mgmt.SetSaveGlobalSettings(func(updates map[string]any) error { return nil })

	req, _ := http.NewRequest("PATCH", ts.URL+"/api/v1/settings", strings.NewReader("{bad json"))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.OK {
		t.Fatal("expected error for invalid JSON body")
	}
	if !strings.Contains(r.Error, "invalid JSON") {
		t.Fatalf("error = %q, want 'invalid JSON'", r.Error)
	}
}

func TestMgmt_GlobalSettings_PatchNotConfigured(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPatch(t, ts.URL+"/api/v1/settings", "tok", map[string]any{"x": 1})
	if r.OK {
		t.Fatal("expected error when saveGlobalSettings is nil")
	}
}

func TestMgmt_GlobalSettings_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtDelete(t, ts.URL+"/api/v1/settings", "tok")
	if r.OK {
		t.Fatal("expected DELETE on settings to fail")
	}
}

// ── Send edge cases ──

func TestMgmt_ProjectSend_InvalidJSON(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/projects/test-project/send", strings.NewReader("{bad"))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.OK {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestMgmt_ProjectSend_NonexistentProject(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/projects/nonexistent/send", "tok", map[string]string{
		"message": "hello",
	})
	if r.OK {
		t.Fatal("expected 404 for send to nonexistent project")
	}
}

// ── Project Detail PATCH edge cases ──

func TestMgmt_ProjectPatch_InvalidJSON(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	req, _ := http.NewRequest("PATCH", ts.URL+"/api/v1/projects/test-project", strings.NewReader("{bad"))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.OK {
		t.Fatal("expected error for invalid JSON body")
	}
}

func TestMgmt_ProjectPatch_UnknownAgentType(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	agentType := "totally-unknown-agent"
	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project", "tok", map[string]any{
		"agent_type": agentType,
	})
	if r.OK {
		t.Fatal("expected error for unknown agent type")
	}
	if !strings.Contains(r.Error, "unknown agent type") {
		t.Fatalf("error = %q", r.Error)
	}
}

func TestMgmt_ProjectPatch_RejectsMissingWorkDirBeforeMutation(t *testing.T) {
	mgmt, ts, e := testManagementServer(t, "tok")
	agent := &stubWorkDirAgent{workDir: "/existing"}
	e.agent = agent

	saveCalled := false
	mgmt.SetSaveProjectSettings(func(projectName string, update ProjectSettingsUpdate) error {
		saveCalled = true
		return nil
	})

	missing := filepath.Join(t.TempDir(), "missing")
	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project", "tok", map[string]any{
		"work_dir": missing,
	})
	if r.OK {
		t.Fatal("expected missing work_dir to be rejected")
	}
	if !strings.Contains(r.Error, "work_dir does not exist") {
		t.Fatalf("error = %q, want work_dir does not exist", r.Error)
	}
	if got := agent.GetWorkDir(); got != "/existing" {
		t.Fatalf("work_dir mutated to %q, want original value", got)
	}
	if saveCalled {
		t.Fatal("saveProjectSettings should not be called when work_dir is invalid")
	}
}

func TestMgmt_ProjectPatch_DisabledCommands(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")
	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project", "tok", map[string]any{
		"disabled_commands": []string{"new", "delete"},
	})
	if !r.OK {
		t.Fatalf("patch disabled_commands failed: %s", r.Error)
	}
	got := e.GetDisabledCommands()
	sort.Strings(got)
	if len(got) != 2 || got[0] != "delete" || got[1] != "new" {
		t.Fatalf("disabled_commands = %v, want [delete new]", got)
	}
}

func TestMgmt_ProjectPatch_AdminFrom(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")
	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project", "tok", map[string]any{
		"admin_from": "user123",
	})
	if !r.OK {
		t.Fatalf("patch admin_from failed: %s", r.Error)
	}
	e.userRolesMu.RLock()
	got := e.adminFrom
	e.userRolesMu.RUnlock()
	if got != "user123" {
		t.Fatalf("adminFrom = %q, want user123", got)
	}
}

func TestMgmt_ProjectDetail_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project", "tok", nil)
	if r.OK {
		t.Fatal("expected POST on project detail to fail")
	}
}

// ── Project Delete edge cases ──

func TestMgmt_ProjectDelete_Success(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")
	var removed string
	mgmt.SetRemoveProject(func(name string) error {
		removed = name
		return nil
	})
	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project", "tok")
	if !r.OK {
		t.Fatalf("delete project failed: %s", r.Error)
	}
	if removed != "test-project" {
		t.Fatalf("removed = %q, want test-project", removed)
	}
	var data map[string]any
	json.Unmarshal(r.Data, &data)
	if data["restart_required"] != true {
		t.Fatal("expected restart_required=true")
	}
}

func TestMgmt_ProjectDelete_Error(t *testing.T) {
	mgmt, ts, _ := testManagementServer(t, "tok")
	mgmt.SetRemoveProject(func(name string) error {
		return errors.New("cannot remove last project")
	})
	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project", "tok")
	if r.OK {
		t.Fatal("expected error from removeProject")
	}
	if !strings.Contains(r.Error, "cannot remove last project") {
		t.Fatalf("error = %q", r.Error)
	}
}

// ── Session switch edge cases ──

func TestMgmt_SessionSwitch_Success(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")

	s1 := e.sessions.GetOrCreateActive("user1")
	s2 := e.sessions.NewSession("user1", "second session")

	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/sessions/switch", "tok", map[string]string{
		"session_key": "user1",
		"session_id":  s2.ID,
	})
	if !r.OK {
		t.Fatalf("switch failed: %s", r.Error)
	}
	current := e.sessions.GetOrCreateActive("user1")
	if current.ID == s1.ID {
		t.Fatal("session did not switch")
	}
}

func TestMgmt_SessionSwitch_MissingFields(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")

	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/sessions/switch", "tok", map[string]string{
		"session_key": "user1",
	})
	if r.OK {
		t.Fatal("expected error for missing session_id")
	}
	if !strings.Contains(r.Error, "required") {
		t.Fatalf("error = %q", r.Error)
	}
}

func TestMgmt_SessionSwitch_InvalidSessionID(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")
	e.sessions.GetOrCreateActive("user1")

	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/sessions/switch", "tok", map[string]string{
		"session_key": "user1",
		"session_id":  "nonexistent-id",
	})
	if r.OK {
		t.Fatal("expected error for nonexistent session_id")
	}
}

func TestMgmt_SessionSwitch_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/sessions/switch", "tok")
	if r.OK {
		t.Fatal("expected GET on session switch to fail")
	}
}

func TestMgmt_SessionSwitch_InvalidJSON(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/projects/test-project/sessions/switch", strings.NewReader("{bad"))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.OK {
		t.Fatal("expected error for invalid JSON")
	}
}

// ── Session detail edge cases ──

func TestMgmt_SessionDetail_NotFound(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/projects/test-project/sessions/nonexistent-id", "tok")
	if r.OK {
		t.Fatal("expected 404 for nonexistent session")
	}
}

func TestMgmt_SessionDetail_DeleteNotFound(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project/sessions/nonexistent-id", "tok")
	if r.OK {
		t.Fatal("expected 404 for deleting nonexistent session")
	}
}

func TestMgmt_SessionDetail_MethodNotAllowed(t *testing.T) {
	_, ts, e := testManagementServer(t, "tok")
	s := e.sessions.GetOrCreateActive("user1")
	r := mgmtPost(t, ts.URL+"/api/v1/projects/test-project/sessions/"+s.ID, "tok", nil)
	if r.OK {
		t.Fatal("expected POST on session detail to fail")
	}
}

func TestMgmt_SessionCreate_InvalidJSON(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/projects/test-project/sessions", strings.NewReader("{bad"))
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var r mgmtResponse
	json.NewDecoder(resp.Body).Decode(&r)
	if r.OK {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestMgmt_Sessions_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project/sessions", "tok")
	if r.OK {
		t.Fatal("expected DELETE on sessions list to fail")
	}
}

// ── Users edge cases ──

func TestMgmt_ProjectUsers_PatchValid(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project/users", "tok", map[string]any{
		"default_role": "member",
		"roles": map[string]any{
			"member": map[string]any{
				"user_ids": []string{"uid-member-1"},
			},
			"admin": map[string]any{
				"user_ids": []string{"uid-admin"},
			},
		},
	})
	if !r.OK {
		t.Fatalf("patch users failed: %s", r.Error)
	}
}

func TestMgmt_ProjectUsers_PatchInvalidRoleConfig(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPatch(t, ts.URL+"/api/v1/projects/test-project/users", "tok", map[string]any{
		"default_role": "nonexistent",
		"roles": map[string]any{
			"admin": map[string]any{
				"user_ids": []string{"uid-admin"},
			},
		},
	})
	if r.OK {
		t.Fatal("expected error when default_role doesn't match any defined role")
	}
	if !strings.Contains(r.Error, "invalid users config") {
		t.Fatalf("error = %q", r.Error)
	}
}

func TestMgmt_ProjectUsers_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtDelete(t, ts.URL+"/api/v1/projects/test-project/users", "tok")
	if r.OK {
		t.Fatal("expected DELETE on users to fail")
	}
}

// ── Heartbeat edge cases ──

// ── Cron edge cases ──

// ── Project routes: empty project name ──

func TestMgmt_ProjectRoutes_EmptyProjectName(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/projects/", "tok")
	// /projects/ with empty trailing slash is dispatched to handleProjectRoutes
	// which returns "project name required" error.
	if r.OK {
		t.Fatal("expected error for empty project name in project routes")
	}
}

// ── Reload edge cases ──

func TestMgmt_Reload_MethodNotAllowed(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtGet(t, ts.URL+"/api/v1/reload", "tok")
	if r.OK {
		t.Fatal("expected GET on reload to fail")
	}
}

func TestMgmt_Reload_NoReloadFunc(t *testing.T) {
	_, ts, _ := testManagementServer(t, "tok")
	r := mgmtPost(t, ts.URL+"/api/v1/reload", "tok", nil)
	if !r.OK {
		t.Fatalf("reload with nil reloadFunc should succeed: %s", r.Error)
	}
}

// TestMgmt_SetupWeixinPoll_RejectsMalformedAPIURL is a regression test for a
// nil-pointer panic in handleSetupWeixinPoll. The handler did
// `u, _ := url.Parse(apiBase + "/")` and then immediately called
// `u.JoinPath(...)`. For inputs like "://" or "%zz", url.Parse returns a nil
// URL plus an error; the discarded error meant the next line crashed the
// management server with `runtime error: invalid memory address or nil
// pointer dereference`. handleSetupWeixinBegin already validated this same
// field; this test pins the symmetric handling here.
func TestMgmt_SetupWeixinPoll_RejectsMalformedAPIURL(t *testing.T) {
	mgmt := NewManagementServer(0, "", nil)

	for _, bad := range []string{"://", "://malformed", "%zz"} {
		body := map[string]any{
			"qr_key":  "abc",
			"api_url": bad,
		}
		buf := new(bytes.Buffer)
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/weixin/poll", buf)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		// Recover any panic so the test reports a meaningful failure rather
		// than crashing the test binary, then assert the handler returned a
		// 4xx (not 5xx and not a panic) for the bad input.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("handleSetupWeixinPoll panicked on api_url=%q: %v", bad, r)
				}
			}()
			mgmt.handleSetupWeixinPoll(w, req)
		}()

		if w.Code != http.StatusBadRequest {
			t.Errorf("api_url=%q: status=%d, want %d (body=%s)", bad, w.Code, http.StatusBadRequest, w.Body.String())
		}
	}
}
