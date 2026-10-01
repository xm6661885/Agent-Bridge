package qq

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agent-bridge/core"

	"github.com/gorilla/websocket"
)

func TestPlatform_Name(t *testing.T) {
	p := &Platform{}
	if got := p.Name(); got != "qq" {
		t.Errorf("Name() = %q, want %q", got, "qq")
	}
}

func TestNew_DefaultWSURL(t *testing.T) {
	p, err := New(map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.wsURL != "ws://127.0.0.1:3001" {
		t.Errorf("wsURL = %q, want %q", platform.wsURL, "ws://127.0.0.1:3001")
	}
	if !platform.shareSessionInChannel {
		t.Error("shareSessionInChannel = false, want true by default")
	}
}

func TestNew_CustomWSURL(t *testing.T) {
	p, err := New(map[string]any{
		"ws_url": "ws://example.com:8080",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.wsURL != "ws://example.com:8080" {
		t.Errorf("wsURL = %q, want %q", platform.wsURL, "ws://example.com:8080")
	}
}

func TestNew_WithToken(t *testing.T) {
	p, err := New(map[string]any{
		"token": "my-secret-token",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.token != "my-secret-token" {
		t.Errorf("token = %q, want %q", platform.token, "my-secret-token")
	}
}

func TestNew_WithAllowFrom(t *testing.T) {
	p, err := New(map[string]any{
		"allow_from": "user1,user2,*",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if platform.allowFrom != "user1,user2,*" {
		t.Errorf("allowFrom = %q, want %q", platform.allowFrom, "user1,user2,*")
	}
}

func TestNew_ShareSessionInChannel(t *testing.T) {
	p, err := New(map[string]any{
		"share_session_in_channel": true,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	platform := p.(*Platform)
	if !platform.shareSessionInChannel {
		t.Error("shareSessionInChannel = false, want true")
	}
}

func TestNew_RequireMention(t *testing.T) {
	p, err := New(map[string]any{"require_mention": true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.(*Platform).requireMention {
		t.Fatal("requireMention = false, want true")
	}
}

func TestNew_GroupReplyAllAlias(t *testing.T) {
	p, err := New(map[string]any{"group_reply_all": false})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.(*Platform).requireMention {
		t.Fatal("group_reply_all=false should require a mention")
	}
}

func TestHandleMessage_RequireMention(t *testing.T) {
	var handled int
	p := &Platform{
		selfID:         123,
		requireMention: true,
		handler: func(core.Platform, *core.Message) {
			handled++
		},
	}

	base := map[string]any{
		"post_type":    "message",
		"message_type": "group",
		"self_id":      float64(123),
		"user_id":      float64(456),
		"group_id":     float64(0), // avoid a group-info API call in this unit test
		"sender":       map[string]any{"nickname": "Alice"},
	}

	withoutMention := clonePayload(base)
	withoutMention["message_id"] = float64(1)
	withoutMention["message"] = []any{
		map[string]any{"type": "text", "data": map[string]any{"text": "hello"}},
	}
	p.handleMessage(withoutMention)
	if handled != 0 {
		t.Fatalf("unmentioned group message was dispatched: handled=%d", handled)
	}

	withMention := clonePayload(base)
	withMention["message_id"] = float64(2)
	withMention["message"] = []any{
		map[string]any{"type": "at", "data": map[string]any{"qq": "123"}},
		map[string]any{"type": "text", "data": map[string]any{"text": " hello"}},
	}
	p.handleMessage(withMention)
	if handled != 1 {
		t.Fatalf("mentioned group message handled=%d, want 1", handled)
	}

	withCQMention := clonePayload(base)
	withCQMention["message_id"] = float64(3)
	withCQMention["message"] = "[CQ:at,qq=123] hello"
	p.handleMessage(withCQMention)
	if handled != 2 {
		t.Fatalf("CQ-mentioned group message handled=%d, want 2", handled)
	}
}

func TestHandleMessage_GroupGetsSerialKeyWhileKeepingPerUserSession(t *testing.T) {
	var got *core.Message
	p := &Platform{handler: func(_ core.Platform, msg *core.Message) { got = msg }}
	p.handleMessage(map[string]any{
		"post_type": "message", "message_type": "group",
		// group_id=0 avoids the external group-info lookup in this unit test.
		"message_id": float64(100), "group_id": float64(0), "user_id": float64(7),
		"sender":  map[string]any{"nickname": "Alice"},
		"message": []any{map[string]any{"type": "text", "data": map[string]any{"text": "hello"}}},
	})
	if got == nil {
		t.Fatal("group message was not dispatched")
	}
	if got.SessionKey != "qq:0:7" {
		t.Errorf("SessionKey = %q, want per-user qq:0:7", got.SessionKey)
	}
	if got.SerialKey != "qq:group:0" {
		t.Errorf("SerialKey = %q, want qq:group:0", got.SerialKey)
	}
}

func TestIsBotMentioned_CQCodeFallback(t *testing.T) {
	p := &Platform{selfID: 123}
	for name, payload := range map[string]map[string]any{
		"message string": {"message": "[CQ:at,qq=123] hello"},
		"raw message":    {"raw_message": "hello [CQ:at,qq=123]"},
		"all":            {"message": "[CQ:at,qq=all]"},
	} {
		if !p.isBotMentioned(payload) {
			t.Errorf("%s: expected mention to be detected", name)
		}
	}
}

func clonePayload(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// verify Platform implements core.Platform
var _ core.Platform = (*Platform)(nil)

// TestStart_FetchesSelfIDWithoutTimeout verifies that Start() completes
// promptly with selfID populated from the get_login_info OneBot API call.
// Regression for a bug where Start invoked callAPI BEFORE launching readLoop,
// so the API response had no consumer and callAPI always timed out after 15s
// — leaving selfID=0 and disabling the self-message filter in handleMessage.
func TestStart_FetchesSelfIDWithoutTimeout(t *testing.T) {
	const botUserID = 999999

	upgrader := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			_, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]any
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			if req["action"] == "get_login_info" {
				echo, _ := req["echo"].(string)
				resp := map[string]any{
					"status":  "ok",
					"retcode": 0,
					"echo":    echo,
					"data":    map[string]any{"user_id": botUserID, "nickname": "TestBot"},
				}
				raw, _ := json.Marshal(resp)
				_ = c.WriteMessage(websocket.TextMessage, raw)
			}
		}
	}))
	defer ts.Close()

	p := &Platform{
		wsURL: "ws" + strings.TrimPrefix(ts.URL, "http"),
	}

	done := make(chan error, 1)
	go func() {
		done <- p.Start(func(core.Platform, *core.Message) {})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = p.Stop()
		t.Fatal("Start did not complete within 5s; readLoop likely starts after callAPI, so get_login_info never gets a response")
	}
	defer p.Stop()

	if p.selfID != botUserID {
		t.Errorf("selfID = %d, want %d (self-message filter would be disabled)", p.selfID, botUserID)
	}
}

func TestNew_AllowGroups(t *testing.T) {
	p, err := New(map[string]any{"allow_groups": []any{"111", int64(222)}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := p.(*Platform).allowGroups; got != "111,222" {
		t.Fatalf("allowGroups = %q, want 111,222", got)
	}
}

func TestHandleMessage_AllowGroupsFiltersOtherGroups(t *testing.T) {
	var handled []string
	p := &Platform{
		selfID:                123,
		allowGroups:           "111, 222",
		shareSessionInChannel: true,
		handler: func(_ core.Platform, msg *core.Message) {
			handled = append(handled, msg.SessionKey)
		},
	}
	p.groupNameCache.Store("111", "allowed") // avoid a get_group_info API call

	mk := func(id, group float64, msgType string) map[string]any {
		return map[string]any{
			"post_type":    "message",
			"message_type": msgType,
			"message_id":   id,
			"user_id":      float64(456),
			"group_id":     group,
			"message":      []any{map[string]any{"type": "text", "data": map[string]any{"text": "hi"}}},
		}
	}
	p.handleMessage(mk(1, 333, "group"))
	p.handleMessage(mk(2, 111, "group"))
	p.handleMessage(mk(3, 0, "private"))

	want := []string{"qq:g:111", "qq:456"}
	if len(handled) != len(want) || handled[0] != want[0] || handled[1] != want[1] {
		t.Fatalf("handled = %v, want %v", handled, want)
	}
}

func TestIsBotMentioned_ReplyToBotMessage(t *testing.T) {
	p := &Platform{selfID: 123}
	p.rememberSent(map[string]any{"message_id": float64(9001)})

	quoted := map[string]any{"message": []any{
		map[string]any{"type": "reply", "data": map[string]any{"id": "9001"}},
		map[string]any{"type": "text", "data": map[string]any{"text": "1"}},
	}}
	if !p.isBotMentioned(quoted) {
		t.Fatal("quoting a bot message should count as a mention")
	}
	other := map[string]any{"message": []any{
		map[string]any{"type": "reply", "data": map[string]any{"id": "42"}},
	}}
	if p.isBotMentioned(other) {
		t.Fatal("quoting someone else's message is not a mention")
	}
	if !p.isBotMentioned(map[string]any{"message": "[CQ:reply,id=9001]1"}) {
		t.Fatal("CQ reply to bot message should count as a mention")
	}
}
