package agentevents

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const historyPA = "018f47a2-9b3c-7def-8abc-0123456789ab"

func historyAppend(t *testing.T, g *DurableGateway, seq uint64, event any) {
	t.Helper()
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.appendDurableEventLocked(context.Background(), historyPA, durableEventRecord{Seq: seq, Event: Envelope{Audience: AudienceDirectChat, Seq: &seq, PersonalityAgentID: historyPA, Event: raw}}); err != nil {
		t.Fatal(err)
	}
}
func historyMessage(n int) map[string]any {
	return map[string]any{"type": "message_end", "message_id": fmt.Sprintf("01992000-0000-7000-8000-%012x", n), "message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": fmt.Sprintf("Message %d", n)}}, "timestamp": "2026-09-12T00:00:00Z"}}
}
func TestBrowserHistoryNewestPagingIndexAndCatchup(t *testing.T) {
	g := openRuntimeGateway(t)
	historyAppend(t, g, 1, map[string]any{"type": "agent_start"})
	for n := 1; n <= 230; n++ {
		historyAppend(t, g, uint64(n+1), historyMessage(n))
	}
	p, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Events) != 100 || len(p.Context) != 1 || *p.Context[0].Seq != 1 || p.LatestSeq != 231 || !p.HasMore || len(p.Index) != 230 {
		t.Fatalf("events=%d context=%d latest=%d index=%d", len(p.Events), len(p.Context), p.LatestSeq, len(p.Index))
	}
	if *p.Events[0].Seq != 132 || *p.BeforeSeq != 132 {
		t.Fatal("not newest 100")
	}
	older, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100, before: *p.BeforeSeq})
	if err != nil {
		t.Fatal(err)
	}
	if len(older.Events) != 100 || *older.Events[0].Seq != 32 || *older.Events[99].Seq != 131 {
		t.Fatal("older window gaps or overlap")
	}
	around, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100, around: p.Index[4].ID})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range around.Events {
		if *e.Seq == p.Index[4].Seq {
			found = true
		}
	}
	if !found {
		t.Fatal("jump target absent")
	}
	historyAppend(t, g, 232, historyMessage(231))
	suffix, err := g.EventCatchUp(context.Background(), historyPA, p.LatestSeq)
	if err != nil || len(suffix) != 1 || *suffix[0].Seq != 232 {
		t.Fatalf("catchup lost append: %v %v", suffix, err)
	}
	cache := g.history[historyPA]
	updated, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if g.history[historyPA] != cache || updated.LatestSeq != 232 || len(updated.Index) != 231 || updated.Index[0].ID != p.Index[0].ID {
		t.Fatal("incremental cache or stable index lost")
	}
}
func TestBrowserHistoryCurrentApprovalAndLogReplacement(t *testing.T) {
	g := openRuntimeGateway(t)
	historyAppend(t, g, 1, map[string]any{"type": "agent_start"})
	var request any
	if err := json.Unmarshal([]byte(`{"type":"approval_requested","request":{"id":"request-1","tool_call_id":"call-1","tool_name":"read_file","action":{"reviewable":"read"},"args_summary":"read"}}`), &request); err != nil {
		t.Fatal(err)
	}
	historyAppend(t, g, 2, request)
	for n := 1; n <= 105; n++ {
		historyAppend(t, g, uint64(n+2), historyMessage(n))
	}
	p, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.PendingApprovals) != 1 || *p.PendingApprovals[0].Seq != 2 || p.ActiveRun == nil || *p.ActiveRun.Seq != 1 {
		t.Fatal("current approval/run lost outside window")
	}
	path := g.eventPath(historyPA)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	first := raw[:strings.IndexByte(string(raw), '\n')+1]
	if err = os.WriteFile(path, first, 0600); err != nil {
		t.Fatal(err)
	}
	p, err = g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if p.LatestSeq != 1 || len(p.Index) != 0 || len(p.PendingApprovals) != 0 {
		t.Fatal("truncated cache reused")
	}
	replacement := path + ".replacement"
	if err = os.WriteFile(replacement, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(replacement, path); err != nil {
		t.Fatal(err)
	}
	p, err = g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if p.LatestSeq != 107 || len(p.Index) != 105 {
		t.Fatal("replacement cache reused")
	}
}
func TestBrowserHistoryAuthorizesBeforeReadingMetadata(t *testing.T) {
	g := openRuntimeGateway(t)
	s := newAuthorizedBrowserServer(&fakeSessionVerifier{personalityAgentID: historyPA}, g, g)
	s.AllowedOrigins = []string{"https://web.example"}
	request := func(query string, cookie bool, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/direct-chat/history?"+query, nil)
		if cookie {
			r.AddCookie(&http.Cookie{Name: BrowserSessionCookie, Value: "fixture"})
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		s.ServeHistory(w, r)
		return w
	}
	valid := "installation_id=" + testDirectChatInstallationID + "&authority_epoch=1"
	for _, tc := range []struct {
		query  string
		cookie bool
		origin string
		status int
	}{{valid, false, "", 401}, {valid, true, "https://evil.example", 403}, {"installation_id=" + testDirectChatInstallationID + "&authority_epoch=2", true, "", 403}, {valid + "&limit=101", true, "", 400}} {
		w := request(tc.query, tc.cookie, tc.origin)
		if w.Code != tc.status {
			t.Fatalf("status %d want %d: %s", w.Code, tc.status, w.Body.String())
		}
		if len(g.history) != 0 {
			t.Fatal("unauthorized request read index")
		}
	}
	w := request(valid, true, "")
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("authorized safe read: %d %s", w.Code, w.Body.String())
	}
}

func TestBrowserHistoryToolOnlySpansRemainPageableAndCarryOriginalRun(t *testing.T) {
	g := openRuntimeGateway(t)
	historyAppend(t, g, 1, map[string]any{"type": "agent_start"})
	historyAppend(t, g, 2, historyMessage(1))
	// A long tool-only interval must not make a page grow with the whole run,
	// nor disappear when the next cursor crosses no message boundary.
	for n := 3; n <= 2010; n++ {
		historyAppend(t, g, uint64(n), map[string]any{"type": "tool_execution_start", "tool_call_id": fmt.Sprintf("tool-%d", n), "tool_name": "read_file", "args": map[string]any{}})
	}
	p, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Events) != 2000 || p.BeforeSeq == nil || *p.BeforeSeq != 11 || len(p.Context) != 1 || *p.Context[0].Seq != 1 {
		t.Fatal("unbounded run or missing original context")
	}
	older, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100, before: *p.BeforeSeq})
	if err != nil {
		t.Fatal(err)
	}
	if len(older.Events) != 10 || *older.Events[9].Seq != 10 || older.HasMore {
		t.Fatal("tool-only interval disappeared at cursor")
	}
}

func TestBrowserHistorySameInodeRewriteInvalidatesIndex(t *testing.T) {
	g := openRuntimeGateway(t)
	historyAppend(t, g, 1, map[string]any{"type": "agent_start"})
	historyAppend(t, g, 2, historyMessage(1))
	before, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	path := g.eventPath(historyPA)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.ReplaceAll(strings.ReplaceAll(string(raw), "Message 1", "Message 9"), "8000-000000000001", "8000-000000000009"))
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	after, err := g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if after.Index[0].ID == before.Index[0].ID || after.Index[0].Title != "Message 9" {
		t.Fatal("same-size replacement served stale index")
	}
	// Rewrite again and regrow beyond the cached size before the next request.
	raw = []byte(strings.ReplaceAll(strings.ReplaceAll(string(raw), "Message 9", "Message 8"), "8000-000000000009", "8000-000000000008"))
	seq := uint64(3)
	event, _ := json.Marshal(historyMessage(2))
	extra, _ := json.Marshal(durableEventRecord{Seq: seq, Event: Envelope{Audience: AudienceDirectChat, Seq: &seq, PersonalityAgentID: historyPA, Event: event}})
	raw = append(raw, append(extra, '\n')...)
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	after, err = g.browserHistory(context.Background(), historyPA, browserHistoryQuery{limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Index) != 2 || after.Index[0].Title != "Message 8" {
		t.Fatal("regrown replacement served stale index")
	}
}

func TestBrowserHistoryOutstandingReceiptsDoNotDependOnBodyWindow(t *testing.T) {
	g := openRuntimeGateway(t)
	const rejected = "00000000-0000-4000-8000-000000000001"
	const superseded = "00000000-0000-4000-8000-000000000002"
	const unrelated = "00000000-0000-4000-8000-000000000003"
	const unknown = "00000000-0000-4000-8000-000000000004"
	historyAppend(t, g, 1, map[string]any{"type": "command_disposition", "command_id": rejected, "command_seq": 1, "status": "rejected", "reject_reason": "not_allowed"})
	historyAppend(t, g, 2, map[string]any{"type": "command_disposition", "command_id": superseded, "command_seq": 2, "status": "superseded"})
	historyAppend(t, g, 3, map[string]any{"type": "command_disposition", "command_id": unrelated, "command_seq": 3, "status": "applied"})
	for n := 1; n <= 105; n++ {
		historyAppend(t, g, uint64(n+3), historyMessage(n))
	}
	q, err := historyQuery(httptest.NewRequest(http.MethodGet, "/direct-chat/history?command_id="+rejected+"&command_id="+superseded+"&command_id="+unknown+"&include_index=false", nil))
	if err != nil {
		t.Fatal(err)
	}
	page, err := g.browserHistory(context.Background(), historyPA, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 100 || *page.Events[0].Seq <= 3 || page.LatestSeq != 108 {
		t.Fatal("receipts expanded bodywindow or changed head")
	}
	if len(page.CommandDispositions) != 2 || *page.CommandDispositions[0].Seq != 1 || *page.CommandDispositions[1].Seq != 2 {
		t.Fatal("missing requested historical outcomes or unrelated outcome leaked")
	}
	if len(page.Index) != 0 {
		t.Fatal("omitted index repeated")
	}
	raw, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"command_dispositions":[`) {
		t.Fatal("receipt wire field missing")
	}
}
func TestBrowserHistoryReceiptQueryIsBounded(t *testing.T) {
	const id = "00000000-0000-4000-8000-000000000001"
	for _, query := range []string{"command_id=bad", "command_id=" + id + "&command_id=" + id, "include_index=maybe", strings.Repeat("command_id="+id+"&", 101)} {
		if _, err := historyQuery(httptest.NewRequest(http.MethodGet, "/direct-chat/history?"+query, nil)); err == nil {
			t.Fatalf("invalid query accepted: %.80s", query)
		}
	}
}
