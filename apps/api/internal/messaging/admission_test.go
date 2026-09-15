package messaging

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// shrinkAdmission swaps the process limiter for one whose bucket drains
// almost never, so tests can empty it deterministically.
func shrinkAdmission(store *Store, ratePerSecond, burst float64) {
	a := newOperationAdmission()
	a.rate = ratePerSecond
	a.burst = burst
	store.admission = a
}

func TestSendAdmissionReplaySurvivesEmptyBucket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScopeForActor(t, ctx, w.humanA)
	shrinkAdmission(w.store.core, 0.0001, 1)

	in := AppendInput{
		PlaceID: ch.PlaceID, Content: "committed once", ClientNonce: "survive-throttle",
	}
	first, created, err := scoped.AppendMessage(ctx, in)
	if err != nil || !created {
		t.Fatalf("first send: created=%v err=%v", created, err)
	}
	// Bucket empty: a new nonce is refused.
	var limited *RateLimitedError
	if _, _, err := scoped.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "new work", ClientNonce: "new-work",
	}); !errors.As(err, &limited) {
		t.Fatalf("second send on empty bucket: err=%v, want RateLimitedError", err)
	}
	if limited.RetryAfter <= 0 {
		t.Fatalf("rate limited without a retry hint: %+v", limited)
	}
	// The committed retry still reaches its receipt while throttled.
	replay, created, err := scoped.AppendMessage(ctx, in)
	if err != nil || created {
		t.Fatalf("replay under empty bucket: created=%v err=%v", created, err)
	}
	if replay.MessageID != first.MessageID || replay.Seq != first.Seq {
		t.Fatalf("replay returned a different receipt: %+v vs %+v", replay, first)
	}
	// Nothing durable grew: one message, seq still 1.
	place, err := w.store.PlaceFor(ctx, ch.PlaceID, w.humanA)
	if err != nil {
		t.Fatalf("reload place: %v", err)
	}
	if place.LastSeq != 1 {
		t.Fatalf("rejected send consumed a sequence: last_seq=%d", place.LastSeq)
	}
	var messages, intents int
	if err := w.store.core.pool.QueryRow(ctx,
		"SELECT count(*) FROM messages WHERE place_id = $1", ch.PlaceID).Scan(&messages); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	// One committed message fans out one intent per notifiable member; every
	// intent must belong to that message — the rejected send issued none.
	if err := w.store.core.pool.QueryRow(ctx, `
		SELECT count(*) FROM message_notification_intents i
		JOIN messages m ON m.message_id = i.message_id
		WHERE m.place_id = $1 AND i.message_id <> $2`,
		ch.PlaceID, first.MessageID).Scan(&intents); err != nil {
		t.Fatalf("count intents: %v", err)
	}
	if messages != 1 || intents != 0 {
		t.Fatalf("rejected send left residue: messages=%d stray_intents=%d", messages, intents)
	}
}

func TestSendAdmissionDifferentActorSameNonceKeepsOwnReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	humanA := w.store.mustScopeForActor(t, ctx, w.humanA)
	humanB := w.store.mustScopeForActor(t, ctx, w.humanB)

	a, created, err := humanA.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "from A", ClientNonce: "shared-nonce",
	})
	if err != nil || !created {
		t.Fatalf("A send: created=%v err=%v", created, err)
	}
	// The nonce key is (place, actor, nonce): B's identical nonce is B's own
	// operation, not a replay of A's receipt and not a conflict.
	b, created, err := humanB.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "from A", ClientNonce: "shared-nonce",
	})
	if err != nil || !created {
		t.Fatalf("B send under A's nonce: created=%v err=%v", created, err)
	}
	if b.MessageID == a.MessageID || b.Seq == a.Seq {
		t.Fatalf("B replayed A's receipt: %+v", b)
	}
	// And A's replay still returns A's receipt.
	replay, created, err := humanA.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "from A", ClientNonce: "shared-nonce",
	})
	if err != nil || created || replay.MessageID != a.MessageID {
		t.Fatalf("A replay after B: created=%v err=%v id=%v", created, err, replay.MessageID)
	}
}

func TestSendAdmissionAppliesToAgentLane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	agent := w.store.mustScopeForActor(t, ctx, w.agent)
	shrinkAdmission(w.store.core, 0.0001, 1)

	if _, created, err := agent.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "agent first", ClientNonce: "agent-1",
	}); err != nil || !created {
		t.Fatalf("agent first send: created=%v err=%v", created, err)
	}
	var limited *RateLimitedError
	if _, _, err := agent.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "agent second", ClientNonce: "agent-2",
	}); !errors.As(err, &limited) {
		t.Fatalf("agent send on empty bucket: err=%v, want RateLimitedError", err)
	}
}

// countingReader proves the upload path reads zero body bytes when the
// pre-body admission phases refuse.
type countingReader struct{ reads atomic.Int64 }

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	copy(p, "x")
	return 1, nil
}

func TestUploadAuthorizationFailureReadsNoBody(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	f := newAttachmentWorld(t, ctx, AttachmentPolicy{})
	ws, ch := f.workspaceWithChannel(t, ctx)
	// Scope resolves against the workspace's installation, but this
	// participant was never added as a member: authorization must fail
	// before any body byte is read.
	stranger, err := koseki.New(f.store.pool).MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint stranger: %v", err)
	}
	outsider := f.store.mustScope(t, ctx, ws.WorkspaceID, Human(stranger))
	body := &countingReader{}
	req := attachmentUploadRequest{
		placeID: ch.PlaceID, clientNonce: "denied-body", filename: "x.txt",
		declaredMIME: "text/plain", declaredSize: 4,
	}
	_, _, _, err = uploadAttachment(ctx, outsider, req, admitAlways, nil, body)
	if err == nil {
		t.Fatal("upload by a non-member must fail")
	}
	if body.reads.Load() != 0 {
		t.Fatalf("denied upload read %d body reads", body.reads.Load())
	}
}

// wsDial opens one messaging socket through the given authorizer.
func wsDial(t *testing.T, ts *httptest.Server, scoped *ScopedStore) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/messaging/ws?workspace_id=" +
		scoped.Scope.WorkspaceID + "&installation_id=" + scoped.Scope.InstallationID +
		"&authority_epoch=" + strconv.FormatInt(scoped.Scope.AuthorityEpoch, 10)
	header := http.Header{
		"Origin": {testOrigin},
		"Cookie": {agentevents.BrowserSessionCookie + "=lease-test"},
	}
	return websocket.DefaultDialer.Dial(url, header)
}

func TestWSConnectionLeaseCapsAndReleases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, _ = w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScopeForActor(t, ctx, w.humanA)
	sessions := &blockingMessagingSessionAuthorizer{
		claims:  agentevents.UserSessionClaims{UserID: w.humanA.ID},
		started: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	close(sessions.release) // session admission lets every upgrade through
	ws := NewWSServer(w.store.core, sessions, NewHub(w.store.core))
	ws.AllowedOrigins = []string{testOrigin}
	// A two-slot scope cap makes the boundary cheap to hit.
	w.store.core.admission.wsScopeCap = 2
	ts := httptest.NewServer(ws)
	defer ts.Close()

	var conns []*websocket.Conn
	for i := 0; i < 2; i++ {
		conn, resp, err := wsDial(t, ts, scoped)
		if err != nil {
			t.Fatalf("dial %d: %v (status %v)", i, err, resp)
		}
		conns = append(conns, conn)
	}
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	// Third connection is refused before upgrade with the shared 429 shape.
	conn, resp, err := wsDial(t, ts, scoped)
	if conn != nil {
		conn.Close()
	}
	if err == nil || resp == nil || resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("over-cap dial: resp=%v err=%v, want 429", resp, err)
	}
	if resp.Header.Get("Retry-After") == "" || resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("over-cap response missing retry contract: %v", resp.Header)
	}
	// The refused attempt holds nothing: a released slot readmits.
	_ = conns[0].Close()
	conns = conns[1:]
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, resp, err = wsDial(t, ts, scoped)
		if err == nil {
			conns = append(conns, conn)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("released slot never readmitted: %v (status %v)", err, resp)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestWSRateLimitedSendKeepsSocketAndReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	scoped := w.store.mustScopeForActor(t, ctx, w.humanA)
	shrinkAdmission(w.store.core, 0.0001, 1)
	sessions := &blockingMessagingSessionAuthorizer{
		claims:  agentevents.UserSessionClaims{UserID: w.humanA.ID},
		started: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	close(sessions.release) // admit immediately
	ws := NewWSServer(w.store.core, sessions, NewHub(w.store.core))
	ws.AllowedOrigins = []string{testOrigin}
	ts := httptest.NewServer(ws)
	defer ts.Close()

	conn, resp, err := wsDial(t, ts, scoped)
	if err != nil {
		t.Fatalf("dial: %v (status %v)", err, resp)
	}
	defer conn.Close()
	// readControl skips broadcast event frames (e.g. the message_created
	// event for a committed send) until a control frame arrives.
	readControl := func() map[string]any {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			_ = conn.SetReadDeadline(deadline)
			var frame map[string]any
			if err := conn.ReadJSON(&frame); err != nil {
				t.Fatalf("read frame: %v", err)
			}
			if frame["type"] != "event" {
				return frame
			}
		}
	}
	send := func(nonce, content string) {
		t.Helper()
		if err := conn.WriteJSON(map[string]any{
			"type": "send", "place_id": ch.PlaceID,
			"content": content, "client_nonce": nonce,
		}); err != nil {
			t.Fatalf("send frame: %v", err)
		}
	}
	// Complete the hello handshake first.
	if err := conn.WriteJSON(map[string]any{"type": "hello", "cursors": map[string]int64{}}); err != nil {
		t.Fatalf("hello frame: %v", err)
	}
	if f := readControl(); f["type"] != "hello_ack" {
		t.Fatalf("first frame: %v", f)
	}
	send("ws-once", "first")
	f := readControl()
	if f["type"] != "receipt" || f["created"] != true {
		t.Fatalf("first send: %v", f)
	}
	msgID, seq := f["message_id"], f["seq"]
	// Bucket empty: a new send is refused without closing the socket.
	send("ws-new", "second")
	f = readControl()
	if f["type"] != "error" || f["code"] != "rate_limited" || f["client_nonce"] != "ws-new" {
		t.Fatalf("rate limited frame: %v", f)
	}
	if ms, _ := f["retry_after_ms"].(float64); ms <= 0 {
		t.Fatalf("rate limited frame missing retry_after_ms: %v", f)
	}
	// The socket is still alive and the committed nonce still receipts.
	send("ws-once", "first")
	f = readControl()
	if f["type"] != "receipt" || f["created"] != false ||
		f["message_id"] != msgID || f["seq"] != seq {
		t.Fatalf("replay over live socket: %v", f)
	}
}
