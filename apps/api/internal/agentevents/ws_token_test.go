package agentevents

import (
	"testing"
	"time"
)

func TestWebSocketRealTokenHelloAndCatchUp(t *testing.T) {
	claims := tokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:         7,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultAgentAudience,
	}
	srv, cs, token := newTokenVerifiedTestServer(t, claims)

	cmd := testCommandEnvelope(1, "00000000-0000-4000-8000-000000000001", []byte(`{"type":"user_message","text":"hi","attachments":[]}`), claims.PersonalityAgentID)
	cs.pushCommand(cmd)

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer " + token}}
	conn, resp, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v (status %d)", err, resp.StatusCode)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             7,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err != nil {
		t.Fatalf("read api hello: %v", err)
	}
	if apiHello.AcceptedGeneration != 7 || apiHello.NextCommandSeq != 1 {
		t.Fatalf("unexpected api hello: %+v", apiHello)
	}

	var received CommandEnvelope
	if err := conn.ReadJSON(&received); err != nil {
		t.Fatalf("read command: %v", err)
	}
	if received.Seq != 1 {
		t.Fatalf("unexpected command seq: %d", received.Seq)
	}
}

func TestWebSocketRealTokenExpiredIsRejected(t *testing.T) {
	claims := tokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:         7,
		Exp:                time.Now().Add(-time.Hour).Unix(),
		Aud:                defaultAgentAudience,
	}
	srv, _, token := newTokenVerifiedTestServer(t, claims)

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer " + token}}
	_, resp, err := dialTestWS(t, server, header)
	if err == nil {
		t.Fatal("expected expired token to be rejected before upgrade")
	}
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestWebSocketRealTokenHelloGenerationMismatchCloses(t *testing.T) {
	claims := tokenClaims{
		TenantID:           "tenant-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:         7,
		Exp:                time.Now().Add(time.Hour).Unix(),
		Aud:                defaultAgentAudience,
	}
	srv, _, token := newTokenVerifiedTestServer(t, claims)

	server := startTestServer(t, srv)
	defer server.Close()

	header := map[string][]string{"Authorization": {"Bearer " + token}}
	conn, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(AgentHello{
		PersonalityAgentID:     "018f47a2-9b3c-7def-8abc-0123456789ab",
		Generation:             99,
		LastSentEventSeq:       0,
		LastReceivedCommandSeq: 0,
		LastAppliedCommandSeq:  0,
	}); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var apiHello ApiHello
	if err := conn.ReadJSON(&apiHello); err == nil {
		t.Fatal("expected connection to close on generation claim mismatch")
	}
}

func newTokenVerifiedTestServer(t *testing.T, claims tokenClaims) (*Server, *fakeCommandSource, string) {
	t.Helper()
	token := signTestToken(t, testSecret, claims)
	v, err := NewHMACTokenVerifier(testSecret, "")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	gv := &fakeGenerationVerifier{latest: claims.Generation}
	cs := newFakeCommandSource()
	es := &fakeEventSink{}
	hl := newFakeHydrationLatch()
	hl.setReady()
	srv := NewServer(v, gv, cs, es, hl)
	return srv, cs, token
}
