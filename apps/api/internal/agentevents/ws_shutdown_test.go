package agentevents

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestGatewayShutdownDrainsSocketsAndAllowsSameGenerationOnReplacement(t *testing.T) {
	srv, _, _, commands, events, latch := newTestServer(t)
	latch.setReady()
	server := startTestServer(t, srv)
	defer server.Close()
	header := map[string][]string{"Authorization": {"Bearer test-token"}}
	first, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	writeTestAgentHello(t, first, 7)
	var hello ApiHello
	if err := conn2Wait(first, &hello, time.Second); err != nil {
		t.Fatal(err)
	}
	// Also own a socket still waiting for its hello.
	uninitialized, _, err := dialTestWS(t, server, header)
	if err != nil {
		t.Fatal(err)
	}
	defer uninitialized.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.ShutdownConnections(ctx); err != nil {
		t.Fatal(err)
	}
	srv.connectionsMu.Lock()
	handlers, connections := len(srv.handlers), len(srv.connections)
	srv.connectionsMu.Unlock()
	if handlers != 0 || connections != 0 {
		t.Fatalf("shutdown retained handlers=%d connections=%d", handlers, connections)
	}
	if err := conn2Wait(first, &hello, time.Second); err == nil {
		t.Fatal("old socket survived API shutdown")
	}
	if _, response, err := dialTestWS(t, server, header); err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("closing API did not report temporary unavailability")
	}
	next := NewServer(srv.Token, srv.Generation, commands, events, latch)
	next.AllowedOrigins = srv.AllowedOrigins
	replacement := startTestServer(t, next)
	defer replacement.Close()
	second, _, err := dialTestWS(t, replacement, header)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	writeTestAgentHello(t, second, 7)
	if err := conn2Wait(second, &hello, time.Second); err != nil {
		t.Fatalf("same generation could not reconnect: %v", err)
	}
	seq := uint64(1)
	if err := second.WriteJSON(OutboundFrame{FrameType: "event", Envelope: &Envelope{Audience: AudienceDirectChat, Seq: &seq, PersonalityAgentID: testPersonalityAgentID, Event: []byte(`{"type":"agent_start"}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := second.WriteJSON(OutboundFrame{FrameType: "command_ack", Ack: &CommandAck{PersonalityAgentID: testPersonalityAgentID, Seq: 1, CommandID: "00000000-0000-4000-8000-000000000001", Status: "received"}}); err != nil {
		t.Fatal(err)
	}
	waitForFakeSideEffects(t, events, commands, 1, 1)
	if err := next.ShutdownConnections(ctx); err != nil {
		t.Fatal(err)
	}
}
