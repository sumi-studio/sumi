package agentstate

import (
	"context"
	"testing"
	"time"
)

// A re-drained chunk that overlaps stored output (a lost commit reply,
// then more output) keeps its new tail instead of being dropped whole on
// the (session, base) dedupe.
func TestAppendTerminalOutputKeepsTailOfOverlappingChunk(t *testing.T) {
	s := newTerminalStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	sess := mustTerminalSession(t, s, pa, "sh")
	epoch := mustClaimTerminal(t, s, pa, "runner-o", time.Minute).Epoch
	appendData := func(base int64, data string) TerminalSession {
		t.Helper()
		out, err := s.AppendTerminalOutput(ctx, pa, sess.SessionID, "runner-o", epoch, []TerminalOutputChunk{{Kind: "data", Base: base, Data: []byte(data)}})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	appendData(0, "abc")
	appendData(0, "abc")                                   // exact replay: no-op
	appendData(0, "abcdef")                                // overlapping replay with a new tail
	appendData(2, "cdefgh")                                // mid-chunk overlap
	if out := appendData(3, "def"); out.OutputBytes != 8 { // fully covered
		t.Fatalf("output_bytes = %d", out.OutputBytes)
	}
	read, err := s.ReadTerminalOutput(ctx, pa, sess.SessionID, 0, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	var got string
	next := int64(0)
	for _, c := range read.Chunks {
		if c.Base != next {
			t.Fatalf("non-contiguous chunk at %d, want %d", c.Base, next)
		}
		got += string(c.Data)
		next = c.Base + int64(len(c.Data))
	}
	if got != "abcdefgh" {
		t.Fatalf("scrollback = %q", got)
	}
}
