package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

// An unreadable ledger is unknown, not proof that this install has no copy.
func TestReturnElsewhereUnreadableLedger(t *testing.T) {
	h := setupReturn(t)
	_, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitDone {
		t.Fatalf("install 1 return: %d\n%s", code, out)
	}
	other := h.secondInstall(t)
	m2, out2 := other.mover()
	if code := m2.ReturnStart(h.ctx, returnURL, other.p.pool, other.config, false); code != exitError {
		t.Fatalf("install 2 return: %d\n%s", code, out2)
	}
	before := other.record(t)
	if before.Outcome != outcomeElsewhere {
		t.Fatalf("outcome %q", before.Outcome)
	}

	// A closed pool makes every ledger read fail deterministically without
	// depending on an unused network port or modifying either live fixture DB.
	unreadable, err := pgxpool.NewWithConfig(h.ctx, other.p.pool.Config())
	if err != nil {
		t.Fatal(err)
	}
	unreadable.Close()
	var output bytes.Buffer
	m3 := newMover(other.home, other.slot, portable.NewService(unreadable), agentstate.NewStore(unreadable), &output)
	m3.wait, m3.poll, m3.unreachable, m3.sealRetries, m3.sealDelay = 0, 10*time.Millisecond, 300*time.Millisecond, 2, 10*time.Millisecond

	for _, command := range []struct {
		name string
		run  func() int
	}{
		{"cancel", func() int { return m3.ReturnCancel(h.ctx, unreadable, other.config) }},
		{"status", func() int { return m3.ReturnStatus(h.ctx, unreadable, other.config) }},
		{"resume", func() int { return m3.ReturnResume(h.ctx, unreadable, other.config, false) }},
		{"same URL", func() int { return m3.ReturnStart(h.ctx, returnURL, unreadable, other.config, false) }},
	} {
		t.Run(command.name, func(t *testing.T) {
			output.Reset()
			if code := command.run(); code != exitError {
				t.Fatalf("exit %d, want %d\n%s", code, exitError, &output)
			}
			if strings.Contains(output.String(), "holds no activated copy") || !strings.Contains(output.String(), "closed pool") {
				t.Errorf("must report the ledger failure instead of asserting absence:\n%s", &output)
			}
			if got := other.record(t); got.Outcome != before.Outcome || got.CompletedAt != before.CompletedAt {
				t.Errorf("failed read changed recorded completion: %+v", got)
			}
		})
	}
}
