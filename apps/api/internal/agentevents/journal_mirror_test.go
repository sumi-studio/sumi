package agentevents

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/journalmirror"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

type mirroredHost struct {
	mirror   *journalmirror.Mirror
	commands *CommandStore
	journal  *BrowserJournal
}

// openMirroredHost starts an API host's journals on fresh, empty directories
// (a replaced container's disk), restoring them from the mirror.
func openMirroredHost(t *testing.T, mirror *journalmirror.Mirror) mirroredHost {
	t.Helper()
	ctx := context.Background()
	commandDir, journalDir := t.TempDir(), privateRuntimeDir(t)
	if _, err := mirror.Attach(ctx, "commands", commandDir); err != nil {
		t.Fatal(err)
	}
	if _, err := mirror.Attach(ctx, "browser-events", journalDir); err != nil {
		t.Fatal(err)
	}
	commands, err := OpenMirroredCommandStore(commandDir, mirror)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = commands.Close() })
	journal, err := OpenMirroredBrowserJournal(journalDir, commands, mirror)
	if err != nil {
		t.Fatal(err)
	}
	journal.PollInterval = 5 * time.Millisecond
	return mirroredHost{mirror: mirror, commands: commands, journal: journal}
}

// Everything the API acknowledged on one host — an admitted Direct Chat
// command and its idempotency key, the visible conversation events, and a
// logged-out browser session — is present on a replacement host that starts
// with empty disks. The replaced host can no longer acknowledge writes.
func TestMirroredJournalsSurviveHostReplacement(t *testing.T) {
	pool := testdb.Create(t)
	ctx := context.Background()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	first, err := journalmirror.Acquire(ctx, pool, "host-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	a := openMirroredHost(t, first)

	command := json.RawMessage(`{"type":"user_message","text":"remember the dentist on Friday","attachments":[]}`)
	admitted, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-1", command)
	if err != nil {
		t.Fatal(err)
	}
	seedProjectedRun(t, a.journal, historyPA, 1, 5)
	eventsA, err := a.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsA) == 0 {
		t.Fatal("host A has no events")
	}
	rawSession := make([]byte, browserSessionIDBytes)
	if _, err := rand.Read(rawSession); err != nil {
		t.Fatal(err)
	}
	session := base64.RawURLEncoding.EncodeToString(rawSession)
	now := time.Now()
	expires := now.Add(time.Hour)
	if err := a.journal.RevokeBrowserSession(ctx, session, expires, now); err != nil {
		t.Fatal(err)
	}

	second, err := journalmirror.Acquire(ctx, pool, "host-b", nil)
	if err != nil {
		t.Fatal(err)
	}
	b := openMirroredHost(t, second)

	replay, err := b.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-1", command)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Seq != admitted.Seq || replay.CommandID != admitted.CommandID {
		t.Fatalf("idempotent replay on host B = seq %d id %s, want seq %d id %s", replay.Seq, replay.CommandID, admitted.Seq, admitted.CommandID)
	}
	next, err := b.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-2", command)
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != admitted.Seq+1 {
		t.Fatalf("next seq on host B = %d, want %d", next.Seq, admitted.Seq+1)
	}
	eventsB, err := b.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(eventsA, eventsB) {
		t.Fatalf("host B events differ: %d vs %d", len(eventsB), len(eventsA))
	}
	seedProjectedRun(t, b.journal, historyPA, 6, 2)
	if err := b.journal.CheckBrowserSession(ctx, session, expires, time.Now()); !errors.Is(err, errBrowserSessionRevoked) {
		t.Fatalf("logged-out session on host B = %v, want revoked", err)
	}

	if _, err := a.commands.Append(ctx, testDirectChatProvenance(historyPA), "idem-3", command); err == nil {
		t.Fatal("replaced host A still acknowledged a command")
	}
	if !first.Fenced() {
		t.Fatal("host A was not fenced")
	}

	third, err := journalmirror.Acquire(ctx, pool, "host-c", nil)
	if err != nil {
		t.Fatal(err)
	}
	c := openMirroredHost(t, third)
	eventsC, err := c.journal.EventCatchUp(ctx, historyPA, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(eventsC) <= len(eventsA) {
		t.Fatalf("host C sees %d events; host B's later run is missing", len(eventsC))
	}
	if seq, err := c.commands.NextCommandSeq(ctx, historyPA); err != nil || seq != next.Seq+1 {
		t.Fatalf("host C next command seq = %d, %v; want %d", seq, err, next.Seq+1)
	}
}
