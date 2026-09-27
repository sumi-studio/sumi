package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// agedClock makes every event file look long quiet, so a verified
// fingerprint is trusted without the test sleeping out eventStatTrustAge.
func agedClock() time.Time { return time.Now().Add(time.Hour) }

// requireEventStatFastPath skips tests of the metadata fast path on a test
// filesystem outside its contract, where every refresh re-verifies in full.
func requireEventStatFastPath(t *testing.T, g *DurableGateway) {
	t.Helper()
	if !g.eventStatFastPath {
		t.Skip("test directory filesystem is outside the event-log metadata fast path contract")
	}
}

// eventFileChangeTime returns the kernel-stamped ctime of path.
func eventFileChangeTime(t *testing.T, path string) time.Time {
	t.Helper()
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		t.Fatal(err)
	}
	return time.Unix(0, stat.Ctim.Nano())
}

// countEventLogReads counts bytes read from personalityAgentID's event log
// through g from now on.
func countEventLogReads(g *DurableGateway, personalityAgentID string) *atomic.Int64 {
	var n atomic.Int64
	open := g.newFile
	path := g.eventPath(personalityAgentID)
	g.newFile = func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		file, err := open(name, flag, perm)
		if err != nil || name != path {
			return file, err
		}
		return &countingDurableFile{durableFileHandle: file, readBytes: &n}, nil
	}
	return &n
}

// seedProjectedRun commits one closed run of n messages through the
// projection append path.
func seedProjectedRun(t *testing.T, g *DurableGateway, personalityAgentID string, first, n int) {
	t.Helper()
	batch := []ProjectedEvent{{RunMarker: RunMarkerStart}}
	for i := first; i < first+n; i++ {
		raw, err := json.Marshal(historyMessage(i))
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, ProjectedEvent{Event: raw})
	}
	batch = append(batch, ProjectedEvent{RunMarker: RunMarkerEnd})
	if err := g.AppendProjectedEvents(context.Background(), personalityAgentID, batch); err != nil {
		t.Fatalf("seed projected run: %v", err)
	}
}

// The direct-chat projector asks the gateway to close an idle run every
// sweep. With nothing committed since the last verification, that check must
// not re-read the whole lifetime log each time.
func TestIdleEventTailRefreshDoesNotRereadLog(t *testing.T) {
	g := openRuntimeGateway(t)
	requireEventStatFastPath(t, g)
	g.nowHook = agedClock
	seedProjectedRun(t, g, historyPA, 1, 64)
	ctx := context.Background()
	if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(g.eventPath(historyPA))
	if err != nil {
		t.Fatal(err)
	}
	before := g.observedEventTail(historyPA)
	reads := countEventLogReads(g, historyPA)
	for i := 0; i < 5; i++ {
		mark := g.observedEventTail(historyPA)
		if err := g.AppendProjectedEvents(ctx, historyPA,
			[]ProjectedEvent{{RunMarker: RunMarkerEnd, IfTail: &mark}}); err != nil {
			t.Fatal(err)
		}
		if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
			t.Fatal(err)
		}
	}
	if got := reads.Load(); got != 0 {
		t.Fatalf("idle refreshes read %d bytes of a %d-byte unchanged event log; want 0", got, info.Size())
	}
	if after := g.observedEventTail(historyPA); after != before {
		t.Fatalf("idle refresh moved the committed tail: %+v -> %+v", before, after)
	}
}

// A trusted fingerprint only stands in for an unchanged file: another
// writer's append must still be folded into the guards, and a later
// same-size rewrite must still invalidate the cached tail.
func TestTrustedEventTailStillSeesOtherWritersAndRewrites(t *testing.T) {
	g := openRuntimeGateway(t)
	requireEventStatFastPath(t, g)
	g.nowHook = agedClock
	seedProjectedRun(t, g, historyPA, 1, 8)
	ctx := context.Background()
	if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
		t.Fatal(err)
	}
	if !g.stateFor(historyPA).eventStatTrusted {
		t.Fatal("a verified, aged event file should be trusted")
	}

	// Another API process over the same durable directory commits a pending
	// approval inside a run.
	other, err := OpenDurableGateway(g.dir, g.commands)
	if err != nil {
		t.Fatal(err)
	}
	approval, err := marshalEvent(map[string]any{
		"type": "approval_requested",
		"request": map[string]any{
			"id": "approval-1", "tool_call_id": "call-1", "tool_name": "journal.note",
			"action": map[string]any{"reviewable": map[string]any{}}, "args_summary": map[string]any{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.AppendProjectedEvents(ctx, historyPA,
		[]ProjectedEvent{{RunMarker: RunMarkerStart}, {Event: approval}}); err != nil {
		t.Fatal(err)
	}
	if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
		t.Fatal(err)
	}
	if got, want := g.observedEventTail(historyPA), other.observedEventTail(historyPA); got != want {
		t.Fatalf("trusted tail missed another writer's append: got %+v want %+v", got, want)
	}
	if !g.IsApprovalPending(historyPA, "approval-1") || !g.IsRunInFlight(historyPA) {
		t.Fatal("another writer's committed approval/run was not folded into the guards")
	}

	// Let the rewrite land in a later filesystem timestamp tick than the
	// verified append, then replace content without changing size.
	time.Sleep(50 * time.Millisecond)
	path := g.eventPath(historyPA)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	replaced := bytes.Replace(original, []byte("agent_start"), []byte("agent_stXrt"), 1)
	if len(replaced) != len(original) || bytes.Equal(replaced, original) {
		t.Fatal("test replacement must change content without changing file size")
	}
	if err := os.WriteFile(path, replaced, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := g.RefreshDurableEventTail(ctx, historyPA); err == nil {
		t.Fatal("same-size rewrite after a trusted fingerprint must invalidate the cached tail")
	}
}

// End to end: an idle projector stops re-reading the log, and new work,
// dispositions, run boundaries, and a restarted projector behave exactly as
// before.
func TestCoreDirectChatIdleSweepDoesNotRereadEventLog(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	requireEventStatFastPath(t, f.gateway)
	f.gateway.nowHook = agedClock
	first := f.sendMessage(t, "idle-1", "first")
	f.sweep(t)
	f.hostComplete(t, "direct-chat:"+first.CommandID, "first reply")
	f.sweep(t)
	// The first quiet sweep re-verifies once after the projector's own
	// appends; from then on the log is unchanged.
	f.sweep(t)
	settled := len(durableEvents(t, f.gateway, f.pa))

	reads := countEventLogReads(f.gateway, f.pa)
	for i := 0; i < 5; i++ {
		f.sweep(t)
	}
	if got := reads.Load(); got != 0 {
		t.Fatalf("idle sweeps read %d bytes of the unchanged event log; want 0", got)
	}
	if got := len(durableEvents(t, f.gateway, f.pa)); got != settled {
		t.Fatalf("idle sweeps committed events: %d -> %d", settled, got)
	}

	second := f.sendMessage(t, "idle-2", "second")
	f.sweep(t)
	f.hostComplete(t, "direct-chat:"+second.CommandID, "second reply")
	f.sweep(t)
	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	assertEnvelopeIntegrity(t, events)
	if got := strings.Join(messageTexts(events, "assistant"), "|"); got != "first reply|second reply" {
		t.Fatalf("assistant replies = %q", got)
	}
	types := eventTypes(events)
	if countEvent(types, "agent_start") != 2 || countEvent(types, "agent_end") != 2 {
		t.Fatalf("run markers = %v", types)
	}
	wantDisposed := map[string]int{first.CommandID: 1, second.CommandID: 1}
	for _, d := range commandDispositions(t, f.gateway, f.pa) {
		id, _ := d["command_id"].(string)
		wantDisposed[id]--
		if d["status"] != "applied" {
			t.Fatalf("disposition %v", d)
		}
	}
	for id, left := range wantDisposed {
		if left != 0 {
			t.Fatalf("command %s dispositions off by %d", id, -left)
		}
	}

	// A restarted projector over the same files re-verifies from scratch and
	// re-emits nothing.
	restarted, gw := f.newProjector(t)
	gw.nowHook = agedClock
	for i := 0; i < 3; i++ {
		syncOn(t, restarted, f.pa)
	}
	if got, want := len(durableEvents(t, gw, f.pa)), len(events); got != want {
		t.Fatalf("restarted projector changed the log: %d -> %d events", want, got)
	}
	if n := len(commandDispositions(t, gw, f.pa)); n != 2 {
		t.Fatalf("dispositions after restart = %d, want 2", n)
	}
}

// Metadata written less than eventStatTrustAge before verification is not
// trusted: such a file stays on the full validation path, refresh after
// refresh, and crosses to the fast path only once it has been quiet past the
// threshold. A wall clock that goes back before the verification instant
// drops the trust again.
func TestFreshEventFileStaysOnFullValidationUntilAged(t *testing.T) {
	g := openRuntimeGateway(t)
	requireEventStatFastPath(t, g)
	seedProjectedRun(t, g, historyPA, 1, 16)
	ctx := context.Background()
	path := g.eventPath(historyPA)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	size := info.Size()
	changed := eventFileChangeTime(t, path)
	var now time.Time
	g.nowHook = func() time.Time { return now }
	reads := countEventLogReads(g, historyPA)
	tail := g.observedEventTail(historyPA)
	refresh := func(step string, wantRead, wantTrusted bool) {
		t.Helper()
		before := reads.Load()
		if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		read := reads.Load() - before
		if wantRead && read < size {
			t.Fatalf("%s: read %d bytes of a %d-byte log; want a full re-verification", step, read, size)
		}
		if !wantRead && read != 0 {
			t.Fatalf("%s: read %d bytes; want the trusted fast path", step, read)
		}
		if got := g.stateFor(historyPA).eventStatTrusted; got != wantTrusted {
			t.Fatalf("%s: trusted = %v, want %v", step, got, wantTrusted)
		}
		if got := g.observedEventTail(historyPA); got != tail {
			t.Fatalf("%s: tail moved on an unchanged log: %+v -> %+v", step, tail, got)
		}
	}

	now = changed.Add(eventStatTrustAge * 3 / 4)
	for i := 0; i < 3; i++ {
		refresh("fresh metadata", true, false)
	}
	now = changed.Add(eventStatTrustAge)
	refresh("metadata exactly at the trust age", true, false)
	now = changed.Add(eventStatTrustAge + time.Nanosecond)
	refresh("first refresh past the trust age", true, true)
	refresh("aged, unchanged metadata", false, true)
	now = now.Add(time.Minute)
	refresh("clock moved forward", false, true)

	// The clock steps back before the verification instant: stop trusting,
	// and keep verifying in full while the file looks newer than the clock.
	now = changed.Add(-time.Minute)
	refresh("clock stepped back", true, false)
	refresh("clock still behind the file", true, false)
	now = changed.Add(eventStatTrustAge + time.Second)
	refresh("clock caught up", true, true)
	refresh("clock caught up, unchanged", false, true)
}

// Off the supported filesystems every refresh re-verifies the full log, as
// before the fast path existed.
func TestEventStatFastPathOffUnsupportedFilesystems(t *testing.T) {
	for _, tc := range []struct {
		name  string
		magic uint32
		want  bool
	}{
		{"ext4", 0xEF53, true},
		{"xfs", 0x58465342, true},
		{"btrfs", 0x9123683E, true},
		{"tmpfs", 0x01021994, true},
		{"overlayfs", 0x794C7630, true},
		{"nfs", 0x6969, false},
		{"smb2", 0xFE534D42, false},
		{"cifs", 0xFF534D42, false},
		{"fuse", 0x65735546, false},
		{"9p", 0x01021997, false},
	} {
		fs := syscall.Statfs_t{Type: int64(tc.magic)}
		if got := localChangeTimeFilesystem(&fs); got != tc.want {
			t.Errorf("%s (%#x): supported = %v, want %v", tc.name, tc.magic, got, tc.want)
		}
	}

	g := openRuntimeGateway(t)
	g.eventStatFastPath = false
	g.nowHook = agedClock
	seedProjectedRun(t, g, historyPA, 1, 16)
	info, err := os.Stat(g.eventPath(historyPA))
	if err != nil {
		t.Fatal(err)
	}
	reads := countEventLogReads(g, historyPA)
	for i := 0; i < 3; i++ {
		if err := g.RefreshDurableEventTail(context.Background(), historyPA); err != nil {
			t.Fatal(err)
		}
		if g.stateFor(historyPA).eventStatTrusted {
			t.Fatal("an unsupported filesystem must never trust event-file metadata")
		}
	}
	if got := reads.Load(); got < 3*info.Size() {
		t.Fatalf("3 refreshes read %d bytes of a %d-byte log; want full re-verification each time", got, info.Size())
	}
}

// An IfTail mark taken while the tail was trusted is stale once another
// writer commits: the precondition sees the refolded tail and skips the
// element instead of committing it on the old observation.
func TestTrustedEventTailRefusesStaleIfTail(t *testing.T) {
	g := openRuntimeGateway(t)
	requireEventStatFastPath(t, g)
	g.nowHook = agedClock
	seedProjectedRun(t, g, historyPA, 1, 8)
	ctx := context.Background()
	if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
		t.Fatal(err)
	}
	if !g.stateFor(historyPA).eventStatTrusted {
		t.Fatal("a verified, aged event file should be trusted")
	}
	mark := g.observedEventTail(historyPA)

	other, err := OpenDurableGateway(g.dir, g.commands)
	if err != nil {
		t.Fatal(err)
	}
	seedProjectedRun(t, other, historyPA, 100, 1)
	committed := len(durableEvents(t, other, historyPA))

	stale, err := json.Marshal(historyMessage(200))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.AppendProjectedEvents(ctx, historyPA, []ProjectedEvent{
		{RunMarker: RunMarkerStart, IfTail: &mark},
		{Event: stale, IfTail: &mark},
		{RunMarker: RunMarkerEnd, IfTail: &mark},
	}); err != nil {
		t.Fatal(err)
	}
	if got := len(durableEvents(t, g, historyPA)); got != committed {
		t.Fatalf("stale IfTail committed %d events", got-committed)
	}
	if got, want := g.observedEventTail(historyPA), other.observedEventTail(historyPA); got != want {
		t.Fatalf("tail after stale IfTail = %+v, want another writer's %+v", got, want)
	}
}

// An append that fails after its bytes reach a trusted log is rolled back;
// the log, the tail, and the next seq are exactly as before the attempt.
func TestTrustedEventTailAfterAppendRollback(t *testing.T) {
	g := openRuntimeGateway(t)
	requireEventStatFastPath(t, g)
	g.nowHook = agedClock
	seedProjectedRun(t, g, historyPA, 1, 8)
	ctx := context.Background()
	if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
		t.Fatal(err)
	}
	if !g.stateFor(historyPA).eventStatTrusted {
		t.Fatal("a verified, aged event file should be trusted")
	}
	path := g.eventPath(historyPA)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before := g.observedEventTail(historyPA)

	open := g.newFile
	g.newFile = func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		if name != path {
			return open(name, flag, perm)
		}
		f, err := os.OpenFile(name, flag|syscall.O_NOFOLLOW, perm)
		if err != nil {
			return nil, err
		}
		return &failingFile{File: f, failSyncOn: 1}, nil
	}
	if err := g.AppendProjectedEvents(ctx, historyPA, []ProjectedEvent{
		{RunMarker: RunMarkerStart}, {Event: json.RawMessage(mustJSON(t, historyMessage(9)))}, {RunMarker: RunMarkerEnd},
	}); err == nil {
		t.Fatal("expected the injected sync failure")
	}
	g.newFile = open

	if err := g.RefreshDurableEventTail(ctx, historyPA); err != nil {
		t.Fatal(err)
	}
	if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, original) {
		t.Fatalf("rolled-back log differs from the committed bytes (err %v)", err)
	}
	if got := g.observedEventTail(historyPA); got != before {
		t.Fatalf("tail after rollback = %+v, want %+v", got, before)
	}
	seedProjectedRun(t, g, historyPA, 9, 1)
	events := durableEvents(t, g, historyPA)
	assertEnvelopeIntegrity(t, events)
	for i, e := range events {
		if e.Seq == nil || *e.Seq != uint64(i+1) {
			t.Fatalf("event %d has seq %v after rollback and retry", i, e.Seq)
		}
	}
}
