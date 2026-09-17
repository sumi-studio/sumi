package filesvc

// Settlement orphan-sweep regressions (public acceptance failure
// files-acc-20260918-root-02): on the canonical JuiceFS mount a stage
// slot name was still visible to the orphan sweep while/after its write
// intent settled (slower staged publish plus directory-listing
// visibility lag versus plain POSIX). The sweep minted a 'recover'
// intent on the parent directory, and that intent's deadGrace hot
// window then fenced every descendant mutation — observed: write
// accepted, then 503 pending_settlement on the stale CAS write and
// every cleanup remove for ~20s.
//
// Three defects made that happen:
//
//	A. attachOrphanName compared rec.Dir == dir literally, but Dir==""
//	   in a nameRec means "the intent's own parent dir" (nameDir) —
//	   every journaled stage slot in a non-root directory was misread
//	   as unowned.
//	B. stageNameID parsed the raw first segment as digits, but minted
//	   names carry a role tag (o<id>-a0-*, c<id>-*) — the re-attach
//	   fallback was dead code.
//	C. Even with A+B, a name whose intent already finished (row dropped
//	   on success) has no provable owner — a single listing sighting
//	   minted a fencing recover immediately. The sweep now requires an
//	   unowned private name to persist across one sweep boundary
//	   (>= stageSweepInterval) before recovering it: a transient
//	   listing ghost never mints an intent, and a genuine orphan is
//	   surfaced one pass later — safe parked under its private name
//	   meanwhile.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// recoverIntents counts 'recover' file_op rows under this store's root.
func recoverIntents(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM file_op WHERE root=$1 AND op='recover'`,
		s.rootID).Scan(&n); err != nil {
		t.Fatalf("count recover intents: %v", err)
	}
	return n
}

// forceSweep runs a reconcile pass with the stage-sweep cadence gate
// opened, so the orphan sweep runs even if a previous pass just ran.
func forceSweep(t *testing.T, s *Store) {
	t.Helper()
	s.lastStageSweep.Store(0)
	s.Reconcile(context.Background())
}

// deadOwnerIntent inserts a pending intent owned by another instance
// declared just now: the reconciler's deadGrace keeps it pending this
// pass (a dead owner's intents age before re-judgment), so its row —
// and any names it journals — survives to the sweep exactly as a live
// in-flight intent's row would.
func deadOwnerIntent(t *testing.T, s *Store, scope, path string, names []nameRec) int64 {
	t.Helper()
	var ver int64
	if err := s.pool.QueryRow(context.Background(),
		`SELECT nextval('file_version_seq')`).Scan(&ver); err != nil {
		t.Fatalf("mint version: %v", err)
	}
	return insertIntent(t, s, intent{
		owner:   "inst-dead0000000000",
		scope:   scope,
		op:      "write",
		path:    path,
		version: ver,
		names:   names,
		at:      time.Now(),
	})
}

// journalStage rewrites intent id's names record to the real minted
// form — o<id>-a0-<rand>, empty Dir (the intent's own parent dir).
func journalStage(t *testing.T, s *Store, id int64) string {
	t.Helper()
	stage := fmt.Sprintf("%so%d-a0-abcdef012345", opStagePrefix, id)
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE file_op SET names=$2 WHERE id=$1`,
		id, mustJSON([]nameRec{{Name: stage, Act: "put"}})); err != nil {
		t.Fatalf("journal stage on intent %d: %v", id, err)
	}
	return stage
}

// A stage slot still listed in a subdirectory while its write intent is
// live is not an orphan: the journaled name record's empty Dir means the
// intent's parent dir, so the sweep must recognize ownership and mint no
// recover intent — and no descendant of that directory may be fenced.
func TestOrphanSweepKeepsJournaledSubdirStageName(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/sub/.keep", "x")
	iid := deadOwnerIntent(t, s, "ws", "sub/f.txt", nil)
	stage := journalStage(t, s, iid)
	putFile(t, dir, "ws/sub/"+stage, "in-flight stage body")

	forceSweep(t, s)

	if n := recoverIntents(t, s); n != 0 {
		t.Fatalf("sweep minted %d recover intents for a journaled stage name", n)
	}
	// Follow-on mutations elsewhere in the directory proceed — no
	// spurious pending_settlement fence from a recover intent.
	if _, _, err := s.WithWrite(ctx, "ws", "sub/g.txt", "write",
		IfVersion{Mode: "any"}, sha("next"), nil,
		func(it intent) (FileInfo, bool, error) {
			it.njDone()
			return FileInfo{Kind: "file", Fingerprint: "fp-g"}, true, nil
		}); err != nil {
		t.Fatalf("follow-on write under swept dir fenced: %v", err)
	}
}

// A stage name whose journal record was lost still embeds its intent id;
// the sweep must re-attach it to the surviving intent row rather than
// minting a recover intent.
func TestOrphanSweepReattachesStageNameByID(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)

	putFile(t, dir, "ws/sub/.keep", "x")
	iid := deadOwnerIntent(t, s, "ws", "sub/f.txt", nil)
	stage := fmt.Sprintf("%so%d-a0-abcdef012345", opStagePrefix, iid)
	putFile(t, dir, "ws/sub/"+stage, "lost-record stage body")

	forceSweep(t, s)

	if n := recoverIntents(t, s); n != 0 {
		t.Fatalf("sweep minted %d recover intents for a re-attachable name", n)
	}
	var raw []byte
	if err := s.pool.QueryRow(context.Background(),
		`SELECT names FROM file_op WHERE id=$1`, iid).Scan(&raw); err != nil {
		t.Fatalf("read names of intent %d: %v", iid, err)
	}
	var recs []nameRec
	if err := json.Unmarshal(raw, &recs); err != nil {
		t.Fatalf("decode names of intent %d: %v", iid, err)
	}
	found := false
	for _, r := range recs {
		if r.Name == stage {
			found = true
		}
	}
	if !found {
		t.Fatalf("stage name %q not re-attached to intent %d (names=%s)", stage, iid, raw)
	}
}

// A private name whose owner intent already completed — its file_op row
// is deleted on success — has no provable owner. One listing sighting is
// not orphan evidence (JuiceFS can report a just-settled stage slot from
// a stale directory listing): the first sweep defers, follow-on
// mutations are not fenced, and only a name still present on the next
// sweep is recovered.
func TestOrphanSweepDefersThenRecoversTrueOrphan(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	// A completed write: its intent row is gone by the time the sweep
	// lists — the post-settlement case that the id checks cannot cover.
	if _, _, err := s.WithWrite(ctx, "ws", "sub/f.txt", "write",
		IfVersion{Mode: "any"}, sha("payload"), nil,
		func(it intent) (FileInfo, bool, error) {
			it.njDone()
			return FileInfo{Kind: "file", Fingerprint: "fp-f"}, true, nil
		}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	putFile(t, dir, "ws/sub/.filesv-op-o4242-a0-deadbeef", "late-listed stage slot")

	forceSweep(t, s) // first sighting — deferred, no recover, no fence
	if n := recoverIntents(t, s); n != 0 {
		t.Fatalf("first sighting minted %d recover intents", n)
	}
	if _, _, err := s.WithWrite(ctx, "ws", "sub/g.txt", "write",
		IfVersion{Mode: "any"}, sha("next"), nil,
		func(it intent) (FileInfo, bool, error) {
			it.njDone()
			return FileInfo{Kind: "file", Fingerprint: "fp-g"}, true, nil
		}); err != nil {
		t.Fatalf("follow-on write fenced by a one-sighting orphan: %v", err)
	}

	forceSweep(t, s) // still present — genuinely unowned, now recovered
	if n := recoverIntents(t, s); n != 1 {
		t.Fatalf("persistent orphan did not get a recover intent (rows=%d)", n)
	}
	if where := scanTreeFor(t, dir, "ws", []byte("late-listed stage slot")); where == "" || containsPrivateSeg(where) {
		t.Fatalf("orphan content destroyed or left hidden: %q", where)
	}
}

// The observed JuiceFS failure mode: a stage name visible for one
// listing but gone by the next (entry-cache ghost, or a just-settled
// slot) must never mint a recover intent — nothing is fenced and no
// durable recovery record is left behind.
func TestOrphanSweepIgnoresTransientListingGhost(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)

	putFile(t, dir, "ws/sub/.filesv-op-o4242-a0-deadbeef", "ghost")
	forceSweep(t, s)
	if n := recoverIntents(t, s); n != 0 {
		t.Fatalf("ghost sighting minted %d recover intents", n)
	}

	if err := os.Remove(filepath.Join(dir, "ws/sub/.filesv-op-o4242-a0-deadbeef")); err != nil {
		t.Fatal(err)
	}
	forceSweep(t, s)
	if n := recoverIntents(t, s); n != 0 {
		t.Fatalf("transient ghost left %d recover intents", n)
	}
}

// A persistent true orphan still gets the designed protection: the
// recover intent minted on the second sweep fences descendant mutations
// inside its deadGrace hot window — a concurrent mutation sees
// pending_settlement rather than racing settlement.
func TestRecoverIntentFencesDescendantsWhileHot(t *testing.T) {
	dsn := pgDSN(t)
	s, _, dir := finalStore(t, dsn)
	ctx := context.Background()

	putFile(t, dir, "ws/sub/.filesv-op-o4242-a0-deadbeef", "true-orphan")
	forceSweep(t, s) // sighting
	forceSweep(t, s) // persistent — recover minted and settled
	if recoverIntents(t, s) != 1 {
		t.Fatal("setup: expected one recover intent")
	}
	_, _, err := s.WithWrite(ctx, "ws", "sub/other.txt", "write",
		IfVersion{Mode: "any"}, sha("x"), nil,
		func(it intent) (FileInfo, bool, error) {
			return FileInfo{Kind: "file", Fingerprint: "fp-o"}, true, nil
		})
	if !errors.Is(err, ErrUnsettled) {
		t.Fatalf("expected ErrUnsettled during hot window, got %v", err)
	}
}

func TestStageNameID(t *testing.T) {
	cases := []struct {
		name string
		want int64
		ok   bool
	}{
		{".filesv-op-o27-a0-64e29be48164", 27, true}, // the observed name form
		{".filesv-op-c9-abcdef", 9, true},
		{".filesv-op-4242-a0-dead", 4242, true}, // bare-id form (legacy/test)
		{".filesv-op-o27-a0-64e29be48164-q-zz", 27, true},
		{".filesv-op-adhoc-deadbeef", 0, false}, // unintented — no owner id
		{".filesv-op-o0-a0-xx", 0, false},       // id must be positive
		{".filesv-op-o-a0-xx", 0, false},
		{".filesv-op-", 0, false},
		{"regular.txt", 0, false},
		{".filesv-op-o12", 12, true},
	}
	for _, c := range cases {
		id, ok := stageNameID(c.name)
		if ok != c.ok || (ok && id != c.want) {
			t.Errorf("stageNameID(%q) = %d,%v want %d,%v", c.name, id, ok, c.want, c.ok)
		}
	}
}
