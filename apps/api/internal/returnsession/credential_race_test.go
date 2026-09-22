package returnsession_test

// Credential and barrier lineage under real concurrent interleavings on
// real PostgreSQL — not sequential old-after-new calls. Each test races
// the operations and asserts the invariants the ordering protocol must
// hold under ANY commit order, then drives the converged end state:
//
//   - file_epoch is the ONE durable order shared by owner resolution,
//     mint supersession and the filesvc barrier lineage.
//   - mint and every revocation serialize on the persona advisory lock;
//     a mint additionally re-reads status + owner under its transaction
//     locks, so a mint can never land under a dead session or a stale
//     lineage no matter how the calls interleave.
//   - a dead session's revocation is session-scoped: it can never reach
//     another lineage's credential; a completed local-mode move is the
//     one transition that retires every active grant for the persona.
//   - a superseded/revoked grant never revives; an earlier lineage's
//     still-active grant legitimately outlives a newer session that
//     dies — that is the designed fallback, not a revival.

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// mint asks the API for the session's storage credential, returning the
// HTTP status and the raw token when one was minted.
func (h *harness) mint(t *testing.T, sessionID, grant string) (int, string) {
	t.Helper()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessionID),
		grant, nil)
	if code != http.StatusCreated {
		return code, ""
	}
	var cred struct {
		Token string `json:"file_token"`
	}
	unmarshal(t, raw, &cred)
	return code, cred.Token
}

// activeFileTokens counts live storage credentials for the persona —
// optionally narrowed to one destination.
func activeFileTokens(t *testing.T, h *harness, dst string) int {
	t.Helper()
	var n int
	q := `SELECT count(*) FROM persona_file_tokens
		WHERE persona_id = $1 AND status = 'active'`
	args := []any{h.persona}
	if dst != "" {
		q += ` AND destination_placement_id = $2`
		args = append(args, dst)
	}
	if err := h.cloud.pool.QueryRow(h.ctx, q, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The mint and every token revocation serialize on the persona advisory
// lock, so N overlapping mints of the same eligible session commit one
// after another: every answer is a valid fresh grant, and exactly one —
// the last committer's — stays active. The rest are superseded, dead.
func TestConcurrentMintsLeaveExactlyOneActive(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create() // cloud mode
	h.driveImport(t, sessionID, grant, "absent", h.persona, "")
	dst := mustPlacement(t, h.local)

	const n = 8
	tokens := make([]string, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			code, tok := h.mint(t, sessionID, grant)
			if code != http.StatusCreated {
				t.Errorf("concurrent mint %d: %d", i, code)
				return
			}
			tokens[i] = tok
		}(i)
	}
	close(start)
	wg.Wait()

	if got := activeFileTokens(t, h, dst); got != 1 {
		t.Fatalf("active credentials after concurrent mints: %d (want exactly 1)", got)
	}
	var live int
	for _, tok := range tokens {
		if tok == "" {
			continue
		}
		if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tok); err == nil {
			live++
		}
	}
	if live != 1 {
		t.Fatalf("authorizing minted tokens: %d alive (want exactly the last committer)", live)
	}
}

// A delayed old-lineage mint racing the newer return's bind can only
// resolve one of two legitimate ways: it committed before the bind did
// (minted under the lineage that owned the store at the time — the
// newer mint then supersedes it), or it observed the committed bind and
// refused. It can never supersede the newer grant and never mint under
// a lineage that no longer owns the store.
func TestDelayedOldMintRacesNewerBind(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessA, _, grantA := h.create() // cloud mode
	h.driveImport(t, sessA, grantA, "absent", h.persona, "")
	codeA, tokA := h.mint(t, sessA, grantA)
	if codeA != http.StatusCreated {
		t.Fatalf("first mint: %d", codeA)
	}
	dst := mustPlacement(t, h.local)

	h.returnPersonaToCloud(t, sessA)

	sessB, _, grantB := h.create()
	// Race the stale session's re-mint against the newer bind. Either
	// order is legal; the assertion below pins the converged state.
	var wg sync.WaitGroup
	start := make(chan struct{})
	var staleCode int
	var staleTok string
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		staleCode, staleTok = h.mint(t, sessA, grantA)
	}()
	bindDone := make(chan int, 1)
	go func() {
		code, _ := h.grantReq(http.MethodPost,
			fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
			grantB, jsonBody(dest(t, h.local, h.persona, "surrendered")))
		bindDone <- code
	}()
	close(start)
	wg.Wait()
	if code := <-bindDone; code != http.StatusOK {
		t.Fatalf("newer bind: %d", code)
	}
	// Exactly two outcomes are legal: the stale mint committed before
	// the bind it raced (201 — superseded below), or it observed the
	// committed bind and refused with the superseded-lineage conflict
	// (409). Any other status is a protocol failure, not a race outcome.
	switch staleCode {
	case http.StatusCreated:
		if staleTok == "" {
			t.Fatal("stale mint reported success without a token")
		}
	case http.StatusConflict:
	default:
		t.Fatalf("stale mint answered %d — only 201 (pre-bind commit) or 409 (superseded lineage) are legal", staleCode)
	}

	// The live lineage mints. Whatever the stale mint did, the newer
	// grant for the same destination retires it.
	codeB, tokB := h.mint(t, sessB, grantB)
	if codeB != http.StatusCreated {
		t.Fatalf("live lineage mint: %d", codeB)
	}
	if got := activeFileTokens(t, h, dst); got != 1 {
		t.Fatalf("active credentials for the destination: %d (want exactly 1)", got)
	}
	for _, tok := range []string{tokA, staleTok} {
		if tok == "" {
			continue
		}
		if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tok); err == nil {
			t.Fatalf("a superseded-lineage token still authorizes")
		}
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tokB); err != nil {
		t.Fatalf("the live lineage's grant does not authorize: %v", err)
	}
	// And now that the bind is durably committed, the stale mint is
	// refused outright — the owner check under the lock, not a race.
	if code, _ := h.mint(t, sessA, grantA); code == http.StatusCreated {
		t.Fatal("superseded session minted after the newer bind committed")
	}
}

// A mint racing the session's own cancel lands before or after the
// status change: before, the minted grant dies with the session when
// its death resolves (aborted here — the destination retires the
// sealed transfer); after, the mint refuses on the status re-read under
// the session row lock. Either way no grant survives its session.
func TestMintRacesSessionCancel(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create() // cloud mode
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}

	start := make(chan struct{})
	var mintCode int
	var mintTok string
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		mintCode, mintTok = h.mint(t, sessionID, grant)
	}()
	cancelDone := make(chan int, 1)
	go func() {
		code, _ := h.ownerReq(http.MethodPost,
			fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
		cancelDone <- code
	}()
	close(start)
	wg.Wait()
	if code := <-cancelDone; code != http.StatusOK {
		t.Fatalf("cancel: %d", code)
	}
	// A sealed session's cancel marks cancelling: the destination's
	// retire proof resolves it.
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelling {
		t.Fatalf("session %s (want cancelling)", got)
	}
	if mintCode == http.StatusCreated && mintTok == "" {
		t.Fatal("mint reported success without a token")
	}

	code, raw = h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("status for transfer key: %d %s", code, raw)
	}
	rec, err := h.local.svc.Retire(h.ctx, h.persona, sessionID, mustPlacement(t, h.local),
		asView(t, raw).TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessionID),
		grant, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("retired report: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAborted {
		t.Fatalf("session %s (want aborted)", got)
	}

	if mintTok != "" {
		if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, mintTok); err == nil {
			t.Fatal("a credential minted before the cancel survived its session's death")
		}
	}
	if got := activeFileTokens(t, h, ""); got != 0 {
		t.Fatalf("active credentials after the session aborted: %d", got)
	}
	// The dead session can never mint again — the status re-read under
	// the lock refuses regardless of how late the call arrives.
	if code, _ := h.mint(t, sessionID, grant); code == http.StatusCreated {
		t.Fatal("an aborted session minted a credential")
	}
}

// The newer return dies before minting: ownership falls back to the
// older completed lineage, whose already-active grant was never revoked
// and keeps authorizing — the intentionally still-active previous
// grant. It is NOT a revival: the token stayed 'active' throughout.
// The restored owner may re-mint, superseding it.
func TestCancelledNewerReturnKeepsPriorGrant(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessA, _, grantA := h.create() // cloud mode
	h.driveImport(t, sessA, grantA, "absent", h.persona, "")
	codeA, tokA := h.mint(t, sessA, grantA)
	if codeA != http.StatusCreated {
		t.Fatalf("first mint: %d", codeA)
	}

	h.returnPersonaToCloud(t, sessA)

	sessB, _, grantB := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
		grantB, jsonBody(dest(t, h.local, h.persona, "surrendered")))
	if code != http.StatusOK {
		t.Fatalf("newer bind: %d %s", code, raw)
	}
	// The newer lineage dies — bound, sealed, then cancelled and
	// retired. It never minted; nothing supersedes the prior grant.
	code, raw = h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessB), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	code, raw = h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sessB), grantB, nil)
	if code != http.StatusOK {
		t.Fatalf("status for transfer key: %d %s", code, raw)
	}
	rec, err := h.local.svc.Retire(h.ctx, h.persona, sessB, mustPlacement(t, h.local),
		asView(t, raw).TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessB),
		grantB, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("retired report: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessB); got != returnsession.StatusAborted {
		t.Fatalf("newer session %s (want aborted)", got)
	}

	// The earlier grant was never revoked: it still authorizes while
	// the older completed lineage owns the store again.
	pid, scope, err := h.sessions.AuthorizeFileToken(h.ctx, tokA)
	if err != nil {
		t.Fatalf("prior grant must stay active after the newer return died: %v", err)
	}
	wantScope, _ := fileaccess.ScopeForPersona(h.persona)
	if pid != h.persona || scope != wantScope {
		t.Fatalf("prior grant resolves %s/%s", pid, scope)
	}
	// The restored owner re-mints: the prior grant is superseded, dead.
	codeA2, tokA2 := h.mint(t, sessA, grantA)
	if codeA2 != http.StatusCreated {
		t.Fatalf("restored owner re-mint: %d", codeA2)
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tokA); err == nil {
		t.Fatal("superseded prior grant still authorizes after the re-mint")
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tokA2); err != nil {
		t.Fatalf("re-minted grant does not authorize: %v", err)
	}
}

// Session death revokes only its own lineage's credentials — never a
// different session's grant, even for the same persona. Two
// destinations keep the grants supersession-disjoint so the scoping is
// what the assertion exercises.
func TestSessionDeathRevokesOnlyItsOwnCredential(t *testing.T) {
	h := setup(t, returnsession.Config{})
	local2 := newPlacement(t, true)

	sessA, _, grantA := h.create() // cloud mode → destination 1
	h.driveImport(t, sessA, grantA, "absent", h.persona, "")
	codeA, tokA := h.mint(t, sessA, grantA)
	if codeA != http.StatusCreated {
		t.Fatalf("first mint: %d", codeA)
	}

	h.returnPersonaToCloud(t, sessA)

	sessB, _, grantB := h.create() // cloud mode → destination 2
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
		grantB, jsonBody(dest(t, local2, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("second bind: %d %s", code, raw)
	}
	codeB, tokB := h.mint(t, sessB, grantB)
	if codeB != http.StatusCreated {
		t.Fatalf("second mint: %d", codeB)
	}

	// B dies (sealed → cancelling → retired → aborted). Its credential
	// dies with it; A's grant for the other destination is untouched —
	// it is a different session's lineage and a different destination.
	code, raw = h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessB), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	code, raw = h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sessB), grantB, nil)
	if code != http.StatusOK {
		t.Fatalf("status for transfer key: %d %s", code, raw)
	}
	rec, err := local2.svc.Retire(h.ctx, h.persona, sessB, mustPlacement(t, local2),
		asView(t, raw).TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessB),
		grantB, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("retired report: %d %s", code, raw)
	}
	if got := h.sessionStatus(sessB); got != returnsession.StatusAborted {
		t.Fatalf("session %s (want aborted)", got)
	}

	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tokB); err == nil {
		t.Fatal("the dead session's credential still authorizes")
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tokA); err != nil {
		t.Fatalf("another session's credential died with it: %v", err)
	}
	// Owner revocation takes the rest — bytes are never touched.
	if _, err := h.sessions.RevokePersonaFileTokens(h.ctx, h.owner); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tokA); err == nil {
		t.Fatal("owner-revoked grant still authorizes")
	}
	if got := activeFileTokens(t, h, ""); got != 0 {
		t.Fatalf("active credentials after owner revocation: %d", got)
	}
}

// The owner's revocation serializes with minting on the same persona
// lock, so racing them can only end in a serialized outcome: whichever
// committed last wins, and no grant can slip between the revoke's scan
// and its commit. A settled final revoke leaves nothing authorizing.
func TestOwnerRevokeRacesMint(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create() // cloud mode
	h.driveImport(t, sessionID, grant, "absent", h.persona, "")

	var mu sync.Mutex
	var issued []string
	for i := 0; i < 6; i++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		var mintCode int
		var revokeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			var tok string
			mintCode, tok = h.mint(t, sessionID, grant)
			if tok != "" {
				mu.Lock()
				issued = append(issued, tok)
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			_, revokeErr = h.sessions.RevokePersonaFileTokens(h.ctx, h.owner)
		}()
		close(start)
		wg.Wait()
		// The session stays completed and owning throughout — nothing in
		// this race can make its mint ineligible, so a non-201 is a
		// protocol failure, not a race outcome. Same for the revoke.
		if mintCode != http.StatusCreated {
			t.Fatalf("round %d: mint answered %d (session is eligible — want 201)", i, mintCode)
		}
		if revokeErr != nil {
			t.Fatalf("round %d: revoke: %v", i, revokeErr)
		}
		if got := activeFileTokens(t, h, ""); got > 1 {
			t.Fatalf("round %d: %d active credentials — revoke/mint interleaved", i, got)
		}
	}
	// Property 1 — the serialized outcome: a revoke that commits after
	// the racing stops leaves nothing authorizing. Every token the race
	// issued is dead; none slipped between a revoke's check and commit.
	if _, err := h.sessions.RevokePersonaFileTokens(h.ctx, h.owner); err != nil {
		t.Fatal(err)
	}
	if got := activeFileTokens(t, h, ""); got != 0 {
		t.Fatalf("active credentials after final revoke: %d", got)
	}
	for _, tok := range issued {
		if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tok); err == nil {
			t.Fatal("a grant minted during the race survived the settled revoke")
		}
	}
	// Property 2 — a mint that commits after the revoke is a fresh
	// legitimate grant: owner revocation is a point-in-time kill, not a
	// ban on future mints (the owner can revoke again).
	code, tok := h.mint(t, sessionID, grant)
	if code != http.StatusCreated || tok == "" {
		t.Fatalf("re-mint after owner revocation: %d", code)
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, tok); err != nil {
		t.Fatalf("post-revoke grant does not authorize: %v", err)
	}
}

// The local→cloud release that broke before epochs: a completed local
// return retains its frozen barrier (the Cloud copy stays read-only);
// when a NEWER cloud-mode return binds, its converge releases that
// barrier under ITS OWN owner+epoch — the store accepts a newer
// lineage's release of an older barrier. The completed session keeps
// making no calls: its reconcile sees it is no longer the owner.
func TestCloudReturnAfterCompletedLocalReleasesBarrier(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)

	sessA, _, grantA := h.createMode("local")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessA),
		grantA, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
	if code != http.StatusOK {
		t.Fatalf("local bind: %d %s", code, raw)
	}
	h.driveImportRest(t, sessA, grantA) // completed: A's barrier retained
	scope, err := fileaccess.ScopeForPersona(h.persona)
	if err != nil {
		t.Fatal(err)
	}
	var epochA int64
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT file_epoch FROM return_sessions WHERE session_id = $1`, sessA).Scan(&epochA); err != nil {
		t.Fatal(err)
	}
	frozenByA := fmt.Sprintf("%s|%s@%d=true", sessA, scope, epochA)
	if ff.frozenCalls[len(ff.frozenCalls)-1] != frozenByA {
		t.Fatalf("completed local return left barrier calls %v (want last %s)", ff.frozenCalls, frozenByA)
	}

	h.returnPersonaToCloud(t, sessA)

	// The newer return keeps files in Cloud: binding it releases the
	// retained barrier under B's lineage — never under A's.
	sessB, _, grantB := h.createMode("cloud")
	ff.frozenCalls = nil
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
		grantB, jsonBody(dest(t, h.local, h.persona, "surrendered")))
	if code != http.StatusOK {
		t.Fatalf("cloud bind after local return: %d %s", code, raw)
	}
	var epochB int64
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT file_epoch FROM return_sessions WHERE session_id = $1`, sessB).Scan(&epochB); err != nil {
		t.Fatal(err)
	}
	if epochB <= epochA {
		t.Fatalf("lineage order broken: B epoch %d not newer than A epoch %d", epochB, epochA)
	}
	wantRelease := fmt.Sprintf("%s|%s@%d=false", sessB, scope, epochB)
	var released bool
	for _, c := range ff.frozenCalls {
		if c == wantRelease {
			released = true
		}
		if strings.HasPrefix(c, sessA+"|") {
			t.Fatalf("the newer bind issued a barrier call under the STALE lineage: %v", ff.frozenCalls)
		}
	}
	if !released {
		t.Fatalf("cloud-mode bind never released the retained barrier: %v (want %s)", ff.frozenCalls, wantRelease)
	}

	// The superseded session's reconcile stays silent — it cannot
	// re-assert or release the barrier it no longer owns.
	ff.frozenCalls = nil
	if err := h.sessions.Reconcile(h.ctx, sessA); err != nil {
		t.Fatalf("stale reconcile: %v", err)
	}
	if len(ff.frozenCalls) != 0 {
		t.Fatalf("superseded session touched the barrier: %v", ff.frozenCalls)
	}
}
