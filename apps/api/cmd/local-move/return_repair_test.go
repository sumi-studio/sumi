package main

// Regression tests for review A's receiver findings: URL validation, the
// settled-record lifecycle, quoted installer config, unrelated-persona
// admission, and bundle durability recovery.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

func TestParseReturnURLRules(t *testing.T) {
	sid := uuid.Must(uuid.NewV7()).String()
	grant := strings.Repeat("a", 43)
	good := "https://api.example.com/api/secretary-return/sessions/" + sid + "#grant=" + grant
	for _, tc := range []struct {
		name string
		raw  string
		ok   bool
	}{
		{"https", good, true},
		{"http loopback", "http://127.0.0.1:8080/api/secretary-return/sessions/" + sid + "#grant=" + grant, true},
		{"http localhost", "http://localhost/api/secretary-return/sessions/" + sid + "#grant=" + grant, true},
		{"http remote", "http://api.example.com/api/secretary-return/sessions/" + sid + "#grant=" + grant, false},
		{"malformed grant", "https://api.example.com/api/secretary-return/sessions/" + sid + "#grant=abc", false},
		{"extra fragment", good + "&x=1", false},
		{"query string", "https://api.example.com/api/secretary-return/sessions/" + sid + "?x=1#grant=" + grant, false},
		{"prefixed path", "https://api.example.com/prefix/api/secretary-return/sessions/" + sid + "#grant=" + grant, false},
		{"userinfo", "https://u:p@api.example.com/api/secretary-return/sessions/" + sid + "#grant=" + grant, false},
		{"empty host", "https:///api/secretary-return/sessions/" + sid + "#grant=" + grant, false},
		{"duplicate grant", good + "&grant=" + grant, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, id, g, err := parseReturnURL(tc.raw)
			if tc.ok {
				if err != nil || id != sid || g != grant {
					t.Fatalf("refused %q: %v", tc.raw, err)
				}
				if strings.Contains(u, "#") || strings.Contains(u, grant) {
					t.Fatalf("the grant stayed in the session URL: %q", u)
				}
			} else if err == nil {
				t.Fatalf("accepted %q", tc.raw)
			}
		})
	}
}

// TestReturnRejectedGrantExitsError: a well-formed grant the server does
// not know is a permanent refusal — the command must fail, not advise a
// resume that can never work.
func TestReturnRejectedGrantExitsError(t *testing.T) {
	h := setupReturn(t)
	_, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	// Same session, a grant the server never minted — valid shape, wrong secret.
	bad := returnURL[:strings.Index(returnURL, "#grant=")+7] + strings.Repeat("z", 43)
	m, out := h.mover()
	code := m.ReturnStart(h.ctx, bad, h.local.pool, config, false)
	if code != exitError {
		t.Fatalf("wrong grant: %d (want exitError, not pending)\n%s", code, out)
	}
	if !strings.Contains(out.String(), "rejected") {
		t.Fatalf("the refusal was not explained:\n%s", out)
	}
	// The record stays inspectable; status reports the rejection honestly.
	out.Reset()
	if code := m.ReturnStatus(h.ctx, h.local.pool, config); code != exitPending {
		t.Fatalf("status: %d\n%s", code, out)
	}
}

// TestFinishedReturnArchivesAndAcceptsNext: a settled return record does
// not block a later authorized return — it is archived aside and the new
// session proceeds. Successive cancel-then-return is the concrete case.
func TestFinishedReturnArchivesAndAcceptsNext(t *testing.T) {
	h := setupReturn(t)
	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()

	// First return: the owner cancels before the mover ever binds.
	sessionID1, returnURL1 := h.newReturn()
	if code := h.ownerCancel(sessionID1); code != http.StatusOK {
		t.Fatalf("owner cancel: %d", code)
	}
	if code := m.ReturnStart(h.ctx, returnURL1, h.local.pool, config, false); code != exitDone {
		t.Fatalf("first return: %d\n%s", code, out)
	}

	// The owner opens a second return for the same secretary — still
	// active on Cloud (the first never reached a bind).
	sessionID2, returnURL2 := h.newReturn()
	if sessionID2 == sessionID1 {
		t.Fatal("the second session re-used the first")
	}
	out.Reset()
	if code := m.ReturnStart(h.ctx, returnURL2, h.local.pool, config, false); code != exitDone {
		t.Fatalf("second return: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	if got := h.sessionStatus(sessionID2); got != returnsession.StatusCompleted {
		t.Fatalf("session2 %s", got)
	}
	// The first record is archived with its evidence, not deleted.
	if _, err := os.Stat(filepath.Join(h.home, "return", "state-"+sessionID1+".json")); err != nil {
		t.Fatalf("the finished record was not archived: %v", err)
	}
}

// TestSlotStateStrangerRules: an install's unrelated personas disqualify a
// return only while they can still act — active, sealed or staged. A
// transferred shell is inert authored history, not a second secretary, and
// is preserved on both admission branches.
func TestSlotStateStrangerRules(t *testing.T) {
	ctx := context.Background()
	slot := uuid.Must(uuid.NewV7()).String()

	newSlotCheck := func(p placement) *returner {
		m := newMover(t.TempDir(), slot, p.svc, p.state, &strings.Builder{})
		return m.newReturner(p.pool, "", false)
	}

	// Surrendered slot + a live stranger refuses, like the absent branch.
	liveStranger := newPlacement(t, true)
	if _, err := liveStranger.pool.Exec(ctx,
		`INSERT INTO core_personas (persona_id, display_name, authority, transfer_id)
		 VALUES ($1, 'sec', 'transferred', 'fwd-1')`, slot); err != nil {
		t.Fatal(err)
	}
	if _, _, err := liveStranger.state.EnsurePersona(ctx, uuid.Must(uuid.NewV7()).String(), nil, "other secretary"); err != nil {
		t.Fatal(err)
	}
	if _, err := newSlotCheck(liveStranger).slotState(ctx); err == nil ||
		!strings.Contains(err.Error(), "different secretary") {
		t.Fatalf("a surrendered slot accepted a second LIVE secretary: %v", err)
	}

	// Surrendered slot + only inert history admits the reclaim.
	inertSurrendered := newPlacement(t, true)
	if _, err := inertSurrendered.pool.Exec(ctx,
		`INSERT INTO core_personas (persona_id, display_name, authority, transfer_id)
		 VALUES ($1, 'sec', 'transferred', 'fwd-1')`, slot); err != nil {
		t.Fatal(err)
	}
	if _, err := inertSurrendered.pool.Exec(ctx,
		`INSERT INTO core_personas (persona_id, display_name, authority, transfer_id)
		 VALUES ($1, 'old secretary', 'transferred', 'fwd-old')`,
		uuid.Must(uuid.NewV7()).String()); err != nil {
		t.Fatal(err)
	}
	d, err := newSlotCheck(inertSurrendered).slotState(ctx)
	if err != nil || d.SlotState != "surrendered" {
		t.Fatalf("inert history blocked the reclaim: %v %v", err, d.SlotState)
	}

	// Absent slot + only inert history is still a fresh-enough target: the
	// old surrendered shell is preserved, not overwritten.
	inertAbsent := newPlacement(t, true)
	if _, err := inertAbsent.pool.Exec(ctx,
		`INSERT INTO core_personas (persona_id, display_name, authority, transfer_id)
		 VALUES ($1, 'old secretary', 'transferred', 'fwd-old')`,
		uuid.Must(uuid.NewV7()).String()); err != nil {
		t.Fatal(err)
	}
	d, err = newSlotCheck(inertAbsent).slotState(ctx)
	if err != nil || d.SlotState != "absent" {
		t.Fatalf("inert history disqualified a fresh target: %v %v", err, d.SlotState)
	}
}

// TestReturnTruncatedBundleRedownloads: a bundle that fails its integrity
// check after being recorded as downloaded (a crash-truncated write) is
// dropped and re-fetched — the import's refusal is not the end.
func TestReturnTruncatedBundleRedownloads(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()

	// Serve a corrupt bundle once: a valid header for this return followed
	// by truncated garbage, so the import's own integrity check fails.
	own, err := h.local.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var once atomic.Bool
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/bundle") && !once.Swap(true) {
			fmt.Fprintf(w, `{"transfer_id":%q,"persona_id":%q,"destination_id":%q}`+"\ntruncated-junk",
				sessionID, h.pid, own)
			return true
		}
		return false
	})
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	h.setIntercept(nil)
	if code != exitDone {
		t.Fatalf("return after corrupt bundle: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCompleted {
		t.Fatalf("session %s", got)
	}
}

// TestCancelResponseLostStaysPending: Cloud accepted the cancel but the
// settle traffic after it dropped. The command must stay resumable —
// exit 3 with guidance — not report a failure: a later return-cancel
// finishes the same session. A permanent grant rejection is the opposite
// case and already exits error (TestReturnRejectedGrantExitsError).
func TestCancelResponseLostStaysPending(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)

	// Stop the download mid-flight so the session is sealed, then lose
	// every request after the cancel itself lands.
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/bundle") {
			panic(http.ErrAbortHandler)
		}
		return false
	})
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitPending {
		t.Fatalf("stopped return: %d\n%s", code, out)
	}
	if got := authority(t, h.cloud, h.pid); got != "sealed" {
		t.Fatalf("source %s", got)
	}
	var cancelLanded atomic.Bool
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/cancel") {
			cancelLanded.Store(true)
			return false
		}
		if cancelLanded.Load() {
			panic(http.ErrAbortHandler)
		}
		return false
	})
	out.Reset()
	code := m.ReturnCancel(h.ctx, h.local.pool, config)
	if code != exitPending {
		t.Fatalf("cancel with lost settle traffic: %d (want pending 3)\n%s", code, out)
	}
	if !strings.Contains(out.String(), "return-cancel") {
		t.Fatalf("no resumption guidance:\n%s", out)
	}
	// The cancel is recorded on Cloud; the local record keeps the grant so
	// the same command can finish it.
	if got := h.sessionStatus(sessionID); got != returnsession.StatusCancelling {
		t.Fatalf("session %s (want cancelling — the cancel did land)", got)
	}
	if _, err := os.Stat(filepath.Join(h.home, "return", "state.json")); err != nil {
		t.Fatalf("the resumable record is gone: %v", err)
	}

	h.setIntercept(nil)
	out.Reset()
	code = m.ReturnCancel(h.ctx, h.local.pool, config)
	if code != exitDone {
		t.Fatalf("retried cancel: %d\n%s", code, out)
	}
	if got := h.sessionStatus(sessionID); got != returnsession.StatusAborted {
		t.Fatalf("session %s", got)
	}
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("cloud authority %s", got)
	}
}

// TestCancelUnreachableStaysPending: the same truth before Cloud answers
// — an unreachable cancel request is pending, not a failure, because the
// cancel may or may not have landed.
func TestCancelUnreachableStaysPending(t *testing.T) {
	h := setupReturn(t)
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)

	// A sealed session is a real cancel target.
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "/bundle") {
			panic(http.ErrAbortHandler)
		}
		return false
	})
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitPending {
		t.Fatalf("stopped return: %d\n%s", code, out)
	}
	h.setIntercept(func(w http.ResponseWriter, r *http.Request) bool {
		panic(http.ErrAbortHandler)
	})
	out.Reset()
	if code := m.ReturnCancel(h.ctx, h.local.pool, config); code != exitPending {
		t.Fatalf("unreachable cancel: %d (want pending 3)\n%s", code, out)
	}
	// Still sealed and still resumable — nothing was falsely marked done.
	if got := h.sessionStatus(sessionID); got != returnsession.StatusSealed {
		t.Fatalf("session %s (want sealed — the cancel never landed)", got)
	}
}

// TestPendingApprovalGuidesThenRecovers: a pending tool approval carries
// into a fresh Local import whose persona has no bound human — the
// activation refuses honestly. The command explains the ordinary path
// (decide it on Cloud, or cancel and return again), the approval row is
// never touched, and the full recovery — cancel, decide on Cloud, new
// return — lands the secretary active on Local.
func TestPendingApprovalGuidesThenRecovers(t *testing.T) {
	h := setupReturn(t)
	// A coherent pending approval, the way the ledger actually holds one:
	// the input parks waiting on the decision, the turn is recorded, the
	// operation sits awaiting_approval inside the input's recorded plan,
	// and the approval carries the recomputably-correct action digest the
	// source's integrity check demands before it will seal.
	digest := agentstate.ActionDigest("exec", "elevated", map[string]any{"cmd": "ls"})
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_inputs
		SET status = 'waiting', waiting_since = now()
		WHERE persona_id = $1 AND input_id = 'in-carried'`, h.pid); err != nil {
		t.Fatal(err)
	}
	// The recorded epochs sit below the writer lease's current generation —
	// the turn ran under an earlier epoch, as a parked approval does.
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO core_writer_leases
		(persona_id, generation, holder_id, expires_at)
		VALUES ($1, 2, 'writer-1', now() + interval '1 hour')`, h.pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO core_turns
		(persona_id, turn_id, input_id, generation, attempt, status)
		VALUES ($1, 'turn-1', 'in-carried', 1, 1, 'interrupted')`, h.pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO core_turn_plans
		(persona_id, input_id, turn_id, generation, plan)
		VALUES ($1, 'in-carried', 'turn-1', 1,
		 '[{"calls":[{"tool":"exec","request":{"cmd":"ls"}}]}]')`, h.pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO core_operations
		(persona_id, operation_id, turn_id, tool, idempotency_key, request, status, claimed_generation)
		VALUES ($1, 'op-1', 'turn-1', 'exec', 'in-carried:tool:0', '{"cmd":"ls"}',
		 'awaiting_approval', 0)`, h.pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO core_tool_approvals
		(persona_id, approval_id, input_id, call_index, operation_id, turn_id,
		 tool, route, required_by, request, action_digest, status)
		VALUES ($1, 'ap-pending', 'in-carried', 0, 'op-1', 'turn-1',
		 'exec', 'elevated', 'intrinsic', '{"cmd":"ls"}', $2, 'pending')`,
		h.pid, digest); err != nil {
		t.Fatal(err)
	}
	sessionID, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)

	// The session view surfaces the pending approval before the move.
	grant := returnURL[strings.Index(returnURL, "#grant=")+7:]
	req, _ := http.NewRequestWithContext(h.ctx, http.MethodGet,
		h.srv.URL+"/api/secretary-return/sessions/"+sessionID, nil)
	req.Header.Set("Authorization", "Bearer "+grant)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var view struct {
		Preflight *struct {
			PendingApprovals int `json:"pending_approvals"`
		} `json:"preflight"`
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if err := json.Unmarshal(raw, &view); err != nil || view.Preflight == nil || view.Preflight.PendingApprovals != 1 {
		t.Fatalf("preflight did not surface the pending approval: %s", raw)
	}

	m, out := h.mover()
	code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false)
	if code != exitPending || !strings.Contains(out.String(), "approval") {
		t.Fatalf("return with pending approval: %d\n%s", code, out)
	}
	if !strings.Contains(out.String(), "return-cancel") || !strings.Contains(out.String(), "new return") {
		t.Fatalf("the guidance is not actionable:\n%s", out)
	}
	// Honest state: sealed on Cloud, staged here — never silently
	// activated, and the approval row is untouched on both sides.
	if got := authority(t, h.cloud, h.pid); got != "sealed" {
		t.Fatalf("cloud authority %s", got)
	}
	if got := authority(t, h.local, h.pid); got != "staged" {
		t.Fatalf("local authority %s", got)
	}
	var pending int
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_tool_approvals
		WHERE persona_id = $1 AND approval_id = 'ap-pending' AND status = 'pending'`, h.pid).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("the approval row was touched: %v %d", err, pending)
	}

	// The offered recovery: cancel, decide the approval on Cloud, start a
	// new return. Retiring the staged copy is safe — the sealed source
	// still serves the identical bundle, so nothing is lost.
	out.Reset()
	if code := m.ReturnCancel(h.ctx, h.local.pool, config); code != exitDone {
		t.Fatalf("cancel: %d\n%s", code, out)
	}
	if got := authority(t, h.cloud, h.pid); got != "active" {
		t.Fatalf("cloud authority after cancel %s", got)
	}
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM core_tool_approvals
		WHERE persona_id = $1 AND approval_id = 'ap-pending' AND status = 'pending'`, h.pid).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("the approval was dropped or decided: %v %d", err, pending)
	}

	// The human settles it on Cloud. Denying is what actually clears a
	// fresh Local: an approved-but-unconsumed grant is re-pended at
	// import (consent must come from the destination's bound human), so
	// approve-without-run would block again — deny, or let it finish on
	// Cloud. The parked input un-waits with the decision.
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_tool_approvals
		SET status = 'denied', decision = 'deny_once', decision_id = 'dec-1',
		    decided_by_kind = 'human', decided_by_id = $2, decided_at = now()
		WHERE persona_id = $1 AND approval_id = 'ap-pending'`, h.pid, h.human); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_inputs
		SET status = 'queued', waiting_since = NULL
		WHERE persona_id = $1 AND input_id = 'in-carried'`, h.pid); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE core_operations
		SET status = 'failed', completed_at = now()
		WHERE persona_id = $1 AND operation_id = 'op-1'`, h.pid); err != nil {
		t.Fatal(err)
	}
	sessionID2, returnURL2 := h.newReturn()
	out.Reset()
	if code := m.ReturnStart(h.ctx, returnURL2, h.local.pool, config, false); code != exitDone {
		t.Fatalf("second return after the decision: %d\n%s", code, out)
	}
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	if got := h.sessionStatus(sessionID2); got != returnsession.StatusCompleted {
		t.Fatalf("session2 %s", got)
	}
	// The decided row travelled as history — no pending work, and the
	// denial's provenance is intact.
	var dstStatus, dstDecision string
	if err := h.local.pool.QueryRow(h.ctx, `SELECT status, decision FROM core_tool_approvals
		WHERE persona_id = $1 AND approval_id = 'ap-pending'`, h.pid).Scan(&dstStatus, &dstDecision); err != nil {
		t.Fatal(err)
	}
	if dstStatus != "denied" || dstDecision != "deny_once" {
		t.Fatalf("the decision did not carry as history: %s %s", dstStatus, dstDecision)
	}
}

// TestReturnDirModeTightened: an existing return/ directory kept loose
// permissions — MkdirAll never chmods — so the entry path tightens it to
// 0700 while leaving its contents alone.
func TestReturnDirModeTightened(t *testing.T) {
	h := setupReturn(t)
	rdir := filepath.Join(h.home, "return")
	if err := os.MkdirAll(rdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(rdir, 0o755); err != nil {
		t.Fatal(err)
	}
	// An authored file inside must survive the mode fix.
	marker := filepath.Join(rdir, "note.txt")
	if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, returnURL := h.newReturn()
	config := writeConfig(t, h.home, h.slot)
	m, out := h.mover()
	if code := m.ReturnStart(h.ctx, returnURL, h.local.pool, config, false); code != exitDone {
		t.Fatalf("return: %d\n%s", code, out)
	}
	fi, err := os.Stat(rdir)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Fatalf("return dir mode %v — the loose mode was kept", fi.Mode().Perm())
	}
	if raw, err := os.ReadFile(marker); err != nil || string(raw) != "keep" {
		t.Fatalf("the mode fix touched existing contents: %v", err)
	}
}

// TestConfigValueQuotingRules: the env-file contract is the installer's —
// shq single quotes, plain double quotes, or bare — and a rewrite keeps
// the file's own style.
func TestConfigValueQuotingRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.env")
	slot := uuid.Must(uuid.NewV7()).String()
	next := uuid.Must(uuid.NewV7()).String()

	write := func(v string) {
		t.Helper()
		if err := os.WriteFile(path, []byte("SUMI_LOCAL_ID='inst'\nSUMI_PERSONA_ID="+v+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct{ line, want string }{
		{"'" + slot + "'", slot},             // installer shq format
		{`"` + slot + `"`, slot},             // double-quoted
		{slot, slot},                         // bare
		{"'it'\\''s-quoted'", "it's-quoted"}, // shq-escaped quote
	} {
		write(tc.line)
		if v, err := configValue(path, "SUMI_PERSONA_ID"); err != nil || v != tc.want {
			t.Fatalf("configValue(%q) = %q, %v", tc.line, v, err)
		}
	}

	// A rewrite on a quoted file keeps the quotes; on a bare file it stays bare.
	write("'" + slot + "'")
	if err := rewriteConfigKey(path, "SUMI_PERSONA_ID", next); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "SUMI_PERSONA_ID='"+next+"'") {
		t.Fatalf("the rewrite dropped the quoting: %s", raw)
	}
	if v, _ := configValue(path, "SUMI_PERSONA_ID"); v != next {
		t.Fatalf("configValue after rewrite: %q", v)
	}
	write(slot)
	if err := rewriteConfigKey(path, "SUMI_PERSONA_ID", next); err != nil {
		t.Fatal(err)
	}
	raw, _ = os.ReadFile(path)
	if !strings.Contains(string(raw), "SUMI_PERSONA_ID="+next+"\n") {
		t.Fatalf("the rewrite added quoting to a bare file: %s", raw)
	}
}
