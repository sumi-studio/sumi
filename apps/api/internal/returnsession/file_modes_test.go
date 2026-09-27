package returnsession_test

// Coverage for the explicit file-mode selection and the scoped storage
// credential lifecycle: no implicit mode, no records-only path, mint
// bound to the session lineage, supersede on re-mint, scope and
// revocation enforcement on the storage proxy.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// createMode is h.create() parameterized by the owner's file-mode
// selection — same grant-extraction contract.
func (h *harness) createMode(mode string) (sessionID, sessionURL, grant string) {
	h.t.Helper()
	code, raw := h.ownerReq(http.MethodPost, "/api/secretary-return/sessions",
		jsonBody(map[string]string{"file_mode": mode}))
	if code != http.StatusCreated {
		h.t.Fatalf("create file_mode=%s: %d %s", mode, code, raw)
	}
	var body struct {
		ReturnURL string             `json:"return_url"`
		Session   returnsession.View `json:"session"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		h.t.Fatalf("create answer: %s", raw)
	}
	parts := strings.SplitN(body.ReturnURL, "#grant=", 2)
	if len(parts) != 2 {
		h.t.Fatalf("return URL has no grant: %s", body.ReturnURL)
	}
	return body.Session.SessionID, parts[0], parts[1]
}

func unmarshal(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

// fakeFiles is a FileStore stand-in: it records freeze calls so a test
// can prove the local-mode fence actually fired (or did not), and that
// unfreezes stay inside their owning lineage.
type fakeFiles struct {
	frozenCalls []string
	listErr     error
}

func (f *fakeFiles) SetScopeFrozen(_ context.Context, scope, owner string, ownerEpoch int64, reason string, frozen bool) error {
	f.frozenCalls = append(f.frozenCalls, fmt.Sprintf("%s|%s@%d=%v", owner, scope, ownerEpoch, frozen))
	return nil
}

func (f *fakeFiles) List(_ context.Context, scope, path, cursor string, limit int) (fileaccess.ListResult, error) {
	return fileaccess.ListResult{}, f.listErr
}

// destMode is dest() with the destination's file-mode declaration —
// the harness helper declares "cloud" (its common case); local-mode
// tests declare what the mover actually runs.
func destMode(t *testing.T, p placement, personaID, slot, mode string) returnsession.Destination {
	d := dest(t, p, personaID, slot)
	d.FileMode = mode
	return d
}

// --- selection --------------------------------------------------------------

func TestCreateRequiresExplicitFileMode(t *testing.T) {
	h := setup(t, returnsession.Config{})
	for _, body := range []string{
		`{}`,
		`{"file_mode":""}`,
		`{"file_mode":"records"}`, // no records-only product path
		`{"file_mode":"bogus"}`,
		`{"file_mode":"LOCAL"}`, // exact vocabulary only
	} {
		code, raw := h.ownerReq(http.MethodPost, "/api/secretary-return/sessions",
			strings.NewReader(body))
		if code == http.StatusCreated {
			t.Fatalf("create with body %s was admitted: %s", body, raw)
		}
	}
	// Both product modes are admitted under the fixture policy.
	for _, mode := range []string{"local", "cloud"} {
		code, raw := h.ownerReq(http.MethodPost, "/api/secretary-return/sessions",
			jsonBody(map[string]string{"file_mode": mode}))
		if code != http.StatusCreated {
			t.Fatalf("create file_mode=%s: %d %s", mode, code, raw)
		}
		// Only one open session at a time — close it before the next.
		var v struct {
			Session returnsession.View `json:"session"`
		}
		unmarshal(t, raw, &v)
		h.ownerReq(http.MethodPost,
			fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", v.Session.SessionID), nil)
	}
}

func TestCreateRefusesModeNotEnabledOnDeployment(t *testing.T) {
	// A deployment that enables only "local" refuses "cloud" sessions —
	// the enabled set is the operator's, not the caller's.
	h := setup(t, returnsession.Config{FileModes: []string{"local"}})
	code, raw := h.ownerReq(http.MethodPost, "/api/secretary-return/sessions",
		jsonBody(map[string]string{"file_mode": "cloud"}))
	if code == http.StatusCreated {
		t.Fatalf("disabled mode was admitted: %s", raw)
	}
}

// --- the local-mode fence ----------------------------------------------------

// Binding a local-mode return must freeze the source scope first. With
// no file service configured the bind refuses rather than sealing an
// unfenced copy window.
func TestLocalBindRequiresFileService(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.createMode("local")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
	if code == http.StatusOK {
		t.Fatalf("local-mode bind sealed without a file service: %s", raw)
	}
	if got := authority(t, h.cloud, h.persona); got == "sealed" {
		t.Fatal("the source sealed without a file fence")
	}
}

func TestLocalBindFreezesSourceScope(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)

	sessionID, _, grant := h.createMode("local")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	if len(ff.frozenCalls) == 0 {
		t.Fatal("local-mode bind did not freeze the source scope")
	}
	wantScope, err := fileaccess.ScopeForPersona(h.persona)
	if err != nil {
		t.Fatal(err)
	}
	// Reconcile converges the desired state, so the log may carry an
	// initial unfreeze or repeated freezes — what must hold is that the
	// converged state after bind is frozen under THIS session's lineage
	// (owner + its durable file_epoch).
	last := ff.frozenCalls[len(ff.frozenCalls)-1]
	var epoch int64
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT file_epoch FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	if last != fmt.Sprintf("%s|%s@%d=true", sessionID, wantScope, epoch) {
		t.Fatalf("freeze calls %v — converged state must be %s|%s@%d=true", ff.frozenCalls, sessionID, wantScope, epoch)
	}
	if got := asView(t, raw).Status; got != returnsession.StatusSealed {
		t.Fatalf("after bind: %s", got)
	}
}

// --- the scoped storage credential -------------------------------------------

func TestFileCredentialLifecycle(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create() // cloud mode
	h.driveImport(t, sessionID, grant, "absent", h.persona, "")

	// Mint under the destination's grant — a real mutation (POST).
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessionID),
		grant, nil)
	if code != http.StatusCreated {
		t.Fatalf("mint: %d %s", code, raw)
	}
	var cred struct {
		Token string `json:"file_token"`
		Scope string `json:"scope"`
	}
	unmarshal(t, raw, &cred)
	if !strings.HasPrefix(cred.Token, "sft_") {
		t.Fatalf("token shape: %q", cred.Token)
	}
	wantScope, _ := fileaccess.ScopeForPersona(h.persona)
	if cred.Scope != wantScope {
		t.Fatalf("scope %q, want %q", cred.Scope, wantScope)
	}
	// Only the hash is stored — the raw token never reaches the DB.
	var stored int
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM persona_file_tokens WHERE token_hash = $1`,
		cred.Token).Scan(&stored); err != nil || stored != 0 {
		t.Fatalf("raw token persisted: %d rows", stored)
	}

	pid, scope, err := h.sessions.AuthorizeFileToken(h.ctx, cred.Token)
	if err != nil || pid != h.persona || scope != wantScope {
		t.Fatalf("authorize: %v %v %v", pid, scope, err)
	}

	// A re-mint supersedes — the lost first token is dead.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessionID),
		grant, nil)
	if code != http.StatusCreated {
		t.Fatalf("re-mint: %d %s", code, raw)
	}
	var cred2 struct {
		Token string `json:"file_token"`
	}
	unmarshal(t, raw, &cred2)
	if cred2.Token == cred.Token {
		t.Fatal("re-mint returned the same token")
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, cred.Token); err == nil {
		t.Fatal("superseded token still authorizes")
	}

	// Owner revocation kills the live credential.
	n, err := h.sessions.RevokePersonaFileTokens(h.ctx, h.owner)
	if err != nil || n < 1 {
		t.Fatalf("revoke: %d %v", n, err)
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, cred2.Token); err == nil {
		t.Fatal("revoked token still authorizes")
	}
}

func TestFileCredentialRefusedOnLocalMode(t *testing.T) {
	h := setup(t, returnsession.Config{})
	h.sessions.SetFileStore(&fakeFiles{})
	sessionID, _, grant := h.createMode("local")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessionID),
		grant, nil)
	if code == http.StatusCreated {
		t.Fatalf("local-mode mint admitted: %s", raw)
	}
}

// --- the storage proxy's authorization ----------------------------------------

// The proxy itself: wrong scope is 403, unknown/revoked token is 401,
// and a valid token reaches the upstream call (502 here — the client
// points at a dead upstream deliberately; authorization already passed).
func TestFileProxyAuthorizesScopeAndRevocation(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	h.driveImport(t, sessionID, grant, "absent", h.persona, "")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessionID),
		grant, nil)
	if code != http.StatusCreated {
		t.Fatalf("mint: %d %s", code, raw)
	}
	var cred struct {
		Token string `json:"file_token"`
		Scope string `json:"scope"`
	}
	unmarshal(t, raw, &cred)

	// A file client pointing at a dead upstream: auth runs locally,
	// the upstream call fails to connect → 502.
	fc, err := fileaccess.NewClient("http://127.0.0.1:1", "unused")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	proxySrv := httptest.NewServer(mux)
	t.Cleanup(proxySrv.Close)
	server, err := returnsession.NewServer(h.sessions, h.proof, proxySrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	server.SetFiles(fc)
	server.RegisterFileProxy(mux)

	scopePath := fmt.Sprintf("/api/secretary-files/v1/files/%s/list", cred.Scope)
	otherPath := "/api/secretary-files/v1/files/other-scope/list"

	get := func(path, token string) (int, []byte) {
		req, _ := http.NewRequest(http.MethodGet, proxySrv.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		buf := make([]byte, 4096)
		n, _ := res.Body.Read(buf)
		return res.StatusCode, buf[:n]
	}

	if code, _ := get(scopePath, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d (want 401)", code)
	}
	if code, _ := get(scopePath, "sft_forged"); code != http.StatusUnauthorized {
		t.Fatalf("forged token: %d (want 401)", code)
	}
	if code, body := get(otherPath, cred.Token); code != http.StatusForbidden {
		t.Fatalf("wrong scope: %d %s (want 403)", code, body)
	}
	if code, body := get(scopePath, cred.Token); code != http.StatusBadGateway {
		t.Fatalf("valid token at dead upstream: %d %s (want 502 — authz passed)", code, body)
	}

	if _, err := h.sessions.RevokePersonaFileTokens(h.ctx, h.owner); err != nil {
		t.Fatal(err)
	}
	if code, _ := get(scopePath, cred.Token); code != http.StatusUnauthorized {
		t.Fatalf("revoked token: %d (want 401)", code)
	}
}

// --- lineage: only the current store owner decides file state ----------------

// returnPersonaToCloud moves the secretary back: a forward local→cloud
// transfer reclaims the transferred Cloud slot and makes the persona
// live on Cloud again so a further return session can exist. supersedes
// is the export under which Cloud gave the persona up (the return
// session's id); the forward transfer id is returned — the lineage the
// next import into the surrendered Local copy names.
func (h *harness) returnPersonaToCloud(t *testing.T, supersedes string) string {
	t.Helper()
	ctx := h.ctx
	forwardID := newID(t)
	destID, err := h.cloud.svc.PlacementID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.local.svc.Seal(ctx, h.persona, forwardID, destID); err != nil {
		t.Fatal(err)
	}
	var bundle bytes.Buffer
	if _, err := h.local.svc.Export(ctx, h.persona, forwardID, &bundle); err != nil {
		t.Fatal(err)
	}
	rec, _, err := h.cloud.svc.ImportReturning(ctx, &bundle, &h.human, true, supersedes)
	if err != nil {
		t.Fatal(err)
	}
	act, err := h.cloud.svc.Activate(ctx, rec.PersonaID, forwardID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.local.svc.Complete(ctx, h.persona, forwardID, act.ActivateProof); err != nil {
		t.Fatal(err)
	}
	mustExec(t, h.cloud.pool, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, h.human, h.persona)
	return forwardID
}

// A newer bound return supersedes the older lineage: the stale session's
// mint is refused outright (checked under the session row lock), and the
// current lineage's mint retires the stale token — a credential can
// never outlive the session that was superseded while still "active".
func TestNewerBoundReturnSupersedesStaleCredential(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessA, _, grantA := h.create() // cloud mode
	h.driveImport(t, sessA, grantA, "absent", h.persona, "")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessA),
		grantA, nil)
	if code != http.StatusCreated {
		t.Fatalf("first mint: %d %s", code, raw)
	}
	var credA struct {
		Token string `json:"file_token"`
	}
	unmarshal(t, raw, &credA)

	h.returnPersonaToCloud(t, sessA)

	sessB, _, grantB := h.create()
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
		grantB, jsonBody(dest(t, h.local, h.persona, "surrendered")))
	if code != http.StatusOK {
		t.Fatalf("second bind: %d %s", code, raw)
	}

	// The superseded lineage cannot mint — even though its own row is
	// still "completed", the newer bound return owns the store now.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessA),
		grantA, nil)
	if code == http.StatusCreated {
		t.Fatalf("superseded session minted: %s", raw)
	}

	// The live lineage mints — and retires the stale destination token.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessB),
		grantB, nil)
	if code != http.StatusCreated {
		t.Fatalf("live lineage mint: %d %s", code, raw)
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, credA.Token); err == nil {
		t.Fatal("stale session's token still authorizes after the newer mint")
	}
}

// A completed local-mode return is the working-store decision: every
// Cloud storage credential for the persona predates it and dies, while
// the retained Cloud copy keeps its read-only barrier.
func TestCompletedLocalRevokesCloudCredentials(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)

	sessA, _, grantA := h.create() // cloud mode
	h.driveImport(t, sessA, grantA, "absent", h.persona, "")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/files-credential", sessA),
		grantA, nil)
	if code != http.StatusCreated {
		t.Fatalf("mint: %d %s", code, raw)
	}
	var credA struct {
		Token string `json:"file_token"`
	}
	unmarshal(t, raw, &credA)
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, credA.Token); err != nil {
		t.Fatalf("minted token does not authorize: %v", err)
	}

	forwardID := h.returnPersonaToCloud(t, sessA)

	// A local-mode return carries the store home — on completion its
	// converge retires every active Cloud file credential.
	sessB, _, grantB := h.createMode("local")
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
		grantB, jsonBody(destMode(t, h.local, h.persona, "surrendered", "local")))
	if code != http.StatusOK {
		t.Fatalf("local bind: %d %s", code, raw)
	}
	code, bundle := h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s/bundle", sessB), grantB, nil)
	if code != http.StatusOK {
		t.Fatalf("download: %d", code)
	}
	rec, _, err := h.local.svc.ImportReturning(h.ctx, bytes.NewReader(bundle), nil, false, forwardID)
	if err != nil {
		t.Fatalf("reclaim import: %v", err)
	}
	act, err := h.local.svc.Activate(h.ctx, rec.PersonaID, sessB)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/activated", sessB),
		grantB, jsonBody(map[string]string{"activate_proof": act.ActivateProof}))
	if code != http.StatusOK {
		t.Fatalf("report: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusCompleted {
		t.Fatalf("local return: %s", v.Status)
	}
	if _, _, err := h.sessions.AuthorizeFileToken(h.ctx, credA.Token); err == nil {
		t.Fatal("cloud credential survived a completed local-mode return")
	}
}

// A superseded session's reconcile cannot drop a newer lineage's
// barrier: converge mutates file state only for the session that
// currently owns the store decision. A completed local return keeps
// owning its retained-copy barrier until a newer bound return takes
// over — from then on its retries and sweep passes touch nothing.
func TestStaleReconcileCannotDropLiveBarrier(t *testing.T) {
	h := setup(t, returnsession.Config{})
	ff := &fakeFiles{}
	h.sessions.SetFileStore(ff)
	scope, err := fileaccess.ScopeForPersona(h.persona)
	if err != nil {
		t.Fatal(err)
	}

	sessA, _, grantA := h.createMode("local")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessA),
		grantA, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
	if code != http.StatusOK {
		t.Fatalf("first bind: %d %s", code, raw)
	}
	h.driveImportRest(t, sessA, grantA) // completes: A's barrier stays (retained copy)

	forwardID := h.returnPersonaToCloud(t, sessA)

	// A newer return takes over the store: the barrier becomes B's.
	sessB, _, grantB := h.createMode("local")
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessB),
		grantB, jsonBody(destMode(t, h.local, h.persona, "surrendered", "local")))
	if code != http.StatusOK {
		t.Fatalf("second bind: %d %s", code, raw)
	}
	_ = forwardID
	ff.frozenCalls = nil

	// Reconcile the superseded session again — a stale retry, a late
	// resume or a sweep pass. It must not touch B's barrier.
	if err := h.sessions.Reconcile(h.ctx, sessA); err != nil {
		t.Fatalf("stale reconcile: %v", err)
	}
	for _, c := range ff.frozenCalls {
		if strings.HasPrefix(c, sessB+"|"+scope+"@") && strings.HasSuffix(c, "=false") {
			t.Fatalf("stale reconcile dropped the live barrier: %v", ff.frozenCalls)
		}
	}
}
