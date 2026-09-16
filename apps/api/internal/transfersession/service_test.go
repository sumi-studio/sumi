package transfersession_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession/transfersessiontest"
)

// placement is one real PostgreSQL database. The Local source uses
// SUMI_TRANSFER_SOURCE_DB_URL when set, so source and destination can run on
// separate PostgreSQL servers; otherwise both use SUMI_TEST_DB_URL.
type placement struct {
	pool  *pgxpool.Pool
	state *agentstate.Store
	svc   *portable.Service
}

func newPlacement(t *testing.T, source bool) placement {
	t.Helper()
	if src := os.Getenv("SUMI_TRANSFER_SOURCE_DB_URL"); source && src != "" && os.Getenv("SUMI_TEST_DB_URL") != "" {
		dst := os.Getenv("SUMI_TEST_DB_URL")
		os.Setenv("SUMI_TEST_DB_URL", src)
		defer os.Setenv("SUMI_TEST_DB_URL", dst)
	}
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return placement{pool: pool, state: agentstate.NewStore(pool), svc: portable.NewService(pool)}
}

func newID(t *testing.T) string {
	t.Helper()
	return uuid.Must(uuid.NewV7()).String()
}

type harness struct {
	t        *testing.T
	ctx      context.Context
	local    placement
	cloud    placement
	sessions *transfersession.Service
	proof    *transfersessiontest.Proof
	srv      *httptest.Server
	pid      string
}

func setup(t *testing.T, cfg transfersession.Config) *harness {
	t.Helper()
	ctx := context.Background()
	h := &harness{t: t, ctx: ctx, local: newPlacement(t, true), cloud: newPlacement(t, false)}
	h.pid = newID(t)
	if _, _, err := h.local.state.EnsurePersona(ctx, h.pid, nil, "Local secretary"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.local.state.SubmitInput(ctx, &agentstate.Input{
		PersonaID: h.pid, InputID: "in-carried", Kind: "message",
		Payload: map[string]any{"text": "明日 9 時に会議"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil {
		t.Fatal(err)
	}
	h.sessions = transfersession.New(h.cloud.pool, cfg)
	h.proof = transfersessiontest.NewProof()
	mux := http.NewServeMux()
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
	server, err := transfersession.NewServer(h.sessions, h.proof, h.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterRoutes(mux)
	return h
}

func (h *harness) request(method, path, grant string, body io.Reader) (int, []byte) {
	h.t.Helper()
	req, err := http.NewRequestWithContext(h.ctx, method, h.srv.URL+path, body)
	if err != nil {
		h.t.Fatal(err)
	}
	if grant != "" {
		req.Header.Set("Authorization", "Bearer "+grant)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}

func jsonBody(v any) io.Reader {
	raw, _ := json.Marshal(v)
	return bytes.NewReader(raw)
}

// flow registers a FIXTURE registration flow for uid (see transfersessiontest).
func (h *harness) flow(uid string, ttl time.Duration) map[string]string {
	id, nonce := newID(h.t), newID(h.t)
	h.proof.Add(id, nonce, uid, ttl)
	return map[string]string{"flow_id": id, "nonce": nonce}
}

func asView(t *testing.T, raw []byte) transfersession.View {
	t.Helper()
	var env struct {
		Session *transfersession.View `json:"session"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if env.Session != nil {
		return *env.Session
	}
	var v transfersession.View
	_ = json.Unmarshal(raw, &v)
	return v
}

func (h *harness) create(uid string) (string, string) {
	h.t.Helper()
	code, raw := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(h.flow(uid, time.Minute)))
	if code != http.StatusCreated {
		h.t.Fatalf("create: %d %s", code, raw)
	}
	var out struct {
		MoveURL string `json:"move_url"`
	}
	_ = json.Unmarshal(raw, &out)
	v := asView(h.t, raw)
	prefix := h.srv.URL + "/api/secretary-transfer/sessions/" + v.SessionID + "#grant="
	if !strings.HasPrefix(out.MoveURL, prefix) {
		h.t.Fatalf("move url %q does not name the session resource", out.MoveURL)
	}
	return v.SessionID, strings.TrimPrefix(out.MoveURL, prefix)
}

// bind takes the session for the local placement, the way sumi-local-move
// does before it seals anything.
func (h *harness) bind(sessionID, grant string) (int, transfersession.View) {
	h.t.Helper()
	own, err := h.local.svc.PlacementID(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	return h.bindAs(sessionID, grant, transfersession.Source{PlacementID: own, PersonaID: h.pid})
}

func (h *harness) bindAs(sessionID, grant string, src transfersession.Source) (int, transfersession.View) {
	h.t.Helper()
	code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sessionID+"/source", grant, jsonBody(src))
	return code, asView(h.t, raw)
}

// seal takes the session for this placement and then seals, in that order —
// the order sumi-local-move uses, and the only order the destination admits.
func (h *harness) seal(sessionID, grant string) []byte {
	h.t.Helper()
	if code, v := h.bind(sessionID, grant); code != http.StatusOK {
		h.t.Fatalf("bind source: %d %+v", code, v)
	}
	dest, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		h.t.Fatal(err)
	}
	if _, err := h.local.svc.Seal(h.ctx, h.pid, sessionID, dest); err != nil {
		h.t.Fatalf("seal: %v", err)
	}
	var buf bytes.Buffer
	if _, err := h.local.svc.Export(h.ctx, h.pid, sessionID, &buf); err != nil {
		h.t.Fatalf("export: %v", err)
	}
	return buf.Bytes()
}

func (h *harness) upload(sessionID, grant string, body io.Reader) (int, transfersession.View) {
	h.t.Helper()
	code, raw := h.request("PUT", "/api/secretary-transfer/sessions/"+sessionID+"/bundle", grant, body)
	return code, asView(h.t, raw)
}

func (h *harness) stage(uid string) (string, string, []byte) {
	h.t.Helper()
	sid, grant := h.create(uid)
	bundle := h.seal(sid, grant)
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusCreated || v.Status != "staged" {
		h.t.Fatalf("upload: %d %+v", code, v)
	}
	return sid, grant, bundle
}

func (h *harness) view(sessionID, grant string) transfersession.View {
	h.t.Helper()
	code, raw := h.request("GET", "/api/secretary-transfer/sessions/"+sessionID, grant, nil)
	if code != http.StatusOK {
		h.t.Fatalf("status: %d %s", code, raw)
	}
	return asView(h.t, raw)
}

// waitClaimDeadline blocks until the session's committed claim_until has
// passed, so a sweep started afterwards is genuinely due.
func (h *harness) waitClaimDeadline(sessionID string, within time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for {
		var due bool
		if err := h.cloud.pool.QueryRow(h.ctx,
			`SELECT claim_until <= now() FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&due); err != nil {
			h.t.Fatal(err)
		}
		if due {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("claim deadline for %s did not pass within %s", sessionID, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (h *harness) importRows(sessionID string) int {
	h.t.Helper()
	var n int
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT count(*) FROM core_transfers WHERE direction = 'import' AND transfer_id = $1`, sessionID).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) dbStatus(sessionID string) string {
	h.t.Helper()
	var s string
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT status FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&s); err != nil {
		h.t.Fatal(err)
	}
	return s
}

func authority(t *testing.T, p placement, pid string) string {
	t.Helper()
	var a string
	err := p.pool.QueryRow(context.Background(), `SELECT authority FROM core_personas WHERE persona_id = $1`, pid).Scan(&a)
	if errors.Is(err, pgx.ErrNoRows) {
		return "absent"
	}
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// provision runs the FIXTURE account transaction under a fresh live flow.
func (h *harness) provision(uid, sessionID string, beforeCommit func(pgx.Tx) error) (string, string, error) {
	f := h.flow(uid, time.Minute)
	return h.proof.ProvisionAccount(h.ctx, h.cloud.pool, f["flow_id"], f["nonce"], sessionID, beforeCommit)
}

func header(t *testing.T, bundle []byte) portable.Header {
	t.Helper()
	var hdr portable.Header
	if err := json.Unmarshal(bundle[:bytes.IndexByte(bundle, '\n')], &hdr); err != nil {
		t.Fatal(err)
	}
	return hdr
}

func randomGrant() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func uidFor(t *testing.T, name string) string { return "fixture-" + name + "-" + newID(t)[24:] }

func TestRegistrationBringsTheSameSecretary(t *testing.T) {
	h := setup(t, transfersession.Config{})
	ctx := h.ctx
	uid, other := uidFor(t, "owner"), uidFor(t, "other")

	if code, _ := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(map[string]string{"flow_id": "x", "nonce": "y"})); code != http.StatusUnauthorized {
		t.Fatalf("unproven flow created a session: %d", code)
	}
	if code, _ := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(h.flow(uid, -time.Second))); code != http.StatusUnauthorized {
		t.Fatalf("expired flow created a session: %d", code)
	}
	if code, _ := h.request("POST", "/api/secretary-transfer/sessions", "",
		jsonBody(map[string]string{"flow_id": "x", "nonce": "y", "firebase_uid": uid})); code != http.StatusBadRequest {
		t.Fatalf("a body-supplied subject was not refused: %d", code)
	}

	sid, grant := h.create(uid)
	if code, raw := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(h.flow(uid, time.Minute))); code != http.StatusConflict || !bytes.Contains(raw, []byte(sid)) {
		t.Fatalf("second open session for one credential: %d %s", code, raw)
	}
	sid2, grant2 := h.create(other)
	for name, g := range map[string]string{"none": "", "random": randomGrant(), "other session's": grant2} {
		if code, _ := h.request("GET", "/api/secretary-transfer/sessions/"+sid, g, nil); code != http.StatusUnauthorized {
			t.Fatalf("%s grant read the session: %d", name, code)
		}
	}
	v := h.view(sid, grant)
	cloudID, _ := h.cloud.svc.PlacementID(ctx)
	if v.Status != "awaiting_bundle" || v.TransferID != sid || v.DestinationPlacementID != cloudID || !v.StateOnly || len(v.NotIncluded) == 0 {
		t.Fatalf("new session view: %+v", v)
	}

	bundle := h.seal(sid, grant)
	if bytes.Contains(bundle, []byte(grant)) {
		t.Fatal("the bundle carries the Cloud grant")
	}
	code, v := h.upload(sid, grant, bytes.NewReader(bundle))
	if code != http.StatusCreated || v.Status != "staged" || v.Arrival == nil || v.Arrival.PersonaID != h.pid ||
		v.Arrival.Continuity.QueuedInputs != 1 || !v.Arrival.ModelConnectionRequired || v.ClaimUntil == nil {
		t.Fatalf("upload: %d %+v", code, v)
	}
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusOK || v.Status != "staged" {
		t.Fatalf("duplicate upload: %d %+v", code, v)
	}
	if code, _ := h.upload(sid2, grant, bytes.NewReader(bundle)); code != http.StatusUnauthorized {
		t.Fatalf("grant uploaded into another session: %d", code)
	}
	if a := authority(t, h.cloud, h.pid); a != "staged" {
		t.Fatalf("cloud authority after staging: %s", a)
	}

	code, raw := h.request("POST", "/api/secretary-transfer/registrant/session", "", jsonBody(h.flow(uid, time.Minute)))
	if code != http.StatusOK || asView(t, raw).Status != "staged" {
		t.Fatalf("registrant view: %d %s", code, raw)
	}
	if code, _ := h.request("POST", "/api/secretary-transfer/registrant/session", "", jsonBody(h.flow(uidFor(t, "stranger"), time.Minute))); code != http.StatusNotFound {
		t.Fatalf("a stranger found a session: %d", code)
	}

	tx, err := h.cloud.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transfersession.ClaimInTx(ctx, tx, sid, transfersession.Subject{Provider: "firebase", Subject: other}); !errors.Is(err, transfersession.ErrNotFound) {
		t.Fatalf("another credential's claim: %v", err)
	}
	_ = tx.Rollback(ctx)
	f := h.flow(uid, -time.Second)
	if _, _, err := h.proof.ProvisionAccount(ctx, h.cloud.pool, f["flow_id"], f["nonce"], sid, nil); !errors.Is(err, transfersessiontest.ErrFixtureProof) {
		t.Fatalf("expired fixture proof provisioned: %v", err)
	}

	humanID, personaID, err := h.provision(uid, sid, nil)
	if err != nil || personaID != h.pid {
		t.Fatalf("provision: %v %s", err, personaID)
	}
	if s, a := h.dbStatus(sid), authority(t, h.cloud, h.pid); s != "provisioned" || a != "staged" {
		t.Fatalf("after provisioning: session %s, authority %s (provisioned is not active)", s, a)
	}
	if code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "", jsonBody(h.flow(uid, time.Minute))); code != http.StatusConflict {
		t.Fatalf("browser cancel after provisioning: %d %s", code, raw)
	}
	v = h.view(sid, grant)
	if v.Status != "activated" || v.ActivateProof == "" {
		t.Fatalf("owed activation on read: %+v", v)
	}
	if code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant, jsonBody(map[string]string{})); code != http.StatusConflict {
		t.Fatalf("grant cancel after activation: %d %s", code, raw)
	}
	var bound string
	if err := h.cloud.pool.QueryRow(ctx, `SELECT human_id FROM core_personas WHERE persona_id = $1`, h.pid).Scan(&bound); err != nil || bound != humanID {
		t.Fatalf("carried persona bound to %q (%v), want %s", bound, err, humanID)
	}
	_, raw = h.request("POST", "/api/secretary-transfer/registrant/session", "", jsonBody(h.flow(uid, time.Minute)))
	if bytes.Contains(raw, []byte("activate_proof")) || asView(t, raw).Status != "activated" {
		t.Fatalf("registrant view after activation: %s", raw)
	}

	if _, err := h.local.svc.Complete(ctx, h.pid, sid, v.ActivateProof); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := h.local.state.AcquireWriter(ctx, h.pid, "local", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("local writer after completion: %v", err)
	}
	if _, err := h.cloud.state.AcquireWriter(ctx, h.pid, "cloud", time.Minute); err != nil {
		t.Fatalf("cloud writer: %v", err)
	}
}

func TestExpiredAdmissionKeepsReceiptAccess(t *testing.T) {
	h := setup(t, transfersession.Config{})
	sid, grant := h.create(uidFor(t, "late"))
	bundle := h.seal(sid, grant)
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE transfer_sessions SET admit_until = now() - interval '1 second' WHERE session_id = $1`, sid); err != nil {
		t.Fatal(err)
	}
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusGone || v.Status != "expired" {
		t.Fatalf("upload after the deadline: %d %+v", code, v)
	}
	if _, err := h.cloud.svc.Status(h.ctx, "import", sid); !errors.Is(err, portable.ErrTransferNotFound) {
		t.Fatalf("an expired session imported: %v", err)
	}
	// The sealed source still reads its session and earns its proof.
	hdr := header(t, bundle)
	if code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant,
		jsonBody(map[string]string{"persona_id": hdr.PersonaID})); code != http.StatusBadRequest {
		t.Fatalf("persona without key: %d %s", code, raw)
	}
	code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant,
		jsonBody(map[string]string{"persona_id": hdr.PersonaID, "transfer_key": hdr.TransferKey}))
	v := asView(t, raw)
	if code != http.StatusOK || !v.Retired || v.RetireProof == "" {
		t.Fatalf("source cancel after expiry: %d %s", code, raw)
	}
	if _, err := h.local.svc.Abort(h.ctx, h.pid, sid, v.RetireProof); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if a := authority(t, h.local, h.pid); a != "active" {
		t.Fatalf("local authority after abort: %s", a)
	}
	if code, _ := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusGone {
		t.Fatalf("late upload after the tombstone: %d", code)
	}
	if rec, err := h.cloud.svc.Status(h.ctx, "import", sid); err != nil || rec.Status != "retired" {
		t.Fatalf("tombstone: %+v %v", rec, err)
	}
}

func TestPartialAndMisaddressedBundles(t *testing.T) {
	h := setup(t, transfersession.Config{})
	sid, grant := h.create(uidFor(t, "partial"))
	bundle := h.seal(sid, grant)
	if code, _ := h.upload(sid, grant, bytes.NewReader(bundle[:len(bundle)/2])); code != http.StatusUnprocessableEntity {
		t.Fatalf("truncated upload: %d", code)
	}
	if v := h.view(sid, grant); v.Status != "awaiting_bundle" || authority(t, h.cloud, h.pid) != "absent" {
		t.Fatalf("truncated upload left %+v", v)
	}
	sid2, grant2 := h.create(uidFor(t, "other"))
	if code, _ := h.upload(sid2, grant2, bytes.NewReader(bundle)); code != http.StatusUnprocessableEntity {
		t.Fatalf("bundle of another transfer: %d", code)
	}
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusCreated || v.Status != "staged" {
		t.Fatalf("retry after the partial upload: %d %+v", code, v)
	}
}

// waitAdvisoryLock waits until some transaction on the cloud database holds a
// transfer advisory lock — the import has read its header and is now
// streaming rows inside its transaction.
func (h *harness) waitAdvisoryLock() {
	h.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		var n int
		if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM pg_locks
			WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&n); err != nil {
			h.t.Fatal(err)
		}
		if n > 0 {
			return
		}
	}
	h.t.Fatal("the import never took its transfer lock")
}

type result struct {
	code int
	v    transfersession.View
}

func TestCancelDuringImportRetiresTheLateStage(t *testing.T) {
	for _, byGrant := range []bool{false, true} {
		t.Run(fmt.Sprintf("source_key=%t", byGrant), func(t *testing.T) {
			h := setup(t, transfersession.Config{})
			uid := uidFor(t, "race")
			sid, grant := h.create(uid)
			bundle := h.seal(sid, grant)
			nl := bytes.IndexByte(bundle, '\n') + 1
			pr, pw := io.Pipe()
			uploaded := make(chan result, 1)
			go func() {
				code, v := h.upload(sid, grant, pr)
				uploaded <- result{code, v}
			}()
			if _, err := pw.Write(bundle[:nl]); err != nil {
				t.Fatal(err)
			}
			h.waitAdvisoryLock()

			cancelled := make(chan result, 1)
			go func() {
				var code int
				var raw []byte
				if byGrant {
					hdr := header(t, bundle)
					code, raw = h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant,
						jsonBody(map[string]string{"persona_id": hdr.PersonaID, "transfer_key": hdr.TransferKey}))
				} else {
					code, raw = h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "", jsonBody(h.flow(uid, time.Minute)))
				}
				cancelled <- result{code, asView(t, raw)}
			}()
			if !byGrant {
				// Without the key nothing can be retired yet; the cancel answers at once.
				if r := <-cancelled; r.code != http.StatusOK || r.v.Status != "cancelled" || r.v.Retired {
					t.Fatalf("browser cancel during import: %+v", r)
				}
			} else {
				time.Sleep(300 * time.Millisecond)
				select {
				case r := <-cancelled:
					t.Fatalf("the source's tombstone did not wait for the import in flight: %+v", r)
				default:
				}
			}
			if _, err := pw.Write(bundle[nl:]); err != nil {
				t.Fatal(err)
			}
			_ = pw.Close()
			up := <-uploaded
			if up.code != http.StatusGone || up.v.Status != "cancelled" || !up.v.Retired || up.v.RetireProof == "" {
				t.Fatalf("late import after cancel: %+v", up)
			}
			if byGrant {
				if r := <-cancelled; r.code != http.StatusOK || r.v.RetireProof != up.v.RetireProof {
					t.Fatalf("source cancel: %+v", r)
				}
			}
			if a := authority(t, h.cloud, h.pid); a != "absent" {
				t.Fatalf("cloud kept a usable stage: %s", a)
			}
			if _, _, err := h.provision(uid, sid, nil); !errors.Is(err, transfersession.ErrClosed) {
				t.Fatalf("claim after cancel: %v", err)
			}
			if _, err := h.local.svc.Abort(h.ctx, h.pid, sid, up.v.RetireProof); err != nil || authority(t, h.local, h.pid) != "active" {
				t.Fatalf("abort: %v", err)
			}
		})
	}
}

func TestCrashBetweenImportAndSessionUpdate(t *testing.T) {
	t.Run("the next read promotes the committed import", func(t *testing.T) {
		h := setup(t, transfersession.Config{})
		sid, grant := h.create(uidFor(t, "crash"))
		bundle := h.seal(sid, grant)
		// The upload's import commits; the handler dies before the session update.
		if _, _, err := h.cloud.svc.Import(h.ctx, bytes.NewReader(bundle), nil, false); err != nil {
			t.Fatal(err)
		}
		if s := h.dbStatus(sid); s != "awaiting_bundle" {
			t.Fatalf("precondition: %s", s)
		}
		if v := h.view(sid, grant); v.Status != "staged" || v.Arrival == nil {
			t.Fatalf("read after the crash: %+v", v)
		}
		if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusOK || v.Status != "staged" {
			t.Fatalf("re-upload after the crash: %d %+v", code, v)
		}
	})
	t.Run("a session closed before the update retires the orphan", func(t *testing.T) {
		h := setup(t, transfersession.Config{})
		sid, grant := h.create(uidFor(t, "orphan"))
		bundle := h.seal(sid, grant)
		if _, _, err := h.cloud.svc.Import(h.ctx, bytes.NewReader(bundle), nil, false); err != nil {
			t.Fatal(err)
		}
		// A cancel committed, and both it and the upload died before retiring.
		if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE transfer_sessions SET status = 'cancelled' WHERE session_id = $1`, sid); err != nil {
			t.Fatal(err)
		}
		if n, err := h.sessions.Sweep(h.ctx); err != nil || n != 1 {
			t.Fatalf("sweep: %d %v", n, err)
		}
		if a := authority(t, h.cloud, h.pid); a != "absent" {
			t.Fatalf("orphan stage survived the sweep: %s", a)
		}
		if n, err := h.sessions.Sweep(h.ctx); err != nil || n != 0 {
			t.Fatalf("sweeper replay: %d %v", n, err)
		}
		v := h.view(sid, grant)
		if v.Status != "cancelled" || v.RetireProof == "" {
			t.Fatalf("view: %+v", v)
		}
		if _, err := h.local.svc.Abort(h.ctx, h.pid, sid, v.RetireProof); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAccountTransactionHoldsTheSession(t *testing.T) {
	t.Run("a cancel waits for the claim and is refused", func(t *testing.T) {
		h := setup(t, transfersession.Config{})
		uid := uidFor(t, "claim")
		sid, grant, _ := h.stage(uid)
		cancelled := make(chan result, 1)
		_, _, err := h.provision(uid, sid, func(pgx.Tx) error {
			go func() {
				code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant, jsonBody(map[string]string{}))
				cancelled <- result{code, asView(t, raw)}
			}()
			select {
			case r := <-cancelled:
				return fmt.Errorf("cancel passed the account transaction: %+v", r)
			case <-time.After(300 * time.Millisecond):
				return nil
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if r := <-cancelled; r.code != http.StatusConflict || r.v.Status == "cancelled" {
			t.Fatalf("cancel after the claim committed: %+v", r)
		}
		if v := h.view(sid, grant); v.Status != "activated" || v.Retired {
			t.Fatalf("view: %+v", v)
		}
	})
	t.Run("a cancel that commits first stops the claim", func(t *testing.T) {
		h := setup(t, transfersession.Config{})
		uid := uidFor(t, "first")
		sid, grant, _ := h.stage(uid)
		if code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "", jsonBody(h.flow(uid, time.Minute))); code != http.StatusOK {
			t.Fatalf("cancel: %d %s", code, raw)
		}
		if _, _, err := h.provision(uid, sid, nil); !errors.Is(err, transfersession.ErrClosed) {
			t.Fatalf("claim after cancel: %v", err)
		}
		var humans int
		if err := h.cloud.pool.QueryRow(h.ctx, `SELECT count(*) FROM credentials WHERE external_subject = $1`, uid).Scan(&humans); err != nil || humans != 0 {
			t.Fatalf("a refused claim left an account: %d %v", humans, err)
		}
		if v := h.view(sid, grant); v.Status != "cancelled" || !v.Retired || authority(t, h.cloud, h.pid) != "absent" {
			t.Fatalf("view: %+v", v)
		}
	})
	t.Run("expiry cannot pass a claim in flight", func(t *testing.T) {
		h := setup(t, transfersession.Config{ClaimTTL: 1500 * time.Millisecond})
		uid := uidFor(t, "expiry")
		sid, grant, _ := h.stage(uid)
		swept := make(chan error, 1)
		_, _, err := h.provision(uid, sid, func(pgx.Tx) error {
			// Wait for the claim deadline the committed row actually
			// carries, not for a duration counted from when this test
			// started: on a loaded host the staged promotion lands well
			// after the upload, and a fixed sleep then sweeps a session
			// that is not due yet and proves nothing.
			h.waitClaimDeadline(sid, 10*time.Second)
			go func() {
				_, err := h.sessions.Sweep(h.ctx)
				swept <- err
			}()
			select {
			case err := <-swept:
				return fmt.Errorf("sweep passed the account transaction: %v", err)
			case <-time.After(300 * time.Millisecond):
				return nil
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := <-swept; err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if s := h.dbStatus(sid); s != "provisioned" {
			t.Fatalf("session after the racing sweep: %s", s)
		}
		if v := h.view(sid, grant); v.Status != "activated" || v.Retired || authority(t, h.cloud, h.pid) != "active" {
			t.Fatalf("view: %+v", v)
		}
	})
}

func TestOwedActivationSurvivesACrash(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "owed")
	sid, grant, _ := h.stage(uid)
	// The account transaction commits; the process dies before activating.
	if _, _, err := h.provision(uid, sid, nil); err != nil {
		t.Fatal(err)
	}
	if n, err := h.sessions.Sweep(h.ctx); err != nil || n != 1 {
		t.Fatalf("sweep: %d %v", n, err)
	}
	if s, a := h.dbStatus(sid), authority(t, h.cloud, h.pid); s != "activated" || a != "active" {
		t.Fatalf("after sweep: %s %s", s, a)
	}
	if n, err := h.sessions.Sweep(h.ctx); err != nil || n != 0 {
		t.Fatalf("sweeper replay: %d %v", n, err)
	}
	v := h.view(sid, grant)
	if _, err := h.local.svc.Complete(h.ctx, h.pid, sid, v.ActivateProof); err != nil {
		t.Fatal(err)
	}
}

func TestStagedClaimDeadline(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "deadline")
	sid, grant, _ := h.stage(uid)
	if _, err := h.cloud.pool.Exec(h.ctx, `UPDATE transfer_sessions SET claim_until = now() - interval '1 second' WHERE session_id = $1`, sid); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.provision(uid, sid, nil); !errors.Is(err, transfersession.ErrExpired) {
		t.Fatalf("claim after the deadline: %v", err)
	}
	if _, err := h.sessions.Sweep(h.ctx); err != nil {
		t.Fatal(err)
	}
	v := h.view(sid, grant)
	if v.Status != "expired" || !v.Retired || v.RetireProof == "" {
		t.Fatalf("view: %+v", v)
	}
	if _, err := h.local.svc.Abort(h.ctx, h.pid, sid, v.RetireProof); err != nil {
		t.Fatal(err)
	}
	// The credential may start again.
	h.create(uid)
}

func TestCreateRefusesAnExistingAccount(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "existing")
	human := newID(t)
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO humans (human_id) VALUES ($1)`, human); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cloud.pool.Exec(h.ctx, `INSERT INTO credentials (provider, external_subject, human_id) VALUES ('firebase', $1, $2)`, uid, human); err != nil {
		t.Fatal(err)
	}
	if code, raw := h.request("POST", "/api/secretary-transfer/sessions", "", jsonBody(h.flow(uid, time.Minute))); code != http.StatusConflict || !bytes.Contains(raw, []byte("existing account")) {
		t.Fatalf("create for an existing account: %d %s", code, raw)
	}
}

func TestNewServerRefusesAnUnsafeBase(t *testing.T) {
	for _, base := range []string{"http://cloud.example", "https://cloud.example/?x=1", "https://u:p@cloud.example", "cloud.example"} {
		if _, err := transfersession.NewServer(nil, transfersessiontest.NewProof(), base); err == nil {
			t.Fatalf("accepted base %q", base)
		}
	}
	if _, err := transfersession.NewServer(nil, nil, "https://cloud.example"); err == nil {
		t.Fatal("accepted a server without a proof adapter")
	}
}
