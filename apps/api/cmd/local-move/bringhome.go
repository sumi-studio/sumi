// bringhome.go drives a return session: it pulls a sealed secretary bundle
// back from the source placement, imports it into this Local install, and
// reports the proofs the source needs to finish or unseal.
//
// The flow is the mirror of drive(): every durable step commits before the
// next request that depends on it, every answer is rebuilt from the local
// portable ledger and the source's session view, and a lost answer or a
// crash at any point is resumed by re-entering the loop — the loop reads
// what actually committed instead of assuming where it left off.
//
// Slot safety is checked against this install's real database before any
// binding is declared: the configured secretary slot may be empty (a fresh
// target), hold this same secretary's surrendered copy (the install that
// moved it away — a reclaim), or anything else (refused, because importing
// over it would overwrite a different secretary). No request reaches the
// source for a slot that fails those checks, and the config is only ever
// retargeted to the persona whose activation committed.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// parseReturnURL takes the URL the owner was given:
//
//	https://<api>/api/secretary-return/sessions/<session_id>#grant=<grant>
//
// and keeps the two halves apart: the session resource goes to the API,
// the fragment grant stays in the state file. Anything else refuses —
// including a move URL for the other direction, which this command does
// not serve.
func parseReturnURL(raw string) (sessionURL, sessionID, grant string, err error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", "", "", err
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && transfersession.IsLoopbackHost(u.Hostname()):
	default:
		// The grant is a live bearer credential — plain http is only
		// tolerable on loopback, the same rule the move URL applies.
		return "", "", "", fmt.Errorf("the return URL must use https (plain http is only allowed for loopback hosts)")
	}
	if u.Host == "" || u.User != nil {
		return "", "", "", fmt.Errorf("the return URL must name a host and must not embed a user or password")
	}
	if u.RawQuery != "" {
		return "", "", "", fmt.Errorf("the return URL must not carry a query string")
	}
	re := regexp.MustCompile(`^` + returnsession.RoutePrefix + `/sessions/([0-9a-f-]{36})$`)
	m := re.FindStringSubmatch(u.Path)
	if m == nil {
		return "", "", "", fmt.Errorf("this does not look like a return URL (expecting %s/sessions/<id>) — a move-to-Cloud URL is handled by `sumi-local-move start`", returnsession.RoutePrefix)
	}
	q, err := url.ParseQuery(u.Fragment)
	if err != nil || len(q) != 1 || len(q["grant"]) != 1 || !grantRe.MatchString(q.Get("grant")) {
		return "", "", "", fmt.Errorf("the return URL must end with #grant=<grant> — check that the whole URL was copied")
	}
	u.Fragment = ""
	u.RawFragment = ""
	return u.String(), m[1], q.Get("grant"), nil
}

// returnState is the durable record of one return on this install, under
// <home>/return/state.json (0600). It is derived memory only: authority
// lives in the local portable ledger and on the source; every field here is
// re-verified against those before it is acted on.
type returnState struct {
	Version    int    `json:"version"`
	SessionURL string `json:"session_url"`
	SessionID  string `json:"session_id"`
	Grant      string `json:"grant"`
	// SlotPersona is this install's configured secretary slot
	// (SUMI_PERSONA_ID) — the slot the checks guarded.
	SlotPersona string `json:"slot_persona_id"`
	// Persona is the secretary the return carries, learned from the source.
	Persona string `json:"persona_id,omitempty"`
	// RetargetTo is the persona id the config must adopt — set when the
	// slot was empty and the import minted a different secretary id.
	RetargetTo   string `json:"retarget_to,omitempty"`
	RetargetDone bool   `json:"retarget_done,omitempty"`
	// Bound/Downloaded mirror progress; the ledger is rechecked regardless,
	// they only shorten a clean resume.
	Bound      bool `json:"bound,omitempty"`
	Downloaded bool `json:"downloaded,omitempty"`
	// FileMode is the session's selected file handling, learned from the
	// sealed session view and never allowed to change mid-run.
	FileMode string `json:"file_mode,omitempty"`
	// FilesDone/CredDone mark the file phase's durable completion for
	// local (copy verified and promoted) and cloud (storage credential
	// minted and configured) modes.
	FilesDone bool `json:"files_done,omitempty"`
	CredDone  bool `json:"cred_done,omitempty"`
	// SvcRestart records an owed service restart: the file retarget
	// stopped the running service while the persona was still staged
	// (a restarted core cannot take the writer lease until activation
	// commits). The value is the marker the restarted service must
	// record; convergeFileService clears it once the restart is proven.
	SvcRestart string `json:"svc_restart,omitempty"`
	// FilesCopied/FilesBytes/FilesQuarantined summarize the carried
	// workspace for the operator and the evidence file.
	FilesCopied      int64 `json:"files_copied,omitempty"`
	FilesBytes       int64 `json:"files_bytes,omitempty"`
	FilesQuarantined int64 `json:"files_quarantined,omitempty"`
	// Outcome records the terminal step the driver reached so status after
	// a finished run still explains what happened.
	Outcome   string    `json:"outcome,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// returner carries the return command's dependencies: the same transport
// and local stores the mover uses, plus the return record's home and the
// install's config file when the operator pointed at one.
type returner struct {
	m          *mover
	pool       *pgxpool.Pool
	rdir       string
	config     string // SUMI_LOCAL_CONFIG
	useConfMdl bool   // --use-config-model
}

func (m *mover) newReturner(pool *pgxpool.Pool, config string, useConfMdl bool) *returner {
	return &returner{
		m: m, pool: pool, config: config, useConfMdl: useConfMdl,
		rdir: filepath.Join(filepath.Dir(m.dir), "return"),
	}
}

func (r *returner) statePath() string  { return filepath.Join(r.rdir, "state.json") }
func (r *returner) bundlePath() string { return filepath.Join(r.rdir, "bundle.ndjson") }

func (r *returner) load() (*returnState, error) {
	raw, err := os.ReadFile(r.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st returnState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("%s is unreadable (%v); it is kept as-is", r.statePath(), err)
	}
	if st.SessionID == "" || st.Grant == "" || st.SlotPersona == "" {
		return nil, fmt.Errorf("%s is incomplete; it is kept as-is", r.statePath())
	}
	return &st, nil
}

// save replaces the return record atomically, like moveState's save.
func (r *returner) save(st *returnState) error {
	st.UpdatedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(r.rdir, ".state-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), r.statePath()); err != nil {
		return err
	}
	d, err := os.Open(r.rdir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// archive moves a settled return record aside — state-<session>.json, the
// same convention the forward move uses — so a later return starts clean
// while the old evidence (grant included, still 0600) stays inspectable.
func (r *returner) archive(st *returnState) error {
	return os.Rename(r.statePath(), filepath.Join(r.rdir, "state-"+st.SessionID+".json"))
}

// rlock takes the same single-driver lock the move commands take: a move
// out and a return home never run at once on one install.
func (m *mover) rlock() (func(), error) { return m.lock() }

// rcall is call() for the return direction: same wire, the return-session
// view decoded instead. The server's error text comes back separately —
// a refusal like the undecided file policy carries its reason in the body
// alongside the session, and the operator should see the actual words.
func (r *returner) rcall(ctx context.Context, method, target, grant string, body io.Reader, contentType string) (returnsession.View, int, string, error) {
	var v returnsession.View
	raw, code, err := r.m.callSession(ctx, method, target, grant, body, contentType)
	if err != nil {
		return v, code, "", err
	}
	if code < 300 {
		if err := json.Unmarshal(raw, &v); err != nil {
			return v, code, "", fmt.Errorf("Cloud answered an unreadable session: %v", err)
		}
		return v, code, "", nil
	}
	var e struct {
		Error   string              `json:"error"`
		Session *returnsession.View `json:"session"`
	}
	_ = json.Unmarshal(raw, &e)
	msg := strings.TrimSpace(e.Error)
	if e.Session != nil {
		return *e.Session, code, msg, nil
	}
	return v, code, msg, fmt.Errorf("Cloud answered HTTP %d: %s", code, msg)
}

// ReturnStart is `sumi-local-move return`: it takes the return URL the
// owner was given and drives the loop until it finishes or runs out of
// work it can do — pending exits 3 so the operator can come back.
func (m *mover) ReturnStart(ctx context.Context, rawURL string, pool *pgxpool.Pool, config string, useConfMdl bool) int {
	sessionURL, sessionID, grant, err := parseReturnURL(rawURL)
	if err != nil {
		m.say("sumi-local-move: %v", err)
		return exitUsage
	}
	release, err := m.rlock()
	if err != nil {
		return m.fail(err)
	}
	defer release()
	r := m.newReturner(pool, config, useConfMdl)
	if err := os.MkdirAll(r.rdir, 0o700); err != nil {
		return m.fail(err)
	}
	// MkdirAll does not fix the mode of an existing directory; the grant
	// lives here, so an inherited loose mode is tightened, contents kept.
	if err := os.Chmod(r.rdir, 0o700); err != nil {
		return m.fail(err)
	}
	st, err := r.load()
	if err != nil {
		return m.fail(err)
	}
	if st != nil && st.SessionURL != sessionURL {
		if st.Outcome == "" {
			m.say("A different return is already recorded here: session %s.", st.SessionID)
			m.say("Run `sumi-local-move return-status` to see where it stands, or")
			m.say("`sumi-local-move return-cancel` to give it up.")
			return exitError
		}
		// The recorded return already settled — archive it the way the
		// forward move does and let this new one proceed.
		if err := r.archive(st); err != nil {
			return m.fail(err)
		}
		st = nil
	}
	if st == nil {
		st = &returnState{
			Version: 1, SessionURL: sessionURL, SessionID: sessionID,
			Grant: grant, SlotPersona: m.personaID,
		}
		// The record exists before the first request: a crash before any
		// progress still resumes instead of starting over.
		if err := r.save(st); err != nil {
			return m.fail(err)
		}
	} else if st.Grant != grant {
		m.say("sumi-local-move: the grant in this return URL does not match the recorded return")
		return exitError
	}
	return r.drive(ctx, st)
}

// ReturnResume is `sumi-local-move return-resume`: continue the recorded
// return — after a pending exit, a crash, or to finish a step that needed
// the operator (the config retarget or the model choice).
func (m *mover) ReturnResume(ctx context.Context, pool *pgxpool.Pool, config string, useConfMdl bool) int {
	release, err := m.rlock()
	if err != nil {
		return m.fail(err)
	}
	defer release()
	r := m.newReturner(pool, config, useConfMdl)
	st, err := r.load()
	if err != nil {
		return m.fail(err)
	}
	if st == nil {
		m.say("No return is recorded on this install.")
		return exitDone
	}
	if st.Outcome != "" {
		m.say("The recorded return already finished (%s).", st.Outcome)
		return exitDone
	}
	return r.drive(ctx, st)
}

// drive is the return loop: local ledger and session view are re-read every
// pass, so a resume re-enters at whatever actually committed. It maps
// "still work to do" to exitPending and a settled outcome to exitDone.
func (r *returner) drive(ctx context.Context, st *returnState) int {
	for i := 0; i < 12; i++ {
		code, again, err := r.step(ctx, st)
		if err != nil {
			switch {
			case errors.Is(err, errPending):
				// Guidance was already printed; the operator comes back.
				return exitPending
			case errors.Is(err, errUnreachable):
				r.m.say("sumi-local-move: %v", err)
				return exitPending
			case errors.Is(err, errGrantRejected):
				// A rejected grant never becomes valid — this is not a
				// "resume later" condition. The record stays so status can
				// still explain what happened; if the pasted URL was wrong,
				// the operator removes it by hand.
				r.m.say("sumi-local-move: %v — Cloud rejected this return's grant and will not accept it on retry.", err)
				r.m.say("If the wrong URL was pasted, remove %s and re-run `sumi-local-move return` with the right one.", r.statePath())
				return exitError
			}
			return r.m.fail(err)
		}
		if !again {
			return code
		}
	}
	r.m.say("sumi-local-move: the return did not settle; run `sumi-local-move return <the same URL>` to continue")
	return exitPending
}

// step is one pass of the loop. It returns again=true when the pass changed
// something durable and another pass decides what is next.
func (r *returner) step(ctx context.Context, st *returnState) (code int, again bool, err error) {
	// The local ledger first: an activated or retired import is truth the
	// source still needs regardless of what its session row says.
	imp, impErr := r.m.src.Status(ctx, "import", st.SessionID)
	hasImp := impErr == nil
	if impErr != nil && !errors.Is(impErr, portable.ErrTransferNotFound) {
		return 0, false, impErr
	}

	v, httpCode, _, err := r.rcall(ctx, http.MethodGet, st.SessionURL, st.Grant, nil, "")
	if err != nil {
		return 0, false, err
	}
	if httpCode != http.StatusOK {
		return 0, false, fmt.Errorf("Cloud answered HTTP %d", httpCode)
	}
	st.Persona = v.PersonaID

	if hasImp {
		switch imp.Status {
		case "activated":
			// This secretary is live here. The source may still be sealed
			// because the activated report was lost — finish that first.
			switch v.Status {
			case returnsession.StatusSealed, returnsession.StatusCancelling:
				return 0, true, r.reportActivated(ctx, st, imp.ActivateProof)
			case returnsession.StatusCompleted:
				return r.finishActive(ctx, st)
			default:
				return r.finishTerminal(ctx, st, v)
			}
		case "retired":
			switch v.Status {
			case returnsession.StatusSealed, returnsession.StatusCancelling:
				return 0, true, r.reportRetired(ctx, st, imp.RetireProof)
			default:
				return r.finishTerminal(ctx, st, v)
			}
		}
	}

	switch v.Status {
	case returnsession.StatusAwaitingDestination:
		if hasImp && imp.Status == "staged" {
			// We staged but the source's seal never committed — retire
			// what we hold; this transfer cannot recover.
			_, err := r.m.src.Retire(ctx, imp.PersonaID, st.SessionID,
				mustPlacement(ctx, r.m.src), "")
			return 0, true, err
		}
		return 0, true, r.bind(ctx, st, v)

	case returnsession.StatusSealed:
		if hasImp && imp.Status == "staged" {
			// The file phase must durably complete before activation —
			// a local-mode copy that cannot finish never produces a
			// live-but-fileless secretary, and the seal stays
			// cancellable while it runs.
			if err := r.filesPhase(ctx, st, v); err != nil {
				return 0, false, err
			}
			return 0, true, r.activate(ctx, st)
		}
		return 0, true, r.downloadAndImport(ctx, st, v)

	case returnsession.StatusCancelling:
		// Someone asked to stop after the seal: retire whatever we hold
		// and report the proof. Nothing here reactivates.
		if hasImp && imp.Status == "staged" {
			return 0, true, r.retireStaged(ctx, st, imp.PersonaID)
		}
		return 0, true, r.retireTombstone(ctx, st, v)

	case returnsession.StatusCompleted:
		// The source already knows; make sure our side is finished too.
		return r.finishActive(ctx, st)

	default:
		return r.finishTerminal(ctx, st, v)
	}
}

func mustPlacement(ctx context.Context, svc *portable.Service) string {
	id, _ := svc.PlacementID(ctx)
	return id
}

// slotState is what this install's secretary slot looks like, checked
// against the real database: the destination the source records must be
// what this install actually is.
func (r *returner) slotState(ctx context.Context) (returnsession.Destination, error) {
	own, err := r.m.src.PlacementID(ctx)
	if err != nil {
		return returnsession.Destination{}, err
	}
	d := returnsession.Destination{PlacementID: own, PersonaID: r.m.personaID}
	var auth *string
	// An unrelated persona is disqualifying only while it can still act —
	// active, mid-seal, or mid-stage. A transferred shell is inert authored
	// history (this install once owned a secretary that left), not a second
	// secretary, and the return must preserve it rather than refuse on it.
	otherLive := 0
	rows, err := r.pool.Query(ctx, `SELECT persona_id::text, authority FROM core_personas`)
	if err != nil {
		return d, err
	}
	for rows.Next() {
		var id, a string
		if err := rows.Scan(&id, &a); err != nil {
			rows.Close()
			return d, err
		}
		if id == r.m.personaID {
			auth = &a
		} else if a != "transferred" {
			otherLive++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return d, err
	}
	if otherLive > 0 {
		return d, fmt.Errorf(
			"this Local install already has a different secretary — " +
				"a return cannot start a second one on the same install. " +
				"Give the return a fresh Local install, or use the install that sent this secretary away")
	}
	switch {
	case auth == nil:
		d.SlotState = "absent"
	case *auth == "transferred":
		d.SlotState = "surrendered"
	default:
		return d, fmt.Errorf(
			"this Local install's secretary slot is not free for a return: it is %s. "+
				"Only the install that sent this secretary away (its surrendered copy) or a fresh install may receive it",
			*auth)
	}
	return d, nil
}

// surrenderedHold is the forward transfer id the local surrendered copy is
// held by — the lineage the reclaim asserts as supersedes.
func (r *returner) surrenderedHold(ctx context.Context, personaID string) (string, error) {
	var hold *string
	if err := r.pool.QueryRow(ctx, `SELECT transfer_id FROM core_personas
		WHERE persona_id = $1 AND authority = 'transferred'`, personaID).Scan(&hold); err != nil {
		return "", err
	}
	if hold == nil || *hold == "" {
		return "", fmt.Errorf("the surrendered copy has no transfer hold recorded")
	}
	return *hold, nil
}

// bind checks this install's real slot, then declares the destination and
// seals the source. The slot checks happen before any request: a wrong
// install refuses locally, never touching authority anywhere.
func (r *returner) bind(ctx context.Context, st *returnState, v returnsession.View) error {
	d, err := r.slotState(ctx)
	if err != nil {
		return err
	}
	if d.SlotState == "surrendered" {
		// The surrendered copy must be THIS secretary's, held by the
		// transfer the source itself recorded.
		if st.SlotPersona != v.PersonaID {
			return fmt.Errorf(
				"this install's surrendered copy is %s but the return carries %s — refusing: a return can only reclaim the secretary that left this install",
				st.SlotPersona, v.PersonaID)
		}
		hold, err := r.surrenderedHold(ctx, st.SlotPersona)
		if err != nil {
			return err
		}
		if v.SurrenderedBy != "" && hold != v.SurrenderedBy {
			return fmt.Errorf(
				"this install's surrendered copy is held by transfer %s, but Cloud recorded %s — the lineage does not match; refusing rather than overwriting the wrong evidence",
				hold, v.SurrenderedBy)
		}
	}
	// The destination declares the file handling it is actually running:
	// the owner's recorded choice on the session. Cloud refuses a
	// declaration that does not match, so an old or mismatched mover can
	// never bind a file-inclusive return and drop the files.
	d.FileMode = v.FileMode
	_, code, msg, err := r.rcall(ctx, http.MethodPost, st.SessionURL+"/destination",
		st.Grant, jsonBody(d), "application/json")
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("Cloud refused the destination (HTTP %d): %s", code, msg)
	}
	st.Bound = true
	return r.save(st)
}

// downloadAndImport fetches the sealed bundle to durable disk, then imports
// it — a reclaim over this install's surrendered copy when the slot holds
// one, a fresh import otherwise. The import only ever stages; activation is
// the next committed step.
func (r *returner) downloadAndImport(ctx context.Context, st *returnState, v returnsession.View) error {
	if !st.Downloaded {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, st.SessionURL+"/bundle", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+st.Grant)
		res, err := r.m.client.Do(req)
		r.m.answered.Store(true)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("%w: %v", errUnreachable, err)
		}
		defer res.Body.Close()
		// The same silent-peer bound the upload applies: headers arriving
		// proves nothing about the body — a 503 that goes quiet after its
		// status line stalls the drain exactly like a sealed bundle going
		// quiet mid-copy. There is no total-duration cap — a large bundle
		// keeps going as long as bytes move — but m.stall without a
		// single byte ends this attempt. Closing the body is what
		// unblocks the read.
		body := &progress{r: res.Body}
		body.last.Store(time.Now().UnixNano())
		var stalled atomic.Bool
		watch := make(chan struct{})
		defer close(watch)
		go func() {
			tick := time.NewTicker(max(r.m.stall/4, 10*time.Millisecond))
			defer tick.Stop()
			for {
				select {
				case <-watch:
					return
				case <-ctx.Done():
					return
				case <-tick.C:
					if body.done.Load() {
						return
					}
					if time.Since(time.Unix(0, body.last.Load())) >= r.m.stall {
						stalled.Store(true)
						_ = res.Body.Close()
						return
					}
				}
			}
		}()
		if res.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<16))
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden:
				// The grant is permanent state: a refusal is an
				// actionable error, never an endless pending (F370).
				return fmt.Errorf("%w: Cloud answered HTTP %d for the bundle", errGrantRejected, res.StatusCode)
			default:
				// 5xx, transport weirdness, or an inconsistent 4xx while
				// the session still says sealed — transient; resume.
				return fmt.Errorf("%w: HTTP %d for the bundle", errUnreachable, res.StatusCode)
			}
		}
		tmp := r.bundlePath() + ".part"
		f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, body); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case stalled.Load():
				return fmt.Errorf("%w: %v for %s; the bundle download is retried by `sumi-local-move return-resume`",
					errUnreachable, errStalled, r.m.stall)
			default:
				return fmt.Errorf("%w: %v", errUnreachable, err)
			}
		}
		// Sync before the rename and the directory after it: a power loss
		// must never leave a truncated bundle behind a committed
		// Downloaded flag — the import's digest check would catch it, but
		// then only manual cleanup could recover.
		if err := f.Sync(); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := os.Rename(tmp, r.bundlePath()); err != nil {
			return err
		}
		if d, err := os.Open(r.rdir); err == nil {
			_ = d.Sync()
			_ = d.Close()
		}
		st.Downloaded = true
		if err := r.save(st); err != nil {
			return err
		}
	}
	f, err := os.Open(r.bundlePath())
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	// The header is read before streaming: it names the secretary and the
	// destination the cut was sealed for, so a bundle meant for someone
	// else or for a different placement refuses with a clear reason before
	// a row moves.
	line, err := br.ReadString('\n')
	if err != nil {
		return r.discardBundle(st, "the recorded bundle is truncated; fetching it again")
	}
	var header struct {
		TransferID  string `json:"transfer_id"`
		PersonaID   string `json:"persona_id"`
		Destination string `json:"destination_id"`
	}
	if err := json.Unmarshal([]byte(line), &header); err != nil {
		return r.discardBundle(st, "the recorded bundle's header is unreadable; fetching it again")
	}
	// A well-formed header naming a different transfer or placement is not
	// a torn file — it is a different bundle, and a refetch returns the
	// same bytes. Refuse permanently rather than retry or, worse, import it.
	if header.TransferID != st.SessionID {
		return fmt.Errorf("the bundle names transfer %s, not this return (%s) — refusing to import it",
			header.TransferID, st.SessionID)
	}
	own := mustPlacement(ctx, r.m.src)
	if header.Destination != own {
		return fmt.Errorf("the bundle was sealed for a different placement (%s, this install is %s) — refusing to import it",
			header.Destination, own)
	}
	rdr := io.MultiReader(bytes.NewBufferString(line), br)

	d, err := r.slotState(ctx)
	if err != nil {
		return err
	}
	var rec portable.Receipt
	switch d.SlotState {
	case "surrendered":
		hold, err := r.surrenderedHold(ctx, st.SlotPersona)
		if err != nil {
			return err
		}
		rec, _, err = r.m.src.ImportReturning(ctx, rdr, nil, false, hold)
	case "absent":
		rec, _, err = r.m.src.Import(ctx, rdr, nil, false)
	}
	if err != nil {
		if st.Downloaded && errors.Is(err, portable.ErrBadBundle) {
			// The recorded bundle fails its own integrity check — a
			// truncated write that survived a crash. The source still
			// serves it while sealed, so drop the file and re-download.
			return r.discardBundle(st, "the recorded bundle failed its integrity check; fetching it again")
		}
		return err
	}
	if d.SlotState == "absent" {
		st.RetargetTo = rec.PersonaID
	}
	st.Persona = rec.PersonaID
	return r.save(st)
}

// discardBundle drops a committed bundle file that proved unusable — a torn
// first line, an unparseable header, or a failed integrity check — and marks
// the transfer undownloaded so the drive loop re-fetches it from the
// still-sealed source. Returning nil keeps the run going: the drive's step
// bound caps in-run retries, a persistently bad source ends pending, and an
// ordinary resume retries once the source is healthy — never manual file
// surgery. A bundle that is well-formed but names a different transfer or
// placement is NOT this case: refusing it permanently is the caller's job.
func (r *returner) discardBundle(st *returnState, reason string) error {
	r.m.say("sumi-local-move: %s", reason)
	if err := os.Remove(r.bundlePath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	st.Downloaded = false
	return r.save(st)
}

// activate commits the staged copy live and immediately reports the proof.
// If the report is lost, resume finds the activated receipt and re-reports
// it — the proof is durable.
func (r *returner) activate(ctx context.Context, st *returnState) error {
	// A pending approval is an identity-scoped act: it carried over in the
	// sealed state and a fresh destination imports the persona unbound, so
	// local activation will refuse until a human decides it. Local has no
	// human-bound flow to do that, and resuming cannot help — the staged
	// import is a snapshot taken before any decision. The honest path is
	// to retire this staged copy, settle the approval on Cloud, and run a
	// new return; say so now instead of letting the ledger's refusal
	// arrive as a raw error. The approval itself is untouched.
	var bound bool
	var pending int
	if err := r.pool.QueryRow(ctx, `SELECT human_id IS NOT NULL,
		(SELECT count(*) FROM core_tool_approvals WHERE persona_id = $1 AND status = 'pending')
		FROM core_personas WHERE persona_id = $1`, st.Persona).Scan(&bound, &pending); err != nil {
		return err
	}
	if !bound && pending > 0 {
		r.m.say("sumi-local-move: %d tool approval(s) still need a human's decision on this secretary", pending)
		r.m.say("sumi-local-move: a fresh Local has no bound human to answer them, so the secretary cannot start here")
		r.m.say("sumi-local-move: run `sumi-local-move return-cancel`, settle the approval(s) on Cloud — deny them or let the approved action finish — then start a new return")
		return errPending
	}
	rec, err := r.m.src.Activate(ctx, st.Persona, st.SessionID)
	if err != nil {
		return err
	}
	return r.reportActivated(ctx, st, rec.ActivateProof)
}

func (r *returner) reportActivated(ctx context.Context, st *returnState, proof string) error {
	// The persona is active locally now — a service the file retarget
	// stopped while it was staged can finally take the writer lease.
	// The restart is owed before the activated report: a completed
	// session must mean the install is actually serving the chosen
	// store, not merely that the rows moved.
	if err := r.convergeFileService(ctx, st); err != nil {
		return err
	}
	_, code, _, err := r.rcall(ctx, http.MethodPost, st.SessionURL+"/activated",
		st.Grant, jsonBody(struct {
			ActivateProof string `json:"activate_proof"`
		}{ActivateProof: proof}), "application/json")
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("Cloud answered HTTP %d", code)
	}
	return nil
}

// retireStaged retires a copy that imported but never activated. The
// incoming rows are removed; on a reclaim the surrendered shell with this
// install's own history stays — inert, held by the transfer that sent it
// away — and the source unseals only on the retire proof.
func (r *returner) retireStaged(ctx context.Context, st *returnState, personaID string) error {
	// A staged import that is being retired may have already promoted part of
	// the carried tree; restore the workspace before reporting the retire so
	// an owner-initiated cancel observed via resume leaves authored bytes,
	// displaced paths, and carried files consistent — the same guarantee the
	// local `return-cancel` command gives. A journal that exists but cannot
	// be read is not "no journal": the retire must not be reported over an
	// unrestored tree, so the drive stays recoverable until it reads.
	j, jerr := r.loadJournal(st)
	if jerr != nil {
		return jerr
	}
	if j != nil && j.Phase != "restored" {
		if rerr := r.restoreWorkspace(st, j); rerr != nil {
			return rerr
		}
	}
	rec, err := r.m.src.Retire(ctx, personaID, st.SessionID, mustPlacement(ctx, r.m.src), "")
	if err != nil {
		return err
	}
	return r.reportRetired(ctx, st, rec.RetireProof)
}

// retireTombstone is cancel before the bundle ever landed: the destination
// records that this transfer will never run, under its own minted proof.
func (r *returner) retireTombstone(ctx context.Context, st *returnState, v returnsession.View) error {
	if v.TransferKey == "" {
		return fmt.Errorf("the session did not provide a transfer key for the tombstone")
	}
	rec, err := r.m.src.Retire(ctx, st.Persona, st.SessionID, mustPlacement(ctx, r.m.src), v.TransferKey)
	if err != nil {
		return err
	}
	return r.reportRetired(ctx, st, rec.RetireProof)
}

func (r *returner) reportRetired(ctx context.Context, st *returnState, proof string) error {
	_, code, _, err := r.rcall(ctx, http.MethodPost, st.SessionURL+"/retired",
		st.Grant, jsonBody(struct {
			RetireProof string `json:"retire_proof"`
		}{RetireProof: proof}), "application/json")
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("Cloud answered HTTP %d", code)
	}
	return nil
}

// finishActive runs once the source has completed: the secretary is active
// here. What remains is making this install point at it and saying plainly
// where the model selection stands.
func (r *returner) finishActive(ctx context.Context, st *returnState) (int, bool, error) {
	if st.RetargetTo != "" && !st.RetargetDone {
		if err := r.retarget(st); err != nil {
			return exitPending, false, err
		}
	}
	// File configuration is convergent: the seal-window phase already
	// ran it, but a crash between activation and finish re-enters here —
	// CredDone and the marker checks keep each step idempotent.
	switch st.FileMode {
	case "cloud":
		if err := r.filesConfigCloud(ctx, st); err != nil {
			return exitPending, false, err
		}
	case "local":
		if err := r.filesConfigLocal(ctx, st); err != nil {
			return exitPending, false, err
		}
	}
	// A crash between local activation and the service restart leaves
	// the owed restart recorded — converge it here too, since the
	// activated report may already have reached Cloud.
	if err := r.convergeFileService(ctx, st); err != nil {
		return exitPending, false, err
	}
	if err := r.modelStep(ctx, st); err != nil {
		return exitPending, false, err
	}
	st.Outcome = "active"
	if err := r.save(st); err != nil {
		return 0, false, err
	}
	r.m.say("The secretary is active on this Sumi Local install, and Sumi Cloud")
	r.m.say("has stopped answering for it. Its state came with it.")
	switch st.FileMode {
	case "local":
		r.m.say("Its files came too: %d file(s), %d byte(s) now live in this", st.FilesCopied, st.FilesBytes)
		r.m.say("install's workspace. The Cloud copy is retained read-only — it is")
		r.m.say("not synced and not a managed backup.")
		if st.FilesQuarantined > 0 {
			r.m.say("%d existing local file(s) with different content were moved aside", st.FilesQuarantined)
			r.m.say("into the quarantine directory recorded in %s — nothing was overwritten.", filepath.Join(r.rdir, "files.json"))
		}
	case "cloud":
		r.m.say("Its files stayed in Cloud — this install reads and writes the same")
		r.m.say("Cloud workspace through a scoped credential, and the service was")
		r.m.say("already pointed at it before the secretary started.")
	default:
		r.m.say("Shared files did not come with it — they stay in Cloud while that")
		r.m.say("policy is decided.")
	}
	return exitDone, false, nil
}

// retarget points the install's config at the imported secretary. It runs
// only after the activation committed on both sides, only rewrites the one
// line, and refuses to touch a config whose current value is not the slot
// this return was recorded against — a half-rewritten or hand-edited file
// is never guessed at.
func (r *returner) retarget(st *returnState) error {
	if st.RetargetTo == st.SlotPersona {
		st.RetargetDone = true
		return r.save(st)
	}
	if r.config == "" {
		r.m.say("The secretary is active here as %s, but this install's", st.RetargetTo)
		r.m.say("configuration still names slot %s. Set SUMI_LOCAL_CONFIG to the", st.SlotPersona)
		r.m.say("install's config.env and re-run `sumi-local-move return <the same URL>`")
		r.m.say("to finish the retarget, or set SUMI_PERSONA_ID=%s there and", st.RetargetTo)
		r.m.say("restart the service.")
		return errPending
	}
	// Guard first: the file must still name the slot we were checked
	// against — an operator's other edit is not ours to overwrite.
	cur, err := configValue(r.config, "SUMI_PERSONA_ID")
	if err != nil {
		return err
	}
	if cur != st.SlotPersona {
		return fmt.Errorf("%s names SUMI_PERSONA_ID=%s, not the slot this return was checked against (%s) — refusing to rewrite it",
			r.config, cur, st.SlotPersona)
	}
	if err := rewriteConfigKey(r.config, "SUMI_PERSONA_ID", st.RetargetTo); err != nil {
		return err
	}
	st.RetargetDone = true
	if err := r.save(st); err != nil {
		return err
	}
	r.m.say("This install now points at the returned secretary (%s). Restart the", st.RetargetTo)
	r.m.say("service to pick the configuration up.")
	return nil
}

var errPending = errors.New("return pending")

// configValue reads a key's effective value from an env file — last
// assignment wins, quoted values unquoted, matching the wrapper's
// config_val. The installer writes SUMI_PERSONA_ID='…' through shq, so a
// packaged config always carries single quotes.
func configValue(path, key string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	val := ""
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, key+"=") {
			val = strings.TrimSpace(strings.TrimPrefix(line, key+"="))
		}
	}
	if val == "" {
		return "", fmt.Errorf("%s does not set %s", path, key)
	}
	return unquoteEnv(val), nil
}

// unquoteEnv mirrors the data-only quoting the packaged wrapper's
// config_val applies: '…' unwraps and \'-escapes ('\”) collapse to a
// literal quote; "…" unwraps plainly; anything else is the bare value.
func unquoteEnv(v string) string {
	if len(v) >= 2 {
		switch {
		case v[0] == '\'' && v[len(v)-1] == '\'':
			return strings.ReplaceAll(v[1:len(v)-1], `'\''`, `'`)
		case v[0] == '"' && v[len(v)-1] == '"':
			return v[1 : len(v)-1]
		}
	}
	return v
}

// rewriteConfigKey replaces the LAST assignment of key — the effective one
// — in an env file, writing through a 0600 temp + rename so a crash cannot
// leave a truncated config. The replacement keeps the file's own quoting
// style: a single-quoted value stays single-quoted (escaped the way the
// installer's shq does it), a double-quoted one stays double-quoted, and a
// bare value stays bare.
func rewriteConfigKey(path, key, value string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	found := false
	for i := len(lines) - 1; i >= 0; i-- {
		trimmed := strings.TrimSpace(lines[i])
		if strings.HasPrefix(trimmed, key+"=") {
			old := strings.TrimSpace(strings.TrimPrefix(trimmed, key+"="))
			out := value
			switch {
			case strings.HasPrefix(old, "'"):
				out = "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
			case strings.HasPrefix(old, `"`):
				out = `"` + value + `"`
			}
			lines[i] = key + "=" + out
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("%s does not set %s", path, key)
	}
	tmp := path + ".part"
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// modelStep is the explicit model handoff. The bundle carries model intent
// honestly: needs_rebinding stands until either the operator chooses this
// install's configured provider (--use-config-model, an explicit authorized
// clear) or a human-bound connection is selected on this install — which a
// packaged Local does not offer, so the flag is the only path that clears
// it here.
func (r *returner) modelStep(ctx context.Context, st *returnState) error {
	var intent []byte
	err := r.pool.QueryRow(ctx, `SELECT model_intent FROM core_personas WHERE persona_id = $1`,
		st.Persona).Scan(&intent)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if len(intent) == 0 {
		return nil
	}
	if !r.useConfMdl {
		r.m.say("This secretary carries a model selection from Cloud. No model")
		r.m.say("connection is chosen on this install yet — it will report")
		r.m.say("\"needs_rebinding\" and cannot reply until one is. To use the")
		r.m.say("provider/model this install is configured with, run:")
		r.m.say("")
		r.m.say("    sumi-local-move return-resume --use-config-model")
		r.m.say("")
		r.m.say("which clears the carried selection explicitly through the authorized path.")
		return errPending
	}
	if err := agentstate.NewStore(r.pool).ClearModelIntent(ctx, st.Persona); err != nil {
		return err
	}
	r.m.say("The carried model selection was cleared; this install's configured")
	r.m.say("provider/model will be used once the service restarts.")
	return nil
}

// finishTerminal explains a session that ended without an active secretary
// here: aborted (the source unsealed — the secretary is back on Cloud),
// cancelled or expired (the seal never happened).
func (r *returner) finishTerminal(ctx context.Context, st *returnState, v returnsession.View) (int, bool, error) {
	st.Outcome = v.Status
	// A service the file retarget stopped is owed its restart whatever
	// the terminal status — the cancel path converges it back onto the
	// restored configuration.
	r.restoreServiceAfterCancel(ctx, st)
	if err := r.save(st); err != nil {
		return 0, false, err
	}
	switch v.Status {
	case returnsession.StatusAborted:
		r.m.say("The return was cancelled and the secretary is active on Sumi Cloud")
		r.m.say("again. Nothing was made active here.")
	case returnsession.StatusCancelled, returnsession.StatusExpired:
		r.m.say("The return ended before the transfer started; nothing moved.")
	default:
		r.m.say("The return is %s; nothing is active here.", v.Status)
	}
	return exitDone, false, nil
}

// ReturnStatus is `sumi-local-move return-status`: what the local ledger
// and the source session both say, for the operator mid-run.
func (m *mover) ReturnStatus(ctx context.Context, pool *pgxpool.Pool, config string) int {
	r := m.newReturner(pool, config, false)
	st, err := r.load()
	if err != nil {
		return m.fail(err)
	}
	if st == nil {
		m.say("No return is recorded on this install.")
		return exitDone
	}
	m.say("Return session %s", st.SessionID)
	if st.Outcome != "" {
		m.say("Outcome recorded here: %s", st.Outcome)
	}
	imp, impErr := r.m.src.Status(ctx, "import", st.SessionID)
	switch {
	case impErr == nil:
		line := fmt.Sprintf("This install: import %s for secretary %s", imp.Status, imp.PersonaID)
		if imp.Supersedes != "" {
			line += fmt.Sprintf(" (reclaim over transfer %s)", imp.Supersedes)
		}
		m.say("%s", line)
	case errors.Is(impErr, portable.ErrTransferNotFound):
		m.say("This install: nothing imported yet")
	default:
		return m.fail(impErr)
	}
	// The Cloud session is still reported when reachable — the ledger's
	// word on where authority rests — but a finished local run does not
	// need it, and an unfinished one treats silence as pending.
	v, code, _, err := r.rcall(ctx, http.MethodGet, st.SessionURL, st.Grant, nil, "")
	switch {
	case errors.Is(err, errGrantRejected):
		m.say("Cloud session: the recorded grant was rejected (%v)", err)
		if st.Outcome == "" {
			return exitPending
		}
	case err != nil:
		m.say("Cloud session: unreachable (%v)", err)
		if st.Outcome == "" {
			return exitPending
		}
	case code != http.StatusOK:
		m.say("Cloud session: HTTP %d", code)
		if st.Outcome == "" {
			return exitPending
		}
	default:
		m.say("Cloud session: %s", v.Status)
		if v.Preflight != nil {
			m.say("At seal time: %d job(s) in flight, carried model selection %q.",
				v.Preflight.ActiveJobs, v.Preflight.ModelIntentKind)
			m.say("Files: %s", v.Preflight.Files)
		}
	}
	return exitDone
}

// ReturnCancel is `sumi-local-move return-cancel`: give up the recorded
// return. Whatever this install already committed is retired honestly — a
// staged copy or, if nothing ever landed, the tombstone — and the source
// unseals only on the proof. An already-active secretary is refused: it is
// the live copy now, and retiring it would be a deletion.
func (m *mover) ReturnCancel(ctx context.Context, pool *pgxpool.Pool, config string) int {
	r := m.newReturner(pool, config, false)
	release, err := m.rlock()
	if err != nil {
		return m.fail(err)
	}
	defer release()
	st, err := r.load()
	if err != nil {
		return m.fail(err)
	}
	if st == nil {
		m.say("No return is recorded on this install.")
		return exitDone
	}
	if st.Outcome == "active" {
		return m.fail(errors.New("the secretary is active on this install — there is no return left to cancel"))
	}
	imp, impErr := r.m.src.Status(ctx, "import", st.SessionID)
	if impErr == nil && imp.Status == "activated" {
		return m.fail(errors.New("the secretary is already active on this install — it cannot be retired"))
	}
	// The restoration journal is read before Cloud is told anything: a
	// journal that exists but cannot be read is not "no journal", and a
	// cancel that cannot restore the tree must stay recoverable with the
	// session untouched rather than announce a restore it skipped.
	j, jerr := r.loadJournal(st)
	if jerr != nil {
		return m.fail(jerr)
	}
	_, code, _, err := r.rcall(ctx, http.MethodPost, st.SessionURL+"/cancel", st.Grant, nil, "")
	if err != nil {
		if errors.Is(err, errUnreachable) {
			m.say("sumi-local-move: %v — the cancel may or may not have reached Cloud; run `sumi-local-move return-cancel` again, or `return-status` to check", err)
			return exitPending
		}
		return m.fail(err)
	}
	if code != http.StatusOK {
		return m.fail(fmt.Errorf("Cloud answered HTTP %d", code))
	}
	// A cancelled copy leaves no half-workspace: every file the journal
	// placed is removed (unless edited since — then it is the operator's
	// and stays), every displaced object goes back, directories the copy
	// created are removed when empty, and staging scratch is dropped.
	if j != nil && j.Phase != "restored" {
		if rerr := r.restoreWorkspace(st, j); rerr != nil {
			return m.fail(rerr)
		}
	}
	// A cancelled cloud-mode return leaves no stale route: the scoped
	// credential it minted is revoked on Cloud with the session, so the
	// config keys it wrote are removed here and the service converges.
	if st.CredDone && r.config != "" {
		for _, k := range []string{"SUMI_FILESVC_URL", "SUMI_FILESVC_TOKEN"} {
			if err := removeConfigKey(r.config, k); err != nil {
				return m.fail(err)
			}
		}
	}
	r.restoreServiceAfterCancel(ctx, st)
	// The session is now cancelled (pre-seal) or cancelling (post-seal).
	// Drive settles it: retire + report, or record the terminal status.
	st.Outcome = ""
	for i := 0; i < 6; i++ {
		_, again, err := r.step(ctx, st)
		if err != nil {
			switch {
			case errors.Is(err, errPending):
				return exitPending
			case errors.Is(err, errUnreachable):
				// Cloud already accepted the cancel; only its answer was
				// lost. The settle work — retire, report — stays
				// resumable, so this is pending, not a failure.
				m.say("sumi-local-move: %v — the cancel is recorded on Cloud; run `sumi-local-move return-cancel` to finish it", err)
				return exitPending
			case errors.Is(err, errGrantRejected):
				m.say("sumi-local-move: %v", err)
				return exitError
			}
			return m.fail(err)
		}
		if !again {
			return exitDone
		}
	}
	m.say("sumi-local-move: the return did not settle; run `sumi-local-move return-status` to see where it stands")
	return exitPending
}
