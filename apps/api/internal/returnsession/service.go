// Package returnsession lets a signed-in owner bring their Cloud secretary
// back to a Sumi Local placement: the same individual continues on the Local
// install that surrendered it, or on a fresh target that never started an
// unrelated secretary.
//
// A session wraps one portable transfer (transfer_id = session_id) with what
// the portable ledger cannot know: which owner asked, the grant the Local
// command uses, the deadlines, and the one destination placement the sealed
// bundle is for. The portable ledger stays the only record of receipts and
// proofs.
//
//	awaiting_destination ─bind+seal─▶ sealed ─activate_proof─▶ completed
//	      │                            │
//	      └──cancel/expiry──▶          └──cancel──▶ cancelling ─retire_proof─▶ aborted
//	                      cancelled / expired
//
// The direction reverses the registration move (internal/transfersession):
// here this placement is the source, so the session seals it and serves the
// bundle instead of staging an upload. Authority stays with the portable
// contract: the source leaves "sealed" only with the destination's
// activate_proof (after its activation committed) or retire_proof (after the
// staged copy or a tombstone was recorded there). The session never grants
// either by itself.
//
// A cancel requested after sealing sets cancelling, which is intent, not
// authority: the destination's activate_proof still completes the transfer
// — a committed activation is never un-done — and its retire_proof aborts
// the seal. Deadline expiry stops new admission (binding and sealing); it
// is never treated as evidence that the destination did not activate.
//
// Bind, seal, reports and cancel are separate transactions. Every gap
// between them is closed by Reconcile, which moves a session forward from
// what actually committed in the export ledger — exactly as
// transfersession's converge-on-commit rule does.
package returnsession

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

var (
	ErrNotFound    = errors.New("return session not found")
	ErrGrant       = errors.New("return grant rejected")
	ErrClosed      = errors.New("return session was cancelled or expired")
	ErrExpired     = errors.New("return session deadline passed")
	ErrConflict    = errors.New("return session conflict")
	ErrOpenSession = errors.New("this secretary already has an open return session")
	ErrDestBound   = errors.New("this return URL already belongs to another Sumi Local placement")
	ErrBadRequest  = errors.New("bad request")
	ErrNoSecretary = errors.New("this account has no secretary to return")
	ErrNotOwned    = errors.New("this secretary is not bound to this account")
	// ErrFilePolicyUndecided is the unresolved product boundary: whether
	// shared files travel with a return is still the user's open
	// decision, so no NEW move is admitted. It never blocks status,
	// cancel or recovery of a session already under way — those resolve
	// the authority that already moved, whatever the file answer is.
	ErrFilePolicyUndecided = errors.New(
		"shared-file handling on return is still an undecided product choice — no new move has begun and none is admitted until it is decided")
)

const (
	StatusAwaitingDestination = "awaiting_destination"
	StatusSealed              = "sealed"
	StatusCancelling          = "cancelling"
	StatusCompleted           = "completed"
	StatusAborted             = "aborted"
	StatusCancelled           = "cancelled"
	StatusExpired             = "expired"

	DefaultAdmitTTL = time.Hour
)

var (
	grantRe  = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

// Owner is the authenticated account asking for the return: the human its
// browser session proved and the secretary bound to that human. Both come
// from the session claims — never from the request body.
type Owner struct {
	HumanID   string
	PersonaID string
}

func (o Owner) valid() bool {
	return uuidv7Re.MatchString(o.HumanID) && uuidv7Re.MatchString(o.PersonaID)
}

// FilePolicy is the product's answer to whether shared files travel with
// a return. The zero value is undecided: admitting a NEW move is refused
// at Create and again at the seal, while sessions already under way still
// resolve — the gate sits at admission, never at recovery.
type FilePolicy string

const (
	// FilePolicyUndecided is the unset state and the only state a
	// production deployment has until the user decides. A public base URL
	// is reachability, not a policy answer — it does not open admission.
	FilePolicyUndecided FilePolicy = ""
	// FilePolicyFixture lets author fixtures exercise the common protocol
	// end-to-end. It is test configuration, not a deployable answer: it
	// exists so the journey is proven without standing in for the user's
	// decision.
	FilePolicyFixture FilePolicy = "fixture"
)

type Config struct {
	// AdmitTTL bounds how long the session admits a destination binding and
	// its seal. It is an admission deadline only: once sealed, the source
	// stays sealed until a proof resolves it — an elapsed deadline is never
	// evidence about what the destination did.
	AdmitTTL time.Duration
	// FilePolicy decides whether a new return may be admitted. Undecided
	// (the zero value) refuses Create and the destination seal; any real
	// production value is added when the user answers the file question.
	FilePolicy FilePolicy
}

type Service struct {
	pool       *pgxpool.Pool
	portable   *portable.Service
	admitTTL   time.Duration
	filePolicy FilePolicy
}

func New(pool *pgxpool.Pool, cfg Config) *Service {
	if cfg.AdmitTTL <= 0 {
		cfg.AdmitTTL = DefaultAdmitTTL
	}
	return &Service{pool: pool, portable: portable.NewService(pool), admitTTL: cfg.AdmitTTL,
		filePolicy: cfg.FilePolicy}
}

// admitNewMove is the file-policy gate. It fires only where a new move
// would begin — opening a session and sealing the source — and is never
// consulted by status, download, proof reports or cancel, so a session
// that already moved authority resolves regardless of the policy knob.
func (s *Service) admitNewMove() error {
	if s.filePolicy == FilePolicyUndecided {
		return ErrFilePolicyUndecided
	}
	return nil
}

// Destination is the one Local placement a session serves, declared by the
// Local command before the source seals. SlotState records what the receiver
// found in its own persona slot: "absent" (a fresh target — the import mints
// the slot) or "surrendered" (the install that sent this secretary away
// still holds its frozen transferred copy, which the import reclaims). It is
// evidence, not authority: the destination's own import admission enforces
// the real precondition — this declaration exists so the obvious mismatch
// refuses before the seal, not after.
type Destination struct {
	PlacementID string `json:"placement_id"`
	PersonaID   string `json:"persona_id"`
	SlotState   string `json:"slot_state"`
}

func (d Destination) valid() bool {
	return uuidv7Re.MatchString(d.PlacementID) && uuidv7Re.MatchString(d.PersonaID) &&
		(d.SlotState == "absent" || d.SlotState == "surrendered")
}

func (d Destination) same(other Destination) bool {
	return d.PlacementID == other.PlacementID && d.PersonaID == other.PersonaID &&
		d.SlotState == other.SlotState
}

// View is a session as its callers see it. The transfer key appears only in
// the grant holder's view: it is what the destination needs to write the
// tombstone for a bundle that never arrived, and the grant holder can read
// the whole bundle anyway. The owner's view omits it — the browser never
// mints a proof.
type View struct {
	SessionID         string       `json:"session_id"`
	TransferID        string       `json:"transfer_id"`
	PersonaID         string       `json:"persona_id"`
	Status            string       `json:"status"`
	AdmitUntil        time.Time    `json:"admit_until"`
	SourcePlacementID string       `json:"source_placement_id"`
	Destination       *Destination `json:"destination,omitempty"`
	// SurrenderedBy is the forward transfer that brought the secretary to
	// this placement, when one did — the lineage a same-install receiver
	// asserts as supersedes when it reclaims its surrendered copy. Empty
	// for a secretary that was created here.
	SurrenderedBy string `json:"surrendered_by,omitempty"`
	TransferKey   string `json:"transfer_key,omitempty"`
	// StateOnly and NotIncluded state the slice explicitly: core state
	// travels; files, jobs, connections and account state do not.
	StateOnly   bool                 `json:"state_only"`
	NotIncluded []portable.Exclusion `json:"not_included"`
	Preflight   *Preflight           `json:"preflight,omitempty"`
	Arrival     *Arrival             `json:"arrival,omitempty"`
}

// Preflight is what the owner and the operator are shown before the move:
// the effects a return actually has. It is descriptive, not a gate — the
// seal itself refuses while unresolved work exists.
type Preflight struct {
	// ActiveJobs counts this placement's non-terminal jobs for the
	// secretary. The seal refuses while any exist, so a return cannot
	// strand in-flight work — it waits for them to finish instead.
	ActiveJobs int `json:"active_jobs"`
	// ModelIntentKind is the carried model-selection kind, when the
	// secretary arrived here by transfer or a selection was snapshotted at
	// the last seal. The destination enforces it as needs_rebinding until
	// its human selects a matching connection or the intent is cleared —
	// on a packaged Local there is no human-bound selection, so the
	// operator clears it explicitly (see docs/cloud-local-return.md).
	ModelIntentKind string `json:"model_intent_kind,omitempty"`
	// PendingApprovals counts tool approvals awaiting a human decision.
	// An approval is an identity-scoped act: a fresh destination imports
	// the persona unbound, and an unbound persona cannot activate while
	// one is pending — the ordinary path is to decide it here before or
	// after the move, or cancel and return again once decided.
	PendingApprovals int `json:"pending_approvals"`
	// Files states the unresolved product boundary plainly: shared files
	// are not part of this transfer and are not deleted; whether they move
	// to Local is a product decision still open. The returned secretary's
	// file references therefore do not become a synced workspace.
	Files string `json:"files"`
}

const filesNotCarried = "shared files do not move with this transfer and are not deleted; whether Cloud files come to Local is an open product decision"

// Arrival summarizes what the destination will continue, from the sealed
// export receipt.
type Arrival struct {
	ContentSHA256 string              `json:"content_sha256"`
	Continuity    portable.Continuity `json:"continuity"`
	Rows          map[string]int64    `json:"rows"`
}

type row struct {
	id         string
	humanID    string
	personaID  string
	grantHash  []byte
	status     string
	dstPlace   *string
	dstPersona *string
	dstSlot    *string
	priorHold  *string
	admitUntil time.Time
}

const rowCols = `session_id, human_id, persona_id, grant_hash, status,
	destination_placement_id, destination_persona_id, destination_slot_state,
	prior_transfer_id, admit_until`

func scanRow(r pgx.Row) (row, error) {
	var x row
	err := r.Scan(&x.id, &x.humanID, &x.personaID, &x.grantHash, &x.status,
		&x.dstPlace, &x.dstPersona, &x.dstSlot, &x.priorHold, &x.admitUntil)
	if errors.Is(err, pgx.ErrNoRows) {
		return x, ErrNotFound
	}
	return x, err
}

func (s *Service) load(ctx context.Context, sessionID string) (row, error) {
	if !uuidv7Re.MatchString(sessionID) {
		return row{}, ErrNotFound
	}
	return scanRow(s.pool.QueryRow(ctx, `SELECT `+rowCols+` FROM return_sessions WHERE session_id = $1`, sessionID))
}

func hashGrant(grant string) []byte {
	h := sha256.Sum256([]byte(grant))
	return h[:]
}

// authorize returns the session only to the holder of its grant. An unknown
// session and a wrong grant are indistinguishable.
func (s *Service) authorize(ctx context.Context, sessionID, grant string) (row, error) {
	if !grantRe.MatchString(grant) {
		return row{}, ErrGrant
	}
	r, err := s.load(ctx, sessionID)
	if errors.Is(err, ErrNotFound) {
		return row{}, ErrGrant
	}
	if err != nil {
		return row{}, err
	}
	if subtle.ConstantTimeCompare(r.grantHash, hashGrant(grant)) != 1 {
		return row{}, ErrGrant
	}
	return r, nil
}

func (s *Service) ownSession(ctx context.Context, sessionID string, owner Owner) (row, error) {
	r, err := s.load(ctx, sessionID)
	if err != nil {
		return row{}, err
	}
	if r.humanID != owner.HumanID {
		return row{}, ErrNotFound
	}
	return r, nil
}

func (r row) destination() (Destination, bool) {
	if r.dstPlace == nil {
		return Destination{}, false
	}
	return Destination{PlacementID: *r.dstPlace, PersonaID: *r.dstPersona, SlotState: *r.dstSlot}, true
}

// Created is returned once, to the owner. Grant is not stored and cannot be
// read again.
type Created struct {
	Grant string
	View  View
}

// Create opens a return session for the secretary bound to the owner's
// human. It refuses a persona this account does not own, a persona that is
// not active here (already moving or already gone), and a persona with an
// open return session — whose id is reported so it can be continued or
// cancelled.
//
// Create is not idempotent and never cancels anything: the grant exists only
// in its answer. When that answer is lost, the retry is refused with the
// open session, and the owner decides from its status — a session still
// awaiting_destination can be cancelled and recreated; a sealed one needs
// no grant to read its status (the Local command holds it).
func (s *Service) Create(ctx context.Context, owner Owner) (Created, string, error) {
	if !owner.valid() {
		return Created{}, "", fmt.Errorf("%w: unsupported owner claims", ErrBadRequest)
	}
	if err := s.admitNewMove(); err != nil {
		return Created{}, "", err
	}
	// The persona must be this account's secretary: the session will seal
	// it, so a claim pointing at someone else's persona refuses here —
	// before any authority moves.
	var boundHuman *string
	var authority string
	err := s.pool.QueryRow(ctx, `SELECT human_id::text, authority FROM core_personas WHERE persona_id = $1`,
		owner.PersonaID).Scan(&boundHuman, &authority)
	if errors.Is(err, pgx.ErrNoRows) {
		return Created{}, "", ErrNoSecretary
	}
	if err != nil {
		return Created{}, "", err
	}
	if boundHuman == nil || *boundHuman != owner.HumanID {
		return Created{}, "", ErrNotOwned
	}
	if authority != "active" {
		return Created{}, "", fmt.Errorf("%w: the secretary's authority is %s, not active — it is already moving or already elsewhere",
			ErrConflict, authority)
	}
	for attempt := 0; ; attempt++ {
		id, err := uuid.NewV7()
		if err != nil {
			return Created{}, "", err
		}
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return Created{}, "", err
		}
		grant := base64.RawURLEncoding.EncodeToString(raw)
		_, err = s.pool.Exec(ctx, `INSERT INTO return_sessions
			(session_id, human_id, persona_id, grant_hash, status, admit_until)
			VALUES ($1, $2, $3, $4, 'awaiting_destination', now() + $5::bigint * interval '1 millisecond')`,
			id.String(), owner.HumanID, owner.PersonaID, hashGrant(grant), s.admitTTL.Milliseconds())
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			var open string
			qerr := s.pool.QueryRow(ctx, `SELECT session_id FROM return_sessions
				WHERE persona_id = $1 AND status IN ('awaiting_destination','sealed','cancelling')`,
				owner.PersonaID).Scan(&open)
			if errors.Is(qerr, pgx.ErrNoRows) && attempt == 0 {
				continue
			}
			if qerr != nil {
				return Created{}, "", err
			}
			return Created{}, open, ErrOpenSession
		}
		if err != nil {
			return Created{}, "", err
		}
		v, err := s.view(ctx, id.String(), false)
		return Created{Grant: grant, View: v}, "", err
	}
}

// BindDestination records the one Local placement this session serves and
// seals the source for it. It is the whole answer to a return URL pasted
// into two Local installs: the second placement learns that the URL is
// taken while this secretary is still active here, instead of sealing a
// secretary whose bundle would reach a receiver that was never checked.
//
// The binding is durable session state and idempotent for the placement
// that holds it — a lost answer, a retry or a restart all reach the same
// row. A closed or already-bound session binds nothing new.
//
// The declared slot state is checked for the obvious mismatch before the
// seal: a surrendered slot must hold THIS secretary's surrendered copy
// (the same persona id), and a slot declared anything but absent or
// surrendered is refused outright — the destination admitted it already
// runs a secretary. Everything deeper is the destination import's own
// admission, enforced where the rows actually are.
func (s *Service) BindDestination(ctx context.Context, sessionID, grant string, dest Destination) (View, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return View{}, err
	}
	if !dest.valid() {
		return View{}, fmt.Errorf("%w: placement_id and persona_id must be uuidv7 and slot_state absent or surrendered", ErrBadRequest)
	}
	if dest.SlotState == "surrendered" && dest.PersonaID != r.personaID {
		return View{}, fmt.Errorf("%w: a surrendered destination slot must hold this secretary's copy (%s), it declared %s — this return would not reach the secretary it is for",
			ErrDestBound, r.personaID, dest.PersonaID)
	}
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	if err := s.bindAndSeal(ctx, sessionID, r.personaID, dest); err != nil {
		v, verr := s.view(ctx, sessionID, true)
		if verr != nil {
			return View{}, verr
		}
		return v, err
	}
	return s.reconciledView(ctx, sessionID, true)
}

func (s *Service) bindAndSeal(ctx context.Context, sessionID, personaID string, dest Destination) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	r, err := scanRow(tx.QueryRow(ctx, `SELECT `+rowCols+` FROM return_sessions WHERE session_id = $1 FOR UPDATE`, sessionID))
	if err != nil {
		return err
	}
	if bound, ok := r.destination(); ok {
		if !bound.same(dest) {
			return fmt.Errorf("%w: it is held by placement %s for secretary slot %s",
				ErrDestBound, bound.PlacementID, bound.PersonaID)
		}
		switch r.status {
		case StatusCancelled, StatusExpired:
			return fmt.Errorf("%w: the session is %s", ErrClosed, r.status)
		}
		// Already ours — a repeated bind falls through to the seal, which
		// replays cleanly when it already committed and retries when the
		// first attempt was lost between the binding write and the seal.
		// The file-policy gate does not fire here: the binding is already
		// durable state, so this continuation is recovery, not a new move.
	} else {
		if err := s.admitNewMove(); err != nil {
			return err
		}
		switch r.status {
		case StatusAwaitingDestination:
		case StatusCancelled, StatusExpired:
			return fmt.Errorf("%w: the session is %s", ErrClosed, r.status)
		default:
			return fmt.Errorf("%w: the session is already %s", ErrConflict, r.status)
		}
		var live bool
		if err := tx.QueryRow(ctx, `SELECT admit_until > now() FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&live); err != nil {
			return err
		}
		if !live {
			return fmt.Errorf("%w: the admission deadline passed", ErrExpired)
		}
		// The persona's current hold is the lineage evidence the receiver's
		// reclaim will assert — capture it before this session's seal moves
		// the hold to the new export.
		var prior *string
		if err := tx.QueryRow(ctx, `SELECT transfer_id FROM core_personas WHERE persona_id = $1`, personaID).Scan(&prior); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE return_sessions
			SET destination_placement_id = $2, destination_persona_id = $3, destination_slot_state = $4,
			    destination_bound_at = now(), prior_transfer_id = $5, updated_at = now()
			WHERE session_id = $1`, sessionID, dest.PlacementID, dest.PersonaID, dest.SlotState, prior); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// The seal runs under the session row lock. That lock is the fence
	// that makes "no export exists yet" decidable: a cancel, a reconcile
	// or a re-bind that needs to know whether the seal can still happen
	// is serialized with it. A cancel arriving now waits, then sees the
	// committed seal; a cancel that landed earlier already wrote
	// cancelling and this pass refuses to seal instead.
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM return_sessions WHERE session_id = $1 FOR UPDATE`,
		sessionID).Scan(&status); err != nil {
		return err
	}
	switch status {
	case StatusAwaitingDestination, StatusSealed:
		// awaiting: the seal is still owed; sealed: a re-bind replays the
		// recorded receipt below.
	case StatusCancelling:
		// The cancel won the window between the binding commit and this
		// lock. Do not seal — the session resolves through the cancel
		// path, and refusing here is what keeps a never-sealed session
		// provably unable to acquire authority later.
		return fmt.Errorf("%w: the session is cancelling", ErrConflict)
	default:
		return fmt.Errorf("%w: the session is %s", ErrClosed, status)
	}
	// The seal joins this transaction (SealTx): the session row lock is
	// held on this connection, so the seal must run here too — acquiring
	// a second pooled connection while holding a lock the waiters need
	// starves the pool against itself (portable.Activate documents the
	// same contract for its advisory lock). It also makes the export
	// ledger and the status update one commit: a crash between the
	// binding commit and this point still leaves the resumable
	// bound-and-awaiting state Reconcile knows.
	if _, err := s.portable.SealTx(ctx, tx, personaID, sessionID, dest.PlacementID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE return_sessions SET status = 'sealed', updated_at = now()
		WHERE session_id = $1 AND status = 'awaiting_destination'`, sessionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Status is the grant holder's view, after reconciling. It stays readable
// after every deadline and terminal status: a sealed source must always be
// able to serve the proof the destination earned, and the destination must
// always be able to learn which proof the source needs.
func (s *Service) Status(ctx context.Context, sessionID, grant string) (View, error) {
	if _, err := s.authorize(ctx, sessionID, grant); err != nil {
		return View{}, err
	}
	return s.reconciledView(ctx, sessionID, true)
}

// Download streams the sealed bundle to the grant holder. Only a sealed
// session serves it: after cancelling there is nothing to import (the
// tombstone needs only the transfer key, which the view carries), and a
// completed or aborted transfer has no more use for the cut.
func (s *Service) Download(ctx context.Context, sessionID, grant string, w io.Writer) (View, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return View{}, err
	}
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	v, err := s.view(ctx, sessionID, true)
	if err != nil {
		return View{}, err
	}
	if v.Status != StatusSealed {
		return v, fmt.Errorf("%w: the session is %s; the bundle is only served while sealed", ErrConflict, v.Status)
	}
	_, err = s.portable.Export(ctx, r.personaID, sessionID, w)
	return v, err
}

// ReportActivated is the destination's report that its activation
// committed, carrying the activate_proof minted there. The source's
// Complete verifies the proof against the transfer key — a wrong or
// invented proof is refused — and the session records completed only after
// the ledger did. The report is accepted while sealed AND while
// cancelling: a committed activation is authority the cancel cannot undo,
// so it wins the race.
func (s *Service) ReportActivated(ctx context.Context, sessionID, grant, activateProof string) (View, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return View{}, err
	}
	if activateProof == "" {
		return View{}, fmt.Errorf("%w: activate_proof is required", ErrBadRequest)
	}
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	v, err := s.view(ctx, sessionID, true)
	if err != nil {
		return View{}, err
	}
	switch v.Status {
	case StatusSealed, StatusCancelling:
	case StatusCompleted:
		return v, nil
	default:
		return v, fmt.Errorf("%w: the session is %s", ErrClosed, v.Status)
	}
	if _, err := s.portable.Complete(ctx, r.personaID, sessionID, activateProof); err != nil {
		return v, err
	}
	if _, err := s.pool.Exec(ctx, `UPDATE return_sessions SET status = 'completed', updated_at = now()
		WHERE session_id = $1 AND status IN ('sealed','cancelling')`, sessionID); err != nil {
		return View{}, err
	}
	return s.reconciledView(ctx, sessionID, true)
}

// ReportRetired is the destination's report that it will never run this
// transfer — the staged copy was deleted or the tombstone written —
// carrying the retire_proof. The source's Abort verifies it and restores
// this placement's authority. It is accepted while sealed or cancelling:
// the grant holder's own cancel takes the same path as the owner's.
func (s *Service) ReportRetired(ctx context.Context, sessionID, grant, retireProof string) (View, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return View{}, err
	}
	if retireProof == "" {
		return View{}, fmt.Errorf("%w: retire_proof is required", ErrBadRequest)
	}
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	v, err := s.view(ctx, sessionID, true)
	if err != nil {
		return View{}, err
	}
	switch v.Status {
	case StatusSealed, StatusCancelling:
	case StatusAborted:
		return v, nil
	default:
		return v, fmt.Errorf("%w: the session is %s", ErrClosed, v.Status)
	}
	if _, err := s.portable.Abort(ctx, r.personaID, sessionID, retireProof); err != nil {
		return v, err
	}
	if _, err := s.pool.Exec(ctx, `UPDATE return_sessions SET status = 'aborted', updated_at = now()
		WHERE session_id = $1 AND status IN ('sealed','cancelling')`, sessionID); err != nil {
		return View{}, err
	}
	return s.reconciledView(ctx, sessionID, true)
}

// CancelByGrant is the Local command's cancel. Before the seal it closes
// admission and nothing moved; after the seal it marks intent — the
// destination then retires the transfer (staged copy or tombstone) and
// reports the proof, and the source unseals only on that proof.
func (s *Service) CancelByGrant(ctx context.Context, sessionID, grant string) (View, error) {
	if _, err := s.authorize(ctx, sessionID, grant); err != nil {
		return View{}, err
	}
	err := s.cancel(ctx, sessionID)
	v, verr := s.reconciledView(ctx, sessionID, true)
	if verr != nil {
		return View{}, verr
	}
	return v, err
}

// CancelByOwner is the signed-in owner's cancel, under the same rules: it
// can stop a session whose seal has not happened or request the stop of a
// sealed one — it can never un-seal by itself, because only the
// destination's retire proof is evidence that no activation happened.
func (s *Service) CancelByOwner(ctx context.Context, sessionID string, owner Owner) (View, error) {
	if _, err := s.ownSession(ctx, sessionID, owner); err != nil {
		return View{}, err
	}
	err := s.cancel(ctx, sessionID)
	v, verr := s.reconciledView(ctx, sessionID, false)
	if verr != nil {
		return View{}, verr
	}
	return v, err
}

// cancel ends admission under the session row lock. Awaiting with no
// destination bound → cancelled: nothing moved. Awaiting with a committed
// binding → cancelling: the binding means the seal may already be in
// flight, so the cancel is advisory — either the in-flight seal commits
// and the destination's retire proof resolves it, or no export exists
// and reconcile closes it cancelled outright (the seal gate makes "no
// export" exclusion-proof, so nothing ever moved). Sealed → cancelling:
// the source stays sealed until the destination's proof lands — expiry
// and caller assertion are never that proof. Terminal states answer
// themselves: completed and aborted are ErrConflict (the outcome already
// landed), cancelled and expired replay their own status.
func (s *Service) cancel(ctx context.Context, sessionID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	var bound bool
	if err := tx.QueryRow(ctx, `SELECT status, destination_bound_at IS NOT NULL
		FROM return_sessions WHERE session_id = $1 FOR UPDATE`,
		sessionID).Scan(&status, &bound); err != nil {
		return err
	}
	switch status {
	case StatusAwaitingDestination:
		want := StatusCancelled
		if bound {
			// Admission already committed: the seal can be in flight
			// right now. A terminal write here would strand a sealed
			// persona under a dead session, so this is a request — the
			// destination's proof decides the outcome.
			want = StatusCancelling
		}
		if _, err := tx.Exec(ctx, `UPDATE return_sessions SET status = $2, updated_at = now()
			WHERE session_id = $1`, sessionID, want); err != nil {
			return err
		}
	case StatusSealed:
		if _, err := tx.Exec(ctx, `UPDATE return_sessions SET status = 'cancelling', updated_at = now()
			WHERE session_id = $1`, sessionID); err != nil {
			return err
		}
	case StatusCancelling, StatusCancelled, StatusExpired:
		// Already decided — replay the recorded status.
	case StatusCompleted:
		return fmt.Errorf("%w: the return already completed; the secretary is active on the destination", ErrConflict)
	case StatusAborted:
		return fmt.Errorf("%w: the return was already aborted; the secretary is active here", ErrConflict)
	}
	return tx.Commit(ctx)
}

// ForOwner is the signed-in owner's view of their current return session:
// the open one, else the most recent. A different owner sees nothing.
func (s *Service) ForOwner(ctx context.Context, owner Owner) (View, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT session_id FROM return_sessions
		WHERE human_id = $1
		ORDER BY (status IN ('awaiting_destination','sealed','cancelling')) DESC, created_at DESC LIMIT 1`,
		owner.HumanID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, ErrNotFound
	}
	if err != nil {
		return View{}, err
	}
	return s.reconciledView(ctx, id, false)
}

// Reconcile moves one session forward from what actually committed in the
// export ledger. Each step is a conditional single-statement update, so
// concurrent reconciles, a sweeper replay or a crash at any point converge
// on the same outcome.
func (s *Service) Reconcile(ctx context.Context, sessionID string) error {
	r, err := s.load(ctx, sessionID)
	if err != nil {
		return err
	}
	rec, lerr := s.portable.Status(ctx, "export", sessionID)
	if lerr != nil && !errors.Is(lerr, portable.ErrTransferNotFound) {
		return lerr
	}
	sealed := lerr == nil

	switch r.status {
	case StatusAwaitingDestination:
		switch {
		case sealed:
			// The bind's seal committed but the status update did not.
			if _, err := s.pool.Exec(ctx, `UPDATE return_sessions
				SET status = 'sealed', updated_at = now()
				WHERE session_id = $1 AND status = 'awaiting_destination'`, sessionID); err != nil {
				return err
			}
		default:
			// Admission expiry: the seal never happened, so nothing moved
			// — the only transition an elapsed deadline may write. A
			// committed destination binding means admission already
			// happened and the seal may be in flight; the deadline bounds
			// the binding, not the seal, so a bound session never expires.
			if _, err := s.pool.Exec(ctx, `UPDATE return_sessions SET status = 'expired', updated_at = now()
				WHERE session_id = $1 AND status = 'awaiting_destination'
				  AND admit_until <= now() AND destination_bound_at IS NULL`, sessionID); err != nil {
				return err
			}
		}
	case StatusSealed, StatusCancelling:
		// The ledger is the authority outcome: a Complete or Abort that
		// committed while the session update was lost still lands here.
		if sealed {
			var want string
			switch rec.Status {
			case "completed":
				want = StatusCompleted
			case "aborted":
				want = StatusAborted
			}
			if want != "" {
				if _, err := s.pool.Exec(ctx, `UPDATE return_sessions SET status = $2, updated_at = now()
					WHERE session_id = $1 AND status IN ('sealed','cancelling')`, sessionID, want); err != nil {
					return err
				}
			}
		} else if r.status == StatusCancelling {
			// A cancel landed after the binding committed but before the
			// seal: there is no export to abort and no retire proof could
			// exist (the destination holds nothing). Under the session
			// row lock — the same fence the bind's seal runs behind — "no
			// export" is decidable durably: the seal either committed
			// already or can never start, because the seal gate only runs
			// while the session is awaiting/sealed. Nothing ever moved,
			// so the cancel resolves to cancelled.
			if err := s.resolveNeverSealedCancel(ctx, sessionID); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolveNeverSealedCancel closes a cancelling session that has no export
// — the seal gate's row lock makes "no export" exclusion-proof against
// in-flight and future seals, so no destination proof is needed.
func (s *Service) resolveNeverSealedCancel(ctx context.Context, sessionID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM return_sessions WHERE session_id = $1 FOR UPDATE`,
		sessionID).Scan(&status); err != nil {
		return err
	}
	if status != StatusCancelling {
		return tx.Commit(ctx)
	}
	var hasExport bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(
		SELECT 1 FROM core_transfers WHERE direction = 'export' AND transfer_id = $1)`,
		sessionID).Scan(&hasExport); err != nil {
		return err
	}
	if !hasExport {
		if _, err := tx.Exec(ctx, `UPDATE return_sessions SET status = 'cancelled', updated_at = now()
			WHERE session_id = $1 AND status = 'cancelling'`, sessionID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Sweep reconciles every session with a due step: an elapsed admission
// deadline, a ledger outcome the session row has not caught up with, a
// bind whose seal committed before its status did, or a bound cancel that
// committed 'cancelling' and crashed before its reconcile — the no-export
// selection is only a work list; resolveNeverSealedCancel re-decides under
// the session row lock, so an in-flight seal (which holds that lock) is
// never misjudged by an unlocked export snapshot.
func (s *Service) Sweep(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT session_id FROM return_sessions
		WHERE (status = 'awaiting_destination' AND admit_until <= now()
		  AND destination_bound_at IS NULL)
		UNION
		SELECT s.session_id FROM core_transfers t
		JOIN return_sessions s ON s.session_id = t.transfer_id
		WHERE t.direction = 'export'
		  AND ((t.status = 'sealed' AND s.status = 'awaiting_destination')
		    OR (t.status IN ('completed','aborted') AND s.status IN ('sealed','cancelling')))
		UNION
		SELECT session_id FROM return_sessions
		WHERE status = 'cancelling'
		  AND NOT EXISTS (
		    SELECT 1 FROM core_transfers t
		    WHERE t.direction = 'export' AND t.transfer_id = return_sessions.session_id)
		LIMIT 200`)
	if err != nil {
		return 0, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var firstErr error
	for _, id := range ids {
		if err := s.Reconcile(ctx, id); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("reconcile %s: %w", id, err)
		}
	}
	return len(ids), firstErr
}

// Run sweeps every interval until ctx ends.
func (s *Service) Run(ctx context.Context, every time.Duration, logf func(string, ...any)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if _, err := s.Sweep(ctx); err != nil && logf != nil && ctx.Err() == nil {
			logf("return session sweep: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) reconciledView(ctx context.Context, sessionID string, grantView bool) (View, error) {
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	return s.view(ctx, sessionID, grantView)
}

func (s *Service) view(ctx context.Context, sessionID string, grantView bool) (View, error) {
	r, err := s.load(ctx, sessionID)
	if err != nil {
		return View{}, err
	}
	own, err := s.portable.PlacementID(ctx)
	if err != nil {
		return View{}, err
	}
	v := View{
		SessionID: r.id, TransferID: r.id, PersonaID: r.personaID, Status: r.status,
		AdmitUntil: r.admitUntil.UTC(), SourcePlacementID: own,
		StateOnly: true, NotIncluded: portable.NotIncluded,
	}
	if dest, ok := r.destination(); ok && grantView {
		v.Destination = &dest
	}
	if r.priorHold != nil {
		v.SurrenderedBy = *r.priorHold
	} else {
		// Not yet bound: the persona's own hold is the lineage evidence —
		// the transfer that brought it here, when it arrived by transfer.
		var hold *string
		if err := s.pool.QueryRow(ctx,
			`SELECT transfer_id FROM core_personas WHERE persona_id = $1`, r.personaID).
			Scan(&hold); err == nil && hold != nil {
			v.SurrenderedBy = *hold
		}
	}
	// Preflight: what the move actually does. Cheap live counts plus the
	// carried intent; the files boundary is stated plainly because the
	// product decision is still open — the transfer neither carries nor
	// deletes them.
	var intentKind string
	var intent json.RawMessage
	var activeJobs, pendingApprovals int64
	if err := s.pool.QueryRow(ctx, `SELECT model_intent,
		(SELECT count(*) FROM core_jobs
		  WHERE persona_id = $1 AND status IN ('queued','running','cancel_requested')),
		(SELECT count(*) FROM core_tool_approvals
		  WHERE persona_id = $1 AND status = 'pending')
		FROM core_personas WHERE persona_id = $1`, r.personaID).
		Scan(&intent, &activeJobs, &pendingApprovals); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return View{}, err
	}
	if len(intent) > 0 {
		var k struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(intent, &k) == nil {
			intentKind = k.Kind
		}
	}
	v.Preflight = &Preflight{ActiveJobs: int(activeJobs), ModelIntentKind: intentKind,
		PendingApprovals: int(pendingApprovals), Files: filesNotCarried}
	rec, err := s.portable.Status(ctx, "export", sessionID)
	if errors.Is(err, portable.ErrTransferNotFound) {
		return v, nil
	}
	if err != nil {
		return View{}, err
	}
	if rec.ContentSHA256 != "" {
		v.Arrival = &Arrival{ContentSHA256: rec.ContentSHA256, Continuity: rec.Continuity, Rows: rec.Rows}
	}
	if grantView && (r.status == StatusSealed || r.status == StatusCancelling) {
		// The transfer key is what the destination needs to tombstone a
		// bundle that never arrived. It is read from the ledger's key
		// column — the grant holder can already download the whole bundle,
		// so this grants nothing new — and it never appears in a receipt.
		if err := s.pool.QueryRow(ctx, `SELECT proof_key FROM core_transfers
			WHERE direction = 'export' AND transfer_id = $1`, sessionID).Scan(&v.TransferKey); err != nil {
			return View{}, err
		}
	}
	return v, nil
}
