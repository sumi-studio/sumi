// Package transfersession lets someone registering for Sumi Cloud bring the
// secretary they already have on a Local placement, before Cloud mints a
// different one.
//
// A session wraps one portable transfer (transfer_id = session_id) with what
// the portable ledger cannot know: which proven credential may claim the
// staged secretary, the grant the Local source uses, and the deadlines. The
// portable ledger stays the only record of persona id, receipts and proofs.
//
//	awaiting_bundle ─upload─▶ staged ─ClaimInTx+ProvisionInTx─▶ provisioned ─Activate─▶ activated
//	      │                     │          (account transaction)
//	      └──cancel/expiry──────┴──▶ cancelled / expired ─▶ staged import retired
//
// Before any of that, one Local placement takes the session with BindSource
// and every later bundle is admitted only for that placement's secretary.
// The binding is what a move URL pasted into a second Local install meets:
// it is refused there while that secretary is still active, rather than
// discovering the conflict with a sealed secretary and a bundle nothing can
// admit.
//
// Authority stays with the portable contract: the Local source leaves
// "sealed" only with the destination's activate_proof (after activation
// committed here) or retire_proof (after the staged copy was deleted or a
// tombstone recorded here). The session never grants either by itself.
//
// Import, session updates, activation and retirement are separate
// transactions. Every gap between them is closed by Reconcile, which moves a
// session forward from what actually committed: a staged import promotes an
// awaiting session, a provisioned session is activated, and a cancelled or
// expired session whose import committed late is retired. Status reads,
// uploads and Sweep all run it, so an interrupted request, a crash or a lost
// response converges without a separate workflow engine.
package transfersession

import (
	"bufio"
	"bytes"
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

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
)

var (
	ErrNotFound      = errors.New("transfer session not found")
	ErrGrant         = errors.New("transfer grant rejected")
	ErrClosed        = errors.New("transfer session was cancelled or expired")
	ErrExpired       = errors.New("transfer session deadline passed")
	ErrConflict      = errors.New("transfer session conflict")
	ErrOpenSession   = errors.New("this credential already has an open transfer session")
	ErrSourceBound   = errors.New("this move URL already belongs to another Sumi Local placement")
	ErrSourceUnbound = errors.New("this move URL has not been taken by a Sumi Local placement yet")
	ErrAccountExists = errors.New("this credential already has an account; bringing a secretary into an existing account is not supported")
	ErrBadRequest    = errors.New("bad request")
	// ErrPending is the account transaction's answer to a subject whose
	// session is still awaiting_bundle: the move URL was never used or the
	// upload has not committed. Registration may finish without a transfer
	// only after the registrant cancels or replaces that session — an
	// abandoned one cannot silently decide which secretary the account gets.
	ErrPending = errors.New("the secretary has not arrived yet; run the move command or cancel the transfer")
)

const (
	StatusAwaitingBundle = "awaiting_bundle"
	StatusStaged         = "staged"
	StatusProvisioned    = "provisioned"
	StatusActivated      = "activated"
	StatusCancelled      = "cancelled"
	StatusExpired        = "expired"

	// ProviderFirebase is the credentials.provider whose external_subject is
	// the Firebase UID an authentication flow proves.
	ProviderFirebase = "firebase"

	DefaultAdmitTTL = time.Hour
	DefaultClaimTTL = 24 * time.Hour
)

var (
	grantRe   = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
	uuidv7Re  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	maxHeader = 1 << 20
)

// Subject is the typed credential identity a trusted authentication flow
// proved: credentials.provider and credentials.external_subject. It is not an
// email address and says nothing about a principal that was later deleted and
// re-created; it only identifies who may continue this session while it is
// open.
type Subject struct {
	Provider string
	Subject  string
}

func (s Subject) valid() bool {
	return s.Provider == ProviderFirebase && s.Subject != "" && len(s.Subject) <= 128
}

type Config struct {
	// AdmitTTL bounds how long the session admits a bundle.
	AdmitTTL time.Duration
	// ClaimTTL bounds how long a staged secretary waits for its account.
	ClaimTTL time.Duration
}

// rowQuerier is the pool or a transaction: the same checks run inside and
// outside one.
type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Service struct {
	pool     *pgxpool.Pool
	portable *portable.Service
	admitTTL time.Duration
	claimTTL time.Duration
}

func New(pool *pgxpool.Pool, cfg Config) *Service {
	if cfg.AdmitTTL <= 0 {
		cfg.AdmitTTL = DefaultAdmitTTL
	}
	if cfg.ClaimTTL <= 0 {
		cfg.ClaimTTL = DefaultClaimTTL
	}
	return &Service{pool: pool, portable: portable.NewService(pool), admitTTL: cfg.AdmitTTL, claimTTL: cfg.ClaimTTL}
}

// View is a session as its callers see it. Proofs appear only in the grant
// holder's view: they are what the Local source needs to end its authority,
// and nothing a registering browser uses.
type View struct {
	SessionID              string     `json:"session_id"`
	TransferID             string     `json:"transfer_id"`
	DestinationPlacementID string     `json:"destination_placement_id"`
	Status                 string     `json:"status"`
	AdmitUntil             time.Time  `json:"admit_until"`
	ClaimUntil             *time.Time `json:"claim_until,omitempty"`
	// Retired reports that this placement recorded it will never run the
	// transfer (staged copy deleted or tombstone written).
	Retired bool     `json:"retired"`
	Arrival *Arrival `json:"arrival,omitempty"`
	// StateOnly and NotIncluded state the slice explicitly: core state
	// travels; files, jobs, connections and account state do not.
	StateOnly     bool                 `json:"state_only"`
	NotIncluded   []portable.Exclusion `json:"not_included"`
	ActivateProof string               `json:"activate_proof,omitempty"`
	RetireProof   string               `json:"retire_proof,omitempty"`
	// Source is the Local placement this session accepts a bundle from,
	// once one has taken it. It appears only in the grant holder's view:
	// it is what the Local command compares itself against, and nothing a
	// registering browser uses.
	Source *Source `json:"source,omitempty"`
}

// Source identifies one Local placement's copy of one secretary: the
// placement id its portable service minted (core_placement) and the persona
// it would seal. Neither is secret — the destination's own placement id
// travels in every bundle header — but the pair is what makes "this move URL
// is already taken by another Local" a decidable statement instead of a
// guess. A persona id alone is not enough: the same secretary keeps its id
// across placements it has moved through.
type Source struct {
	PlacementID string `json:"placement_id"`
	PersonaID   string `json:"persona_id"`
}

func (s Source) valid() bool {
	return uuidv7Re.MatchString(s.PlacementID) && uuidv7Re.MatchString(s.PersonaID)
}

func (s Source) same(other Source) bool {
	return s.PlacementID == other.PlacementID && s.PersonaID == other.PersonaID
}

func (r row) source() (Source, bool) {
	if r.srcPlace == nil || r.srcPersona == nil {
		return Source{}, false
	}
	return Source{PlacementID: *r.srcPlace, PersonaID: *r.srcPersona}, true
}

// Arrival summarizes the verified import.
type Arrival struct {
	PersonaID     string              `json:"persona_id"`
	ContentSHA256 string              `json:"content_sha256"`
	Continuity    portable.Continuity `json:"continuity"`
	Rows          map[string]int64    `json:"rows"`
	// ModelConnectionRequired: credentials never travel, so the secretary
	// cannot answer until a model connection is selected on this placement.
	ModelConnectionRequired bool `json:"model_connection_required"`
}

type row struct {
	id         string
	provider   string
	subject    string
	grantHash  []byte
	status     string
	admitUntil time.Time
	claimUntil *time.Time
	srcPlace   *string
	srcPersona *string
}

const rowCols = `session_id, claim_provider, claim_subject, grant_hash, status, admit_until, claim_until, source_placement_id, source_persona_id`

func scanRow(r pgx.Row) (row, error) {
	var x row
	err := r.Scan(&x.id, &x.provider, &x.subject, &x.grantHash, &x.status, &x.admitUntil, &x.claimUntil,
		&x.srcPlace, &x.srcPersona)
	if errors.Is(err, pgx.ErrNoRows) {
		return x, ErrNotFound
	}
	return x, err
}

func (s *Service) load(ctx context.Context, sessionID string) (row, error) {
	if !uuidv7Re.MatchString(sessionID) {
		return row{}, ErrNotFound
	}
	return scanRow(s.pool.QueryRow(ctx, `SELECT `+rowCols+` FROM transfer_sessions WHERE session_id = $1`, sessionID))
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

func (s *Service) ownSubject(ctx context.Context, sessionID string, subj Subject) (row, error) {
	r, err := s.load(ctx, sessionID)
	if err != nil {
		return row{}, err
	}
	if r.provider != subj.Provider || r.subject != subj.Subject {
		return row{}, ErrNotFound
	}
	return r, nil
}

// Created is returned once, to the registering browser. Grant is not stored
// and cannot be read again.
type Created struct {
	Grant string
	View  View
}

// Create opens a session for a credential a trusted flow proved. It refuses a
// credential that already has an account, linked or unlinked — choosing or
// employing a secretary in an existing account is a separate product
// decision — and a credential with an open session, whose id is reported so that session can be
// continued or cancelled.
//
// Create is not idempotent and never cancels anything: the grant exists only
// in its answer. When that answer is lost, the retry is refused with the open
// session, and the registrant decides from its status (see
// docs/local-cloud-move.md, "A lost move URL"): a session that is still
// awaiting_bundle can be replaced with CancelBySubject(expect
// awaiting_bundle) and a new Create; one whose secretary arrived needs no
// grant to finish.
func (s *Service) Create(ctx context.Context, subj Subject) (Created, string, error) {
	if !subj.valid() {
		return Created{}, "", fmt.Errorf("%w: unsupported credential subject", ErrBadRequest)
	}
	// The whole row is the account, not its active flag. credentials is
	// UNIQUE (provider, external_subject) and a trigger refuses rebinding to
	// a different human (0002), so an unlinked row still names the one human
	// this credential will ever belong to: koseki re-activates it for that
	// human and refuses it for anyone else. Admitting it here would let the
	// registrant seal and stage a secretary whose account transaction can
	// never bind the credential.
	if err := refuseExistingAccount(ctx, s.pool, subj); err != nil {
		return Created{}, "", err
	}
	// Two attempts: the open session named by the unique index can close
	// between the refused insert and the read, and the answer for that is a
	// fresh session, not a 500.
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
		_, err = s.pool.Exec(ctx, `INSERT INTO transfer_sessions
			(session_id, claim_provider, claim_subject, grant_hash, status, admit_until)
			VALUES ($1, $2, $3, $4, 'awaiting_bundle', now() + $5::bigint * interval '1 millisecond')`,
			id.String(), subj.Provider, subj.Subject, hashGrant(grant), s.admitTTL.Milliseconds())
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			var open string
			qerr := s.pool.QueryRow(ctx, `SELECT session_id FROM transfer_sessions
				WHERE claim_provider = $1 AND claim_subject = $2
				  AND status IN ('awaiting_bundle','staged','provisioned')`, subj.Provider, subj.Subject).Scan(&open)
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

// refuseExistingAccount reports ErrAccountExists when this credential is
// already recorded against a human, linked or unlinked. Bringing a Local
// secretary into an account that exists is a separate product decision; this
// session only serves a registration that creates one.
func refuseExistingAccount(ctx context.Context, q rowQuerier, subj Subject) error {
	var owned bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM credentials
		WHERE provider = $1 AND external_subject = $2)`, subj.Provider, subj.Subject).Scan(&owned); err != nil {
		return err
	}
	if owned {
		return ErrAccountExists
	}
	return nil
}

// BindSource records the one Local placement that may seal for this session,
// before it seals. It is the whole answer to a move URL pasted into two Local
// installs: the second placement learns that the URL is taken while its
// secretary is still active, instead of sealing a secretary whose bundle the
// destination can never admit.
//
// The binding is durable session state, not an observation of who is running
// now, and Upload re-reads it, so it also fences a bundle that arrives later
// from a placement that never bound. It is idempotent for the placement that
// holds it: a lost answer, a retry, a restart or a repeated "start" of the
// same move URL on the same placement all reach the same row.
//
// It is taken under the session row lock, so two placements racing for one
// URL produce exactly one winner and one ErrSourceBound — never two seals.
// A closed or already-staged session binds nothing: by then the question is
// no longer which placement may seal.
//
// Binding also re-checks credential ownership, which is the last moment
// before a seal at which an account created since Create can still be
// answered without any Local authority having moved.
func (s *Service) BindSource(ctx context.Context, sessionID, grant string, src Source) (View, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return View{}, err
	}
	if !src.valid() {
		return View{}, fmt.Errorf("%w: placement_id and persona_id must be uuidv7", ErrBadRequest)
	}
	// Converge first: a session whose import committed, whose deadline
	// passed or whose cancel is still un-retired must answer from what
	// actually happened, not from a stale row.
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	if err := s.bind(ctx, sessionID, Subject{Provider: r.provider, Subject: r.subject}, src); err != nil {
		v, verr := s.view(ctx, sessionID, true)
		if verr != nil {
			return View{}, verr
		}
		return v, err
	}
	return s.reconciledView(ctx, sessionID, true)
}

func (s *Service) bind(ctx context.Context, sessionID string, subj Subject, src Source) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	r, err := scanRow(tx.QueryRow(ctx, `SELECT `+rowCols+` FROM transfer_sessions WHERE session_id = $1 FOR UPDATE`, sessionID))
	if err != nil {
		return err
	}
	if bound, ok := r.source(); ok {
		if !bound.same(src) {
			return fmt.Errorf("%w: it is held by secretary %s on placement %s",
				ErrSourceBound, bound.PersonaID, bound.PlacementID)
		}
		// Already ours. Re-reporting the status is the caller's business;
		// a staged or closed session is not an error for the holder.
		return tx.Commit(ctx)
	}
	switch r.status {
	case StatusAwaitingBundle:
	case StatusCancelled, StatusExpired:
		return fmt.Errorf("%w: the session is %s", ErrClosed, r.status)
	default:
		return fmt.Errorf("%w: the session is already %s", ErrConflict, r.status)
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT admit_until > now() FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&live); err != nil {
		return err
	}
	if !live {
		return fmt.Errorf("%w: the bundle deadline passed", ErrExpired)
	}
	if err := refuseExistingAccount(ctx, tx, subj); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE transfer_sessions
		SET source_placement_id = $2, source_persona_id = $3, source_bound_at = now(), updated_at = now()
		WHERE session_id = $1`, sessionID, src.PlacementID, src.PersonaID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Status is the grant holder's view, after reconciling. It stays readable
// after every deadline: the sealed source must always be able to learn the
// proof it earned.
func (s *Service) Status(ctx context.Context, sessionID, grant string) (View, error) {
	if _, err := s.authorize(ctx, sessionID, grant); err != nil {
		return View{}, err
	}
	return s.reconciledView(ctx, sessionID, true)
}

// ForSubject is the registering browser's view of the credential's current
// session: the open one, else the most recent. A different credential sees
// nothing.
func (s *Service) ForSubject(ctx context.Context, subj Subject) (View, error) {
	var id string
	err := s.pool.QueryRow(ctx, `SELECT session_id FROM transfer_sessions
		WHERE claim_provider = $1 AND claim_subject = $2
		ORDER BY (status IN ('awaiting_bundle','staged','provisioned')) DESC, created_at DESC LIMIT 1`,
		subj.Provider, subj.Subject).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, ErrNotFound
	}
	if err != nil {
		return View{}, err
	}
	return s.reconciledView(ctx, id, false)
}

func (s *Service) reconciledView(ctx context.Context, sessionID string, proofs bool) (View, error) {
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, err
	}
	return s.view(ctx, sessionID, proofs)
}

// Upload stages the Local source's bundle. Admission requires an open
// session before its deadline; the import itself runs in portable's own
// transaction, and the session is promoted afterwards. A repeated upload of
// a session that already staged returns the current view without reading
// the body. If the session was cancelled or expired while the import ran,
// the staged copy is retired before answering ErrClosed.
func (s *Service) Upload(ctx context.Context, sessionID, grant string, body io.Reader) (View, bool, error) {
	if _, err := s.authorize(ctx, sessionID, grant); err != nil {
		return View{}, false, err
	}
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, false, err
	}
	var admitted bool
	var status string
	var boundPersona *string
	if err := s.pool.QueryRow(ctx, `SELECT status, status = 'awaiting_bundle' AND admit_until > now(), source_persona_id
		FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&status, &admitted, &boundPersona); err != nil {
		return View{}, false, err
	}
	// A closed session answers first: its source needs the tombstone path,
	// not a lecture about who holds the URL.
	if status == StatusCancelled || status == StatusExpired {
		v, err := s.view(ctx, sessionID, true)
		if err != nil {
			return View{}, false, err
		}
		return v, false, ErrClosed
	}

	// Read the header before deciding anything else: it names the secretary
	// this bundle carries, and one line is all it costs.
	br := bufio.NewReaderSize(body, 1<<16)
	line, err := br.ReadSlice('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		if errors.Is(err, bufio.ErrBufferFull) {
			return View{}, false, fmt.Errorf("%w: bundle header too long", portable.ErrBadBundle)
		}
		return View{}, false, err
	}
	var hdr struct {
		TransferID string `json:"transfer_id"`
		PersonaID  string `json:"persona_id"`
	}
	if len(line) > maxHeader || json.Unmarshal(bytes.TrimSpace(line), &hdr) != nil {
		return View{}, false, fmt.Errorf("%w: the first line must be the bundle header", portable.ErrBadBundle)
	}
	if hdr.TransferID != sessionID {
		return View{}, false, fmt.Errorf("%w: bundle transfer_id %q is not this session's transfer", portable.ErrBadBundle, hdr.TransferID)
	}
	// Admission belongs to the placement that took this move URL before it
	// sealed, at every status — a second placement uploading after the first
	// one staged must be told that, not handed the winner's view and left to
	// discover the substitution at Complete.
	switch {
	case boundPersona == nil:
		return View{}, false, fmt.Errorf("%w: no Sumi Local placement has taken this move URL, so its bundle cannot be admitted "+
			"(run sumi-local-move start, which takes it before it seals)", ErrSourceUnbound)
	case *boundPersona != hdr.PersonaID:
		return View{}, false, fmt.Errorf("%w: it accepts secretary %s, and this bundle carries %s; nothing was imported",
			ErrSourceBound, *boundPersona, hdr.PersonaID)
	}
	if !admitted {
		// The bound source's own repeat: a staged session answers its view,
		// and an admission deadline that passed without an import is closed.
		v, err := s.view(ctx, sessionID, true)
		if err != nil {
			return View{}, false, err
		}
		if status == StatusAwaitingBundle {
			return v, false, ErrClosed
		}
		return v, false, nil
	}
	_, created, importErr := s.portable.Import(ctx, io.MultiReader(bytes.NewReader(append([]byte(nil), line...)), br), nil, false)
	if importErr != nil && !errors.Is(importErr, portable.ErrTransferConflict) {
		return View{}, false, importErr
	}
	// A conflict here is a retired transfer (cancelled with the source's
	// key before the bundle arrived) or a lost race; reconcile decides.
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return View{}, false, err
	}
	v, err := s.view(ctx, sessionID, true)
	if err != nil {
		return View{}, false, err
	}
	switch v.Status {
	case StatusCancelled, StatusExpired:
		return v, false, ErrClosed
	case StatusAwaitingBundle:
		return v, false, importErr
	}
	return v, created, nil
}

// CancelByGrant is the Local source's cancel. personaID and transferKey come
// from the source's sealed bundle header: when the bundle never arrived they
// let this placement record the tombstone whose retire_proof the source
// needs, and a late upload of that transfer is refused forever.
func (s *Service) CancelByGrant(ctx context.Context, sessionID, grant, personaID, transferKey string) (View, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return View{}, err
	}
	if (personaID == "") != (transferKey == "") {
		return View{}, fmt.Errorf("%w: persona_id and transfer_key are given together", ErrBadRequest)
	}
	// A tombstone forecloses this transfer for good. Only the placement that
	// took the move URL may write one, so a second placement's mistaken
	// cancel cannot foreclose the transfer the bound source is still moving.
	if bound, ok := r.source(); ok && personaID != "" && bound.PersonaID != personaID {
		return View{}, fmt.Errorf("%w: it accepts secretary %s, and the cancel names %s; nothing was retired",
			ErrSourceBound, bound.PersonaID, personaID)
	}
	if err := s.cancel(ctx, sessionID, ""); errors.Is(err, ErrConflict) {
		v, verr := s.reconciledView(ctx, sessionID, true)
		if verr != nil {
			return View{}, verr
		}
		return v, err
	} else if err != nil {
		return View{}, err
	}
	if transferKey != "" {
		rec, err := s.portable.Status(ctx, "import", sessionID)
		bare := errors.Is(err, portable.ErrTransferNotFound) ||
			(err == nil && rec.Status == "retired" && rec.ContentSHA256 == "")
		if err != nil && !errors.Is(err, portable.ErrTransferNotFound) {
			return View{}, err
		}
		if bare {
			own, err := s.portable.PlacementID(ctx)
			if err != nil {
				return View{}, err
			}
			if _, err := s.portable.Retire(ctx, personaID, sessionID, own, transferKey); err != nil {
				return View{}, err
			}
		}
	}
	return s.reconciledView(ctx, sessionID, true)
}

// CancelBySubject is the registering browser's cancel. expectStatus, when
// set, is the open status the person was shown ("awaiting_bundle" or
// "staged"): the cancel commits only if the session is still in it, so
// replacing a move URL nobody used can never discard a secretary that arrived
// meanwhile. A session that is already closed answers its view unchanged.
func (s *Service) CancelBySubject(ctx context.Context, sessionID string, subj Subject, expectStatus string) (View, error) {
	if expectStatus != "" && expectStatus != StatusAwaitingBundle && expectStatus != StatusStaged {
		return View{}, fmt.Errorf("%w: expect_status must be awaiting_bundle or staged", ErrBadRequest)
	}
	if _, err := s.ownSubject(ctx, sessionID, subj); err != nil {
		return View{}, err
	}
	err := s.cancel(ctx, sessionID, expectStatus)
	if err != nil && !errors.Is(err, ErrConflict) {
		return View{}, err
	}
	v, verr := s.reconciledView(ctx, sessionID, false)
	if verr != nil {
		return View{}, verr
	}
	return v, err
}

// cancel ends admission and claim under the session row lock, which is the
// same lock ClaimInTx holds through the account transaction: a cancel either
// commits before the claim (which then refuses) or sees provisioned and
// refuses itself. Retiring the staged copy follows in Reconcile.
//
// An awaiting session whose import already committed counts as staged, so an
// expectation of awaiting_bundle cannot pass a secretary that arrived before
// the session caught up. An import still running is not visible yet; if the
// cancel wins, that stage is retired and its source keeps authority.
func (s *Service) cancel(ctx context.Context, sessionID, expectStatus string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM transfer_sessions WHERE session_id = $1 FOR UPDATE`,
		sessionID).Scan(&status); err != nil {
		return err
	}
	if status == StatusAwaitingBundle {
		var arrived bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM core_transfers
			WHERE direction = 'import' AND transfer_id = $1 AND status = 'staged')`, sessionID).Scan(&arrived); err != nil {
			return err
		}
		if arrived {
			status = StatusStaged
		}
	}
	switch status {
	case StatusProvisioned, StatusActivated:
		return fmt.Errorf("%w: the account was already created with this secretary (%s)", ErrConflict, status)
	case StatusAwaitingBundle, StatusStaged:
		if expectStatus != "" && expectStatus != status {
			return fmt.Errorf("%w: the session is %s now, not %s; nothing was cancelled", ErrConflict, status, expectStatus)
		}
		if _, err := tx.Exec(ctx, `UPDATE transfer_sessions SET status = 'cancelled', updated_at = now()
			WHERE session_id = $1`, sessionID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Reconcile moves one session forward from what actually committed. Each
// step is a conditional single-statement update or an idempotent portable
// call, so concurrent reconciles, a sweeper replay or a crash at any point
// converge on the same outcome.
func (s *Service) Reconcile(ctx context.Context, sessionID string) error {
	r, err := s.load(ctx, sessionID)
	if err != nil {
		return err
	}
	rec, lerr := s.portable.Status(ctx, "import", sessionID)
	if lerr != nil && !errors.Is(lerr, portable.ErrTransferNotFound) {
		return lerr
	}
	imported := lerr == nil

	switch r.status {
	case StatusAwaitingBundle:
		switch {
		case imported && rec.Status == "staged":
			// Only an admitted upload can have committed this import; the
			// session update was interrupted.
			if _, err := s.pool.Exec(ctx, `UPDATE transfer_sessions
				SET status = 'staged', claim_until = now() + $2::bigint * interval '1 millisecond', updated_at = now()
				WHERE session_id = $1 AND status = 'awaiting_bundle'`, sessionID, s.claimTTL.Milliseconds()); err != nil {
				return err
			}
		case imported && rec.Status == "retired":
			if _, err := s.pool.Exec(ctx, `UPDATE transfer_sessions SET status = 'cancelled', updated_at = now()
				WHERE session_id = $1 AND status = 'awaiting_bundle'`, sessionID); err != nil {
				return err
			}
		default:
			if _, err := s.pool.Exec(ctx, `UPDATE transfer_sessions SET status = 'expired', updated_at = now()
				WHERE session_id = $1 AND status = 'awaiting_bundle' AND admit_until <= now()`, sessionID); err != nil {
				return err
			}
		}
	case StatusStaged:
		if _, err := s.pool.Exec(ctx, `UPDATE transfer_sessions SET status = 'expired', updated_at = now()
			WHERE session_id = $1 AND status = 'staged' AND claim_until <= now()`, sessionID); err != nil {
			return err
		}
	case StatusProvisioned:
		if !imported {
			return fmt.Errorf("%w: provisioned session %s has no import ledger", ErrConflict, sessionID)
		}
		// Activation is the obligation provisioning left behind. Cloud
		// authority starts at this commit, not at provisioning.
		if _, err := s.portable.Activate(ctx, rec.PersonaID, sessionID); err != nil {
			return fmt.Errorf("activate provisioned transfer: %w", err)
		}
		if _, err := s.pool.Exec(ctx, `UPDATE transfer_sessions SET status = 'activated', updated_at = now()
			WHERE session_id = $1 AND status = 'provisioned'`, sessionID); err != nil {
			return err
		}
	}

	// A closed session never runs its transfer here. Read the ledger again:
	// an import admitted before the close may have committed since.
	r, err = s.load(ctx, sessionID)
	if err != nil {
		return err
	}
	if r.status != StatusCancelled && r.status != StatusExpired {
		return nil
	}
	rec, lerr = s.portable.Status(ctx, "import", sessionID)
	if errors.Is(lerr, portable.ErrTransferNotFound) {
		return nil
	}
	if lerr != nil {
		return lerr
	}
	if rec.Status != "staged" {
		return nil
	}
	own, err := s.portable.PlacementID(ctx)
	if err != nil {
		return err
	}
	if _, err := s.portable.Retire(ctx, rec.PersonaID, sessionID, own, ""); err != nil {
		return fmt.Errorf("retire the staged copy of a closed session: %w", err)
	}
	return nil
}

// Sweep reconciles every session with a due step: an elapsed deadline, an
// owed activation, or a committed import the session has not caught up with.
func (s *Service) Sweep(ctx context.Context) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT session_id FROM transfer_sessions
		WHERE (status = 'awaiting_bundle' AND admit_until <= now())
		   OR (status = 'staged' AND claim_until <= now())
		   OR status = 'provisioned'
		UNION
		SELECT s.session_id FROM core_transfers t
		JOIN transfer_sessions s ON s.session_id = t.transfer_id
		WHERE t.direction = 'import' AND t.status = 'staged'
		  AND s.status IN ('awaiting_bundle','cancelled','expired')
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
			logf("transfer session sweep: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) view(ctx context.Context, sessionID string, proofs bool) (View, error) {
	r, err := s.load(ctx, sessionID)
	if err != nil {
		return View{}, err
	}
	own, err := s.portable.PlacementID(ctx)
	if err != nil {
		return View{}, err
	}
	v := View{
		SessionID: r.id, TransferID: r.id, DestinationPlacementID: own, Status: r.status,
		AdmitUntil: r.admitUntil.UTC(), StateOnly: true, NotIncluded: portable.NotIncluded,
	}
	if r.claimUntil != nil {
		t := r.claimUntil.UTC()
		v.ClaimUntil = &t
	}
	if src, ok := r.source(); ok && proofs {
		v.Source = &src
	}
	rec, err := s.portable.Status(ctx, "import", sessionID)
	if errors.Is(err, portable.ErrTransferNotFound) {
		return v, nil
	}
	if err != nil {
		return View{}, err
	}
	v.Retired = rec.Status == "retired"
	if rec.ContentSHA256 != "" {
		v.Arrival = &Arrival{PersonaID: rec.PersonaID, ContentSHA256: rec.ContentSHA256,
			Continuity: rec.Continuity, Rows: rec.Rows, ModelConnectionRequired: true}
	}
	if proofs {
		v.ActivateProof, v.RetireProof = rec.ActivateProof, rec.RetireProof
	}
	return v, nil
}

// Claim is a staged session locked inside an account transaction.
type Claim struct {
	SessionID string
	PersonaID string
	subject   Subject
}

// ClaimForSubjectInTx is the account transaction's claim for the open
// session the proved credential chose: the subject — not a request body —
// selects it. ok is false when the credential has no open session and the
// account mints a fresh secretary. An awaiting session answers ErrPending
// (the registrant must still run the move or cancel it), a provisioned one
// ErrConflict (its account transaction already ran), and a staged one is
// locked and claimed exactly as ClaimInTx does.
func (s *Service) ClaimForSubjectInTx(ctx context.Context, tx pgx.Tx, subj Subject) (Claim, bool, error) {
	if !subj.valid() {
		return Claim{}, false, fmt.Errorf("%w: unsupported credential subject", ErrBadRequest)
	}
	var sessionID, status string
	err := tx.QueryRow(ctx, `SELECT session_id, status FROM transfer_sessions
		WHERE claim_provider = $1 AND claim_subject = $2
		  AND status IN ('awaiting_bundle','staged','provisioned')`, subj.Provider, subj.Subject).
		Scan(&sessionID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, err
	}
	switch status {
	case StatusStaged:
		claim, err := ClaimInTx(ctx, tx, sessionID, subj)
		if err != nil {
			return Claim{}, false, err
		}
		return claim, true, nil
	case StatusAwaitingBundle:
		// The session may still sit on awaiting_bundle while its import
		// already committed (the promotion was interrupted). Decide under
		// the session row lock so a staging that committed between the
		// read and here still claims rather than answering ErrPending.
		r, lerr := scanRow(tx.QueryRow(ctx, `SELECT `+rowCols+` FROM transfer_sessions WHERE session_id = $1 FOR UPDATE`, sessionID))
		if lerr != nil {
			return Claim{}, false, lerr
		}
		switch r.status {
		case StatusAwaitingBundle:
			var arrived bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM core_transfers
				WHERE direction = 'import' AND transfer_id = $1 AND status = 'staged')`, sessionID).Scan(&arrived); err != nil {
				return Claim{}, false, err
			}
			if !arrived {
				// No committed import and the admission deadline passed: this
				// is what the sweep writes, decided under the claim's own row
				// lock so the deadline answer never waits on a sweep tick —
				// or on a deployment that runs no sweep. An import committed
				// before promotion still promotes and claims above: admission
				// is enforced at upload, so a staged import always arrived in
				// time.
				expired, err := tx.Exec(ctx, `UPDATE transfer_sessions
					SET status = 'expired', updated_at = now()
					WHERE session_id = $1 AND status = 'awaiting_bundle' AND admit_until <= now()`, sessionID)
				if err != nil {
					return Claim{}, false, err
				}
				if expired.RowsAffected() == 0 {
					return Claim{}, false, ErrPending
				}
				return Claim{}, false, nil
			}
			// The import committed but the promotion did not: promote now,
			// inside the claim's own lock, with the service's claim
			// deadline — Reconcile would write the same row.
			if _, err := tx.Exec(ctx, `UPDATE transfer_sessions
				SET status = 'staged', claim_until = now() + $2::bigint * interval '1 millisecond', updated_at = now()
				WHERE session_id = $1 AND status = 'awaiting_bundle'`, sessionID, s.claimTTL.Milliseconds()); err != nil {
				return Claim{}, false, err
			}
			fallthrough
		case StatusStaged:
			claim, err := ClaimInTx(ctx, tx, sessionID, subj)
			if err != nil {
				return Claim{}, false, err
			}
			return claim, true, nil
		default:
			return Claim{}, false, fmt.Errorf("%w: session is already %s", ErrConflict, r.status)
		}
	default:
		return Claim{}, false, fmt.Errorf("%w: session is already %s", ErrConflict, status)
	}
}

// ClaimInTx locks a staged session for the account transaction of a live
// flow that proved subj. The transaction must already have verified that
// flow; an expired or replayed proof never reaches this call. The session
// row lock is held to commit, so a cancel or expiry cannot pass it. The
// returned PersonaID is the carried secretary's id, which the account uses
// instead of minting one. A session of another credential is ErrNotFound.
func ClaimInTx(ctx context.Context, tx pgx.Tx, sessionID string, subj Subject) (Claim, error) {
	if !uuidv7Re.MatchString(sessionID) || !subj.valid() {
		return Claim{}, ErrNotFound
	}
	r, err := scanRow(tx.QueryRow(ctx, `SELECT `+rowCols+` FROM transfer_sessions WHERE session_id = $1 FOR UPDATE`, sessionID))
	if err != nil {
		return Claim{}, err
	}
	if r.provider != subj.Provider || r.subject != subj.Subject {
		return Claim{}, ErrNotFound
	}
	switch r.status {
	case StatusStaged:
	case StatusCancelled, StatusExpired:
		return Claim{}, ErrClosed
	case StatusAwaitingBundle:
		return Claim{}, fmt.Errorf("%w: the secretary has not arrived yet", ErrConflict)
	default:
		return Claim{}, fmt.Errorf("%w: session is already %s", ErrConflict, r.status)
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT claim_until > now() FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&live); err != nil {
		return Claim{}, err
	}
	if !live {
		return Claim{}, ErrExpired
	}
	var personaID, ledgerStatus string
	err = tx.QueryRow(ctx, `SELECT persona_id, status FROM core_transfers
		WHERE direction = 'import' AND transfer_id = $1 FOR SHARE`, sessionID).Scan(&personaID, &ledgerStatus)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && ledgerStatus != "staged") {
		return Claim{}, fmt.Errorf("%w: the staged import is not available", ErrConflict)
	}
	if err != nil {
		return Claim{}, err
	}
	return Claim{SessionID: sessionID, PersonaID: personaID, subject: subj}, nil
}

// ProvisionInTx completes the claim inside the same account transaction,
// after that transaction created humanID and bound the claimed credential to
// it. It binds the staged persona to the human and records the activation
// obligation; the caller runs Service.Reconcile (or leaves it to Sweep)
// after commit. The persona is not active until that activation commits.
func ProvisionInTx(ctx context.Context, tx pgx.Tx, claim Claim, humanID string) error {
	if claim.SessionID == "" || !claim.subject.valid() || !uuidv7Re.MatchString(humanID) {
		return fmt.Errorf("%w: provision requires a claim from ClaimInTx and a human id", ErrBadRequest)
	}
	var status, provider, subject string
	if err := tx.QueryRow(ctx, `SELECT status, claim_provider, claim_subject FROM transfer_sessions
		WHERE session_id = $1 FOR UPDATE`, claim.SessionID).Scan(&status, &provider, &subject); err != nil {
		return err
	}
	if status != StatusStaged || provider != claim.subject.Provider || subject != claim.subject.Subject {
		return fmt.Errorf("%w: session is %s", ErrConflict, status)
	}
	var bound bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM credentials
		WHERE provider = $1 AND external_subject = $2 AND human_id = $3 AND active)`,
		provider, subject, humanID).Scan(&bound); err != nil {
		return err
	}
	if !bound {
		return fmt.Errorf("%w: the claimed credential is not bound to human %s in this transaction", ErrBadRequest, humanID)
	}
	if _, err := agentstate.BindHumanInTx(ctx, tx, claim.PersonaID, humanID); err != nil {
		return fmt.Errorf("bind carried secretary: %w", err)
	}
	_, err := tx.Exec(ctx, `UPDATE transfer_sessions SET status = 'provisioned', human_id = $2, updated_at = now()
		WHERE session_id = $1`, claim.SessionID, humanID)
	return err
}
