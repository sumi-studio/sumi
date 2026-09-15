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
	ErrAccountExists = errors.New("this credential already has an account; bringing a secretary into an existing account is not supported")
	ErrBadRequest    = errors.New("bad request")
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
}

const rowCols = `session_id, claim_provider, claim_subject, grant_hash, status, admit_until, claim_until`

func scanRow(r pgx.Row) (row, error) {
	var x row
	err := r.Scan(&x.id, &x.provider, &x.subject, &x.grantHash, &x.status, &x.admitUntil, &x.claimUntil)
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
// credential that already has an account — choosing or employing a secretary
// in an existing account is a separate product decision — and a credential
// with an open session, whose id is reported so that session can be
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
	var hasAccount bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM credentials
		WHERE provider = $1 AND external_subject = $2 AND active)`, subj.Provider, subj.Subject).Scan(&hasAccount); err != nil {
		return Created{}, "", err
	}
	if hasAccount {
		return Created{}, "", ErrAccountExists
	}
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
		if qerr := s.pool.QueryRow(ctx, `SELECT session_id FROM transfer_sessions
			WHERE claim_provider = $1 AND claim_subject = $2
			  AND status IN ('awaiting_bundle','staged','provisioned')`, subj.Provider, subj.Subject).Scan(&open); qerr != nil {
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
	if err := s.pool.QueryRow(ctx, `SELECT status, status = 'awaiting_bundle' AND admit_until > now()
		FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&status, &admitted); err != nil {
		return View{}, false, err
	}
	if !admitted {
		v, err := s.view(ctx, sessionID, true)
		if err != nil {
			return View{}, false, err
		}
		if status == StatusCancelled || status == StatusExpired || status == StatusAwaitingBundle {
			return v, false, ErrClosed
		}
		return v, false, nil
	}

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
	}
	if len(line) > maxHeader || json.Unmarshal(bytes.TrimSpace(line), &hdr) != nil {
		return View{}, false, fmt.Errorf("%w: the first line must be the bundle header", portable.ErrBadBundle)
	}
	if hdr.TransferID != sessionID {
		return View{}, false, fmt.Errorf("%w: bundle transfer_id %q is not this session's transfer", portable.ErrBadBundle, hdr.TransferID)
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
	if _, err := s.authorize(ctx, sessionID, grant); err != nil {
		return View{}, err
	}
	if (personaID == "") != (transferKey == "") {
		return View{}, fmt.Errorf("%w: persona_id and transfer_key are given together", ErrBadRequest)
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
