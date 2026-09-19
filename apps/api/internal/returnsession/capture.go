// capture.go binds a sealed local-mode return to one durable immutable
// capture of the source workspace. The binding — scope_id, capture_id,
// manifest_sha — is persisted on the session row, so a lost create
// response or a mover restart re-derives the same association instead of
// minting a second one. Owner and epoch are NEVER caller input: the
// filesvc capture is recorded under the session id and its durable
// file_epoch, derived here from the authorized session row.
//
// Every read surface (entries, row bytes) resolves the persisted
// capture_id — a grant can never name a capture it was not bound to.
// A retake carries the caller's expected_scope_id: a mismatch answers
// the CURRENT binding (the caller's manifest is stale; it re-syncs),
// and a new capture whose resolved scope identity differs from the
// persisted one is refused and released — the grant no longer names
// the same tree.
package returnsession

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
)

// CaptureBinding is the persisted capture association — what the mover's
// reads are bound to. It is also the recovery answer: any number of
// POSTs, restarts and lost responses converge on this triple.
type CaptureBinding struct {
	ScopeID     string `json:"scope_id"`
	CaptureID   string `json:"capture_id"`
	ManifestSHA string `json:"manifest_sha"`
	// Retaken is true on a retake response that minted a new capture —
	// the caller must abandon the old manifest's staging. On every other
	// response it is false: the association is unchanged.
	Retaken bool `json:"retaken,omitempty"`
}

// ErrScopeChanged means a retake resolved a different scope identity
// than the persisted binding — the volume anchor was renamed out and
// recreated. The copy can never be coherent; the session stays sealed
// for an operator decision, not a silent rebind.
var ErrScopeChanged = errors.New("the Cloud workspace's scope identity changed since the copy bound its capture")

// CaptureStore is the filesvc capture surface returnsession needs —
// *fileaccess.Client satisfies it.
type CaptureStore interface {
	CreateCapture(ctx context.Context, scope, owner string, epoch int64, expectedScopeID string) (*fileaccess.CaptureMeta, error)
	GetCapture(ctx context.Context, id, owner string, epoch int64) (*fileaccess.CaptureMeta, error)
	ReleaseCapture(ctx context.Context, id, owner string, epoch int64) error
}

// expectedScope is the retake identity check: a rebinding keeps the
// prior scope_id as its expectation so a changed anchor refuses inside
// the capture transaction; a first bind expects nothing.
func expectedScope(b CaptureBinding) string {
	return b.ScopeID
}

// captureSession authorizes the grant for capture surfaces: local-mode
// only, and only while sealed — the copy window is the only time the
// grant reaches captured bytes. A superseded or resolved session's
// reconcile lands a terminal status before the mode check, so the
// refusal carries the real state.
func (s *Service) captureSession(ctx context.Context, sessionID, grant string) (row, error) {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return r, err
	}
	if r.fileModeStr() != string(FileModeLocal) {
		return r, fmt.Errorf("%w: this return did not select local file copying", ErrBadRequest)
	}
	if s.capture == nil {
		return r, fmt.Errorf("%w: capture service is not configured", ErrCaptureUnconfigured)
	}
	if err := s.Reconcile(ctx, sessionID); err != nil {
		return r, err
	}
	r, err = s.load(ctx, sessionID)
	if err != nil {
		return r, err
	}
	if r.status != StatusSealed {
		return r, fmt.Errorf("%w: the session is %s; the captured workspace is bound for the copy only while sealed",
			ErrConflict, r.status)
	}
	return r, nil
}

// scanBinding reads the persisted triple from a row cursor — all three
// columns are nullable (NULL while unbound).
func scanBinding(row pgx.Row) (*CaptureBinding, error) {
	var b CaptureBinding
	var sid, cid, msha *string
	if err := row.Scan(&sid, &cid, &msha); err != nil {
		return nil, err
	}
	if cid == nil {
		return nil, nil
	}
	b.ScopeID, b.CaptureID, b.ManifestSHA = *sid, *cid, *msha
	return &b, nil
}

// loadBinding reads the persisted triple (nil when unbound).
func (s *Service) loadBinding(ctx context.Context, sessionID string) (*CaptureBinding, error) {
	b, err := scanBinding(s.pool.QueryRow(ctx, `SELECT capture_scope_id, capture_id, capture_manifest_sha
		FROM return_sessions WHERE session_id = $1`, sessionID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return b, err
}

// EnsureCapture returns the session's capture association, binding one
// if none exists. Idempotent by construction: the association persists
// on the session row, so a retried POST after a lost response — or a
// fresh mover process — gets the SAME capture, not a new one. A
// persisted binding whose capture expired server-side is re-created
// under the same scope identity inside one transaction.
func (s *Service) EnsureCapture(ctx context.Context, sessionID, grant string) (*CaptureBinding, error) {
	r, err := s.captureSession(ctx, sessionID, grant)
	if err != nil {
		return nil, err
	}
	scope, err := fileaccess.ScopeForPersona(r.personaID)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, r.personaID); err != nil {
		return nil, err
	}
	existing, err := scanBinding(tx.QueryRow(ctx, `SELECT capture_scope_id, capture_id, capture_manifest_sha
		FROM return_sessions WHERE session_id = $1 FOR UPDATE`, sessionID))
	if err != nil {
		return nil, err
	}
	var b CaptureBinding
	if existing != nil {
		b = *existing
		// A persisted binding is the answer unless the capture is gone
		// server-side (released/expired) — then the SAME session, owner
		// and epoch mint a replacement under the same lock.
		if _, err := s.capture.GetCapture(ctx, b.CaptureID, sessionID, r.fileEpoch); err == nil {
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return &b, nil
		}
	}
	// Only the persona's current file authority may bind a capture — a
	// stale session can never attach lineage to a store a newer bound
	// return owns.
	owner, err := s.fileStoreOwnerTx(ctx, tx, r.personaID)
	if err != nil {
		return nil, err
	}
	if owner != sessionID {
		return nil, fmt.Errorf("%w: a newer return superseded this session's storage authority", ErrConflict)
	}
	meta, err := s.capture.CreateCapture(ctx, scope, sessionID, r.fileEpoch, expectedScope(b))
	if err != nil {
		return nil, err
	}
	if existing != nil && b.ScopeID != "" && meta.ScopeID != b.ScopeID {
		// The expired binding's anchor no longer resolves — recreating
		// under it would silently switch trees. Refuse; the new capture
		// is released and the stale binding left for the operator.
		_ = s.capture.ReleaseCapture(ctx, meta.CaptureID, sessionID, r.fileEpoch)
		return nil, ErrScopeChanged
	}
	if _, err := tx.Exec(ctx, `UPDATE return_sessions
		SET capture_scope_id = $2, capture_id = $3, capture_manifest_sha = $4
		WHERE session_id = $1`,
		sessionID, meta.ScopeID, meta.CaptureID, meta.ManifestSHA); err != nil {
		// The uncommitted capture is released best-effort — a leaked
		// reservation expires on its own, a leaked binding would not.
		_ = s.capture.ReleaseCapture(ctx, meta.CaptureID, sessionID, r.fileEpoch)
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = s.capture.ReleaseCapture(ctx, meta.CaptureID, sessionID, r.fileEpoch)
		return nil, err
	}
	return &CaptureBinding{ScopeID: meta.ScopeID, CaptureID: meta.CaptureID, ManifestSHA: meta.ManifestSHA}, nil
}

// CaptureView answers GET capture — the persisted association for
// restart/lost-response recovery, or 404 while unbound.
func (s *Service) CaptureView(ctx context.Context, sessionID, grant string) (*CaptureBinding, error) {
	if _, err := s.captureSession(ctx, sessionID, grant); err != nil {
		return nil, err
	}
	b, err := s.loadBinding(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("%w: no capture is bound yet", ErrNotFound)
	}
	return b, nil
}

// captureAuth is the read-path authorization: the grant's sealed
// local-mode session must hold a persisted binding, and the returned
// triple is that binding plus the session's durable lineage — never a
// caller-supplied capture id, owner or epoch.
func (s *Service) captureAuth(ctx context.Context, sessionID, grant string) (string, string, int64, error) {
	r, err := s.captureSession(ctx, sessionID, grant)
	if err != nil {
		return "", "", 0, err
	}
	b, err := s.loadBinding(ctx, sessionID)
	if err != nil {
		return "", "", 0, err
	}
	if b == nil {
		return "", "", 0, fmt.Errorf("%w: no capture is bound yet", ErrConflict)
	}
	return b.CaptureID, sessionID, r.fileEpoch, nil
}

// AuthorizeCaptureEntries resolves the capture id and lineage the grant
// may page.
func (s *Service) AuthorizeCaptureEntries(ctx context.Context, sessionID, grant string) (string, string, int64, error) {
	return s.captureAuth(ctx, sessionID, grant)
}

// AuthorizeCaptureRead resolves the capture id and lineage a row read
// binds to. seq/offset/len are caller-chosen ranges INSIDE the bound
// manifest — they can never reach another capture's bytes.
func (s *Service) AuthorizeCaptureRead(ctx context.Context, sessionID, grant string) (string, string, int64, error) {
	return s.captureAuth(ctx, sessionID, grant)
}

// RetakeCapture mints a fresh coherent manifest when the bound capture
// loses a required object or expires. expectedScopeID is the caller's
// bound scope identity: a stale expectation answers the CURRENT binding
// (retaken=false — a lost earlier retake response recovers identically);
// a matching expectation creates the new capture and refuses if its
// resolved scope identity differs. The checkpointed filesvc has no
// expected_scope_id parameter yet, so the identity compare happens here
// on the metadata the service resolved inside its own transaction.
func (s *Service) RetakeCapture(ctx context.Context, sessionID, grant, expectedScopeID string) (*CaptureBinding, error) {
	r, err := s.captureSession(ctx, sessionID, grant)
	if err != nil {
		return nil, err
	}
	scope, err := fileaccess.ScopeForPersona(r.personaID)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, r.personaID); err != nil {
		return nil, err
	}
	existing, err := scanBinding(tx.QueryRow(ctx, `SELECT capture_scope_id, capture_id, capture_manifest_sha
		FROM return_sessions WHERE session_id = $1 FOR UPDATE`, sessionID))
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("%w: no capture is bound yet — bind before retaking", ErrConflict)
	}
	b := *existing
	if expectedScopeID == "" || expectedScopeID != b.ScopeID {
		// The caller's notion is stale (a lost earlier retake answer):
		// the current binding is the truth it must re-sync to.
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &b, nil
	}
	owner, err := s.fileStoreOwnerTx(ctx, tx, r.personaID)
	if err != nil {
		return nil, err
	}
	if owner != sessionID {
		return nil, fmt.Errorf("%w: a newer return superseded this session's storage authority", ErrConflict)
	}
	meta, err := s.capture.CreateCapture(ctx, scope, sessionID, r.fileEpoch, b.ScopeID)
	if err != nil {
		return nil, err
	}
	if meta.ScopeID != b.ScopeID {
		_ = s.capture.ReleaseCapture(ctx, meta.CaptureID, sessionID, r.fileEpoch)
		return nil, ErrScopeChanged
	}
	if _, err := tx.Exec(ctx, `UPDATE return_sessions
		SET capture_id = $2, capture_manifest_sha = $3
		WHERE session_id = $1`, sessionID, meta.CaptureID, meta.ManifestSHA); err != nil {
		_ = s.capture.ReleaseCapture(ctx, meta.CaptureID, sessionID, r.fileEpoch)
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = s.capture.ReleaseCapture(ctx, meta.CaptureID, sessionID, r.fileEpoch)
		return nil, err
	}
	// The superseded capture is released outside the lock — the binding
	// already moved on, so a release failure only leaks a reservation
	// that expires on its own.
	_ = s.capture.ReleaseCapture(ctx, b.CaptureID, sessionID, r.fileEpoch)
	return &CaptureBinding{
		ScopeID:     meta.ScopeID,
		CaptureID:   meta.CaptureID,
		ManifestSHA: meta.ManifestSHA,
		Retaken:     true,
	}, nil
}

// ReleaseCapture drops the persisted association and frees the capture.
// Idempotent — an unbound session answers success.
func (s *Service) ReleaseCapture(ctx context.Context, sessionID, grant string) error {
	r, err := s.authorize(ctx, sessionID, grant)
	if err != nil {
		return err
	}
	if r.fileModeStr() != string(FileModeLocal) {
		return fmt.Errorf("%w: this return did not select local file copying", ErrBadRequest)
	}
	b, err := s.loadBinding(ctx, sessionID)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `UPDATE return_sessions
		SET capture_id = NULL, capture_scope_id = NULL, capture_manifest_sha = NULL
		WHERE session_id = $1`, sessionID); err != nil {
		return err
	}
	if b != nil && s.capture != nil {
		_ = s.capture.ReleaseCapture(ctx, b.CaptureID, sessionID, r.fileEpoch)
	}
	return nil
}

// releaseBoundCapture clears any binding when the session resolves — the
// copy window is over, so the manifest's object reservation must not
// outlive it. Best-effort: a filesvc outage leaves a reservation that
// expires on its own; the binding row is always cleared.
func (s *Service) releaseBoundCapture(ctx context.Context, sessionID string) {
	if s.capture == nil {
		return
	}
	var cid *string
	var epoch int64
	if err := s.pool.QueryRow(ctx, `SELECT capture_id, file_epoch FROM return_sessions
		WHERE session_id = $1`, sessionID).Scan(&cid, &epoch); err != nil || cid == nil {
		return
	}
	if _, err := s.pool.Exec(ctx, `UPDATE return_sessions
		SET capture_id = NULL, capture_scope_id = NULL, capture_manifest_sha = NULL
		WHERE session_id = $1`, sessionID); err != nil {
		if s.logf != nil {
			s.logf("return %s: clear capture binding: %v", sessionID, err)
		}
		return
	}
	if err := s.capture.ReleaseCapture(ctx, *cid, sessionID, epoch); err != nil && s.logf != nil {
		s.logf("return %s: release capture %s: %v", sessionID, *cid, err)
	}
}

// ErrCaptureUnconfigured means the deployment has no capture service —
// a local-mode copy cannot be fenced without it.
var ErrCaptureUnconfigured = errors.New("capture service is not configured")
