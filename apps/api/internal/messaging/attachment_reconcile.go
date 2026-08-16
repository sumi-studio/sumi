package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// AttachmentReconcileInterval is how often the background reconciler runs.
const AttachmentReconcileInterval = time.Minute

// attachmentReconcileBatch bounds one pass so a large backlog is worked off
// over several runs instead of one long transaction.
const attachmentReconcileBatch = 500

// AttachmentReconciliation reports what one idempotent pass changed.
type AttachmentReconciliation struct {
	// Reservations that expired without finalizing; their bytes left the ledger.
	ReleasedReservations int
	// Finalized drafts older than the unbound TTL that were queued for deletion.
	ExpiredDrafts int
	// Blobs whose deletion was confirmed; their bytes left the ledger.
	DeletedBlobs int
	// Retained metadata rows of expired drafts that were removed after deletion.
	PurgedDraftRows int
	// Published blobs with no live metadata (crash between publish and commit,
	// or metadata already deleted) that were removed.
	OrphanBlobs int
	// Settled reservation receipts older than the unbound TTL that were removed.
	PurgedReceipts int
	// Live rows whose blob is missing on disk. Reported, never mutated: this is
	// data loss to notice, not garbage to collect.
	MissingBlobs int
}

// ReconcileAttachments runs every idempotent reconciliation step once. Each
// step is safe to repeat and to interleave with live uploads:
//
//   - expired reservations release their ledger bytes in one transaction;
//   - unbound drafts past their TTL move into the deletion outbox;
//   - queued deletions unlink bytes first and only then confirm the state
//     transition and release the ledger, so a crash replays the unlink;
//   - published blobs older than the reservation TTL with no live row are
//     orphans of an ambiguous finalize or of a completed deletion and are
//     removed;
//   - settled receipts past the unbound TTL are purged.
func (s *Store) ReconcileAttachments(ctx context.Context) (AttachmentReconciliation, error) {
	var report AttachmentReconciliation
	if !s.AttachmentsEnabled() {
		return report, nil
	}
	policy := s.attachmentPolicy
	now := time.Now()

	released, err := s.releaseExpiredReservations(ctx, now)
	if err != nil {
		return report, err
	}
	report.ReleasedReservations = released

	expired, err := s.expireUnboundDrafts(ctx, now.Add(-policy.UnboundTTL))
	if err != nil {
		return report, err
	}
	report.ExpiredDrafts = expired

	deleted, purged, err := s.processAttachmentDeletions(ctx)
	if err != nil {
		return report, err
	}
	report.DeletedBlobs, report.PurgedDraftRows = deleted, purged

	orphans, missing, err := s.sweepAttachmentBlobs(ctx, now.Add(-policy.ReservationTTL))
	if err != nil {
		return report, err
	}
	report.OrphanBlobs, report.MissingBlobs = orphans, missing

	receipts, err := s.purgeSettledUploadReceipts(ctx, now.Add(-policy.UnboundTTL))
	if err != nil {
		return report, err
	}
	report.PurgedReceipts = receipts
	return report, nil
}

func (s *Store) releaseExpiredReservations(ctx context.Context, now time.Time) (int, error) {
	released := 0
	for {
		rows, err := s.pool.Query(ctx, `
			SELECT upload_id, workspace_id, declared_bytes
			FROM message_attachment_uploads
			WHERE state = 'reserved' AND expires_at <= $1
			ORDER BY expires_at, upload_id
			LIMIT $2`, now, attachmentReconcileBatch)
		if err != nil {
			return released, fmt.Errorf("query expired reservations: %w", err)
		}
		type expired struct {
			id, workspace string
			bytes         int64
		}
		var batch []expired
		for rows.Next() {
			var e expired
			if err := rows.Scan(&e.id, &e.workspace, &e.bytes); err != nil {
				rows.Close()
				return released, fmt.Errorf("scan expired reservation: %w", err)
			}
			batch = append(batch, e)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return released, err
		}
		if len(batch) == 0 {
			return released, nil
		}
		for _, e := range batch {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				return released, fmt.Errorf("begin reservation release: %w", err)
			}
			// Keep the same global -> Workspace -> upload lock order as reserve.
			if _, err := lockAttachmentUsage(ctx, tx, e.workspace); err != nil {
				_ = tx.Rollback(ctx)
				return released, err
			}
			var bytes int64
			err = tx.QueryRow(ctx, `
				SELECT declared_bytes FROM message_attachment_uploads
				WHERE upload_id = $1 AND state = 'reserved' AND expires_at <= $2
				FOR UPDATE`, e.id, now).Scan(&bytes)
			if errors.Is(err, pgx.ErrNoRows) {
				_ = tx.Rollback(ctx)
				continue
			}
			if err != nil {
				_ = tx.Rollback(ctx)
				return released, fmt.Errorf("lock expired reservation: %w", err)
			}
			// A reservation's deterministic staging and published paths are both
			// removed while its row is locked before quota can leave the ledger.
			if err := s.blobs.DiscardStaging(e.id); err != nil {
				_ = tx.Rollback(ctx)
				return released, fmt.Errorf("discard expired attachment staging: %w", err)
			}
			if err := s.blobs.Remove(e.id); err != nil {
				_ = tx.Rollback(ctx)
				return released, fmt.Errorf("remove expired attachment blob: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				UPDATE message_attachment_uploads
				SET state = 'released', settled_at = now(), staging_token = NULL, staging_expires_at = NULL
				WHERE upload_id = $1`, e.id); err != nil {
				_ = tx.Rollback(ctx)
				return released, fmt.Errorf("release reservation: %w", err)
			}
			if err := adjustAttachmentUsage(ctx, tx, e.workspace, -bytes, -1); err != nil {
				_ = tx.Rollback(ctx)
				return released, err
			}
			if err := tx.Commit(ctx); err != nil {
				return released, fmt.Errorf("commit reservation release: %w", err)
			}
			released++
		}
		if len(batch) < attachmentReconcileBatch {
			return released, nil
		}
	}
}

func (s *Store) expireUnboundDrafts(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE message_attachments SET blob_state = 'deleting'
		WHERE attachment_id IN (
			SELECT attachment_id FROM message_attachments
			WHERE message_id IS NULL AND blob_state = 'stored' AND created_at < $1
			ORDER BY created_at, attachment_id
			LIMIT $2
		)`, cutoff, attachmentReconcileBatch)
	if err != nil {
		return 0, fmt.Errorf("expire unbound drafts: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// processAttachmentDeletions is the deletion outbox worker. For every queued
// row it removes the blob (idempotently) and then, in one transaction,
// confirms the state transition and releases the ledger. Retained rows of
// expired drafts are removed once deleted; tombstoned messages keep theirs.
func (s *Store) processAttachmentDeletions(ctx context.Context) (int, int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT attachment_id, workspace_id FROM message_attachments
		WHERE blob_state = 'deleting'
		ORDER BY created_at, attachment_id
		LIMIT $1`, attachmentReconcileBatch)
	if err != nil {
		return 0, 0, fmt.Errorf("query attachment deletions: %w", err)
	}
	type deletion struct{ id, workspace string }
	var ids []deletion
	for rows.Next() {
		var item deletion
		if err := rows.Scan(&item.id, &item.workspace); err != nil {
			rows.Close()
			return 0, 0, fmt.Errorf("scan attachment deletion: %w", err)
		}
		ids = append(ids, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}
	deleted, purged := 0, 0
	for _, item := range ids {
		if err := s.blobs.Remove(item.id); err != nil {
			return deleted, purged, fmt.Errorf("remove attachment blob %s: %w", item.id, err)
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return deleted, purged, fmt.Errorf("begin deletion confirm: %w", err)
		}
		var (
			workspaceID string
			size        int64
			messageID   *string
		)
		// Keep the global -> Workspace -> attachment order even though the
		// byte unlink itself happens before the durable confirmation.
		if _, err := lockAttachmentUsage(ctx, tx, item.workspace); err != nil {
			_ = tx.Rollback(ctx)
			return deleted, purged, err
		}
		err = tx.QueryRow(ctx, `
			UPDATE message_attachments
			SET blob_state = 'deleted', blob_deleted_at = now()
			WHERE attachment_id = $1 AND blob_state = 'deleting'
			RETURNING workspace_id, size_bytes, message_id`, item.id).Scan(&workspaceID, &size, &messageID)
		if errors.Is(err, pgx.ErrNoRows) {
			// A concurrent pass already confirmed it.
			_ = tx.Rollback(ctx)
			continue
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return deleted, purged, fmt.Errorf("confirm attachment deletion: %w", err)
		}
		if workspaceID != item.workspace {
			_ = tx.Rollback(ctx)
			return deleted, purged, errors.New("attachment deletion workspace changed")
		}
		if err := adjustAttachmentUsage(ctx, tx, workspaceID, -size, -1); err != nil {
			_ = tx.Rollback(ctx)
			return deleted, purged, err
		}
		if messageID == nil {
			// An expired draft has no history to preserve.
			if _, err := tx.Exec(ctx, `
				DELETE FROM message_attachment_uploads WHERE attachment_id = $1`, item.id); err != nil {
				_ = tx.Rollback(ctx)
				return deleted, purged, fmt.Errorf("purge draft receipt: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				DELETE FROM message_attachments WHERE attachment_id = $1 AND blob_state = 'deleted'`, item.id); err != nil {
				_ = tx.Rollback(ctx)
				return deleted, purged, fmt.Errorf("purge draft row: %w", err)
			}
			purged++
		}
		if err := tx.Commit(ctx); err != nil {
			return deleted, purged, fmt.Errorf("commit deletion confirm: %w", err)
		}
		deleted++
	}
	return deleted, purged, nil
}

func (s *Store) sweepAttachmentBlobs(ctx context.Context, cutoff time.Time) (int, int, error) {
	candidates, err := s.blobs.Sweep(cutoff)
	if err != nil {
		return 0, 0, err
	}
	orphans := 0
	for _, id := range candidates {
		removed, err := s.removeOrphanAttachmentBlob(ctx, id, cutoff)
		if err != nil {
			return orphans, 0, err
		}
		if removed {
			orphans++
		}
	}
	// Missing blobs for live rows are reported, not repaired. Advance a stable
	// keyset cursor rather than repeatedly inspecting the oldest 500 forever.
	s.missingBlobScan.Lock()
	defer s.missingBlobScan.Unlock()
	rows, err := s.queryStoredAttachmentBatch(ctx, cutoff)
	if err != nil {
		return orphans, 0, fmt.Errorf("query stored attachments: %w", err)
	}
	var stored []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return orphans, 0, fmt.Errorf("scan stored attachment: %w", err)
		}
		stored = append(stored, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return orphans, 0, err
	}
	if len(stored) > 0 {
		var createdAt time.Time
		if err := s.pool.QueryRow(ctx, `SELECT created_at FROM message_attachments WHERE attachment_id = $1`, stored[len(stored)-1]).Scan(&createdAt); err != nil {
			return orphans, 0, fmt.Errorf("read missing-blob cursor: %w", err)
		}
		s.missingBlobScan.createdAt = createdAt
		s.missingBlobScan.attachmentID = stored[len(stored)-1]
	} else if !s.missingBlobScan.createdAt.IsZero() {
		// An empty keyset pass reached the end. Wrap next pass so older rows
		// remain observable after a restore or later corruption.
		s.missingBlobScan.createdAt = time.Time{}
		s.missingBlobScan.attachmentID = ""
	}
	missing := 0
	for _, id := range stored {
		blob, err := s.blobs.Open(id)
		if errors.Is(err, ErrAttachmentNotFound) {
			missing++
			continue
		}
		if err != nil {
			return orphans, missing, err
		}
		_ = blob.Close()
	}
	return orphans, missing, nil
}

// removeOrphanAttachmentBlob holds the same usage -> reservation order as a
// finalizer before rechecking liveness and mtime. Keeping that lock across the
// unlink closes the stale Sweep snapshot/finalize TOCTOU window.
func (s *Store) removeOrphanAttachmentBlob(ctx context.Context, id string, cutoff time.Time) (bool, error) {
	var workspaceID string
	err := s.pool.QueryRow(ctx, `SELECT workspace_id FROM message_attachment_uploads WHERE upload_id = $1`, id).Scan(&workspaceID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("query orphan reservation: %w", err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin orphan check: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if workspaceID != "" {
		if _, err := lockAttachmentUsage(ctx, tx, workspaceID); err != nil {
			return false, err
		}
		var ignored string
		err := tx.QueryRow(ctx, `SELECT upload_id FROM message_attachment_uploads WHERE upload_id = $1 FOR UPDATE`, id).Scan(&ignored)
		if !errors.Is(err, pgx.ErrNoRows) && err != nil {
			return false, fmt.Errorf("lock orphan reservation: %w", err)
		}
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM message_attachments WHERE attachment_id = $1 AND blob_state <> 'deleted')`, id).Scan(&live); err != nil {
		return false, fmt.Errorf("recheck orphan liveness: %w", err)
	}
	if live {
		return false, nil
	}
	older, err := s.blobs.PublishedBefore(id, cutoff)
	if err != nil {
		return false, fmt.Errorf("recheck orphan age %s: %w", id, err)
	}
	if !older {
		return false, nil
	}
	if err := s.blobs.Remove(id); err != nil {
		return false, fmt.Errorf("remove orphaned attachment %s: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit orphan check: %w", err)
	}
	return true, nil
}

func (s *Store) queryStoredAttachmentBatch(ctx context.Context, cutoff time.Time) (pgx.Rows, error) {
	if s.missingBlobScan.createdAt.IsZero() {
		return s.pool.Query(ctx, `
			SELECT attachment_id FROM message_attachments
			WHERE blob_state = 'stored' AND created_at < $1
			ORDER BY created_at, attachment_id LIMIT $2`, cutoff, attachmentReconcileBatch)
	}
	return s.pool.Query(ctx, `
		SELECT attachment_id FROM message_attachments
		WHERE blob_state = 'stored' AND created_at < $1
		  AND (created_at > $2 OR (created_at = $2 AND attachment_id > $3))
		ORDER BY created_at, attachment_id LIMIT $4`, cutoff, s.missingBlobScan.createdAt,
		s.missingBlobScan.attachmentID, attachmentReconcileBatch)
}

func (s *Store) purgeSettledUploadReceipts(ctx context.Context, cutoff time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM message_attachment_uploads
		WHERE upload_id IN (
			SELECT upload_id FROM message_attachment_uploads
			WHERE state <> 'reserved' AND settled_at < $1
			ORDER BY settled_at, upload_id
			LIMIT $2
		)`, cutoff, attachmentReconcileBatch)
	if err != nil {
		return 0, fmt.Errorf("purge settled upload receipts: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// RunAttachmentReconciler runs ReconcileAttachments on a timer until ctx is
// done. A failing pass is logged, not fatal: the next pass sees the same
// backlog. The first pass runs immediately so a restart clears crash debris.
func (s *Store) RunAttachmentReconciler(ctx context.Context, interval time.Duration) {
	if !s.AttachmentsEnabled() {
		return
	}
	if interval <= 0 {
		interval = AttachmentReconcileInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		report, err := s.ReconcileAttachments(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Printf("messaging attachment reconcile: %v", err)
		case report != (AttachmentReconciliation{}):
			log.Printf("messaging attachment reconcile: %+v", report)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
