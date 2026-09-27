package filesvc

// capture_store.go — durable manifest persistence in the filesvc
// database. Capture rows, manifest entries, and ordered slice records
// live here so a process restart retrieves the same capture: reads are
// served from these tables, never from re-walking a live tree.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// captureDDL is appended to Store.migrate — one schema home, migrated
// under the same single-writer lock as every other filesvc table.
const captureDDL = `
	CREATE TABLE IF NOT EXISTS file_capture (
		capture_id   text PRIMARY KEY,
		scope        text NOT NULL,
		owner        text NOT NULL DEFAULT '',
		owner_epoch  bigint NOT NULL DEFAULT 0,
		volume       text NOT NULL,
		anchor_ino   bigint NOT NULL,
		scope_id     text NOT NULL,
		format       jsonb NOT NULL,
		manifest_sha text NOT NULL,
		status       text NOT NULL DEFAULT 'active',
		entry_count  bigint NOT NULL DEFAULT 0,
		unsupported  bigint NOT NULL DEFAULT 0,
		created_at   timestamptz NOT NULL DEFAULT now(),
		expires_at   timestamptz NOT NULL,
		released_at  timestamptz
	);
	CREATE INDEX IF NOT EXISTS file_capture_scope ON file_capture(scope, status, created_at);
	CREATE TABLE IF NOT EXISTS file_capture_entry (
		capture_id text NOT NULL REFERENCES file_capture(capture_id),
		seq        bigint NOT NULL,
		path       bytea NOT NULL,
		name       bytea NOT NULL,
		parent_ino bigint NOT NULL,
		ino        bigint NOT NULL,
		node_type  smallint NOT NULL,
		mode       smallint NOT NULL,
		uid        integer NOT NULL,
		gid        integer NOT NULL,
		nlink      integer NOT NULL,
		length     bigint NOT NULL,
		mtime_ns   bigint NOT NULL,
		ctime_ns   bigint NOT NULL,
		link_target bytea,
		link_group text NOT NULL DEFAULT '',
		map_sha    text NOT NULL DEFAULT '',
		supported  boolean NOT NULL DEFAULT true,
		PRIMARY KEY (capture_id, seq)
	);
	CREATE INDEX IF NOT EXISTS file_capture_entry_ino
		ON file_capture_entry(capture_id, ino);
	CREATE TABLE IF NOT EXISTS file_capture_slice (
		capture_id text NOT NULL REFERENCES file_capture(capture_id),
		ino        bigint NOT NULL,
		indx       integer NOT NULL,
		seq        integer NOT NULL,
		slice_id   bigint NOT NULL,
		pos        bigint NOT NULL,
		size       bigint NOT NULL,
		off        bigint NOT NULL,
		len        bigint NOT NULL,
		PRIMARY KEY (capture_id, ino, indx, seq)
	);
`

// barrierAuthority is the scope's current durable authority recorded
// in file_freeze: while a barrier stands it is (owner, owner_epoch);
// after release the tombstone's (released_by, released_epoch) remains
// the latest authority so a superseded lineage can never re-assert.
// currentAuthority reads it inside tx — callers must hold the barrier
// advisory lock appropriate to their boundary.
func currentAuthority(ctx context.Context, tx pgx.Tx, scope string) (owner string, epoch int64, err error) {
	var o, rb string
	var oe, re int64
	var released bool
	err = tx.QueryRow(ctx, `
		SELECT owner, owner_epoch, released_at IS NOT NULL,
		       released_by, released_epoch
		  FROM file_freeze WHERE scope = $1`, scope).
		Scan(&o, &oe, &released, &rb, &re)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, fmt.Errorf("%w: no durable barrier authority for scope",
			ErrCaptureStale)
	}
	if err != nil {
		return "", 0, err
	}
	if released {
		return rb, re, nil
	}
	return o, oe, nil
}

// saveCaptureAuthorized admits a capture: under the scope's barrier
// advisory lock (exclusive — the same lock SetScopeFrozen takes) it
// verifies the caller's (owner, epoch) is the current durable
// authority, then inserts the manifest. The check and the grant commit
// atomically: a barrier assertion either lands first (stale capture
// refused) or waits behind this tx (capture admitted while its lineage
// legitimately held). No DB lock is held over storage or remote calls —
// this tx does pure row work.
func (s *Store) saveCaptureAuthorized(ctx context.Context, c *captureRow,
	entries []captureEntryRow, slices []captureSliceRow) error {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout*4)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, c.Scope); err != nil {
		return err
	}
	authOwner, authEpoch, err := currentAuthority(ctx, tx, c.Scope)
	if err != nil {
		return err
	}
	if authOwner != c.Owner || authEpoch != c.OwnerEpoch {
		return fmt.Errorf("%w: %s@%d is not the scope's current authority",
			ErrCaptureStale, c.Owner, c.OwnerEpoch)
	}
	fmtJSON, err := json.Marshal(c.Format)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO file_capture (capture_id, scope, owner, owner_epoch,
			volume, anchor_ino, scope_id, format, manifest_sha, status,
			entry_count, unsupported, created_at, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		c.CaptureID, c.Scope, c.Owner, c.OwnerEpoch, c.Volume,
		int64(c.AnchorIno), c.ScopeID, fmtJSON, c.ManifestSHA, c.Status,
		c.EntryCount, c.Unsupported, c.CreatedAt, c.ExpiresAt); err != nil {
		return err
	}
	// Batch insert entries and slices — a manifest can be thousands of
	// rows; per-row Exec would turn capture into a round-trip storm.
	eBatch := &pgx.Batch{}
	for _, e := range entries {
		eBatch.Queue(`
			INSERT INTO file_capture_entry (capture_id, seq, path, name,
				parent_ino, ino, node_type, mode, uid, gid, nlink, length,
				mtime_ns, ctime_ns, link_target, link_group, map_sha, supported)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
			e.CaptureID, e.Seq, e.Path, e.Name, int64(e.ParentIno), int64(e.Ino),
			int16(e.NodeType), int16(e.Mode), int32(e.UID), int32(e.GID),
			int32(e.Nlink), int64(e.Length), e.MtimeNS, e.CtimeNS,
			e.Link, e.LinkGroup, e.MapSHA, e.Supported)
	}
	sBatch := &pgx.Batch{}
	for _, sl := range slices {
		sBatch.Queue(`
			INSERT INTO file_capture_slice (capture_id, ino, indx, seq,
				slice_id, pos, size, off, len)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			sl.CaptureID, int64(sl.Ino), int32(sl.Indx), int32(sl.Seq),
			int64(sl.SliceID), int64(sl.Pos), int64(sl.Size), int64(sl.Off), int64(sl.Len))
	}
	if err := tx.SendBatch(ctx, eBatch).Close(); err != nil {
		return err
	}
	if err := tx.SendBatch(ctx, sBatch).Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) getCapture(ctx context.Context, id string) (*captureRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout)
	defer cancel()
	var c captureRow
	var fmtJSON []byte
	var anchor int64
	err := s.pool.QueryRow(ctx, `
		SELECT capture_id, scope, owner, owner_epoch, volume, anchor_ino,
		       scope_id, format, manifest_sha, status, entry_count,
		       unsupported, created_at, expires_at, released_at
		  FROM file_capture WHERE capture_id = $1`, id).
		Scan(&c.CaptureID, &c.Scope, &c.Owner, &c.OwnerEpoch, &c.Volume,
			&anchor, &c.ScopeID, &fmtJSON, &c.ManifestSHA, &c.Status,
			&c.EntryCount, &c.Unsupported, &c.CreatedAt, &c.ExpiresAt, &c.ReleasedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrCaptureNotFound
	}
	if err != nil {
		return nil, err
	}
	c.AnchorIno = uint64(anchor)
	if err := json.Unmarshal(fmtJSON, &c.Format); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) entryStream(ctx context.Context, id string,
	afterSeq, limit int64) ([]captureEntryRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT seq, path, name, parent_ino, ino, node_type, mode, uid, gid,
		       nlink, length, mtime_ns, ctime_ns, link_target, link_group,
		       map_sha, supported
		  FROM file_capture_entry
		 WHERE capture_id = $1 AND seq > $2
		 ORDER BY seq LIMIT $3`, id, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []captureEntryRow
	for rows.Next() {
		var e captureEntryRow
		var pino, ino int64
		var nt, mode int16
		var uid, gid, nlink int32
		var length int64
		if err := rows.Scan(&e.Seq, &e.Path, &e.Name, &pino, &ino, &nt, &mode,
			&uid, &gid, &nlink, &length, &e.MtimeNS, &e.CtimeNS,
			&e.Link, &e.LinkGroup, &e.MapSHA, &e.Supported); err != nil {
			return nil, err
		}
		e.CaptureID = id
		e.ParentIno, e.Ino = uint64(pino), uint64(ino)
		e.NodeType, e.Mode = uint8(nt), uint16(mode)
		e.UID, e.GID, e.Nlink = uint32(uid), uint32(gid), uint32(nlink)
		e.Length = uint64(length)
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) getEntry(ctx context.Context, id string, seq int64) (*captureEntryRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout)
	defer cancel()
	var e captureEntryRow
	var pino, ino int64
	var nt, mode int16
	var uid, gid, nlink int32
	var length int64
	err := s.pool.QueryRow(ctx, `
		SELECT seq, path, name, parent_ino, ino, node_type, mode, uid, gid,
		       nlink, length, mtime_ns, ctime_ns, link_target, link_group,
		       map_sha, supported
		  FROM file_capture_entry WHERE capture_id = $1 AND seq = $2`, id, seq).
		Scan(&e.Seq, &e.Path, &e.Name, &pino, &ino, &nt, &mode,
			&uid, &gid, &nlink, &length, &e.MtimeNS, &e.CtimeNS,
			&e.Link, &e.LinkGroup, &e.MapSHA, &e.Supported)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrCaptureNotFound
	}
	if err != nil {
		return nil, err
	}
	e.CaptureID = id
	e.ParentIno, e.Ino = uint64(pino), uint64(ino)
	e.NodeType, e.Mode = uint8(nt), uint16(mode)
	e.UID, e.GID, e.Nlink = uint32(uid), uint32(gid), uint32(nlink)
	e.Length = uint64(length)
	return &e, nil
}

func (s *Store) slicesFor(ctx context.Context, id string, ino uint64) ([]captureSliceRow, error) {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout)
	defer cancel()
	rows, err := s.pool.Query(ctx, `
		SELECT ino, indx, seq, slice_id, pos, size, off, len
		  FROM file_capture_slice
		 WHERE capture_id = $1 AND ino = $2
		 ORDER BY indx, seq`, id, int64(ino))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []captureSliceRow
	for rows.Next() {
		var sl captureSliceRow
		var inoI, indx, seq, sid, pos, size, off, ln int64
		if err := rows.Scan(&inoI, &indx, &seq, &sid, &pos, &size, &off, &ln); err != nil {
			return nil, err
		}
		sl.CaptureID = id
		sl.Ino = uint64(inoI)
		sl.Indx, sl.Seq = uint32(indx), int(seq)
		sl.SliceID, sl.Pos, sl.Size, sl.Off, sl.Len =
			uint64(sid), uint32(pos), uint32(size), uint32(off), uint32(ln)
		out = append(out, sl)
	}
	return out, rows.Err()
}

func (s *Store) setCaptureStatus(ctx context.Context, id, status string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `
		UPDATE file_capture SET status = $2,
			released_at = CASE WHEN $2='released' THEN now() ELSE released_at END
		 WHERE capture_id = $1`, id, status)
	return tag.RowsAffected() > 0, err
}

// assertCaptureLineage is the read-side lineage check: a shared
// advisory lock on the scope's barrier key serializes this verdict
// against an in-flight SetScopeFrozen (its exclusive lock), so the
// observed authority is either before or after that assertion — never
// mid-commit. (owner, epoch) must equal the CURRENT authority exactly:
// a request carrying an older generation's lineage gains no grant.
func (s *Store) assertCaptureLineage(ctx context.Context, scope, owner string, epoch int64) error {
	ctx, cancel := context.WithTimeout(ctx, s.dbTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))`, scope); err != nil {
		return err
	}
	authOwner, authEpoch, err := currentAuthority(ctx, tx, scope)
	if err != nil {
		return err
	}
	if authOwner != owner || authEpoch != epoch {
		return fmt.Errorf("%w: %s@%d is not the scope's current authority",
			ErrCaptureStale, owner, epoch)
	}
	return tx.Commit(ctx)
}
