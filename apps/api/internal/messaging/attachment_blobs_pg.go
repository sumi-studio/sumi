package messaging

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresAttachments keeps attachment bytes in PostgreSQL
// (messaging_attachment_blobs). It serves hosts whose local disk does not
// survive replacement. Its semantics follow DiskAttachments: a staging blob
// and a published blob can exist for one id; Commit publishes with
// no-replace semantics against live blobs; Sweep drops abandoned staging
// blobs and reports old published ids for reconciliation.
//
// Bytes are stored in fixed-size chunks; Open reads one chunk at a time, so
// a download never holds a whole attachment in memory.
type PostgresAttachments struct {
	pool *pgxpool.Pool
}

const postgresAttachmentChunk = 256 << 10

// NewPostgresAttachments returns a blob store over pool. The migrations that
// create messaging_attachment_blobs must already be applied.
func NewPostgresAttachments(pool *pgxpool.Pool) (*PostgresAttachments, error) {
	if pool == nil {
		return nil, errors.New("postgres attachments require a database")
	}
	return &PostgresAttachments{pool: pool}, nil
}

func (p *PostgresAttachments) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Minute)
}

func (p *PostgresAttachments) Stage(id string, r io.Reader, expected int64) (StagedBlob, error) {
	if expected <= 0 {
		return StagedBlob{}, ErrAttachmentEmpty
	}
	if expected > MaxAttachmentBytes {
		return StagedBlob{}, ErrAttachmentTooLarge
	}
	if !validAttachmentID(id) {
		return StagedBlob{}, ErrAttachmentNotFound
	}
	ctx, cancel := p.ctx()
	defer cancel()
	// Claiming the staging row first gives the disk backend's O_EXCL answer
	// to a concurrent upload of the same id. A crash after the claim leaves an
	// incomplete staging blob, which StagingExists reports and Sweep removes,
	// exactly like a leftover staging file.
	var blobID int64
	err := p.pool.QueryRow(ctx, `
		INSERT INTO messaging_attachment_blobs (attachment_id, kind) VALUES ($1, 'staging')
		ON CONFLICT (attachment_id, kind) DO NOTHING RETURNING blob_id`, id).Scan(&blobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return StagedBlob{}, ErrAttachmentUploadInProgress
	}
	if err != nil {
		return StagedBlob{}, fmt.Errorf("claim attachment staging blob: %w", err)
	}
	staged := StagedBlob{ID: id, tempPath: fmt.Sprintf("postgres:%d", blobID)}
	committed := false
	defer func() {
		if !committed {
			_, _ = p.pool.Exec(context.Background(), `DELETE FROM messaging_attachment_blobs WHERE blob_id = $1`, blobID)
		}
	}()

	digest := sha256.New()
	head := make([]byte, 0, 512)
	limited := io.LimitReader(r, expected+1)
	buffer := make([]byte, postgresAttachmentChunk)
	var size int64
	chunk := 0
	for {
		read, readErr := io.ReadFull(limited, buffer)
		if read > 0 {
			data := buffer[:read]
			if len(head) < 512 {
				head = append(head, data[:min(512-len(head), len(data))]...)
			}
			size += int64(read)
			if size > expected {
				return StagedBlob{}, ErrAttachmentTooLarge
			}
			_, _ = digest.Write(data)
			// Each chunk renews changed_at, as each write renews a staging
			// file's mtime, so Sweep never takes an upload that is still
			// streaming. A staging row Sweep already took stops the upload.
			tag, err := p.pool.Exec(ctx, `
				WITH touched AS (
					UPDATE messaging_attachment_blobs SET changed_at = now()
					WHERE blob_id = $1 AND kind = 'staging' RETURNING blob_id)
				INSERT INTO messaging_attachment_blob_chunks (blob_id, chunk, data)
				SELECT blob_id, $2, $3 FROM touched`, blobID, chunk, data)
			if err != nil {
				return StagedBlob{}, fmt.Errorf("write attachment staging blob: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return StagedBlob{}, errors.New("attachment staging blob was swept during upload")
			}
			chunk++
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return StagedBlob{}, readErr
		}
	}
	if size != expected {
		return StagedBlob{}, ErrAttachmentSizeMismatch
	}
	tag, err := p.pool.Exec(ctx, `UPDATE messaging_attachment_blobs SET size = $2, complete = true, changed_at = now() WHERE blob_id = $1 AND kind = 'staging'`,
		blobID, size)
	if err != nil {
		return StagedBlob{}, fmt.Errorf("complete attachment staging blob: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return StagedBlob{}, errors.New("attachment staging blob was swept during upload")
	}
	staged.Size = size
	staged.SHA256 = digest.Sum(nil)
	staged.Head = head
	committed = true
	return staged, nil
}

func (p *PostgresAttachments) stagedBlobID(staged StagedBlob) (int64, error) {
	var blobID int64
	if _, err := fmt.Sscanf(staged.tempPath, "postgres:%d", &blobID); err != nil || blobID <= 0 {
		return 0, errors.New("staged attachment has no staging blob")
	}
	return blobID, nil
}

func (p *PostgresAttachments) Commit(staged StagedBlob) error {
	blobID, err := p.stagedBlobID(staged)
	if err != nil {
		return err
	}
	ctx, cancel := p.ctx()
	defer cancel()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var attachmentID string
	var complete bool
	if err := tx.QueryRow(ctx, `SELECT attachment_id, complete FROM messaging_attachment_blobs WHERE blob_id = $1 AND kind = 'staging' FOR UPDATE`,
		blobID).Scan(&attachmentID, &complete); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.New("staged attachment blob is gone")
		}
		return err
	}
	if attachmentID != staged.ID || !complete {
		return errors.New("staged attachment blob does not match its upload")
	}
	// The caller has proven any published blob for this id ownerless (its
	// metadata never committed); only then does publishing replace it.
	if _, err := tx.Exec(ctx, `DELETE FROM messaging_attachment_blobs WHERE attachment_id = $1 AND kind = 'published'`, attachmentID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE messaging_attachment_blobs SET kind = 'published', changed_at = now() WHERE blob_id = $1`, blobID); err != nil {
		return fmt.Errorf("publish attachment blob: %w", err)
	}
	return tx.Commit(ctx)
}

func (p *PostgresAttachments) Discard(staged StagedBlob) error {
	if staged.tempPath == "" {
		return nil
	}
	blobID, err := p.stagedBlobID(staged)
	if err != nil {
		return err
	}
	ctx, cancel := p.ctx()
	defer cancel()
	_, err = p.pool.Exec(ctx, `DELETE FROM messaging_attachment_blobs WHERE blob_id = $1 AND kind = 'staging'`, blobID)
	return err
}

func (p *PostgresAttachments) StagingExists(id string) (bool, error) {
	ctx, cancel := p.ctx()
	defer cancel()
	var exists bool
	err := p.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM messaging_attachment_blobs WHERE attachment_id = $1 AND kind = 'staging')`, id).Scan(&exists)
	return exists, err
}

func (p *PostgresAttachments) DiscardStaging(id string) error {
	ctx, cancel := p.ctx()
	defer cancel()
	_, err := p.pool.Exec(ctx, `DELETE FROM messaging_attachment_blobs WHERE attachment_id = $1 AND kind = 'staging'`, id)
	return err
}

// postgresBlobReader serves one published blob chunk by chunk. A blob's
// chunks never change after Stage completes, so reads by blob_id are
// consistent; a blob removed mid-download ends the read with an error.
type postgresBlobReader struct {
	pool   *pgxpool.Pool
	blobID int64
	size   int64
	offset int64
	chunk  int64
	data   []byte
}

func (r *postgresBlobReader) Read(buffer []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}
	index := r.offset / postgresAttachmentChunk
	if r.data == nil || r.chunk != index {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		var data []byte
		err := r.pool.QueryRow(ctx, `SELECT data FROM messaging_attachment_blob_chunks WHERE blob_id = $1 AND chunk = $2`,
			r.blobID, index).Scan(&data)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, errors.New("attachment blob was removed during read")
		}
		if err != nil {
			return 0, fmt.Errorf("read attachment blob: %w", err)
		}
		r.chunk, r.data = index, data
	}
	within := r.offset - r.chunk*postgresAttachmentChunk
	if within >= int64(len(r.data)) {
		return 0, fmt.Errorf("attachment blob chunk %d is short", r.chunk)
	}
	read := copy(buffer, r.data[within:])
	r.offset += int64(read)
	return read, nil
}

func (r *postgresBlobReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.offset
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative position")
	}
	r.offset = offset
	return offset, nil
}

func (r *postgresBlobReader) Close() error {
	r.data = nil
	return nil
}

func (p *PostgresAttachments) Open(id string) (io.ReadSeekCloser, error) {
	if !validAttachmentID(id) {
		return nil, ErrAttachmentNotFound
	}
	ctx, cancel := p.ctx()
	defer cancel()
	var blobID, size int64
	err := p.pool.QueryRow(ctx, `SELECT blob_id, size FROM messaging_attachment_blobs WHERE attachment_id = $1 AND kind = 'published'`, id).Scan(&blobID, &size)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAttachmentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("open attachment: %w", err)
	}
	return &postgresBlobReader{pool: p.pool, blobID: blobID, size: size}, nil
}

func (p *PostgresAttachments) Remove(id string) error {
	ctx, cancel := p.ctx()
	defer cancel()
	_, err := p.pool.Exec(ctx, `DELETE FROM messaging_attachment_blobs WHERE attachment_id = $1 AND kind = 'published'`, id)
	return err
}

func (p *PostgresAttachments) Sweep(cutoff time.Time) ([]string, error) {
	ctx, cancel := p.ctx()
	defer cancel()
	// A staging blob this old is an upload that never finalized: nothing can
	// name it, so nothing can read it.
	if _, err := p.pool.Exec(ctx, `DELETE FROM messaging_attachment_blobs WHERE kind = 'staging' AND changed_at < $1`, cutoff); err != nil {
		return nil, fmt.Errorf("sweep attachment staging blobs: %w", err)
	}
	rows, err := p.pool.Query(ctx, `SELECT attachment_id FROM messaging_attachment_blobs WHERE kind = 'published' AND changed_at < $1 ORDER BY attachment_id`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("sweep attachment blobs: %w", err)
	}
	defer rows.Close()
	var older []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if validAttachmentID(id) {
			older = append(older, id)
		}
	}
	return older, rows.Err()
}

func (p *PostgresAttachments) PublishedBefore(id string, cutoff time.Time) (bool, error) {
	ctx, cancel := p.ctx()
	defer cancel()
	var before bool
	err := p.pool.QueryRow(ctx, `SELECT changed_at < $2 FROM messaging_attachment_blobs WHERE attachment_id = $1 AND kind = 'published'`, id, cutoff).Scan(&before)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return before, err
}
