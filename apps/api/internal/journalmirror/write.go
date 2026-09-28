package journalmirror

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// flushTimeout bounds one mirrored Sync. A Sync that cannot reach the
// database fails with a ReplicationError; the journals roll back locally.
const flushTimeout = 15 * time.Second

// resyncBlock bounds the memory of a whole-file resynchronization.
const resyncBlock = 1 << 20

type op struct {
	truncate bool
	size     int64
	offset   int64
	data     []byte
}

func (m *Mirror) resolve(path string) (attachedDir, string, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return attachedDir{}, "", "", err
	}
	dir := filepath.Dir(abs)
	m.mu.RLock()
	attached, ok := m.dirs[dir]
	m.mu.RUnlock()
	if !ok {
		return attachedDir{}, "", "", fmt.Errorf("journal mirror: %s is outside every attached directory", abs)
	}
	name := filepath.Base(abs)
	if !Mirrored(name) {
		return attachedDir{}, "", "", fmt.Errorf("journal mirror: %s is not a mirrored journal file", name)
	}
	return attached, name, dir, nil
}

// Wrap returns a handle whose Sync commits every write and truncation made
// through it to PostgreSQL after the local fsync. Read-only handles are
// returned unchanged.
func (m *Mirror) Wrap(path string, flag int, f File) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return f, nil
	}
	attached, name, dir, err := m.resolve(path)
	if err != nil {
		return nil, err
	}
	return &mirroredFile{File: f, m: m, dir: attached, localDir: dir, name: name,
		path: filepath.Join(dir, name), appendMode: flag&os.O_APPEND != 0, create: flag&os.O_CREATE != 0}, nil
}

// WrapAtomicWrite returns write (the journals' temp-file-and-rename
// replacement) with PostgreSQL as its acknowledgement point. The new content
// commits to PostgreSQL first and is then written locally, so a crash
// between the two leaves PostgreSQL ahead, which the next Attach restores.
func (m *Mirror) WrapAtomicWrite(write func(string, []byte, os.FileMode) error) func(string, []byte, os.FileMode) error {
	return func(path string, data []byte, perm os.FileMode) error {
		attached, name, dir, err := m.resolve(path)
		if err != nil {
			return err
		}
		gen, err := m.flush(context.Background(), attached, name, []op{{truncate: true}, {offset: 0, data: data}})
		if err != nil {
			m.markDiverged(attached.logical, name, 0)
			return err
		}
		if err := write(path, data, perm); err != nil {
			// PostgreSQL holds content the caller is told failed; the local
			// file is reconciled to PostgreSQL at the next Attach, or
			// replaces it at the next Sync of a handle.
			m.markDiverged(attached.logical, name, 0)
			return err
		}
		if err := writeAcked(dir, name, attached.lineage, gen); err != nil {
			m.markDiverged(attached.logical, name, 0)
			return fmt.Errorf("journal mirror: record acknowledged generation of %s: %w", name, err)
		}
		m.clearDiverged(attached.logical, name)
		return nil
	}
}

const fencedSQLState = "SJ001"

// flush commits ops to one mirrored file in a single pipelined round trip:
// the owner check, the file's operations and its generation bump run as one
// implicit transaction, and only the written bytes travel.
func (m *Mirror) flush(ctx context.Context, dir attachedDir, name string, ops []op) (int64, error) {
	if err := m.lostError(); err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	batch := &pgx.Batch{}
	batch.Queue(`SELECT api_journal_mirror_check_owner($1)`, m.epoch)
	batch.Queue(`INSERT INTO api_journal_mirror_files (dir, name, size) VALUES ($1, $2, 0) ON CONFLICT (dir, name) DO NOTHING`, dir.logical, name)
	for _, o := range ops {
		if o.truncate {
			batch.Queue(`SELECT api_journal_mirror_truncate($1, $2, $3)`, dir.logical, name, o.size)
		} else if len(o.data) > 0 {
			batch.Queue(`SELECT api_journal_mirror_write($1, $2, $3, $4)`, dir.logical, name, o.offset, o.data)
		}
	}
	var gen int64
	batch.Queue(`UPDATE api_journal_mirror_files SET gen = gen + 1, updated_at = now() WHERE dir = $1 AND name = $2 RETURNING gen`,
		dir.logical, name).QueryRow(func(row pgx.Row) error { return row.Scan(&gen) })
	if err := m.pool.SendBatch(ctx, batch).Close(); err != nil {
		return 0, m.flushError(err)
	}
	if m.flushHook != nil {
		if err := m.flushHook(dir.logical, name); err != nil {
			return 0, &ReplicationError{Err: err}
		}
	}
	return gen, nil
}

func (m *Mirror) flushError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == fencedSQLState {
		m.lose(ErrFenced)
		return ErrFenced
	}
	if lost := m.lostError(); lost != nil {
		return lost
	}
	return &ReplicationError{Err: err}
}

// resync makes PostgreSQL's copy of a file equal the local file when the
// operations since the last acknowledged Sync are uncertain. Until then both
// copies were equal, and every change since touched only offsets at or after
// from; so PostgreSQL's bytes before from are the local ones, and only the
// local tail is sent, in bounded blocks inside one transaction.
func (m *Mirror) resync(ctx context.Context, dir attachedDir, name, path string, from int64) (int64, error) {
	if err := m.lostError(); err != nil {
		return 0, err
	}
	local, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("journal mirror: read %s for resync: %w", name, err)
	}
	defer local.Close()
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return 0, m.flushError(err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT api_journal_mirror_check_owner($1)`, m.epoch); err != nil {
		return 0, m.flushError(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO api_journal_mirror_files (dir, name, size) VALUES ($1, $2, 0) ON CONFLICT (dir, name) DO NOTHING`, dir.logical, name); err != nil {
		return 0, m.flushError(err)
	}
	var remoteSize int64
	if err := tx.QueryRow(ctx, `SELECT size FROM api_journal_mirror_files WHERE dir = $1 AND name = $2 FOR UPDATE`, dir.logical, name).Scan(&remoteSize); err != nil {
		return 0, m.flushError(err)
	}
	start := min(from, remoteSize)
	if _, err := tx.Exec(ctx, `SELECT api_journal_mirror_truncate($1, $2, $3)`, dir.logical, name, start); err != nil {
		return 0, m.flushError(err)
	}
	if _, err := local.Seek(start, io.SeekStart); err != nil {
		return 0, err
	}
	if err := uploadFile(ctx, tx, dir.logical, name, start, local); err != nil {
		return 0, m.flushError(err)
	}
	var gen int64
	if err := tx.QueryRow(ctx, `UPDATE api_journal_mirror_files SET gen = gen + 1, updated_at = now() WHERE dir = $1 AND name = $2 RETURNING gen`,
		dir.logical, name).Scan(&gen); err != nil {
		return 0, m.flushError(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, m.flushError(err)
	}
	return gen, nil
}

// uploadFile writes r to a mirrored file from offset on, in bounded blocks.
func uploadFile(ctx context.Context, tx pgx.Tx, dir, name string, offset int64, r io.Reader) error {
	block := make([]byte, resyncBlock)
	for {
		n, err := io.ReadFull(r, block)
		if n > 0 {
			if _, execErr := tx.Exec(ctx, `SELECT api_journal_mirror_write($1, $2, $3, $4)`, dir, name, offset, block[:n]); execErr != nil {
				return execErr
			}
			offset += int64(n)
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// mirroredFile queues positional writes and truncations and commits them to
// PostgreSQL in order on Sync. The journals call Sync while they hold the
// file's flock, so commits reach PostgreSQL in the same order as the local
// writes.
//
// If a commit fails or its outcome is unknown, or a handle closes with writes
// it never synced, the operation queue no longer describes the difference
// between the two copies. The file is then marked diverged with the lowest
// offset changed since its last acknowledged Sync, and its next Sync (again
// under the journal's lock) replaces PostgreSQL's copy from that offset on
// with the local bytes.
type mirroredFile struct {
	File
	m          *Mirror
	dir        attachedDir
	localDir   string
	name       string
	path       string
	appendMode bool
	// create is set until the first Sync of a handle opened with O_CREATE,
	// so a synced empty file exists in the mirror too.
	create bool

	mu      sync.Mutex
	pending []op
}

func (f *mirroredFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var offset int64
	var err error
	if f.appendMode {
		offset, err = f.File.Seek(0, io.SeekEnd)
	} else {
		offset, err = f.File.Seek(0, io.SeekCurrent)
	}
	if err != nil {
		return 0, err
	}
	n, err := f.File.Write(p)
	if n > 0 {
		f.pending = append(f.pending, op{offset: offset, data: append([]byte(nil), p[:n]...)})
	}
	return n, err
}

func (f *mirroredFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.File.Truncate(size); err != nil {
		f.m.markDiverged(f.dir.logical, f.name, 0)
		return err
	}
	f.pending = append(f.pending, op{truncate: true, size: size})
	return nil
}

// Sync fsyncs the local file, then commits to PostgreSQL. A failed local
// fsync is returned as is; a failed or ambiguous commit is a
// *ReplicationError; a lost ownership is ErrFenced or ErrLeaseLost.
func (f *mirroredFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.File.Sync(); err != nil {
		return err
	}
	var gen int64
	var err error
	from := minOffset(f.pending)
	switch {
	case f.m.diverged(f.dir.logical, f.name):
		f.m.markDiverged(f.dir.logical, f.name, from)
		gen, err = f.m.resync(context.Background(), f.dir, f.name, f.path, f.m.divergedFrom(f.dir.logical, f.name))
	case len(f.pending) == 0 && !f.create:
		return f.m.lostError()
	default:
		gen, err = f.m.flush(context.Background(), f.dir, f.name, f.pending)
	}
	f.pending, f.create = nil, false
	if err != nil {
		f.m.markDiverged(f.dir.logical, f.name, from)
		return err
	}
	if err := writeAcked(f.localDir, f.name, f.dir.lineage, gen); err != nil {
		f.m.markDiverged(f.dir.logical, f.name, from)
		return fmt.Errorf("journal mirror: record acknowledged generation of %s: %w", f.name, err)
	}
	f.m.clearDiverged(f.dir.logical, f.name)
	return nil
}

// Close does not commit: it may run after the journal released the file's
// lock. Unsynced writes were never acknowledged, so the file is marked
// diverged and reconciled by its next Sync.
func (f *mirroredFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) > 0 {
		f.m.markDiverged(f.dir.logical, f.name, minOffset(f.pending))
		f.pending = nil
	}
	return f.File.Close()
}

// minOffset is the lowest file offset that ops change; math.MaxInt64 if none.
func minOffset(ops []op) int64 {
	from := int64(math.MaxInt64)
	for _, o := range ops {
		at := o.offset
		if o.truncate {
			at = o.size
		}
		from = min(from, at)
	}
	return from
}

func (m *Mirror) diverged(dir, name string) bool {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	_, ok := m.divergedFiles[dir+"/"+name]
	return ok
}

// divergedFrom is the lowest offset changed since the file's last
// acknowledged Sync.
func (m *Mirror) divergedFrom(dir, name string) int64 {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	return m.divergedFiles[dir+"/"+name]
}

func (m *Mirror) markDiverged(dir, name string, from int64) {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	if m.divergedFiles == nil {
		m.divergedFiles = map[string]int64{}
	}
	key := dir + "/" + name
	if current, ok := m.divergedFiles[key]; !ok || from < current {
		m.divergedFiles[key] = from
	}
}

func (m *Mirror) clearDiverged(dir, name string) {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	delete(m.divergedFiles, dir+"/"+name)
}
