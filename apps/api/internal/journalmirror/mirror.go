// Package journalmirror keeps the API's file journals durable in PostgreSQL
// when the host disk may be discarded.
//
// The Direct Chat command logs, the browser event logs and the browser-session
// revocation list are files that the agentevents package appends to, fsyncs and
// sometimes rolls back by truncation. Their correctness is built on POSIX file
// semantics (flock, positional writes, fsync as the acknowledgement point), and
// that code stays unchanged. This package adds one rule underneath it: a
// file's Sync succeeds only after the same bytes are committed to PostgreSQL.
// On start, the mirror writes PostgreSQL's copy back into the (possibly empty)
// directory before the journals open it. A replaced host therefore resumes
// with every write the previous host acknowledged.
//
// Exactly one API process may own the mirror. Acquire increments an owner
// epoch, and every flush re-checks that epoch inside its transaction; a flush
// from a superseded process fails with ErrFenced and changes nothing.
package journalmirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ChunkSize is the storage unit for file bytes in PostgreSQL.
const ChunkSize = 64 << 10

// ErrFenced reports that another API process acquired the mirror after this
// one. The caller must stop serving: its local files can no longer be made
// durable.
var ErrFenced = errors.New("journal mirror is owned by a newer API process")

// ErrConflict reports local files that neither match PostgreSQL's copy nor
// extend it with an unacknowledged tail.
var ErrConflict = errors.New("local journal files conflict with the PostgreSQL mirror")

// File is the handle shape used by the agentevents journals. It is an alias
// of an unnamed interface, identical to agentevents.JournalFile.
type File = interface {
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
	Truncate(size int64) error
	Close() error
	Fd() uintptr
}

var (
	dirNameRe  = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	fileNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._=-]{0,254}$`)
)

// Mirrored reports whether a directory entry carries journal state. Lock files
// only carry flock state, and temporary files belong to an atomic write in
// progress; both are recreated by the journals themselves.
func Mirrored(name string) bool {
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".lock") || strings.HasSuffix(name, ".tmp") {
		return false
	}
	return fileNameRe.MatchString(name)
}

// Mirror is one API process's ownership of the PostgreSQL journal mirror.
type Mirror struct {
	pool   *pgxpool.Pool
	holder string
	epoch  int64

	mu   sync.RWMutex
	dirs map[string]string // absolute directory -> logical name

	fenced   atomic.Bool
	onFenced func(error)
	fenceMu  sync.Once

	divergedMu    sync.Mutex
	divergedFiles map[string]struct{}
}

// Acquire takes ownership of the mirror for this process. holder is a
// human-readable identity recorded for operators (host and process).
// onFenced, if not nil, is called once when a flush finds a newer owner.
func Acquire(ctx context.Context, pool *pgxpool.Pool, holder string, onFenced func(error)) (*Mirror, error) {
	if pool == nil {
		return nil, errors.New("journal mirror requires a database")
	}
	holder = strings.TrimSpace(holder)
	if holder == "" {
		holder = "unknown"
	}
	var epoch int64
	err := pool.QueryRow(ctx, `
		INSERT INTO api_journal_mirror_owner (singleton, epoch, holder, acquired_at)
		VALUES (true, 1, $1, now())
		ON CONFLICT (singleton) DO UPDATE
		SET epoch = api_journal_mirror_owner.epoch + 1, holder = EXCLUDED.holder, acquired_at = now()
		RETURNING epoch`, holder).Scan(&epoch)
	if err != nil {
		return nil, fmt.Errorf("acquire journal mirror: %w", err)
	}
	return &Mirror{pool: pool, holder: holder, epoch: epoch, dirs: map[string]string{}, onFenced: onFenced}, nil
}

// Epoch is this process's owner epoch.
func (m *Mirror) Epoch() int64 { return m.epoch }

// Fenced reports whether a newer process has taken the mirror.
func (m *Mirror) Fenced() bool { return m.fenced.Load() }

// AttachReport describes what Attach did to a directory.
type AttachReport struct {
	Dir      string
	Seeded   bool // the mirror adopted the local files as its first copy
	Files    int  // mirrored files after attach
	Bytes    int64
	Restored []string // files written from PostgreSQL
	// Trimmed lists local files that extended PostgreSQL's copy with bytes
	// that were never acknowledged; they were cut back to the mirrored size.
	Trimmed  map[string]int64
	Duration time.Duration
}

// Attach binds a local directory to a logical mirror name and reconciles it
// before any journal opens the directory.
//
// The first attach of a logical name adopts whatever the directory holds
// (possibly nothing) as the mirror's initial copy: that is how an existing
// host's journals move into PostgreSQL. Every later attach restores
// PostgreSQL's copy into the directory. A local file that equals the mirrored
// bytes is kept; one that only adds bytes after them is trimmed, because the
// mirror is the acknowledgement point and those bytes were never reported
// durable. Any other difference is ErrConflict and nothing is changed.
func (m *Mirror) Attach(ctx context.Context, logical, dir string) (AttachReport, error) {
	started := time.Now()
	report := AttachReport{Dir: logical, Trimmed: map[string]int64{}}
	if !dirNameRe.MatchString(logical) {
		return report, fmt.Errorf("invalid journal mirror name %q", logical)
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return report, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return report, fmt.Errorf("create journal directory: %w", err)
	}
	m.mu.Lock()
	for existingDir, existingName := range m.dirs {
		if existingDir == abs || existingName == logical {
			m.mu.Unlock()
			return report, fmt.Errorf("journal mirror %q or directory %s is already attached", logical, abs)
		}
	}
	m.mu.Unlock()

	local, err := readLocal(abs)
	if err != nil {
		return report, err
	}

	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return report, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := m.checkOwner(ctx, tx); err != nil {
		return report, err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM api_journal_mirror_dirs WHERE dir = $1)`, logical).Scan(&exists); err != nil {
		return report, err
	}
	if !exists {
		var total int64
		for _, content := range local {
			total += int64(len(content))
		}
		if _, err := tx.Exec(ctx, `INSERT INTO api_journal_mirror_dirs (dir, seeded_by, seeded_files, seeded_bytes) VALUES ($1, $2, $3, $4)`,
			logical, m.holder, len(local), total); err != nil {
			return report, err
		}
		for _, name := range sortedNames(local) {
			if err := writeFileAt(ctx, tx, logical, name, 0, local[name], true); err != nil {
				return report, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return report, err
		}
		report.Seeded, report.Files, report.Bytes = true, len(local), total
	} else {
		remote, err := readRemote(ctx, tx, logical)
		if err != nil {
			return report, err
		}
		if err := tx.Commit(ctx); err != nil {
			return report, err
		}
		var conflicts []string
		for name, content := range local {
			want := remote[name] // absent means the mirrored copy is empty
			if bytes.Equal(content, want) {
				continue
			}
			if !bytes.HasPrefix(content, want) {
				conflicts = append(conflicts, name)
			}
		}
		if len(conflicts) > 0 {
			sort.Strings(conflicts)
			return report, fmt.Errorf("%w: %s in %s differ from mirror %q; move them aside or restore the matching database", ErrConflict, strings.Join(conflicts, ", "), abs, logical)
		}
		for _, name := range sortedNames(local) {
			content, want := local[name], remote[name]
			if len(content) > len(want) {
				if err := os.Truncate(filepath.Join(abs, name), int64(len(want))); err != nil {
					return report, fmt.Errorf("trim unacknowledged tail of %s: %w", name, err)
				}
				report.Trimmed[name] = int64(len(content) - len(want))
			}
		}
		for _, name := range sortedNames(remote) {
			if _, ok := local[name]; ok {
				continue
			}
			if err := writeLocalFile(filepath.Join(abs, name), remote[name]); err != nil {
				return report, err
			}
			report.Restored = append(report.Restored, name)
		}
		if err := syncDir(abs); err != nil {
			return report, err
		}
		report.Files = len(remote)
		for _, content := range remote {
			report.Bytes += int64(len(content))
		}
	}
	m.mu.Lock()
	m.dirs[abs] = logical
	m.mu.Unlock()
	report.Duration = time.Since(started)
	return report, nil
}

func (m *Mirror) resolve(path string) (string, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	m.mu.RLock()
	logical, ok := m.dirs[filepath.Dir(abs)]
	m.mu.RUnlock()
	if !ok {
		return "", "", fmt.Errorf("journal mirror: %s is outside every attached directory", abs)
	}
	name := filepath.Base(abs)
	if !Mirrored(name) {
		return "", "", fmt.Errorf("journal mirror: %s is not a mirrored journal file", name)
	}
	return logical, name, nil
}

// Wrap returns a handle whose Sync commits every write and truncation made
// through it to PostgreSQL after the local fsync. Read-only handles are
// returned unchanged.
func (m *Mirror) Wrap(path string, flag int, f File) (File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return f, nil
	}
	logical, name, err := m.resolve(path)
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	return &mirroredFile{File: f, m: m, dir: logical, name: name, path: abs, appendMode: flag&os.O_APPEND != 0}, nil
}

// ReplaceFile records a whole-file replacement (the journals' atomic
// temp-file-and-rename writes) after the local write succeeded.
func (m *Mirror) ReplaceFile(ctx context.Context, path string, data []byte) error {
	logical, name, err := m.resolve(path)
	if err != nil {
		return err
	}
	return m.flush(ctx, logical, name, []op{{truncate: true, size: 0}, {offset: 0, data: data}})
}

// WrapAtomicWrite returns write with its successful results mirrored.
func (m *Mirror) WrapAtomicWrite(write func(string, []byte, os.FileMode) error) func(string, []byte, os.FileMode) error {
	return func(path string, data []byte, perm os.FileMode) error {
		if err := write(path, data, perm); err != nil {
			return err
		}
		return m.ReplaceFile(context.Background(), path, data)
	}
}

type op struct {
	truncate bool
	size     int64
	offset   int64
	data     []byte
}

func (m *Mirror) checkOwner(ctx context.Context, tx pgx.Tx) error {
	if m.fenced.Load() {
		return ErrFenced
	}
	var epoch int64
	err := tx.QueryRow(ctx, `SELECT epoch FROM api_journal_mirror_owner WHERE singleton FOR SHARE`).Scan(&epoch)
	if err != nil {
		return fmt.Errorf("read journal mirror owner: %w", err)
	}
	if epoch != m.epoch {
		m.fence()
		return ErrFenced
	}
	return nil
}

func (m *Mirror) fence() {
	m.fenced.Store(true)
	m.fenceMu.Do(func() {
		if m.onFenced != nil {
			go m.onFenced(ErrFenced)
		}
	})
}

// flushTimeout bounds one mirrored Sync. A Sync that cannot reach the
// database fails like a failed fsync: the journals roll back or poison.
const flushTimeout = 15 * time.Second

func (m *Mirror) flush(ctx context.Context, dir, name string, ops []op) error {
	if len(ops) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("journal mirror: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := m.checkOwner(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO api_journal_mirror_files (dir, name, size) VALUES ($1, $2, 0) ON CONFLICT (dir, name) DO NOTHING`, dir, name); err != nil {
		return fmt.Errorf("journal mirror: %w", err)
	}
	for _, o := range ops {
		if o.truncate {
			err = truncateFile(ctx, tx, dir, name, o.size)
		} else {
			err = writeFileAt(ctx, tx, dir, name, o.offset, o.data, false)
		}
		if err != nil {
			return fmt.Errorf("journal mirror %s/%s: %w", dir, name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("journal mirror commit: %w", err)
	}
	return nil
}

// mirroredFile queues positional writes and truncations and commits them to
// PostgreSQL in order on Sync. The journals call Sync while they hold the
// file's flock, so commits reach PostgreSQL in the same order as the local
// writes.
//
// If a commit fails, or a handle closes with writes it never synced, the
// operation queue no longer describes the difference between the two copies.
// The file is then marked diverged, and its next successful Sync (again under
// the journal's lock) replaces PostgreSQL's copy with the whole local file.
type mirroredFile struct {
	File
	m          *Mirror
	dir, name  string
	path       string
	appendMode bool

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
		f.m.markDiverged(f.dir, f.name)
		return err
	}
	f.pending = append(f.pending, op{truncate: true, size: size})
	return nil
}

func (f *mirroredFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.File.Sync(); err != nil {
		return err
	}
	ops := f.pending
	if f.m.diverged(f.dir, f.name) {
		content, err := os.ReadFile(f.path)
		if err != nil {
			return fmt.Errorf("journal mirror: read %s for resync: %w", f.name, err)
		}
		ops = []op{{truncate: true, size: 0}, {offset: 0, data: content}}
	} else if len(ops) == 0 {
		if f.m.fenced.Load() {
			return ErrFenced
		}
		return nil
	}
	if err := f.m.flush(context.Background(), f.dir, f.name, ops); err != nil {
		f.m.markDiverged(f.dir, f.name)
		f.pending = nil
		return err
	}
	f.m.clearDiverged(f.dir, f.name)
	f.pending = nil
	return nil
}

// Close does not commit: it may run after the journal released the file's
// lock. Unsynced writes were never acknowledged, so the file is marked
// diverged and reconciled by its next Sync.
func (f *mirroredFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) > 0 {
		f.m.markDiverged(f.dir, f.name)
		f.pending = nil
	}
	return f.File.Close()
}

func (m *Mirror) diverged(dir, name string) bool {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	_, ok := m.divergedFiles[dir+"/"+name]
	return ok
}

func (m *Mirror) markDiverged(dir, name string) {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	if m.divergedFiles == nil {
		m.divergedFiles = map[string]struct{}{}
	}
	m.divergedFiles[dir+"/"+name] = struct{}{}
}

func (m *Mirror) clearDiverged(dir, name string) {
	m.divergedMu.Lock()
	defer m.divergedMu.Unlock()
	delete(m.divergedFiles, dir+"/"+name)
}

func writeFileAt(ctx context.Context, tx pgx.Tx, dir, name string, offset int64, data []byte, create bool) error {
	if create {
		if _, err := tx.Exec(ctx, `INSERT INTO api_journal_mirror_files (dir, name, size) VALUES ($1, $2, 0)`, dir, name); err != nil {
			return err
		}
	}
	var size int64
	if err := tx.QueryRow(ctx, `SELECT size FROM api_journal_mirror_files WHERE dir = $1 AND name = $2 FOR UPDATE`, dir, name).Scan(&size); err != nil {
		return err
	}
	if offset > size {
		// A write past the end leaves a hole of zero bytes, as a file would.
		if err := truncateFile(ctx, tx, dir, name, offset); err != nil {
			return err
		}
		size = offset
	}
	if len(data) == 0 {
		return nil
	}
	end := offset + int64(len(data))
	first, last := offset/ChunkSize, (end-1)/ChunkSize
	existing := map[int64][]byte{}
	rows, err := tx.Query(ctx, `SELECT chunk, data FROM api_journal_mirror_chunks WHERE dir = $1 AND name = $2 AND chunk BETWEEN $3 AND $4`, dir, name, first, last)
	if err != nil {
		return err
	}
	for rows.Next() {
		var index int64
		var chunk []byte
		if err := rows.Scan(&index, &chunk); err != nil {
			rows.Close()
			return err
		}
		existing[index] = chunk
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for index := first; index <= last; index++ {
		chunkStart := index * ChunkSize
		chunk := existing[index]
		lo := max(offset, chunkStart) - chunkStart
		hi := min(end, chunkStart+ChunkSize) - chunkStart
		if int64(len(chunk)) < hi {
			grown := make([]byte, hi)
			copy(grown, chunk)
			chunk = grown
		}
		copy(chunk[lo:hi], data[chunkStart+lo-offset:chunkStart+hi-offset])
		batch.Queue(`INSERT INTO api_journal_mirror_chunks (dir, name, chunk, data) VALUES ($1, $2, $3, $4)
			ON CONFLICT (dir, name, chunk) DO UPDATE SET data = EXCLUDED.data`, dir, name, index, chunk)
	}
	batch.Queue(`UPDATE api_journal_mirror_files SET size = $3, updated_at = now() WHERE dir = $1 AND name = $2`, dir, name, max(size, end))
	return tx.SendBatch(ctx, batch).Close()
}

func truncateFile(ctx context.Context, tx pgx.Tx, dir, name string, size int64) error {
	var current int64
	if err := tx.QueryRow(ctx, `SELECT size FROM api_journal_mirror_files WHERE dir = $1 AND name = $2 FOR UPDATE`, dir, name).Scan(&current); err != nil {
		return err
	}
	if size < 0 {
		return fmt.Errorf("negative truncate size %d", size)
	}
	switch {
	case size < current:
		keep := (size + ChunkSize - 1) / ChunkSize // chunks wholly or partly below size
		if _, err := tx.Exec(ctx, `DELETE FROM api_journal_mirror_chunks WHERE dir = $1 AND name = $2 AND chunk >= $3`, dir, name, keep); err != nil {
			return err
		}
		if size%ChunkSize != 0 {
			if _, err := tx.Exec(ctx, `UPDATE api_journal_mirror_chunks SET data = substring(data FROM 1 FOR $4) WHERE dir = $1 AND name = $2 AND chunk = $3`,
				dir, name, size/ChunkSize, size%ChunkSize); err != nil {
				return err
			}
		}
	case size > current:
		// Extending truncation reads back as zero bytes.
		zeros := make([]byte, size-current)
		return writeFileAt(ctx, tx, dir, name, current, zeros, false)
	}
	_, err := tx.Exec(ctx, `UPDATE api_journal_mirror_files SET size = $3, updated_at = now() WHERE dir = $1 AND name = $2`, dir, name, size)
	return err
}

func readRemote(ctx context.Context, tx pgx.Tx, dir string) (map[string][]byte, error) {
	files := map[string][]byte{}
	sizes := map[string]int64{}
	rows, err := tx.Query(ctx, `SELECT name, size FROM api_journal_mirror_files WHERE dir = $1`, dir)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		var size int64
		if err := rows.Scan(&name, &size); err != nil {
			rows.Close()
			return nil, err
		}
		sizes[name] = size
		files[name] = make([]byte, 0, size)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = tx.Query(ctx, `SELECT name, chunk, data FROM api_journal_mirror_chunks WHERE dir = $1 ORDER BY name, chunk`, dir)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		var index int64
		var data []byte
		if err := rows.Scan(&name, &index, &data); err != nil {
			rows.Close()
			return nil, err
		}
		content := files[name]
		if int64(len(content)) != index*ChunkSize {
			rows.Close()
			return nil, fmt.Errorf("journal mirror %s/%s is missing chunk before %d", dir, name, index)
		}
		files[name] = append(content, data...)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for name, size := range sizes {
		if int64(len(files[name])) != size {
			return nil, fmt.Errorf("journal mirror %s/%s has %d bytes, recorded size %d", dir, name, len(files[name]), size)
		}
	}
	return files, nil
}

func readLocal(dir string) (map[string][]byte, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := map[string][]byte{}
	for _, entry := range entries {
		if !Mirrored(entry.Name()) {
			continue
		}
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("journal entry %s is not a regular file", entry.Name())
		}
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		files[entry.Name()] = content
	}
	return files, nil
}

func writeLocalFile(path string, content []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("restore %s: %w", filepath.Base(path), err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("restore %s: %w", filepath.Base(path), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("restore %s: %w", filepath.Base(path), err)
	}
	return file.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func sortedNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// VerifyReport compares a directory with a mirror without changing either.
type VerifyReport struct {
	Dir       string   `json:"dir"`
	Files     int      `json:"files"`
	Bytes     int64    `json:"bytes"`
	Missing   []string `json:"missing,omitempty"`   // mirrored, absent locally
	Extra     []string `json:"extra,omitempty"`     // local, not mirrored
	Different []string `json:"different,omitempty"` // present in both, bytes differ
}

// Equal reports whether the directory holds exactly the mirrored files.
func (r VerifyReport) Equal() bool {
	return len(r.Missing) == 0 && len(r.Extra) == 0 && len(r.Different) == 0
}

// Verify reads a mirror and a directory and reports every difference. It
// takes no ownership and writes nothing, so it can run beside a live API
// (the result is then a snapshot of a moving target) or after it stopped.
func Verify(ctx context.Context, pool *pgxpool.Pool, logical, dir string) (VerifyReport, error) {
	report := VerifyReport{Dir: logical}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly, IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return report, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM api_journal_mirror_dirs WHERE dir = $1)`, logical).Scan(&exists); err != nil {
		return report, err
	}
	if !exists {
		return report, fmt.Errorf("journal mirror %q has not been seeded", logical)
	}
	remote, err := readRemote(ctx, tx, logical)
	if err != nil {
		return report, err
	}
	local, err := readLocal(dir)
	if err != nil {
		return report, err
	}
	for _, name := range sortedNames(remote) {
		report.Files++
		report.Bytes += int64(len(remote[name]))
		content, ok := local[name]
		switch {
		case !ok:
			report.Missing = append(report.Missing, name)
		case !bytes.Equal(content, remote[name]):
			report.Different = append(report.Different, name)
		}
	}
	for _, name := range sortedNames(local) {
		if _, ok := remote[name]; !ok {
			report.Extra = append(report.Extra, name)
		}
	}
	return report, nil
}
