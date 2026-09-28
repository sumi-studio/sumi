package journalmirror

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	// ErrNotInitialized reports a database whose mirror directory was never
	// initialized. A restore-only start refuses it instead of treating
	// "nothing" as the journals.
	ErrNotInitialized = errors.New("journal mirror has not been initialized in this database")
	// ErrNothingToAdopt reports an adoption from a directory without journal
	// files; an empty mirror is initialized explicitly with InitializeEmpty.
	ErrNothingToAdopt = errors.New("journal directory has no journal files to adopt")
	// ErrWrongMirror reports a directory that is not a replica of this
	// database's mirror (another lineage, or files of unknown origin).
	ErrWrongMirror = errors.New("journal directory does not belong to this journal mirror")
	// ErrMirrorBehind reports a mirror older than changes this host already
	// acknowledged (a database restored from an older backup, for example).
	ErrMirrorBehind = errors.New("journal mirror is older than what this host acknowledged")
)

// AttachMode selects what Attach may do with a database whose mirror
// directory was never initialized.
type AttachMode int

const (
	// AttachRestore requires an initialized mirror. It is the mode of every
	// host whose disk may start empty (a Container).
	AttachRestore AttachMode = iota
	// AttachAdopt initializes an uninitialized mirror from the directory's
	// existing journal files (a host that already holds the journals). With
	// an initialized mirror it behaves like AttachRestore, so it may stay
	// configured across restarts, except that it may replace an empty
	// initialization that never acknowledged a write.
	AttachAdopt
)

// AttachOptions configure Attach.
type AttachOptions struct {
	Mode AttachMode
	// Progress, if not nil, is called after each file and every 8 MiB.
	Progress func(AttachProgress)
}

// AttachProgress describes a reconciliation in progress.
type AttachProgress struct {
	Dir       string `json:"dir"`
	FilesDone int    `json:"files_done"`
	Files     int    `json:"files"`
	BytesDone int64  `json:"bytes_done"`
	Bytes     int64  `json:"bytes"`
}

// AttachReport describes what Attach did to a directory.
type AttachReport struct {
	Dir         string
	Lineage     string
	Initialized bool // this attach adopted the directory as the mirror's first copy
	// OverUnusedEmpty reports that the adoption replaced an empty
	// initialization that had never acknowledged a write.
	OverUnusedEmpty bool
	Files           int // mirrored files after attach
	Bytes           int64
	Restored        []string // absent locally; written from PostgreSQL
	Replaced        []string // differed locally; PostgreSQL's copy written
	// Quarantined lists local files, with their sizes, that were moved aside
	// before PostgreSQL's copy replaced them or because PostgreSQL has no
	// such file. Their bytes are kept under QuarantineDir.
	Quarantined   map[string]int64
	QuarantineDir string
	Duration      time.Duration
}

const (
	markerName     = ".journal-mirror"
	quarantineRoot = ".journal-mirror-quarantine"
	restoringExt   = ".restoring"
	ackedExt       = ".acked"
	progressEvery  = 8 << 20
)

// querier is a pool or a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type remoteMeta struct {
	size int64
	gen  int64
}

type ackedRecord struct {
	lineage string
	gen     int64
}

type localState struct {
	lineage string           // from the directory marker; empty if absent
	files   map[string]int64 // mirrored files and their sizes
	acked   map[string]ackedRecord
}

// Attach binds a local directory to a logical mirror name and reconciles it
// with PostgreSQL before any journal opens it.
//
// For an initialized mirror, PostgreSQL's copy is the acknowledged state
// unless the host's own markers show otherwise:
//
//   - The directory marker or a file's acknowledgement names another
//     lineage: ErrWrongMirror.
//   - A file's recorded generation is newer than PostgreSQL's: the database
//     is older than what this host acknowledged. ErrMirrorBehind.
//   - Files exist but the directory has no marker and they differ from the
//     mirror: files of unknown origin. ErrWrongMirror. The one exception is
//     AttachAdopt over an empty initialization that never acknowledged a
//     write (a mistaken init-empty); it adopts the files and keeps the
//     mirror's lineage.
//
// These refusals change nothing. Otherwise every local file that differs
// from PostgreSQL's copy (an unacknowledged tail, a whole-file replacement
// that never committed, or a rollback after an ambiguous commit) is moved to
// a quarantine directory and PostgreSQL's copy is written in its place;
// missing files are restored. Bytes are streamed one chunk at a time.
func (m *Mirror) Attach(ctx context.Context, logical, dir string, opts AttachOptions) (AttachReport, error) {
	started := time.Now()
	report := AttachReport{Dir: logical, Quarantined: map[string]int64{}}
	if !dirNameRe.MatchString(logical) {
		return report, fmt.Errorf("invalid journal mirror name %q", logical)
	}
	if err := m.lostError(); err != nil {
		return report, err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return report, err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return report, fmt.Errorf("create journal directory: %w", err)
	}
	m.mu.RLock()
	for existingDir, existing := range m.dirs {
		if existingDir == abs || existing.logical == logical {
			m.mu.RUnlock()
			return report, fmt.Errorf("journal mirror %q or directory %s is already attached", logical, abs)
		}
	}
	m.mu.RUnlock()

	if err := removeRestoringTemps(abs); err != nil {
		return report, err
	}
	local, err := scanLocal(abs, m.logf)
	if err != nil {
		return report, err
	}
	var lineage, how string
	err = m.pool.QueryRow(ctx, `SELECT lineage::text, initialized_how FROM api_journal_mirror_dirs WHERE dir = $1`, logical).Scan(&lineage, &how)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if opts.Mode != AttachAdopt {
			return report, fmt.Errorf("%w: %q. Start the host that holds these journals once with SUMI_API_JOURNAL_MIRROR=postgres-adopt, or restore the database that contains its mirror. For a new installation without journals, run `sumi-journal-mirror init-empty`",
				ErrNotInitialized, logical)
		}
		if err := m.adopt(ctx, logical, abs, local, "", &report); err != nil {
			return report, err
		}
	case err != nil:
		return report, fmt.Errorf("read journal mirror %q: %w", logical, err)
	default:
		// An empty initialization that never acknowledged a write holds no
		// journal state; the host that holds the journals may adopt over it.
		// adopt re-checks "never acknowledged" in its transaction.
		if opts.Mode == AttachAdopt && how == "empty" && local.lineage == "" && len(local.acked) == 0 && len(local.files) > 0 {
			err := m.adopt(ctx, logical, abs, local, lineage, &report)
			if err == nil {
				break
			}
			if !errors.Is(err, errMirrorUsed) {
				return report, err
			}
		}
		if err := m.reconcile(ctx, logical, abs, lineage, how, local, opts, &report); err != nil {
			return report, err
		}
	}
	m.mu.Lock()
	m.dirs[abs] = attachedDir{logical: logical, lineage: report.Lineage}
	m.mu.Unlock()
	report.Duration = time.Since(started)
	return report, nil
}

// unknownFilesAdvice is the way forward when a directory without a marker
// holds files the mirror does not. It never offers to replace either copy.
func unknownFilesAdvice(how string, mode AttachMode) string {
	switch {
	case how == "empty" && mode == AttachAdopt:
		return "The mirror was initialized empty and has since acknowledged writes, so these files cannot be adopted over it: both hold journal state. Keep both copies and see the runbook (\"Journal files of unknown origin\")"
	case how == "empty":
		return "If these are this installation's journals and the empty initialization was a mistake, start this host once with SUMI_API_JOURNAL_MIRROR=postgres-adopt: it adopts them while the empty mirror has acknowledged nothing, and refuses otherwise"
	default:
		return "If this host's journals are the ones to keep, attach the database that contains their mirror. If this directory is not a journal directory of this installation, point SUMI_COMMAND_LOG_DIR / SUMI_BROWSER_EVENT_DIR at the right one. See the runbook (\"Journal files of unknown origin\")"
	}
}

// errMirrorUsed reports that an empty initialization has acknowledged a
// write, so it holds journal state and cannot be adopted over.
var errMirrorUsed = errors.New("empty journal mirror has acknowledged writes")

// adoptTransactionTimeout bounds the one adoption transaction; see
// writeTransactionTimeout.
const adoptTransactionTimeout = time.Hour

// adopt initializes the mirror from the directory's journal files. With
// emptyLineage set, it replaces that empty initialization, keeping its
// lineage, provided the mirror still has no file: nothing was ever
// acknowledged through it, so no acknowledged journal is overwritten. A host
// that attached the empty mirror then restores the adopted files.
func (m *Mirror) adopt(ctx context.Context, logical, abs string, local localState, emptyLineage string, report *AttachReport) error {
	if foreign := local.lineage; foreign != "" {
		return fmt.Errorf("%w: %s is a replica of journal mirror lineage %s, which this database does not contain; attach the database that holds it instead of adopting",
			ErrWrongMirror, abs, foreign)
	}
	for name, rec := range local.acked {
		return fmt.Errorf("%w: %s carries an acknowledgement of lineage %s for %s, which this database does not contain; attach the database that holds it instead of adopting",
			ErrWrongMirror, abs, rec.lineage, name)
	}
	if len(local.files) == 0 {
		return fmt.Errorf("%w: %s. Nothing was changed. An empty directory is never adopted: a missing mount or a wrong SUMI_COMMAND_LOG_DIR / SUMI_BROWSER_EVENT_DIR looks exactly like this. Check that the path is the one this host's API has been writing to (see the runbook: \"A journal directory is empty\")",
			ErrNothingToAdopt, abs)
	}
	lineage := emptyLineage
	if lineage == "" {
		lineage = uuid.NewString()
	}
	var total int64
	for _, size := range local.files {
		total += size
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT set_config('idle_in_transaction_session_timeout', $1, true), set_config('transaction_timeout', $2, true)`,
		timeoutSetting(writeTransactionTimeout), timeoutSetting(adoptTransactionTimeout)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT api_journal_mirror_check_owner($1)`, m.epoch); err != nil {
		return m.flushError(err)
	}
	if emptyLineage == "" {
		if _, err := tx.Exec(ctx, `
			INSERT INTO api_journal_mirror_dirs (dir, lineage, initialized_how, initialized_by, initialized_files, initialized_bytes)
			VALUES ($1, $2, 'adopted', $3, $4, $5)`, logical, lineage, m.holder, len(local.files), total); err != nil {
			return fmt.Errorf("initialize journal mirror %q: %w", logical, err)
		}
	} else {
		tag, err := tx.Exec(ctx, `
			UPDATE api_journal_mirror_dirs
			SET initialized_how = 'adopted', initialized_at = now(), initialized_by = $3, initialized_files = $4, initialized_bytes = $5
			WHERE dir = $1 AND lineage = $2 AND initialized_how = 'empty'
			  AND NOT EXISTS (SELECT 1 FROM api_journal_mirror_files WHERE dir = $1)`,
			logical, lineage, m.holder+" over an unused empty initialization", len(local.files), total)
		if err != nil {
			return fmt.Errorf("adopt over empty journal mirror %q: %w", logical, err)
		}
		if tag.RowsAffected() != 1 {
			return errMirrorUsed
		}
		report.OverUnusedEmpty = true
	}
	for _, name := range sortedKeys(local.files) {
		if _, err := tx.Exec(ctx, `INSERT INTO api_journal_mirror_files (dir, name, size, gen) VALUES ($1, $2, 0, 1)`, logical, name); err != nil {
			return fmt.Errorf("adopt %s: %w", name, err)
		}
		f, err := os.Open(filepath.Join(abs, name))
		if err != nil {
			return err
		}
		err = uploadFile(ctx, tx, logical, name, 0, f)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("adopt %s: %w", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("initialize journal mirror %q: %w", logical, err)
	}
	for name := range local.files {
		if err := writeAcked(abs, name, lineage, 1); err != nil {
			return err
		}
	}
	if err := writeMarker(abs, logical, lineage); err != nil {
		return err
	}
	report.Initialized, report.Lineage = true, lineage
	report.Files, report.Bytes = len(local.files), total
	return nil
}

func (m *Mirror) reconcile(ctx context.Context, logical, abs, lineage, how string, local localState, opts AttachOptions, report *AttachReport) error {
	progress := opts.Progress
	report.Lineage = lineage
	remote, err := readRemoteMeta(ctx, m.pool, logical)
	if err != nil {
		return err
	}

	// Refusals first; they change nothing.
	if local.lineage != "" && local.lineage != lineage {
		return fmt.Errorf("%w: %s is a replica of lineage %s, this database's mirror %q is lineage %s. Attach the matching database, or move the directory aside to restore this mirror into an empty one",
			ErrWrongMirror, abs, local.lineage, logical, lineage)
	}
	var behind []string
	for _, name := range sortedKeys(local.acked) {
		rec := local.acked[name]
		if rec.lineage != lineage {
			return fmt.Errorf("%w: %s acknowledged %s under lineage %s, this database's mirror %q is lineage %s",
				ErrWrongMirror, abs, name, rec.lineage, logical, lineage)
		}
		if pg, ok := remote[name]; rec.gen > 0 && (!ok || pg.gen < rec.gen) {
			behind = append(behind, fmt.Sprintf("%s (host acknowledged generation %d, database has %d)", name, rec.gen, pg.gen))
		}
	}
	if len(behind) > 0 {
		return fmt.Errorf("%w: %s in %s. The database may have been restored from an older backup; nothing was changed. Attach the database that holds these changes, or copy the directory aside before deciding which copy to keep",
			ErrMirrorBehind, strings.Join(behind, ", "), abs)
	}
	if local.lineage == "" && len(local.files) > 0 {
		var unknown []string
		for _, name := range sortedKeys(local.files) {
			pg, ok := remote[name]
			same := ok && pg.size == local.files[name]
			if same {
				if same, err = sameContent(ctx, m.pool, logical, name, pg.size, filepath.Join(abs, name)); err != nil {
					return err
				}
			}
			if !same {
				unknown = append(unknown, name)
			}
		}
		if len(unknown) > 0 {
			return fmt.Errorf("%w: %s holds journal files of unknown origin (%s) that differ from mirror %q and has no mirror marker; nothing was changed and both copies are intact. %s",
				ErrWrongMirror, abs, strings.Join(unknown, ", "), logical, unknownFilesAdvice(how, opts.Mode))
		}
	}

	var total int64
	for _, f := range remote {
		total += f.size
	}
	p := AttachProgress{Dir: logical, Files: len(remote), Bytes: total}
	lastReported := int64(0)
	advance := func(n int64) {
		p.BytesDone += n
		if progress != nil && p.BytesDone-lastReported >= progressEvery {
			lastReported = p.BytesDone
			progress(p)
		}
	}
	quarantine := func(name string) error {
		if report.QuarantineDir == "" {
			stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
			report.QuarantineDir = filepath.Join(abs, quarantineRoot, fmt.Sprintf("%s-epoch%d", stamp, m.epoch))
			if err := os.MkdirAll(report.QuarantineDir, 0o700); err != nil {
				return fmt.Errorf("create journal quarantine: %w", err)
			}
		}
		if err := os.Rename(filepath.Join(abs, name), filepath.Join(report.QuarantineDir, name)); err != nil {
			return fmt.Errorf("quarantine %s: %w", name, err)
		}
		report.Quarantined[name] = local.files[name]
		return nil
	}

	for _, name := range sortedKeys(remote) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("journal mirror %q restore interrupted after %d of %d files: %w", logical, p.FilesDone, p.Files, err)
		}
		pg := remote[name]
		size, present := local.files[name]
		if present {
			same := size == pg.size
			if same {
				if same, err = sameContent(ctx, m.pool, logical, name, pg.size, filepath.Join(abs, name)); err != nil {
					return err
				}
			}
			if same {
				advance(pg.size)
			} else {
				if err := quarantine(name); err != nil {
					return err
				}
				if err := restoreFile(ctx, m.pool, logical, name, pg.size, abs, advance); err != nil {
					return err
				}
				report.Replaced = append(report.Replaced, name)
			}
		} else {
			if err := restoreFile(ctx, m.pool, logical, name, pg.size, abs, advance); err != nil {
				return err
			}
			report.Restored = append(report.Restored, name)
		}
		if err := writeAcked(abs, name, lineage, pg.gen); err != nil {
			return err
		}
		p.FilesDone++
		if progress != nil {
			progress(p)
		}
	}
	for _, name := range sortedKeys(local.files) {
		if _, ok := remote[name]; !ok {
			// Created locally, never acknowledged (an acknowledged file would
			// be in the mirror, or the refusals above would have fired).
			if err := quarantine(name); err != nil {
				return err
			}
		}
	}
	for name := range local.acked {
		if _, ok := remote[name]; !ok {
			if err := os.Remove(ackedPath(abs, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if local.lineage == "" {
		if err := writeMarker(abs, logical, lineage); err != nil {
			return err
		}
	}
	if report.QuarantineDir != "" {
		if err := syncDir(report.QuarantineDir); err != nil {
			return err
		}
	}
	if err := syncDir(abs); err != nil {
		return err
	}
	report.Files, report.Bytes = len(remote), total
	return nil
}

func readRemoteMeta(ctx context.Context, pool querier, logical string) (map[string]remoteMeta, error) {
	rows, err := pool.Query(ctx, `SELECT name, size, gen FROM api_journal_mirror_files WHERE dir = $1`, logical)
	if err != nil {
		return nil, fmt.Errorf("read journal mirror %q: %w", logical, err)
	}
	defer rows.Close()
	files := map[string]remoteMeta{}
	for rows.Next() {
		var name string
		var f remoteMeta
		if err := rows.Scan(&name, &f.size, &f.gen); err != nil {
			return nil, err
		}
		files[name] = f
	}
	return files, rows.Err()
}

// streamChunks calls fn for each stored chunk of a file in order, holding one
// chunk in memory at a time, and checks that the chunks cover exactly size
// bytes.
func streamChunks(ctx context.Context, pool querier, logical, name string, size int64, fn func(offset int64, data []byte) error) error {
	rows, err := pool.Query(ctx, `SELECT chunk, data FROM api_journal_mirror_chunks WHERE dir = $1 AND name = $2 ORDER BY chunk`, logical, name)
	if err != nil {
		return fmt.Errorf("read journal mirror %s/%s: %w", logical, name, err)
	}
	defer rows.Close()
	var offset int64
	for rows.Next() {
		var index int64
		var data []byte
		if err := rows.Scan(&index, &data); err != nil {
			return err
		}
		if index*ChunkSize != offset {
			return fmt.Errorf("journal mirror %s/%s is missing bytes before chunk %d", logical, name, index)
		}
		if err := fn(offset, data); err != nil {
			return err
		}
		offset += int64(len(data))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read journal mirror %s/%s: %w", logical, name, err)
	}
	if offset != size {
		return fmt.Errorf("journal mirror %s/%s holds %d bytes, recorded size %d", logical, name, offset, size)
	}
	return nil
}

var errDiffers = errors.New("differs")

func sameContent(ctx context.Context, pool querier, logical, name string, size int64, path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buf := make([]byte, ChunkSize)
	err = streamChunks(ctx, pool, logical, name, size, func(offset int64, data []byte) error {
		n, err := f.ReadAt(buf[:len(data)], offset)
		if n != len(data) || !bytes.Equal(buf[:n], data) {
			return errDiffers
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		return nil
	})
	if errors.Is(err, errDiffers) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() == size, nil
}

// restoreFile writes PostgreSQL's copy of a file through a temporary file
// and renames it into place, so a crash never leaves a partial journal.
func restoreFile(ctx context.Context, pool querier, logical, name string, size int64, abs string, advance func(int64)) error {
	temp := filepath.Join(abs, "."+name+restoringExt)
	f, err := os.OpenFile(temp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("restore %s: %w", name, err)
	}
	err = streamChunks(ctx, pool, logical, name, size, func(_ int64, data []byte) error {
		if _, err := f.Write(data); err != nil {
			return err
		}
		advance(int64(len(data)))
		return nil
	})
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(temp, filepath.Join(abs, name))
	}
	if err != nil {
		_ = os.Remove(temp)
		return fmt.Errorf("restore %s: %w", name, err)
	}
	return nil
}

func removeRestoringTemps(abs string) error {
	entries, err := os.ReadDir(abs)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") && strings.HasSuffix(name, restoringExt) && entry.Type().IsRegular() {
			if err := os.Remove(filepath.Join(abs, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func scanLocal(abs string, logf func(string, ...any)) (localState, error) {
	state := localState{files: map[string]int64{}, acked: map[string]ackedRecord{}}
	lineage, err := readMarker(abs)
	if err != nil {
		return state, err
	}
	state.lineage = lineage
	entries, err := os.ReadDir(abs)
	if err != nil {
		return state, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if Mirrored(name) {
			if !entry.Type().IsRegular() {
				return state, fmt.Errorf("journal entry %s is not a regular file", name)
			}
			info, err := entry.Info()
			if err != nil {
				return state, err
			}
			state.files[name] = info.Size()
			continue
		}
		if file, ok := ackedFileName(name); ok && entry.Type().IsRegular() {
			rec, err := readAcked(filepath.Join(abs, name))
			if err != nil {
				// A torn marker only weakens the older-backup check for this
				// file; it never makes a local file win.
				logf("journal mirror: ignoring unreadable acknowledgement %s: %v", name, err)
				continue
			}
			state.acked[file] = rec
		}
	}
	return state, nil
}

func ackedPath(dir, name string) string { return filepath.Join(dir, "."+name+ackedExt) }

func ackedFileName(entry string) (string, bool) {
	if !strings.HasPrefix(entry, ".") || !strings.HasSuffix(entry, ackedExt) {
		return "", false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(entry, "."), ackedExt)
	return name, Mirrored(name)
}

// writeAcked records, beside a file, the mirror generation of its last
// acknowledged change. The record has a fixed size and is rewritten in
// place, so it is never observed half old and half new on a sector.
func writeAcked(dir, name, lineage string, gen int64) error {
	record := fmt.Sprintf("%s %020d\n", lineage, gen)
	path := ackedPath(dir, name)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
		created = true
	}
	if err != nil {
		return err
	}
	if _, err := f.WriteAt([]byte(record), 0); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if created {
		return syncDir(dir)
	}
	return nil
}

func readAcked(path string) (ackedRecord, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ackedRecord{}, err
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 2 || len(raw) != 58 {
		return ackedRecord{}, fmt.Errorf("malformed acknowledgement %q", raw)
	}
	if _, err := uuid.Parse(fields[0]); err != nil {
		return ackedRecord{}, err
	}
	gen, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || gen < 0 {
		return ackedRecord{}, fmt.Errorf("malformed acknowledgement generation %q", fields[1])
	}
	return ackedRecord{lineage: fields[0], gen: gen}, nil
}

const markerHeader = "sumi-journal-mirror 1"

func writeMarker(dir, logical, lineage string) error {
	path := filepath.Join(dir, markerName)
	temp := path + restoringExt
	content := fmt.Sprintf("%s %s %s\n", markerHeader, logical, lineage)
	if err := os.WriteFile(temp, []byte(content), 0o600); err != nil {
		return err
	}
	f, err := os.Open(temp)
	if err != nil {
		return err
	}
	err = f.Sync()
	_ = f.Close()
	if err != nil {
		return err
	}
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	return syncDir(dir)
}

// readMarker returns the lineage recorded in dir, or "" if there is none.
func readMarker(dir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, markerName))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	fields := strings.Fields(string(raw))
	if len(fields) != 4 || fields[0]+" "+fields[1] != markerHeader {
		return "", fmt.Errorf("journal mirror marker %s is malformed; move it aside only if this directory is known to belong to the attached database", filepath.Join(dir, markerName))
	}
	if _, err := uuid.Parse(fields[3]); err != nil {
		return "", fmt.Errorf("journal mirror marker %s has an invalid lineage: %w", filepath.Join(dir, markerName), err)
	}
	return fields[3], nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func sortedKeys[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
