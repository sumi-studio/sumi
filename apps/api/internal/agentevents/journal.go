package agentevents

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type BrowserJournal struct {
	historyMu sync.Mutex
	history   map[string]*browserHistoryIndex
	dir       string
	commands  *CommandStore
	mu        sync.Mutex

	// journalDir is a pinned descriptor for the private, shared runtime
	// directory. Authoritative PAID locks are opened relative to it and their
	// inode identities are pinned on first use, so replacing a path cannot
	// silently split lock domains inside a running API process.
	journalDir         *os.File
	journalDirIdentity durableFileIdentity
	journalLocksMu     sync.Mutex
	journalLockIDs     map[string]durableFileIdentity

	// PollInterval bounds the polling interval used by WaitFor and Live.
	// A zero value uses the safe default (50ms).
	PollInterval time.Duration
	// MaxPersonalityAgentTails and MaxAckTail bound process memory without changing
	// durable replay. Zero values use conservative defaults.
	MaxPersonalityAgentTails int
	// MaxBrowserSessionRevocations bounds the durable browser-session denylist.
	// Zero uses maxRevokedSessions.
	MaxBrowserSessionRevocations int

	tails map[string]*personalityAgentLogState
	// browserSubscribers carry volatile frames only. Durable replay always
	// reads the event log, so disconnecting a slow browser cannot lose durable
	// history or grow this process without bound.
	browserSubscribers    map[string]map[uint64]chan browserVolatileBatch
	nextBrowserSubscriber uint64
	clock                 uint64
	newFile               func(string, int, os.FileMode) (durableFileHandle, error)
	writeAtomic           func(string, []byte, os.FileMode) error
	// browserSessionMutationHook is test-only synchronization invoked while
	// the shared exclusive lifecycle lock is held.
	browserSessionMutationHook func(browserSessionMutationKind)
	// browserSessionLockAttemptHook is test-only synchronization invoked
	// immediately before attempting the shared lifecycle lock.
	browserSessionLockAttemptHook func()
	// eventStatFastPath records that the runtime directory's filesystem meets
	// the event-log metadata fast path contract (see eventStatTrustAge).
	eventStatFastPath bool
	// nowHook is test-only: it moves the wall clock the fast path compares
	// against file change times, without sleeping.
	nowHook func() time.Time

	// stateMu protects run-in-flight and pending-approval state derived from
	// durable events. Readers also take mu first so they cannot observe the
	// interval after an event is durably appended but before its derived state
	// is updated.
	stateMu          sync.RWMutex
	runInFlight      map[string]bool
	pendingApprovals map[string]map[string]bool
}

type personalityAgentLogState struct {
	eventSeq  uint64
	eventSize int64
	eventCRC  uint32
	// eventStat fingerprints the event file as of the last refresh that
	// verified this tail against its full content under the exclusive lock,
	// at wall time eventStatVerifiedNS. eventStatTrusted lets a later refresh
	// skip re-reading the log while the fingerprint is unchanged — see
	// eventStatTrustAge for the contract.
	eventStat           eventFileStat
	eventStatVerifiedNS int64
	eventStatTrusted    bool
	// tailObserved records that a refresh under the event-file lock has
	// folded this persona's committed log into the session guards — the
	// diagnostic distinction between "verified idle" and "never looked".
	tailObserved bool
	// runStarts/runOpen are the committed run-marker state, folded from
	// every event line as it is observed under the event-file lock (own
	// appends and other writers' tails alike). Projected lifecycle markers
	// derive their identity from these counters at commit time, never from
	// a caller's cached view.
	runStarts uint64
	runOpen   bool
	lastUsed  uint64
}

type durableFileIdentity struct {
	device uint64
	inode  uint64
}

func crc32OfFilePrefix(file io.ReadSeeker, size int64) (uint32, error) {
	if size <= 0 {
		return 0, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	h := crc32.New(crc32.IEEETable)
	if _, err := io.CopyN(h, file, size); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func updateCRC(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crc32.IEEETable, data)
}

const eventStatTrustAge = 2 * time.Second

type eventFileStat struct {
	device  uint64
	inode   uint64
	size    int64
	ctimeNS int64
}

func statEventFile(file durableFileHandle) (eventFileStat, bool) {
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &stat); err != nil {
		return eventFileStat{}, false
	}
	return eventFileStat{
		device:  uint64(stat.Dev),
		inode:   uint64(stat.Ino),
		size:    stat.Size,
		ctimeNS: stat.Ctim.Nano(),
	}, true
}

// localChangeTimeFilesystem reports whether a filesystem meets condition (1)
// of the event-log metadata fast path.
func localChangeTimeFilesystem(fs *syscall.Statfs_t) bool {
	switch uint32(fs.Type) {
	case 0xEF53, // ext2/3/4
		0x58465342, // XFS
		0x9123683E, // Btrfs
		0x01021994, // tmpfs
		0x794C7630: // overlayfs
		return true
	default:
		return false
	}
}

func (g *BrowserJournal) now() time.Time {
	if g.nowHook != nil {
		return g.nowHook()
	}
	return time.Now()
}

var errBrowserRuntimeUnavailable = errors.New("browser runtime is unavailable")

type directChatReadiness struct {
	ready  bool
	reason string
}

func (r directChatReadiness) directChatStatusFrame() directChatStatusFrame {
	status := "unavailable"
	if r.ready {
		status = "ready"
	}
	return directChatStatusFrame{
		Type:   "direct_chat_status",
		Status: status,
		Reason: r.reason,
	}
}

const (
	connectionLeaseStateVersion  = uint64(1)
	maxConnectionLeaseStateBytes = 4096
	connectionLeaseIDBytes       = 32
)

type durableEventRecord struct {
	Seq   uint64   `json:"seq"`
	Event Envelope `json:"event"`
}

// UnmarshalJSON makes durable event-log recovery fail-closed on duplicate keys,
// unknown fields, trailing data, and sequence values outside the JSON-safe range.
func (r *durableEventRecord) UnmarshalJSON(data []byte) error {
	if err := checkDuplicateKeys(data); err != nil {
		return fmt.Errorf("durable event record json: %w", err)
	}
	type raw struct {
		Seq   *uint64   `json:"seq"`
		Event *Envelope `json:"event"`
	}
	var v raw
	if err := unmarshalStrict(data, &v); err != nil {
		return err
	}
	if v.Seq == nil || v.Event == nil {
		return errors.New("durable event record requires seq and event")
	}
	if *v.Seq > maxJSONSafeInteger {
		return fmt.Errorf("durable event record seq %d exceeds JSON-safe integer range", *v.Seq)
	}
	if err := validateEnvelope(*v.Event); err != nil {
		return fmt.Errorf("durable event record event: %w", err)
	}
	*r = durableEventRecord{Seq: *v.Seq, Event: *v.Event}
	return nil
}

// durableFileHandle abstracts the per-personality-agent log file so tests can
// inject deterministic write/sync/truncate failures without changing
// production call sites.
type durableFileHandle interface {
	io.Seeker
	io.Reader
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
	Fd() uintptr
}

// OpenMirroredBrowserJournal is OpenBrowserJournal with every event log,
// dedup index and session-revocation write committed through mirror before
// it is reported durable. The mirror must already have reconciled dir.
func OpenMirroredBrowserJournal(dir string, commands *CommandStore, mirror FileMirror) (*BrowserJournal, error) {
	if mirror == nil {
		return nil, errors.New("browser journal mirror is required")
	}
	g, err := OpenBrowserJournal(dir, commands)
	if err != nil {
		return nil, err
	}
	open := g.newFile
	g.newFile = func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
		file, err := open(name, flag, perm)
		if err != nil {
			return nil, err
		}
		mirrored, err := mirror.Wrap(name, flag, file)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return mirrored, nil
	}
	g.writeAtomic = mirror.WrapAtomicWrite(g.writeAtomic)
	return g, nil
}

func OpenBrowserJournal(dir string, commands *CommandStore) (*BrowserJournal, error) {
	if dir == "" {
		return nil, errors.New("browser journal directory is required")
	}
	if commands == nil {
		return nil, errors.New("command store is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve browser journal directory: %w", err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("create browser journal directory: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("resolve browser journal directory symlinks: %w", err)
	}
	if filepath.Clean(resolved) != filepath.Clean(abs) {
		return nil, errors.New("browser journal path must not contain symlinks")
	}
	dirFD, err := syscall.Open(
		abs,
		syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC,
		0,
	)
	if err != nil {
		return nil, fmt.Errorf("open browser journal directory: %w", err)
	}
	journalDir := os.NewFile(uintptr(dirFD), abs)
	if journalDir == nil {
		_ = syscall.Close(dirFD)
		return nil, errors.New("open browser journal directory")
	}
	identity, err := validatePinnedJournalDirectory(abs, journalDir)
	if err != nil {
		_ = journalDir.Close()
		return nil, err
	}
	var fs syscall.Statfs_t
	eventStatFastPath := syscall.Fstatfs(dirFD, &fs) == nil && localChangeTimeFilesystem(&fs)
	return &BrowserJournal{
		dir:                          abs,
		commands:                     commands,
		journalDir:                   journalDir,
		journalDirIdentity:           identity,
		journalLockIDs:               make(map[string]durableFileIdentity),
		PollInterval:                 50 * time.Millisecond,
		MaxPersonalityAgentTails:     128,
		MaxBrowserSessionRevocations: maxRevokedSessions,
		tails:                        make(map[string]*personalityAgentLogState),
		browserSubscribers:           make(map[string]map[uint64]chan browserVolatileBatch),
		runInFlight:                  make(map[string]bool),
		pendingApprovals:             make(map[string]map[string]bool),
		eventStatFastPath:            eventStatFastPath,
		newFile: func(name string, flag int, perm os.FileMode) (durableFileHandle, error) {
			return os.OpenFile(name, flag|syscall.O_NOFOLLOW, perm)
		},
		writeAtomic: writeFileAtomic,
	}, nil
}

func fileIdentity(info os.FileInfo) (durableFileIdentity, *syscall.Stat_t, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return durableFileIdentity{}, nil, errors.New("filesystem identity is unavailable")
	}
	return durableFileIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, stat, nil
}

func validatePinnedJournalDirectory(path string, directory *os.File) (durableFileIdentity, error) {
	if directory == nil {
		return durableFileIdentity{}, errors.New("browser journal directory is not open")
	}
	fdInfo, err := directory.Stat()
	if err != nil {
		return durableFileIdentity{}, fmt.Errorf("inspect browser journal directory descriptor: %w", err)
	}
	fdIdentity, fdStat, err := fileIdentity(fdInfo)
	if err != nil {
		return durableFileIdentity{}, err
	}
	if !fdInfo.IsDir() ||
		fdInfo.Mode().Perm() != 0o700 ||
		fdStat.Uid != uint32(os.Geteuid()) ||
		fdStat.Nlink != 2 {
		return durableFileIdentity{}, errors.New(
			"browser journal directory must be owner-only, owned by the API user, and have link count 2",
		)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return durableFileIdentity{}, fmt.Errorf("inspect browser journal directory path: %w", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 {
		return durableFileIdentity{}, errors.New("browser journal path must not be a symlink")
	}
	pathIdentity, pathStat, err := fileIdentity(pathInfo)
	if err != nil {
		return durableFileIdentity{}, err
	}
	if !pathInfo.IsDir() ||
		pathInfo.Mode().Perm() != 0o700 ||
		pathStat.Uid != uint32(os.Geteuid()) ||
		pathStat.Nlink != fdStat.Nlink ||
		pathIdentity != fdIdentity {
		return durableFileIdentity{}, errors.New("browser journal directory path no longer identifies the pinned private directory")
	}
	return fdIdentity, nil
}

func (g *BrowserJournal) revalidateJournalDirectory() error {
	identity, err := validatePinnedJournalDirectory(g.dir, g.journalDir)
	if err != nil {
		return err
	}
	if identity != g.journalDirIdentity {
		return errors.New("browser journal directory identity changed")
	}
	return nil
}

func validateJournalLockFile(file *os.File) (durableFileIdentity, error) {
	if file == nil {
		return durableFileIdentity{}, errors.New("persona journal lock is not open")
	}
	info, err := file.Stat()
	if err != nil {
		return durableFileIdentity{}, fmt.Errorf("inspect persona journal lock: %w", err)
	}
	identity, stat, err := fileIdentity(info)
	if err != nil {
		return durableFileIdentity{}, err
	}
	if !info.Mode().IsRegular() ||
		info.Mode().Perm() != 0o600 ||
		stat.Uid != uint32(os.Geteuid()) ||
		stat.Nlink != 1 {
		return durableFileIdentity{}, errors.New(
			"persona journal lock must be a private, singly linked regular file owned by the API user",
		)
	}
	return identity, nil
}

// openJournalLock is the only authoritative PAID lock opener. All API
// replicas must use the same POSIX-flock- and atomic-rename-coherent
// filesystem, mounted at an owner-only directory and operated by the same
// trusted service UID. Same-UID mutation is outside this trust boundary.
func (g *BrowserJournal) openJournalLock(personalityAgentID string) (*os.File, error) {
	if err := g.revalidateJournalDirectory(); err != nil {
		return nil, err
	}
	name := "journal-" + safeFileID(personalityAgentID) + ".lock"
	fd, err := syscall.Openat(
		int(g.journalDir.Fd()),
		name,
		syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW|syscall.O_CLOEXEC,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open persona journal lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(g.dir, name))
	if file == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("open persona journal lock")
	}
	identity, err := validateJournalLockFile(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	g.journalLocksMu.Lock()
	pinned, exists := g.journalLockIDs[personalityAgentID]
	if !exists {
		g.journalLockIDs[personalityAgentID] = identity
	}
	g.journalLocksMu.Unlock()
	if exists && pinned != identity {
		_ = file.Close()
		return nil, errors.New("persona journal lock inode changed")
	}

	// Re-open by name through the pinned directory after the first open. The
	// two descriptors must identify the same inode, closing the replacement
	// window around the pathname lookup.
	checkFD, err := syscall.Openat(
		int(g.journalDir.Fd()),
		name,
		syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC,
		0,
	)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("re-open persona journal lock for identity check: %w", err)
	}
	check := os.NewFile(uintptr(checkFD), filepath.Join(g.dir, name))
	checkIdentity, checkErr := validateJournalLockFile(check)
	_ = check.Close()
	if checkErr != nil {
		_ = file.Close()
		return nil, checkErr
	}
	if checkIdentity != identity {
		_ = file.Close()
		return nil, errors.New("persona journal lock changed during open")
	}
	if err := g.revalidateJournalDirectory(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (g *BrowserJournal) pollInterval() time.Duration {
	if g.PollInterval > 0 {
		return g.PollInterval
	}
	return 50 * time.Millisecond
}

func (g *BrowserJournal) maxPersonalityAgentTails() int {
	if g.MaxPersonalityAgentTails > 0 {
		return g.MaxPersonalityAgentTails
	}
	return 128
}

func (g *BrowserJournal) Append(
	ctx context.Context,
	provenance DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (CommandEnvelope, error) {
	envelope, _, err := g.appendWithIdempotencyStatus(
		ctx,
		provenance,
		idempotencyKey,
		command,
	)
	return envelope, err
}

// AppendWithIdempotencyStatus is the browser receipt variant of Append. It
// returns true only when the atomic command-store decision reused an existing
// durable command for the same key and authenticated bytes.
func (g *BrowserJournal) AppendWithIdempotencyStatus(
	ctx context.Context,
	provenance DirectChatProvenance,
	idempotencyKey string,
	command json.RawMessage,
) (CommandEnvelope, bool, error) {
	return g.appendWithIdempotencyStatus(ctx, provenance, idempotencyKey, command)
}

// IsPersonalityAgentReady reports the authoritative Ready latch for the one
// global runtime identity. Tenant is intentionally absent from this key.
func (g *BrowserJournal) IsPersonalityAgentReady(ctx context.Context, personalityAgentID string) (bool, error) {
	readiness, err := g.directChatReadiness(ctx, personalityAgentID)
	if err != nil {
		return false, err
	}
	return readiness.ready, nil
}

// Live polls the durable command log starting at fromSeq and streams each
// command in order. The first poll reads from fromSeq, so a command appended
// concurrently with this call is not lost. next advances only after a command
// is successfully sent, and the next poll continues from that point.

// errTornDurableEventTail marks a partial final event record — a crash mid-
// write left bytes that no newline terminated. Unlike every other decode
// failure this is repairable: the next writer would truncate the record under
// the event-file lock anyway, so read paths may do the same and re-verify.
var errTornDurableEventTail = errors.New("durable event log tail is torn")

// EventCatchUp returns the durable event suffix after lastConsumedSeq. It
// verifies the complete retained log on every read, so a gap or corrupt record
// fails closed rather than being silently skipped during browser reconnect.
// A torn final record is first repaired under the exclusive event-file lock —
// the same truncation the append path performs — then re-verified; malformed
// complete records and non-contiguous data remain hard errors.
func (g *BrowserJournal) EventCatchUp(ctx context.Context, personalityAgentID string, lastConsumedSeq uint64) ([]Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return nil, err
	}
	if lastConsumedSeq > maxJSONSafeInteger {
		return nil, fmt.Errorf("browser event cursor %d exceeds JSON-safe integer range", lastConsumedSeq)
	}
	out, err := g.eventCatchUpScan(ctx, personalityAgentID, lastConsumedSeq)
	if errors.Is(err, errTornDurableEventTail) {
		if repairErr := g.RefreshDurableEventTail(ctx, personalityAgentID); repairErr != nil {
			return nil, err
		}
		out, err = g.eventCatchUpScan(ctx, personalityAgentID, lastConsumedSeq)
	}
	return out, err
}

func (g *BrowserJournal) eventCatchUpScan(ctx context.Context, personalityAgentID string, lastConsumedSeq uint64) ([]Envelope, error) {
	path := g.eventPath(personalityAgentID)
	file, err := g.newFile(path, os.O_RDONLY, 0o600)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_SH); err != nil {
		return nil, fmt.Errorf("lock durable event log for browser replay: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	r := bufio.NewReader(file)
	var previous uint64
	var out []Envelope
	for {
		line, readErr := r.ReadBytes('\n')
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) != 0 {
			var record durableEventRecord
			if err := json.Unmarshal(trimmed, &record); err != nil {
				if errors.Is(readErr, io.EOF) && isIncompleteJSONError(err) {
					return nil, fmt.Errorf("decode durable event log for browser replay: %w", errTornDurableEventTail)
				}
				return nil, fmt.Errorf("decode durable event log for browser replay: %w", err)
			}
			if record.Seq != previous+1 {
				return nil, fmt.Errorf("durable event log is non-contiguous: got %d after %d", record.Seq, previous)
			}
			if record.Event.Seq == nil || *record.Event.Seq != record.Seq {
				return nil, fmt.Errorf("durable event record seq mismatch: outer %d, inner %v", record.Seq, record.Event.Seq)
			}
			if record.Event.PersonalityAgentID != personalityAgentID {
				return nil, fmt.Errorf("durable event record personality agent mismatch: got %q, want %q", record.Event.PersonalityAgentID, personalityAgentID)
			}
			previous = record.Seq
			if record.Seq > lastConsumedSeq {
				out = append(out, record.Event)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read durable event log for browser replay: %w", readErr)
		}
	}
	if lastConsumedSeq > previous {
		return nil, fmt.Errorf("browser event cursor %d is ahead of durable tail %d", lastConsumedSeq, previous)
	}
	return out, nil
}

// CommandDispositionFor returns the exact durable terminal disposition for a
// command, when one has already been committed. The scan deliberately retains
// only the matching event: browser idempotency reconciliation must not turn a
// lifetime event log into an unbounded in-memory disposition index.
// A torn final record is first repaired under the exclusive event-file lock —
// the same truncation the append path performs — then re-verified; malformed
// complete records and non-contiguous data remain hard errors.
func (g *BrowserJournal) CommandDispositionFor(
	ctx context.Context,
	command CommandEnvelope,
) (json.RawMessage, bool, error) {
	disposition, found, err := g.commandDispositionScan(ctx, command)
	if errors.Is(err, errTornDurableEventTail) {
		if repairErr := g.RefreshDurableEventTail(ctx, command.PersonalityAgentID); repairErr != nil {
			return nil, false, err
		}
		disposition, found, err = g.commandDispositionScan(ctx, command)
	}
	return disposition, found, err
}

func (g *BrowserJournal) commandDispositionScan(
	ctx context.Context,
	command CommandEnvelope,
) (json.RawMessage, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := command.Validate(); err != nil {
		return nil, false, fmt.Errorf("validate command disposition lookup: %w", err)
	}
	persisted, found, err := g.commands.GetCommand(
		ctx,
		command.PersonalityAgentID,
		command.Seq,
	)
	if err != nil {
		return nil, false, fmt.Errorf("load command for disposition lookup: %w", err)
	}
	if !found || persisted.CommandID != command.CommandID {
		return nil, false, errors.New("command disposition lookup does not match durable command log")
	}

	path := g.eventPath(command.PersonalityAgentID)
	file, err := g.newFile(path, os.O_RDONLY, 0o600)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_SH); err != nil {
		return nil, false, fmt.Errorf("lock durable event log for command disposition lookup: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	r := bufio.NewReader(file)
	var previous uint64
	var disposition json.RawMessage
	for {
		line, readErr := r.ReadBytes('\n')
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) != 0 {
			var record durableEventRecord
			if err := json.Unmarshal(trimmed, &record); err != nil {
				if errors.Is(readErr, io.EOF) && isIncompleteJSONError(err) {
					return nil, false, fmt.Errorf("decode durable event log for command disposition lookup: %w", errTornDurableEventTail)
				}
				return nil, false, fmt.Errorf("decode durable event log for command disposition lookup: %w", err)
			}
			if record.Seq != previous+1 {
				return nil, false, fmt.Errorf("durable event log is non-contiguous: got %d after %d", record.Seq, previous)
			}
			if record.Event.Seq == nil || *record.Event.Seq != record.Seq {
				return nil, false, fmt.Errorf("durable event record seq mismatch: outer %d, inner %v", record.Seq, record.Event.Seq)
			}
			if record.Event.PersonalityAgentID != command.PersonalityAgentID {
				return nil, false, fmt.Errorf(
					"durable event record personality agent mismatch: got %q, want %q",
					record.Event.PersonalityAgentID,
					command.PersonalityAgentID,
				)
			}
			previous = record.Seq

			if eventType(record.Event.Event) == "command_disposition" {
				var correlation struct {
					CommandID  string `json:"command_id"`
					CommandSeq uint64 `json:"command_seq"`
				}
				if err := json.Unmarshal(record.Event.Event, &correlation); err != nil {
					return nil, false, fmt.Errorf("decode durable command disposition correlation: %w", err)
				}
				if correlation.CommandID == command.CommandID && correlation.CommandSeq == command.Seq {
					if disposition != nil {
						return nil, false, errors.New("durable event log contains duplicate command dispositions")
					}
					disposition = append(json.RawMessage(nil), record.Event.Event...)
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, false, fmt.Errorf("read durable event log for command disposition lookup: %w", readErr)
		}
	}
	return disposition, disposition != nil, nil
}

// SubscribeBrowserVolatile registers one bounded live-only receiver. Adjacent
// deltas are combined while pending; sustained overload still disconnects.
func (g *BrowserJournal) SubscribeBrowserVolatile(personalityAgentID string) (<-chan browserVolatileBatch, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.nextBrowserSubscriber++
	id := g.nextBrowserSubscriber
	out := make(chan browserVolatileBatch, 1)
	if g.browserSubscribers[personalityAgentID] == nil {
		g.browserSubscribers[personalityAgentID] = make(map[uint64]chan browserVolatileBatch)
	}
	g.browserSubscribers[personalityAgentID][id] = out
	var once sync.Once
	return out, func() {
		once.Do(func() {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.removeBrowserSubscriberLocked(personalityAgentID, id)
		})
	}
}

func (g *BrowserJournal) publishVolatileLocked(personalityAgentID string, envelope Envelope) {
	for id, subscriber := range g.browserSubscribers[personalityAgentID] {
		// Only this publisher sends, under g.mu. Taking the pending batch
		// transfers its ownership; the consumer cannot be reading it too.
		var batch browserVolatileBatch
		select {
		case batch = <-subscriber:
		default:
		}
		if !batch.append(envelope) {
			g.removeBrowserSubscriberLocked(personalityAgentID, id)
			continue
		}
		subscriber <- batch
	}
}

func (g *BrowserJournal) removeBrowserSubscriberLocked(personalityAgentID string, id uint64) {
	subscribers := g.browserSubscribers[personalityAgentID]
	if subscriber, ok := subscribers[id]; ok {
		delete(subscribers, id)
		close(subscriber)
	}
	if len(subscribers) == 0 {
		delete(g.browserSubscribers, personalityAgentID)
	}
}

func (g *BrowserJournal) stateFor(personalityAgentID string) *personalityAgentLogState {
	g.clock++
	st, ok := g.tails[personalityAgentID]
	if ok {
		st.lastUsed = g.clock
		return st
	}
	st = &personalityAgentLogState{lastUsed: g.clock}
	g.tails[personalityAgentID] = st
	g.evictInactiveTailsLocked(personalityAgentID)
	return st
}

func (g *BrowserJournal) evictInactiveTailsLocked(activePersonalityAgentID string) {
	for len(g.tails) > g.maxPersonalityAgentTails() {
		var evictID string
		var oldest uint64
		for personalityAgentID, state := range g.tails {
			if personalityAgentID == activePersonalityAgentID {
				continue
			}
			if evictID == "" || state.lastUsed < oldest || (state.lastUsed == oldest && personalityAgentID < evictID) {
				evictID, oldest = personalityAgentID, state.lastUsed
			}
		}
		if evictID == "" {
			return
		}
		delete(g.tails, evictID)
	}
}

// ProjectedEvent is one browser-visible event a non-runtime producer asks the
// gateway to commit. DedupKey is the fact's durable identity: a replayed
// projection regenerates the same key, so AppendProjectedEvents can refuse to
// double-commit under the event-file lock — across process restarts and
// concurrent projectors alike. Events whose wire content already carries their
// identity (message_id, request/command ids) leave DedupKey zero and default
// to sha256(Event).
//
// RunMarker instead asks for a lifecycle marker whose need is decided under
// the append lock from committed run state — never from the caller's cache:
// RunMarkerStart emits {"type":"agent_start"} iff the committed log has no
// open run and some later element of the same batch commits (a replayed or
// fully deduplicated batch leaves no bare start behind); RunMarkerEnd emits
// {"type":"agent_end"} iff a run is open at that point in the batch. A marker
// that is not needed commits nothing and consumes no seq. Event and DedupKey
// are ignored for marker elements: the gateway mints the marker bytes and the
// dedup key from the committed run index it holds under the lock.
type ProjectedEvent struct {
	Event     json.RawMessage
	DedupKey  [sha256.Size]byte
	RunMarker string
	// IfTail is a staleness precondition for callers that decided this
	// emission on an observation made outside the event-file lock (for
	// example an idleness read against PG): the element is emitted only if
	// the committed tail under the lock still equals the mark the caller
	// captured with observedEventTail BEFORE that observation. A skipped
	// element consumes no seq and no marker identity; the caller re-
	// evaluates on its next pass.
	IfTail *EventTailMark
}

// EventTailMark identifies one committed event-log tail: the seq plus the
// running CRC of the log's content, so an equality check under the append
// lock detects every committed change — appends, rewrites, truncations —
// since the mark was taken.
type EventTailMark struct {
	Seq uint64
	CRC uint32
}

// observedEventTail returns the event-log tail this gateway last folded
// under the event-file lock. It is a cheap read of committed state — a mark
// older than another writer's commits simply fails the IfTail check once and
// the caller re-marks with the refreshed tail on its next pass.
func (g *BrowserJournal) observedEventTail(personalityAgentID string) EventTailMark {
	g.mu.Lock()
	defer g.mu.Unlock()
	st := g.stateFor(personalityAgentID)
	return EventTailMark{Seq: st.eventSeq, CRC: st.eventCRC}
}

// Run-marker kinds understood by AppendProjectedEvents.
const (
	RunMarkerStart = "start"
	RunMarkerEnd   = "end"
)

var (
	projectedRunStartBytes = json.RawMessage(`{"type":"agent_start"}`)
	projectedRunEndBytes   = json.RawMessage(`{"type":"agent_end"}`)
)

// runMarkerKey is the durable dedup identity of one run's start or end
// marker. The index of the run within the persona's committed history makes
// each key unique while remaining a pure function of the log — a recovered
// index can therefore rebuild it positionally.
func runMarkerKey(personaID, kind string, index uint64) [sha256.Size]byte {
	h := sha256.New()
	h.Write([]byte("sumi-core-direct-chat\x00run-" + kind + "\x00"))
	h.Write([]byte(personaID))
	fmt.Fprintf(h, "\x00%d", index)
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	return key
}

// foldRunMarkerLocked folds the committed run-marker state forward for one
// event line. It runs on every path that observes a committed line — tail
// refresh and both append paths — so the counters in
// personalityAgentLogState always describe the durable log, not one
// projector's view of it.
func foldRunMarkerLocked(st *personalityAgentLogState, event json.RawMessage) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(event, &head); err != nil {
		return
	}
	switch head.Type {
	case "agent_start":
		st.runStarts++
		st.runOpen = true
	case "agent_end":
		st.runOpen = false
	}
}

// dedupIndexPath is the per-persona sidecar of projected-event dedup keys.
// The index is an ordered preimage stream, not a positional map: non-projected
// writers (the runtime path does not maintain the index) interleave event
// lines without index records. Missing coverage is recovered by scanning the
// committed log — anonymous run markers get their deterministic positional
// key (the k-th committed agent_start's identity is runMarkerKey("start",k)),
// every other line is covered by hashing its stored inner event.
func (g *BrowserJournal) dedupIndexPath(personalityAgentID string) string {
	return filepath.Join(g.dir, "events-"+safeFileID(personalityAgentID)+".dedup")
}

func (g *BrowserJournal) projectedKeySet(
	personalityAgentID string,
	file durableFileHandle,
	st *personalityAgentLogState,
) (map[[sha256.Size]byte]struct{}, durableFileHandle, error) {
	index, err := g.newFile(g.dedupIndexPath(personalityAgentID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	size, err := index.Seek(0, io.SeekEnd)
	if err != nil {
		index.Close()
		return nil, nil, fmt.Errorf("size dedup index: %w", err)
	}
	// A torn index tail is dropped; the lines it described are re-covered by
	// content hashing below.
	if torn := size % sha256.Size; torn != 0 {
		if err := index.Truncate(size - torn); err != nil {
			index.Close()
			return nil, nil, fmt.Errorf("truncate torn dedup index: %w", err)
		}
		if err := index.Sync(); err != nil {
			index.Close()
			return nil, nil, fmt.Errorf("sync dedup index truncation: %w", err)
		}
		size -= torn
	}
	covered := size / sha256.Size
	if covered > int64(st.eventSeq) {
		// Phantom preimages of events that never committed.
		if err := index.Truncate(int64(st.eventSeq) * sha256.Size); err != nil {
			index.Close()
			return nil, nil, fmt.Errorf("truncate phantom dedup keys: %w", err)
		}
		if err := index.Sync(); err != nil {
			index.Close()
			return nil, nil, err
		}
		covered = int64(st.eventSeq)
	}
	keys := make(map[[sha256.Size]byte]struct{}, st.eventSeq)
	if covered > 0 {
		raw := make([]byte, covered*sha256.Size)
		if _, err := index.Seek(0, io.SeekStart); err != nil {
			index.Close()
			return nil, nil, fmt.Errorf("seek dedup index: %w", err)
		}
		if _, err := io.ReadFull(index, raw); err != nil {
			index.Close()
			return nil, nil, fmt.Errorf("read dedup index: %w", err)
		}
		for i := int64(0); i < covered; i++ {
			var k [sha256.Size]byte
			copy(k[:], raw[i*sha256.Size:(i+1)*sha256.Size])
			keys[k] = struct{}{}
		}
	}
	if covered < int64(st.eventSeq) {
		// Rebuild missing index coverage from committed events after an interrupted write.
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			index.Close()
			return nil, nil, fmt.Errorf("seek event log for dedup rebuild: %w", err)
		}
		var backfill bytes.Buffer
		var seq, startOrd, endOrd uint64
		r := bufio.NewReader(file)
		for {
			line, readErr := r.ReadBytes('\n')
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				var rec durableEventRecord
				if err := json.Unmarshal(trimmed, &rec); err != nil {
					index.Close()
					return nil, nil, fmt.Errorf("parse committed event for dedup rebuild: %w", err)
				}
				seq++
				var k [sha256.Size]byte
				var head struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal(rec.Event.Event, &head)
				switch head.Type {
				case "agent_start":
					k = runMarkerKey(personalityAgentID, "start", startOrd)
					startOrd++
				case "agent_end":
					k = runMarkerKey(personalityAgentID, "end", endOrd)
					endOrd++
				default:
					k = sha256.Sum256(rec.Event.Event)
					if dk, ok := commandDispositionDedupKey(rec.Event.Event); ok {
						k = dk
					}
				}
				if int64(seq) > covered {
					keys[k] = struct{}{}
					backfill.Write(k[:])
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				index.Close()
				return nil, nil, fmt.Errorf("read event log for dedup rebuild: %w", readErr)
			}
		}
		if backfill.Len() > 0 {
			// Truncations above do not move the file offset: a torn or
			// phantom drop to zero leaves it at the old size, and the
			// write would land mid-file leaving a garbage leading record
			// and every preimage permanently misaligned. Reposition at
			// the post-truncation end before writing.
			if _, err := index.Seek(0, io.SeekEnd); err != nil {
				index.Close()
				return nil, nil, fmt.Errorf("seek dedup index end before backfill: %w", err)
			}
			if _, err := index.Write(backfill.Bytes()); err != nil {
				index.Close()
				return nil, nil, fmt.Errorf("backfill dedup index: %w", err)
			}
			if err := index.Sync(); err != nil {
				index.Close()
				return nil, nil, fmt.Errorf("sync dedup index backfill: %w", err)
			}
		}
	}
	if _, err := index.Seek(0, io.SeekEnd); err != nil {
		index.Close()
		return nil, nil, fmt.Errorf("position dedup index: %w", err)
	}
	return keys, index, nil
}

func (g *BrowserJournal) AppendProjectedEvents(
	ctx context.Context,
	personalityAgentID string,
	events []ProjectedEvent,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	if err := lockMutexContext(ctx, &g.mu); err != nil {
		return err
	}
	defer g.mu.Unlock()

	st := g.stateFor(personalityAgentID)
	file, err := g.newFile(g.eventPath(personalityAgentID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable event log for projected append: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()
	if err := g.refreshEventTailLocked(file, st, personalityAgentID); err != nil {
		return err
	}

	// Every emission decision is taken here, under the lock, against the
	// committed log — a stale or overlapping projector's cached view of run
	// state cannot mint a marker for a run index that is already closed or
	// skip one the log says is needed. Content dedup still runs against the
	// durable index; the index load is skipped for pure-marker batches.
	hasContent := false
	for _, pe := range events {
		if pe.RunMarker == "" {
			hasContent = true
			break
		}
	}
	var keys map[[sha256.Size]byte]struct{}
	var index durableFileHandle
	if hasContent {
		keys, index, err = g.projectedKeySet(personalityAgentID, file, st)
		if err != nil {
			return err
		}
		defer index.Close()
	} else {
		keys = make(map[[sha256.Size]byte]struct{})
	}

	// Pass 1: which content elements are new? Markers are decided in pass 2.
	// The staleness precondition (IfTail) is evaluated against the just-
	// refreshed committed tail — st.eventSeq/eventCRC do not change while the
	// batch emits, so an element decided on a stale observation is skipped
	// here and in pass 2 alike.
	commit := make([]bool, len(events))
	elemKey := make([][sha256.Size]byte, len(events))
	for i, pe := range events {
		if pe.RunMarker != "" {
			continue
		}
		if pe.IfTail != nil &&
			(st.eventSeq != pe.IfTail.Seq || st.eventCRC != pe.IfTail.CRC) {
			continue
		}
		key := pe.DedupKey
		if key == ([sha256.Size]byte{}) {
			key = sha256.Sum256(pe.Event)
			if dk, ok := commandDispositionDedupKey(pe.Event); ok {
				// A command_disposition emitted without an explicit
				// DedupKey still keys on its command_id — the same
				// identity index recovery reconstructs — so write and
				// recovery can never disagree about what the committed
				// line covers.
				key = dk
			}
		}
		elemKey[i] = key
		if _, seen := keys[key]; !seen {
			keys[key] = struct{}{}
			commit[i] = true
		}
	}
	// later[i] reports whether any content element after i will commit — a
	// run-start marker is only emitted when the batch actually lands content.
	later := make([]bool, len(events)+1)
	for i := len(events) - 1; i >= 0; i-- {
		later[i] = later[i+1] || commit[i]
	}

	var keyBuf bytes.Buffer
	var buf bytes.Buffer
	envelopes := make([]Envelope, 0, len(events))
	simOpen := st.runOpen
	simStarts := st.runStarts
	for i, pe := range events {
		if pe.IfTail != nil &&
			(st.eventSeq != pe.IfTail.Seq || st.eventCRC != pe.IfTail.CRC) {
			// The observation this element was decided on is no longer
			// current — skip without consuming a seq or marker identity.
			continue
		}
		var eventBytes json.RawMessage
		var key [sha256.Size]byte
		switch pe.RunMarker {
		case "":
			if !commit[i] {
				continue
			}
			eventBytes, key = pe.Event, elemKey[i]
		case RunMarkerStart:
			if simOpen || !later[i+1] {
				continue
			}
			eventBytes = projectedRunStartBytes
			key = runMarkerKey(personalityAgentID, "start", simStarts)
			simOpen = true
			simStarts++
		case RunMarkerEnd:
			if !simOpen {
				continue
			}
			eventBytes = projectedRunEndBytes
			key = runMarkerKey(personalityAgentID, "end", simStarts-1)
			simOpen = false
		default:
			return fmt.Errorf("projected event %d: unknown run marker %q", i, pe.RunMarker)
		}
		seq := st.eventSeq + uint64(len(envelopes)) + 1
		envelope := Envelope{
			Audience:           AudienceDirectChat,
			Seq:                &seq,
			PersonalityAgentID: personalityAgentID,
			Event:              eventBytes,
		}
		if err := validateEnvelope(envelope); err != nil {
			return fmt.Errorf("projected event %d: %w", i, err)
		}
		line, err := json.Marshal(durableEventRecord{Seq: seq, Event: envelope})
		if err != nil {
			return err
		}
		keyBuf.Write(key[:])
		buf.Write(line)
		buf.WriteByte('\n')
		envelopes = append(envelopes, envelope)
	}
	if len(envelopes) == 0 {
		return nil
	}
	if index == nil {
		// A pure-marker batch that actually emitted still needs the index —
		// a committed marker is part of the preimage stream. (The common
		// no-op marker batch above never touches it.)
		if _, index, err = g.projectedKeySet(personalityAgentID, file, st); err != nil {
			return err
		}
		defer index.Close()
	}

	// Preimage ordering: the dedup keys reach stable storage before the events
	// they describe. A crash between leaves phantom index records, which the
	// next load truncates — no fact is ever suppressed that did not commit.
	if _, err := index.Write(keyBuf.Bytes()); err != nil {
		return fmt.Errorf("write dedup index: %w", err)
	}
	if err := index.Sync(); err != nil {
		return fmt.Errorf("sync dedup index: %w", err)
	}

	preWriteOffset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	data := buf.Bytes()
	written, writeErr := file.Write(data)
	if writeErr != nil || written != len(data) {
		var opErr error
		if writeErr != nil {
			opErr = fmt.Errorf("write durable event log: %w", writeErr)
		} else {
			opErr = fmt.Errorf("short write to durable event log: wrote %d of %d bytes", written, len(data))
		}
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}
	if syncErr := file.Sync(); syncErr != nil {
		opErr := fmt.Errorf("sync durable event log: %w", syncErr)
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}

	st.eventSeq += uint64(len(envelopes))
	st.eventSize = preWriteOffset + int64(len(data))
	st.eventCRC = updateCRC(st.eventCRC, data)
	// The simulated run state becomes committed state only now that the
	// write+fsync landed.
	st.runStarts = simStarts
	st.runOpen = simOpen
	for _, envelope := range envelopes {
		g.updateAgentSessionStateLocked(personalityAgentID, envelope.Event)
	}
	return nil
}

func (g *BrowserJournal) CommitCommandDisposition(
	ctx context.Context,
	command CommandEnvelope,
	disposition json.RawMessage,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := command.Validate(); err != nil {
		return false, fmt.Errorf("validate command disposition commit: %w", err)
	}
	persisted, found, err := g.commands.GetCommand(
		ctx,
		command.PersonalityAgentID,
		command.Seq,
	)
	if err != nil {
		return false, fmt.Errorf("load command for disposition commit: %w", err)
	}
	if !found || persisted.CommandID != command.CommandID {
		return false, errors.New("command disposition commit does not match durable command log")
	}
	key := commandDispositionKey(command.CommandID)
	if dk, ok := commandDispositionDedupKey(disposition); !ok || dk != key {
		return false, errors.New("command disposition does not name the committed command")
	}
	if err := lockMutexContext(ctx, &g.mu); err != nil {
		return false, err
	}
	defer g.mu.Unlock()
	st := g.stateFor(command.PersonalityAgentID)
	file, err := g.newFile(g.eventPath(command.PersonalityAgentID), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return false, err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return false, fmt.Errorf("lock durable event log for command disposition: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()
	if err := g.refreshEventTailLocked(file, st, command.PersonalityAgentID); err != nil {
		return false, err
	}
	keys, index, err := g.projectedKeySet(command.PersonalityAgentID, file, st)
	if err != nil {
		return false, err
	}
	defer index.Close()
	if _, seen := keys[key]; seen {
		return false, nil
	}
	seq := st.eventSeq + 1
	envelope := Envelope{
		Audience:           AudienceDirectChat,
		Seq:                &seq,
		PersonalityAgentID: command.PersonalityAgentID,
		Event:              disposition,
	}
	if err := validateEnvelope(envelope); err != nil {
		return false, fmt.Errorf("command disposition event: %w", err)
	}
	line, err := json.Marshal(durableEventRecord{Seq: seq, Event: envelope})
	if err != nil {
		return false, err
	}
	// Preimage ordering: the dedup key reaches stable storage before the
	// event it describes, so a crash between leaves a phantom index record
	// the next load truncates — never a suppressed receipt.
	if _, err := index.Write(key[:]); err != nil {
		return false, fmt.Errorf("write dedup index: %w", err)
	}
	if err := index.Sync(); err != nil {
		return false, fmt.Errorf("sync dedup index: %w", err)
	}
	preWriteOffset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return false, err
	}
	data := append(line, '\n')
	written, writeErr := file.Write(data)
	if writeErr != nil || written != len(data) {
		var opErr error
		if writeErr != nil {
			opErr = fmt.Errorf("write durable event log: %w", writeErr)
		} else {
			opErr = fmt.Errorf("short write to durable event log: wrote %d of %d bytes", written, len(data))
		}
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return false, rbErr
		}
		return false, opErr
	}
	if syncErr := file.Sync(); syncErr != nil {
		opErr := fmt.Errorf("sync durable event log: %w", syncErr)
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return false, rbErr
		}
		return false, opErr
	}
	st.eventSeq = seq
	st.eventSize = preWriteOffset + int64(len(data))
	st.eventCRC = updateCRC(st.eventCRC, data)
	foldRunMarkerLocked(st, disposition)
	g.updateAgentSessionStateLocked(command.PersonalityAgentID, disposition)
	return true, nil
}

func (g *BrowserJournal) appendDurableEventLocked(
	ctx context.Context,
	personalityAgentID string,
	record durableEventRecord,
) error {
	st := g.stateFor(personalityAgentID)
	path := g.eventPath(personalityAgentID)
	file, err := g.newFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable event log for append: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()

	if err := g.refreshEventTailLocked(file, st, personalityAgentID); err != nil {
		return err
	}
	if record.Seq != st.eventSeq+1 {
		return fmt.Errorf("event seq is not contiguous: got %d, want %d", record.Seq, st.eventSeq+1)
	}
	if record.Event.Seq == nil || *record.Event.Seq != record.Seq {
		return fmt.Errorf("durable event record seq mismatch: outer %d, inner %v", record.Seq, record.Event.Seq)
	}
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data := append(line, '\n')

	preWriteOffset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(data)
	if writeErr != nil || written != len(data) {
		var opErr error
		if writeErr != nil {
			opErr = fmt.Errorf("write durable event log: %w", writeErr)
		} else {
			opErr = fmt.Errorf("short write to durable event log: wrote %d of %d bytes", written, len(data))
		}
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}
	if syncErr := file.Sync(); syncErr != nil {
		opErr := fmt.Errorf("sync durable event log: %w", syncErr)
		if rbErr := rollbackDurableFile(file, preWriteOffset, opErr); rbErr != nil {
			return rbErr
		}
		return opErr
	}

	st.eventSeq = record.Seq
	st.eventSize = preWriteOffset + int64(len(data))
	st.eventCRC = updateCRC(st.eventCRC, data)
	foldRunMarkerLocked(st, record.Event.Event)
	return nil
}

func (g *BrowserJournal) updateAgentSessionStateLocked(personalityAgentID string, event json.RawMessage) {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	g.applyEventStateLocked(personalityAgentID, event)
}

func (g *BrowserJournal) applyEventStateLocked(personalityAgentID string, event json.RawMessage) {
	type eventHead struct {
		Type      string `json:"type"`
		RequestID string `json:"request_id"`
		Request   struct {
			ID string `json:"id"`
		} `json:"request"`
	}
	var head eventHead
	if err := json.Unmarshal(event, &head); err != nil {
		return
	}

	switch head.Type {
	case "agent_start":
		// One run spans turn replacement during hard/soft steering as well as
		// assistant message, tool, and continuation boundaries. Only AgentEnd
		// closes the abort-admission window.
		g.runInFlight[personalityAgentID] = true
	case "agent_end":
		g.runInFlight[personalityAgentID] = false
	case "approval_requested":
		if head.Request.ID == "" {
			return
		}
		if g.pendingApprovals[personalityAgentID] == nil {
			g.pendingApprovals[personalityAgentID] = make(map[string]bool)
		}
		g.pendingApprovals[personalityAgentID][head.Request.ID] = true
	case "approval_resolved":
		if head.RequestID == "" {
			return
		}
		if g.pendingApprovals[personalityAgentID] != nil {
			delete(g.pendingApprovals[personalityAgentID], head.RequestID)
			if len(g.pendingApprovals[personalityAgentID]) == 0 {
				delete(g.pendingApprovals, personalityAgentID)
			}
		}
	}
}

// RefreshDurableEventTail advances this process's cached view of the committed
// event log under the exclusive event-file lock: records another writer
// appended are folded into run and session-guard state, and a torn or
// unterminated tail left by a crashed writer is truncated exactly as the
// append path repairs it. Read/guard paths that must reflect committed state
// call this before consulting the in-memory maps. Malformed complete records
// and non-contiguous data still fail closed.
func (g *BrowserJournal) RefreshDurableEventTail(ctx context.Context, personalityAgentID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePersonalityAgentID(personalityAgentID); err != nil {
		return err
	}
	if err := lockMutexContext(ctx, &g.mu); err != nil {
		return err
	}
	defer g.mu.Unlock()
	st := g.stateFor(personalityAgentID)
	file, err := g.newFile(g.eventPath(personalityAgentID), os.O_RDWR, 0o600)
	if os.IsNotExist(err) {
		// No committed log: guards must reflect an empty history.
		g.resetEventTailLocked(st, personalityAgentID)
		st.tailObserved = true
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	if err := flockContext(ctx, file.Fd(), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock durable event log for tail refresh: %w", err)
	}
	defer func() { _ = unlockDurableFile(file) }()
	return g.refreshEventTailLocked(file, st, personalityAgentID)
}

// EnsureAgentSessionStateRebuilt refreshes the in-flight and pending-approval
// command guard state for personalityAgentID from the committed event log.
// It is called by the browser WebSocket before command admission begins so
// that guards remain authoritative across API process restarts — and against
// writers on other processes: every call folds the newly committed tail
// rather than latching a one-shot rebuild, so a pending approval committed
// by a sibling API process becomes visible without a restart. If the durable
// log is corrupt, non-contiguous, or otherwise unreadable, refresh returns an
// error and the caller must fail closed rather than admitting commands.
func (g *BrowserJournal) EnsureAgentSessionStateRebuilt(ctx context.Context, personalityAgentID string) error {
	if err := g.RefreshDurableEventTail(ctx, personalityAgentID); err != nil {
		return fmt.Errorf("rebuild agent session state: %w", err)
	}
	return nil
}

// ClaimIdleRuntime excludes durable event publication while the manager
// reserves its stop. Lock order is gateway.mu -> manager.mu; process stopping
// must happen only after this method returns. Browser admission touches/ensures
// the manager before appending a command, so the manager also rechecks activity.

// HasPendingRuntimeWork reconstructs accepted, nonterminal commands and unclosed
// runs from durable evidence. The idle probe is read-only: it neither reserves
// a stop nor publishes activity. A concurrent completion can make this hint
// stale; the caller must use ordinary fenced runtime admission, never replay
// commands or effects itself.

// IsRunInFlight reports whether a durable agent_start has not yet been closed
// by agent_end. It is used by the browser command guard to reject meaningless
// aborts without closing the window during tool execution, continuation calls,
// or hard/soft-steer turn replacement.
func (g *BrowserJournal) IsRunInFlight(personalityAgentID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.runInFlight[personalityAgentID]
}

// IsApprovalPending reports whether an approval with requestID is still awaiting
// a decision in the agent session. It is used by the browser WebSocket to reject
// approval_decision commands for unknown or already-resolved requests.
func (g *BrowserJournal) IsApprovalPending(personalityAgentID, requestID string) bool {
	if requestID == "" {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.pendingApprovals[personalityAgentID] != nil && g.pendingApprovals[personalityAgentID][requestID]
}

// resetEventTailLocked drops the cached committed-tail position so the caller
// rescans the log from the beginning. Session guards derived from the same
// committed records are cleared alongside it: the rescan refolds them from
// scratch, and a stale entry must never outlive the history that minted it.
func (g *BrowserJournal) resetEventTailLocked(st *personalityAgentLogState, personalityAgentID string) {
	st.eventSeq = 0
	st.eventSize = 0
	st.eventCRC = 0
	st.runStarts = 0
	st.runOpen = false
	st.tailObserved = false
	st.eventStat = eventFileStat{}
	st.eventStatVerifiedNS = 0
	st.eventStatTrusted = false
	g.stateMu.Lock()
	delete(g.pendingApprovals, personalityAgentID)
	delete(g.runInFlight, personalityAgentID)
	g.stateMu.Unlock()
}

func (g *BrowserJournal) refreshEventTailLocked(file durableFileHandle, st *personalityAgentLogState, personalityAgentID string) error {
	// An idle log is the common case — the direct-chat projector checks it
	// every sweep. Under the metadata fast path contract (eventStatTrustAge)
	// a trusted, unchanged fingerprint proves the tail was folded from the
	// file's current bytes, so re-reading the whole lifetime log is skipped.
	if st.eventStatTrusted && g.now().UnixNano() >= st.eventStatVerifiedNS {
		if current, ok := statEventFile(file); ok && current == st.eventStat && current.size == st.eventSize {
			return nil
		}
	}
	st.eventStatTrusted = false
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek durable event log: %w", err)
	}
	if size == st.eventSize {
		// File size is unchanged, but another process may have truncated and
		// rewritten to the same length. Verify the cached CRC before trusting
		// the in-memory tail state.
		crc, err := crc32OfFilePrefix(file, size)
		if err != nil {
			return fmt.Errorf("checksum durable event log prefix: %w", err)
		}
		if crc == st.eventCRC {
			g.noteVerifiedEventFileLocked(file, st)
			return nil
		}
		g.resetEventTailLocked(st, personalityAgentID)
	} else if size < st.eventSize {
		g.resetEventTailLocked(st, personalityAgentID)
	}
	if st.eventSize > 0 && size > st.eventSize {
		// Before scanning an appended tail, confirm the existing prefix has
		// not been rewritten underneath us.
		prefixCRC, err := crc32OfFilePrefix(file, st.eventSize)
		if err != nil {
			return fmt.Errorf("checksum durable event log prefix: %w", err)
		}
		if prefixCRC != st.eventCRC {
			g.resetEventTailLocked(st, personalityAgentID)
		}
	}

	start := st.eventSize
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("seek durable event log for tail refresh: %w", err)
	}

	r := bufio.NewReader(file)
	offset := start
	last := st.eventSeq
	crc := st.eventCRC
	for {
		lineStart := offset
		line, readErr := r.ReadBytes('\n')
		if len(line) > 0 {
			offset += int64(len(line))
			crc = updateCRC(crc, line)
		}

		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return fmt.Errorf("read durable event log: %w", readErr)
			}
			continue
		}

		var existing durableEventRecord
		if err := json.Unmarshal(trimmed, &existing); err != nil {
			if readErr == io.EOF && isIncompleteJSONError(err) {
				if truncErr := file.Truncate(lineStart); truncErr != nil {
					return fmt.Errorf("truncate partial durable event tail: %w", truncErr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					return fmt.Errorf("sync after truncating partial durable event tail: %w", syncErr)
				}
				crc, err = crc32OfFilePrefix(file, lineStart)
				if err != nil {
					return fmt.Errorf("checksum truncated durable event log: %w", err)
				}
				offset = lineStart
				break
			}
			if readErr == io.EOF {
				return fmt.Errorf("decode durable event log: final record is malformed but complete: %w", err)
			}
			return fmt.Errorf("decode durable event log: %w", err)
		}
		if existing.Seq != last+1 {
			return fmt.Errorf("durable event log is non-contiguous: got %d after %d", existing.Seq, last)
		}
		// The same envelope checks eventCatchUpScan enforces on replay:
		// a record this file would refuse to replay must not fold into
		// session guards or be appended after. These fail closed with the
		// record's bytes preserved — only a torn tail may be truncated.
		if existing.Event.Seq == nil || *existing.Event.Seq != existing.Seq {
			return fmt.Errorf("durable event record seq mismatch: outer %d, inner %v", existing.Seq, existing.Event.Seq)
		}
		if existing.Event.PersonalityAgentID != personalityAgentID {
			return fmt.Errorf("durable event record personality agent mismatch: got %q, want %q", existing.Event.PersonalityAgentID, personalityAgentID)
		}
		last = existing.Seq
		foldRunMarkerLocked(st, existing.Event.Event)
		// Session guards follow the same committed-state rule as run
		// markers: every record another writer appended since this process
		// last looked is folded now, under the event lock, so command
		// admission on this process sees them without waiting for a restart.
		g.updateAgentSessionStateLocked(personalityAgentID, existing.Event.Event)

		if readErr == io.EOF {
			if len(line) > 0 && line[len(line)-1] != '\n' {
				if _, werr := file.Write([]byte{'\n'}); werr != nil {
					return fmt.Errorf("repair missing trailing newline in durable event log: %w", werr)
				}
				if syncErr := file.Sync(); syncErr != nil {
					return fmt.Errorf("sync repaired durable event log trailing newline: %w", syncErr)
				}
				crc = updateCRC(crc, []byte{'\n'})
				offset += 1
			}
			break
		}
		if readErr != nil {
			return fmt.Errorf("read durable event log: %w", readErr)
		}
	}

	st.eventSeq = last
	st.eventSize = offset
	st.eventCRC = crc
	st.tailObserved = true
	g.noteVerifiedEventFileLocked(file, st)
	return nil
}

// noteVerifiedEventFileLocked records the fingerprint of an event file whose
// content st has just verified in full under the exclusive lock. It is
// trusted only under the metadata fast path contract (eventStatTrustAge):
// a supported filesystem, exactly the verified length, and a change time
// already older than eventStatTrustAge.
func (g *BrowserJournal) noteVerifiedEventFileLocked(file durableFileHandle, st *personalityAgentLogState) {
	st.eventStatTrusted = false
	if !g.eventStatFastPath {
		return
	}
	fingerprint, ok := statEventFile(file)
	if !ok || fingerprint.size != st.eventSize {
		return
	}
	now := g.now().UnixNano()
	st.eventStat = fingerprint
	st.eventStatVerifiedNS = now
	st.eventStatTrusted = now-fingerprint.ctimeNS > int64(eventStatTrustAge)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary file for atomic write: %w", err)
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		return fmt.Errorf("set temporary file permissions: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}
	removeTmp = false
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("publish file atomically: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open runtime-state parent directory after rename: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("sync runtime-state parent directory after rename: %w", err)
	}
	return nil
}

func unlockDurableFile(f durableFileHandle) error {
	if f == nil {
		return nil
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}

func rollbackDurableFile(f durableFileHandle, offset int64, origErr error) error {
	var truncErr, syncErr error
	if f != nil {
		truncErr = f.Truncate(offset)
		syncErr = f.Sync()
	}
	if truncErr != nil || syncErr != nil {
		return fmt.Errorf("append failure %v; rollback could not be confirmed (truncate=%v, sync=%v)", origErr, truncErr, syncErr)
	}
	return nil
}

func stringPointerEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// quarantineDurableStateSuffix marks state this process cannot verify. It must
// not match the "runtime-*.json" scan so a quarantined file is never rescanned
// and never re-quarantined.
const quarantineDurableStateSuffix = ".unverifiable"

func (g *BrowserJournal) eventPath(personalityAgentID string) string {
	return filepath.Join(g.dir, "events-"+safeFileID(personalityAgentID)+".jsonl")
}
func safeFileID(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }

// LookupAdmission reconciles an already fsynced command without requiring the
// runtime to be Ready. Missing runtime state is not evidence of absent admission.
func (g *BrowserJournal) LookupAdmission(ctx context.Context, provenance IncomingProvenance, key string, command json.RawMessage) (CommandEnvelope, bool, error) {
	return g.commands.Lookup(ctx, provenance, key, command)
}

// PrepareAttention restores the continuing PA and pins it against idle reaping
// while its source authorization and final admission are checked by the caller.

func (g *BrowserJournal) directChatReadiness(ctx context.Context, personalityAgentID string) (directChatReadiness, error) {
	return directChatReadiness{ready: true}, ctx.Err()
}

func (g *BrowserJournal) appendWithIdempotencyStatus(ctx context.Context, provenance IncomingProvenance, key string, command json.RawMessage) (CommandEnvelope, bool, error) {
	return g.commands.appendWithIdempotencyStatus(ctx, provenance, key, command)
}
