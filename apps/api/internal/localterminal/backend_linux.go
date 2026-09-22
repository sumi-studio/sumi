//go:build linux

package localterminal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
	"golang.org/x/sys/unix"
)

type record struct {
	Operation     runtimeprovision.ProcessOperation `json:"operation"`
	RequestDigest string                            `json:"request_digest"`
	Tail          []byte                            `json:"tail,omitempty"`
	Gap           bool                              `json:"gap,omitempty"`
}
type running struct {
	record
	command         *exec.Cmd
	terminal        *os.File
	readDone        chan struct{}
	done            chan struct{}
	attached        bool
	dirty           bool
	cancelRequested bool
	timedOut        bool
	timer           *time.Timer
}
type backend struct {
	mu         sync.Mutex
	cfg        Config
	lock       *os.File
	closed     bool
	operations map[string]*running
	wg         sync.WaitGroup
}

func New(cfg Config) (ProcessBackend, error) {
	if _, e := uuid.Parse(cfg.PersonaID); e != nil {
		return nil, errors.New("Local terminal requires a valid persona")
	}
	if !filepath.IsAbs(cfg.WorkspaceRoot) || !filepath.IsAbs(cfg.JournalRoot) {
		return nil, errors.New("Local terminal workspace and journal must be absolute")
	}
	if cfg.OutputLimit <= 0 {
		cfg.OutputLimit = 256 << 10
	}
	if cfg.OutputLimit < 4096 || cfg.OutputLimit > 4<<20 {
		return nil, errors.New("Local terminal output limit must be 4 KiB to 4 MiB")
	}
	for _, dir := range []string{cfg.WorkspaceRoot, cfg.JournalRoot} {
		if e := os.MkdirAll(dir, 0700); e != nil {
			return nil, e
		}
	}
	lock, e := os.OpenFile(filepath.Join(cfg.JournalRoot, "owner.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		lock.Close()
		return nil, errors.New("Local terminal journal already has a live owner")
	}
	b := &backend{cfg: cfg, lock: lock, operations: map[string]*running{}}
	// Loading a durable pre-launch marker never executes anything. A whole host
	// process restart cannot recover the PTY descriptor. We deliberately never
	// store or signal a PID from disk: that number may now belong to someone else.
	entries, e := os.ReadDir(cfg.JournalRoot)
	if e != nil {
		b.Close()
		return nil, fmt.Errorf("%w: %v", ErrJournal, e)
	}
	// Validate every record before rewriting any: when a later record is
	// untrusted, startup must leave ALL bytes unchanged — an earlier
	// valid nonterminal marker rewritten to indeterminate before the failure
	// would lose the very history the degraded run exists to preserve.
	loaded := make([]*running, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, e := os.ReadFile(filepath.Join(cfg.JournalRoot, entry.Name()))
		if e != nil {
			b.Close()
			return nil, fmt.Errorf("%w: %v", ErrJournal, e)
		}
		var rec record
		if e = json.Unmarshal(raw, &rec); e != nil || rec.Operation.PersonalityAgentID != cfg.PersonaID || entry.Name() != rec.Operation.OperationID+".json" || rec.Operation.StdoutBase < 0 || rec.Operation.StdoutBytes < rec.Operation.StdoutBase || rec.Operation.StdoutBytes-rec.Operation.StdoutBase != int64(len(rec.Tail)) {
			b.Close()
			return nil, fmt.Errorf("%w: invalid record %s; refusing to relaunch", ErrJournal, entry.Name())
		}
		loaded = append(loaded, &running{record: rec})
	}
	for _, r := range loaded {
		if !r.Operation.State.Terminal() {
			r.Operation.State = runtimeprovision.ProcessIndeterminate
			r.Operation.Error = "Local host restarted; PTY cannot be reattached; uncollected output may be lost and surviving descendants are not proven stopped"
			now := time.Now()
			r.Operation.FinishedAt = &now
			r.Gap = true
			if e = b.save(r); e != nil {
				b.Close()
				return nil, fmt.Errorf("%w: %v", ErrJournal, e)
			}
		}
		b.operations[r.Operation.OperationID] = r
	}
	return b, nil
}
func (b *backend) save(r *running) error {
	raw, e := json.Marshal(r.record)
	if e != nil {
		return e
	}
	file, e := os.CreateTemp(b.cfg.JournalRoot, ".terminal-")
	if e != nil {
		return e
	}
	name := file.Name()
	defer os.Remove(name)
	if _, e = file.Write(raw); e == nil {
		e = file.Sync()
	}
	closeErr := file.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, filepath.Join(b.cfg.JournalRoot, r.Operation.OperationID+".json")); e != nil {
		return e
	}
	dir, e := os.Open(b.cfg.JournalRoot)
	if e != nil {
		return e
	}
	defer dir.Close()
	if e = dir.Sync(); e == nil {
		r.dirty = false
	}
	return e
}
func (b *backend) workspace(request runtimeprovision.ProcessStartRequest) (string, error) {
	root, e := filepath.EvalSymlinks(b.cfg.WorkspaceRoot)
	if e != nil {
		return "", e
	}
	scope := filepath.Join(root, b.cfg.PersonaID)
	if e = os.MkdirAll(scope, 0700); e != nil {
		return "", e
	}
	scope, e = filepath.EvalSymlinks(scope)
	if e != nil {
		return "", e
	}
	if filepath.Dir(scope) != root {
		return "", runtimeprovision.ErrProcessWorkspace
	}
	cwd, e := filepath.EvalSymlinks(filepath.Join(scope, request.Cwd))
	if e != nil {
		return "", runtimeprovision.ErrProcessWorkspace
	}
	rel, e := filepath.Rel(scope, cwd)
	if e != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", runtimeprovision.ErrProcessWorkspace
	}
	return cwd, nil
}
func (b *backend) StartProcess(ctx context.Context, request runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error) {
	if e := request.Validate(); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	if request.PersonalityAgentID != b.cfg.PersonaID {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotFound
	}
	if !request.Interactive || !request.TTY {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotInteractive
	}
	if e := ctx.Err(); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return runtimeprovision.ProcessOperation{}, errors.New("Local terminal host closed")
	}
	id := runtimeprovision.ProcessOperationID(request.PersonalityAgentID, request.OriginatingToolCallID)
	raw, _ := json.Marshal(request)
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if r := b.operations[id]; r != nil {
		if !r.Operation.Tombstone && r.RequestDigest != digest {
			return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrConflict
		}
		return b.snapshot(r), nil
	}
	cwd, e := b.workspace(request)
	if e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	now := time.Now()
	r := &running{record: record{RequestDigest: digest, Operation: runtimeprovision.ProcessOperation{OperationID: id, PersonalityAgentID: request.PersonalityAgentID, OriginatingToolCallID: request.OriginatingToolCallID, Executable: request.Executable, Args: request.Args, Cwd: request.Cwd, WorkspaceBind: cwd, TimeoutSeconds: request.TimeoutSeconds, Interactive: true, TTY: true, State: runtimeprovision.ProcessAccepted, OccurredAt: now}}}
	// This fsync must precede process creation. If either reply or host is lost,
	// the durable identity prevents an automatic second shell.
	if e = b.save(r); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	b.operations[id] = r
	command := exec.Command(request.Executable, request.Args...)
	command.Dir = cwd
	command.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + filepath.Join(b.cfg.WorkspaceRoot, b.cfg.PersonaID), "LANG=C.UTF-8", "TERM=xterm-256color"}
	for k, v := range request.Env {
		command.Env = append(command.Env, k+"="+v)
	}
	terminal, e := pty.StartWithSize(command, &pty.Winsize{Cols: 80, Rows: 24})
	if e != nil {
		r.Operation.State = runtimeprovision.ProcessFailed
		r.Operation.Error = "Local PTY launch failed: " + e.Error()
		r.Operation.FinishedAt = &now
		_ = b.save(r)
		return b.snapshot(r), nil
	}
	// Pollable descriptors let Close/deadlines interrupt a blocked Read/Write.
	if terminal, e = pollablePTY(terminal); e != nil {
		if terminal != nil {
			_ = terminal.Close()
		}
		_ = command.Process.Kill()
		_ = command.Wait()
		r.Operation.State = runtimeprovision.ProcessFailed
		r.Operation.Error = "Local PTY could not enter nonblocking mode"
		r.Operation.FinishedAt = &now
		_ = b.save(r)
		return b.snapshot(r), nil
	}
	r.command = command
	r.terminal = terminal
	r.readDone = make(chan struct{})
	r.done = make(chan struct{})
	r.attached = true
	r.Operation.State = runtimeprovision.ProcessRunning
	r.Operation.StartedAt = &now
	// Failure to persist the running update does not undo a real launch. The
	// accepted marker remains sufficient to prevent relaunch after a crash.
	_ = b.save(r)
	b.wg.Add(2)
	go b.read(r)
	go b.wait(r)
	lifetime := request.TimeoutSeconds
	if lifetime <= 0 {
		lifetime = 8 * 3600
	}
	r.timer = time.AfterFunc(time.Duration(lifetime)*time.Second, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if !r.Operation.State.Terminal() {
			r.timedOut = true
			b.stop(r)
		}
	})
	return b.snapshot(r), nil
}
func (b *backend) read(r *running) {
	defer b.wg.Done()
	defer close(r.readDone)
	buffer := make([]byte, 32<<10)
	for {
		n, e := r.terminal.Read(buffer)
		b.mu.Lock()
		if n > 0 {
			r.Tail = append(r.Tail, buffer[:n]...)
			r.Operation.StdoutBytes += int64(n)
			if len(r.Tail) > b.cfg.OutputLimit {
				r.Tail = append([]byte(nil), r.Tail[len(r.Tail)-b.cfg.OutputLimit:]...)
			}
			r.Operation.StdoutBase = r.Operation.StdoutBytes - int64(len(r.Tail))
			r.dirty = true
		}
		if e != nil {
			r.attached = false
		}
		b.mu.Unlock()
		if e != nil {
			return
		}
	}
}
func (b *backend) wait(r *running) {
	defer b.wg.Done()
	defer close(r.done)
	e := r.command.Wait()
	// Descendants can keep the slave open after the shell exits. Drain a bounded
	// tail, then hang up this terminal; this is not proof that escaped descendants
	// stopped or that the workspace is physically quiescent.
	select {
	case <-r.readDone:
	case <-time.After(500 * time.Millisecond):
		_ = r.terminal.Close()
		<-r.readDone
	}
	_ = r.terminal.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.timer != nil {
		r.timer.Stop()
	}
	now := time.Now()
	r.Operation.FinishedAt = &now
	code := r.command.ProcessState.ExitCode()
	r.Operation.ExitCode = &code
	switch {
	case r.timedOut:
		r.Operation.State = runtimeprovision.ProcessFailed
		r.Operation.Error = "Local terminal lifetime timeout"
	case r.cancelRequested:
		r.Operation.State = runtimeprovision.ProcessCancelled
	case e != nil:
		r.Operation.State = runtimeprovision.ProcessFailed
		r.Operation.Error = "Local shell exited unsuccessfully"
	default:
		r.Operation.State = runtimeprovision.ProcessSucceeded
	}
	if e := b.save(r); e != nil {
		r.Operation.Error += "; final journal update failed"
	}
}
func (b *backend) snapshot(r *running) runtimeprovision.ProcessOperation {
	op := r.Operation
	op.OutputAttached = r.attached && !op.State.Terminal()
	op.StdoutTruncated = op.StdoutBase > 0
	op.Quiesced = op.Tombstone
	return op
}
func (b *backend) lookup(request runtimeprovision.ProcessLookupRequest) (*running, error) {
	if e := request.Validate(); e != nil {
		return nil, e
	}
	if request.PersonalityAgentID != b.cfg.PersonaID {
		return nil, runtimeprovision.ErrProcessNotFound
	}
	r := b.operations[request.OperationID]
	if r == nil {
		return nil, runtimeprovision.ErrProcessNotFound
	}
	return r, nil
}
func (b *backend) ProcessStatus(ctx context.Context, request runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, e := b.lookup(request)
	if e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	return b.snapshot(r), nil
}
func (b *backend) ReadProcessOutput(ctx context.Context, request runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error) {
	if e := request.Validate(); e != nil {
		return runtimeprovision.ProcessOutput{}, e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, e := b.lookup(request.ProcessLookupRequest)
	if e != nil {
		return runtimeprovision.ProcessOutput{}, e
	}
	if r.dirty {
		if e = b.save(r); e != nil {
			return runtimeprovision.ProcessOutput{}, e
		}
	}
	out := runtimeprovision.ProcessOutput{OperationID: request.OperationID, Stream: request.Stream, Offset: request.Offset, NextOffset: request.Offset, EOF: r.Operation.State.Terminal(), BaseOffset: r.Operation.StdoutBase, Truncated: r.Operation.StdoutBase > 0}
	if request.Stream == "stderr" {
		return out, nil
	}
	offset := request.Offset
	if offset < r.Operation.StdoutBase {
		offset = r.Operation.StdoutBase
		out.Gap = true
	}
	if offset > r.Operation.StdoutBytes {
		return out, nil
	}
	limit := request.Limit
	if limit == 0 {
		limit = 64 << 10
	}
	end := offset + int64(limit)
	if end > r.Operation.StdoutBytes {
		end = r.Operation.StdoutBytes
	}
	out.Content = string(r.Tail[offset-r.Operation.StdoutBase : end-r.Operation.StdoutBase])
	out.NextOffset = end
	out.EOF = out.EOF && end == r.Operation.StdoutBytes
	if r.Gap && r.Operation.StdoutBytes >= request.Offset {
		out.Gaps = []runtimeprovision.ProcessOutputGap{{At: r.Operation.StdoutBytes, Note: "Local host restarted; output after this retained boundary may be lost"}}
	}
	return out, nil
}
func (b *backend) WriteProcessInput(ctx context.Context, request runtimeprovision.ProcessInputRequest) (runtimeprovision.ProcessInputReceipt, error) {
	if e := request.Validate(); e != nil {
		return runtimeprovision.ProcessInputReceipt{}, e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, e := b.lookup(request.ProcessLookupRequest)
	if e != nil {
		return runtimeprovision.ProcessInputReceipt{}, e
	}
	if r.Operation.State.Terminal() || r.terminal == nil {
		return runtimeprovision.ProcessInputReceipt{}, runtimeprovision.ErrProcessNotInteractive
	}
	if e = ctx.Err(); e != nil {
		return runtimeprovision.ProcessInputReceipt{}, e
	}
	data := request.Data
	if request.EOF {
		data = append(append([]byte{}, data...), 4)
	}
	deadline := time.Now().Add(2 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = r.terminal.SetWriteDeadline(deadline)
	n, e := r.terminal.Write(data)
	_ = r.terminal.SetWriteDeadline(time.Time{})
	if e != nil || n != len(data) {
		return runtimeprovision.ProcessInputReceipt{Indeterminate: true, Detail: "PTY write did not complete; a prefix may have been accepted"}, nil
	}
	return runtimeprovision.ProcessInputReceipt{Delivered: true}, nil
}
func (b *backend) ResizeProcess(ctx context.Context, request runtimeprovision.ProcessResizeRequest) (runtimeprovision.ProcessOperation, error) {
	if e := request.Validate(); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, e := b.lookup(request.ProcessLookupRequest)
	if e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	if r.Operation.State.Terminal() || r.terminal == nil {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrProcessNotInteractive
	}
	if e = terminalControl(r.terminal, func(fd int) error {
		return unix.IoctlSetWinsize(fd, unix.TIOCSWINSZ, &unix.Winsize{Col: uint16(request.Cols), Row: uint16(request.Rows)})
	}); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	return b.snapshot(r), nil
}
func (b *backend) SignalProcess(ctx context.Context, request runtimeprovision.ProcessSignalRequest) (runtimeprovision.ProcessInputReceipt, error) {
	if e := request.Validate(); e != nil {
		return runtimeprovision.ProcessInputReceipt{}, e
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, e := b.lookup(request.ProcessLookupRequest)
	if e != nil {
		return runtimeprovision.ProcessInputReceipt{}, e
	}
	if r.Operation.State.Terminal() || r.command == nil {
		return runtimeprovision.ProcessInputReceipt{}, runtimeprovision.ErrProcessNotInteractive
	}
	signals := map[string]syscall.Signal{"INT": syscall.SIGINT, "QUIT": syscall.SIGQUIT, "TSTP": syscall.SIGTSTP, "TERM": syscall.SIGTERM, "HUP": syscall.SIGHUP, "KILL": syscall.SIGKILL, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2}
	name := strings.ToUpper(request.Signal)
	sig := signals[name]
	if name == "INT" || name == "QUIT" || name == "TSTP" {
		e = terminalControl(r.terminal, func(fd int) error { return unix.IoctlSetInt(fd, unix.TIOCSIG, int(sig)) })
	} else {
		e = r.command.Process.Signal(sig)
	}
	if e != nil {
		return runtimeprovision.ProcessInputReceipt{Indeterminate: true, Detail: "Local signal delivery could not be confirmed"}, nil
	}
	return runtimeprovision.ProcessInputReceipt{Delivered: true}, nil
}
func (b *backend) stop(r *running) {
	if r.command == nil || r.Operation.State.Terminal() {
		return
	}
	r.cancelRequested = true
	_ = r.command.Process.Signal(syscall.SIGHUP)
	_ = r.command.Process.Kill()
}
func (b *backend) CancelProcess(ctx context.Context, request runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	if b.closed {
		return runtimeprovision.ProcessOperation{}, errors.New("Local terminal host closed")
	}
	r, e := b.lookup(request)
	if errors.Is(e, runtimeprovision.ErrProcessNotFound) && request.TombstoneIfAbsent && request.PersonalityAgentID == b.cfg.PersonaID {
		now := time.Now()
		r = &running{record: record{Operation: runtimeprovision.ProcessOperation{OperationID: request.OperationID, PersonalityAgentID: request.PersonalityAgentID, State: runtimeprovision.ProcessCancelled, Tombstone: true, OccurredAt: now, FinishedAt: &now}}}
		if e = b.save(r); e != nil {
			return runtimeprovision.ProcessOperation{}, e
		}
		b.operations[request.OperationID] = r
	} else if e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	b.stop(r)
	return b.snapshot(r), nil
}

// ReleaseProcessTombstone is used only after the caller re-authorizes launch.
// A launched, failed, cancelled-after-launch, or indeterminate operation is
// execution history, not a releasable fence. Never forget it to permit replay.
func (b *backend) ReleaseProcessTombstone(ctx context.Context, request runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	if b.closed {
		return runtimeprovision.ProcessOperation{}, errors.New("Local terminal host closed")
	}
	r, e := b.lookup(request)
	if e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	if !r.Operation.Tombstone || r.Operation.State != runtimeprovision.ProcessCancelled || r.RequestDigest != "" || r.Operation.Executable != "" || r.Operation.StartedAt != nil || r.Operation.ExitCode != nil || r.Operation.StdoutBytes != 0 || len(r.Tail) != 0 || r.command != nil || r.terminal != nil {
		return runtimeprovision.ProcessOperation{}, runtimeprovision.ErrConflict
	}
	// Hold the mutex through unlink+directory fsync. A racing start/cancel
	// cannot interleave a new journal under this identity. On failure retain
	// the in-memory fence; do not silently allow a same-process retry to start.
	directory, e := os.Open(b.cfg.JournalRoot)
	if e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	defer directory.Close()
	if e = os.Remove(filepath.Join(b.cfg.JournalRoot, request.OperationID+".json")); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	if e = directory.Sync(); e != nil {
		return runtimeprovision.ProcessOperation{}, e
	}
	delete(b.operations, request.OperationID)
	return b.snapshot(r), nil
}

func (b *backend) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for _, r := range b.operations {
		b.stop(r)
		if r.terminal != nil {
			_ = r.terminal.Close()
		}
	}
	b.mu.Unlock()
	b.wg.Wait()
	if b.lock != nil {
		_ = unix.Flock(int(b.lock.Fd()), unix.LOCK_UN)
		return b.lock.Close()
	}
	return nil
}

// Fd switches an os.File to blocking mode. Rewrap a nonblocking duplicate
// after the PTY library finishes its setup and use RawConn for later ioctls.
func pollablePTY(original *os.File) (*os.File, error) {
	fd, e := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if e == nil {
		e = unix.SetNonblock(fd, true)
	}
	_ = original.Close()
	if e != nil {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		return nil, e
	}
	file := os.NewFile(uintptr(fd), "local-terminal")
	if e = file.SetReadDeadline(time.Time{}); e != nil {
		file.Close()
		return nil, e
	}
	return file, nil
}
func terminalControl(file *os.File, operation func(int) error) error {
	raw, e := file.SyscallConn()
	if e != nil {
		return e
	}
	var inner error
	if e = raw.Control(func(fd uintptr) { inner = operation(int(fd)) }); e != nil {
		return e
	}
	return inner
}
