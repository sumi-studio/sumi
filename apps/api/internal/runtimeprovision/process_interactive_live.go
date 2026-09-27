package runtimeprovision

// Live tail for interactive TTY sessions.
//
// The durable scrollback is fed from the daemon's json-file journal
// (process_interactive_service.go). The daemon's log copier frames the
// stream on '\n': bytes after the last newline stay in the daemon's
// memory until a newline, a 16 KiB buffer fill, or the container's end.
// For a terminal that is the part a person is looking at — the shell
// prompt, the echo of each typed character, `read -p` questions, `\r`
// progress lines. Measured on Docker 29.7.2 (2026-09-27): a prompt
// printed without a newline did not reach the journal for as long as no
// newline followed, so a fresh terminal showed no prompt until the
// person typed something.
//
// A TTY session therefore also gets a raw output attach, opened after
// `docker create` and before `docker start`, so its byte 0 is the
// stream's byte 0 — the same absolute offsets the journal-fed ttylog
// uses. The live tail keeps the bytes the journal has not delivered yet
// and serves them to readers past the durable end. It never writes the
// ttylog: the journal stays the only durable source, and every byte it
// commits is compared with the live bytes at the same offset. Any
// disagreement, a journaled gap (rotation, vanished journal), or an
// unbounded backlog switches the tail off for the rest of the session;
// readers then fall back to journal-only output, which is complete but
// shows a partial line only once it ends. A provisioner restart also
// runs journal-only for sessions that were already running: a
// re-opened attach starts at an unknown stream position.

import (
	"bytes"
	"context"
	"io"
	"sync"
	"unicode/utf8"
)

// liveTailMax bounds either stream's bytes held ahead of the other. The
// daemon flushes a partial line at 16 KiB, so a healthy journal is
// never this far behind; exceeding it means the journal pump stalled.
const liveTailMax = 4 << 20

// liveOutputBackend is implemented by backends that attach a TTY
// session's output stream at launch.
type liveOutputBackend interface {
	TakeProcessLiveOutput(operationID string) io.ReadCloser
}

// openProcessOutput attaches a raw output stream to a created (not yet
// started) TTY container. With a TTY the daemon sends one merged stream
// without multiplexing headers.
func (b *DockerBackend) openProcessOutput(ctx context.Context, o ProcessOperation) (io.ReadCloser, error) {
	conn, br, err := b.attachStream(ctx, o, "stream=1&stdin=0&stdout=1&stderr=1&logs=0", "process output attach")
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{br, conn}, nil
}

// TakeProcessLiveOutput hands over (once) the output attach LaunchProcess
// opened for the operation.
func (b *DockerBackend) TakeProcessLiveOutput(operationID string) io.ReadCloser {
	if v, ok := b.liveOutputs.LoadAndDelete(operationID); ok {
		return v.(io.ReadCloser)
	}
	return nil
}

// liveTail tracks the live stream against the journal-committed prefix.
// At most one of pending/verify is non-empty:
//   - pending: live bytes at [durable, next) the journal has not committed;
//   - verify: committed bytes at [next, durable) the live stream has not
//     delivered yet (the copier can read a chunk before this reader does).
type liveTail struct {
	mu      sync.Mutex
	on      bool
	durable int64
	next    int64
	pending []byte
	verify  []byte
	rc      io.ReadCloser
	// diverged journals a loss boundary when live and journal bytes
	// disagree: readers may already hold live bytes the journal does not
	// confirm, so the scrollback must carry an explicit marker.
	diverged func()
}

func (l *liveTail) offLocked() {
	l.on = false
	l.pending, l.verify = nil, nil
	if l.rc != nil {
		_ = l.rc.Close()
		l.rc = nil
	}
}

func (l *liveTail) divergeLocked() {
	served := len(l.pending) > 0 || l.next > l.durable
	l.offLocked()
	if served && l.diverged != nil {
		l.diverged()
	}
}

// off switches the tail off for the rest of the session.
func (l *liveTail) off() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.offLocked()
}

func (l *liveTail) active() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.on
}

// delivered records bytes read from the live stream.
func (l *liveTail) delivered(p []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.on {
		return
	}
	if n := min(len(p), len(l.verify)); n > 0 {
		if !bytes.Equal(p[:n], l.verify[:n]) {
			l.divergeLocked()
			return
		}
		l.verify = l.verify[n:]
		p = p[n:]
		l.next += int64(n)
	}
	l.pending = append(l.pending, p...)
	l.next += int64(len(p))
	if len(l.pending) > liveTailMax {
		l.offLocked()
	}
}

// committed records bytes the journal pump appended to the ttylog at
// absolute offset at. Called with the ttylog lock held.
func (l *liveTail) committed(at int64, p []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.on {
		return
	}
	if at != l.durable {
		l.divergeLocked()
		return
	}
	n := min(len(p), len(l.pending))
	if !bytes.Equal(p[:n], l.pending[:n]) {
		l.divergeLocked()
		return
	}
	l.pending = l.pending[n:]
	if len(p)-n > liveTailMax-len(l.verify) {
		l.offLocked()
		return
	}
	l.verify = append(l.verify, p[n:]...)
	l.durable += int64(len(p))
}

// read returns up to limit live bytes starting at absolute offset,
// cut at a UTF-8 boundary (the transport carries text; a torn rune is
// served with its remaining bytes on the next read). ok is false when
// the tail cannot serve that offset.
func (l *liveTail) read(offset int64, limit int) (data []byte, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.on || offset < l.durable || offset > l.next {
		return nil, false
	}
	p := l.pending[offset-l.durable:]
	if len(p) > limit {
		p = p[:limit]
	}
	p = completeRunes(p)
	return append([]byte(nil), p...), true
}

// completeRunes drops a trailing incomplete UTF-8 sequence (at most 3
// bytes). Invalid bytes that can never complete are kept.
func completeRunes(p []byte) []byte {
	for i := 1; i <= 3 && i <= len(p); i++ {
		c := p[len(p)-i]
		if c < 0x80 {
			return p
		}
		if utf8.RuneStart(c) {
			if !utf8.FullRune(p[len(p)-i:]) {
				return p[:len(p)-i]
			}
			return p
		}
	}
	return p
}

// startLive begins serving rc as the session's live tail. It is only
// called for a stream attached before the container started, so rc's
// first byte is absolute offset 0. The ttylog must still be a fresh,
// gap-free prefix of that stream; whatever the journal pump committed
// before this call becomes the first bytes to verify.
func (io_ *interactiveIO) startLive(rc io.ReadCloser) {
	io_.mu.Lock()
	defer io_.mu.Unlock()
	if io_.closed {
		_ = rc.Close()
		return
	}
	t := io_.tty
	l := &io_.live
	t.mu.Lock()
	ok := t.base == 0 && !t.gapped && t.total <= liveTailMax
	var committed []byte
	if ok && t.total > 0 {
		committed = make([]byte, t.total)
		if _, err := t.log.ReadAt(committed, 0); err != nil {
			ok = false
		}
	}
	if ok {
		l.mu.Lock()
		l.on, l.durable, l.next, l.verify, l.rc = true, t.total, 0, committed, rc
		// Called under the ttylog or tail lock; recordGap takes the
		// ttylog lock itself.
		l.diverged = func() {
			go func() {
				_ = t.recordGap("live terminal output disagreed with the output journal; output around here may be shown incorrectly")
			}()
		}
		l.mu.Unlock()
		t.onAppend, t.onGap = l.committed, l.off
	}
	t.mu.Unlock()
	if !ok {
		_ = rc.Close()
		return
	}
	go func() {
		// A disconnected attach is no longer an output source. The
		// journal still owns recovery, including any final partial line.
		defer l.off()
		buf := make([]byte, 32<<10)
		for {
			n, err := rc.Read(buf)
			if n > 0 {
				l.delivered(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()
}
