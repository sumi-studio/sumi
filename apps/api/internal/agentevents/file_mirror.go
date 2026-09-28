package agentevents

import (
	"errors"
	"io"
	"os"
)

// JournalFile is the handle the command and event journals write through.
// It is an alias of an unnamed interface so an implementation in another
// package (internal/journalmirror) satisfies FileMirror without an adapter.
type JournalFile = interface {
	io.Reader
	io.Writer
	io.Seeker
	Sync() error
	Truncate(size int64) error
	Close() error
	Fd() uintptr
}

// FileMirror extends journal durability beyond the local disk. Wrap returns
// a handle whose Sync succeeds only after its writes are durable in the
// mirror; WrapAtomicWrite does the same for whole-file replacements. Local
// POSIX semantics (flock, positional writes, rollback by truncation) are
// unchanged: the mirror is consulted only at the acknowledgement point.
type FileMirror interface {
	Wrap(path string, flag int, file JournalFile) (JournalFile, error)
	WrapAtomicWrite(write func(string, []byte, os.FileMode) error) func(string, []byte, os.FileMode) error
}

// replicationOnly reports whether a Sync error is a mirror's failure to
// replicate bytes whose local fsync succeeded (the mirror's error implements
// LocalDurable). The local file is then in the state the journal wrote, and
// the mirror repairs its copy at the next Sync; the journal's in-memory state
// stays correct without poisoning it.
func replicationOnly(err error) bool {
	var durable interface{ LocalDurable() bool }
	return errors.As(err, &durable) && durable.LocalDurable()
}
