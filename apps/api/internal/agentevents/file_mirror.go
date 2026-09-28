package agentevents

import (
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
