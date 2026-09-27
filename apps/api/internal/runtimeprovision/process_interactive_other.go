//go:build !linux

package runtimeprovision

import (
	"context"
	"fmt"
	"os"
)

// journalInode has no portable identity off unix — interactive
// operation is a docker-on-linux path, so a zero inode simply
// degrades rotation detection to the size check.
func journalInode(st os.FileInfo) uint64 { return 0 }

// signalProcessGroup cannot resolve a container's foreground process
// group off-Linux (no /proc tpgid, no cgroup membership check). The
// typed error keeps a signal request honest rather than pretending a
// delivery that did not happen.
func (b *DockerBackend) signalProcessGroup(ctx context.Context, o ProcessOperation, sig string) error {
	return fmt.Errorf("%w: foreground-group signals require linux", ErrSignalUnavailable)
}
