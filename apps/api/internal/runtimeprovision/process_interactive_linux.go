//go:build linux

package runtimeprovision

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// journalInode identifies a journal file so the tailer detects
// rotation or replacement — a persisted inode that no longer matches
// the path means the bytes after the committed offset are gone.
func journalInode(st os.FileInfo) uint64 {
	if t, ok := st.Sys().(*syscall.Stat_t); ok {
		return t.Ino
	}
	return 0
}

// interactiveSignalNumbers resolves the allowlisted names to host
// signal numbers. Keep in sync with processSignalAllowlist.
var interactiveSignalNumbers = map[string]syscall.Signal{
	"INT": syscall.SIGINT, "TERM": syscall.SIGTERM, "HUP": syscall.SIGHUP,
	"QUIT": syscall.SIGQUIT, "KILL": syscall.SIGKILL, "TSTP": syscall.SIGTSTP,
	"USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2,
}

// signalProcessGroup delivers sig to the session's current foreground
// process group. The container's init is the session leader of the
// allocated PTY, so /proc/<init>/stat's tpgid field names the
// foreground job exactly as the line discipline sees it — a running
// build gets the signal, not indiscriminately PID 1. Every hop is
// verified against /proc cgroup membership: a stale or foreign group
// is an error, never a kill of an unrelated host process.
func (b *DockerBackend) signalProcessGroup(ctx context.Context, o ProcessOperation, sig string) error {
	num, ok := interactiveSignalNumbers[sig]
	if !ok {
		return fmt.Errorf("%w: signal not permitted", ErrInvalidProcessRequest)
	}
	cid, err := b.containerID(ctx, o)
	if err != nil {
		return err
	}
	init, err := b.containerInitPID(ctx, o)
	if err != nil {
		return err
	}
	pgid, err := containerForegroundPgrp(init, cid)
	if err != nil {
		return err
	}
	if err := syscall.Kill(-pgid, num); err != nil {
		return fmt.Errorf("%w: signal %s to group %d: %v", ErrSignalUnavailable, sig, pgid, err)
	}
	return nil
}

// containerID resolves the full container id for membership checks.
func (b *DockerBackend) containerID(ctx context.Context, o ProcessOperation) (string, error) {
	raw, err := b.processDocker(ctx, "inspect", "--format", "{{.Id}}", processContainer(o))
	if err != nil {
		return "", errors.Join(fmt.Errorf("%w: container lookup", ErrProcessNotFound), err)
	}
	id := strings.TrimSpace(string(raw))
	if id == "" {
		return "", fmt.Errorf("%w: container lookup", ErrProcessNotFound)
	}
	return id, nil
}

// containerInitPID resolves the container init's host pid.
func (b *DockerBackend) containerInitPID(ctx context.Context, o ProcessOperation) (int, error) {
	raw, err := b.processDocker(ctx, "inspect", "--format", "{{.State.Pid}}", processContainer(o))
	if err != nil {
		return 0, errors.Join(fmt.Errorf("%w: container init lookup", ErrProcessNotFound), err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return 0, fmt.Errorf("%w: container has no live init", ErrSignalUnavailable)
	}
	return pid, nil
}

// containerForegroundPgrp reads the init's foreground process group
// from /proc/<pid>/stat (tpgid, field 8). If no foreground job owns
// the terminal the field is 0 or stale, in which case the init's own
// process group is the honest target — the session leader's group is
// the foreground when the shell sits at a prompt. Both candidates are
// verified to still belong to the container's cgroup before use.
func containerForegroundPgrp(init int, cid string) (int, error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", init))
	if err != nil {
		return 0, fmt.Errorf("%w: container init stat: %v", ErrSignalUnavailable, err)
	}
	// comm is parenthesized and may itself contain spaces or parens;
	// fields after the last ')' are state(3) ppid(4) pgrp(5)
	// session(6) tty_nr(7) tpgid(8).
	i := strings.LastIndexByte(string(raw), ')')
	if i < 0 {
		return 0, fmt.Errorf("%w: malformed proc stat", ErrSignalUnavailable)
	}
	f := strings.Fields(string(raw)[i+1:])
	if len(f) < 6 {
		return 0, fmt.Errorf("%w: short proc stat", ErrSignalUnavailable)
	}
	pgrp, _ := strconv.Atoi(f[2])
	tpgid, _ := strconv.Atoi(f[5])
	for _, cand := range []int{tpgid, pgrp} {
		if cand <= 0 {
			continue
		}
		if procInContainer(cand, cid) {
			return cand, nil
		}
	}
	return 0, fmt.Errorf("%w: no foreground group inside container", ErrSignalUnavailable)
}

// procInContainer verifies that process group pgid is still a member
// of the named container before a group signal is sent to it — the
// kill target can never drift to an unrelated host process between
// inspection and delivery.
func procInContainer(pgid int, cid string) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pgid))
	if err != nil {
		return false
	}
	return strings.Contains(string(raw), cid)
}
