//go:build linux

// Package composeanchor keeps a Compose invocation's process tree attached to
// a child subreaper until the kernel reports that no descendant remains.
package composeanchor

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const pollInterval = 10 * time.Millisecond

// Run must be entered as a session and process-group leader. It stops itself
// before starting argv, so its parent can acquire a pidfd and acknowledge the
// stable anchor before any Compose code or helper can run.
func Run(argv []string) error {
	if len(argv) == 0 {
		return errors.New("compose anchor requires a command")
	}
	pid := os.Getpid()
	pgid, err := unix.Getpgid(0)
	if err != nil {
		return fmt.Errorf("read anchor process group: %w", err)
	}
	sid, err := unix.Getsid(0)
	if err != nil {
		return fmt.Errorf("read anchor session: %w", err)
	}
	if pid != pgid || pid != sid {
		return fmt.Errorf("compose anchor is not an isolated session leader: pid=%d pgid=%d sid=%d", pid, pgid, sid)
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("make compose anchor a child subreaper: %w", err)
	}

	signals := make(chan os.Signal, 8)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR2)
	defer signal.Stop(signals)
	if err := unix.Kill(pid, syscall.SIGSTOP); err != nil {
		return fmt.Errorf("stop compose anchor before launch: %w", err)
	}

	command := exec.Command(argv[0], argv[1:]...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: pid}
	if err := command.Start(); err != nil {
		return fmt.Errorf("start anchored command: %w", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()

	var commandErr error
	commandDone := false
	descendantsAbsent := false
	var descendantsDone <-chan error
	terminating := false
	forcing := false
	var containmentErr error
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case received := <-signals:
			terminating = true
			if received == syscall.SIGUSR2 {
				forcing = true
			}
		case commandErr = <-waited:
			commandDone = true
			done := make(chan error, 1)
			descendantsDone = done
			go func() { done <- waitForDescendantAbsence() }()
		case err := <-descendantsDone:
			descendantsAbsent = true
			descendantsDone = nil
			containmentErr = errors.Join(containmentErr, err)
		case <-ticker.C:
		}

		if terminating {
			signal := syscall.SIGTERM
			if forcing {
				signal = syscall.SIGKILL
			}
			if !commandDone {
				if err := command.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) &&
					!errors.Is(err, syscall.ESRCH) {
					containmentErr = errors.Join(containmentErr, fmt.Errorf("signal anchored command: %w", err))
				}
			}
			if err := signalContainedDescendants(pid, signal); err != nil {
				containmentErr = errors.Join(containmentErr, err)
			}
		}
		if commandDone && descendantsAbsent {
			if terminating {
				return errors.Join(fmt.Errorf("anchored command terminated: %w", commandErr), containmentErr)
			}
			return errors.Join(commandErr, containmentErr)
		}
	}
}

// waitForDescendantAbsence uses the child-subreaper contract as the emptiness
// proof. Once the command leader has been reaped, every surviving descendant
// is either a direct child or has a living ancestor that is a direct child.
// The kernel reparents descendants before reporting their parent's exit, so
// wait4 returning ECHILD is an atomic proof that the anchored tree is empty.
func waitForDescendantAbsence() error {
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, 0, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.ECHILD) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("wait for anchored descendants: %w", err)
		}
		if pid <= 0 {
			return errors.New("wait for anchored descendants returned no child")
		}
	}
}

func signalContainedDescendants(group int, signal syscall.Signal) error {
	members, err := otherGroupMembers(group)
	if err != nil {
		return err
	}
	direct, err := directChildren(group)
	if err != nil {
		return err
	}
	seen := make(map[int]struct{}, len(members)+len(direct))
	for _, pid := range members {
		seen[pid] = struct{}{}
	}
	for pid := range direct {
		seen[pid] = struct{}{}
	}
	var result error
	for pid := range seen {
		pidfd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			if errors.Is(err, syscall.ESRCH) {
				continue
			}
			result = errors.Join(result, fmt.Errorf("open pidfd for group member %d: %w", pid, err))
			continue
		}
		// Recheck membership after acquiring the stable handle. A process that
		// exited or changed groups between the scan and pidfd_open is not ours.
		member, checkErr := processInGroup(pid, group)
		startTime, adopted := direct[pid]
		if adopted {
			identity, identityErr := readProcessStatIdentity(pid)
			if identityErr != nil || identity.ppid != group || identity.startTime != startTime {
				_ = unix.Close(pidfd)
				if identityErr != nil && !errors.Is(identityErr, os.ErrNotExist) {
					result = errors.Join(result, fmt.Errorf("recheck adopted child %d: %w", pid, identityErr))
				}
				continue
			}
		}
		if adopted || (checkErr == nil && member) {
			if err := unix.PidfdSendSignal(pidfd, signal, nil, 0); err != nil && !errors.Is(err, syscall.ESRCH) {
				result = errors.Join(result, fmt.Errorf("signal group member %d: %w", pid, err))
			}
		}
		_ = unix.Close(pidfd)
	}
	return result
}

func directChildren(anchor int) (map[int]uint64, error) {
	tasks, err := os.ReadDir(filepath.Join("/proc", strconv.Itoa(anchor), "task"))
	if err != nil {
		return nil, fmt.Errorf("scan anchor tasks for direct children: %w", err)
	}
	seen := make(map[int]uint64)
	for _, task := range tasks {
		raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(anchor), "task", task.Name(), "children"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("read anchor direct children: %w", err)
		}
		for _, field := range strings.Fields(string(raw)) {
			pid, err := strconv.Atoi(field)
			if err != nil || pid <= 0 {
				return nil, errors.New("kernel returned an invalid direct-child PID")
			}
			identity, err := readProcessStatIdentity(pid)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return nil, fmt.Errorf("inspect anchor direct child: %w", err)
			}
			if identity.ppid == anchor {
				seen[pid] = identity.startTime
			}
		}
	}
	return seen, nil
}

func otherGroupMembers(group int) ([]int, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, fmt.Errorf("scan /proc for anchored group: %w", err)
	}
	var members []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == group {
			continue
		}
		member, err := processInGroup(pid, group)
		if err == nil && member {
			members = append(members, pid)
		}
	}
	return members, nil
}

func processInGroup(pid, group int) (bool, error) {
	identity, err := readProcessStatIdentity(pid)
	if err != nil {
		return false, err
	}
	return identity.pgid == group, nil
}

type processStatIdentity struct {
	ppid      int
	pgid      int
	startTime uint64
}

func readProcessStatIdentity(pid int) (processStatIdentity, error) {
	file, err := os.Open(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processStatIdentity{}, err
	}
	defer file.Close()
	line, err := bufio.NewReader(file).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return processStatIdentity{}, err
	}
	closeParen := strings.LastIndex(line, ") ")
	if closeParen < 0 {
		return processStatIdentity{}, errors.New("malformed process stat")
	}
	fields := strings.Fields(line[closeParen+2:])
	if len(fields) < 20 {
		return processStatIdentity{}, errors.New("short process stat")
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return processStatIdentity{}, err
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return processStatIdentity{}, err
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTime == 0 {
		return processStatIdentity{}, errors.New("invalid process start time")
	}
	return processStatIdentity{ppid: ppid, pgid: pgid, startTime: startTime}, nil
}
