//go:build linux

// Package composeanchor keeps a Compose invocation's process-group identity
// alive until every other member of that group has disappeared.
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
		case <-ticker.C:
		}
		if commandDone {
			reapExitedChildren()
		}

		if terminating {
			signal := syscall.SIGTERM
			if forcing {
				signal = syscall.SIGKILL
			}
			if err := signalOtherGroupMembers(pid, signal); err != nil {
				containmentErr = errors.Join(containmentErr, err)
			}
		}
		members, err := otherGroupMembers(pid)
		if err != nil {
			containmentErr = errors.Join(containmentErr, err)
			continue
		}
		if commandDone && len(members) == 0 {
			if terminating {
				return errors.Join(fmt.Errorf("anchored command terminated: %w", commandErr), containmentErr)
			}
			return errors.Join(commandErr, containmentErr)
		}
	}
}

func reapExitedChildren() {
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return
		}
	}
}

func signalOtherGroupMembers(group int, signal syscall.Signal) error {
	members, err := otherGroupMembers(group)
	if err != nil {
		return err
	}
	var result error
	for _, pid := range members {
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
		if checkErr == nil && member {
			if err := unix.PidfdSendSignal(pidfd, signal, nil, 0); err != nil && !errors.Is(err, syscall.ESRCH) {
				result = errors.Join(result, fmt.Errorf("signal group member %d: %w", pid, err))
			}
		}
		_ = unix.Close(pidfd)
	}
	return result
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
	file, err := os.Open(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return false, err
	}
	defer file.Close()
	line, err := bufio.NewReader(file).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	closeParen := strings.LastIndex(line, ") ")
	if closeParen < 0 {
		return false, errors.New("malformed process stat")
	}
	fields := strings.Fields(line[closeParen+2:])
	if len(fields) < 4 {
		return false, errors.New("short process stat")
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return false, err
	}
	return pgid == group, nil
}
