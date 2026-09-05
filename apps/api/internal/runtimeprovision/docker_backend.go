package runtimeprovision

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const maximumSupervisorCleanupBound = 2 * time.Hour
const defaultSupervisorPipeWait = time.Second
const supervisorControlFD = 3
const supervisorControlDrainJoinReserve = time.Second
const supervisorBoundAdvertisementWait = 100 * time.Millisecond

type nestedControlState uint8

const (
	nestedIdle nestedControlState = iota
	nestedStarted
	nestedVerified
)

// DockerBackend reaches Docker only through the root-owned supervisor. No
// Docker client or socket is exposed through the protocol or linked into API,
// runtime, executor, or broker.
type DockerBackend struct {
	supervisor       string
	baseEnvironment  []string
	operationTimeout time.Duration
	runner           commandRunner
}

type commandRunner interface {
	Run(context.Context, string, []string, []string) ([]byte, error)
}

type execCommandRunner struct {
	terminationGrace time.Duration
	pipeWait         time.Duration
}

type supervisorControlTracker struct {
	mu            sync.Mutex
	cleanupBound  time.Duration
	boundReady    chan struct{}
	boundOnce     sync.Once
	nestedPID     int
	nestedStart   uint64
	nestedPIDFD   int
	nestedState   nestedControlState
	supervisorPID int
	protocolErr   error
	closed        bool
}

func newSupervisorControlTracker() *supervisorControlTracker {
	return &supervisorControlTracker{boundReady: make(chan struct{}), nestedPIDFD: -1}
}

type nestedProcessIdentity struct {
	pid       int
	startTime uint64
}

type processIdentitySnapshot struct {
	state     byte
	ppid      int
	pgid      int
	sid       int
	startTime uint64
}

type supervisorCommandError struct {
	cause      error
	diagnostic string
}

func (err *supervisorCommandError) Error() string { return err.cause.Error() }
func (err *supervisorCommandError) Unwrap() error { return err.cause }

func (tracker *supervisorControlTracker) consume(control io.ReadWriter) error {
	acknowledge := func(event, value string) error {
		_, err := fmt.Fprintf(control, "ack-%s %s\n", event, value)
		return err
	}
	reject := func(event, value string) {
		_, _ = fmt.Fprintf(control, "reject-%s %s\n", event, value)
	}
	recordProtocolError := func(err error) {
		tracker.mu.Lock()
		tracker.protocolErr = errors.Join(tracker.protocolErr, err)
		tracker.mu.Unlock()
	}
	scanner := bufio.NewScanner(control)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			recordProtocolError(fmt.Errorf("malformed supervisor control record %q", scanner.Text()))
			continue
		}
		switch fields[0] {
		case "cleanup-bound-ms":
			value, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || value <= 0 {
				recordProtocolError(errors.New("invalid supervisor cleanup bound"))
				continue
			}
			if value > int64(maximumSupervisorCleanupBound/time.Millisecond) {
				recordProtocolError(errors.New("supervisor cleanup bound is out of range"))
				continue
			}
			bound := time.Duration(value) * time.Millisecond
			tracker.mu.Lock()
			if tracker.closed {
				tracker.mu.Unlock()
				continue
			}
			if tracker.cleanupBound != 0 {
				tracker.protocolErr = errors.Join(tracker.protocolErr, errors.New("duplicate supervisor cleanup bound"))
			} else {
				tracker.cleanupBound = bound
			}
			tracker.mu.Unlock()
			tracker.boundOnce.Do(func() { close(tracker.boundReady) })
		case "nested-start":
			identity, parseErr := parseNestedProcessIdentity(fields[1])
			if parseErr != nil {
				recordProtocolError(parseErr)
				continue
			}
			tracker.mu.Lock()
			if tracker.closed || tracker.nestedState != nestedIdle {
				tracker.protocolErr = errors.Join(tracker.protocolErr, errors.New("nested-start outside idle state"))
				tracker.mu.Unlock()
				reject(fields[0], fields[1])
				continue
			}
			pidfd, openErr := captureStableNestedProcess(identity, tracker.supervisorPID)
			if openErr != nil {
				tracker.protocolErr = errors.Join(tracker.protocolErr, fmt.Errorf("open nested anchor pidfd: %w", openErr))
				tracker.mu.Unlock()
				reject(fields[0], fields[1])
				continue
			}
			tracker.nestedPID = identity.pid
			tracker.nestedStart = identity.startTime
			tracker.nestedPIDFD = pidfd
			tracker.nestedState = nestedStarted
			tracker.mu.Unlock()
			if err := acknowledge(fields[0], fields[1]); err != nil {
				recordProtocolError(fmt.Errorf("acknowledge nested-start: %w", err))
			}
		case "nested-ready":
			identity, parseErr := parseNestedProcessIdentity(fields[1])
			if parseErr != nil {
				recordProtocolError(parseErr)
				continue
			}
			tracker.mu.Lock()
			if tracker.nestedState != nestedStarted || tracker.nestedPID != identity.pid ||
				tracker.nestedStart != identity.startTime {
				tracker.protocolErr = errors.Join(tracker.protocolErr, errors.New("nested-ready does not match the live started anchor"))
				tracker.mu.Unlock()
				reject(fields[0], fields[1])
				continue
			}
			if verifyErr := verifyNestedProcess(identity, tracker.supervisorPID); verifyErr != nil {
				tracker.protocolErr = errors.Join(tracker.protocolErr, fmt.Errorf("verify nested anchor: %w", verifyErr))
				tracker.mu.Unlock()
				reject(fields[0], fields[1])
				continue
			}
			tracker.nestedState = nestedVerified
			tracker.mu.Unlock()
			if err := acknowledge(fields[0], fields[1]); err != nil {
				recordProtocolError(fmt.Errorf("acknowledge nested-ready: %w", err))
			}
		case "nested-done":
			identity, parseErr := parseNestedProcessIdentity(fields[1])
			if parseErr != nil {
				recordProtocolError(parseErr)
				continue
			}
			tracker.mu.Lock()
			if tracker.nestedState != nestedVerified || tracker.nestedPID != identity.pid ||
				tracker.nestedStart != identity.startTime {
				tracker.protocolErr = errors.Join(tracker.protocolErr, errors.New("nested-done does not match the live verified anchor"))
				tracker.mu.Unlock()
				reject(fields[0], fields[1])
				continue
			}
			liveErr := unix.PidfdSendSignal(tracker.nestedPIDFD, 0, nil, 0)
			if liveErr == nil || !errors.Is(liveErr, syscall.ESRCH) {
				if liveErr == nil {
					liveErr = errors.New("nested anchor is still live")
				}
				tracker.protocolErr = errors.Join(tracker.protocolErr, fmt.Errorf("nested-done before anchor absence: %w", liveErr))
				tracker.mu.Unlock()
				reject(fields[0], fields[1])
				continue
			}
			tracker.mu.Unlock()
			if err := acknowledge(fields[0], fields[1]); err != nil {
				recordProtocolError(fmt.Errorf("acknowledge nested-done: %w", err))
				continue
			}
			tracker.mu.Lock()
			_ = unix.Close(tracker.nestedPIDFD)
			tracker.nestedPID = 0
			tracker.nestedStart = 0
			tracker.nestedPIDFD = -1
			tracker.nestedState = nestedIdle
			tracker.mu.Unlock()
		default:
			recordProtocolError(fmt.Errorf("unknown supervisor control event %q", fields[0]))
		}
	}
	tracker.mu.Lock()
	if err := scanner.Err(); err != nil {
		tracker.protocolErr = errors.Join(tracker.protocolErr, fmt.Errorf("scan supervisor control: %w", err))
	}
	if tracker.nestedState != nestedIdle {
		tracker.protocolErr = errors.Join(tracker.protocolErr, errors.New("supervisor control closed with a live nested anchor"))
	}
	err := tracker.protocolErr
	tracker.mu.Unlock()
	return err
}

func parseNestedProcessIdentity(value string) (nestedProcessIdentity, error) {
	pidText, startText, ok := strings.Cut(value, ":")
	if !ok || strings.Contains(startText, ":") {
		return nestedProcessIdentity{}, errors.New("invalid nested anchor identity")
	}
	pid64, err := strconv.ParseInt(pidText, 10, 32)
	if err != nil || pid64 <= 0 || pid64 > 1<<30 {
		return nestedProcessIdentity{}, errors.New("nested PID is out of range")
	}
	startTime, err := strconv.ParseUint(startText, 10, 64)
	if err != nil || startTime == 0 {
		return nestedProcessIdentity{}, errors.New("nested start time is invalid")
	}
	return nestedProcessIdentity{pid: int(pid64), startTime: startTime}, nil
}

func captureStableNestedProcess(identity nestedProcessIdentity, supervisorPID int) (int, error) {
	before, err := readProcessIdentity(identity.pid)
	if err != nil {
		return -1, err
	}
	if err := validateNestedProcessSnapshot(before, identity, supervisorPID); err != nil {
		return -1, err
	}
	pidfd, err := unix.PidfdOpen(identity.pid, 0)
	if err != nil {
		return -1, err
	}
	after, err := readProcessIdentity(identity.pid)
	if err != nil {
		_ = unix.Close(pidfd)
		return -1, err
	}
	if before != after {
		_ = unix.Close(pidfd)
		return -1, errors.New("nested anchor identity changed while acquiring pidfd")
	}
	if err := validateNestedProcessSnapshot(after, identity, supervisorPID); err != nil {
		_ = unix.Close(pidfd)
		return -1, err
	}
	return pidfd, nil
}

func verifyNestedProcess(identity nestedProcessIdentity, supervisorPID int) error {
	snapshot, err := readProcessIdentity(identity.pid)
	if err != nil {
		return err
	}
	return validateNestedProcessSnapshot(snapshot, identity, supervisorPID)
}

func validateNestedProcessSnapshot(snapshot processIdentitySnapshot, identity nestedProcessIdentity, supervisorPID int) error {
	if snapshot.state != 'T' || snapshot.pgid != identity.pid ||
		snapshot.sid != identity.pid || snapshot.startTime != identity.startTime {
		return errors.New("nested anchor is not the reported stopped session leader")
	}
	if snapshot.ppid != supervisorPID {
		return errors.New("nested anchor is not a direct supervisor child")
	}
	return nil
}

func readProcessIdentity(pid int) (processIdentitySnapshot, error) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processIdentitySnapshot{}, err
	}
	line := string(raw)
	closeParen := strings.LastIndex(line, ") ")
	if closeParen < 0 {
		return processIdentitySnapshot{}, errors.New("malformed nested process stat")
	}
	fields := strings.Fields(line[closeParen+2:])
	if len(fields) < 20 || len(fields[0]) != 1 {
		return processIdentitySnapshot{}, errors.New("short nested process stat")
	}
	parseInt := func(index int) (int, error) {
		value, err := strconv.Atoi(fields[index])
		if err != nil {
			return 0, errors.New("invalid nested process stat")
		}
		return value, nil
	}
	ppid, err := parseInt(1)
	if err != nil {
		return processIdentitySnapshot{}, err
	}
	pgid, err := parseInt(2)
	if err != nil {
		return processIdentitySnapshot{}, err
	}
	sid, err := parseInt(3)
	if err != nil {
		return processIdentitySnapshot{}, err
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTime == 0 {
		return processIdentitySnapshot{}, errors.New("invalid nested process start time")
	}
	return processIdentitySnapshot{
		state: fields[0][0], ppid: ppid, pgid: pgid, sid: sid, startTime: startTime,
	}, nil
}

func (tracker *supervisorControlTracker) grace(override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	select {
	case <-tracker.boundReady:
	case <-time.After(supervisorBoundAdvertisementWait):
	}
	tracker.mu.Lock()
	bound := tracker.cleanupBound
	tracker.mu.Unlock()
	if bound <= 0 {
		// Cancellation can win before a loaded supervisor publishes its required
		// first control record. The fallback must cover every accepted advertised
		// bound; a shorter historical default could truncate valid host cleanup.
		bound = maximumSupervisorCleanupBound
	}
	return bound
}

func (tracker *supervisorControlTracker) forceNestedAndRetain() (int, error) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.nestedState == nestedIdle || tracker.nestedPID <= 0 || tracker.nestedPIDFD < 0 {
		return -1, nil
	}
	joinFD, err := unix.FcntlInt(uintptr(tracker.nestedPIDFD), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("retain nested anchor pidfd: %w", err)
	}
	if err := forceNestedPIDFD(tracker.nestedPIDFD); err != nil {
		_ = unix.Close(joinFD)
		return -1, err
	}
	return joinFD, nil
}

func (tracker *supervisorControlTracker) forceNestedAndTake() (int, error) {
	// The caller must first join the control reader, which makes ownership of
	// the tracked pidfd exclusive and prevents a later nested-done from closing
	// the transferred descriptor.
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.nestedState == nestedIdle || tracker.nestedPID <= 0 || tracker.nestedPIDFD < 0 {
		return -1, nil
	}
	pidfd := tracker.nestedPIDFD
	err := forceNestedPIDFD(pidfd)
	tracker.nestedPID = 0
	tracker.nestedStart = 0
	tracker.nestedPIDFD = -1
	tracker.nestedState = nestedIdle
	return pidfd, err
}

func forceNestedPIDFD(pidfd int) error {
	// A protocol-valid anchor is stopped until its supervisor sends CONT. Make
	// it runnable before requesting the anchor's graceful force path; otherwise
	// SIGUSR2 remains pending and killing the detached supervisor group cannot
	// reach either the anchor or its descendants.
	for _, signal := range []syscall.Signal{syscall.SIGCONT, syscall.SIGUSR2} {
		if err := unix.PidfdSendSignal(pidfd, signal, nil, 0); err != nil {
			if errors.Is(err, syscall.ESRCH) {
				return nil
			}
			return fmt.Errorf("signal nested anchor %s: %w", signal, err)
		}
	}
	return nil
}

func waitForNestedExit(pidfd int) error {
	if pidfd < 0 {
		return nil
	}
	defer unix.Close(pidfd)
	poll := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	for {
		count, err := unix.Poll(poll, -1)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("join nested anchor: %w", err)
		}
		if count > 0 && poll[0].Revents&unix.POLLIN != 0 {
			return nil
		}
		if count > 0 {
			return fmt.Errorf("join nested anchor: unexpected pidfd poll events %#x", poll[0].Revents)
		}
	}
}

func (tracker *supervisorControlTracker) close() {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.closed = true
	if tracker.nestedPIDFD >= 0 {
		_ = unix.Close(tracker.nestedPIDFD)
	}
	tracker.nestedPID = 0
	tracker.nestedStart = 0
	tracker.nestedPIDFD = -1
	tracker.nestedState = nestedIdle
}

func supervisorEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment)+1)
	for _, item := range environment {
		if strings.HasPrefix(item, "SUMI_SUPERVISOR_CONTROL_FD=") {
			continue
		}
		result = append(result, item)
	}
	return append(result, "SUMI_SUPERVISOR_CONTROL_FD="+strconv.Itoa(supervisorControlFD))
}

func (runner execCommandRunner) Run(ctx context.Context, path string, args, environment []string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, &supervisorCommandError{cause: err, diagnostic: "supervisor operation canceled before launch"}
	}
	controlFDs, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, &supervisorCommandError{cause: err, diagnostic: "cannot establish supervisor cleanup control"}
	}
	controlParent := os.NewFile(uintptr(controlFDs[0]), "supervisor-control-parent")
	controlChild := os.NewFile(uintptr(controlFDs[1]), "supervisor-control-child")
	tracker := newSupervisorControlTracker()
	command := exec.Command(path, args...)
	command.Env = supervisorEnvironment(environment)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.ExtraFiles = []*os.File{controlChild}
	pipeWait := runner.pipeWait
	if pipeWait <= 0 {
		pipeWait = defaultSupervisorPipeWait
	}
	command.WaitDelay = pipeWait
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		_ = controlParent.Close()
		_ = controlChild.Close()
		return nil, &supervisorCommandError{
			cause:      err,
			diagnostic: sanitizeSupervisorError(stderr.String(), environment),
		}
	}
	tracker.supervisorPID = command.Process.Pid
	_ = controlChild.Close()
	supervisorPIDFD, err := unix.PidfdOpen(command.Process.Pid, 0)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		_ = controlParent.Close()
		return nil, &supervisorCommandError{cause: err, diagnostic: "cannot acquire stable supervisor process handle"}
	}
	consumeDone := make(chan error, 1)
	go func() { consumeDone <- tracker.consume(controlParent) }()
	defer func() {
		_ = unix.Close(supervisorPIDFD)
		_ = controlParent.Close()
		tracker.close()
	}()
	exited := make(chan struct{})
	go func() {
		poll := []unix.PollFd{{Fd: int32(supervisorPIDFD), Events: unix.POLLIN}}
		for {
			_, err := unix.Poll(poll, -1)
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			break
		}
		close(exited)
	}()
	drainControl := func(deadline time.Time) error {
		wait := time.Until(deadline)
		if wait > supervisorControlDrainJoinReserve {
			wait = supervisorControlDrainJoinReserve
		}
		if wait <= 0 {
			_ = controlParent.Close()
			return errors.Join(errors.New("supervisor control drain exceeded its advertised reserve"), <-consumeDone)
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case err := <-consumeDone:
			return err
		case <-timer.C:
			_ = controlParent.Close()
			return errors.Join(errors.New("supervisor control drain exceeded its advertised reserve"), <-consumeDone)
		}
	}
	forceAndJoinFinalNested := func() error {
		pidfd, forceErr := tracker.forceNestedAndTake()
		return errors.Join(forceErr, waitForNestedExit(pidfd))
	}
	select {
	case <-exited:
		err := command.Wait()
		controlErr := drainControl(time.Now().Add(supervisorControlDrainJoinReserve))
		nestedErr := forceAndJoinFinalNested()
		if err != nil {
			return nil, &supervisorCommandError{
				cause:      errors.Join(err, controlErr, nestedErr),
				diagnostic: sanitizeSupervisorError(stderr.String(), environment),
			}
		}
		if controlErr != nil || nestedErr != nil {
			return nil, &supervisorCommandError{
				cause:      errors.Join(controlErr, nestedErr),
				diagnostic: "supervisor cleanup control protocol failed closed",
			}
		}
		return stdout.Bytes(), nil
	case <-ctx.Done():
		// Prefer a graceful supervisor trap so a partially prepared generation
		// can be rolled back. Same-group children follow the supervisor signal;
		// its detached Compose session is tracked over the control descriptor.
	}

	// command.Wait has not run, so the supervisor PID remains reserved even if
	// it exits while signals are delivered. Its process-group number therefore
	// cannot be recycled under either group signal.
	termErr := normalizeNoProcess(syscall.Kill(-command.Process.Pid, syscall.SIGTERM))
	grace := tracker.grace(runner.terminationGrace)
	cleanupDeadline := time.Now().Add(grace)
	forceDeadline := cleanupDeadline.Add(-supervisorControlDrainJoinReserve)
	if grace <= supervisorControlDrainJoinReserve {
		forceDeadline = cleanupDeadline
	}
	forceAfter := time.Until(forceDeadline)
	if forceAfter < 0 {
		forceAfter = 0
	}
	timer := time.NewTimer(forceAfter)
	var killErr error
	nestedJoinFD := -1
	cleanupExpired := false
	select {
	case <-exited:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	case <-timer.C:
		nestedJoinFD, killErr = tracker.forceNestedAndRetain()
		remaining := time.Until(cleanupDeadline)
		if remaining < 0 {
			remaining = 0
		}
		finalTimer := time.NewTimer(remaining)
		select {
		case <-exited:
			if !finalTimer.Stop() {
				select {
				case <-finalTimer.C:
				default:
				}
			}
		case <-finalTimer.C:
			cleanupExpired = true
			killErr = errors.Join(killErr, normalizeNoProcess(syscall.Kill(-command.Process.Pid, syscall.SIGKILL)))
			<-exited
		}
	}
	// A child may have kept inherited output descriptors open after the shell
	// exited. WaitDelay closes those pipes; this final group kill removes any
	// remaining same-hierarchy descendant before returning.
	killErr = errors.Join(killErr, normalizeNoProcess(syscall.Kill(-command.Process.Pid, syscall.SIGKILL)))
	waitErr := command.Wait()
	controlErr := drainControl(cleanupDeadline)
	// Once the supervisor and its control writer are gone, no new anchor can be
	// admitted. Retain and force any anchor that became current after the first
	// escalation, then join every retained pidfd. Anchor exit is the proof that
	// its child-subreaper wait4 observed exact descendant absence; never close a
	// live stable handle merely because the advertised deadline was reached.
	killErr = errors.Join(killErr, controlErr, waitForNestedExit(nestedJoinFD), forceAndJoinFinalNested())
	if time.Now().After(cleanupDeadline) {
		cleanupExpired = true
	}
	diagnostic := sanitizeSupervisorError(stderr.String(), environment)
	if cleanupExpired {
		diagnostic = "supervisor cleanup exceeded its advertised bound; host lifecycle state is indeterminate and requires reconciliation; " + diagnostic
	}
	return nil, &supervisorCommandError{
		cause:      errors.Join(ctx.Err(), termErr, killErr, waitErr),
		diagnostic: diagnostic,
	}
}

func normalizeNoProcess(err error) error {
	if err == nil {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := joined.Unwrap()
		normalized := make([]error, 0, len(parts))
		for _, part := range parts {
			normalized = append(normalized, normalizeNoProcess(part))
		}
		return errors.Join(normalized...)
	}
	if errors.Is(err, syscall.ESRCH) {
		if wrapped := errors.Unwrap(err); wrapped != nil {
			return normalizeNoProcess(wrapped)
		}
		return nil
	}
	return err
}

func sanitizeSupervisorError(message string, environment []string) string {
	for _, item := range environment {
		name, value, ok := strings.Cut(item, "=")
		if !ok || value == "" {
			continue
		}
		switch name {
		case "SUMI_LOCAL_CONTROL_BEARER", "SUMI_AGENT_WRAPPING_KEY",
			"SUMI_APPROVAL_SECRET_DIGEST_KEY", "SUMI_PROVIDER_API_KEY",
			"SUMI_EXECUTION_REVIEWER_API_KEY", "SUMI_ESCALATION_REVIEWER_API_KEY",
			"SUMI_EXPECTED_RPC_NONCE", "SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE":
			message = strings.ReplaceAll(message, value, "<redacted:"+name+">")
		}
	}
	message = strings.TrimSpace(message)
	if len(message) > 4096 {
		message = message[len(message)-4096:]
	}
	if message == "" {
		return "no safe supervisor diagnostic"
	}
	return message
}

type DockerBackendConfig struct {
	SupervisorPath   string
	BaseEnvironment  []string
	OperationTimeout time.Duration
}

func NewDockerBackend(config DockerBackendConfig) (*DockerBackend, error) {
	if config.SupervisorPath == "" {
		return nil, errors.New("docker backend requires a supervisor path")
	}
	info, err := os.Stat(config.SupervisorPath)
	if err != nil {
		return nil, fmt.Errorf("docker supervisor is not executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, errors.New("docker supervisor is not an executable regular file")
	}
	return &DockerBackend{
		supervisor:       config.SupervisorPath,
		baseEnvironment:  append([]string(nil), config.BaseEnvironment...),
		operationTimeout: config.OperationTimeout,
		runner:           execCommandRunner{},
	}, nil
}

type supervisorInspection struct {
	PersonalityAgentID      string  `json:"personality_agent_id"`
	Phase                   Phase   `json:"phase"`
	Generation              uint64  `json:"generation,omitempty"`
	RPCBootNonce            string  `json:"rpc_boot_nonce,omitempty"`
	ReapedThroughGeneration *uint64 `json:"reaped_through_generation,omitempty"`
}

func (backend *DockerBackend) Prepare(ctx context.Context, request PrepareRequest) (PreparedEpoch, error) {
	output, err := backend.run(ctx, "prepare", request.PersonalityAgentID, nil, nil)
	if err != nil {
		return PreparedEpoch{}, err
	}
	inspection, err := parseSupervisorInspection(output, request.PersonalityAgentID)
	if err != nil {
		return PreparedEpoch{}, err
	}
	if inspection.Phase != PhasePrepared || inspection.Epoch == nil {
		return PreparedEpoch{}, errors.New("docker supervisor prepare did not return a prepared epoch")
	}
	return *inspection.Epoch, nil
}

func (backend *DockerBackend) Activate(ctx context.Context, request ActivateRequest) error {
	expected := map[string]string{
		"SUMI_EXPECTED_RPC_GENERATION": fmt.Sprint(request.Generation),
		"SUMI_EXPECTED_RPC_NONCE":      request.RPCBootNonce,
	}
	_, err := backend.run(ctx, "activate", request.PersonalityAgentID, activationEnvironment(request.Activation), expected)
	return err
}

func activationEnvironment(config ActivationConfig) map[string]string {
	values := map[string]string{
		"SUMI_GATEWAY_URL":                           config.GatewayURL,
		"SUMI_LOCAL_CONTROL_BEARER":                  config.LocalControlBearer,
		"SUMI_LOCAL_CONTROL_SERVER_UID":              fmt.Sprint(config.LocalControlServerUID),
		"SUMI_LOCAL_CONTROL_SOCKET_GID":              fmt.Sprint(config.LocalControlSocketGID),
		"SUMI_AGENT_WRAPPING_KEY":                    config.AgentWrappingKey,
		"SUMI_AGENT_WRAPPING_KEY_ID":                 config.AgentWrappingKeyID,
		"SUMI_APPROVAL_SECRET_DIGEST_KEY":            config.ApprovalSecretDigestKey,
		"SUMI_PROVIDER_API_KEY":                      config.ProviderAPIKey,
		"SUMI_EXECUTION_REVIEWER_API_KEY":            config.ExecutionReviewerAPIKey,
		"SUMI_EXECUTION_REVIEWER_MODEL_PRESET":       config.ExecutionReviewerModelPreset,
		"SUMI_EXECUTION_REVIEWER_MODEL_API_KEY_ENV":  "SUMI_EXECUTION_REVIEWER_API_KEY",
		"SUMI_ESCALATION_REVIEWER_API_KEY":           config.EscalationReviewerAPIKey,
		"SUMI_ESCALATION_REVIEWER_MODEL_PRESET":      config.EscalationReviewerModelPreset,
		"SUMI_ESCALATION_REVIEWER_MODEL_API_KEY_ENV": "SUMI_ESCALATION_REVIEWER_API_KEY",
	}
	if config.ModelPreset != "" {
		values["SUMI_MODEL_PRESET"] = config.ModelPreset
	}
	if config.ModelID != "" {
		values["SUMI_MODEL_ID"] = config.ModelID
	}
	for name, value := range map[string]string{
		"SUMI_EXECUTION_REVIEWER_MODEL_ID":             config.ExecutionReviewerModelID,
		"SUMI_EXECUTION_REVIEWER_MODEL_BASE_URL":       config.ExecutionReviewerModelBaseURL,
		"SUMI_EXECUTION_REVIEWER_MODEL_ACCOUNT_SCOPE":  config.ExecutionReviewerAccountScope,
		"SUMI_ESCALATION_REVIEWER_MODEL_ID":            config.EscalationReviewerModelID,
		"SUMI_ESCALATION_REVIEWER_MODEL_BASE_URL":      config.EscalationReviewerModelBaseURL,
		"SUMI_ESCALATION_REVIEWER_MODEL_ACCOUNT_SCOPE": config.EscalationReviewerAccountScope,
	} {
		if value != "" {
			values[name] = value
		}
	}
	if config.AllowInsecureLoopbackGateway {
		values["SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY"] = "true"
	}
	if config.LogFilter != "" {
		values["SUMI_LOG"] = config.LogFilter
	}
	if attestation := config.ReapAttestation; attestation != nil {
		values["SUMI_REAP_ATTESTATION_PERSONALITY_AGENT_ID"] = attestation.PersonalityAgentID
		values["SUMI_REAP_ATTESTATION_EPOCH_GENERATION"] = fmt.Sprint(attestation.EpochGeneration)
		values["SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE"] = attestation.RPCBootNonce
		values["SUMI_REAPED_THROUGH_GENERATION"] = fmt.Sprint(attestation.ReapedThroughGeneration)
	}
	return values
}

func (backend *DockerBackend) Abort(ctx context.Context, epoch PreparedEpoch) (Inspection, error) {
	expected := map[string]string{
		"SUMI_EXPECTED_RPC_GENERATION": fmt.Sprint(epoch.Generation),
		"SUMI_EXPECTED_RPC_NONCE":      epoch.RPCBootNonce,
	}
	output, err := backend.run(ctx, "abort", epoch.PersonalityAgentID, nil, expected)
	if err != nil {
		return Inspection{}, err
	}
	return parseExactSupervisorReap(output, epoch)
}

func (backend *DockerBackend) Inspect(ctx context.Context, personalityAgentID string) (Inspection, error) {
	output, err := backend.run(ctx, "inspect-epoch", personalityAgentID, nil, nil)
	if err != nil {
		return Inspection{}, err
	}
	return parseSupervisorInspection(output, personalityAgentID)
}

func (backend *DockerBackend) Stop(ctx context.Context, epoch PreparedEpoch) (Inspection, error) {
	expected := map[string]string{
		"SUMI_EXPECTED_RPC_GENERATION": fmt.Sprint(epoch.Generation),
		"SUMI_EXPECTED_RPC_NONCE":      epoch.RPCBootNonce,
	}
	output, err := backend.run(ctx, "stop-epoch", epoch.PersonalityAgentID, nil, expected)
	if err != nil {
		return Inspection{}, err
	}
	return parseExactSupervisorReap(output, epoch)
}

func (backend *DockerBackend) Reconcile(ctx context.Context, request ReconcileRequest) (Inspection, error) {
	if err := request.Validate(); err != nil {
		return Inspection{}, err
	}
	var expected map[string]string
	if request.FencedEpoch != nil {
		expected = map[string]string{
			"SUMI_EXPECTED_RPC_GENERATION": fmt.Sprint(request.FencedEpoch.Generation),
			"SUMI_EXPECTED_RPC_NONCE":      request.FencedEpoch.RPCBootNonce,
		}
	}
	output, err := backend.run(ctx, "reconcile", request.PersonalityAgentID, nil, expected)
	if err != nil {
		return Inspection{}, err
	}
	return parseSupervisorInspection(output, request.PersonalityAgentID)
}

func (backend *DockerBackend) run(ctx context.Context, action, personalityAgentID string, supplied, forced map[string]string) ([]byte, error) {
	environment, err := mergeEnvironment(backend.baseEnvironment, supplied, forced, personalityAgentID)
	if err != nil {
		return nil, err
	}
	timeout := backend.operationTimeout
	if timeout <= 0 {
		timeout = 15 * time.Minute
	}
	if action == "abort" || action == "stop-epoch" {
		timeout = 90 * time.Second
	}
	// Once accepted, a host lifecycle transition is atomic with respect to a
	// caller disconnect. A bounded daemon-owned context lets the caller retry
	// and recover the committed epoch instead of killing Compose mid-allocation.
	operationContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	output, err := backend.runner.Run(operationContext, backend.supervisor, []string{action}, environment)
	if err != nil {
		var commandError *supervisorCommandError
		if errors.As(err, &commandError) {
			return nil, fmt.Errorf("docker supervisor %s failed: %s", action, commandError.diagnostic)
		}
		return nil, fmt.Errorf("docker supervisor %s failed", action)
	}
	return output, nil
}

var reservedEnvironment = map[string]bool{
	"SUMI_PERSONALITY_AGENT_ID":    true,
	"SUMI_COMPOSE_PROJECT":         true,
	"SUMI_EXPECTED_RPC_GENERATION": true,
	"SUMI_EXPECTED_RPC_NONCE":      true,
	"SUMI_CONFIG_FILE":             true,
}

var allowedActivationEnvironment = map[string]bool{
	"SUMI_GATEWAY_URL":                             true,
	"SUMI_LOCAL_CONTROL_BEARER":                    true,
	"SUMI_LOCAL_CONTROL_SERVER_UID":                true,
	"SUMI_LOCAL_CONTROL_SOCKET_GID":                true,
	"SUMI_AGENT_WRAPPING_KEY":                      true,
	"SUMI_AGENT_WRAPPING_KEY_ID":                   true,
	"SUMI_APPROVAL_SECRET_DIGEST_KEY":              true,
	"SUMI_PROVIDER_API_KEY":                        true,
	"SUMI_EXECUTION_REVIEWER_API_KEY":              true,
	"SUMI_EXECUTION_REVIEWER_MODEL_PRESET":         true,
	"SUMI_EXECUTION_REVIEWER_MODEL_ID":             true,
	"SUMI_EXECUTION_REVIEWER_MODEL_BASE_URL":       true,
	"SUMI_EXECUTION_REVIEWER_MODEL_ACCOUNT_SCOPE":  true,
	"SUMI_EXECUTION_REVIEWER_MODEL_API_KEY_ENV":    true,
	"SUMI_ESCALATION_REVIEWER_API_KEY":             true,
	"SUMI_ESCALATION_REVIEWER_MODEL_PRESET":        true,
	"SUMI_ESCALATION_REVIEWER_MODEL_ID":            true,
	"SUMI_ESCALATION_REVIEWER_MODEL_BASE_URL":      true,
	"SUMI_ESCALATION_REVIEWER_MODEL_ACCOUNT_SCOPE": true,
	"SUMI_ESCALATION_REVIEWER_MODEL_API_KEY_ENV":   true,
	"SUMI_MODEL_PRESET":                            true,
	"SUMI_MODEL_ID":                                true,
	"SUMI_ALLOW_INSECURE_LOOPBACK_GATEWAY":         true,
	"SUMI_LOG":                                     true,
	"SUMI_REAP_ATTESTATION_PERSONALITY_AGENT_ID":   true,
	"SUMI_REAP_ATTESTATION_EPOCH_GENERATION":       true,
	"SUMI_REAP_ATTESTATION_RPC_BOOT_NONCE":         true,
	"SUMI_REAPED_THROUGH_GENERATION":               true,
}

func mergeEnvironment(base []string, supplied, forced map[string]string, personalityAgentID string) ([]string, error) {
	values := make(map[string]string, len(base)+len(supplied)+len(forced)+1)
	for _, item := range base {
		name, value, ok := strings.Cut(item, "=")
		if ok && name != "" {
			values[name] = value
		}
	}
	for name, value := range supplied {
		if reservedEnvironment[name] {
			return nil, fmt.Errorf("activation environment may not set reserved variable %s", name)
		}
		if !allowedActivationEnvironment[name] {
			return nil, fmt.Errorf("activation environment variable %s is not allowed", name)
		}
		if !validEnvironmentName(name) || strings.ContainsRune(value, 0) {
			return nil, fmt.Errorf("activation environment contains invalid variable %q", name)
		}
		values[name] = value
	}
	for name, value := range forced {
		values[name] = value
	}
	values["SUMI_PERSONALITY_AGENT_ID"] = personalityAgentID
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	environment := make([]string, 0, len(names))
	for _, name := range names {
		environment = append(environment, name+"="+values[name])
	}
	return environment, nil
}

func validEnvironmentName(name string) bool {
	if name == "" || !((name[0] >= 'A' && name[0] <= 'Z') || name[0] == '_') {
		return false
	}
	for index := 1; index < len(name); index++ {
		char := name[index]
		if !((char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_') {
			return false
		}
	}
	return true
}

func parseSupervisorInspection(output []byte, expectedPersonalityAgentID string) (Inspection, error) {
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	var wire supervisorInspection
	if err := decoder.Decode(&wire); err != nil {
		return Inspection{}, errors.New("docker supervisor returned invalid machine output")
	}
	if err := decoder.Decode(&struct{}{}); err == nil {
		return Inspection{}, errors.New("docker supervisor returned multiple machine records")
	} else if !errors.Is(err, io.EOF) {
		return Inspection{}, errors.New("docker supervisor returned trailing machine output")
	}
	if wire.PersonalityAgentID != expectedPersonalityAgentID {
		return Inspection{}, errors.New("docker supervisor returned a different personality agent")
	}
	inspection := Inspection{
		PersonalityAgentID:      wire.PersonalityAgentID,
		Phase:                   wire.Phase,
		ReapedThroughGeneration: wire.ReapedThroughGeneration,
	}
	if wire.Phase == PhasePrepared || wire.Phase == PhaseActive || wire.Phase == PhaseRecovery {
		epoch := PreparedEpoch{
			PersonalityAgentID:   wire.PersonalityAgentID,
			Generation:           wire.Generation,
			RPCBootNonce:         wire.RPCBootNonce,
			OpaquePreparedHandle: dockerPreparedHandle(wire.PersonalityAgentID, wire.Generation, wire.RPCBootNonce),
		}
		inspection.Epoch = &epoch
	}
	if err := inspection.Validate(); err != nil {
		return Inspection{}, fmt.Errorf("docker supervisor returned invalid epoch: %w", err)
	}
	return inspection, nil
}

func parseExactSupervisorReap(output []byte, epoch PreparedEpoch) (Inspection, error) {
	inspection, err := parseSupervisorInspection(output, epoch.PersonalityAgentID)
	if err != nil {
		return Inspection{}, err
	}
	if inspection.Phase != PhaseUnknown || inspection.ReapedThroughGeneration == nil ||
		*inspection.ReapedThroughGeneration != epoch.Generation {
		return Inspection{}, errors.New("docker supervisor teardown did not return the exact observed-empty reap receipt")
	}
	return inspection, nil
}

func dockerPreparedHandle(personalityAgentID string, generation uint64, nonce string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("sumi-docker-prepared-v1\x00%s\x00%d\x00%s", personalityAgentID, generation, nonce)))
	return "docker-v1-" + hex.EncodeToString(digest[:])
}
