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
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const defaultSupervisorCleanupBound = 100 * time.Second
const supervisorCleanupDeliveryOverhead = time.Second
const defaultSupervisorPipeWait = time.Second
const supervisorControlFD = 3

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
	mu           sync.Mutex
	cleanupBound time.Duration
	boundReady   chan struct{}
	boundOnce    sync.Once
	nestedPID    int
	nestedPIDFD  int
	closed       bool
}

func newSupervisorControlTracker() *supervisorControlTracker {
	return &supervisorControlTracker{boundReady: make(chan struct{}), nestedPIDFD: -1}
}

type supervisorCommandError struct {
	cause      error
	diagnostic string
}

func (err *supervisorCommandError) Error() string { return err.cause.Error() }
func (err *supervisorCommandError) Unwrap() error { return err.cause }

func (tracker *supervisorControlTracker) consume(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || value <= 0 {
			continue
		}
		switch fields[0] {
		case "cleanup-bound-ms":
			if value > int64((2*time.Hour)/time.Millisecond) {
				continue
			}
			bound := time.Duration(value) * time.Millisecond
			tracker.mu.Lock()
			if tracker.closed {
				tracker.mu.Unlock()
				continue
			}
			tracker.cleanupBound = bound
			tracker.mu.Unlock()
			tracker.boundOnce.Do(func() { close(tracker.boundReady) })
		case "nested-start":
			if value > 1<<30 {
				continue
			}
			pid := int(value)
			pidfd, err := unix.PidfdOpen(pid, 0)
			if err != nil {
				continue
			}
			tracker.mu.Lock()
			if tracker.closed {
				tracker.mu.Unlock()
				_ = unix.Close(pidfd)
				continue
			}
			if tracker.nestedPIDFD >= 0 {
				_ = unix.Close(tracker.nestedPIDFD)
			}
			tracker.nestedPID = pid
			tracker.nestedPIDFD = pidfd
			tracker.mu.Unlock()
		case "nested-done":
			tracker.mu.Lock()
			if tracker.nestedPID == int(value) {
				if tracker.nestedPIDFD >= 0 {
					_ = unix.Close(tracker.nestedPIDFD)
				}
				tracker.nestedPID = 0
				tracker.nestedPIDFD = -1
			}
			tracker.mu.Unlock()
		}
	}
}

func (tracker *supervisorControlTracker) grace(override time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	select {
	case <-tracker.boundReady:
	case <-time.After(100 * time.Millisecond):
	}
	tracker.mu.Lock()
	bound := tracker.cleanupBound
	tracker.mu.Unlock()
	if bound <= 0 {
		bound = defaultSupervisorCleanupBound
	}
	return bound + supervisorCleanupDeliveryOverhead
}

func (tracker *supervisorControlTracker) signalNested(signal syscall.Signal) error {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.nestedPID <= 0 || tracker.nestedPIDFD < 0 {
		return nil
	}
	if err := unix.PidfdSendSignal(tracker.nestedPIDFD, 0, nil, 0); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	pgid, err := syscall.Getpgid(tracker.nestedPID)
	if err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	if pgid != tracker.nestedPID {
		return unix.PidfdSendSignal(tracker.nestedPIDFD, signal, nil, 0)
	}
	err = syscall.Kill(-tracker.nestedPID, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func (tracker *supervisorControlTracker) close() {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.closed = true
	if tracker.nestedPIDFD >= 0 {
		_ = unix.Close(tracker.nestedPIDFD)
	}
	tracker.nestedPID = 0
	tracker.nestedPIDFD = -1
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
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		return nil, &supervisorCommandError{cause: err, diagnostic: "cannot establish supervisor cleanup control"}
	}
	tracker := newSupervisorControlTracker()
	command := exec.Command(path, args...)
	command.Env = supervisorEnvironment(environment)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.ExtraFiles = []*os.File{controlWrite}
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
		_ = controlRead.Close()
		_ = controlWrite.Close()
		return nil, &supervisorCommandError{
			cause:      err,
			diagnostic: sanitizeSupervisorError(stderr.String(), environment),
		}
	}
	_ = controlWrite.Close()
	go tracker.consume(controlRead)
	defer func() {
		_ = controlRead.Close()
		tracker.close()
	}()
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			return nil, &supervisorCommandError{
				cause:      err,
				diagnostic: sanitizeSupervisorError(stderr.String(), environment),
			}
		}
		return stdout.Bytes(), nil
	case <-ctx.Done():
		// Prefer a graceful supervisor trap so a partially prepared generation
		// can be rolled back. Same-group children follow the supervisor signal;
		// its detached Compose session is tracked over the control descriptor.
	}

	termErr := signalSupervisorHierarchy(command.Process.Pid, syscall.SIGTERM)
	grace := tracker.grace(runner.terminationGrace)
	timer := time.NewTimer(grace)
	var waitErr error
	var killErr error
	cleanupExpired := false
	select {
	case waitErr = <-waited:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	case <-timer.C:
		cleanupExpired = true
		killErr = errors.Join(
			tracker.signalNested(syscall.SIGKILL),
			signalSupervisorHierarchy(command.Process.Pid, syscall.SIGKILL),
		)
		waitErr = <-waited
	}
	// A child may have kept inherited output descriptors open after the shell
	// exited. WaitDelay closes those pipes; this final group kill removes any
	// remaining same-hierarchy descendant before returning.
	killErr = errors.Join(
		killErr,
		tracker.signalNested(syscall.SIGKILL),
		signalSupervisorHierarchy(command.Process.Pid, syscall.SIGKILL),
	)
	diagnostic := sanitizeSupervisorError(stderr.String(), environment)
	if cleanupExpired {
		diagnostic = "supervisor cleanup exceeded its advertised bound; host lifecycle state is indeterminate and requires reconciliation; " + diagnostic
	}
	return nil, &supervisorCommandError{
		cause:      errors.Join(ctx.Err(), termErr, killErr, waitErr),
		diagnostic: diagnostic,
	}
}

func signalSupervisorHierarchy(pid int, signal syscall.Signal) error {
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
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
		"SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX":  fmt.Sprint(config.LocalControlBearerExpiresAtUnix),
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
	"SUMI_LOCAL_CONTROL_BEARER_EXPIRES_AT_UNIX":    true,
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
