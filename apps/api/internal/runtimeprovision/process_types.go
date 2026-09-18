package runtimeprovision

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrProcessNotFound = errors.New("process operation not found")
var ErrProcessBusy = errors.New("process capacity exhausted")
var ErrInvalidProcessRequest = errors.New("invalid process request")

// ErrProcessWorkspace marks a canonical files-scope launch that cannot be
// fulfilled: the files volume is unconfigured, unmounted, the volume UUID
// changed, the persona scope is missing, or the binding record refuses it.
// It is configuration-level — retrying the identical request will not
// self-heal — so callers should fail the owning job rather than requeue it.
var ErrProcessWorkspace = errors.New("process workspace unavailable")

type ProcessState string

const (
	ProcessAccepted      ProcessState = "accepted"
	ProcessRunning       ProcessState = "running"
	ProcessSucceeded     ProcessState = "succeeded"
	ProcessFailed        ProcessState = "failed"
	ProcessCancelled     ProcessState = "cancelled"
	ProcessIndeterminate ProcessState = "indeterminate"
)

func (s ProcessState) terminal() bool {
	return s == ProcessSucceeded || s == ProcessFailed || s == ProcessCancelled || s == ProcessIndeterminate
}

type ProcessStartRequest struct {
	PersonalityAgentID    string   `json:"personality_agent_id"`
	OriginatingToolCallID string   `json:"originating_tool_call_id"`
	Executable            string   `json:"executable"`
	Args                  []string `json:"args"`
	Cwd                   string   `json:"cwd"`
	TimeoutSeconds        int      `json:"timeout_seconds"`
	// Env carries bounded extra environment for the process. Backend-owned
	// names (PATH, HOME, LANG) are refused rather than overridden so the
	// fixed launch contract cannot be weakened from a request.
	Env map[string]string `json:"env,omitempty"`
	// Image selects among server-pinned image references only: "" (or
	// "agent") is the agent image, "job" is the job toolchain image. It is
	// never a free-form reference — a requester cannot pick the container
	// it runs in beyond the deployment's own pins.
	Image string `json:"image,omitempty"`
	// Workspace selects where /workspace comes from. "" keeps the backend's
	// legacy workspace volume; "files-scope" bind-mounts the persona's
	// canonical files scope, resolved and verified server-side (mount +
	// volume UUID check, binding record). A requester cannot name a path —
	// it can only ask for the canonical scope or the legacy default.
	Workspace string `json:"workspace,omitempty"`
}

func (r ProcessStartRequest) Validate() error {
	if _, err := uuid.Parse(r.PersonalityAgentID); err != nil {
		return fmt.Errorf("%w: invalid personality agent ID", ErrInvalidProcessRequest)
	}
	if r.OriginatingToolCallID == "" || len(r.OriginatingToolCallID) > 1024 || strings.ContainsRune(r.OriginatingToolCallID, 0) {
		return fmt.Errorf("%w: invalid tool call ID", ErrInvalidProcessRequest)
	}
	if r.Executable == "" || len(r.Executable) > 1024 || strings.ContainsRune(r.Executable, 0) || len(r.Args) > 128 {
		return fmt.Errorf("%w: invalid executable or arguments", ErrInvalidProcessRequest)
	}
	n := len(r.Executable)
	for _, a := range r.Args {
		n += len(a)
		if strings.ContainsRune(a, 0) {
			return fmt.Errorf("%w: argument contains NUL", ErrInvalidProcessRequest)
		}
	}
	if n > 32<<10 {
		return fmt.Errorf("%w: argv exceeds 32 KiB", ErrInvalidProcessRequest)
	}
	if len(r.Cwd) > 1024 || strings.ContainsRune(r.Cwd, 0) || path.IsAbs(r.Cwd) || (r.Cwd != "" && path.Clean(r.Cwd) != r.Cwd) {
		return fmt.Errorf("%w: cwd must be clean and relative", ErrInvalidProcessRequest)
	}
	for _, c := range strings.Split(r.Cwd, "/") {
		if c == ".." {
			return fmt.Errorf("%w: cwd escapes workspace", ErrInvalidProcessRequest)
		}
	}
	if r.TimeoutSeconds < 0 || r.TimeoutSeconds > 3600 {
		return fmt.Errorf("%w: timeout must be 1..3600 or zero for default", ErrInvalidProcessRequest)
	}
	if err := validateProcessEnv(r.Env); err != nil {
		return err
	}
	switch r.Image {
	case "", "agent", "job":
	default:
		return fmt.Errorf("%w: unknown image selector", ErrInvalidProcessRequest)
	}
	switch r.Workspace {
	case "", "files-scope":
	default:
		return fmt.Errorf("%w: unknown workspace selector", ErrInvalidProcessRequest)
	}
	return nil
}

// processEnvBounds keep a request's environment a bounded request field,
// not a side channel for oversized launch state.
const (
	processEnvMaxKeys    = 32
	processEnvMaxName    = 64
	processEnvMaxValue   = 4096
	processEnvMaxPayload = 8 << 10
)

// processEnvReserved names the environment the launch contract owns; a
// request may not silently override or shadow it.
var processEnvReserved = map[string]bool{"PATH": true, "HOME": true, "LANG": true}

var processEnvName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateProcessEnv(env map[string]string) error {
	if len(env) == 0 {
		return nil
	}
	if len(env) > processEnvMaxKeys {
		return fmt.Errorf("%w: too many env vars", ErrInvalidProcessRequest)
	}
	total := 0
	for k, v := range env {
		if len(k) > processEnvMaxName || !processEnvName.MatchString(k) {
			return fmt.Errorf("%w: invalid env name %q", ErrInvalidProcessRequest, k)
		}
		if processEnvReserved[k] {
			return fmt.Errorf("%w: env name %q is backend-owned", ErrInvalidProcessRequest, k)
		}
		if len(v) > processEnvMaxValue || strings.ContainsRune(v, 0) {
			return fmt.Errorf("%w: invalid env value for %s", ErrInvalidProcessRequest, k)
		}
		total += len(k) + len(v)
	}
	if total > processEnvMaxPayload {
		return fmt.Errorf("%w: env payload too large", ErrInvalidProcessRequest)
	}
	return nil
}
func (r ProcessStartRequest) canonical() ProcessStartRequest {
	if r.Cwd == "" {
		r.Cwd = "."
	}
	if r.TimeoutSeconds == 0 {
		r.TimeoutSeconds = 600
	}
	if r.Args == nil {
		r.Args = []string{}
	}
	return r
}

// ProcessOperationID is the deterministic operation identity for a
// (personality agent, tool call) pair — the same value StartProcess
// journals. A job runner derives it without submitting a request so it can
// inspect, cancel, or reconcile a possibly-launched operation after any
// restart, before deciding whether a launch is still needed.
func ProcessOperationID(personalityAgentID, originatingToolCallID string) string {
	return processID(ProcessStartRequest{
		PersonalityAgentID:    personalityAgentID,
		OriginatingToolCallID: originatingToolCallID,
	})
}

func processID(r ProcessStartRequest) string {
	h := sha256.New()
	h.Write([]byte("sumi.workspace-process.v1\x00"))
	for _, s := range []string{r.PersonalityAgentID, r.OriginatingToolCallID} {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(len(s)))
		h.Write(b[:])
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))
}

type ProcessLookupRequest struct {
	PersonalityAgentID string `json:"personality_agent_id"`
	OperationID        string `json:"operation_id"`
	// OriginatingToolCallID is optional identity evidence recorded on a
	// tombstone record so a replayed start sees a complete operation.
	// Reads ignore it — the deterministic operation ID already binds the
	// (persona, tool call) pair.
	OriginatingToolCallID string `json:"originating_tool_call_id,omitempty"`
	// TombstoneIfAbsent, honored by CancelProcess only, asks the service to
	// durably journal a terminal cancellation when the operation has no
	// record yet. That is the mechanical fence behind an "absent" verdict:
	// once the tombstone exists, a StartProcess still inside its
	// pre-journal window replays the cancellation and can never launch —
	// so publishing 'never started' / 'observed absent' no longer depends
	// on how long the caller happened to wait.
	TombstoneIfAbsent bool `json:"tombstone_if_absent,omitempty"`
}

func (r ProcessLookupRequest) Validate() error {
	if _, e := uuid.Parse(r.PersonalityAgentID); e != nil {
		return fmt.Errorf("%w: invalid personality agent ID", ErrInvalidProcessRequest)
	}
	b, e := hex.DecodeString(r.OperationID)
	if e != nil || len(b) != 32 || strings.ToLower(r.OperationID) != r.OperationID {
		return fmt.Errorf("%w: invalid operation ID", ErrInvalidProcessRequest)
	}
	if len(r.OriginatingToolCallID) > 1024 || strings.ContainsRune(r.OriginatingToolCallID, 0) {
		return fmt.Errorf("%w: invalid tool call ID", ErrInvalidProcessRequest)
	}
	return nil
}

type ProcessOutputRequest struct {
	ProcessLookupRequest
	Stream string `json:"stream"`
	Offset int64  `json:"offset"`
	Limit  int    `json:"limit"`
}

func (r ProcessOutputRequest) Validate() error {
	if e := r.ProcessLookupRequest.Validate(); e != nil {
		return e
	}
	if (r.Stream != "stdout" && r.Stream != "stderr") || r.Offset < 0 || (r.Limit != 0 && r.Limit < 4) || r.Limit > 64<<10 {
		return fmt.Errorf("%w: invalid output range or stream", ErrInvalidProcessRequest)
	}
	return nil
}

type ProcessOperation struct {
	OperationID           string            `json:"operation_id"`
	PersonalityAgentID    string            `json:"personality_agent_id"`
	OriginatingToolCallID string            `json:"originating_tool_call_id"`
	Executable            string            `json:"executable"`
	Args                  []string          `json:"args"`
	Cwd                   string            `json:"cwd"`
	TimeoutSeconds        int               `json:"timeout_seconds"`
	Env                   map[string]string `json:"env,omitempty"`
	Image                 string            `json:"image,omitempty"`
	// WorkspaceBind is the server-resolved canonical files-scope path the
	// backend bind-mounts as /workspace, set only after the files mount and
	// volume UUID checks pass. FilesVolumeUUID records which volume the
	// bind was verified against. Both stay empty for backends without a
	// canonical scope (e.g. the fake backend in tests).
	WorkspaceBind   string       `json:"workspace_bind,omitempty"`
	FilesVolumeUUID string       `json:"files_volume_uuid,omitempty"`
	State           ProcessState `json:"state"`
	EventID         string       `json:"event_id"`
	OccurredAt      time.Time    `json:"occurred_at"`
	StartedAt       *time.Time   `json:"started_at,omitempty"`
	FinishedAt      *time.Time   `json:"finished_at,omitempty"`
	ExitCode        *int         `json:"exit_code,omitempty"`
	StdoutBytes     int64        `json:"stdout_bytes"`
	StderrBytes     int64        `json:"stderr_bytes"`
	StdoutTruncated bool         `json:"stdout_truncated"`
	StderrTruncated bool         `json:"stderr_truncated"`
	Error           string       `json:"error,omitempty"`
	// Tombstone marks a record the cancel fence created for an operation
	// that never journaled a launch — state is 'cancelled' but nothing
	// ever executed. Readers can distinguish "ran and was stopped" from
	// "fenced before it existed"; a replayed StartProcess returns this
	// record instead of launching.
	Tombstone bool `json:"tombstone,omitempty"`
}
type ProcessOutput struct {
	OperationID string `json:"operation_id"`
	Stream      string `json:"stream"`
	Offset      int64  `json:"offset"`
	NextOffset  int64  `json:"next_offset"`
	Content     string `json:"content"`
	EOF         bool   `json:"eof"`
	Truncated   bool   `json:"truncated"`
}
type ProcessCompletionReceipt struct {
	PersonalityAgentID string `json:"personality_agent_id"`
	OperationID        string `json:"operation_id"`
	EventID            string `json:"event_id"`
	CommandID          string `json:"command_id"`
	CommandSeq         uint64 `json:"command_seq"`
}
