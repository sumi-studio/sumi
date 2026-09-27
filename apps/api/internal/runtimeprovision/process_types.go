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

// ErrProcessNotInteractive rejects input, resize, and signal calls on an
// operation that was not launched in interactive mode. The failure is a
// definite contract violation, not a transient backend error.
var ErrProcessNotInteractive = errors.New("process operation is not interactive")

// ErrProcessResizeUnsupported reports a daemon or backend that cannot
// resize the container's TTY. Callers must surface the typed failure
// rather than pretending a resize landed.
var ErrProcessResizeUnsupported = errors.New("process resize unsupported by backend")

// ErrProcessInputUnsupported reports a backend that cannot open a
// container input stream — e.g. a daemon reachable only over a
// non-unix DOCKER_HOST, which cannot serve the stdin hijack.
var ErrProcessInputUnsupported = errors.New("process input attach unsupported by backend")

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

// Terminal is the exported form for drivers in other packages that
// observe process state (the interactive session driver).
func (s ProcessState) Terminal() bool { return s.terminal() }

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
	// Interactive keeps the container's stdin open and accepts
	// WriteProcessInput/ResizeProcess/SignalProcess calls for the
	// operation's lifetime. One logical interactive operation is a
	// single long-lived container, never a sequence of one-shot execs.
	// The same launch contract applies: server-pinned image, resolved
	// workspace, hardened container flags — interactivity only adds a
	// held input/output stream to an otherwise identical environment.
	Interactive bool `json:"interactive,omitempty"`
	// TTY allocates a pseudo-terminal for the process (merged output
	// stream, line discipline, resizable winsize). TTY requires
	// Interactive; a plain interactive op gets pipes on all streams.
	TTY bool `json:"tty,omitempty"`
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
	if r.TTY && !r.Interactive {
		return fmt.Errorf("%w: tty requires interactive mode", ErrInvalidProcessRequest)
	}
	if r.Interactive && !r.TTY {
		// Every interactive consumer in this slice is a terminal —
		// a real PTY. A piped interactive op would interleave two
		// output streams with no winsize and no line discipline for
		// zero product value; refusing keeps the contract honest.
		return fmt.Errorf("%w: interactive mode requires a tty", ErrInvalidProcessRequest)
	}
	maxTimeout := 3600
	if r.Interactive {
		// Interactive sessions are bounded too, but the bound is a
		// day-scale lifetime limit, not the one-shot job ceiling.
		maxTimeout = 86400
	}
	if r.TimeoutSeconds < 0 || r.TimeoutSeconds > maxTimeout {
		return fmt.Errorf("%w: timeout must be 1..%d or zero for default", ErrInvalidProcessRequest, maxTimeout)
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
		if r.Interactive {
			// An interactive session defaults to an 8-hour lifetime; the
			// deadline is enforced like any other process timeout and
			// surfaces as a visible 'timeout' end reason.
			r.TimeoutSeconds = 28800
		}
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
	WorkspaceBind   string `json:"workspace_bind,omitempty"`
	FilesVolumeUUID string `json:"files_volume_uuid,omitempty"`
	// Interactive/TTY echo the launch mode recorded at accept time so a
	// replayed or recovered operation keeps its input/output contract.
	Interactive bool `json:"interactive,omitempty"`
	TTY         bool `json:"tty,omitempty"`
	// StdoutBase is the absolute byte offset of the earliest retained
	// interactive output byte. StdoutBytes is the absolute emitted
	// total, so the retained window is [StdoutBase, StdoutBytes). A
	// read offset below StdoutBase is an explicit gap, never silent
	// truncation. Zero for batch operations.
	StdoutBase      int64        `json:"stdout_base,omitempty"`
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
	// Quiesced is computed evidence filled at read time, never journalled:
	// the operation is terminal AND the runtime proves no physical writer
	// remains — nothing ever launched, or the operation container was
	// verifiably stopped and removed. Terminal state alone does NOT imply
	// quiescence: an indeterminate op whose container outcome was never
	// observed stays false until the reconcile loop confirms removal.
	Quiesced bool `json:"quiesced,omitempty"`
	// OutputAttached is computed at read time for interactive ops: the
	// journal pump is attached to the daemon journal and appending
	// records. False on a live op means output is degraded — the op may
	// still accept input, but emitted bytes are not being captured.
	OutputAttached bool `json:"output_attached,omitempty"`
}
type ProcessOutput struct {
	OperationID string `json:"operation_id"`
	Stream      string `json:"stream"`
	Offset      int64  `json:"offset"`
	NextOffset  int64  `json:"next_offset"`
	Content     string `json:"content"`
	EOF         bool   `json:"eof"`
	Truncated   bool   `json:"truncated"`
	// BaseOffset is the absolute offset of the earliest retained byte
	// for interactive streams. When Gap is true, the requested offset
	// was below BaseOffset: bytes [Offset, BaseOffset) are gone and the
	// returned content starts at BaseOffset. Batch operations always
	// report BaseOffset 0 / Gap false and keep their existing
	// Truncated semantics.
	BaseOffset int64 `json:"base_offset,omitempty"`
	Gap        bool  `json:"gap,omitempty"`
	// Gaps carries journaled loss boundaries (journal rotation/vanish,
	// uncertified resume) at absolute offsets >= the requested offset.
	// Each event marks a position where emitted bytes may have been
	// lost — the window size is genuinely unknown, so the event is a
	// boundary, never an invented byte range.
	Gaps []ProcessOutputGap `json:"gaps,omitempty"`
}

// ProcessOutputGap is one journaled output-loss boundary.
type ProcessOutputGap struct {
	At   int64  `json:"at"`
	Note string `json:"note,omitempty"`
}

// ProcessInputRequest writes bytes to an interactive operation's
// stdin. For a TTY operation the bytes go through the line
// discipline: an EOF keypress is the byte 0x04, an interrupt is 0x03 —
// delivery semantics are real, not simulated. EOF asks the backend to
// close the container's input stream where the transport allows it.
type ProcessInputRequest struct {
	ProcessLookupRequest
	Data []byte `json:"data"`
	EOF  bool   `json:"eof,omitempty"`
}

func (r ProcessInputRequest) Validate() error {
	if e := r.ProcessLookupRequest.Validate(); e != nil {
		return e
	}
	if len(r.Data) > 64<<10 {
		return fmt.Errorf("%w: input payload exceeds 64 KiB", ErrInvalidProcessRequest)
	}
	if len(r.Data) == 0 && !r.EOF {
		return fmt.Errorf("%w: empty input", ErrInvalidProcessRequest)
	}
	return nil
}

type ProcessResizeRequest struct {
	ProcessLookupRequest
	Cols int `json:"cols"`
	Rows int `json:"rows"`
}

func (r ProcessResizeRequest) Validate() error {
	if e := r.ProcessLookupRequest.Validate(); e != nil {
		return e
	}
	if r.Cols < 2 || r.Cols > 1000 || r.Rows < 2 || r.Rows > 500 {
		return fmt.Errorf("%w: terminal size out of range", ErrInvalidProcessRequest)
	}
	return nil
}

// processSignalAllowlist is the complete set of signals a caller may
// send to an interactive container's process group. SIGKILL is
// included deliberately: it is the honest way to stop a wedged
// session, and the resulting exit still lands as an ordinary terminal
// outcome. There is no free-form numeric signal path.
var processSignalAllowlist = map[string]string{
	"INT": "SIGINT", "TERM": "SIGTERM", "HUP": "SIGHUP",
	"QUIT": "SIGQUIT", "KILL": "SIGKILL", "TSTP": "SIGTSTP",
	"USR1": "SIGUSR1", "USR2": "SIGUSR2",
}

type ProcessSignalRequest struct {
	ProcessLookupRequest
	Signal string `json:"signal"`
}

func (r ProcessSignalRequest) Validate() error {
	if e := r.ProcessLookupRequest.Validate(); e != nil {
		return e
	}
	if _, ok := processSignalAllowlist[strings.ToUpper(r.Signal)]; !ok {
		return fmt.Errorf("%w: signal not permitted", ErrInvalidProcessRequest)
	}
	return nil
}

// ProcessInputReceipt is the evidence for one input write.
// Delivered means every byte was accepted by the container's input
// stream. Indeterminate means the stream was lost mid-write — the
// caller must not assume delivery or non-delivery and must not
// blindly replay, because a partial prefix may already have taken
// effect in the terminal.
type ProcessInputReceipt struct {
	Delivered     bool   `json:"delivered"`
	Indeterminate bool   `json:"indeterminate,omitempty"`
	Detail        string `json:"detail,omitempty"`
}
type ProcessCompletionReceipt struct {
	PersonalityAgentID string `json:"personality_agent_id"`
	OperationID        string `json:"operation_id"`
	EventID            string `json:"event_id"`
	CommandID          string `json:"command_id"`
	CommandSeq         uint64 `json:"command_seq"`
}
