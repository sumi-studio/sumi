package runtimeprovision

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrProcessNotFound = errors.New("process operation not found")
var ErrProcessBusy = errors.New("process capacity exhausted")
var ErrInvalidProcessRequest = errors.New("invalid process request")

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
}

func (r ProcessLookupRequest) Validate() error {
	if _, e := uuid.Parse(r.PersonalityAgentID); e != nil {
		return fmt.Errorf("%w: invalid personality agent ID", ErrInvalidProcessRequest)
	}
	b, e := hex.DecodeString(r.OperationID)
	if e != nil || len(b) != 32 || strings.ToLower(r.OperationID) != r.OperationID {
		return fmt.Errorf("%w: invalid operation ID", ErrInvalidProcessRequest)
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
	OperationID           string       `json:"operation_id"`
	PersonalityAgentID    string       `json:"personality_agent_id"`
	OriginatingToolCallID string       `json:"originating_tool_call_id"`
	Executable            string       `json:"executable"`
	Args                  []string     `json:"args"`
	Cwd                   string       `json:"cwd"`
	TimeoutSeconds        int          `json:"timeout_seconds"`
	State                 ProcessState `json:"state"`
	EventID               string       `json:"event_id"`
	OccurredAt            time.Time    `json:"occurred_at"`
	StartedAt             *time.Time   `json:"started_at,omitempty"`
	FinishedAt            *time.Time   `json:"finished_at,omitempty"`
	ExitCode              *int         `json:"exit_code,omitempty"`
	StdoutBytes           int64        `json:"stdout_bytes"`
	StderrBytes           int64        `json:"stderr_bytes"`
	StdoutTruncated       bool         `json:"stdout_truncated"`
	StderrTruncated       bool         `json:"stderr_truncated"`
	Error                 string       `json:"error,omitempty"`
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
