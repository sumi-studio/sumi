package jobexec

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

// toolCallPrefix derives the backend operation's tool-call identity from
// the job id. One job ↔ one operation: the derived id is deterministic, so
// a restarted driver (or a duplicated sweep) addresses the same journal
// record instead of minting a second execution. job.start ids are
// server-derived ("op:<input>:<index>") and direct submissions are caller
// ids — both are stable strings ≤256 chars, well inside the 1024-char
// tool-call bound.
const toolCallPrefix = "job:"

// operationID is the deterministic backend operation for a job.
func operationID(j agentstate.Job) string {
	return runtimeprovision.ProcessOperationID(j.PersonaID, toolCallPrefix+j.JobID)
}

func processLookup(j agentstate.Job) runtimeprovision.ProcessLookupRequest {
	return runtimeprovision.ProcessLookupRequest{
		PersonalityAgentID: j.PersonaID,
		OperationID:        operationID(j),
	}
}

// processRequest maps a subprocess job spec to the host-agnostic process
// request. The job contract (validateJobRequest) and the process contract
// (ProcessStartRequest.Validate) bound differently — a spec that satisfies
// the first but not the second is returned as an error and the driver fails
// the job deterministically rather than retrying an impossible request.
//
// Every job launch asks for the job toolchain image and the canonical
// files scope: the selector names are allowlisted server-side, never
// free-form references, and the scope bind is resolved and verified by the
// provisioner — the driver cannot name a path or an image.
func processRequest(j agentstate.Job) (runtimeprovision.ProcessStartRequest, error) {
	rawArgv, _ := j.Request["command"].([]any)
	argv := make([]string, 0, len(rawArgv))
	for _, a := range rawArgv {
		s, ok := a.(string)
		if !ok {
			return runtimeprovision.ProcessStartRequest{}, fmt.Errorf("%w: command entries must be strings", runtimeprovision.ErrInvalidProcessRequest)
		}
		argv = append(argv, s)
	}
	if len(argv) == 0 {
		return runtimeprovision.ProcessStartRequest{}, fmt.Errorf("%w: command is required", runtimeprovision.ErrInvalidProcessRequest)
	}
	cwd, _ := j.Request["cwd"].(string)
	env := map[string]string{}
	if raw, ok := j.Request["env"].(map[string]any); ok {
		for k, v := range raw {
			s, ok := v.(string)
			if !ok {
				return runtimeprovision.ProcessStartRequest{}, fmt.Errorf("%w: env values must be strings", runtimeprovision.ErrInvalidProcessRequest)
			}
			env[k] = s
		}
	}
	if len(env) == 0 {
		env = nil
	}
	req := runtimeprovision.ProcessStartRequest{
		PersonalityAgentID:    j.PersonaID,
		OriginatingToolCallID: toolCallPrefix + j.JobID,
		Executable:            argv[0],
		Args:                  argv[1:],
		Cwd:                   cwd,
		Env:                   env,
		Image:                 "job",
		Workspace:             "files-scope",
	}
	if ms, ok := j.Request["timeout_ms"].(float64); ok && ms > 0 {
		req.TimeoutSeconds = int(math.Ceil(ms / 1000))
	}
	if err := req.Validate(); err != nil {
		return runtimeprovision.ProcessStartRequest{}, err
	}
	return req, nil
}

// jobStatusFor maps a terminal operation to the job verdict. Cancelled ops
// report cancelled only after the backend reached a durable terminal state
// — the job never reports cancelled while the process could still write.
func jobStatusFor(op runtimeprovision.ProcessOperation) (status, jobErr string) {
	switch op.State {
	case runtimeprovision.ProcessSucceeded:
		return "done", ""
	case runtimeprovision.ProcessCancelled:
		return "cancelled", ""
	case runtimeprovision.ProcessIndeterminate:
		if op.Error != "" {
			return "failed", op.Error
		}
		return "failed", "backend outcome indeterminate; the operation was not repeated"
	default: // ProcessFailed
		if op.Error != "" {
			return "failed", op.Error
		}
		if op.ExitCode != nil {
			return "failed", fmt.Sprintf("exit %d", *op.ExitCode)
		}
		return "failed", "process failed"
	}
}

// resultBytes bounds the output a job result reads from the journal — a
// head of output, where command identity and errors live.
const resultBytes = 16 << 10

// resultJSONBytes bounds the *encoded* result document. Output strings
// carry multi-byte runes and JSON escaping overhead, so the bound is
// measured after marshaling, not on the raw strings.
const resultJSONBytes = 48 << 10

// outcomeJSONBytes mirrors AttachLostOutcome's durable bound.
const outcomeJSONBytes = 48 << 10

// streamOutput is one stream's faithful record: available content (possibly
// truncated at the read bound), or an explicit unavailability marker.
// 'unavailable' is never confused with 'empty'.
type streamOutput struct {
	value     string
	truncated bool
	sanitized bool
	err       error
}

// jobResult builds the durable result for a terminal op. The shape follows
// the existing runner contract (exit_code/stdout/stderr/truncation/duration)
// plus backend evidence so a reader can trace the container that ran it.
// A stream that cannot be read is marked <stream>_unavailable with the read
// error — and the caller decides whether to commit now or retry first;
// nothing silently becomes "".
func (d *Driver) jobResult(ctx context.Context, op runtimeprovision.ProcessOperation) (map[string]any, error) {
	result := map[string]any{
		"backend":           "docker-process",
		"operation_id":      op.OperationID,
		"exit_code":         nil,
		"stdout":            "",
		"stderr":            "",
		"stdout_truncated":  false,
		"stderr_truncated":  false,
		"stdout_bytes":      op.StdoutBytes,
		"stderr_bytes":      op.StderrBytes,
		"files_volume_uuid": op.FilesVolumeUUID,
	}
	if op.ExitCode != nil {
		result["exit_code"] = *op.ExitCode
	}
	if op.Tombstone {
		// Fenced before launch — the record states the work never ran
		// rather than implying an empty cancelled execution.
		result["outcome"] = "never_started"
	}
	if op.Error != "" {
		if op.Error == "process timeout exceeded" {
			result["timed_out"] = true
		} else {
			result["backend_error"] = op.Error
		}
	}
	if op.StartedAt != nil && op.FinishedAt != nil {
		result["duration_ms"] = op.FinishedAt.Sub(*op.StartedAt).Milliseconds()
	}
	var readErr error
	for _, stream := range []string{"stdout", "stderr"} {
		out := d.readStream(ctx, op, stream)
		if out.err != nil {
			result[stream+"_unavailable"] = true
			result[stream+"_read_error"] = truncErr(out.err)
			if readErr == nil {
				readErr = out.err
			}
			continue
		}
		result[stream] = out.value
		result[stream+"_truncated"] = out.truncated
		if out.sanitized {
			result[stream+"_sanitized"] = true
		}
	}
	fitResult(result, resultJSONBytes, "stdout", "stderr")
	return result, readErr
}

// observedOutcome is the AttachLostOutcome payload — the same bounded
// actual-output contract as jobResult, recorded as evidence under the lost
// verdict. Read failures degrade per-stream the same way: the verdict's
// evidence records what could not be retrieved rather than pretending
// empty.
func (d *Driver) observedOutcome(ctx context.Context, op runtimeprovision.ProcessOperation) map[string]any {
	if op.Tombstone {
		// A fence record: the operation was cancelled before it ever
		// journaled a launch — 'absent', not 'cancelled after running'.
		return map[string]any{
			"state": "absent",
			"note":  "the deterministic operation was fenced before it ever journaled a launch; the job never reached the process service",
		}
	}
	out := map[string]any{"state": string(op.State)}
	if op.ExitCode != nil {
		out["exit_code"] = *op.ExitCode
	}
	if op.Error != "" {
		out["error"] = op.Error
	}
	if op.FinishedAt != nil {
		out["finished_at"] = op.FinishedAt.UTC().Format(time.RFC3339Nano)
	}
	if op.StartedAt != nil && op.FinishedAt != nil {
		out["duration_ms"] = op.FinishedAt.Sub(*op.StartedAt).Milliseconds()
	}
	for _, stream := range []string{"stdout", "stderr"} {
		so := d.readStream(ctx, op, stream)
		if so.err != nil {
			out[stream+"_unavailable"] = true
			out[stream+"_read_error"] = truncErr(so.err)
			out[stream] = ""
			continue
		}
		out[stream] = so.value
		out[stream+"_truncated"] = so.truncated
		if so.sanitized {
			out[stream+"_sanitized"] = true
		}
	}
	fitResult(out, outcomeJSONBytes, "stdout", "stderr")
	return out
}

// readStream pulls one bounded output head from the durable journal. The
// read asks for resultBytes; truncated is true when the journal reports
// more than we took (EOF false) or already cut at its own 1 MiB cap —
// a 16–100 KiB output is truthfully flagged rather than presented as
// complete. The content is made storable: invalid UTF-8 is replaced and
// control bytes are dropped so the result survives CompleteJob's jsonb
// constraints, and the sanitization is recorded.
func (d *Driver) readStream(ctx context.Context, op runtimeprovision.ProcessOperation, stream string) streamOutput {
	opCtx, cancel := d.opCtx(ctx)
	defer cancel()
	out, err := d.proc.ReadProcessOutput(opCtx, runtimeprovision.ProcessOutputRequest{
		ProcessLookupRequest: runtimeprovision.ProcessLookupRequest{
			PersonalityAgentID: op.PersonalityAgentID,
			OperationID:        op.OperationID,
		},
		Stream: stream,
		Limit:  resultBytes,
	})
	if err != nil {
		return streamOutput{err: err}
	}
	value, sanitized := sanitizeOutput(out.Content)
	truncated := out.Truncated || !out.EOF
	return streamOutput{value: value, truncated: truncated, sanitized: sanitized}
}

// sanitizeOutput makes raw process output storable and readable: invalid
// UTF-8 becomes the replacement rune (jsonb text cannot carry it) and
// control bytes other than tab/newline are dropped (NUL is rejected by
// jsonb outright). The second return reports whether the stored text is
// not a byte-exact rendering of the stream.
func sanitizeOutput(s string) (string, bool) {
	clean := strings.ToValidUTF8(s, "�")
	var b strings.Builder
	b.Grow(len(clean))
	for _, r := range clean {
		if r == '\n' || r == '\t' || (r >= 0x20 && r != 0x7f && utf8.ValidRune(r)) {
			b.WriteRune(r)
		}
	}
	out := b.String()
	return out, out != s
}

// fitResult shrinks the output fields until the document marshals under
// bound. JSON escaping and multi-byte runes make string length a bad proxy
// for document size, so the check runs on the encoded form. Streams are
// shortened longest-first and always flagged truncated — the record stays
// honest about being a prefix.
func fitResult(doc map[string]any, bound int, streams ...string) {
	for i := 0; i < 12; i++ {
		b, err := json.Marshal(doc)
		if err == nil && len(b) <= bound {
			return
		}
		longest, longestLen := "", 0
		for _, s := range streams {
			if v, ok := doc[s].(string); ok && len(v) > longestLen {
				longest, longestLen = s, len(v)
			}
		}
		if longestLen == 0 {
			return
		}
		// Cut at a rune boundary — a mid-rune slice would make the value
		// invalid UTF-8 and fail jsonb storage.
		cut, _ := sanitizeOutput(doc[longest].(string)[:longestLen/2])
		doc[longest] = cut
		doc[longest+"_truncated"] = true
		doc["output_fit_to_result_bound"] = true
	}
}

// truncErr bounds a recorded read error — the message is evidence, not a
// channel for unbounded backend text.
func truncErr(err error) string {
	s := err.Error()
	if len(s) > 512 {
		s = s[:512]
	}
	clean, _ := sanitizeOutput(s)
	return clean
}
