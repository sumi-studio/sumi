package processoperations

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/runtimeprovision"
)

const testPA = "01a08510-9330-7c92-87c4-6b31c0c98836"

type fakeBackend struct {
	starts  []runtimeprovision.ProcessStartRequest
	pending []runtimeprovision.ProcessOperation
	acks    []runtimeprovision.ProcessCompletionReceipt
	failAck bool
}

func (b *fakeBackend) StartProcess(_ context.Context, r runtimeprovision.ProcessStartRequest) (runtimeprovision.ProcessOperation, error) {
	b.starts = append(b.starts, r)
	return runtimeprovision.ProcessOperation{PersonalityAgentID: r.PersonalityAgentID}, nil
}
func (*fakeBackend) ProcessStatus(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	return runtimeprovision.ProcessOperation{}, nil
}
func (*fakeBackend) ReadProcessOutput(context.Context, runtimeprovision.ProcessOutputRequest) (runtimeprovision.ProcessOutput, error) {
	return runtimeprovision.ProcessOutput{}, nil
}
func (*fakeBackend) CancelProcess(context.Context, runtimeprovision.ProcessLookupRequest) (runtimeprovision.ProcessOperation, error) {
	return runtimeprovision.ProcessOperation{}, nil
}
func (b *fakeBackend) PendingProcessCompletions(context.Context) ([]runtimeprovision.ProcessOperation, error) {
	return b.pending, nil
}
func (b *fakeBackend) AcknowledgeProcessCompletion(_ context.Context, r runtimeprovision.ProcessCompletionReceipt) error {
	b.acks = append(b.acks, r)
	if b.failAck {
		b.failAck = false
		return errors.New("receipt persistence unavailable")
	}
	return nil
}

func TestStartRequiresTransportOwnerAndCurrentEpoch(t *testing.T) {
	body := `{"originating_tool_call_id":"call-1","executable":"/bin/true","args":[],"cwd":".","timeout_seconds":10}`
	b := &fakeBackend{}
	s := &Server{Backend: b}
	auth := agentevents.LocalRuntimeAuthorization{PersonalityAgentID: testPA}
	for _, tc := range []struct {
		body  string
		epoch bool
		code  int
	}{
		{body, true, 200},
		{body, false, 409},
		{strings.TrimSuffix(body, "}") + `,"personality_agent_id":"someone-else"}`, true, 400},
	} {
		before := len(b.starts)
		released := false
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/process-operations/start", strings.NewReader(tc.body))
		s.handler("start")(w, r, auth, func() { released = true }, func(op func() error) (bool, error) {
			if !released {
				t.Fatal("body read pinned the initial epoch lease")
			}
			if !tc.epoch {
				return false, nil
			}
			return true, op()
		})
		if w.Code != tc.code {
			t.Fatalf("got %d want %d: %s", w.Code, tc.code, w.Body.String())
		}
		if tc.code == 200 {
			if len(b.starts) != before+1 || b.starts[before].PersonalityAgentID != testPA {
				t.Fatal("start lost transport owner")
			}
		} else if len(b.starts) != before {
			t.Fatal("invalid or stale request started a process")
		}
	}
}

func TestStartAcceptsEscapedArgumentsWithinArgvLimit(t *testing.T) {
	argument := strings.Repeat("\x01", 32*1024-len("/bin/echo"))
	body, err := json.Marshal(startBody{OriginatingToolCallID: "call-escaped", Executable: "/bin/echo", Args: []string{argument}})
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBackend{}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/process-operations/start", strings.NewReader(string(body)))
	(&Server{Backend: b}).handler("start")(w, r,
		agentevents.LocalRuntimeAuthorization{PersonalityAgentID: testPA}, func() {},
		func(op func() error) (bool, error) { return true, op() })
	if w.Code != 200 || len(b.starts) != 1 || b.starts[0].Args[0] != argument {
		t.Fatalf("valid escaped argv rejected: status=%d", w.Code)
	}
}

type fakeDelivery struct {
	receipts         map[string]Receipt
	prepares, admits int
	failEvent        string
}

func (d *fakeDelivery) Prepare(context.Context, string) (func(), error) {
	d.prepares++
	return func() {}, nil
}
func (d *fakeDelivery) Lookup(_ context.Context, key string, op runtimeprovision.ProcessOperation) (Receipt, bool, error) {
	if op.EventID == d.failEvent {
		return Receipt{}, false, errors.New("unavailable event")
	}
	r, ok := d.receipts[key]
	return r, ok, nil
}
func (d *fakeDelivery) Admit(_ context.Context, key string, op runtimeprovision.ProcessOperation) (Receipt, error) {
	d.admits++
	r := Receipt{CommandID: op.EventID, Seq: uint64(d.admits)}
	d.receipts[key] = r
	return r, nil
}

func terminalOperation() runtimeprovision.ProcessOperation {
	now := time.Date(2026, 9, 9, 10, 0, 0, 0, time.UTC)
	exit := 0
	return runtimeprovision.ProcessOperation{PersonalityAgentID: testPA, OperationID: strings.Repeat("a", 64), OriginatingToolCallID: "call-1", EventID: "01a08578-d9b5-731b-8f8c-d57dd25e8087", State: "succeeded", FinishedAt: &now, ExitCode: &exit, StdoutBytes: 5}
}

func TestLostCompletionAcknowledgmentDoesNotReadmitOrRestart(t *testing.T) {
	op := terminalOperation()
	b := &fakeBackend{pending: []runtimeprovision.ProcessOperation{op}, failAck: true}
	d := &fakeDelivery{receipts: map[string]Receipt{}}
	s := &Server{Backend: b, Delivery: d}
	if s.DeliverPending(context.Background()) == nil {
		t.Fatal("lost receipt acknowledgment was hidden")
	}
	if err := s.DeliverPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.prepares != 1 || d.admits != 1 || len(b.starts) != 0 {
		t.Fatalf("duplicated work: prepare=%d admit=%d starts=%d", d.prepares, d.admits, len(b.starts))
	}
	if len(b.acks) != 2 || b.acks[0] != b.acks[1] || b.acks[0].OperationID != op.OperationID {
		t.Fatal("receipt identity changed on retry")
	}
}

func TestUnavailableCompletionDoesNotPreventOtherDelivery(t *testing.T) {
	first := terminalOperation()
	second := first
	second.EventID = "01a08578-d9b5-731b-8f8c-d57dd25e8088"
	second.OperationID = strings.Repeat("b", 64)
	b := &fakeBackend{pending: []runtimeprovision.ProcessOperation{first, second}}
	d := &fakeDelivery{receipts: map[string]Receipt{}, failEvent: first.EventID}
	if (&Server{Backend: b, Delivery: d}).DeliverPending(context.Background()) == nil {
		t.Fatal("unavailable event must be reported")
	}
	if d.admits != 1 || len(b.acks) != 1 || b.acks[0].EventID != second.EventID {
		t.Fatal("one failure starved another completion")
	}
}

func TestCompletionPreservesOwnOperationWithoutHumanUtterance(t *testing.T) {
	op := terminalOperation()
	p, body, err := completionInput("local", op)
	if err != nil {
		t.Fatal(err)
	}
	if p.Actor.Kind != "personality_agent" || p.Actor.PrincipalID != testPA || p.Source.Surface != "workspace_operation" || p.Source.OperationID != op.OperationID {
		t.Fatal("completion lost its owner or became another source")
	}
	if string(body) != `{"type":"external_event","content":""}` {
		t.Fatal("fabricated participant utterance")
	}
	op.FinishedAt = nil
	if _, _, err := completionInput("local", op); err == nil {
		t.Fatal("nonterminal operation was delivered")
	}
}

func TestCompletionReconcilesPersistedAdmissionAfterRestartWithoutWakingPA(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := agentevents.OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	op := terminalOperation()
	p, command, err := completionInput("local", op)
	if err != nil {
		t.Fatal(err)
	}
	key := "process-completed/" + op.EventID
	first, err := store.Append(ctx, p, key, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = agentevents.OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runtimeDir := t.TempDir()
	if err := os.Chmod(runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	gateway, err := agentevents.OpenDurableGateway(runtimeDir, store)
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBackend{pending: []runtimeprovision.ProcessOperation{op}}
	// No spawner or running runtime: replay must use the persisted receipt.
	delivery := &GatewayDelivery{Gateway: gateway, TenantID: "local"}
	if err := (&Server{Backend: b, Delivery: delivery}).DeliverPending(ctx); err != nil {
		t.Fatal(err)
	}
	if len(b.acks) != 1 || b.acks[0].CommandID != first.CommandID || b.acks[0].CommandSeq != first.Seq {
		t.Fatal("completion receipt was not recovered")
	}
	changed := op
	changed.StdoutBytes++
	if _, _, err := delivery.Lookup(ctx, key, changed); err == nil {
		t.Fatal("changed completion reused an existing receipt")
	}
	commands, err := store.CatchUp(ctx, testPA, 0)
	if err != nil || len(commands) != 1 {
		t.Fatalf("recovery duplicated completion: commands=%d err=%v", len(commands), err)
	}
}
