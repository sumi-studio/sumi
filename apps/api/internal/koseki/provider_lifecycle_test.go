package koseki

import (
	"errors"
	"reflect"
	"testing"
)

func TestProviderOperationStatusRecoversPendingAndTerminalStates(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "provider-status-owner")
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.AutoRegister(ctx, "firebase", "provider-status-other")
	if err != nil {
		t.Fatal(err)
	}

	pendingNonce := testNonce(t)
	pending, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-status-owner", "github.com", "link", "account_settings", pendingNonce)
	if err != nil {
		t.Fatal(err)
	}
	gotPending, err := store.ProviderOperationStatus(ctx, owner.HumanID, pending.OperationID, pendingNonce)
	if err != nil {
		t.Fatal(err)
	}
	if gotPending.Status != "pending" || gotPending.Operation != "link" || gotPending.Provider != "github.com" ||
		gotPending.TerminalOutcome != "" || gotPending.CompletedAt != nil {
		t.Fatalf("pending status: %+v", gotPending)
	}
	if _, err := store.ProviderOperationStatus(ctx, owner.HumanID, pending.OperationID, testNonce(t)); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("wrong nonce: %v", err)
	}
	if _, err := store.ProviderOperationStatus(ctx, other.HumanID, pending.OperationID, pendingNonce); !errors.Is(err, ErrAuthProofMismatch) {
		t.Fatalf("wrong Human: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", pending.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderOperationStatus(ctx, owner.HumanID, pending.OperationID, pendingNonce); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("expired pending: %v", err)
	}

	linkNonce := testNonce(t)
	link, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-status-owner", "github.com", "link", "same_email_recovery", linkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, link.OperationID, linkNonce, "provider-status-owner", "provider-status-github"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", link.OperationID); err != nil {
		t.Fatal(err)
	}
	linked, err := store.ProviderOperationStatus(ctx, owner.HumanID, link.OperationID, linkNonce)
	if err != nil {
		t.Fatalf("expired terminal status: %v", err)
	}
	if linked.Status != "completed" || linked.TerminalOutcome != "linked" || linked.CompletedAt == nil {
		t.Fatalf("linked status: %+v", linked)
	}

	alreadyNonce := testNonce(t)
	already, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-status-owner", "github.com", "link", "notice_action", alreadyNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderLink(ctx, already.OperationID, alreadyNonce, "provider-status-owner", "provider-status-github"); err != nil {
		t.Fatal(err)
	}
	alreadyLinked, err := store.ProviderOperationStatus(ctx, owner.HumanID, already.OperationID, alreadyNonce)
	if err != nil || alreadyLinked.Status != "completed" || alreadyLinked.TerminalOutcome != "already_linked" {
		t.Fatalf("already-linked status: %+v %v", alreadyLinked, err)
	}

	unlinkNonce := testNonce(t)
	unlink, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-status-owner", "github.com", "unlink", "account_settings", unlinkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteProviderUnlink(ctx, unlink.OperationID, unlinkNonce, "provider-status-owner", "provider-status-github"); err != nil {
		t.Fatal(err)
	}
	unlinked, err := store.ProviderOperationStatus(ctx, owner.HumanID, unlink.OperationID, unlinkNonce)
	if err != nil || unlinked.Status != "completed" || unlinked.TerminalOutcome != "unlinked" {
		t.Fatalf("unlinked status: %+v %v", unlinked, err)
	}

	failNonce := testNonce(t)
	failedOperation, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-status-owner", "google.com", "link", "provider_sign_in", failNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FailProviderOperation(ctx, failedOperation.OperationID, failNonce, "cancelled"); err != nil {
		t.Fatal(err)
	}
	failed, err := store.ProviderOperationStatus(ctx, owner.HumanID, failedOperation.OperationID, failNonce)
	if err != nil || failed.Status != "failed" || failed.TerminalOutcome != "cancelled" || failed.CompletedAt == nil {
		t.Fatalf("failed status: %+v %v", failed, err)
	}

	var operationsBefore, eventsBefore int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM provider_operations").Scan(&operationsBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM credential_security_events").Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	repeated, err := store.ProviderOperationStatus(ctx, owner.HumanID, failedOperation.OperationID, failNonce)
	if err != nil {
		t.Fatal(err)
	}
	var operationsAfter, eventsAfter int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM provider_operations").Scan(&operationsAfter); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM credential_security_events").Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(failed, repeated) || operationsBefore != operationsAfter || eventsBefore != eventsAfter {
		t.Fatalf("status was not stable/read-only: first=%+v repeated=%+v operations=%d/%d events=%d/%d",
			failed, repeated, operationsBefore, operationsAfter, eventsBefore, eventsAfter)
	}
}

func TestProviderOperationStatusFailsClosedWithoutOneConsistentAuditEvent(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "provider-audit-owner")
	if err != nil {
		t.Fatal(err)
	}

	missingNonce := testNonce(t)
	missing, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-audit-owner", "github.com", "link", "account_settings", missingNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE provider_operations SET status='completed',
		terminal_outcome='linked', completed_at=now() WHERE operation_id=$1`, missing.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderOperationStatus(ctx, owner.HumanID, missing.OperationID, missingNonce); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("missing audit event: %v", err)
	}

	contradictoryNonce := testNonce(t)
	contradictory, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-audit-owner", "github.com", "link", "account_settings", contradictoryNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO credential_security_events
		(operation_id, human_id, provider, event_type, decision_path, terminal_outcome)
		VALUES ($1,$2,'github.com','provider_unlinked','account_settings','unlinked')`,
		contradictory.OperationID, owner.HumanID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE provider_operations SET status='completed',
		terminal_outcome='linked', completed_at=now() WHERE operation_id=$1`, contradictory.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderOperationStatus(ctx, owner.HumanID, contradictory.OperationID, contradictoryNonce); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("contradictory audit event: %v", err)
	}

	duplicateNonce := testNonce(t)
	duplicate, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-audit-owner", "google.com", "link", "provider_sign_in", duplicateNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.FailProviderOperation(ctx, duplicate.OperationID, duplicateNonce, "cancelled"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO credential_security_events
		(operation_id, human_id, provider, event_type, decision_path, terminal_outcome)
		VALUES ($1,$2,'google.com','provider_link_failed','provider_sign_in','cancelled')`,
		duplicate.OperationID, owner.HumanID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProviderOperationStatus(ctx, owner.HumanID, duplicate.OperationID, duplicateNonce); !errors.Is(err, ErrInvalidAuthFlow) {
		t.Fatalf("duplicate audit events: %v", err)
	}
}
