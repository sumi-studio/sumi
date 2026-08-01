package koseki

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestProviderUnlinkFenceSerializesFirebaseUIDAndReleasesOnTerminalState(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "provider-fence-owner")
	if err != nil {
		t.Fatal(err)
	}

	type beginResult struct {
		operation ProviderOperation
		nonce     string
		err       error
	}
	start := make(chan struct{})
	results := make(chan beginResult, 2)
	var wg sync.WaitGroup
	requests := []struct {
		provider string
		nonce    string
	}{
		{provider: "google.com", nonce: testNonce(t)},
		{provider: "github.com", nonce: testNonce(t)},
	}
	for _, request := range requests {
		request := request
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			operation, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-fence-owner", request.provider, "unlink", "account_settings", request.nonce)
			results <- beginResult{operation: operation, nonce: request.nonce, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var winner beginResult
	succeeded, fenced := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			succeeded++
			winner = result
		case errors.Is(result.err, ErrProviderOperationPending):
			fenced++
		default:
			t.Fatalf("concurrent begin: %v", result.err)
		}
	}
	if succeeded != 1 || fenced != 1 {
		t.Fatalf("succeeded=%d fenced=%d", succeeded, fenced)
	}

	repeated, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-fence-owner", winner.operation.Provider, "unlink", "account_settings", winner.nonce)
	if err != nil || repeated.OperationID != winner.operation.OperationID {
		t.Fatalf("same-nonce recovery: %+v %v", repeated, err)
	}
	if _, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-fence-owner", "github.com", "link", "account_settings", testNonce(t)); !errors.Is(err, ErrProviderOperationPending) {
		t.Fatalf("link crossed pending unlink fence: %v", err)
	}
	if _, err := store.FailProviderOperation(ctx, winner.operation.OperationID, winner.nonce, "cancelled"); err != nil {
		t.Fatal(err)
	}
	linkNonce := testNonce(t)
	link, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-fence-owner", "google.com", "link", "account_settings", linkNonce)
	if err != nil {
		t.Fatalf("released unlink fence for link: %v", err)
	}
	if _, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-fence-owner", "github.com", "unlink", "account_settings", testNonce(t)); !errors.Is(err, ErrProviderOperationPending) {
		t.Fatalf("unlink crossed pending link fence: %v", err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", link.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginProviderOperation(ctx, owner.HumanID, "provider-fence-owner", "github.com", "unlink", "account_settings", testNonce(t)); err != nil {
		t.Fatalf("expired link intent retained unlink fence: %v", err)
	}
}

func TestCompletedEmailLinkProofRequiresSameHumanAndFirebaseUID(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "email-proof-owner")
	if err != nil {
		t.Fatal(err)
	}
	other, err := store.AutoRegister(ctx, "firebase", "email-proof-other")
	if err != nil {
		t.Fatal(err)
	}
	assertProof := func(humanID, firebaseUID string, want bool) {
		t.Helper()
		got, err := store.HasCompletedEmailLinkProof(ctx, humanID, firebaseUID)
		if err != nil || got != want {
			t.Fatalf("proof human=%s uid=%s: got=%v want=%v err=%v", humanID, firebaseUID, got, want, err)
		}
	}
	assertProof(owner.HumanID, "email-proof-owner", false)

	providerNonce := testNonce(t)
	providerFlow, err := store.StartAuthFlow(ctx, StartAuthFlowRequest{
		Intent: IntentSignIn, Channel: ChannelProvider, ExpectedProvider: "github.com",
		Continuation: "/direct-chat", Nonce: providerNonce, TTL: 10 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResolveAuthProof(ctx, providerFlow.FlowID, providerNonce, VerifiedIdentity{
		FirebaseUID: "email-proof-owner", SignInProvider: "github.com", ProviderSubject: "email-proof-github",
	}); err != nil {
		t.Fatal(err)
	}
	assertProof(owner.HumanID, "email-proof-owner", false)

	emailNonce := testNonce(t)
	emailFlow := startEmailFlow(t, ctx, store, IntentSignIn, "proof@example.com", emailNonce)
	if _, err := store.ResolveAuthProof(ctx, emailFlow.FlowID, emailNonce, emailProof("email-proof-owner", "proof@example.com")); err != nil {
		t.Fatal(err)
	}
	assertProof(owner.HumanID, "email-proof-owner", true)
	assertProof(owner.HumanID, "email-proof-other", false)
	assertProof(other.HumanID, "email-proof-owner", false)
}

func TestExpiredProviderUnlinkCanStillTerminalizeItsDurableFence(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "expired-unlink-owner")
	if err != nil {
		t.Fatal(err)
	}
	nonce := testNonce(t)
	operation, err := store.BeginProviderOperation(ctx, owner.HumanID, "expired-unlink-owner", "github.com", "unlink", "account_settings", nonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", operation.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.FailProviderOperation(ctx, operation.OperationID, nonce, "last_login_method"); err != nil {
		t.Fatalf("terminalize expired unlink: %v", err)
	}
	status, err := store.ProviderOperationStatus(ctx, owner.HumanID, operation.OperationID, nonce)
	if err != nil || status.Status != "failed" || status.TerminalOutcome != "last_login_method" {
		t.Fatalf("expired unlink status: %+v %v", status, err)
	}
}

func TestProviderLinkSameNonceReplayHonorsExpiryAndPendingUnlinkFence(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "link-replay-owner")
	if err != nil {
		t.Fatal(err)
	}

	expiredNonce := testNonce(t)
	expired, err := store.BeginProviderOperation(ctx, owner.HumanID, "link-replay-owner", "google.com", "link", "account_settings", expiredNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "UPDATE provider_operations SET expires_at=now()-interval '1 second' WHERE operation_id=$1", expired.OperationID); err != nil {
		t.Fatal(err)
	}
	unlinkNonce := testNonce(t)
	unlink, err := store.BeginProviderOperation(ctx, owner.HumanID, "link-replay-owner", "github.com", "unlink", "account_settings", unlinkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginProviderOperation(ctx, owner.HumanID, "link-replay-owner", "google.com", "link", "account_settings", expiredNonce); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("expired same-nonce link replay: %v", err)
	}
	if _, err := store.FailProviderOperation(ctx, unlink.OperationID, unlinkNonce, "cancelled"); err != nil {
		t.Fatal(err)
	}

	linkNonce := testNonce(t)
	link, err := store.BeginProviderOperation(ctx, owner.HumanID, "link-replay-owner", "google.com", "link", "account_settings", linkNonce)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, "ALTER TABLE provider_operations DISABLE TRIGGER provider_operations_pending_unlink_uid_fence"); err != nil {
		t.Fatal(err)
	}
	legacyUnlinkNonce := testNonce(t)
	legacyUnlink, err := store.BeginProviderOperation(ctx, owner.HumanID, "link-replay-owner", "github.com", "unlink", "account_settings", legacyUnlinkNonce)
	if _, enableErr := store.pool.Exec(ctx, "ALTER TABLE provider_operations ENABLE TRIGGER provider_operations_pending_unlink_uid_fence"); enableErr != nil {
		t.Fatal(enableErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.BeginProviderOperation(ctx, owner.HumanID, "link-replay-owner", "google.com", "link", "account_settings", linkNonce); !errors.Is(err, ErrProviderOperationPending) {
		t.Fatalf("same-nonce link replay bypassed pending unlink fence: %v", err)
	}
	if link.OperationID == "" || legacyUnlink.OperationID == "" {
		t.Fatal("legacy conflict fixture was not persisted")
	}
}

func TestProviderOperationExpiryUsesDatabaseClockForStatusAndCompletion(t *testing.T) {
	store, ctx := authFlowStore(t)
	owner, err := store.AutoRegister(ctx, "firebase", "database-clock-owner")
	if err != nil {
		t.Fatal(err)
	}
	nonce := testNonce(t)
	operation, err := store.BeginProviderOperation(ctx, owner.HumanID, "database-clock-owner", "github.com", "link", "account_settings", nonce)
	if err != nil {
		t.Fatal(err)
	}
	if got := operation.ExpiresAt.Sub(operation.CreatedAt); got != ProviderOperationTTL {
		t.Fatalf("provider TTL was not derived from one DB timestamp: got %s want %s", got, ProviderOperationTTL)
	}

	var expiresAt, databaseNow time.Time
	if err := store.pool.QueryRow(ctx, `UPDATE provider_operations
		SET expires_at=now()-interval '1 second' WHERE operation_id=$1
		RETURNING expires_at, now()`, operation.OperationID).Scan(&expiresAt, &databaseNow); err != nil {
		t.Fatal(err)
	}
	// This models the reported skew: PostgreSQL has expired the operation while
	// a lagging API wall clock would still consider it live.
	laggingAPINow := databaseNow.Add(-time.Hour)
	if !laggingAPINow.Before(expiresAt) || expiresAt.After(databaseNow) {
		t.Fatalf("invalid skew fixture: API=%s expires=%s DB=%s", laggingAPINow, expiresAt, databaseNow)
	}

	if _, err := store.ProviderOperationStatus(ctx, owner.HumanID, operation.OperationID, nonce); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("status followed API clock instead of DB expiry: %v", err)
	}
	if _, err := store.PendingProviderOperation(ctx, operation.OperationID, nonce); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("pending completion followed API clock instead of DB expiry: %v", err)
	}
	if _, err := store.CompleteProviderLink(ctx, operation.OperationID, nonce, "database-clock-owner", "github-subject"); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("link completion followed API clock instead of DB expiry: %v", err)
	}
	if _, err := store.FailProviderOperation(ctx, operation.OperationID, nonce, "cancelled"); !errors.Is(err, ErrAuthFlowExpired) {
		t.Fatalf("link failure followed API clock instead of DB expiry: %v", err)
	}
}

func TestCompleteProviderLinkWaitsForUIDFenceAndRechecksDatabaseExpiry(t *testing.T) {
	store, ctx := authFlowStore(t)
	const (
		firebaseUID     = "link-completion-fence-owner"
		provider        = "github.com"
		providerSubject = "link-completion-fence-subject"
	)
	owner, err := store.AutoRegister(ctx, "firebase", firebaseUID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BindCredential(ctx, provider, providerSubject, owner.HumanID); err != nil {
		t.Fatal(err)
	}
	nonce := testNonce(t)
	operation, err := store.BeginProviderOperation(ctx, owner.HumanID, firebaseUID, provider, "link", "account_settings", nonce)
	if err != nil {
		t.Fatal(err)
	}

	// Model the unlink boundary while its Firebase deletion and local commit are
	// in flight. Completion must wait here before it locks or inspects the link
	// operation, then evaluate expiry using the database's current wall clock.
	unlinkTx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unlinkTx.Rollback(ctx) }()
	var unlinkPID int
	if err := unlinkTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&unlinkPID); err != nil {
		t.Fatal(err)
	}
	if _, err := unlinkTx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "provider-unlink:"+firebaseUID); err != nil {
		t.Fatal(err)
	}

	completion := make(chan error, 1)
	go func() {
		_, err := store.CompleteProviderLink(ctx, operation.OperationID, nonce, firebaseUID, providerSubject)
		completion <- err
	}()

	var completionStartedAt time.Time
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	waiting := false
	for !waiting {
		select {
		case err := <-completion:
			t.Fatalf("link completion escaped the unlink fence before expiry: %v", err)
		case <-poll.C:
			err := store.pool.QueryRow(ctx, `SELECT a.xact_start
				FROM pg_stat_activity a
				WHERE a.datname=current_database()
				  AND $1::integer=ANY(pg_blocking_pids(a.pid))
				ORDER BY a.xact_start LIMIT 1`, unlinkPID).Scan(&completionStartedAt)
			switch {
			case err == nil:
				waiting = true
			case errors.Is(err, pgx.ErrNoRows):
			default:
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("link completion never waited on the unlink boundary")
		}
	}
	// Expire the operation just after the waiting transaction began. Its
	// transaction-start now() remains before this instant; clock_timestamp()
	// after the UID lock is released is after it.
	var expiresAt time.Time
	if err := unlinkTx.QueryRow(ctx, `UPDATE provider_operations
		SET expires_at=$2::timestamptz+interval '1 millisecond'
		WHERE operation_id=$1 RETURNING expires_at`, operation.OperationID, completionStartedAt).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	var expiredAtDatabase bool
	if err := unlinkTx.QueryRow(ctx, "SELECT clock_timestamp() > $1", expiresAt).Scan(&expiredAtDatabase); err != nil {
		t.Fatal(err)
	}
	if !expiredAtDatabase {
		t.Fatalf("database clock did not cross fixture expiry: started=%s expires=%s", completionStartedAt, expiresAt)
	}
	if _, err := unlinkTx.Exec(ctx, `UPDATE credentials SET active=false, unlinked_at=clock_timestamp()
		WHERE provider=$1 AND external_subject=$2 AND human_id=$3`, provider, providerSubject, owner.HumanID); err != nil {
		t.Fatal(err)
	}
	if err := unlinkTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-completion:
		if !errors.Is(err, ErrAuthFlowExpired) {
			t.Fatalf("completion did not recheck expiry after UID fence: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("link completion remained blocked after unlink commit")
	}
	var active bool
	if err := store.pool.QueryRow(ctx, `SELECT active FROM credentials
		WHERE provider=$1 AND external_subject=$2 AND human_id=$3`, provider, providerSubject, owner.HumanID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatal("expired link completion reactivated the credential after unlink")
	}
}

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
