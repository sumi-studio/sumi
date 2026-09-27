package returnsession

// Decisive reproduction of the delayed completed-Local revocation defect
// (ROOT-CREDENTIAL-DISPOSITION.md). convergeFileState resolves the owner
// in an UNLOCKED read and then calls the persona-wide revocation in a
// separate transaction. The schedule under test:
//
//	T1  completed local-mode A is resolved as owner (unlocked check)
//	T2  newer Cloud B binds — owner becomes B
//	T3  B mints a real credential through MintFileCredential
//	T4  A's delayed revocation arrives carrying its stale verdict
//
// The pre-repair helper ran `WHERE persona_id = $1 AND status = 'active'`
// unconditionally under the lock and killed B's grant at T4. The repair
// re-resolves the owner inside the locked revocation transaction: at T4
// the owner is B, so A's call is a committed no-op and B's credential
// keeps authorizing. These are internal-package tests because the gap
// being exercised is between converge's two calls — the boundary
// primitive is invoked directly with the stale verdict it must defend.

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

func newID() string { return uuid.Must(uuid.NewV7()).String() }

// repairFixture: one human, one owned active persona, one Service.
func repairFixture(t *testing.T) (*Service, *pgxpool.Pool, context.Context, Owner) {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	human, persona := newID(), newID()
	if _, err := pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, human); err != nil {
		t.Fatal(err)
	}
	state := agentstate.NewStore(pool)
	if _, _, err := state.EnsurePersona(ctx, persona, nil, "Cloud secretary"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`,
		human, persona); err != nil {
		t.Fatal(err)
	}
	svc := New(pool, Config{FilePolicy: FilePolicyFixture})
	return svc, pool, ctx, Owner{HumanID: human, PersonaID: persona}
}

// seedSession creates a session via Create (real grant + epoch) then
// drives its row to the given bound status/mode directly — the same
// SQL-shortcut convention seal_race_test.go uses for durable states the
// test is not exercising.
func seedSession(t *testing.T, svc *Service, pool *pgxpool.Pool, ctx context.Context,
	owner Owner, fileMode, status, dstPlace string) (sessionID, grant string) {
	t.Helper()
	created, _, err := svc.Create(ctx, owner, fileMode)
	if err != nil {
		t.Fatalf("create %s session: %v", fileMode, err)
	}
	sessionID, grant = created.View.SessionID, created.Grant
	if dstPlace == "" {
		dstPlace = newID()
	}
	if _, err := pool.Exec(ctx, `UPDATE return_sessions
		SET status = $2, destination_placement_id = $3, destination_persona_id = $4,
		    destination_slot_state = 'absent', destination_bound_at = now()
		WHERE session_id = $1`, sessionID, status, dstPlace, owner.PersonaID); err != nil {
		t.Fatal(err)
	}
	return sessionID, grant
}

// The defect itself, proven on the pre-repair statement shape: once A's
// stale verdict enters an UNGUARDED persona-wide revocation, a credential
// B minted through the real API in the gap dies. Then the repaired call
// — same caller, same predicate, carrying A's session as the expected
// owner — is a committed no-op, and B's re-minted credential stays usable.
func TestStaleCompletedLocalRevokeCannotKillNewerGrant(t *testing.T) {
	svc, pool, ctx, owner := repairFixture(t)
	dst := newID()

	// A: completed local-mode return — the retained-copy owner. T1.
	sessA, _ := seedSession(t, svc, pool, ctx, owner, "local", StatusCompleted, dst)

	// B: newer Cloud return, bound and sealed. T2 — after A's unlocked
	// owner check would have read owner=A.
	sessB, grantB := seedSession(t, svc, pool, ctx, owner, "cloud", StatusSealed, dst)

	// T3: B mints through the real mint path — API-authorized grant.
	credB, err := svc.MintFileCredential(ctx, sessB, grantB)
	if err != nil {
		t.Fatalf("newer lineage mint: %v", err)
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credB.Token); err != nil {
		t.Fatalf("minted credential does not authorize: %v", err)
	}

	// BEFORE — the exact statement the pre-repair helper ran under the
	// lock with A's stale verdict: unconditionally kills B's grant.
	tag, err := pool.Exec(ctx, `UPDATE persona_file_tokens
		SET status = 'revoked', resolved_at = now()
		WHERE persona_id = $1 AND status = 'active'`, owner.PersonaID)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("pre-repair statement revoked %d rows (want 1 — B's grant)", tag.RowsAffected())
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credB.Token); err == nil {
		t.Fatal("defect not reproduced: B's credential survived the unguarded revoke")
	}

	// B re-mints — recovery from the lost grant, superseding the dead one.
	credB2, err := svc.MintFileCredential(ctx, sessB, grantB)
	if err != nil {
		t.Fatalf("re-mint: %v", err)
	}

	// AFTER — T4: A's delayed revocation arrives carrying the SAME
	// stale verdict, now guarded: the owner is re-resolved inside the
	// locked transaction, sees B, and the call revokes nothing.
	n, err := svc.revokeFileTokens(ctx, owner.PersonaID, sessA,
		`persona_id = $1 AND status = 'active'`)
	if err != nil {
		t.Fatalf("guarded revoke: %v", err)
	}
	if n != 0 {
		t.Fatalf("stale owner verdict revoked %d credentials (want 0)", n)
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credB2.Token); err != nil {
		t.Fatalf("B's credential must remain usable after the stale revoke: %v", err)
	}

	// The full path agrees: A's converge is now a complete no-op for
	// credentials — owner check or guarded revoke, B's grant survives.
	if err := svc.convergeFileState(ctx, sessA); err != nil {
		t.Fatalf("stale converge: %v", err)
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credB2.Token); err != nil {
		t.Fatalf("B's credential must survive the full stale converge: %v", err)
	}
}

// The guard must not neuter the legitimate case: when the completed
// local-mode session IS still the owner, its delayed persona-wide
// revocation retires the grants that predate its store decision.
func TestOwnerVerdictStillRevokesWhenCurrent(t *testing.T) {
	svc, pool, ctx, owner := repairFixture(t)
	dst := newID()

	// An older cloud-era grant that predates the local move.
	sessOld, grantOld := seedSession(t, svc, pool, ctx, owner, "cloud", StatusCompleted, dst)
	credOld, err := svc.MintFileCredential(ctx, sessOld, grantOld)
	if err != nil {
		t.Fatalf("older mint: %v", err)
	}
	// The older session must end completed-and-bound for its mint to be
	// legal — and it must NOT be the newest owner once A completes.
	sessA, _ := seedSession(t, svc, pool, ctx, owner, "local", StatusCompleted, dst)

	n, err := svc.revokeFileTokens(ctx, owner.PersonaID, sessA,
		`persona_id = $1 AND status = 'active'`)
	if err != nil {
		t.Fatalf("guarded revoke: %v", err)
	}
	if n != 1 {
		t.Fatalf("current owner revoked %d credentials (want 1)", n)
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credOld.Token); err == nil {
		t.Fatal("the superseded-era grant survived a legitimate revoke")
	}
}

// The "" escape is for inherently self-scoped predicates only: a dead
// session's revocation retires its own tokens regardless of who owns
// the store now — including a newer live lineage's store.
func TestSelfScopedRevokeIgnoresOwner(t *testing.T) {
	svc, pool, ctx, owner := repairFixture(t)
	dst := newID()

	sessA, grantA := seedSession(t, svc, pool, ctx, owner, "cloud", StatusSealed, dst)
	credA, err := svc.MintFileCredential(ctx, sessA, grantA)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	// A dies; a newer lineage binds and takes over.
	if _, err := pool.Exec(ctx, `UPDATE return_sessions SET status = 'aborted' WHERE session_id = $1`,
		sessA); err != nil {
		t.Fatal(err)
	}
	sessB, grantB := seedSession(t, svc, pool, ctx, owner, "cloud", StatusSealed, dst)

	n, err := svc.revokeFileTokens(ctx, owner.PersonaID, "",
		`persona_id = $1 AND session_id = $2 AND status = 'active'`, sessA)
	if err != nil {
		t.Fatalf("self-scoped revoke: %v", err)
	}
	if n != 1 {
		t.Fatalf("dead session's own grant was not revoked (n=%d)", n)
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credA.Token); err == nil {
		t.Fatal("dead session's grant still authorizes")
	}
	// And B — the current owner — can still mint: self-scoped death
	// revocations never constrain the live lineage.
	credB, err := svc.MintFileCredential(ctx, sessB, grantB)
	if err != nil {
		t.Fatalf("live lineage mint after self-scoped revoke: %v", err)
	}
	if _, _, err := svc.AuthorizeFileToken(ctx, credB.Token); err != nil {
		t.Fatalf("live lineage grant does not authorize: %v", err)
	}
}
