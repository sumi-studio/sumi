// Package transfersessiontest is FIXTURE authority for tests only. It stands
// in for the authentication adapter and account transaction the account
// owner has not supplied yet. It is not Firebase, not the auth flow, and not
// signup acceptance; never mount it on a production server.
package transfersessiontest

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

var ErrFixtureProof = errors.New("fixture flow is unknown, mismatched or expired")

// Proof is a fixture registration-flow table: flow id + nonce → the
// credential that flow "proved", until it expires.
type Proof struct {
	mu    sync.Mutex
	flows map[string]flow
}

type flow struct {
	nonce   string
	subject transfersession.Subject
	expires time.Time
}

func NewProof() *Proof { return &Proof{flows: map[string]flow{}} }

func (p *Proof) Add(flowID, nonce, firebaseUID string, ttl time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flows[flowID] = flow{nonce: nonce,
		subject: transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: firebaseUID},
		expires: time.Now().Add(ttl)}
}

func (p *Proof) live(flowID, nonce string) (transfersession.Subject, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.flows[flowID]
	if !ok || f.nonce != nonce || !time.Now().Before(f.expires) {
		return transfersession.Subject{}, ErrFixtureProof
	}
	return f.subject, nil
}

func (p *Proof) RegistrantSubject(_ context.Context, _ *http.Request, flowID, nonce string) (transfersession.Subject, error) {
	return p.live(flowID, nonce)
}

// ProvisionAccount stands in for the account transaction: it checks the
// fixture flow is live, creates a humans row and the firebase credential,
// claims and provisions the session in one transaction, and returns the
// human id. It creates no agents, employments, secrets or Direct Chat — that
// is the account owner's integration. beforeCommit, when set, runs inside
// the transaction after provisioning (tests use it to race other callers).
func (p *Proof) ProvisionAccount(ctx context.Context, pool *pgxpool.Pool, flowID, nonce, sessionID string,
	beforeCommit func(pgx.Tx) error) (string, string, error) {
	subj, err := p.live(flowID, nonce)
	if err != nil {
		return "", "", err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	claim, err := transfersession.ClaimInTx(ctx, tx, sessionID, subj)
	if err != nil {
		return "", "", err
	}
	humanID := uuid.Must(uuid.NewV7()).String()
	if _, err := tx.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		return "", "", err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO credentials (provider, external_subject, human_id) VALUES ($1, $2, $3)`,
		subj.Provider, subj.Subject, humanID); err != nil {
		return "", "", err
	}
	if err := transfersession.ProvisionInTx(ctx, tx, claim, humanID); err != nil {
		return "", "", err
	}
	if beforeCommit != nil {
		if err := beforeCommit(tx); err != nil {
			return "", "", err
		}
	}
	return humanID, claim.PersonaID, tx.Commit(ctx)
}
