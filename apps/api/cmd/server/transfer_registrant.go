package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// transferPublicBaseURLEnv is the trusted public base URL move URLs are
// built from — never the request's Host header. It must be https except on
// literal loopback, which transfersession.NewServer enforces.
const transferPublicBaseURLEnv = "SUMI_TRANSFER_PUBLIC_BASE_URL"

// transferSweepInterval bounds how long a committed activation obligation,
// an interrupted import promotion, or a closed session's staged copy waits
// for its reconcile step after the request that owed it is gone.
const transferSweepInterval = 30 * time.Second

var errRegistrantProofRejected = errors.New("the registration flow is not a live verified proof")

// registrantFlowProof is the production transfersession.RegistrantProof: it
// derives the claimed credential from the live verified auth flow the
// request names — never from a request-body subject or an ambient session.
// The request must carry the browser origin, the CSRF double-submit token,
// and the same browser epoch the flow was bound to.
type registrantFlowProof struct {
	store   *koseki.Store
	origins []string
}

func (p registrantFlowProof) RegistrantSubject(ctx context.Context, r *http.Request, flowID, nonce string) (transfersession.Subject, error) {
	if !agentevents.BrowserOriginAllowed(r, p.origins) || !agentevents.BrowserCSRFValid(r) {
		return transfersession.Subject{}, errRegistrantProofRejected
	}
	firebaseUID, flowEpoch, err := p.store.RegistrantProofSubject(ctx, flowID, nonce)
	if err != nil {
		return transfersession.Subject{}, err
	}
	requestEpoch, err := agentevents.RequestBrowserEpochHash(r)
	if err != nil {
		return transfersession.Subject{}, err
	}
	// A flow bound to a browser epoch proves its subject only for that jar;
	// a flow without one (a non-browser start) has nothing to compare.
	if flowEpoch != "" && requestEpoch != flowEpoch {
		return transfersession.Subject{}, errRegistrantProofRejected
	}
	return transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: firebaseUID}, nil
}

// secretaryTransferMount is the transfer server's service (kept for the
// background sweep and handed to the registration store's claim path) and
// its mounted routes.
type secretaryTransferMount struct {
	service *transfersession.Service
	server  *transfersession.Server
}

// secretaryTransferFromEnv wires the secretary-move surface. With the env
// unset the feature is simply off: no routes, no sweep, and the account
// transaction's claim consult finds no sessions. With it set, 戸籍-backed
// authentication is required — the only thing that can produce a live
// verified registration flow — and the same service instance serves the
// routes, the sweep, and the account-transaction claim so deadlines agree.
func secretaryTransferFromEnv(
	pool *pgxpool.Pool,
	registrationStore *koseki.Store,
	allowedOrigins []string,
) (*secretaryTransferMount, error) {
	base := strings.TrimSpace(os.Getenv(transferPublicBaseURLEnv))
	if base == "" {
		return nil, nil
	}
	if registrationStore == nil {
		return nil, fmt.Errorf("%s requires 戸籍-backed authentication (a control-plane database)", transferPublicBaseURLEnv)
	}
	service := transfersession.New(pool, transfersession.Config{})
	registrationStore.Transfers = service
	server, err := transfersession.NewServer(service,
		registrantFlowProof{store: registrationStore, origins: allowedOrigins}, base)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", transferPublicBaseURLEnv, err)
	}
	return &secretaryTransferMount{service: service, server: server}, nil
}
